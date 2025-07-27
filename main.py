# main.py
"""
Main orchestrator for Atlan S3 Connector
Handles the complete workflow: Discovery → Cataloging → Lineage → AI Enhancement
"""

import logging
import asyncio
import time
from typing import List, Dict, Optional
from dataclasses import asdict

from config import S3Config, ConnectionConfig, AIConfig
from s3_connector import S3Connector
from lineage_builder import LineageBuilder
from ai_enhancer import AIEnhancer
from pii_classifier import PIIClassifier, PIIClassification, CIARating
from pii_inventory import PIIInventoryManager
from utils import AtlanUtils, PerformanceMonitor
from pyatlan.model.assets import Process

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('atlan_s3_connector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AtlanS3Pipeline:
    """Main pipeline orchestrator for S3 connector"""
    
    def __init__(self):
        self.s3_config = S3Config()
        self.connection_config = ConnectionConfig()
        self.ai_config = AIConfig()
        
        # Initialize components
        self.s3_connector = S3Connector(self.s3_config)
        self.lineage_builder = LineageBuilder(self.connection_config)
        self.ai_enhancer = AIEnhancer(self.ai_config, self.s3_connector.atlan_client) if self.ai_config.google_api_key != "your-google-api-key" else None
        self.pii_inventory_manager = PIIInventoryManager(self.s3_connector.atlan_client)
        self.utils = AtlanUtils()
        self.performance_monitor = PerformanceMonitor()
        
    async def run_pipeline(self, enable_ai: bool = True, enable_pii_inventory: bool = True) -> Dict[str, any]:
        """
        Execute the complete S3 connector pipeline
        
        Args:
            enable_ai: Whether to run AI enhancement features
            enable_pii_inventory: Whether to generate PII inventory
            
        Returns:
            Dictionary containing pipeline results and metrics
        """
        logger.info("Starting Atlan S3 Connector Pipeline")
        pipeline_start = time.time()
        
        try:
            # Phase 1: Discover and catalog S3 objects
            logger.info("Phase 1: S3 Asset Discovery and Cataloging")
            with self.performance_monitor.measure("s3_discovery"):
                s3_objects = await self.s3_connector.discover_s3_objects()
                cataloged_assets = await self.s3_connector.catalog_s3_objects(s3_objects)
            
            logger.info(f"Cataloged {len(cataloged_assets)} S3 assets")
            
            # Phase 2: Build lineage relationships
            logger.info("Phase 2: Lineage Relationship Building (with cleanup)")
            with self.performance_monitor.measure("lineage_building"):
                # Phase 2a: Create table-level lineage processes
                table_lineage_batch = []
                # Use connection-level qualified name for lineage processes (not database/schema level)
                # Extract just the connection part: "default/postgres/1752268493" from "default/postgres/1752268493/FOOD_BEVERAGE/SALES_ORDERS"
                postgres_connection_parts = self.connection_config.postgres_connection_qn.split('/')
                lineage_connection_qn = '/'.join(postgres_connection_parts[:3])  # Take first 3 parts: default/postgres/1752268493
                logger.info(f"Using lineage connection QN: {lineage_connection_qn}")
                column_processes_info = self.lineage_builder.build_lineage(
                    cataloged_assets, 
                    lineage_connection_qn, 
                    table_lineage_batch, 
                    cleanup_first=True
                )
                
                # Save table-level processes first
                if table_lineage_batch:
                    logger.info(f"Phase 2a: Saving {len(table_lineage_batch)} table-level lineage processes...")
                    
                    # Log process details for debugging
                    for i, process in enumerate(table_lineage_batch):
                        logger.info(f"Table Process {i+1}: {process.name}")
                        # process_id attribute is not supported in this version of the SDK
                        # logger.info(f"  Process ID: {process.process_id}")
                        logger.info(f"  Inputs: {len(process.inputs) if process.inputs else 0}")
                        logger.info(f"  Outputs: {len(process.outputs) if process.outputs else 0}")
                        logger.info(f"  Connection: {process.connection_qualified_name}")
                        if process.inputs:
                            for j, inp in enumerate(process.inputs):
                                logger.info(f"    Input {j+1}: {inp.guid if hasattr(inp, 'guid') else 'No GUID'}")
                        if process.outputs:
                            for j, out in enumerate(process.outputs):
                                logger.info(f"    Output {j+1}: {out.guid if hasattr(out, 'guid') else 'No GUID'}")
                    
                    try:
                        logger.info(f"Saving {len(table_lineage_batch)} lineage processes to Atlan...")
                        table_response = self.s3_connector.atlan_client.asset.save(table_lineage_batch)
                        logger.info(f"Table lineage save response received")
                        
                        # Debug: Log response details
                        logger.info(f"Response type: {type(table_response)}")
                        logger.info(f"Response attributes: {[attr for attr in dir(table_response) if not attr.startswith('_')]}")
                        
                        # Check if there are any errors in the response
                        if hasattr(table_response, 'mutated_entities') and table_response.mutated_entities:
                            logger.info(f"Mutated entities count: {len(table_response.mutated_entities)}")
                            for i, entity in enumerate(table_response.mutated_entities[:3]):  # Log first 3
                                logger.info(f"  Entity {i+1}: {getattr(entity, 'type_name', 'Unknown')} - {getattr(entity, 'name', 'No name')} (GUID: {getattr(entity, 'guid', 'No GUID')})")
                        
                        if hasattr(table_response, 'partial_updated_entities') and table_response.partial_updated_entities:
                            logger.info(f"Partial updated entities count: {len(table_response.partial_updated_entities)}")
                        
                        if hasattr(table_response, 'guid_assignments') and table_response.guid_assignments:
                            logger.info(f"GUID assignments count: {len(table_response.guid_assignments)}")
                            # Log first few GUID assignments
                            for i, (temp_guid, real_guid) in enumerate(list(table_response.guid_assignments.items())[:3]):
                                logger.info(f"  Assignment {i+1}: {temp_guid} -> {real_guid}")
                        
                        # Check for errors
                        if hasattr(table_response, 'errors') and table_response.errors:
                            logger.error(f"Response errors: {table_response.errors}")
                        else:
                            logger.info("No errors in response")
                        
                        created_table_processes = []
                        
                        # More robustly get all processes from the response, whether created or updated
                        if table_response:
                            try:
                                # Try different approaches to get the created processes
                                logger.info("Attempting to retrieve created processes from response...")
                                
                                # Method 1: Try to get mutated entities directly
                                try:
                                    if hasattr(table_response, 'mutated_entities') and table_response.mutated_entities:
                                        logger.info(f"Method 1 - Found {len(table_response.mutated_entities)} mutated entities")
                                        for entity in table_response.mutated_entities:
                                            if hasattr(entity, 'type_name') and entity.type_name == 'Process':
                                                created_table_processes.append(entity)
                                                logger.info(f"Found Process from mutated entities: {entity.name} (GUID: {entity.guid})")
                                except Exception as method1_error:
                                    logger.warning(f"Method 1 failed: {str(method1_error)}")
                                
                                # Method 2: Try guid_assignments
                                if not created_table_processes and hasattr(table_response, 'guid_assignments'):
                                    try:
                                        logger.info(f"Method 2 - Found {len(table_response.guid_assignments)} GUID assignments")
                                        # The guid_assignments contains mapping of temporary GUIDs to real GUIDs
                                        # We need to search for the processes using the real GUIDs
                                        real_guids = list(table_response.guid_assignments.values())
                                        if real_guids:
                                            logger.info(f"Searching for processes with GUIDs: {real_guids[:5]}...")  # Log first 5
                                            for guid in real_guids:
                                                try:
                                                    asset = self.s3_connector.atlan_client.asset.get_by_guid(guid)
                                                    if hasattr(asset, 'type_name') and asset.type_name == 'Process':
                                                        created_table_processes.append(asset)
                                                        logger.info(f"Found Process by GUID: {asset.name} (GUID: {asset.guid})")
                                                except Exception as guid_error:
                                                    logger.debug(f"GUID {guid} is not a Process: {str(guid_error)}")
                                    except Exception as method2_error:
                                        logger.warning(f"Method 2 failed: {str(method2_error)}")
                                
                                # Method 3: Search for recently created processes if other methods fail
                                if not created_table_processes:
                                    logger.info("Method 3 - Searching for recently created processes...")
                                    try:
                                        from pyatlan.model.fluent_search import FluentSearch
                                        
                                        # Wait a moment for the processes to be indexed
                                        time.sleep(3)
                                        
                                        search_request = (
                                            FluentSearch()
                                            .where(FluentSearch.asset_type(Process))
                                            .where(FluentSearch.active_assets())
                                            .where(Process.CONNECTION_QUALIFIED_NAME.eq(lineage_connection_qn))
                                        ).to_request()
                                        
                                        recent_processes = list(self.s3_connector.atlan_client.asset.search(search_request))
                                        logger.info(f"Method 3 - Found {len(recent_processes)} processes in connection")
                                        
                                        # Filter for our processes by name pattern and recent creation
                                        current_time = time.time()
                                        for process in recent_processes:
                                            if any(pattern in process.name for pattern in ["ETL:", "Extract:", "Load:"]):
                                                # Check if process was created recently (within last 5 minutes)
                                                if hasattr(process, 'create_time') and process.create_time:
                                                    create_timestamp = process.create_time / 1000  # Convert from milliseconds
                                                    if current_time - create_timestamp < 300:  # 5 minutes
                                                        created_table_processes.append(process)
                                                        logger.info(f"Found recent process: {process.name} (GUID: {process.guid})")
                                                else:
                                                    # If no create_time, assume it's recent since we just created it
                                                    created_table_processes.append(process)
                                                    logger.info(f"Found our process: {process.name} (GUID: {process.guid})")
                                    except Exception as method3_error:
                                        logger.error(f"Method 3 failed: {str(method3_error)}")
                                
                                # Method 4: Direct verification - search for processes by name
                                if not created_table_processes:
                                    logger.info("Method 4 - Direct search by process names...")
                                    try:
                                        # Wait a bit more for indexing
                                        time.sleep(5)
                                        
                                        # Search for each process by name
                                        for original_process in table_lineage_batch:
                                            try:
                                                search_request = (
                                                    FluentSearch()
                                                    .where(FluentSearch.asset_type(Process))
                                                    .where(FluentSearch.active_assets())
                                                    .where(Process.NAME.eq(original_process.name))
                                                    .where(Process.CONNECTION_QUALIFIED_NAME.eq(lineage_connection_qn))
                                                ).to_request()
                                                
                                                found_processes = list(self.s3_connector.atlan_client.asset.search(search_request))
                                                if found_processes:
                                                    found_process = found_processes[0]  # Take the first match
                                                    created_table_processes.append(found_process)
                                                    logger.info(f"Found process by name: {found_process.name} (GUID: {found_process.guid})")
                                                else:
                                                    logger.warning(f"Could not find process by name: {original_process.name}")
                                            except Exception as name_search_error:
                                                logger.error(f"Error searching for process {original_process.name}: {str(name_search_error)}")
                                    except Exception as method4_error:
                                        logger.error(f"Method 4 failed: {str(method4_error)}")
                                
                                # Method 5: Fallback - use the processes we sent for creation
                                if not created_table_processes:
                                    logger.info("Method 5 - Using original processes as fallback...")
                                    # This is a fallback - we'll use the processes we created, but they won't have real GUIDs
                                    # This might work for column lineage creation if the processes were actually saved
                                    logger.warning("Using original process objects - column lineage may fail if GUIDs are not real")
                                    created_table_processes = table_lineage_batch.copy()
                                
                                logger.info(f"Total table lineage processes to use: {len(created_table_processes)}")
                                
                                for process in created_table_processes:
                                    logger.info(f"Retrieved table process: {process.name} (GUID: {getattr(process, 'guid', 'No GUID')})")
                                    
                            except Exception as response_error:
                                logger.error(f"Error processing response: {str(response_error)}")
                                logger.error(f"Response type: {type(table_response)}")
                                logger.error(f"Response attributes: {dir(table_response)}")
                        
                        # Phase 2b: Create column-level lineage processes if we have column info
                        if column_processes_info and created_table_processes:
                            logger.info(f"Phase 2b: Creating {len(column_processes_info)} column-level lineage processes...")
                            
                            column_lineage_batch = self.lineage_builder.create_column_lineage_processes(
                                column_processes_info, 
                                created_table_processes
                            )
                            
                            if column_lineage_batch:
                                logger.info(f"Saving {len(column_lineage_batch)} column lineage processes...")
                                
                                try:
                                    column_response = self.s3_connector.atlan_client.asset.save(column_lineage_batch)
                                    logger.info(f"Column lineage save response received")
                                    
                                    # Use the correct response handling as per sample code
                                    try:
                                        from pyatlan.model.assets import ColumnProcess
                                        created_column_processes = column_response.assets_created(ColumnProcess)
                                        updated_columns = column_response.assets_updated(Column)
                                        
                                        logger.info(f"Successfully created {len(created_column_processes)} column lineage processes in Atlan")
                                        logger.info(f"Updated {len(updated_columns)} columns with lineage information")
                                        
                                        for i, process in enumerate(created_column_processes[:3]):  # Log first 3
                                            logger.info(f"Created column process {i+1}: {process.name} (GUID: {process.guid})")
                                            
                                    except Exception as response_parse_error:
                                        logger.warning(f"Could not parse column response using standard method: {str(response_parse_error)}")
                                        # Fallback to checking mutated entities
                                        if hasattr(column_response, 'mutated_entities') and column_response.mutated_entities:
                                            logger.info(f"Fallback: Found {len(column_response.mutated_entities)} mutated entities")
                                        elif hasattr(column_response, 'guid_assignments') and column_response.guid_assignments:
                                            logger.info(f"Fallback: Found {len(column_response.guid_assignments)} GUID assignments")
                                        
                                except Exception as e:
                                    logger.error(f"Failed to save column lineage processes: {str(e)}")
                                    logger.error(f"Error type: {type(e).__name__}")
                            else:
                                logger.warning("No column lineage processes were created")
                        elif column_processes_info and not created_table_processes:
                            logger.warning(f"Have {len(column_processes_info)} column processes to create but no table processes found")
                            logger.warning("This suggests the table lineage creation may have failed silently")
                        else:
                            logger.info("Skipping column lineage creation - no column info or table processes")
                            
                    except Exception as e:
                        logger.error(f"Failed to save table lineage processes: {str(e)}")
                        logger.error(f"Error type: {type(e).__name__}")
                else:
                    logger.warning("No lineage relationships were created - no matching tables found")
            
            logger.info(f"Built lineage for {len(cataloged_assets)} assets")
            
            # Phase 3: AI Enhancement and PII Classification
            ai_results = {}
            pii_inventory_results = {}
            
            if enable_ai and self.ai_enhancer:
                logger.info("Phase 3: AI Enhancement and PII Classification")
                with self.performance_monitor.measure("ai_enhancement"):
                    # Pass Atlan client to AI enhancer for PII classification
                    if not self.ai_enhancer.atlan_client:
                        self.ai_enhancer.atlan_client = self.s3_connector.atlan_client
                        self.ai_enhancer.pii_classifier = PIIClassifier(self.s3_connector.atlan_client)
                    
                    # Generate intelligent descriptions
                    ai_descriptions = await self.ai_enhancer.generate_asset_descriptions(cataloged_assets)
                    
                    # Classify PII data with enhanced classifier
                    pii_classifications = await self.ai_enhancer.classify_pii_data(cataloged_assets)
                    
                    # Store PII classifications for later use
                    self.ai_enhancer.pii_classifications = pii_classifications
                    
                    # Generate compliance tags based on PII classifications
                    compliance_tags = await self.ai_enhancer.generate_compliance_tags(cataloged_assets)
                    
                    # Update assets with AI insights and PII classifications
                    await self.s3_connector.update_assets_with_ai_insights(
                        cataloged_assets, ai_descriptions, pii_classifications, compliance_tags
                    )
                    
                    ai_results = {
                        "descriptions_generated": len(ai_descriptions),
                        "pii_classifications": len(pii_classifications),
                        "compliance_tags": len(compliance_tags)
                    }
                    
                    # Phase 3b: Propagate PII classifications through lineage
                    if enable_pii_inventory and self.ai_enhancer.pii_classifier:
                        logger.info("Phase 3b: Propagating PII classifications through lineage")
                        
                        propagation_results = {
                            "assets_processed": 0,
                            "propagated_to": 0,
                            "failed": 0
                        }
                        
                        for asset_info in cataloged_assets:
                            asset = asset_info['asset']
                            asset_key = asset_info['metadata']['key']
                            
                            if asset_key in pii_classifications and pii_classifications[asset_key].get('has_pii', False):
                                try:
                                    # Create a PIIClassification object from the dictionary
                                    from pii_classifier import PIIClassification, CIARating, ConfidentialityLevel, IntegrityLevel, AvailabilityLevel
                                    
                                    pii_data = pii_classifications[asset_key]
                                    cia_data = pii_data.get('cia_rating', {})
                                    
                                    # Convert string values to enum values
                                    conf_level = ConfidentialityLevel(cia_data.get('confidentiality', 'Low'))
                                    int_level = IntegrityLevel(cia_data.get('integrity', 'Low'))
                                    avail_level = AvailabilityLevel(cia_data.get('availability', 'Low'))
                                    
                                    classification = PIIClassification(
                                        has_pii=pii_data.get('has_pii', False),
                                        pii_types=pii_data.get('pii_types', []),
                                        sensitivity_level=pii_data.get('sensitivity_level', 'Low'),
                                        confidence=pii_data.get('confidence', 0.7),
                                        cia_rating=CIARating(
                                            confidentiality=conf_level,
                                            integrity=int_level,
                                            availability=avail_level
                                        ),
                                        sensitive_columns=pii_data.get('sensitive_columns', [])
                                    )
                                    
                                    # Propagate classification through lineage
                                    result = await self.ai_enhancer.pii_classifier.propagate_classification_through_lineage(
                                        asset.guid, classification
                                    )
                                    
                                    propagation_results["assets_processed"] += 1
                                    propagation_results["propagated_to"] += len(result.get("propagated_to", []))
                                    propagation_results["failed"] += len(result.get("failed", []))
                                    
                                except Exception as e:
                                    logger.error(f"Failed to propagate classification for {asset_key}: {str(e)}")
                                    propagation_results["failed"] += 1
                        
                        # Generate PII inventory report
                        try:
                            pii_inventory = await self.ai_enhancer.pii_classifier.generate_pii_inventory_report()
                            
                            # Save inventory report to file
                            import json
                            with open('pii_inventory_report.json', 'w') as f:
                                json.dump(pii_inventory, f, indent=2, default=str)
                            
                            logger.info("Generated PII inventory report: pii_inventory_report.json")
                            pii_inventory_results = {
                                "report_generated": True,
                                "report_file": "pii_inventory_report.json",
                                "propagation_results": propagation_results
                            }
                            
                        except Exception as e:
                            logger.error(f"Failed to generate PII inventory report: {str(e)}")
                            pii_inventory_results = {
                                "report_generated": False,
                                "error": str(e),
                                "propagation_results": propagation_results
                            }
            
            # Compile results
            pipeline_duration = time.time() - pipeline_start
            results = {
                "success": True,
                "duration_seconds": pipeline_duration,
                "assets_cataloged": len(cataloged_assets),
                "ai_results": ai_results,
                "pii_inventory_results": pii_inventory_results,
                "performance_metrics": self.performance_monitor.get_metrics()
            }
            
            logger.info(f"Pipeline completed successfully in {pipeline_duration:.2f} seconds")
            return results
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "duration_seconds": time.time() - pipeline_start
            }
    
async def main():
    """Main execution function"""
    pipeline = AtlanS3Pipeline()
    
    # Run full pipeline
    results = await pipeline.run_pipeline(enable_ai=True)
    
    # Print results
    print("\n" + "="*50)
    print("ATLAN S3 CONNECTOR PIPELINE RESULTS")
    print("="*50)
    print(f"Success: {results['success']}")
    print(f"Duration: {results.get('duration_seconds', 0):.2f} seconds")
    print(f"Assets Cataloged: {results.get('assets_cataloged', 0)}")
    
    if results.get('ai_results'):
        print(f"AI Descriptions: {results['ai_results'].get('descriptions_generated', 0)}")
        print(f"PII Classifications: {results['ai_results'].get('pii_classifications', 0)}")
        print(f"Compliance Tags: {results['ai_results'].get('compliance_tags', 0)}")
    
    print("="*50)
    
    # Save detailed results to file
    import json
    with open('pipeline_results.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print("Detailed results saved to pipeline_results.json")

if __name__ == "__main__":
    asyncio.run(main())
