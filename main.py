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
                column_processes_info = self.lineage_builder.build_lineage(
                    cataloged_assets, 
                    self.s3_connector.connection_qn, 
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
                        table_response = self.s3_connector.atlan_client.asset.save(table_lineage_batch)
                        logger.info(f"Table lineage save response received")
                        
                        # Check if there are any errors in the response
                        if hasattr(table_response, 'mutated_entities') and table_response.mutated_entities:
                            logger.info(f"Mutated entities: {table_response.mutated_entities}")
                        
                        if hasattr(table_response, 'partial_updated_entities') and table_response.partial_updated_entities:
                            logger.info(f"Partial updated entities: {table_response.partial_updated_entities}")
                        
                        created_table_processes = []
                        
                        # Debug: Log response attributes
                        logger.info(f"Response type: {type(table_response)}")
                        
                        # More robustly get all processes from the response, whether created or updated
                        if table_response:
                            try:
                                created_assets = table_response.assets_created(asset_type=Process)
                                updated_assets = table_response.assets_updated(asset_type=Process)
                                created_table_processes.extend(created_assets)
                                created_table_processes.extend(updated_assets)
                                
                                logger.info(f"Found {len(created_assets)} created and {len(updated_assets)} updated processes.")
                                logger.info(f"Total table lineage processes to use: {len(created_table_processes)}")

                                for process in created_table_processes:
                                    logger.info(f"Retrieved table process: {process.name} (GUID: {process.guid})")
                            except Exception as response_error:
                                logger.error(f"Error processing response: {str(response_error)}")
                                # Try to get all assets from the response regardless of type
                                try:
                                    all_created = table_response.assets_created()
                                    all_updated = table_response.assets_updated()
                                    logger.info(f"All created assets: {len(all_created)}")
                                    logger.info(f"All updated assets: {len(all_updated)}")
                                    
                                    # Filter for Process assets manually
                                    for asset in all_created + all_updated:
                                        if hasattr(asset, 'type_name') and asset.type_name == 'Process':
                                            created_table_processes.append(asset)
                                            logger.info(f"Found Process asset: {asset.name} (GUID: {asset.guid})")
                                except Exception as fallback_error:
                                    logger.error(f"Fallback response processing failed: {str(fallback_error)}")
                        
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
                                    
                                    if hasattr(column_response, 'assets_created'):
                                        from pyatlan.model.assets import ColumnProcess
                                        created_column_processes = column_response.assets_created(asset_type=ColumnProcess)
                                        logger.info(f"Successfully created {len(created_column_processes)} column lineage processes in Atlan")
                                        
                                        for process in created_column_processes:
                                            logger.info(f"Created column process: {process.name} (GUID: {process.guid})")
                                    else:
                                        logger.warning("Column save response doesn't have assets_created method")
                                        
                                except Exception as e:
                                    logger.error(f"Failed to save column lineage processes: {str(e)}")
                                    logger.error(f"Error type: {type(e).__name__}")
                            else:
                                logger.warning("No column lineage processes were created")
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
