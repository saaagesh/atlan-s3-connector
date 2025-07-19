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
        self.ai_enhancer = AIEnhancer(self.ai_config) if self.ai_config.google_api_key != "your-google-api-key" else None
        self.utils = AtlanUtils()
        self.performance_monitor = PerformanceMonitor()
        
    async def run_pipeline(self, enable_ai: bool = True) -> Dict[str, any]:
        """
        Execute the complete S3 connector pipeline
        
        Args:
            enable_ai: Whether to run AI enhancement features
            
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
                        logger.info(f"  Inputs: {len(process.inputs) if process.inputs else 0}")
                        logger.info(f"  Outputs: {len(process.outputs) if process.outputs else 0}")
                        logger.info(f"  Connection: {process.connection_qualified_name}")
                    
                    try:
                        table_response = self.s3_connector.atlan_client.asset.save(table_lineage_batch)
                        logger.info(f"Table lineage save response received")
                        
                        created_table_processes = []
                        
                        # Debug: Log response attributes
                        logger.info(f"Response type: {type(table_response)}")
                        
                        # More robustly get all processes from the response, whether created or updated
                        if table_response:
                            created_assets = table_response.assets_created(asset_type=Process)
                            updated_assets = table_response.assets_updated(asset_type=Process)
                            created_table_processes.extend(created_assets)
                            created_table_processes.extend(updated_assets)
                            
                            logger.info(f"Found {len(created_assets)} created and {len(updated_assets)} updated processes.")
                            logger.info(f"Total table lineage processes to use: {len(created_table_processes)}")

                            for process in created_table_processes:
                                logger.info(f"Retrieved table process: {process.name} (GUID: {process.guid})")
                        
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
            
            # Phase 3: AI Enhancement (if enabled)
            ai_results = {}
            if enable_ai and self.ai_enhancer:
                logger.info("Phase 3: AI Enhancement")
                with self.performance_monitor.measure("ai_enhancement"):
                    # Generate intelligent descriptions
                    ai_descriptions = await self.ai_enhancer.generate_asset_descriptions(cataloged_assets)
                    
                    # Classify PII data
                    pii_classifications = await self.ai_enhancer.classify_pii_data(cataloged_assets)
                    
                    # Generate compliance tags
                    compliance_tags = await self.ai_enhancer.generate_compliance_tags(cataloged_assets)
                    
                    # Update assets with AI insights
                    await self.s3_connector.update_assets_with_ai_insights(
                        cataloged_assets, ai_descriptions, pii_classifications, compliance_tags
                    )
                    
                    ai_results = {
                        "descriptions_generated": len(ai_descriptions),
                        "pii_classifications": len(pii_classifications),
                        "compliance_tags": len(compliance_tags)
                    }
            
            # Compile results
            pipeline_duration = time.time() - pipeline_start
            results = {
                "success": True,
                "duration_seconds": pipeline_duration,
                "assets_cataloged": len(cataloged_assets),
                "ai_results": ai_results,
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
    results = await pipeline.run_pipeline(enable_ai=False)
    
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
