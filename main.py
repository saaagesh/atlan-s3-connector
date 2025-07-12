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

from config import AtlanConfig, S3Config, ConnectionConfig, AIConfig
from s3_connector import S3Connector
from lineage_builder import LineageBuilder
from ai_enhancer import AIEnhancer
from utils import AtlanUtils, PerformanceMonitor

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
        self.atlan_config = AtlanConfig()
        self.s3_config = S3Config()
        self.connection_config = ConnectionConfig()
        self.ai_config = AIConfig()
        
        # Initialize components
        self.s3_connector = S3Connector(self.s3_config, self.atlan_config)
        self.lineage_builder = LineageBuilder(self.connection_config, self.atlan_config)
        self.ai_enhancer = AIEnhancer(self.ai_config) if self.ai_config.openai_api_key != "your-openai-key" else None
        self.utils = AtlanUtils(self.atlan_config)
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
            logger.info("Phase 2: Lineage Relationship Building")
            with self.performance_monitor.measure("lineage_building"):
                upstream_lineage = await self.lineage_builder.build_upstream_lineage(cataloged_assets)
                downstream_lineage = await self.lineage_builder.build_downstream_lineage(cataloged_assets)
                column_lineage = await self.lineage_builder.build_column_lineage(cataloged_assets)
            
            logger.info(f"Built {len(upstream_lineage)} upstream and {len(downstream_lineage)} downstream relationships")
            
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
            
            # Phase 4: Validation and Quality Checks
            logger.info("Phase 4: Quality Validation")
            with self.performance_monitor.measure("validation"):
                validation_results = await self.validate_pipeline_results(
                    cataloged_assets, upstream_lineage, downstream_lineage
                )
            
            # Compile results
            pipeline_duration = time.time() - pipeline_start
            results = {
                "success": True,
                "duration_seconds": pipeline_duration,
                "assets_cataloged": len(cataloged_assets),
                "upstream_lineage_count": len(upstream_lineage),
                "downstream_lineage_count": len(downstream_lineage),
                "column_lineage_count": len(column_lineage),
                "ai_results": ai_results,
                "validation_results": validation_results,
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
    
    async def validate_pipeline_results(
        self, 
        assets: List[Dict], 
        upstream_lineage: List[Dict], 
        downstream_lineage: List[Dict]
    ) -> Dict[str, any]:
        """Validate pipeline results for quality assurance"""
        
        validation_results = {
            "total_assets": len(assets),
            "assets_with_lineage": 0,
            "assets_with_descriptions": 0,
            "pii_assets_tagged": 0,
            "compliance_coverage": 0,
            "quality_score": 0
        }
        
        # Count assets with various attributes
        for asset in assets:
            if asset.get("upstream_lineage") or asset.get("downstream_lineage"):
                validation_results["assets_with_lineage"] += 1
            
            if asset.get("description") and len(asset["description"]) > 10:
                validation_results["assets_with_descriptions"] += 1
            
            if asset.get("pii_classification"):
                validation_results["pii_assets_tagged"] += 1
            
            if asset.get("compliance_tags"):
                validation_results["compliance_coverage"] += 1
        
        # Calculate quality score
        total_assets = len(assets)
        if total_assets > 0:
            quality_score = (
                (validation_results["assets_with_lineage"] / total_assets * 0.4) +
                (validation_results["assets_with_descriptions"] / total_assets * 0.3) +
                (validation_results["pii_assets_tagged"] / total_assets * 0.2) +
                (validation_results["compliance_coverage"] / total_assets * 0.1)
            ) * 100
            validation_results["quality_score"] = round(quality_score, 2)
        
        return validation_results
    
    async def run_incremental_update(self) -> Dict[str, any]:
        """Run incremental updates for new or modified S3 objects"""
        logger.info("Running incremental update")
        
        # Get last run timestamp
        last_run = await self.utils.get_last_run_timestamp()
        
        # Find new/modified objects since last run
        new_objects = await self.s3_connector.get_modified_objects_since(last_run)
        
        if not new_objects:
            logger.info("No new or modified objects found")
            return {"success": True, "new_objects": 0}
        
        # Process new objects
        cataloged_assets = await self.s3_connector.catalog_s3_objects(new_objects)
        
        # Build lineage for new assets
        await self.lineage_builder.build_upstream_lineage(cataloged_assets)
        await self.lineage_builder.build_downstream_lineage(cataloged_assets)
        
        # Update timestamp
        await self.utils.update_last_run_timestamp()
        
        logger.info(f"Incremental update completed: {len(new_objects)} objects processed")
        return {"success": True, "new_objects": len(new_objects)}

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
    print(f"Upstream Lineage: {results.get('upstream_lineage_count', 0)}")
    print(f"Downstream Lineage: {results.get('downstream_lineage_count', 0)}")
    print(f"Column Lineage: {results.get('column_lineage_count', 0)}")
    
    if results.get('ai_results'):
        print(f"AI Descriptions: {results['ai_results'].get('descriptions_generated', 0)}")
        print(f"PII Classifications: {results['ai_results'].get('pii_classifications', 0)}")
        print(f"Compliance Tags: {results['ai_results'].get('compliance_tags', 0)}")
    
    if results.get('validation_results'):
        print(f"Quality Score: {results['validation_results'].get('quality_score', 0)}%")
    
    print("="*50)
    
    # Save detailed results to file
    import json
    with open('pipeline_results.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print("Detailed results saved to pipeline_results.json")

if __name__ == "__main__":
    asyncio.run(main())
