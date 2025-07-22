# pii_inventory.py
"""
PII Inventory Management for Atlan S3 Connector
Provides comprehensive inventory reporting of PII data across the pipeline
"""

import logging
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd
import os

from pyatlan.model.assets import Asset, Table, Column, S3Object
from pyatlan.model.fluent_search import FluentSearch
from pyatlan.client.atlan import AtlanClient

logger = logging.getLogger(__name__)

class PIIInventoryManager:
    """
    Manages PII inventory across the data pipeline
    """
    
    def __init__(self, atlan_client: AtlanClient):
        self.atlan_client = atlan_client
    
    async def generate_inventory_report(self, output_format: str = "json") -> Dict[str, Any]:
        """
        Generate a comprehensive inventory report of PII data across the pipeline
        
        Args:
            output_format: Format for the report ("json" or "csv")
            
        Returns:
            Dictionary containing the PII inventory report
        """
        logger.info("Generating PII inventory report")
        
        # Initialize report structure
        report = {
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total_assets_scanned": 0,
                "assets_with_pii": 0,
                "high_sensitivity_assets": 0,
                "medium_sensitivity_assets": 0,
                "low_sensitivity_assets": 0
            },
            "by_source": {
                "postgres": {
                    "total": 0,
                    "with_pii": 0
                },
                "s3": {
                    "total": 0,
                    "with_pii": 0
                },
                "snowflake": {
                    "total": 0,
                    "with_pii": 0
                }
            },
            "by_pii_type": {
                "email": 0,
                "phone": 0,
                "name": 0,
                "address": 0,
                "id": 0,
                "financial": 0,
                "health": 0,
                "biometric": 0
            },
            "sensitive_assets": []
        }
        
        # Query all assets with PII classification
        try:
            # Search for S3 objects with PII classification
            s3_assets = await self._get_assets_with_pii_classification(S3Object)
            report["by_source"]["s3"]["total"] = len(s3_assets)
            report["by_source"]["s3"]["with_pii"] = sum(1 for asset in s3_assets if asset.get("has_pii", False))
            
            # Search for PostgreSQL tables with PII classification
            postgres_assets = await self._get_assets_with_pii_classification(Table, source_filter="postgres")
            report["by_source"]["postgres"]["total"] = len(postgres_assets)
            report["by_source"]["postgres"]["with_pii"] = sum(1 for asset in postgres_assets if asset.get("has_pii", False))
            
            # Search for Snowflake tables with PII classification
            snowflake_assets = await self._get_assets_with_pii_classification(Table, source_filter="snowflake")
            report["by_source"]["snowflake"]["total"] = len(snowflake_assets)
            report["by_source"]["snowflake"]["with_pii"] = sum(1 for asset in snowflake_assets if asset.get("has_pii", False))
            
            # Combine all assets
            all_assets = s3_assets + postgres_assets + snowflake_assets
            
            # Update summary statistics
            report["summary"]["total_assets_scanned"] = len(all_assets)
            report["summary"]["assets_with_pii"] = sum(1 for asset in all_assets if asset.get("has_pii", False))
            report["summary"]["high_sensitivity_assets"] = sum(1 for asset in all_assets if asset.get("sensitivity_level") in ["High", "Restricted"])
            report["summary"]["medium_sensitivity_assets"] = sum(1 for asset in all_assets if asset.get("sensitivity_level") == "Medium")
            report["summary"]["low_sensitivity_assets"] = sum(1 for asset in all_assets if asset.get("sensitivity_level") == "Low")
            
            # Update PII type counts
            for asset in all_assets:
                if asset.get("has_pii", False):
                    for pii_type in asset.get("pii_types", []):
                        if pii_type.lower() in report["by_pii_type"]:
                            report["by_pii_type"][pii_type.lower()] += 1
            
            # Add sensitive assets to the report
            for asset in all_assets:
                if asset.get("has_pii", False):
                    sensitive_asset = {
                        "name": asset.get("name", ""),
                        "qualified_name": asset.get("qualified_name", ""),
                        "type": asset.get("type_name", ""),
                        "source": self._determine_source(asset.get("qualified_name", "")),
                        "sensitivity_level": asset.get("sensitivity_level", "Low"),
                        "pii_types": asset.get("pii_types", []),
                        "cia_rating": asset.get("cia_rating", {}),
                        "compliance_tags": asset.get("compliance_tags", []),
                        "sensitive_columns": asset.get("sensitive_columns", [])
                    }
                    report["sensitive_assets"].append(sensitive_asset)
            
            # Generate output in requested format
            if output_format.lower() == "csv":
                self._save_report_as_csv(report)
            else:
                self._save_report_as_json(report)
            
            return report
            
        except Exception as e:
            logger.error(f"Failed to generate PII inventory report: {str(e)}")
            return {
                "error": str(e),
                "generated_at": datetime.now().isoformat()
            }
    
    async def _get_assets_with_pii_classification(self, asset_type, source_filter: str = None) -> List[Dict[str, Any]]:
        """
        Get assets with PII classification from Atlan
        
        Args:
            asset_type: Type of asset to query
            source_filter: Optional filter for source system
            
        Returns:
            List of assets with PII classification
        """
        assets = []
        
        try:
            # Build the search request
            search_builder = (
                FluentSearch()
                .where(FluentSearch.asset_type(asset_type))
                .where(FluentSearch.active_assets())
            )
            
            # Add source filter if provided
            if source_filter:
                if asset_type == Table:
                    search_builder = search_builder.where(Table.CONNECTION_NAME.contains(source_filter))
            
            request = search_builder.to_request()
            
            # Execute the search
            for asset in self.atlan_client.asset.search(request):
                # Check if the asset has PII classification
                has_pii = False
                pii_types = []
                sensitivity_level = "Low"
                cia_rating = {}
                sensitive_columns = []
                compliance_tags = []
                
                # Extract PII classification from custom metadata
                try:
                    pii_metadata = asset.get_custom_metadata("PIIClassification")
                    if pii_metadata:
                        has_pii = pii_metadata.get("hasPII") == "Yes"
                        pii_types = [t.strip() for t in pii_metadata.get("piiTypes", "").split(",") if t.strip()]
                        sensitivity_level = pii_metadata.get("sensitivityLevel", "Low")
                        
                        if "sensitiveColumns" in pii_metadata:
                            sensitive_columns = [c.strip() for c in pii_metadata.get("sensitiveColumns", "").split(",") if c.strip()]
                except:
                    pass
                
                # Extract CIA ratings from custom metadata
                try:
                    cia_metadata = asset.get_custom_metadata("CIARating")
                    if cia_metadata:
                        cia_rating = {
                            "confidentiality": cia_metadata.get("confidentiality", "Low"),
                            "integrity": cia_metadata.get("integrity", "Low"),
                            "availability": cia_metadata.get("availability", "Low")
                        }
                except:
                    pass
                
                # Extract compliance tags
                try:
                    if hasattr(asset, "atlan_tags") and asset.atlan_tags:
                        compliance_tags = [tag for tag in asset.atlan_tags]
                except:
                    pass
                
                # Add asset to the list
                assets.append({
                    "guid": asset.guid,
                    "name": asset.name,
                    "qualified_name": asset.qualified_name,
                    "type_name": asset.type_name,
                    "has_pii": has_pii,
                    "pii_types": pii_types,
                    "sensitivity_level": sensitivity_level,
                    "cia_rating": cia_rating,
                    "sensitive_columns": sensitive_columns,
                    "compliance_tags": compliance_tags
                })
            
            return assets
            
        except Exception as e:
            logger.error(f"Failed to get assets with PII classification: {str(e)}")
            return []
    
    def _determine_source(self, qualified_name: str) -> str:
        """Determine the source system from qualified name"""
        if "postgres" in qualified_name.lower():
            return "postgres"
        elif "snowflake" in qualified_name.lower():
            return "snowflake"
        elif "s3" in qualified_name.lower():
            return "s3"
        else:
            return "unknown"
    
    def _save_report_as_json(self, report: Dict[str, Any]) -> str:
        """Save the report as a JSON file"""
        filename = f"pii_inventory_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            logger.info(f"PII inventory report saved as {filename}")
            return filename
        except Exception as e:
            logger.error(f"Failed to save PII inventory report as JSON: {str(e)}")
            return ""
    
    def _save_report_as_csv(self, report: Dict[str, Any]) -> str:
        """Save the report as CSV files"""
        try:
            # Create a directory for the reports
            report_dir = f"pii_inventory_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            os.makedirs(report_dir, exist_ok=True)
            
            # Save summary as CSV
            summary_df = pd.DataFrame([{
                "total_assets_scanned": report["summary"]["total_assets_scanned"],
                "assets_with_pii": report["summary"]["assets_with_pii"],
                "high_sensitivity_assets": report["summary"]["high_sensitivity_assets"],
                "medium_sensitivity_assets": report["summary"]["medium_sensitivity_assets"],
                "low_sensitivity_assets": report["summary"]["low_sensitivity_assets"],
                "generated_at": report["generated_at"]
            }])
            summary_df.to_csv(f"{report_dir}/summary.csv", index=False)
            
            # Save by source as CSV
            source_data = []
            for source, counts in report["by_source"].items():
                source_data.append({
                    "source": source,
                    "total": counts["total"],
                    "with_pii": counts["with_pii"]
                })
            source_df = pd.DataFrame(source_data)
            source_df.to_csv(f"{report_dir}/by_source.csv", index=False)
            
            # Save by PII type as CSV
            pii_type_data = []
            for pii_type, count in report["by_pii_type"].items():
                pii_type_data.append({
                    "pii_type": pii_type,
                    "count": count
                })
            pii_type_df = pd.DataFrame(pii_type_data)
            pii_type_df.to_csv(f"{report_dir}/by_pii_type.csv", index=False)
            
            # Save sensitive assets as CSV
            sensitive_assets_df = pd.DataFrame(report["sensitive_assets"])
            sensitive_assets_df.to_csv(f"{report_dir}/sensitive_assets.csv", index=False)
            
            logger.info(f"PII inventory report saved in directory {report_dir}")
            return report_dir
        except Exception as e:
            logger.error(f"Failed to save PII inventory report as CSV: {str(e)}")
            return ""
    
    async def generate_pii_dashboard_data(self) -> Dict[str, Any]:
        """
        Generate data for a PII dashboard
        
        Returns:
            Dictionary containing dashboard data
        """
        # This is a placeholder for generating dashboard data
        # In a real implementation, you would query Atlan for PII data
        # and format it for visualization
        
        report = await self.generate_inventory_report()
        
        # Format data for dashboard
        dashboard_data = {
            "summary": report["summary"],
            "by_source": [
                {"name": source, "total": data["total"], "with_pii": data["with_pii"]}
                for source, data in report["by_source"].items()
            ],
            "by_pii_type": [
                {"name": pii_type, "count": count}
                for pii_type, count in report["by_pii_type"].items()
            ],
            "by_sensitivity": [
                {"level": "High/Restricted", "count": report["summary"]["high_sensitivity_assets"]},
                {"level": "Medium", "count": report["summary"]["medium_sensitivity_assets"]},
                {"level": "Low", "count": report["summary"]["low_sensitivity_assets"]}
            ],
            "sensitive_assets": report["sensitive_assets"]
        }
        
        return dashboard_data