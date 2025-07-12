# lineage_builder.py
"""
Advanced Lineage Builder for Atlan S3 Connector
Establishes upstream and downstream relationships with column-level lineage
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
import asyncio
from datetime import datetime

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.lineage import LineageRequest, LineageDirection
from pyatlan.model.assets import Asset, Table, Column

from config import ConnectionConfig, AtlanConfig, FILE_LINEAGE_MAPPING, COLUMN_MAPPINGS

logger = logging.getLogger(__name__)

class LineageBuilder:
    """Advanced lineage builder with intelligent relationship mapping"""
    
    def __init__(self, connection_config: ConnectionConfig, atlan_config: AtlanConfig):
        self.connection_config = connection_config
        self.atlan_config = atlan_config
        
        # Initialize Atlan client
        self.atlan_client = AtlanClient(
            base_url=atlan_config.base_url,
            api_key=atlan_config.api_key
        )
        
        # Cache for resolved assets
        self.asset_cache = {}
        
        # Lineage patterns for intelligent mapping
        self.lineage_patterns = {
            "exact_match": 1.0,      # Exact table name match
            "partial_match": 0.8,    # Partial name match
            "schema_match": 0.9,     # Schema structure match
            "business_logic": 0.7    # Business logic inference
        }
    
    async def build_upstream_lineage(self, s3_assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Build upstream lineage from Postgres to S3
        
        Args:
            s3_assets: List of S3 asset information
            
        Returns:
            List of upstream lineage relationships
        """
        logger.info("Building upstream lineage (Postgres → S3)")
        
        upstream_relationships = []
        
        for s3_asset in s3_assets:
            try:
                # Get corresponding Postgres table
                postgres_table = await self._find_postgres_source(s3_asset)
                
                if postgres_table:
                    relationship = await self._create_upstream_relationship(
                        postgres_table, s3_asset
                    )
                    upstream_relationships.append(relationship)
                    
                    # Build column-level lineage
                    column_lineage = await self._build_column_lineage(
                        postgres_table, s3_asset, "upstream"
                    )
                    relationship['column_lineage'] = column_lineage
                    
                    logger.info(f"Created upstream lineage: {postgres_table['name']} → {s3_asset['metadata']['key']}")
                
            except Exception as e:
                logger.error(f"Failed to build upstream lineage for {s3_asset['metadata']['key']}: {str(e)}")
        
        logger.info(f"Built {len(upstream_relationships)} upstream relationships")
        return upstream_relationships
    
    async def build_downstream_lineage(self, s3_assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Build downstream lineage from S3 to Snowflake
        
        Args:
            s3_assets: List of S3 asset information
            
        Returns:
            List of downstream lineage relationships
        """
        logger.info("Building downstream lineage (S3 → Snowflake)")
        
        downstream_relationships = []
        
        for s3_asset in s3_assets:
            try:
                # Get corresponding Snowflake table
                snowflake_table = await self._find_snowflake_target(s3_asset)
                
                if snowflake_table:
                    relationship = await self._create_downstream_relationship(
                        s3_asset, snowflake_table
                    )
                    downstream_relationships.append(relationship)
                    
                    # Build column-level lineage
                    column_lineage = await self._build_column_lineage(
                        s3_asset, snowflake_table, "downstream"
                    )
                    relationship['column_lineage'] = column_lineage
                    
                    logger.info(f"Created downstream lineage: {s3_asset['metadata']['key']} → {snowflake_table['name']}")
                
            except Exception as e:
                logger.error(f"Failed to build downstream lineage for {s3_asset['metadata']['key']}: {str(e)}")
        
        logger.info(f"Built {len(downstream_relationships)} downstream relationships")
        return downstream_relationships
    
    async def build_column_lineage(self, s3_assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Build comprehensive column-level lineage across the entire pipeline
        
        Args:
            s3_assets: List of S3 asset information
            
        Returns:
            List of column-level lineage relationships
        """
        logger.info("Building column-level lineage")
        
        column_relationships = []
        
        for s3_asset in s3_assets:
            try:
                # Get source and target tables
                postgres_table = await self._find_postgres_source(s3_asset)
                snowflake_table = await self._find_snowflake_target(s3_asset)
                
                if postgres_table and snowflake_table:
                    # Build end-to-end column lineage
                    end_to_end_lineage = await self._build_end_to_end_column_lineage(
                        postgres_table, s3_asset, snowflake_table
                    )
                    column_relationships.extend(end_to_end_lineage)
                
            except Exception as e:
                logger.error(f"Failed to build column lineage for {s3_asset['metadata']['key']}: {str(e)}")
        
        logger.info(f"Built {len(column_relationships)} column-level relationships")
        return column_relationships
    
    async def _find_postgres_source(self, s3_asset: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find corresponding Postgres table for S3 asset"""
        
        asset_key = s3_asset['metadata']['key']
        file_mapping = s3_asset['metadata']['file_mapping']
        
        # Check if mapping exists
        if not file_mapping.get('postgres_table'):
            logger.warning(f"No Postgres mapping found for {asset_key}")
            return None
        
        postgres_table_name = file_mapping['postgres_table']
        
        # Build qualified name for Postgres table
        postgres_qualified_name = f"{self.connection_config.postgres_connection_qn}/{postgres_table_name}"
        
        try:
            # Try to get cached asset first
            if postgres_qualified_name in self.asset_cache:
                return self.asset_cache[postgres_qualified_name]
            
            # Search for the Postgres table
            postgres_table = await self._get_asset_by_qualified_name(postgres_qualified_name)
            
            if postgres_table:
                # Cache the result
                self.asset_cache[postgres_qualified_name] = postgres_table
                return postgres_table
            
        except Exception as e:
            logger.error(f"Failed to find Postgres table {postgres_table_name}: {str(e)}")
        
        return None
    
    async def _find_snowflake_target(self, s3_asset: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find corresponding Snowflake table for S3 asset"""
        
        asset_key = s3_asset['metadata']['key']
        file_mapping = s3_asset['metadata']['file_mapping']
        
        # Check if mapping exists
        if not file_mapping.get('snowflake_table'):
            logger.warning(f"No Snowflake mapping found for {asset_key}")
            return None
        
        snowflake_table_name = file_mapping['snowflake_table']
        
        # Build qualified name for Snowflake table
        snowflake_qualified_name = f"{self.connection_config.snowflake_connection_qn}/{snowflake_table_name}"
        
        try:
            # Try to get cached asset first
            if snowflake_qualified_name in self.asset_cache:
                return self.asset_cache[snowflake_qualified_name]
            
            # Search for the Snowflake table
            snowflake_table = await self._get_asset_by_qualified_name(snowflake_qualified_name)
            
            if snowflake_table:
                # Cache the result
                self.asset_cache[snowflake_qualified_name] = snowflake_table
                return snowflake_table
            
        except Exception as e:
            logger.error(f"Failed to find Snowflake table {snowflake_table_name}: {str(e)}")
        
        return None
    
    async def _get_asset_by_qualified_name(self, qualified_name: str) -> Optional[Dict[str, Any]]:
        """Get asset by qualified name from Atlan"""
        
        try:
            # Use Atlan SDK to search for asset
            search_results = self.atlan_client.asset.search(
                query=f'qualifiedName:"{qualified_name}"',
                size=1
            )
            
            if search_results.assets and len(search_results.assets) > 0:
                asset = search_results.assets[0]
                
                # Get columns for the asset
                columns = await self._get_asset_columns(asset)
                
                return {
                    'asset': asset,
                    'name': asset.name,
                    'qualified_name': asset.qualified_name,
                    'guid': asset.guid,
                    'columns': columns
                }
            
        except Exception as e:
            logger.error(f"Failed to get asset {qualified_name}: {str(e)}")
        
        return None
    
    async def _get_asset_columns(self, asset: Asset) -> List[Dict[str, Any]]:
        """Get columns for a table asset"""
        
        columns = []
        
        try:
            # Get lineage to find columns
            lineage_request =
