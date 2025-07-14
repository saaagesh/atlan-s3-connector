# lineage_builder.py
"""
Advanced Lineage Builder for Atlan S3 Connector
Establishes upstream and downstream relationships with column-level lineage
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
import asyncio
from datetime import datetime

from atlan_client import get_atlan_client
from pyatlan.model.assets import Asset, Table, Column, Process
from pyatlan.model.fluent_search import FluentSearch
from pyatlan.errors import NotFoundError

from config import ConnectionConfig

logger = logging.getLogger(__name__)

class LineageBuilder:
    """Builds lineage using Atlan Process assets."""

    def __init__(self, connection_config: ConnectionConfig):
        self.connection_config = connection_config
        self.atlan_client = get_atlan_client()
        self.postgres_tables = []
        self.snowflake_tables = []

    def _get_postgres_table(self, table_name: str) -> Optional[Table]:
        """Fetches a PostgreSQL table asset from Atlan by name."""
        if not self.postgres_tables:
            logger.info("Fetching postgres tables from Atlan and populating cache.")
            request = (
                FluentSearch()
                .where(FluentSearch.asset_type(Table))
                .where(FluentSearch.active_assets())
                .where(Table.CONNECTION_NAME.eq(self.connection_config.postgres_connection_name))
            ).to_request()
            for result in self.atlan_client.asset.search(request):
                self.postgres_tables.append(result)
            logger.info(f"Found {len(self.postgres_tables)} PostgreSQL tables.")
        
        for table in self.postgres_tables:
            if table.name == table_name:
                return table
        logger.warning(f"PostgreSQL table '{table_name}' not found in Atlan cache.")
        return None

    def _get_snowflake_table(self, table_name: str) -> Optional[Table]:
        """Fetches a Snowflake table asset from Atlan by name."""
        if not self.snowflake_tables:
            logger.info("Fetching snowflake tables from Atlan and populating cache.")
            request = (
                FluentSearch()
                .where(FluentSearch.asset_type(Table))
                .where(FluentSearch.active_assets())
                .where(Table.CONNECTION_NAME.eq(self.connection_config.snowflake_connection_name))
            ).to_request()
            for result in self.atlan_client.asset.search(request):
                self.snowflake_tables.append(result)
            logger.info(f"Found {len(self.snowflake_tables)} Snowflake tables.")

        for table in self.snowflake_tables:
            if table.name == table_name:
                return table
        logger.warning(f"Snowflake table '{table_name}' not found in Atlan cache.")
        return None

    def build_lineage(self, s3_assets: List[Dict[str, Any]], connection_qn: str, lineage_batch: list):
        """Builds lineage between S3 assets and their corresponding DB tables."""
        logger.info("Building lineage...")
        for s3_asset_info in s3_assets:
            s3_asset = s3_asset_info['asset']
            # Assumes file name matches table name, e.g., "customers.csv" -> "CUSTOMERS"
            table_name = s3_asset_info['metadata']['key'].split('.')[0].upper()
            
            pg_table = self._get_postgres_table(table_name)
            sf_table = self._get_snowflake_table(table_name)

            if pg_table:
                process_pg_to_s3 = Process.creator(
                    name=f"{pg_table.name} - Postgres to S3",
                    connection_qualified_name=connection_qn,
                    inputs=[pg_table],
                    outputs=[s3_asset]
                )
                lineage_batch.append(process_pg_to_s3)
                logger.info(f"Created process lineage between postgres table {pg_table.name} and s3 object {s3_asset.name}.")

            if sf_table:
                process_s3_to_sf = Process.creator(
                    name=f"{sf_table.name} - S3 to Snowflake",
                    connection_qualified_name=connection_qn,
                    inputs=[s3_asset],
                    outputs=[sf_table]
                )
                lineage_batch.append(process_s3_to_sf)
                logger.info(f"Created process lineage between s3 object {s3_asset.name} and snowflake table {sf_table.name}.")

    
