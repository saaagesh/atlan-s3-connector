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
from pyatlan.model.assets import Asset, Table, Column, Process, ColumnProcess, S3Object
from pyatlan.model.fluent_search import FluentSearch
from pyatlan.errors import NotFoundError

from config import ConnectionConfig

logger = logging.getLogger(__name__)

class LineageBuilder:
    """Builds lineage using Atlan Process assets with column-level lineage support."""

    def __init__(self, connection_config: ConnectionConfig):
        self.connection_config = connection_config
        self.atlan_client = get_atlan_client()
        self.postgres_tables = []
        self.snowflake_tables = []
        self.postgres_columns_cache = {}  # Cache for table columns
        self.snowflake_columns_cache = {}  # Cache for table columns

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

    def _get_table_columns(self, table: Table, table_type: str) -> List[Column]:
        """Fetches columns for a given table with caching."""
        cache_key = f"{table_type}_{table.qualified_name}"
        
        if cache_key in self.postgres_columns_cache or cache_key in self.snowflake_columns_cache:
            cache = self.postgres_columns_cache if table_type == "postgres" else self.snowflake_columns_cache
            return cache[cache_key]
        
        try:
            logger.info(f"Fetching columns for {table_type} table: {table.name}")
            request = (
                FluentSearch()
                .where(FluentSearch.asset_type(Column))
                .where(FluentSearch.active_assets())
                .where(Column.TABLE_QUALIFIED_NAME.eq(table.qualified_name))
            ).to_request()
            
            columns = list(self.atlan_client.asset.search(request))
            
            # Cache the results
            if table_type == "postgres":
                self.postgres_columns_cache[cache_key] = columns
            else:
                self.snowflake_columns_cache[cache_key] = columns
                
            logger.info(f"Found {len(columns)} columns for {table_type} table {table.name}")
            return columns
            
        except Exception as e:
            logger.error(f"Error fetching columns for {table_type} table {table.name}: {str(e)}")
            return []

    def _create_column_lineage_mappings(self, s3_columns: List[Dict], db_columns: List[Column]) -> List[Tuple[Column, str]]:
        """
        Creates column mappings between S3 CSV columns and database columns.
        Assumes direct mapping by column name (case-insensitive).
        
        Args:
            s3_columns: List of S3 column info from schema inference
            db_columns: List of database Column assets
            
        Returns:
            List of tuples (db_column, s3_column_name)
        """
        mappings = []
        
        # Create a case-insensitive lookup for S3 columns
        s3_column_names = {col['name'].lower(): col['name'] for col in s3_columns}
        
        for db_column in db_columns:
            db_col_name_lower = db_column.name.lower()
            if db_col_name_lower in s3_column_names:
                s3_col_name = s3_column_names[db_col_name_lower]
                mappings.append((db_column, s3_col_name))
                logger.debug(f"Mapped column: {db_column.name} -> {s3_col_name}")
        
        logger.info(f"Created {len(mappings)} column mappings out of {len(db_columns)} database columns")
        return mappings

    def _create_rich_column_mapping_description(self, pg_columns: List[Column], sf_columns: List[Column], 
                                              s3_columns: List[Dict], table_name: str) -> str:
        """
        Create a rich description showing detailed column mappings
        
        Args:
            pg_columns: PostgreSQL columns
            sf_columns: Snowflake columns  
            s3_columns: S3 column information
            table_name: Table name
            
        Returns:
            Rich description with column mapping details
        """
        description_parts = [f"Data lineage for {table_name} with detailed column mappings:"]
        
        # Create column mappings
        pg_mappings = self._create_column_lineage_mappings(s3_columns, pg_columns) if pg_columns else []
        sf_mappings = self._create_column_lineage_mappings(s3_columns, sf_columns) if sf_columns else []
        
        # Build mapping details
        if pg_mappings and sf_mappings:
            description_parts.append("\n📊 COLUMN MAPPINGS:")
            
            # Create lookup for end-to-end mappings
            s3_to_sf_lookup = {s3_col: sf_col.name for sf_col, s3_col in sf_mappings}
            
            for pg_col, s3_col in pg_mappings:
                sf_col_name = s3_to_sf_lookup.get(s3_col, "❌ No mapping")
                description_parts.append(f"  • {pg_col.name} → {s3_col} → {sf_col_name}")
        
        # Add S3 schema details
        if s3_columns:
            description_parts.append(f"\n📄 S3 SCHEMA ({len(s3_columns)} columns):")
            for col in s3_columns[:5]:  # Show first 5 columns
                sample_info = ""
                if col.get('sample_values'):
                    samples = [str(v) for v in col['sample_values'][:2]]
                    sample_info = f" [e.g., {', '.join(samples)}]"
                description_parts.append(f"  • {col['name']} ({col['type']}){sample_info}")
            
            if len(s3_columns) > 5:
                description_parts.append(f"  • ... and {len(s3_columns) - 5} more columns")
        
        # Add statistics
        mapped_count = len(pg_mappings) if pg_mappings else 0
        total_pg_cols = len(pg_columns) if pg_columns else 0
        total_sf_cols = len(sf_columns) if sf_columns else 0
        
        description_parts.append(f"\n📈 MAPPING STATISTICS:")
        description_parts.append(f"  • PostgreSQL columns: {total_pg_cols}")
        description_parts.append(f"  • S3 columns: {len(s3_columns)}")
        description_parts.append(f"  • Snowflake columns: {total_sf_cols}")
        description_parts.append(f"  • Successfully mapped: {mapped_count}")
        
        if mapped_count > 0 and total_pg_cols > 0:
            match_percentage = (mapped_count / total_pg_cols) * 100
            description_parts.append(f"  • Match rate: {match_percentage:.1f}%")
        
        return "\n".join(description_parts)

    def cleanup_existing_lineage(self, connection_qn: str) -> int:
        """
        Clean up existing lineage processes for this connection to make the script idempotent.
        
        Args:
            connection_qn: Connection qualified name where lineage processes are stored
            
        Returns:
            Number of processes cleaned up
        """
        logger.info(f"Cleaning up existing lineage processes for connection: {connection_qn}")
        
        try:
            # Search for existing Process assets related to this connection
            request = (
                FluentSearch()
                .where(FluentSearch.asset_type(Process))
                .where(FluentSearch.active_assets())
                .where(Process.CONNECTION_QUALIFIED_NAME.eq(connection_qn))
            ).to_request()
            
            existing_processes = list(self.atlan_client.asset.search(request))
            
            if existing_processes:
                logger.info(f"Found {len(existing_processes)} existing lineage processes to clean up")
                
                # Log details of processes to be deleted
                for process in existing_processes:
                    logger.info(f"Will delete process: {process.name} (GUID: {process.guid}, Process ID: {getattr(process, 'process_id', 'N/A')})")
                
                # Delete existing processes
                guids_to_delete = [process.guid for process in existing_processes]
                
                # Delete in batches to avoid API limits
                batch_size = 20
                deleted_count = 0
                
                for i in range(0, len(guids_to_delete), batch_size):
                    batch_guids = guids_to_delete[i:i + batch_size]
                    try:
                        self.atlan_client.asset.delete_by_guid(batch_guids)
                        deleted_count += len(batch_guids)
                        logger.info(f"Deleted batch of {len(batch_guids)} processes")
                    except Exception as e:
                        logger.warning(f"Failed to delete batch of processes: {str(e)}")
                
                logger.info(f"Successfully cleaned up {deleted_count} existing lineage processes")
                return deleted_count
            else:
                logger.info("No existing lineage processes found to clean up")
                return 0
                
        except Exception as e:
            logger.error(f"Error during lineage cleanup: {str(e)}")
            return 0

    def build_lineage(self, s3_assets: List[Dict[str, Any]], connection_qn: str, lineage_batch: list, cleanup_first: bool = True):
        """Builds lineage through S3 objects: PostgreSQL → S3 → Snowflake."""
        logger.info("Building lineage with S3 objects as intermediate nodes...")
        
        # Clean up existing lineage first if requested
        if cleanup_first:
            self.cleanup_existing_lineage(connection_qn)
        
        # Store column processes to create after table processes are saved
        column_processes_to_create = []
        
        # Store the connection qualified name for column processes
        lineage_connection_qn = connection_qn
        
        # Process each S3 asset and create lineage connections through them
        for s3_asset_info in s3_assets:
            s3_asset = s3_asset_info['asset']
            s3_metadata = s3_asset_info['metadata']
            
            # Assumes file name matches table name, e.g., "customers.csv" -> "CUSTOMERS"
            table_name = s3_metadata['key'].split('.')[0].upper()
            s3_columns = s3_metadata.get('schema_info', {}).get('columns', [])
            
            logger.info(f"Processing S3 asset: {s3_asset.name} -> Looking for table: {table_name}")
            logger.info(f"S3 columns found: {len(s3_columns)}")
            
            # Debug: Show available tables if not found
            pg_table = self._get_postgres_table(table_name)
            sf_table = self._get_snowflake_table(table_name)
            
            logger.info(f"PostgreSQL table found: {pg_table.name if pg_table else 'None'}")
            logger.info(f"Snowflake table found: {sf_table.name if sf_table else 'None'}")
            
            # If tables not found, show what's available
            if not pg_table and self.postgres_tables:
                logger.info(f"Available PostgreSQL tables: {[t.name for t in self.postgres_tables[:10]]}")
            if not sf_table and self.snowflake_tables:
                logger.info(f"Available Snowflake tables: {[t.name for t in self.snowflake_tables[:10]]}")
            
            # Create lineage if we have both tables and the S3 object
            logger.info(f"Checking assets for {table_name}:")
            logger.info(f"  pg_table: {pg_table is not None}")
            logger.info(f"  sf_table: {sf_table is not None}")  
            logger.info(f"  s3_asset: {s3_asset is not None}")
            
            if pg_table and sf_table and s3_asset:
                logger.info(f"All required assets found for {table_name} - proceeding with lineage creation")
                
                pg_columns = self._get_table_columns(pg_table, "postgres")
                sf_columns = self._get_table_columns(sf_table, "snowflake")
                
                logger.info(f"PostgreSQL columns: {len(pg_columns)}")
                logger.info(f"Snowflake columns: {len(sf_columns)}")
                
                # Debug: Log table GUIDs and qualified names
                logger.info(f"PostgreSQL table GUID: {pg_table.guid}, QN: {pg_table.qualified_name}")
                logger.info(f"Snowflake table GUID: {sf_table.guid}, QN: {sf_table.qualified_name}")
                logger.info(f"S3 asset GUID: {s3_asset.guid}, QN: {s3_asset.qualified_name}")

                # Create a consolidated process for PostgreSQL -> Snowflake (for business lineage)
                try:
                    logger.info(f"Creating end-to-end process with connection: {connection_qn}")
                    
                    e2e_process = Process.creator(
                        name=f"ETL: {pg_table.name} → {sf_table.name}",
                        connection_qualified_name=connection_qn,
                        inputs=[Table.ref_by_qualified_name(qualified_name=pg_table.qualified_name)],
                        outputs=[Table.ref_by_qualified_name(qualified_name=sf_table.qualified_name)]
                    )
                    
                    # Set additional attributes
                    e2e_process.description = f"End-to-end lineage from {pg_table.name} to {sf_table.name}, intermediated by S3."
                    e2e_process.sql = f"-- ETL process from {pg_table.name} to {sf_table.name} via S3"
                    
                    # Debug: Log process details
                    logger.info(f"Process name: {e2e_process.name}")
                    logger.info(f"Process connection: {e2e_process.connection_qualified_name}")
                    
                    lineage_batch.append(e2e_process)
                    logger.info(f"Created end-to-end process for {table_name}")
                except Exception as e:
                    logger.error(f"Error creating end-to-end process for {table_name}: {str(e)}")
                    logger.error(f"Error type: {type(e).__name__}")
                    continue
                
                # Create PostgreSQL → S3 process
                pg_to_s3_process = Process.creator(
                    name=f"Extract: {pg_table.name} → {s3_asset.name}",
                    connection_qualified_name=connection_qn,
                    inputs=[Table.ref_by_qualified_name(qualified_name=pg_table.qualified_name)],
                    outputs=[S3Object.ref_by_qualified_name(qualified_name=s3_asset.qualified_name)]
                )
                
                pg_to_s3_process.description = f"Extract data from PostgreSQL {pg_table.name} to S3 object {s3_asset.name}"
                pg_to_s3_process.source_url = f"s3://atlan-tech-challenge-sk/{s3_asset.name}"
                pg_to_s3_process.sql = f"""-- Extract from PostgreSQL to S3
SELECT * FROM {pg_table.name};
-- Output to: s3://atlan-tech-challenge-sk/{s3_asset.name}"""
                
                lineage_batch.append(pg_to_s3_process)
                logger.info(f"Created PostgreSQL → S3 process for {table_name}")
                
                # Create S3 → Snowflake process
                s3_to_sf_process = Process.creator(
                    name=f"Load: {s3_asset.name} → {sf_table.name}",
                    connection_qualified_name=connection_qn,
                    inputs=[S3Object.ref_by_qualified_name(qualified_name=s3_asset.qualified_name)],
                    outputs=[Table.ref_by_qualified_name(qualified_name=sf_table.qualified_name)]
                )
                
                s3_to_sf_process.description = f"Load data from S3 object {s3_asset.name} to Snowflake table {sf_table.name}"
                s3_to_sf_process.sql = f"""-- Load from S3 to Snowflake
COPY INTO {sf_table.name} 
FROM @s3_stage/{s3_asset.name}
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);"""
                
                lineage_batch.append(s3_to_sf_process)
                logger.info(f"Created S3 → Snowflake process for {table_name}")
                
                # Prepare column-level lineage if we have S3 schema and columns
                if s3_columns and pg_columns and sf_columns:
                    # Log column details for debugging
                    logger.info(f"S3 columns: {[col['name'] for col in s3_columns]}")
                    logger.info(f"PostgreSQL columns: {[col.name for col in pg_columns]}")
                    logger.info(f"Snowflake columns: {[col.name for col in sf_columns]}")
                    
                    # Create column mappings
                    pg_mappings = self._create_column_lineage_mappings(s3_columns, pg_columns)
                    sf_mappings = self._create_column_lineage_mappings(s3_columns, sf_columns)
                    
                    logger.info(f"PostgreSQL mappings: {len(pg_mappings)} - {[(pg_col.name, s3_col) for pg_col, s3_col in pg_mappings]}")
                    logger.info(f"Snowflake mappings: {len(sf_mappings)} - {[(sf_col.name, s3_col) for sf_col, s3_col in sf_mappings]}")
                    
                    if pg_mappings and sf_mappings:
                        # Create lookup for S3 column to Snowflake column
                        s3_to_sf_lookup = {s3_col: sf_col for sf_col, s3_col in sf_mappings}
                        
                        # Prepare column lineage processes (to be created after table process is saved)
                        for pg_column, s3_col_name in pg_mappings:
                            if s3_col_name in s3_to_sf_lookup:
                                sf_column = s3_to_sf_lookup[s3_col_name]
                                
                                logger.info(f"Preparing column lineage: {pg_column.name} → {sf_column.name}")
                                
                                # Store column process info for later creation
                                column_processes_to_create.append({
                                    'table_process_name': e2e_process.name,  # Use the e2e process name for lookup
                                    'pg_column': pg_column,
                                    'sf_column': sf_column,
                                    's3_col_name': s3_col_name,
                                    'table_name': table_name,
                                    's3_asset': s3_asset,
                                    'pg_table': pg_table,
                                    'sf_table': sf_table,
                                    'connection_qn': lineage_connection_qn
                                })
                            else:
                                logger.warning(f"No Snowflake mapping found for S3 column: {s3_col_name}")
                        
                        logger.info(f"Prepared {len([p for p in column_processes_to_create if p['table_name'] == table_name])} column lineage processes for {table_name}")
                    else:
                        logger.warning(f"No column mappings found for {table_name}")
                        logger.warning(f"PG mappings empty: {len(pg_mappings) == 0}")
                        logger.warning(f"SF mappings empty: {len(sf_mappings) == 0}")
                
            else:
                logger.warning(f"Missing required assets for {table_name} - skipping lineage")
                logger.warning(f"  PostgreSQL table: {'Found' if pg_table else 'NOT FOUND'}")
                logger.warning(f"  Snowflake table: {'Found' if sf_table else 'NOT FOUND'}")
                logger.warning(f"  S3 asset: {'Found' if s3_asset else 'NOT FOUND'}")
                
                if not pg_table:
                    logger.warning(f"  Available PostgreSQL tables: {[t.name for t in self.postgres_tables[:5]]}")
                if not sf_table:
                    logger.warning(f"  Available Snowflake tables: {[t.name for t in self.snowflake_tables[:5]]}")

            # Log summary of lineage created for this S3 asset
            logger.info(f"Completed lineage preparation for S3 asset: {s3_asset.name} (table: {table_name})")
        
        # Return column process info for second phase creation
        logger.info(f"Created {len(lineage_batch)} table-level lineage processes")
        logger.info(f"Prepared {len(column_processes_to_create)} column-level processes for second phase creation")
        
        return column_processes_to_create

    def create_column_lineage_processes(self, column_processes_info: List[Dict], created_table_processes: List[Process]) -> List[ColumnProcess]:
        """
        Create column lineage processes using the saved table processes as parents.
        S3 objects are referenced in descriptions only, not as lineage connections.
        
        Args:
            column_processes_info: List of column process information from build_lineage
            created_table_processes: List of saved table processes with GUIDs
            
        Returns:
            List of ColumnProcess objects ready to be saved
        """
        logger.info("Creating column lineage processes with parent table processes...")
        
        # Create a lookup for table processes by their name
        table_process_lookup = {p.name: p for p in created_table_processes if hasattr(p, 'name')}
        
        column_lineage_batch = []
        
        for col_info in column_processes_info:
            table_process_name = col_info['table_process_name']
            
            # Find the corresponding saved table process
            if table_process_name in table_process_lookup:
                parent_process = table_process_lookup[table_process_name]
                
                logger.info(f"Creating column lineage: {col_info['pg_column'].name} → {col_info['sf_column'].name} (parent: {parent_process.guid})")
                
                # Create column-level lineage process with parent table process
                column_process = ColumnProcess.creator(
                    name=f"{col_info['pg_column'].name} → {col_info['sf_column'].name}",
                    connection_qualified_name=col_info['connection_qn'],
                    inputs=[Column.ref_by_qualified_name(qualified_name=col_info['pg_column'].qualified_name)],
                    outputs=[Column.ref_by_qualified_name(qualified_name=col_info['sf_column'].qualified_name)],
                    parent=Process.ref_by_guid(guid=parent_process.guid)
                )
                
                # Add description and SQL - S3 is mentioned only in description
                column_process.description = f"Direct column lineage: {col_info['pg_table'].name}.{col_info['pg_column'].name} → {col_info['sf_table'].name}.{col_info['sf_column'].name} (via S3 {col_info['s3_asset'].name})"
                column_process.sql = f"-- Column mapping: {col_info['pg_column'].name} -> {col_info['sf_column'].name} via S3 column {col_info['s3_col_name']}"
                
                column_lineage_batch.append(column_process)
            else:
                logger.warning(f"Could not find parent table process with name: {table_process_name}")
        
        logger.info(f"Created {len(column_lineage_batch)} column lineage processes")
        return column_lineage_batch

