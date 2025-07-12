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
            lineage_request = LineageRequest(
                guid=asset.guid,
                direction=LineageDirection.BOTH,
                depth=1
            )

            lineage_response = self.atlan_client.lineage.get_lineage(lineage_request)

            # Extract column information from lineage
            if hasattr(asset, 'columns') and asset.columns:
                for column in asset.columns:
                    columns.append({
                        'name': column.name,
                        'qualified_name': column.qualified_name,
                        'guid': column.guid,
                        'data_type': getattr(column, 'data_type', 'unknown')
                    })

        except Exception as e:
            logger.warning(f"Could not get columns for asset {asset.name}: {str(e)}")
            # Fallback: try to get columns from asset attributes
            if hasattr(asset, 'attributes') and asset.attributes:
                for attr_name, attr_value in asset.attributes.items():
                    if 'column' in attr_name.lower():
                        columns.append({
                            'name': attr_name,
                            'qualified_name': f"{asset.qualified_name}/{attr_name}",
                            'guid': None,
                            'data_type': 'unknown'
                        })

        return columns

    async def _create_upstream_relationship(self, postgres_table: Dict[str, Any], s3_asset: Dict[str, Any]) -> Dict[str, Any]:
        """Create upstream lineage relationship"""

        relationship = {
            'type': 'upstream',
            'source': {
                'qualified_name': postgres_table['qualified_name'],
                'name': postgres_table['name'],
                'guid': postgres_table['guid'],
                'asset_type': 'Table'
            },
            'target': {
                'qualified_name': s3_asset['qualified_name'],
                'name': s3_asset['metadata']['key'],
                'guid': s3_asset.get('guid'),
                'asset_type': 'S3Object'
            },
            'relationship_type': 'DataFlow',
            'confidence_score': self._calculate_relationship_confidence(postgres_table, s3_asset),
            'created_at': datetime.now().isoformat(),
            'metadata': {
                'process_type': 'ETL_EXTRACT',
                'description': f"Data extracted from Postgres table {postgres_table['name']} to S3 object {s3_asset['metadata']['key']}"
            }
        }

        # Create the actual lineage in Atlan
        try:
            await self._create_atlan_lineage(relationship)
        except Exception as e:
            logger.error(f"Failed to create Atlan lineage: {str(e)}")

        return relationship

    async def _create_downstream_relationship(self, s3_asset: Dict[str, Any], snowflake_table: Dict[str, Any]) -> Dict[str, Any]:
        """Create downstream lineage relationship"""

        relationship = {
            'type': 'downstream',
            'source': {
                'qualified_name': s3_asset['qualified_name'],
                'name': s3_asset['metadata']['key'],
                'guid': s3_asset.get('guid'),
                'asset_type': 'S3Object'
            },
            'target': {
                'qualified_name': snowflake_table['qualified_name'],
                'name': snowflake_table['name'],
                'guid': snowflake_table['guid'],
                'asset_type': 'Table'
            },
            'relationship_type': 'DataFlow',
            'confidence_score': self._calculate_relationship_confidence(s3_asset, snowflake_table),
            'created_at': datetime.now().isoformat(),
            'metadata': {
                'process_type': 'ETL_LOAD',
                'description': f"Data loaded from S3 object {s3_asset['metadata']['key']} to Snowflake table {snowflake_table['name']}"
            }
        }

        # Create the actual lineage in Atlan
        try:
            await self._create_atlan_lineage(relationship)
        except Exception as e:
            logger.error(f"Failed to create Atlan lineage: {str(e)}")

        return relationship

    async def _build_column_lineage(self, source: Dict[str, Any], target: Dict[str, Any], direction: str) -> List[Dict[str, Any]]:
        """Build column-level lineage between source and target"""

        column_relationships = []

        source_columns = source.get('columns', [])

        # For S3 assets, get columns from schema_info
        if 'metadata' in target and 'schema_info' in target['metadata']:
            target_columns = target['metadata']['schema_info'].get('columns', [])
        else:
            target_columns = target.get('columns', [])

        # Map columns based on name similarity and business logic
        for source_col in source_columns:
            best_match = self._find_best_column_match(source_col, target_columns)

            if best_match:
                column_relationship = {
                    'source_column': {
                        'name': source_col['name'],
                        'qualified_name': source_col.get('qualified_name'),
                        'data_type': source_col.get('data_type', 'unknown')
                    },
                    'target_column': {
                        'name': best_match['name'],
                        'qualified_name': best_match.get('qualified_name'),
                        'data_type': best_match.get('type', 'unknown')
                    },
                    'transformation_type': self._determine_transformation_type(source_col, best_match),
                    'confidence_score': self._calculate_column_confidence(source_col, best_match),
                    'direction': direction
                }

                column_relationships.append(column_relationship)

        return column_relationships

    async def _build_end_to_end_column_lineage(self, postgres_table: Dict[str, Any], s3_asset: Dict[str, Any], snowflake_table: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Build end-to-end column lineage across the entire pipeline"""

        end_to_end_lineage = []

        postgres_columns = postgres_table.get('columns', [])
        s3_columns = s3_asset['metadata']['schema_info'].get('columns', [])
        snowflake_columns = snowflake_table.get('columns', [])

        # Map columns across all three systems
        for pg_col in postgres_columns:
            # Find matching S3 column
            s3_match = self._find_best_column_match(pg_col, s3_columns)

            if s3_match:
                # Find matching Snowflake column
                sf_match = self._find_best_column_match(s3_match, snowflake_columns)

                if sf_match:
                    lineage_chain = {
                        'postgres_column': {
                            'name': pg_col['name'],
                            'qualified_name': pg_col.get('qualified_name'),
                            'data_type': pg_col.get('data_type', 'unknown')
                        },
                        's3_column': {
                            'name': s3_match['name'],
                            'data_type': s3_match.get('type', 'unknown')
                        },
                        'snowflake_column': {
                            'name': sf_match['name'],
                            'qualified_name': sf_match.get('qualified_name'),
                            'data_type': sf_match.get('data_type', 'unknown')
                        },
                        'pipeline_stage': 'end_to_end',
                        'transformations': [
                            'postgres_to_s3_extract',
                            's3_to_snowflake_load'
                        ],
                        'confidence_score': min(
                            self._calculate_column_confidence(pg_col, s3_match),
                            self._calculate_column_confidence(s3_match, sf_match)
                        )
                    }

                    end_to_end_lineage.append(lineage_chain)

        return end_to_end_lineage

    def _find_best_column_match(self, source_column: Dict[str, Any], target_columns: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find the best matching column in target based on name and type"""

        source_name = source_column['name'].lower()
        best_match = None
        best_score = 0.0

        for target_col in target_columns:
            target_name = target_col['name'].lower()

            # Exact match
            if source_name == target_name:
                return target_col

            # Calculate similarity score
            score = self._calculate_name_similarity(source_name, target_name)

            # Boost score for business logic matches
            if self._is_business_logic_match(source_name, target_name):
                score += 0.2

            if score > best_score and score > 0.6:  # Minimum threshold
                best_score = score
                best_match = target_col

        return best_match

    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two column names"""

        # Simple similarity based on common substrings
        if name1 == name2:
            return 1.0

        # Check for common patterns
        common_patterns = ['id', 'name', 'date', 'time', 'code', 'number']

        for pattern in common_patterns:
            if pattern in name1 and pattern in name2:
                return 0.8

        # Check for partial matches
        if name1 in name2 or name2 in name1:
            return 0.7

        # Check for similar prefixes/suffixes
        if name1[:3] == name2[:3] or name1[-3:] == name2[-3:]:
            return 0.6

        return 0.0

    def _is_business_logic_match(self, name1: str, name2: str) -> bool:
        """Check if columns match based on business logic"""

        # Define business logic mappings
        business_mappings = {
            'customer_id': ['customerid', 'cust_id', 'customer_number'],
            'order_id': ['orderid', 'order_number', 'order_no'],
            'product_id': ['productid', 'prod_id', 'product_number'],
            'employee_id': ['employeeid', 'emp_id', 'employee_number']
        }

        for canonical, variants in business_mappings.items():
            if (name1 == canonical and name2 in variants) or (name2 == canonical and name1 in variants):
                return True
            if name1 in variants and name2 in variants:
                return True

        return False

    def _determine_transformation_type(self, source_col: Dict[str, Any], target_col: Dict[str, Any]) -> str:
        """Determine the type of transformation between columns"""

        source_type = source_col.get('data_type', '').lower()
        target_type = target_col.get('type', target_col.get('data_type', '')).lower()

        if source_type == target_type:
            return 'direct_copy'
        elif 'int' in source_type and 'string' in target_type:
            return 'type_conversion_int_to_string'
        elif 'date' in source_type and 'string' in target_type:
            return 'type_conversion_date_to_string'
        elif source_type and target_type:
            return f'type_conversion_{source_type}_to_{target_type}'
        else:
            return 'unknown_transformation'

    def _calculate_relationship_confidence(self, source: Dict[str, Any], target: Dict[str, Any]) -> float:
        """Calculate confidence score for relationship"""

        # Base confidence
        confidence = 0.7

        # Boost for exact name matches
        source_name = source.get('name', '').replace('.csv', '').upper()
        target_name = target.get('name', target.get('metadata', {}).get('key', '')).replace('.csv', '').upper()

        if source_name == target_name:
            confidence += 0.2

        # Boost for schema similarity
        source_columns = len(source.get('columns', []))
        target_columns = len(target.get('columns', target.get('metadata', {}).get('schema_info', {}).get('columns', [])))

        if source_columns > 0 and target_columns > 0:
            column_ratio = min(source_columns, target_columns) / max(source_columns, target_columns)
            confidence += column_ratio * 0.1

        return min(1.0, confidence)

    def _calculate_column_confidence(self, source_col: Dict[str, Any], target_col: Dict[str, Any]) -> float:
        """Calculate confidence score for column mapping"""

        source_name = source_col['name'].lower()
        target_name = target_col['name'].lower()

        if source_name == target_name:
            return 1.0
        elif self._is_business_logic_match(source_name, target_name):
            return 0.9
        else:
            return self._calculate_name_similarity(source_name, target_name)

    async def _create_atlan_lineage(self, relationship: Dict[str, Any]) -> None:
        """Create lineage relationship in Atlan"""

        try:
            # This would use the Atlan SDK to create actual lineage
            # For now, we'll log the relationship creation
            logger.info(f"Creating lineage: {relationship['source']['name']} → {relationship['target']['name']}")

            # In a real implementation, you would use:
            # lineage_request = LineageRequest(...)
            # self.atlan_client.lineage.create_lineage(lineage_request)

        except Exception as e:
            logger.error(f"Failed to create Atlan lineage: {str(e)}")
            raise

    async def validate_lineage_relationships(self, relationships: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate created lineage relationships"""

        validation_results = {
            'total_relationships': len(relationships),
            'high_confidence': 0,
            'medium_confidence': 0,
            'low_confidence': 0,
            'validation_errors': []
        }

        for relationship in relationships:
            confidence = relationship.get('confidence_score', 0.0)

            if confidence >= 0.8:
                validation_results['high_confidence'] += 1
            elif confidence >= 0.6:
                validation_results['medium_confidence'] += 1
            else:
                validation_results['low_confidence'] += 1

            # Validate relationship structure
            if not relationship.get('source') or not relationship.get('target'):
                validation_results['validation_errors'].append(
                    f"Missing source or target in relationship: {relationship.get('type', 'unknown')}"
                )

        return validation_results

    async def get_lineage_impact_analysis(self, asset_qualified_name: str) -> Dict[str, Any]:
        """Get impact analysis for a specific asset"""

        try:
            # Get upstream and downstream lineage
            lineage_request = LineageRequest(
                qualified_name=asset_qualified_name,
                direction=LineageDirection.BOTH,
                depth=3
            )

            lineage_response = self.atlan_client.lineage.get_lineage(lineage_request)

            impact_analysis = {
                'asset': asset_qualified_name,
                'upstream_count': len(lineage_response.upstream_assets) if hasattr(lineage_response, 'upstream_assets') else 0,
                'downstream_count': len(lineage_response.downstream_assets) if hasattr(lineage_response, 'downstream_assets') else 0,
                'total_impact': 0,
                'critical_dependencies': []
            }

            impact_analysis['total_impact'] = impact_analysis['upstream_count'] + impact_analysis['downstream_count']

            return impact_analysis

        except Exception as e:
            logger.error(f"Failed to get impact analysis for {asset_qualified_name}: {str(e)}")
            return {
                'asset': asset_qualified_name,
                'error': str(e),
                'upstream_count': 0,
                'downstream_count': 0,
                'total_impact': 0
            }
