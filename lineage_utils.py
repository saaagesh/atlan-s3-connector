# lineage_utils.py
"""
Utility functions for lineage analysis and debugging
"""

import logging
from typing import Dict, List, Any, Optional
from atlan_client import get_atlan_client
from pyatlan.model.assets import Table, Column, Process
from pyatlan.model.fluent_search import FluentSearch

logger = logging.getLogger(__name__)

class LineageAnalyzer:
    """Utility class for analyzing and debugging lineage"""
    
    def __init__(self):
        self.atlan_client = get_atlan_client()
    
    def analyze_table_columns(self, connection_name: str, table_name: str) -> Dict[str, Any]:
        """
        Analyze columns for a specific table
        
        Args:
            connection_name: Name of the connection
            table_name: Name of the table
            
        Returns:
            Dictionary with table and column information
        """
        logger.info(f"Analyzing columns for table: {table_name} in connection: {connection_name}")
        
        try:
            # Find the table
            table_request = (
                FluentSearch()
                .where(FluentSearch.asset_type(Table))
                .where(FluentSearch.active_assets())
                .where(Table.CONNECTION_NAME.eq(connection_name))
                .where(Table.NAME.eq(table_name))
            ).to_request()
            
            tables = list(self.atlan_client.asset.search(table_request))
            
            if not tables:
                logger.warning(f"Table {table_name} not found in connection {connection_name}")
                return {"error": "Table not found"}
            
            table = tables[0]
            
            # Find columns for the table
            column_request = (
                FluentSearch()
                .where(FluentSearch.asset_type(Column))
                .where(FluentSearch.active_assets())
                .where(Column.TABLE_QUALIFIED_NAME.eq(table.qualified_name))
            ).to_request()
            
            columns = list(self.atlan_client.asset.search(column_request))
            
            result = {
                "table_name": table.name,
                "table_qualified_name": table.qualified_name,
                "table_guid": table.guid,
                "column_count": len(columns),
                "columns": []
            }
            
            for column in columns:
                col_info = {
                    "name": column.name,
                    "qualified_name": column.qualified_name,
                    "guid": column.guid,
                    "data_type": getattr(column, 'data_type', 'Unknown'),
                    "order": getattr(column, 'order', 0)
                }
                result["columns"].append(col_info)
            
            # Sort columns by order
            result["columns"].sort(key=lambda x: x["order"])
            
            logger.info(f"Found {len(columns)} columns for table {table_name}")
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing table columns: {str(e)}")
            return {"error": str(e)}
    
    def compare_table_schemas(self, pg_connection: str, sf_connection: str, table_name: str) -> Dict[str, Any]:
        """
        Compare schemas between Postgres and Snowflake tables
        
        Args:
            pg_connection: Postgres connection name
            sf_connection: Snowflake connection name
            table_name: Table name to compare
            
        Returns:
            Comparison results
        """
        logger.info(f"Comparing schemas for table: {table_name}")
        
        pg_analysis = self.analyze_table_columns(pg_connection, table_name)
        sf_analysis = self.analyze_table_columns(sf_connection, table_name)
        
        if "error" in pg_analysis or "error" in sf_analysis:
            return {
                "error": "Could not analyze one or both tables",
                "postgres_error": pg_analysis.get("error"),
                "snowflake_error": sf_analysis.get("error")
            }
        
        # Compare columns
        pg_columns = {col["name"].lower(): col for col in pg_analysis["columns"]}
        sf_columns = {col["name"].lower(): col for col in sf_analysis["columns"]}
        
        matching_columns = []
        pg_only_columns = []
        sf_only_columns = []
        
        # Find matching columns
        for col_name in pg_columns:
            if col_name in sf_columns:
                matching_columns.append({
                    "column_name": col_name,
                    "postgres": pg_columns[col_name],
                    "snowflake": sf_columns[col_name]
                })
            else:
                pg_only_columns.append(pg_columns[col_name])
        
        # Find Snowflake-only columns
        for col_name in sf_columns:
            if col_name not in pg_columns:
                sf_only_columns.append(sf_columns[col_name])
        
        result = {
            "table_name": table_name,
            "postgres_table": pg_analysis,
            "snowflake_table": sf_analysis,
            "matching_columns": matching_columns,
            "postgres_only_columns": pg_only_columns,
            "snowflake_only_columns": sf_only_columns,
            "match_percentage": len(matching_columns) / max(len(pg_columns), len(sf_columns)) * 100
        }
        
        logger.info(f"Schema comparison complete: {len(matching_columns)} matching columns, {result['match_percentage']:.1f}% match")
        return result
    
    def analyze_s3_to_db_mapping(self, s3_columns: List[Dict], db_columns: List[Dict]) -> Dict[str, Any]:
        """
        Analyze potential mappings between S3 CSV columns and database columns
        
        Args:
            s3_columns: List of S3 column information
            db_columns: List of database column information
            
        Returns:
            Mapping analysis results
        """
        logger.info("Analyzing S3 to database column mappings")
        
        # Create case-insensitive lookups
        s3_lookup = {col['name'].lower(): col for col in s3_columns}
        db_lookup = {col['name'].lower(): col for col in db_columns}
        
        exact_matches = []
        potential_matches = []
        s3_only = []
        db_only = []
        
        # Find exact matches
        for s3_col_name in s3_lookup:
            if s3_col_name in db_lookup:
                exact_matches.append({
                    "s3_column": s3_lookup[s3_col_name],
                    "db_column": db_lookup[s3_col_name],
                    "match_type": "exact"
                })
            else:
                # Look for potential matches (similar names)
                potential_match = self._find_similar_column_name(s3_col_name, list(db_lookup.keys()))
                if potential_match:
                    potential_matches.append({
                        "s3_column": s3_lookup[s3_col_name],
                        "db_column": db_lookup[potential_match],
                        "match_type": "similar",
                        "similarity_reason": f"Similar to {potential_match}"
                    })
                else:
                    s3_only.append(s3_lookup[s3_col_name])
        
        # Find database-only columns
        for db_col_name in db_lookup:
            if db_col_name not in s3_lookup:
                # Check if it was already matched as a potential match
                already_matched = any(
                    match["db_column"]["name"].lower() == db_col_name 
                    for match in potential_matches
                )
                if not already_matched:
                    db_only.append(db_lookup[db_col_name])
        
        result = {
            "total_s3_columns": len(s3_columns),
            "total_db_columns": len(db_columns),
            "exact_matches": exact_matches,
            "potential_matches": potential_matches,
            "s3_only_columns": s3_only,
            "db_only_columns": db_only,
            "match_percentage": len(exact_matches) / max(len(s3_columns), len(db_columns)) * 100
        }
        
        logger.info(f"Mapping analysis complete: {len(exact_matches)} exact matches, {len(potential_matches)} potential matches")
        return result
    
    def _find_similar_column_name(self, target: str, candidates: List[str]) -> Optional[str]:
        """Find similar column names using simple heuristics"""
        target = target.lower()
        
        # Look for partial matches
        for candidate in candidates:
            candidate_lower = candidate.lower()
            
            # Check if one contains the other
            if target in candidate_lower or candidate_lower in target:
                return candidate
            
            # Check for common variations
            target_clean = target.replace('_', '').replace('-', '')
            candidate_clean = candidate_lower.replace('_', '').replace('-', '')
            
            if target_clean == candidate_clean:
                return candidate
        
        return None
    
    def generate_lineage_report(self, connection_names: Dict[str, str], table_names: List[str]) -> Dict[str, Any]:
        """
        Generate a comprehensive lineage report
        
        Args:
            connection_names: Dict with 'postgres' and 'snowflake' connection names
            table_names: List of table names to analyze
            
        Returns:
            Comprehensive lineage report
        """
        logger.info("Generating comprehensive lineage report")
        
        report = {
            "timestamp": str(logger.info),
            "connections": connection_names,
            "tables_analyzed": len(table_names),
            "table_reports": []
        }
        
        for table_name in table_names:
            logger.info(f"Analyzing table: {table_name}")
            
            table_report = {
                "table_name": table_name,
                "schema_comparison": self.compare_table_schemas(
                    connection_names["postgres"],
                    connection_names["snowflake"],
                    table_name
                )
            }
            
            report["table_reports"].append(table_report)
        
        logger.info("Lineage report generation complete")
        return report

def print_column_mapping_summary(mapping_analysis: Dict[str, Any]):
    """Print a formatted summary of column mapping analysis"""
    
    print("\n" + "="*60)
    print("COLUMN MAPPING ANALYSIS SUMMARY")
    print("="*60)
    
    print(f"S3 Columns: {mapping_analysis['total_s3_columns']}")
    print(f"DB Columns: {mapping_analysis['total_db_columns']}")
    print(f"Match Percentage: {mapping_analysis['match_percentage']:.1f}%")
    
    print(f"\nExact Matches ({len(mapping_analysis['exact_matches'])}):")
    for match in mapping_analysis['exact_matches']:
        s3_col = match['s3_column']
        db_col = match['db_column']
        print(f"  ✓ {s3_col['name']} ({s3_col.get('type', 'unknown')}) -> {db_col['name']}")
    
    if mapping_analysis['potential_matches']:
        print(f"\nPotential Matches ({len(mapping_analysis['potential_matches'])}):")
        for match in mapping_analysis['potential_matches']:
            s3_col = match['s3_column']
            db_col = match['db_column']
            print(f"  ? {s3_col['name']} -> {db_col['name']} ({match['similarity_reason']})")
    
    if mapping_analysis['s3_only_columns']:
        print(f"\nS3-Only Columns ({len(mapping_analysis['s3_only_columns'])}):")
        for col in mapping_analysis['s3_only_columns']:
            print(f"  - {col['name']} ({col.get('type', 'unknown')})")
    
    if mapping_analysis['db_only_columns']:
        print(f"\nDB-Only Columns ({len(mapping_analysis['db_only_columns'])}):")
        for col in mapping_analysis['db_only_columns']:
            print(f"  - {col['name']}")
    
    print("="*60)

if __name__ == "__main__":
    # Example usage
    analyzer = LineageAnalyzer()
    
    # Example: Analyze a specific table
    # result = analyzer.analyze_table_columns("your-postgres-connection", "CUSTOMERS")
    # print(f"Found {result.get('column_count', 0)} columns")