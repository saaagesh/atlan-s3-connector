#!/usr/bin/env python3
"""
Debug script to specifically check Snowflake column descriptions
"""

import os
import sys
import asyncio
import json
from dotenv import load_dotenv

# Add the current directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)

from atlan_client import get_atlan_client
from pyatlan.model.assets import Column, Table
from pyatlan.model.fluent_search import FluentSearch
from config import ConnectionConfig

async def debug_snowflake_columns(table_name=None):
    """Debug column descriptions for Snowflake tables"""
    print(f"Debugging Snowflake column descriptions")
    
    atlan_client = get_atlan_client()
    connection_config = ConnectionConfig()
    
    # Get the Snowflake connection name
    snowflake_connection = connection_config.snowflake_connection_name
    print(f"Using Snowflake connection: {snowflake_connection}")
    
    # Find tables in the Snowflake connection
    table_request = (
        FluentSearch()
        .where(FluentSearch.asset_type(Table))
        .where(FluentSearch.active_assets())
        .where(Table.CONNECTION_NAME.eq(snowflake_connection))
    )
    
    if table_name:
        table_request = table_request.where(Table.NAME.eq(table_name))
    
    table_results = list(atlan_client.asset.search(table_request.to_request()))
    print(f"Found {len(table_results)} Snowflake tables")
    
    for table in table_results:
        print(f"\n{'='*50}")
        print(f"TABLE: {table.name}")
        print(f"Qualified Name: {table.qualified_name}")
        print(f"GUID: {table.guid}")
        print(f"{'='*50}")
        
        # Search for columns of the table
        search_request = (
            FluentSearch()
            .where(FluentSearch.asset_type(Column))
            .where(Column.TABLE_QUALIFIED_NAME.eq(table.qualified_name))
        ).to_request()
        
        column_results = list(atlan_client.asset.search(search_request))
        print(f"Found {len(column_results)} columns for table {table.name}")
        
        # Print details for each column
        for col in column_results:
            if isinstance(col, Column):
                print(f"\nColumn: {col.name}")
                print(f"  Data Type: {getattr(col, 'data_type', 'Unknown')}")
                print(f"  Description: '{col.description or 'None'}'")
                print(f"  User Description: '{col.user_description or 'None'}'")
                
                # Check if the column has a description
                has_description = bool(col.user_description or col.description)
                print(f"  Has Description: {has_description}")
                
                # Create a JSON representation for API testing
                col_json = {
                    "name": col.name,
                    "qualified_name": col.qualified_name,
                    "guid": col.guid,
                    "description": col.description or "",
                    "user_description": col.user_description or "",
                    "data_type": getattr(col, 'data_type', ''),
                    "has_description": has_description
                }
                print(f"  JSON: {json.dumps(col_json)}")

if __name__ == "__main__":
    table_name = sys.argv[1] if len(sys.argv) > 1 else "CUSTOMERS"
    asyncio.run(debug_snowflake_columns(table_name))