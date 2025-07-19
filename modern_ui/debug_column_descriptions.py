#!/usr/bin/env python3
"""
Debug script to check column descriptions for a specific table
"""

import os
import sys
import asyncio
from dotenv import load_dotenv

# Add the current directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)

from atlan_client import get_atlan_client
from pyatlan.model.assets import Column
from pyatlan.model.fluent_search import FluentSearch

async def debug_column_descriptions(table_qualified_name):
    """Debug column descriptions for a specific table"""
    print(f"Debugging column descriptions for table: {table_qualified_name}")
    
    atlan_client = get_atlan_client()
    
    # Search for columns of the table
    search_request = (
        FluentSearch()
        .where(FluentSearch.asset_type(Column))
        .where(Column.TABLE_QUALIFIED_NAME.eq(table_qualified_name))
    ).to_request()
    
    results = list(atlan_client.asset.search(search_request))
    print(f"Found {len(results)} columns for table {table_qualified_name}")
    
    # Print details for each column
    for col in results:
        if isinstance(col, Column):
            print(f"\nColumn: {col.name}")
            print(f"  Qualified Name: {col.qualified_name}")
            print(f"  GUID: {col.guid}")
            print(f"  Description: '{col.description or 'None'}'")
            print(f"  User Description: '{col.user_description or 'None'}'")
            print(f"  Data Type: {getattr(col, 'data_type', 'Unknown')}")
            
            # Check if the column has a description
            has_description = bool(col.user_description or col.description)
            print(f"  Has Description: {has_description}")
            
            # Print all attributes for debugging
            print("  All attributes:")
            for attr_name in dir(col):
                if not attr_name.startswith('_') and not callable(getattr(col, attr_name)):
                    try:
                        attr_value = getattr(col, attr_name)
                        if attr_value is not None and attr_value != "":
                            print(f"    {attr_name}: {attr_value}")
                    except:
                        pass

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python debug_column_descriptions.py <table_qualified_name>")
        sys.exit(1)
    
    table_qualified_name = sys.argv[1]
    asyncio.run(debug_column_descriptions(table_qualified_name))