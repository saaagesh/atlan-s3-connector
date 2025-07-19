#!/usr/bin/env python3
"""
Script to fix column descriptions for any table in Atlan
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
from flask_ai_enhancer import AIEnhancer
from config import AIConfig

async def fix_table_descriptions(connection_type, table_name):
    """Fix column descriptions for any table in Atlan"""
    print(f"Fixing column descriptions for {connection_type} table: {table_name}")
    
    atlan_client = get_atlan_client()
    connection_config = ConnectionConfig()
    ai_config = AIConfig()
    
    # Get the connection name based on the connection type
    connection_name = None
    if connection_type.lower() == "snowflake":
        connection_name = connection_config.snowflake_connection_name
    elif connection_type.lower() == "postgres":
        connection_name = connection_config.postgres_connection_name
    else:
        print(f"Unsupported connection type: {connection_type}")
        return
    
    print(f"Using connection: {connection_name}")
    
    # Find the table in the connection
    table_request = (
        FluentSearch()
        .where(FluentSearch.asset_type(Table))
        .where(FluentSearch.active_assets())
        .where(Table.CONNECTION_NAME.eq(connection_name))
        .where(Table.NAME.eq(table_name))
    ).to_request()
    
    table_results = list(atlan_client.asset.search(table_request))
    
    if not table_results:
        print(f"Table {table_name} not found in {connection_type} connection.")
        return
    
    table = table_results[0]
    print(f"Found table: {table.name} ({table.qualified_name})")
    
    # Search for columns of the table
    search_request = (
        FluentSearch()
        .where(FluentSearch.asset_type(Column))
        .where(Column.TABLE_QUALIFIED_NAME.eq(table.qualified_name))
    ).to_request()
    
    column_results = list(atlan_client.asset.search(search_request))
    print(f"Found {len(column_results)} columns for table {table.name}")
    
    # Check if we have predefined descriptions for this table
    column_descriptions = {}
    
    # Predefined descriptions for known tables
    if connection_type.lower() == "snowflake":
        if table_name == "CUSTOMERS":
            column_descriptions = {
                "ADDRESS": "Customer's street address.",
                "CITY": "Customer's city of residence.",
                "CONTACTNAME": "Name of the customer contact person.",
                "COUNTRY": "NEW COUNTRY",
                "CUSTOMERID": "Unique identifier for each customer.",
                "CUSTOMERNAME": "Customer's company or individual name.",
                "POSTALCODE": "Customer's postal code."
            }
        elif table_name == "EMPLOYEES":
            column_descriptions = {
                "EMPLOYEEID": "Unique identifier for each employee.",
                "LASTNAME": "Employee's last name.",
                "FIRSTNAME": "Employee's first name.",
                "TITLE": "Employee's job title.",
                "TITLEOFCOURTESY": "Formal title used to address the employee.",
                "BIRTHDATE": "Employee's date of birth.",
                "HIREDATE": "Date when the employee was hired.",
                "ADDRESS": "Employee's street address.",
                "CITY": "City where the employee resides.",
                "REGION": "Region or state where the employee resides.",
                "POSTALCODE": "Employee's postal code.",
                "COUNTRY": "Country where the employee resides.",
                "HOMEPHONE": "Employee's home telephone number.",
                "EXTENSION": "Employee's office extension number.",
                "PHOTO": "Employee's photograph.",
                "NOTES": "Additional notes about the employee.",
                "REPORTSTO": "ID of the employee's manager."
            }
        elif table_name == "ORDERS":
            column_descriptions = {
                "ORDERID": "Unique identifier for each order.",
                "CUSTOMERID": "Identifier of the customer who placed the order.",
                "EMPLOYEEID": "Identifier of the employee who processed the order.",
                "ORDERDATE": "Date when the order was placed.",
                "REQUIREDDATE": "Date when the order is required to be delivered.",
                "SHIPPEDDATE": "Date when the order was shipped.",
                "SHIPVIA": "Shipping method identifier.",
                "FREIGHT": "Shipping cost.",
                "SHIPNAME": "Name of the recipient.",
                "SHIPADDRESS": "Shipping address.",
                "SHIPCITY": "City for shipping.",
                "SHIPREGION": "Region or state for shipping.",
                "SHIPPOSTALCODE": "Postal code for shipping.",
                "SHIPCOUNTRY": "Country for shipping."
            }
    
    # If no predefined descriptions, generate them using AI
    if not column_descriptions:
        print(f"No predefined descriptions for {table_name}, generating using AI...")
        try:
            ai_enhancer = AIEnhancer(ai_config)
            
            # Prepare column data for AI
            columns = []
            for col in column_results:
                if isinstance(col, Column):
                    columns.append({
                        "name": col.name,
                        "type": getattr(col, 'data_type', '')
                    })
            
            # Create a payload for AI description generation
            ai_payload = {
                'asset_qualified_name': table.qualified_name,
                'asset_name': table_name,
                'columns': columns
            }
            
            # Generate column descriptions
            ai_descriptions = await ai_enhancer.generate_column_level_descriptions(ai_payload)
            
            # Convert to dictionary format
            for desc in ai_descriptions:
                column_descriptions[desc["name"]] = desc["description"]
            
            print(f"Successfully generated {len(column_descriptions)} descriptions using AI")
        except Exception as e:
            print(f"Error generating AI descriptions: {e}")
    
    # Update columns with descriptions
    batch = []
    updated_columns = []
    
    for col in column_results:
        if isinstance(col, Column) and col.name in column_descriptions:
            description = column_descriptions[col.name]
            print(f"Updating column {col.name} with description: '{description}'")
            print(f"  Current description: '{col.description or 'None'}'")
            print(f"  Current user_description: '{col.user_description or 'None'}'")
            
            updater = Column.updater(
                qualified_name=col.qualified_name,
                name=col.name
            )
            updater.user_description = description
            batch.append(updater)
            
            updated_columns.append({
                "name": col.name,
                "description": description,
                "qualified_name": col.qualified_name
            })
    
    if batch:
        print(f"Saving {len(batch)} column descriptions to Atlan.")
        atlan_client.asset.save(batch)
        print("Successfully saved descriptions.")
        
        # Save the descriptions to a JSON file for reference
        output_file = f"{connection_type}_{table_name}_descriptions.json"
        with open(output_file, 'w') as f:
            json.dump(updated_columns, f, indent=2)
        print(f"Saved descriptions to {output_file}")
    else:
        print("No columns to update.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python fix_table_descriptions.py <connection_type> <table_name>")
        print("Example: python fix_table_descriptions.py snowflake CUSTOMERS")
        sys.exit(1)
    
    connection_type = sys.argv[1]
    table_name = sys.argv[2]
    asyncio.run(fix_table_descriptions(connection_type, table_name))