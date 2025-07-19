#!/usr/bin/env python3
"""
Debug script to check API response for column descriptions
"""

import os
import sys
import json
import requests
from dotenv import load_dotenv

# Add the current directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)

def debug_columns_api(table_qualified_name):
    """Debug the columns API response"""
    print(f"Debugging columns API response for table: {table_qualified_name}")
    
    # Call the API
    url = "http://localhost:5001/api/columns"
    payload = {
        "asset_qualified_name": table_qualified_name,
        "source_type": "table"
    }
    
    response = requests.post(url, json=payload)
    
    if response.status_code == 200:
        data = response.json()
        print(f"API Response Status: {response.status_code}")
        print(f"Success: {data.get('success', False)}")
        
        columns = data.get('columns', [])
        print(f"Found {len(columns)} columns")
        
        for col in columns:
            print(f"\nColumn: {col.get('name')}")
            print(f"  Description: '{col.get('description', '')}'")
            print(f"  Has Description: {col.get('has_description', False)}")
            print(f"  Type: {col.get('type', '')}")
            print(f"  Qualified Name: {col.get('qualified_name', '')}")
    else:
        print(f"API Error: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python debug_api_response.py <table_qualified_name>")
        sys.exit(1)
    
    table_qualified_name = sys.argv[1]
    debug_columns_api(table_qualified_name)