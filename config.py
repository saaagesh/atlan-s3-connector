# config.py
"""
Configuration file for Atlan S3 Connector
"""
import os
from dataclasses import dataclass
from typing import Dict, List
from dotenv import load_dotenv

load_dotenv()

@dataclass
class AtlanConfig:
    """Atlan connection configuration"""
    base_url: str = "https://tech-challenge.atlan.com"
    api_key: str = os.getenv("ATLAN_API_KEY")
    
@dataclass
class S3Config:
    """S3 configuration"""
    bucket_name: str = "atlan-tech-challenge"
    region: str = "us-east-1"
    unique_suffix: str = "SK"
    
@dataclass
class ConnectionConfig:
    """Existing connection configurations"""
    postgres_connection_id: str = "7f8a2b43-30f5-4571-86e7-d127ea6fc1f4"
    snowflake_connection_id: str = "21712c95-a5ed-49b7-8695-181fa1ace40c"
    postgres_connection_qn: str = "default/postgres/1752268493/FOOD_BEVERAGE/SALES_ORDERS"
    snowflake_connection_qn: str = "default/snowflake/1752268526/FOOD_BEVERAGE/SALES_ORDERS"

@dataclass
class AIConfig:
    """AI enhancement configuration"""
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "your-openai-key")
    pii_detection_threshold: float = 0.8
    enable_auto_description: bool = True
    enable_pii_classification: bool = True

# File mapping for lineage establishment
FILE_LINEAGE_MAPPING = {
    "CATEGORIES.csv": {
        "postgres_table": "CATEGORIES",
        "snowflake_table": "CATEGORIES",
        "description": "Product category master data"
    },
    "CUSTOMERS.csv": {
        "postgres_table": "CUSTOMERS", 
        "snowflake_table": "CUSTOMERS",
        "description": "Customer master data with PII"
    },
    "EMPLOYEES.csv": {
        "postgres_table": "EMPLOYEES",
        "snowflake_table": "EMPLOYEES", 
        "description": "Employee information - sensitive data"
    },
    "ORDERDETAILS.csv": {
        "postgres_table": "ORDERDETAILS",
        "snowflake_table": "ORDERDETAILS",
        "description": "Order line item details"
    },
    "ORDERS.csv": {
        "postgres_table": "ORDERS",
        "snowflake_table": "ORDERS",
        "description": "Order header information"
    },
    "PRODUCTS.csv": {
        "postgres_table": "PRODUCTS",
        "snowflake_table": "PRODUCTS",
        "description": "Product master data"
    },
    "SHIPPERS.csv": {
        "postgres_table": "SHIPPERS",
        "snowflake_table": "SHIPPERS",
        "description": "Shipping company information"
    },
    "SUPPLIERS.csv": {
        "postgres_table": "SUPPLIERS",
        "snowflake_table": "SUPPLIERS",
        "description": "Supplier master data"
    }
}

# PII Detection Patterns
PII_PATTERNS = {
    "email": [r"email", r"mail", r"@"],
    "phone": [r"phone", r"tel", r"mobile", r"cell"],
    "name": [r"name", r"first", r"last", r"full"],
    "address": [r"address", r"street", r"city", r"zip", r"postal"],
    "id": [r"id", r"ssn", r"social", r"passport", r"license"],
    "financial": [r"account", r"card", r"credit", r"bank", r"routing"]
}

# Compliance Tags for Singapore/Indonesia regulations
COMPLIANCE_TAGS = {
    "singapore_pdpa": "Singapore Personal Data Protection Act",
    "indonesia_pp71": "Indonesia PP No. 71/2019",
    "gdpr_equivalent": "GDPR Equivalent Protection",
    "financial_sensitive": "Financial Services Sensitive Data"
}

# Column-level lineage mapping templates
COLUMN_MAPPINGS = {
    "id_columns": ["id", "customer_id", "employee_id", "order_id", "product_id"],
    "name_columns": ["name", "first_name", "last_name", "company_name"],
    "contact_columns": ["email", "phone", "address", "city", "country"],
    "financial_columns": ["price", "amount", "cost", "total", "tax"]
}
