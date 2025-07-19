# config.py
"""
Configuration file for Atlan S3 Connector
"""
import os
from dataclasses import dataclass
from typing import Dict, List
from dotenv import load_dotenv

load_dotenv()

ATLAN_BASE_URL = os.getenv("ATLAN_BASE_URL")
ATLAN_API_KEY = os.getenv("ATLAN_API_KEY")

    
@dataclass
class S3Config:
    """S3 configuration"""
    bucket_name: str = "atlan-tech-challenge"
    region: str = "us-east-1"
    unique_suffix: str = "sk" 
    
@dataclass
class ConnectionConfig:
    """Existing connection configurations"""
    postgres_connection_id: str = "7f8a2b43-30f5-4571-86e7-d127ea6fc1f4"
    snowflake_connection_id: str = "21712c95-a5ed-49b7-8695-181fa1ace40c"
    postgres_connection_qn: str = "default/postgres/1752268493/FOOD_BEVERAGE/SALES_ORDERS"
    snowflake_connection_qn: str = "default/snowflake/1752268526/FOOD_BEVERAGE/SALES_ORDERS"
    postgres_connection_name: str = "postgres-sk"
    snowflake_connection_name: str = "snowflake-sk"

@dataclass
class AIConfig:
    """AI enhancement configuration"""
    google_api_key: str = os.getenv("GOOGLE_API_KEY")
    gemini_model: str = "gemini-2.5-flash"
    pii_detection_threshold: float = 0.8
    enable_auto_description: bool = True
    enable_pii_classification: bool = True

# PII Detection Patterns (UNUSED - Replaced by Gemini model)
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
