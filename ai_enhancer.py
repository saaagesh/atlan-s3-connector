# ai_enhancer.py
"""
Advanced AI Enhancement for Atlan S3 Connector
Provides intelligent data classification, automated documentation, and compliance insights
"""

import google.generativeai as genai
import logging
import re
import asyncio
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import json

from config import AIConfig, PII_PATTERNS, COMPLIANCE_TAGS

logger = logging.getLogger(__name__)

class AIEnhancer:
    """AI-powered enhancement engine for data cataloging"""
    
    def __init__(self, ai_config: AIConfig):
        self.ai_config = ai_config
        genai.configure(api_key=ai_config.google_api_key)
        self.model = genai.GenerativeModel(ai_config.gemini_model)
        
        # Initialize PII detection patterns
        self.pii_patterns = PII_PATTERNS
        self.compliance_tags = COMPLIANCE_TAGS
    
    async def generate_asset_descriptions(self, assets: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Generate intelligent, business-focused descriptions for assets
        
        Args:
            assets: List of asset information
            
        Returns:
            Dictionary mapping asset keys to generated descriptions
        """
        logger.info("Generating AI-powered asset descriptions")
        
        descriptions = {}
        
        for asset_info in assets:
            try:
                asset_key = asset_info['metadata']['key']
                schema_info = asset_info['metadata']['schema_info']
                
                logger.info(f"Generating description for asset: {asset_key}")
                
                # Generate description using GPT
                description = await self._generate_smart_description(asset_key, schema_info)
                descriptions[asset_key] = description
                
                logger.info(f"AI Generated description for {asset_key}:")
                logger.info(f"Full description: {description}")
                logger.info(f"Description length: {len(description)} characters")
                
            except Exception as e:
                logger.error(f"Failed to generate description for {asset_info['metadata']['key']}: {str(e)}")
                # Fallback to template-based description
                fallback_desc = self._get_fallback_description(asset_info['metadata']['key'])
                descriptions[asset_info['metadata']['key']] = fallback_desc
                logger.info(f"Using fallback description for {asset_info['metadata']['key']}: {fallback_desc}")
        
        logger.info(f"Generated descriptions for {len(descriptions)} assets")
        return descriptions
    
    async def _generate_smart_description(self, asset_key: str, schema_info: Dict) -> str:
        """Generate intelligent description using Gemini"""
        
        # Extract table name from file
        table_name = asset_key.replace('.csv', '')
        
        # Build context prompt
        column_info = ""
        if schema_info.get('columns'):
            column_info = ", ".join([f"{col['name']} ({col['type']})" for col in schema_info['columns']])
        
        prompt = f"""
        You are a data governance expert creating business-friendly descriptions for a data catalog.
        
        File: {asset_key}
        Table: {table_name}
        Columns: {column_info}
        
        Create a concise, professional description (2-3 sentences) that explains:
        1. What business purpose this data serves
        2. Key information it contains
        3. How it fits in the data pipeline (Postgres → S3 → Snowflake → Looker)
        
        Focus on business value, not technical details. Use clear, non-technical language.

        If its a .csv file, always term it as a "data file" and avoid using "table" terminology.
        """
        
        try:
            response = self.model.generate_content(prompt)
            return response.text.strip()
            
        except Exception as e:
            logger.error(f"Gemini API error: {str(e)}")
            return self._get_fallback_description(asset_key)
    
    def _get_fallback_description(self, asset_key: str) -> str:
        """Generate fallback description when AI is unavailable"""
        table_name = asset_key.replace('.csv', '')
        return f"Data file for {table_name} loaded from transactional systems into the data lake for analytics processing."
    
    async def classify_pii_data(self, assets: List[Dict[str, Any]]) -> Dict[str, Dict]:
        """
        PII classification temporarily disabled - returning empty results
        
        Args:
            assets: List of asset information
            
        Returns:
            Dictionary mapping asset keys to empty PII classification results
        """
        logger.info("PII classification is disabled - skipping PII analysis")
        
        pii_classifications = {}
        
        for asset_info in assets:
            asset_key = asset_info['metadata']['key']
            # Return empty PII classification to avoid breaking the pipeline
            pii_classifications[asset_key] = {
                "has_pii": False,
                "confidence": 0.0,
                "pii_types": [],
                "sensitive_columns": []
            }
            logger.info(f"Skipping PII analysis for {asset_key}")
        
        logger.info(f"Returned empty PII classifications for {len(pii_classifications)} assets")
        return pii_classifications
    
    # PII analysis methods commented out - PII processing is disabled
    # async def _analyze_pii_content(self, asset_key: str, schema_info: Dict) -> Dict:
    #     """Analyze asset for PII content using AI - DISABLED"""
    #     pass
    
    # async def _ai_pii_analysis(self, asset_key: str, schema_info: Dict) -> Dict:
    #     """Use AI to analyze potential PII in ambiguous cases - DISABLED"""
    #     pass
    
    # def _calculate_risk_level(self, pii_types: List[str], confidence: float) -> str:
    #     """Calculate risk level based on PII types and confidence - DISABLED"""
    #     pass
    
    async def generate_compliance_tags(self, assets: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """
        Generate compliance tags based on data content and regulations
        
        Args:
            assets: List of asset information
            
        Returns:
            Dictionary mapping asset keys to compliance tags
        """
        logger.info("Generating compliance tags")
        
        compliance_tags = {}
        
        for asset_info in assets:
            try:
                asset_key = asset_info['metadata']['key']
                
                logger.info(f"Generating compliance tags for asset: {asset_key}")
                
                # Generate compliance tags based on content
                tags = await self._generate_compliance_tags_for_asset(asset_key, asset_info)
                compliance_tags[asset_key] = tags
                
                logger.info(f"Generated compliance tags for {asset_key}: {tags}")
                
            except Exception as e:
                logger.error(f"Failed to generate compliance tags for {asset_info['metadata']['key']}: {str(e)}")
                # Add empty tags to avoid breaking the pipeline
                compliance_tags[asset_info['metadata']['key']] = []
        
        logger.info(f"Generated compliance tags for {len(compliance_tags)} assets")
        return compliance_tags
    
    async def _generate_compliance_tags_for_asset(self, asset_key: str, asset_info: Dict) -> List[str]:
        """Generate compliance tags for a specific asset"""
        
        tags = []
        schema_info = asset_info['metadata']['schema_info']
        
        # Check for PII content
        if schema_info.get('columns'):
            has_personal_data = any(
                any(pattern in col['name'].lower() for pattern in ['name', 'email', 'phone', 'address'])
                for col in schema_info['columns']
            )
            
            if has_personal_data:
                tags.extend([
                    self.compliance_tags['singapore_pdpa'],
                    self.compliance_tags['indonesia_pp71'],
                    self.compliance_tags['gdpr_equivalent']
                ])
        
        # Check for financial data
        has_financial_data = any(
            any(pattern in col['name'].lower() for pattern in ['price', 'amount', 'cost', 'payment'])
            for col in schema_info.get('columns', [])
        )
        
        if has_financial_data:
            tags.append(self.compliance_tags['financial_sensitive'])
        
        # Add file-specific tags
        if 'CUSTOMER' in asset_key.upper():
            tags.append("Customer Data Protection Required")
        elif 'EMPLOYEE' in asset_key.upper():
            tags.append("HR Data - Restricted Access")
        elif 'ORDER' in asset_key.upper():
            tags.append("Transaction Data - Audit Required")
        
        return list(set(tags))  # Remove duplicates
    
    
