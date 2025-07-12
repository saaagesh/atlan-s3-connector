# ai_enhancer.py
"""
Advanced AI Enhancement for Atlan S3 Connector
Provides intelligent data classification, automated documentation, and compliance insights
"""

import openai
import logging
import re
import asyncio
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import json

from config import AIConfig, PII_PATTERNS, COMPLIANCE_TAGS, COLUMN_MAPPINGS

logger = logging.getLogger(__name__)

class AIEnhancer:
    """AI-powered enhancement engine for data cataloging"""
    
    def __init__(self, ai_config: AIConfig):
        self.ai_config = ai_config
        openai.api_key = ai_config.openai_api_key
        
        # Initialize PII detection patterns
        self.pii_patterns = PII_PATTERNS
        self.compliance_tags = COMPLIANCE_TAGS
        
        # Business context templates
        self.business_context_templates = {
            "CUSTOMERS": "Customer master data containing personal information for CRM and marketing purposes",
            "ORDERS": "Transaction records tracking customer purchases and order fulfillment",
            "PRODUCTS": "Product catalog with pricing, descriptions, and inventory information",
            "EMPLOYEES": "Human resources data including employee personal and professional information",
            "SUPPLIERS": "Vendor and supplier information for procurement and supply chain management",
            "CATEGORIES": "Product categorization system for inventory management and reporting",
            "SHIPPERS": "Logistics and shipping partner information for order fulfillment",
            "ORDERDETAILS": "Detailed line items for each order including quantities and pricing"
        }
    
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
                
                # Generate description using GPT
                description = await self._generate_smart_description(asset_key, schema_info)
                descriptions[asset_key] = description
                
            except Exception as e:
                logger.error(f"Failed to generate description for {asset_info['metadata']['key']}: {str(e)}")
                # Fallback to template-based description
                descriptions[asset_info['metadata']['key']] = self._get_fallback_description(asset_info['metadata']['key'])
        
        logger.info(f"Generated descriptions for {len(descriptions)} assets")
        return descriptions
    
    async def _generate_smart_description(self, asset_key: str, schema_info: Dict) -> str:
        """Generate intelligent description using OpenAI GPT"""
        
        # Extract table name from file
        table_name = asset_key.replace('.csv', '')
        
        # Build context prompt
        column_info = ""
        if schema_info.get('columns'):
            column_info = ", ".join([f"{col['name']} ({col['type']})" for col in schema_info['columns']])
        
        # Create business context
        business_context = self.business_context_templates.get(table_name, "Business data file")
        
        prompt = f"""
        You are a data governance expert creating business-friendly descriptions for a data catalog.
        
        File: {asset_key}
        Table: {table_name}
        Columns: {column_info}
        Business Context: {business_context}
        
        Create a concise, professional description (2-3 sentences) that explains:
        1. What business purpose this data serves
        2. Key information it contains
        3. How it fits in the data pipeline (Postgres → S3 → Snowflake → Looker)
        
        Focus on business value, not technical details. Use clear, non-technical language.
        """
        
        try:
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a data governance expert who creates clear, business-focused descriptions for data assets."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=150,
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"OpenAI API error: {str(e)}")
            return self._get_fallback_description(asset_key)
    
    def _get_fallback_description(self, asset_key: str) -> str:
        """Generate fallback description when AI is unavailable"""
        table_name = asset_key.replace('.csv', '')
        base_description = self.business_context_templates.get(table_name, "Data file")
        return f"{base_description} loaded from transactional systems into the data lake for analytics processing."
    
    async def classify_pii_data(self, assets: List[Dict[str, Any]]) -> Dict[str, Dict]:
        """
        Classify assets for PII content with confidence scoring
        
        Args:
            assets: List of asset information
            
        Returns:
            Dictionary mapping asset keys to PII classification results
        """
        logger.info("Classifying PII data with AI")
        
        pii_classifications = {}
        
        for asset_info in assets:
            try:
                asset_key = asset_info['metadata']['key']
                schema_info = asset_info['metadata']['schema_info']
                
                # Analyze columns for PII
                pii_analysis = await self._analyze_pii_content(asset_key, schema_info)
                pii_classifications[asset_key] = pii_analysis
                
            except Exception as e:
                logger.error(f"Failed to classify PII for {asset_info['metadata']['key']}: {str(e)}")
        
        logger.info(f"Classified PII for {len(pii_classifications)} assets")
        return pii_classifications
    
    async def _analyze_pii_content(self, asset_key: str, schema_info: Dict) -> Dict:
        """Analyze asset for PII content using pattern matching and AI"""
        
        pii_indicators = []
        confidence_scores = []
        
        if not schema_info.get('columns'):
            return {"has_pii": False, "confidence": 0.0, "pii_types": [], "sensitive_columns": []}
        
        # Pattern-based detection
        for column in schema_info['columns']:
            col_name = column['name'].lower()
            
            for pii_type, patterns in self.pii_patterns.items():
                for pattern in patterns:
                    if re.search(pattern, col_name):
                        pii_indicators.append({
                            "column": column['name'],
                            "pii_type": pii_type,
                            "confidence": 0.9,
                            "method": "pattern_matching"
                        })
                        confidence_scores.append(0.9)
        
        # AI-based analysis for ambiguous cases
        if len(pii_indicators) == 0:
            ai_analysis = await self._ai_pii_analysis(asset_key, schema_info)
            pii_indicators.extend(ai_analysis.get('indicators', []))
            confidence_scores.extend(ai_analysis.get('confidence_scores', []))
        
        # Calculate overall confidence
        overall_confidence = max(confidence_scores) if confidence_scores else 0.0
        has_pii = overall_confidence > self.ai_config.pii_detection_threshold
        
        # Extract unique PII types and sensitive columns
        pii_types = list(set([indicator['pii_type'] for indicator in pii_indicators]))
        sensitive_columns = list(set([indicator['column'] for indicator in pii_indicators]))
        
        return {
            "has_pii": has_pii,
            "confidence": overall_confidence,
            "pii_types": pii_types,
            "sensitive_columns": sensitive_columns,
            "detailed_analysis": pii_indicators,
            "risk_level": self._calculate_risk_level(pii_types, overall_confidence)
        }
    
    async def _ai_pii_analysis(self, asset_key: str, schema_info: Dict) -> Dict:
        """Use AI to analyze potential PII in ambiguous cases"""
        
        column_names = [col['name'] for col in schema_info['columns']]
        
        prompt = f"""
        Analyze these column names for potential PII (Personally Identifiable Information):
        File: {asset_key}
        Columns: {', '.join(column_names)}
        
        For each column, determine:
        1. Does it likely contain PII? (yes/no)
        2. What type of PII? (name, email, phone, address, id, financial, other)
        3. Confidence level (0.0-1.0)
        
        Consider Singapore/Indonesia privacy regulations (PDPA, PP No. 71/2019).
        
        Return JSON format:
        {{"analysis": [{{"column": "name", "has_pii": true, "pii_type": "name", "confidence": 0.95}}]}}
        """
        
        try:
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a data privacy expert analyzing data for PII compliance."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=300,
                temperature=0.1
            )
            
            # Parse AI response
            ai_result = json.loads(response.choices[0].message.content.strip())
            
            indicators = []
            confidence_scores = []
            
            for analysis in ai_result.get('analysis', []):
                if analysis.get('has_pii'):
                    indicators.append({
                        "column": analysis['column'],
                        "pii_type": analysis['pii_type'],
                        "confidence": analysis['confidence'],
                        "method": "ai_analysis"
                    })
                    confidence_scores.append(analysis['confidence'])
            
            return {
                "indicators": indicators,
                "confidence_scores": confidence_scores
            }
            
        except Exception as e:
            logger.error(f"AI PII analysis failed: {str(e)}")
            return {"indicators": [], "confidence_scores": []}
    
    def _calculate_risk_level(self, pii_types: List[str], confidence: float) -> str:
        """Calculate risk level based on PII types and confidence"""
        
        high_risk_types = ['financial', 'id', 'email']
        medium_risk_types = ['name', 'phone', 'address']
        
        if confidence < 0.5:
            return "LOW"
        elif any(pii_type in high_risk_types for pii_type in pii_types):
            return "HIGH"
        elif any(pii_type in medium_risk_types for pii_type in pii_types):
            return "MEDIUM"
        else:
            return "LOW"
    
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
                
                # Generate compliance tags based on content
                tags = await self._generate_compliance_tags_for_asset(asset_key, asset_info)
                compliance_tags[asset_key] = tags
                
            except Exception as e:
                logger.error(f"Failed to generate compliance tags for {asset_info['metadata']['key']}: {str(e)}")
        
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
    
    async def generate_business_glossary_terms(self, assets: List[Dict[str, Any]]) -> Dict[str, Dict]:
        """
        Generate business glossary terms with definitions
        
        Args:
            assets: List of asset information
            
        Returns:
            Dictionary mapping terms to definitions and metadata
        """
        logger.info("Generating business glossary terms")
        
        glossary_terms = {}
        
        # Extract unique column names across all assets
        all_columns = set()
        for asset_info in assets:
            schema_info = asset_info['metadata']['schema_info']
            if schema_info.get('columns'):
                all_columns.update([col['name'] for col in schema_info['columns']])
        
        # Generate definitions for business terms
        for column_name in all_columns:
            try:
                definition = await self._generate_business_definition(column_name)
                glossary_terms[column_name] = {
                    "definition": definition,
                    "category": self._categorize_business_term(column_name),
                    "usage_context": self._get_usage_context(column_name),
                    "created_at": datetime.now().isoformat()
                }
            except Exception as e:
                logger.error(f"Failed to generate definition for {column_name}: {str(e)}")
        
        logger.info(f"Generated {len(glossary_terms)} business glossary terms")
        return glossary_terms
    
    async def _generate_business_definition(self, column_name: str) -> str:
        """Generate business definition for a column/term"""
        
        prompt = f"""
        Create a clear, business-friendly definition for this data field: "{column_name}"
        
        Context: This is from a food & beverage company's transactional data system.
        
        Requirements:
        - Write for business users, not technical users
        - 1-2 sentences maximum
        - Focus on business purpose and meaning
        - No technical jargon
        
        Definition:
        """
        
        try:
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a business analyst creating clear definitions for data terms."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=100,
                temperature=0.3
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"Failed to generate definition for {column_name}: {str(e)}")
            return f"Business data field: {column_name}"
    
    def _categorize_business_term(self, column_name: str) -> str:
        """Categorize business term based on column name"""
        
        col_lower = column_name.lower()
        
        if any(pattern in col_lower for pattern in ['id', 'key', 'number']):
            return "Identifiers"
        elif any(pattern in col_lower for pattern in ['name', 'title', 'description']):
            return "Descriptive"
        elif any(pattern in col_lower for pattern in ['date', 'time', 'created', 'modified']):
            return "Temporal"
        elif any(pattern in col_lower for pattern in ['price', 'cost', 'amount', 'total']):
            return "Financial"
        elif any(pattern in col_lower for pattern in ['email', 'phone', 'address']):
            return "Contact Information"
        else:
            return "General"
    
    def _get_usage_context(self, column_name: str) -> str:
        """Get usage context for business term"""
        
        contexts = {
            "customer": "Customer relationship management and analytics",
            "order": "Order processing and fulfillment tracking",
            "product": "Product catalog management and inventory",
            "employee": "Human resources and workforce management",
            "supplier": "Vendor relationship and procurement management",
            "category": "Product categorization and organization",
            "shipper": "Logistics and delivery management"
        }
        
        col_lower = column_name.lower()
        for key, context in contexts.items():
            if key in col_lower:
                return context
        
        return "General business operations"
    
    async def generate_data_quality_insights(self, assets: List[Dict[str, Any]]) -> Dict[str, Dict]:
        """
        Generate data quality insights and recommendations
        
        Args:
            assets: List of asset information
            
        Returns:
            Dictionary mapping asset keys to quality insights
        """
        logger.info("Generating data quality insights")
        
        quality_insights = {}
        
        for asset_info in assets:
            try:
                asset_key = asset_info['metadata']['key']
                schema_info = asset_info['metadata']['schema_info']
                
                insights = await self._analyze_data_quality(asset_key, schema_info)
                quality_insights[asset_key] = insights
                
            except Exception as e:
                logger.error(f"Failed to generate quality insights for {asset_info['metadata']['key']}: {str(e)}")
        
        logger.info(f"Generated quality insights for {len(quality_insights)} assets")
        return quality_insights
    
    async def _analyze_data_quality(self, asset_key: str, schema_info: Dict) -> Dict:
        """Analyze data quality for an asset"""
        
        insights = {
            "quality_score": 0.0,
            "issues": [],
            "recommendations": [],
            "strengths": []
        }
        
        if not schema_info.get('columns'):
            insights["issues"].append("No schema information available")
            return insights
        
        # Analyze column structure
        columns = schema_info['columns']
        
        # Check for potential issues
        if len(columns) == 0:
            insights["issues"].append("No columns detected")
        elif len(columns) > 50:
            insights["issues"].append("Large number of columns may indicate denormalized data")
        
        # Check for naming conventions
        column_names = [col['name'] for col in columns]
        inconsistent_naming = self._check_naming_consistency(column_names)
        if inconsistent_naming:
            insights["issues"].append("Inconsistent column naming patterns detected")
            insights["recommendations"].append("Standardize column naming conventions")
        
        # Check for potential duplicates
        if len(column_names) != len(set(column_names)):
            insights["issues"].append("Duplicate column names detected")
        
        # Identify strengths
        if any('id' in col.lower() for col in column_names):
            insights["strengths"].append("Contains identifier columns for relationships")
        
        if any('date' in col.lower() or 'time' in col.lower() for col in column_names):
            insights["strengths"].append("Contains temporal data for time-based analysis")
        
        # Calculate quality score
        base_score = 70
        base_score -= len(insights["issues"]) * 10
        base_score += len(insights["strengths"]) * 5
        insights["quality_score"] = max(0, min(100, base_score))
        
        return insights
    
    def _check_naming_consistency(self, column_names: List[str]) -> bool:
        """Check if column names follow consistent patterns"""
        
        # Check for mixed case patterns
        has_snake_case = any('_' in name for name in column_names)
        has_camel_case = any(name != name.lower() and '_' not in name for name in column_names)
        
        return has_snake_case and has_camel_case
