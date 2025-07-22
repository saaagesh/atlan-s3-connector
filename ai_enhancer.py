# ai_enhancer.py
"""
AI Enhancer for Atlan S3 Connector
Generates descriptions, tags, and PII classifications using a generative AI model.
"""

import logging
from typing import List, Dict, Any
import google.generativeai as genai
import json

from config import AIConfig
from pii_classifier import PIIClassifier

logger = logging.getLogger(__name__)

class AIEnhancer:
    """
    A class to handle AI-powered enhancements for Atlan assets.
    """
    def __init__(self, config: AIConfig, atlan_client=None):
        self.config = config
        self.atlan_client = atlan_client
        
        # Initialize PII classifier if Atlan client is provided
        self.pii_classifier = PIIClassifier(atlan_client) if atlan_client else None
        
        # Initialize Gemini model if API key is provided
        if config.google_api_key and config.google_api_key != "your-google-api-key":
            genai.configure(api_key=config.google_api_key)
            self.model = genai.GenerativeModel(config.gemini_model)
        else:
            self.model = None
            logger.warning("Google API key is not configured. AI-based enhancements will be disabled.")

    async def generate_asset_descriptions(self, assets: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Generates intelligent descriptions for a list of S3 assets using AI.
        """
        if not self.model:
            return {}
        
        descriptions = {}
        logger.info(f"Generating descriptions for {len(assets)} assets.")
        
        for asset in assets:
            try:
                # Create context-rich prompt for asset description
                asset_name = asset['metadata']['key']
                schema_info = asset['metadata'].get('schema_info', {})
                columns = schema_info.get('columns', [])
                
                prompt = f"""
                Generate a business-friendly description for this data asset:
                
                Asset Name: {asset_name}
                File Type: CSV
                Columns: {len(columns)}
                
                Column Details:
                """
                
                for col in columns[:10]:  # Limit to first 10 columns
                    sample_values = col.get('sample_values', [])
                    sample_str = ', '.join(str(v) for v in sample_values[:3]) if sample_values else 'N/A'
                    prompt += f"- {col['name']} ({col['type']}): Examples: {sample_str}\n"
                
                prompt += """
                
                Please provide:
                1. A concise business description of what this dataset contains
                2. Potential use cases for this data
                3. Data quality observations based on the schema
                
                Keep the description professional and suitable for a data catalog.
                """
                
                response = self.model.generate_content(prompt)
                descriptions[asset_name] = response.text.strip()
                
            except Exception as e:
                logger.error(f"Failed to generate description for {asset_name}: {str(e)}")
                descriptions[asset_name] = f"Data file containing {len(columns)} columns of business data."
        
        return descriptions

    async def classify_pii_data(self, assets: List[Dict[str, Any]]) -> Dict[str, Dict]:
        """
        Classifies PII data within the given assets using Gemini AI model.
        """
        logger.info(f"Classifying PII data for {len(assets)} assets using Gemini.")
        pii_classifications = {}
        
        # If AI model is available, use it for classification
        if self.model:
            for asset in assets:
                asset_name = asset['metadata']['key']
                try:
                    schema_info = asset['metadata'].get('schema_info', {})
                    columns = schema_info.get('columns', [])
                    
                    # Skip if no columns are available
                    if not columns:
                        logger.warning(f"No columns available for {asset_name}, skipping PII classification")
                        continue
                    
                    # Prepare column definitions in the format expected by the model
                    column_definitions = []
                    for col in columns:
                        sample_values = col.get('sample_values', [])
                        sample_str = ', '.join(str(v) for v in sample_values[:3]) if sample_values else 'N/A'
                        
                        column_definition = {
                            "column_name": col['name'],
                            "column_qualified_name": f"{asset_name}.{col['name']}",
                            "column_description": f"Sample values: {sample_str}",
                            "column_data_type": col['type']
                        }
                        column_definitions.append(column_definition)
                    
                    # Prepare table definition
                    table_definition = {
                        'table_name': asset_name,
                        'table_description': f"S3 object containing data with {len(columns)} columns",
                        'columns': column_definitions
                    }
                    
                    # Create  PII detection prompt
                    prompt = """
                    Analyze the following data schema for Personally Identifiable Information (PII).
                    
                    For each column, determine if it contains PII based on the column name and sample data.
                    
                    Common PII types include:
                    - Names (firstname, lastname, fullname, contactname)
                    - Contact info (email, phone)
                    - Location data (address, city, postalcode)
                    - IDs (customerid, userid, employeeid)
                    - Financial data (accountnumber, creditcard)
                    
                    Respond with a JSON object containing:
                    {
                      "classifications": [
                        {
                          "table_name": "string",
                          "columns": [
                            {
                              "column_name": "string",
                              "pi_classification_type": "string", 
                              "has_pii": boolean
                            }
                          ]
                        }
                      ]
                    }
                    
                    Where pi_classification_type is one of: "name", "contact", "location", "id", "financial", "other", or "not_pii"
                    
                    Now evaluate the following table definition:
                    """ + json.dumps([table_definition])
                    
                    # Call Gemini model
                    response = self.model.generate_content(prompt)
                    response_text = response.text.strip()
                    
                    # Try to extract JSON from the response
                    try:
                        # Find JSON content in the response (it might be wrapped in markdown code blocks)
                        json_start = response_text.find('{')
                        json_end = response_text.rfind('}') + 1
                        
                        if json_start >= 0 and json_end > json_start:
                            json_content = response_text[json_start:json_end]
                            ai_classification = json.loads(json_content)
                            
                            # Process the classification results
                            if 'classifications' in ai_classification and len(ai_classification['classifications']) > 0:
                                table_classification = ai_classification['classifications'][0]
                                column_classifications = table_classification.get('columns', [])
                                
                                # Extract PII information
                                has_pii = False
                                pii_types = []
                                sensitive_columns = []
                                
                                for col_class in column_classifications:
                                    pi_type = col_class.get('pi_classification_type')
                                    column_name = col_class.get('column_name')
                                    has_pii_column = col_class.get('has_pii', False)
                                    
                                    # If column contains PII
                                    if pi_type != "not_pii" and has_pii_column:
                                        has_pii = True
                                        
                                        # Add PII type if not already in the list
                                        if pi_type not in pii_types:
                                            pii_types.append(pi_type)
                                        
                                        # Add column to sensitive columns
                                        sensitive_columns.append(column_name)
                                
                                # Determine compliance tags based on PII types
                                compliance_tags = []
                                
                                # Always add PII tag if any PII is detected
                                if has_pii:
                                    compliance_tags.append("PII")
                                
                                # Singapore PDPA / PDPA
                                if any(pi_type in ["name", "contact", "location"] for pi_type in pii_types):
                                    compliance_tags.append("PDPA")
                                
                                # GDPR
                                if len(pii_types) >= 2 and "name" in pii_types:
                                    compliance_tags.append("GDPR")
                                
                                # Financial data
                                if "financial" in pii_types:
                                    compliance_tags.append("Financial")
                                
                                # Customer data
                                if "name" in pii_types or "id" in pii_types or "customer" in ' '.join(sensitive_columns).lower():
                                    compliance_tags.append("Customer Data")
                                
                                # Sensitive data
                                if has_pii:
                                    compliance_tags.append("Sensitive")
                                
                                # Create simplified CIA rating
                                cia_rating = {
                                    "confidentiality": "Medium" if has_pii else "Low",
                                    "integrity": "Medium" if has_pii else "Low",
                                    "availability": "Low"
                                }
                                
                                # Create final classification
                                pii_classifications[asset_name] = {
                                    'has_pii': has_pii,
                                    'pii_types': pii_types,
                                    'sensitivity_level': "Medium" if has_pii else "Low",
                                    'confidence': 0.9,  # High confidence with Gemini model
                                    'cia_rating': cia_rating,
                                    'sensitive_columns': sensitive_columns,
                                    'compliance_tags': compliance_tags
                                }
                                
                                logger.info(f"PII classification for {asset_name}: {pii_types} (Sensitivity: {highest_sensitivity})")
                            else:
                                logger.warning(f"No classification data found in AI response for {asset_name}")
                                pii_classifications[asset_name] = {
                                    'has_pii': False,
                                    'pii_types': [],
                                    'sensitivity_level': 'Low',
                                    'confidence': 0.5,
                                    'cia_rating': {"confidentiality": "Low", "integrity": "Low", "availability": "Low"},
                                    'sensitive_columns': []
                                }
                        else:
                            logger.warning(f"Could not find valid JSON in AI response for {asset_name}")
                            
                    except Exception as json_error:
                        logger.error(f"Failed to parse AI response for {asset_name}: {str(json_error)}")
                        pii_classifications[asset_name] = {
                            'has_pii': False,
                            'pii_types': [],
                            'sensitivity_level': 'Low',
                            'confidence': 0.0,
                            'cia_rating': {"confidentiality": "Low", "integrity": "Low", "availability": "Low"},
                            'sensitive_columns': []
                        }
                        
                except Exception as e:
                    logger.error(f"Failed to classify PII for {asset_name} using AI: {str(e)}")
                    pii_classifications[asset_name] = {
                        'has_pii': False,
                        'pii_types': [],
                        'sensitivity_level': 'Low',
                        'confidence': 0.0,
                        'cia_rating': {"confidentiality": "Low", "integrity": "Low", "availability": "Low"},
                        'sensitive_columns': []
                    }
        else:
            # Fallback to rule-based classification if AI model is not available
            if self.pii_classifier:
                for asset in assets:
                    asset_name = asset['metadata']['key']
                    try:
                        # Use rule-based classification
                        classification = self.pii_classifier.detect_pii_rule_based(asset)
                        
                        # Convert to dictionary for storage
                        pii_classifications[asset_name] = {
                            'has_pii': classification.has_pii,
                            'pii_types': classification.pii_types,
                            'sensitivity_level': classification.sensitivity_level,
                            'confidence': classification.confidence,
                            'cia_rating': {
                                'confidentiality': classification.cia_rating.confidentiality.value,
                                'integrity': classification.cia_rating.integrity.value,
                                'availability': classification.cia_rating.availability.value
                            },
                            'sensitive_columns': classification.sensitive_columns
                        }
                        
                    except Exception as e:
                        logger.error(f"Failed to classify PII for {asset_name} using rule-based approach: {str(e)}")
                        pii_classifications[asset_name] = {
                            'has_pii': False,
                            'pii_types': [],
                            'confidence': 0.0
                        }
            else:
                logger.warning("No PII classification method available (neither AI nor rule-based)")
        
        return pii_classifications

    async def generate_compliance_tags(self, assets: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """
        Generates compliance-related tags for assets based on PII classification.
        Uses the compliance_tags already determined during PII classification.
        """
        logger.info(f"Generating compliance tags for {len(assets)} assets.")
        compliance_tags = {}
        
        # First, check if we have PII classifications with compliance tags already determined
        for asset in assets:
            asset_name = asset['metadata']['key']
            
            # If we have PII classifications with compliance tags, use them
            if hasattr(self, 'pii_classifications') and asset_name in self.pii_classifications:
                pii_data = self.pii_classifications[asset_name]
                if 'compliance_tags' in pii_data:
                    compliance_tags[asset_name] = pii_data['compliance_tags']
                    logger.info(f"Using pre-determined compliance tags for {asset_name}: {pii_data['compliance_tags']}")
                    continue
            
            # If we have a PII classifier, use it to determine compliance tags
            if self.pii_classifier:
                try:
                    # Use rule-based classification to determine tags
                    classification = self.pii_classifier.detect_pii_rule_based(asset)
                    compliance_tags[asset_name] = self.pii_classifier._determine_compliance_tags(classification)
                    logger.info(f"Generated compliance tags using rule-based approach for {asset_name}: {compliance_tags[asset_name]}")
                except Exception as e:
                    logger.error(f"Failed to generate compliance tags for {asset_name} using rule-based approach: {str(e)}")
                    compliance_tags[asset_name] = []
            
            # If AI model is available and we don't have tags yet, use AI-based approach
            elif self.model and (asset_name not in compliance_tags or not compliance_tags[asset_name]):
                schema_info = asset['metadata'].get('schema_info', {})
                columns = schema_info.get('columns', [])
                
                tags = []
                
                # Analyze column names for compliance indicators
                column_names = [col['name'].lower() for col in columns]
                
                # Singapore PDPA compliance
                if any(indicator in ' '.join(column_names) for indicator in ['email', 'phone', 'name', 'address', 'contact']):
                    tags.append('singapore_pdpa')
                
                # Financial data indicators
                if any(indicator in ' '.join(column_names) for indicator in ['account', 'payment', 'credit', 'bank', 'financial']):
                    tags.append('financial_sensitive')
                
                # Customer data indicators
                if any(indicator in ' '.join(column_names) for indicator in ['customer', 'client', 'user']):
                    tags.append('gdpr_equivalent')
                
                # Indonesian compliance
                if 'indonesia' in asset_name.lower() or any('id' in col_name for col_name in column_names):
                    tags.append('indonesia_pp71')
                
                compliance_tags[asset_name] = tags
                logger.info(f"Generated compliance tags using AI-based approach for {asset_name}: {tags}")
            
            # Fallback if neither classifier nor AI is available
            else:
                compliance_tags[asset_name] = []
        
        # Store the compliance tags for future reference
        self.compliance_tags_cache = compliance_tags
        
        return compliance_tags
