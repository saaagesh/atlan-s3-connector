# flask_app/ai_enhancer.py
"""
AI Enhancer for the Flask UI, with added UI-specific methods.
"""

import logging
from typing import List, Dict, Any
import google.generativeai as genai

# Since this file is in a subdirectory, we need to handle imports carefully.
# We will rely on the sys.path modification in app.py to find the config module.
from config import AIConfig

logger = logging.getLogger(__name__)

import json

# ... (imports)

class AIEnhancer:
    """
    A class to handle AI-powered enhancements, specifically for the Flask UI.
    """
    def __init__(self, config: AIConfig):
        self.config = config
        api_key = config.google_api_key

        if not api_key or "your-google-api-key" in api_key:
            raise ValueError(
                "Google API key is not configured correctly. "
                "Please ensure the GOOGLE_API_KEY in flask_app/.env is set to a valid key."
            )
        
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(config.gemini_model)

    async def generate_asset_description(self, payload: Dict[str, Any]) -> str:
        """
        Generates an AI description for an asset (S3 object or table) using the Gemini API.
        
        Args:
            payload: Dictionary containing asset information
            
        Returns:
            Generated description string
        """
        asset_name = payload.get('name', 'Unknown Asset')
        asset_type = payload.get('type', 'unknown')
        
        logger.info(f"Generating description for asset: {asset_name} (type: {asset_type})")
        
        # Build context for the prompt based on asset type
        context = f"Asset name: {asset_name}\nAsset type: {asset_type}\n"
        
        # Add schema information for S3 objects
        if asset_type == 's3' and 'schema_info' in payload:
            schema_info = payload['schema_info']
            columns = schema_info.get('columns', [])
            
            if columns:
                context += f"\nThis is a CSV file with {len(columns)} columns:\n"
                for col in columns:
                    col_name = col.get('name', 'Unknown')
                    col_type = col.get('type', 'Unknown')
                    sample_values = col.get('sample_values', [])
                    
                    context += f"- {col_name} ({col_type})"
                    if sample_values:
                        samples = ', '.join([str(s) for s in sample_values[:3]])
                        context += f" - Sample values: {samples}"
                    context += "\n"
        
        prompt = f"""
        As a senior data analyst, your task is to write a clear, business-friendly description for a data asset.
        
        Here's information about the asset:
        {context}
        
        Generate a concise (2-3 sentences) but informative description that explains:
        1. What this asset likely contains
        2. Its potential business purpose
        3. How it might be used in analytics or operations
        
        The description should be professional and helpful for data users who need to understand this asset.
        
        Description:
        """
        
        try:
            response = await self.model.generate_content_async(prompt)
            description = response.text.strip()
            
            logger.info(f"Successfully generated description for {asset_name}")
            return description
            
        except Exception as e:
            logger.error(f"Failed to generate asset description: {e}")
            return f"This appears to be a {asset_type} asset named {asset_name}."

    async def generate_column_level_descriptions(self, payload: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Generates real AI descriptions for a list of columns using the Gemini API.
        """
        logger.info(f"Generating descriptions for {len(payload['columns'])} columns in asset {payload['asset_qualified_name']}.")

        column_names = [col['name'] for col in payload['columns']]
        table_name = payload['asset_qualified_name'].split('/')[-1]

        prompt = f"""
        As a senior data analyst, your task is to write a clear, business-friendly description for each database column.
        The table is named '{table_name}'.
        
        Generate a concise (under 15 words) description for each of the following columns: {', '.join(column_names)}

        Return the output as a single, valid JSON object where the keys are the exact column names and the values are the descriptions.
        Example format:
        {{
          "COLUMN_NAME_1": "Description for column 1.",
          "COLUMN_NAME_2": "Description for column 2."
        }}
        
        JSON object:
        """

        try:
            response = await self.model.generate_content_async(prompt)
            
            # Clean the response to extract only the JSON part
            cleaned_response = response.text.strip().replace('```json', '').replace('```', '').strip()
            
            descriptions_map = json.loads(cleaned_response)
            
            generated_descriptions = []
            for col_name, description in descriptions_map.items():
                generated_descriptions.append({
                    "name": col_name,
                    "description": description
                })
            
            return generated_descriptions

        except Exception as e:
            logger.error(f"Failed to generate or parse AI descriptions: {e}")
            return [{"name": col['name'], "description": f"Error during AI generation: {e}"} for col in payload['columns']]

    async def generate_glossary_readme(self, payload: Dict[str, Any]) -> str:
        """
        Generates an AI-powered README for a business glossary term or category.
        
        Args:
            payload: Dictionary containing glossary item information
            
        Returns:
            Generated README content as markdown string
        """
        item_name = payload.get('name', 'Unknown Item')
        item_type = payload.get('type', 'term')
        description = payload.get('description', '')
        
        logger.info(f"Generating README for glossary {item_type}: {item_name}")
        
        # Build context based on item type
        if item_type == 'category':
            prompt = f"""
            As a business analyst and data governance expert, create a comprehensive README for a business glossary category.
            
            Category Name: {item_name}
            Current Description: {description if description else 'No description provided'}
            
            Generate a well-structured README in markdown format that includes:
            
            1. **Overview** - What this category represents in the business context
            2. **Purpose** - Why this category exists and its business value
            3. **Scope** - What types of terms belong in this category
            4. **Usage Guidelines** - How teams should use terms from this category
            5. **Related Categories** - How this category relates to other business areas
            6. **Governance** - Who maintains this category and approval processes
            
            The README should be professional, clear, and help business users understand when and how to use terms from this category.
            
            README Content:
            """
        else:  # term
            prompt = f"""
            As a business analyst and data governance expert, create a comprehensive README for a business glossary term.
            
            Term Name: {item_name}
            Current Description: {description if description else 'No description provided'}
            
            Generate a well-structured README in markdown format that includes:
            
            1. **Definition** - Clear, business-friendly definition of this term
            2. **Business Context** - How this term is used in business operations
            3. **Data Context** - How this term relates to data assets and fields
            4. **Usage Examples** - Practical examples of when to use this term
            5. **Related Terms** - Other glossary terms that are related or similar
            6. **Calculation/Rules** - If applicable, how this term is calculated or determined
            7. **Data Sources** - Where data for this term typically comes from
            8. **Governance** - Who owns this term and approval processes for changes
            
            The README should help both business and technical users understand exactly what this term means and how to use it consistently.
            
            README Content:
            """
        
        try:
            response = await self.model.generate_content_async(prompt)
            readme_content = response.text.strip()
            
            logger.info(f"Successfully generated README for {item_type}: {item_name}")
            return readme_content
            
        except Exception as e:
            logger.error(f"Failed to generate glossary README: {e}")
            return f"""# {item_name}

## Overview
This is a business glossary {item_type} that requires documentation.

{f"**Current Description:** {description}" if description else ""}

## Status
README generation failed. Please manually update this content.

**Error:** {str(e)}
"""

