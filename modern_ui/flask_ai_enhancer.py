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

