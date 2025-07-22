# ai_enhancer.py
"""
AI Enhancer for Atlan S3 Connector
Generates descriptions, tags, and PII classifications using a generative AI model.
"""

import logging
from typing import List, Dict, Any
import google.generativeai as genai

from config import AIConfig

logger = logging.getLogger(__name__)

class AIEnhancer:
    """
    A class to handle AI-powered enhancements for Atlan assets.
    """
    def __init__(self, config: AIConfig):
        self.config = config
        if config.google_api_key and config.google_api_key != "your-google-api-key":
            genai.configure(api_key=config.google_api_key)
            self.model = genai.GenerativeModel(config.gemini_model)
        else:
            self.model = None
            logger.warning("Google API key is not configured. AIEnhancer will be disabled.")

    async def generate_asset_descriptions(self, assets: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Generates intelligent descriptions for a list of S3 assets.
        This is a placeholder for the actual implementation.
        """
        if not self.model:
            return {}
        logger.info(f"Generating descriptions for {len(assets)} assets (original method).")
        # In a real implementation, you would call the Gemini API here.
        return {asset['metadata']['key']: "A description from the original AI enhancer." for asset in assets}

    async def classify_pii_data(self, assets: List[Dict[str, Any]]) -> Dict[str, Dict]:
        """
        Classifies PII data within the given assets.
        This is a placeholder for the actual implementation.
        """
        if not self.model:
            return {}
        logger.info(f"Classifying PII data for {len(assets)} assets (original method).")
        # In a real implementation, you would call the Gemini API here.
        return {asset['metadata']['key']: {'has_pii': False, 'pii_types': []} for asset in assets}

    async def generate_compliance_tags(self, assets: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """
        Generates compliance-related tags for assets.
        Uses the correct tag names from the config.
        """
        if not self.model:
            return {}
            
        from config import COMPLIANCE_TAGS
        
        logger.info(f"Generating compliance tags for {len(assets)} assets")
        
        # Get the actual tag names from the config
        tag_names = list(COMPLIANCE_TAGS.keys())
        
        # For demonstration, assign random tags from our config
        import random
        result = {}
        
        for asset in assets:
            # Select 1-2 random tags from our defined compliance tags
            asset_key = asset['metadata']['key']
            num_tags = random.randint(1, 2)
            selected_tags = random.sample(tag_names, min(num_tags, len(tag_names)))
            result[asset_key] = selected_tags
            
            logger.info(f"Generated tags for {asset_key}: {selected_tags}")
            
        return result