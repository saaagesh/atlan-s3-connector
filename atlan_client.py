# atlan_client.py
"""
Initializes and provides a singleton Atlan client instance.
"""

import os
from pyatlan.client.atlan import AtlanClient

_client = None

def get_atlan_client() -> AtlanClient:
    """
    Retrieves a singleton instance of the AtlanClient.
    The client is initialized using environment variables for the base URL and API key.
    """
    global _client
    if _client is None:
        base_url = os.getenv("ATLAN_BASE_URL")
        api_key = os.getenv("ATLAN_API_KEY")

        if not base_url or not api_key:
            raise ValueError("ATLAN_BASE_URL and ATLAN_API_KEY must be set as environment variables.")

        _client = AtlanClient(base_url=base_url, api_key=api_key)
    
    return _client
