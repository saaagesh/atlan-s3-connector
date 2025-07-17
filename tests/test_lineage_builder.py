import pytest
from unittest.mock import MagicMock, AsyncMock

from lineage_builder import LineageBuilder
from config import ConnectionConfig, AtlanConfig

# Mock configurations
@pytest.fixture
def mock_connection_config():
    """Provides a mock ConnectionConfig."""
    return ConnectionConfig(
        postgres_connection_qn="default/postgres/123",
        snowflake_connection_qn="default/snowflake/456"
    )

@pytest.fixture
def mock_atlan_config():
    """Provides a mock AtlanConfig."""
    return AtlanConfig(
        base_url="https://fake-tenant.atlan.com",
        api_key="fake-api-key"
    )

# Mock for the AtlanClient
@pytest.fixture
def mock_atlan_client(mocker):
    """Mocks the AtlanClient and its search capabilities."""
    mock_client = MagicMock()
    
    # Mock the asset search to be an async function
    mock_client.asset.search = AsyncMock()
    
    # Patch the AtlanClient instantiation in the lineage_builder module
    mocker.patch("lineage_builder.AtlanClient", return_value=mock_client)
    
    return mock_client

# Sample S3 asset data for tests
@pytest.fixture
def sample_s3_assets():
    """Provides a list of sample S3 assets for testing."""
    key = "CUSTOMERS.csv"
    table_name = key[:-4].upper()
    return [
        {
            "metadata": {
                "key": key,
                "file_mapping": {
                    "postgres_table": table_name,
                    "snowflake_table": table_name,
                    "description": f"Data for table {table_name}"
                }
            }
        }
    ]

# --- Test Cases ---

@pytest.mark.asyncio
async def test_lineage_builder_initialization(mock_atlan_client, mock_connection_config, mock_atlan_config):
    """
    Tests if the LineageBuilder initializes correctly and sets up the Atlan client.
    """
    # Action
    builder = LineageBuilder(mock_connection_config, mock_atlan_config)
    
    # Assert
    assert builder.atlan_client is not None
    assert builder.connection_config == mock_connection_config
    # Verify that the AtlanClient was called with the correct config
    from lineage_builder import AtlanClient
    AtlanClient.assert_called_once_with(
        base_url=mock_atlan_config.base_url,
        api_key=mock_atlan_config.api_key
    )

@pytest.mark.asyncio
async def test_find_postgres_source_success(mock_connection_config, mock_atlan_config, sample_s3_assets):
    """
    Tests if _find_postgres_source successfully finds a mocked Postgres table asset.
    """
    # Setup: Configure the mock client to return a fake asset
    mock_asset_return = {
        "name": "CUSTOMERS",
        "qualified_name": "default/postgres/123/CUSTOMERS",
        "guid": "guid-postgres-customers"
    }

    # Action
    builder = LineageBuilder(mock_connection_config, mock_atlan_config)
    # Mock the internal _get_asset_by_qualified_name to avoid further API calls in this unit test
    builder._get_asset_by_qualified_name = AsyncMock(return_value=mock_asset_return)
    
    postgres_table = await builder._find_postgres_source(sample_s3_assets[0])
    
    # Assert
    assert postgres_table is not None
    assert postgres_table['name'] == "CUSTOMERS"
    assert postgres_table['guid'] == "guid-postgres-customers"
    # Verify that the client's search method was called with the correct query
    builder._get_asset_by_qualified_name.assert_called_once_with("default/postgres/123/CUSTOMERS")

@pytest.mark.asyncio
async def test_find_postgres_source_not_found(mock_connection_config, mock_atlan_config, sample_s3_assets):
    """
    Tests the behavior of _find_postgres_source when no asset is found in Atlan.
    """
    # Action
    builder = LineageBuilder(mock_connection_config, mock_atlan_config)
    builder._get_asset_by_qualified_name = AsyncMock(return_value=None)
    
    postgres_table = await builder._find_postgres_source(sample_s3_assets[0])
    
    # Assert
    assert postgres_table is None
    builder._get_asset_by_qualified_name.assert_called_once_with("default/postgres/123/CUSTOMERS")

if __name__ == "__main__":
    pytest.main()
