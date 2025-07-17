import boto3
import pytest
from moto import mock_aws
from s3_connector import S3Connector
from config import S3Config, AtlanConfig
from unittest.mock import MagicMock, AsyncMock

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

@pytest.fixture
def s3_client(aws_credentials):
    """Yield a mock S3 client."""
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")

@pytest.fixture
def mock_atlan_config():
    """Provides a mock AtlanConfig."""
    return AtlanConfig(
        base_url="https://fake-tenant.atlan.com",
        api_key="fake-api-key"
    )

@pytest.fixture
def mock_atlan_client(mocker):
    """Mocks the AtlanClient."""
    mock_client = MagicMock()
    mocker.patch("s3_connector.AtlanClient", return_value=mock_client)
    return mock_client

def test_s3_connector_initialization(s3_client, mock_atlan_config, mock_atlan_client):
    """
    Tests if the S3Connector initializes correctly.
    """
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)
    s3_config = S3Config(bucket_name=bucket_name)
    
    connector = S3Connector(s3_config, mock_atlan_config)
    
    assert connector.s3_config.bucket_name == bucket_name
    assert connector.atlan_config == mock_atlan_config

@pytest.mark.asyncio
async def test_discover_s3_objects(s3_client, mock_atlan_config, mock_atlan_client):
    """
    Tests the discover_s3_objects method to ensure it correctly lists .csv files.
    """
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.put_object(Bucket=bucket_name, Key="file1.csv", Body="data")
    s3_client.put_object(Bucket=bucket_name, Key="file2.csv", Body="data")
    s3_client.put_object(Bucket=bucket_name, Key="other.txt", Body="data")
    
    s3_config = S3Config(bucket_name=bucket_name)
    connector = S3Connector(s3_config, mock_atlan_config)
    
    # Mock the internal metadata extraction to isolate the discovery logic
    connector._extract_object_metadata = AsyncMock(return_value={"key": "mocked"})
    
    s3_objects = await connector.discover_s3_objects()
    
    assert len(s3_objects) == 2
    assert connector._extract_object_metadata.call_count == 2

@pytest.mark.asyncio
async def test_discover_s3_objects_empty_bucket(s3_client, mock_atlan_config, mock_atlan_client):
    """
    Tests discover_s3_objects on an empty bucket.
    """
    bucket_name = "empty-bucket"
    s3_client.create_bucket(Bucket=bucket_name)
    
    s3_config = S3Config(bucket_name=bucket_name)
    connector = S3Connector(s3_config, mock_atlan_config)
    
    s3_objects = await connector.discover_s3_objects()
    
    assert len(s3_objects) == 0

if __name__ == "__main__":
    pytest.main()
