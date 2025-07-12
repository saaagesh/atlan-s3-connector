# s3_connector.py
"""
Core S3 Connector implementation for Atlan
Handles S3 object discovery, metadata extraction, and asset creation
"""

import boto3
import pandas as pd
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
import asyncio
from io import StringIO

from pyatlan.client.atlan import AtlanClient
from pyatlan.model.assets import S3Object, S3Bucket
from pyatlan.model.enums import AtlanConnectorType
from pyatlan.model.response import AssetMutationResponse

from config import S3Config, AtlanConfig, FILE_LINEAGE_MAPPING

logger = logging.getLogger(__name__)

class S3Connector:
    """Main S3 connector class for Atlan integration"""
    
    def __init__(self, s3_config: S3Config, atlan_config: AtlanConfig):
        self.s3_config = s3_config
        self.atlan_config = atlan_config
        
        # Initialize clients
        self.s3_client = boto3.client('s3', region_name=s3_config.region)
        self.atlan_client = AtlanClient(
            base_url=atlan_config.base_url,
            api_key=atlan_config.api_key
        )
        
        # Create unique connection qualifier
        self.connection_qualifier = f"s3-{s3_config.unique_suffix.lower()}"
        
    async def discover_s3_objects(self) -> List[Dict[str, Any]]:
        """
        Discover all objects in the S3 bucket
        
        Returns:
            List of S3 object metadata dictionaries
        """
        logger.info(f"Discovering objects in bucket: {self.s3_config.bucket_name}")
        
        try:
            # List all objects in the bucket
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_config.bucket_name
            )
            
            objects = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Only process CSV files for this use case
                    if obj['Key'].endswith('.csv'):
                        object_metadata = await self._extract_object_metadata(obj)
                        objects.append(object_metadata)
            
            logger.info(f"Discovered {len(objects)} CSV objects")
            return objects
            
        except Exception as e:
            logger.error(f"Error discovering S3 objects: {str(e)}")
            raise
    
    async def _extract_object_metadata(self, s3_object: Dict) -> Dict[str, Any]:
        """
        Extract comprehensive metadata from S3 object
        
        Args:
            s3_object: S3 object information from boto3
            
        Returns:
            Enhanced metadata dictionary
        """
        object_key = s3_object['Key']
        
        # Get object head for additional metadata
        head_response = self.s3_client.head_object(
            Bucket=self.s3_config.bucket_name,
            Key=object_key
        )
        
        # Sample the CSV for schema inference
        schema_info = await self._infer_csv_schema(object_key)
        
        # Generate unique ARN for Atlan
        unique_arn = f"arn:aws:s3:::{self.s3_config.bucket_name}-{self.s3_config.unique_suffix}/{object_key}"
        
        metadata = {
            'key': object_key,
            'bucket': self.s3_config.bucket_name,
            'size': s3_object['Size'],
            'last_modified': s3_object['LastModified'],
            'etag': s3_object['ETag'].strip('"'),
            'storage_class': s3_object.get('StorageClass', 'STANDARD'),
            'content_type': head_response.get('ContentType', 'text/csv'),
            'unique_arn': unique_arn,
            'schema_info': schema_info,
            'file_mapping': FILE_LINEAGE_MAPPING.get(object_key, {}),
            'qualified_name': f"default/s3/{self.connection_qualifier}/{self.s3_config.bucket_name}/{object_key}"
        }
        
        return metadata
    
    async def _infer_csv_schema(self, object_key: str) -> Dict[str, Any]:
        """
        Infer schema from CSV file by reading first few rows
        
        Args:
            object_key: S3 object key
            
        Returns:
            Schema information dictionary
        """
        try:
            # Read first 1000 bytes to infer schema
            response = self.s3_client.get_object(
                Bucket=self.s3_config.bucket_name,
                Key=object_key,
                Range='bytes=0-999'
            )
            
            content = response['Body'].read().decode('utf-8')
            
            # Use pandas to infer schema
            df = pd.read_csv(StringIO(content), nrows=5)
            
            columns = []
            for col_name in df.columns:
                col_type = str(df[col_name].dtype)
                col_info = {
                    'name': col_name,
                    'type': col_type,
                    'sample_values': df[col_name].dropna().head(3).tolist()
                }
                columns.append(col_info)
            
            schema_info = {
                'columns': columns,
                'row_count_sample': len(df),
                'column_count': len(columns),
                'inferred_at': datetime.now().isoformat()
            }
            
            return schema_info
            
        except Exception as e:
            logger.warning(f"Could not infer schema for {object_key}: {str(e)}")
            return {
                'columns': [],
                'row_count_sample': 0,
                'column_count': 0,
                'error': str(e)
            }
    
    async def catalog_s3_objects(self, s3_objects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Create S3 assets in Atlan catalog
        
        Args:
            s3_objects: List of S3 object metadata
            
        Returns:
            List of created asset information
        """
        logger.info(f"Cataloging {len(s3_objects)} S3 objects in Atlan")
        
        cataloged_assets = []
        
        # First, ensure S3 connection exists
        await self._ensure_s3_connection()
        
        # Create S3 bucket asset
        bucket_asset = await self._create_s3_bucket_asset()
        
        # Create individual S3 object assets
        for s3_obj in s3_objects:
            try:
                asset_info = await self._create_s3_object_asset(s3_obj, bucket_asset)
                cataloged_assets.append(asset_info)
                logger.info(f"Cataloged asset: {s3_obj['key']}")
                
            except Exception as e:
                logger.error(f"Failed to catalog {s3_obj['key']}: {str(e)}")
        
        logger.info(f"Successfully cataloged {len(cataloged_assets)} assets")
        return cataloged_assets
    
    async def _ensure_s3_connection(self) -> None:
        """Ensure S3 connection exists in Atlan"""
        # This would typically involve creating a connection
        # For this challenge, we'll assume it's created manually
        # In production, you'd use the Atlan SDK to create the connection
        pass
    
    async def _create_s3_bucket_asset(self) -> S3Bucket:
        """Create S3 bucket asset in Atlan"""
        try:
            unique_bucket_name = f"{self.s3_config.bucket_name}-{self.s3_config.unique_suffix}"
            
            bucket = S3Bucket.create(
                name=unique_bucket_name,
                connection_qualified_name=f"default/s3/{self.connection_qualifier}"
            )
            
            bucket.aws_arn = f"arn:aws:s3:::{unique_bucket_name}"
            bucket.description = f"S3 bucket for Atlan tech challenge - {self.s3_config.unique_suffix}"
            
            # Create the bucket asset
            response = self.atlan_client.asset.save(bucket)
            
            if response.created_assets:
                logger.info(f"Created S3 bucket asset: {unique_bucket_name}")
                return response.created_assets[0]
            else:
                logger.warning("No bucket asset created in response")
                return bucket
                
        except Exception as e:
            logger.error(f"Failed to create S3 bucket asset: {str(e)}")
            raise
    
    async def _create_s3_object_asset(self, s3_obj: Dict[str, Any], bucket_asset: S3Bucket) -> Dict[str, Any]:
        """
        Create individual S3 object asset in Atlan
        
        Args:
            s3_obj: S3 object metadata
            bucket_asset: Parent S3 bucket asset
            
        Returns:
            Created asset information
        """
        try:
            # Create S3 object asset
            s3_object = S3Object.create(
                name=s3_obj['key'],
                s3_bucket_qualified_name=bucket_asset.qualified_name
            )
            
            # Set basic properties
            s3_object.aws_arn = s3_obj['unique_arn']
            s3_object.description = s3_obj['file_mapping'].get('description', f"CSV file: {s3_obj['key']}")
            s3_object.s3_object_size = s3_obj['size']
            s3_object.s3_object_last_modified_time = s3_obj['last_modified']
            s3_object.s3_object_content_type = s3_obj['content_type']
            s3_object.s3_object_e_tag = s3_obj['etag']
            
            # Add schema information as custom metadata
            if s3_obj['schema_info']['columns']:
                schema_summary = f"Columns: {', '.join([col['name'] for col in s3_obj['schema_info']['columns']])}"
                s3_object.user_description = schema_summary
            
            # Add custom attributes for tracking
            s3_object.source_created_by = "Atlan S3 Connector"
            s3_object.source_updated_by = "Atlan S3 Connector"
            
            # Create the asset
            response = self.atlan_client.asset.save(s3_object)
            
            if response.created_assets:
                created_asset = response.created_assets[0]
                logger.info(f"Created S3 object asset: {s3_obj['key']}")
                
                return {
                    'asset': created_asset,
                    'metadata': s3_obj,
                    'qualified_name': created_asset.qualified_name,
                    'guid': created_asset.guid
                }
            else:
                raise Exception("No asset created in response")
                
        except Exception as e:
            logger.error(f"Failed to create S3 object asset for {s3_obj['key']}: {str(e)}")
            raise
    
    async def get_modified_objects_since(self, timestamp: datetime) -> List[Dict[str, Any]]:
        """
        Get S3 objects modified since a given timestamp
        
        Args:
            timestamp: Timestamp to check against
            
        Returns:
            List of modified objects
        """
        all_objects = await self.discover_s3_objects()
        
        modified_objects = [
            obj for obj in all_objects
            if obj['last_modified'] > timestamp
        ]
        
        logger.info(f"Found {len(modified_objects)} objects modified since {timestamp}")
        return modified_objects
    
    async def update_assets_with_ai_insights(
        self, 
        assets: List[Dict[str, Any]], 
        descriptions: Dict[str, str],
        pii_classifications: Dict[str, Dict],
        compliance_tags: Dict[str, List[str]]
    ) -> None:
        """
        Update assets with AI-generated insights
        
        Args:
            assets: List of asset information
            descriptions: AI-generated descriptions
            pii_classifications: PII classification results
            compliance_tags: Compliance tags
        """
        logger.info("Updating assets with AI insights")
        
        for asset_info in assets:
            try:
                asset_key = asset_info['metadata']['key']
                
                # Get the asset to update
                asset = asset_info['asset']
                
                # Update description if available
                if asset_key in descriptions:
                    asset.description = descriptions[asset_key]
                
                # Add PII classification tags
                if asset_key in pii_classifications:
                    pii_info = pii_classifications[asset_key]
                    
                    # Add classification as custom metadata
                    if pii_info.get('has_pii'):
                        asset.classifications = ['PII_DATA']
                    
                    # Add confidence score
                    asset.user_description = f"{asset.user_description or ''} | PII Confidence: {pii_info.get('confidence', 0):.2f}"
                
                # Add compliance tags
                if asset_key in compliance_tags:
                    for tag in compliance_tags[asset_key]:
                        if not hasattr(asset, 'assigned_terms'):
                            asset.assigned_terms = []
                        asset.assigned_terms.append(tag)
                
                # Update the asset
                self.atlan_client.asset.save(asset)
                
            except Exception as e:
                logger.error(f"Failed to update asset {asset_info['metadata']['key']} with AI insights: {str(e)}")
        
        logger.info("Completed updating assets with AI insights")
