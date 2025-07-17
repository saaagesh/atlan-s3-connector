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

from atlan_client import get_atlan_client
from pyatlan.model.assets import S3Object, S3Bucket, Connection
from pyatlan.model.enums import AtlanConnectorType
from pyatlan.model.fluent_search import FluentSearch
from pyatlan.model.response import AssetMutationResponse
from pyatlan.errors import NotFoundError

from config import S3Config

logger = logging.getLogger(__name__)

class S3Connector:
    """Main S3 connector class for Atlan integration"""
    
    def __init__(self, s3_config: S3Config):
        self.s3_config = s3_config
        self.connection_qn: Optional[str] = None
        
        # Initialize clients
        self.s3_client = boto3.client(
            's3',
            region_name=s3_config.region,
            aws_access_key_id=s3_config.aws_access_key_id,
            aws_secret_access_key=s3_config.aws_secret_access_key
        )
        self.atlan_client = get_atlan_client()
        
        # Create unique connection qualifier
        self.connection_qualifier = f"s3-{self.s3_config.unique_suffix.lower()}"
        
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
        
        # Convention-based mapping: "TABLE_NAME.csv" -> "TABLE_NAME"
        table_name = object_key[:-4].upper() if object_key.endswith('.csv') else object_key.upper()
        file_mapping = {
            "postgres_table": table_name,
            "snowflake_table": table_name,
            "description": f"Data for table {table_name}"
        }

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
            'file_mapping': file_mapping,
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
        Create S3 assets in Atlan catalog, checking for existing assets first.
        
        Args:
            s3_objects: List of S3 object metadata
            
        Returns:
            List of created or existing asset information
        """
        logger.info(f"Cataloging {len(s3_objects)} S3 objects in Atlan")
        
        # 1. Get or create connection and bucket
        self.connection_qn = await self._get_or_create_s3_connection()
        bucket_asset = await self._get_or_create_s3_bucket()

        # 2. Fetch existing S3 objects in the bucket to avoid re-creating them
        logger.info("Fetching existing S3 objects from Atlan...")
        existing_assets_map = {}
        try:
            request = (
                FluentSearch()
                .where(FluentSearch.asset_type(S3Object))
                .where(S3Object.S3BUCKET_QUALIFIED_NAME.eq(bucket_asset.qualified_name))
            ).to_request()
            for asset in self.atlan_client.asset.search(request):
                existing_assets_map[asset.name] = asset
            logger.info(f"Found {len(existing_assets_map)} existing S3 objects in the bucket.")
        except NotFoundError:
            logger.info("No existing S3 objects found in the bucket.")

        # 3. Prepare batch for new assets and identify already cataloged assets
        asset_batch = []
        cataloged_assets = []
        s3_objects_map = {s3_obj['key']: s3_obj for s3_obj in s3_objects}

        for s3_obj_key, s3_obj_meta in s3_objects_map.items():
            if s3_obj_key in existing_assets_map:
                # Asset already exists, add it to the list to be returned
                existing_asset = existing_assets_map[s3_obj_key]
                cataloged_assets.append({
                    'asset': existing_asset,
                    'metadata': s3_obj_meta,
                    'qualified_name': existing_asset.qualified_name,
                    'guid': existing_asset.guid
                })
            else:
                # Asset is new, add it to the batch for creation
                self._create_s3_object_asset(bucket_asset.qualified_name, s3_obj_meta, asset_batch)

        # 4. Save the batch of new assets, if any
        if asset_batch:
            logger.info(f"Creating {len(asset_batch)} new S3 object assets...")
            response = self.atlan_client.asset.save(asset_batch)
            if response and response.assets_created(asset_type=S3Object):
                created_assets_map = {asset.name: asset for asset in response.assets_created(asset_type=S3Object)}
                # Correlate created assets back to the original s3_objects
                for s3_obj in s3_objects:
                    if s3_obj['key'] in created_assets_map:
                        created_asset = created_assets_map[s3_obj['key']]
                        cataloged_assets.append({
                            'asset': created_asset,
                            'metadata': s3_obj,
                            'qualified_name': created_asset.qualified_name,
                            'guid': created_asset.guid
                        })
                logger.info(f"Successfully created {len(created_assets_map)} new assets.")
            else:
                logger.warning("Creation batch was processed, but no new assets were reported as created.")
        
        logger.info(f"Total cataloged assets (existing + new): {len(cataloged_assets)}")
        return cataloged_assets

    async def _get_or_create_s3_connection(self) -> str:
        """
        Retrieve the qualified name of an existing AWS S3 connection or create a new one.
        """
        connection_name = f"aws-s3-connection-{self.s3_config.unique_suffix.lower()}"
        
        request = (
            FluentSearch()
            .where(FluentSearch.asset_type(Connection))
            .where(Connection.NAME.eq(connection_name))
            .where(Connection.STATUS.eq("ACTIVE"))
        ).to_request()

        search_results = list(self.atlan_client.asset.search(request))
        
        if search_results:
            logger.info(f"Found existing connection: {search_results[0].qualified_name}")
            return search_results[0].qualified_name
        else:
            logger.info(f"Connection '{connection_name}' not found, creating a new one.")
            admin_role_guid = self.atlan_client.role_cache.get_id_for_name("$admin")
            connection = Connection.creator(
                client=self.atlan_client,
                name=connection_name,
                connector_type=AtlanConnectorType.S3,
                admin_roles=[admin_role_guid]
            )
            response = self.atlan_client.asset.save(connection)
            created_connection = response.assets_created(asset_type=Connection)[0]
            logger.info(f"Successfully created new connection: {created_connection.qualified_name}")
            return created_connection.qualified_name

    async def _get_or_create_s3_bucket(self) -> S3Bucket:
        """
        Retrieves an S3 bucket asset from Atlan if it exists, or creates it if it does not.
        """
        unique_bucket_name = f"{self.s3_config.bucket_name}-{self.s3_config.unique_suffix}"
        
        # Attempt to find the asset first
        try:
            request = (
                FluentSearch()
                .where(FluentSearch.asset_type(S3Bucket))
                .where(S3Bucket.NAME.eq(unique_bucket_name))
                .where(S3Bucket.CONNECTION_QUALIFIED_NAME.eq(self.connection_qn))
                .where(S3Bucket.STATUS.eq("ACTIVE"))
            ).to_request()

            search_results = list(self.atlan_client.asset.search(request))
            
            if search_results:
                logger.info(f"Found existing S3 bucket asset: {search_results[0].qualified_name}")
                return search_results[0]
        except NotFoundError:
            # This is expected if the asset doesn't exist
            pass

        # If not found, create it
        logger.info(f"S3 Bucket '{unique_bucket_name}' not found in Atlan. Creating the asset.")
        s3bucket = S3Bucket.creator(
            name=unique_bucket_name,
            connection_qualified_name=self.connection_qn,
            aws_arn=f"arn:aws:s3:::{unique_bucket_name}"
        )
        response = self.atlan_client.asset.save(s3bucket)
        
        # Important: Verify creation and retrieve the full asset
        if response and response.assets_created(asset_type=S3Bucket):
            created_bucket = response.assets_created(asset_type=S3Bucket)[0]
            logger.info(f"Successfully created S3 Bucket asset with GUID: {created_bucket.guid}")
            
            # Re-fetch the asset to ensure it's fully available
            try:
                retrieved_asset = self.atlan_client.asset.get_by_guid(created_bucket.guid, asset_type=S3Bucket)
                logger.info(f"Successfully retrieved created bucket: {retrieved_asset.qualified_name}")
                return retrieved_asset
            except NotFoundError as e:
                logger.error("Failed to retrieve the newly created S3 bucket asset.")
                raise e
        else:
            logger.error("S3 bucket asset creation did not return the expected response.")
            raise RuntimeError("Failed to create S3 bucket asset in Atlan.")

    def _create_s3_object_asset(self, bucket_qualified_name: str, s3_obj: Dict[str, Any], asset_batch: list):
        """
        Creates and adds an S3 object asset to a batch.
        """
        creator = S3Object.creator(
            name=s3_obj['key'],
            s3_bucket_name=self.s3_config.bucket_name,
            s3_bucket_qualified_name=bucket_qualified_name,
            connection_qualified_name=self.connection_qn,
            aws_arn=s3_obj['unique_arn']
        )
        creator.description = s3_obj['file_mapping'].get('description', f"CSV file: {s3_obj['key']}")
        creator.s3_object_size = s3_obj['size']
        creator.s3_object_last_modified_time = s3_obj['last_modified']
        creator.s3_object_content_type = s3_obj['content_type']
        creator.s3_e_tag = s3_obj['etag']
        
        if s3_obj['schema_info']['columns']:
            schema_summary = f"Columns: {', '.join([col['name'] for col in s3_obj['schema_info']['columns']])}"
            # Store schema info in description field initially, AI will update user_description later
            creator.description = schema_summary
            
        asset_batch.append(creator)
    
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
    
    def add_tags_to_asset(self, asset, tags_to_add):
        """
        Add Atlan tags to an asset using the client's add_atlan_tags method
        
        Args:
            asset: The asset object
            tags_to_add: List of tag names to add
        """
        try:
            logger.info(f"Adding Atlan tags to {asset.qualified_name}: {tags_to_add}")
            
            # Use the client's add_atlan_tags method
            result = self.atlan_client.add_atlan_tags(
                asset_type=S3Object,
                qualified_name=asset.qualified_name,
                atlan_tag_names=tags_to_add
            )
            
            logger.info(f"Successfully added Atlan tags to {asset.qualified_name}: {tags_to_add}")
            return result
            
        except Exception as e:
            logger.error(f"Error adding Atlan tags to {asset.qualified_name}: {str(e)}")
            raise
    
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
                
                logger.info(f"Updating asset {asset_key} with AI insights")
                
                # Get the asset to update
                asset = asset_info['asset']
                updates_made = []
                
                # Create an updater for the S3Object
                updater = S3Object.updater(
                    qualified_name=asset.qualified_name,
                    name=asset.name
                )
                
                # Update description if available
                if asset_key in descriptions:
                    ai_description = descriptions[asset_key]
                    # Set AI-generated business description in user_description field
                    updater.user_description = ai_description
                    # Also update main description field
                    updater.description = ai_description
                    updates_made.append("AI description")
                    logger.info(f"Setting AI description for {asset_key}")
                    logger.info(f"AI Description: {ai_description}")
                else:
                    logger.warning(f"No AI description found for {asset_key}")
                
                # PII logic commented out for now
                # if asset_key in pii_classifications:
                #     pii_info = pii_classifications[asset_key]
                #     if pii_info.get('has_pii'):
                #         logger.info(f"PII detected in {asset_key}: {pii_info.get('pii_types', [])}")
                #         logger.info(f"PII confidence: {pii_info.get('confidence', 0):.2f}")
                
                # Apply compliance tags using the new tagging method
                if asset_key in compliance_tags and compliance_tags[asset_key]:
                    try:
                        logger.info(f"Applying compliance tags to {asset_key}: {compliance_tags[asset_key]}")
                        self.add_tags_to_asset(asset, compliance_tags[asset_key])
                        updates_made.append("compliance tags")
                        logger.info(f"Successfully applied compliance tags to {asset_key}")
                    except Exception as tag_error:
                        logger.error(f"Failed to apply compliance tags to {asset_key}: {str(tag_error)}")
                
                # Update the asset
                if updates_made:
                    self.atlan_client.asset.save(updater)
                    logger.info(f"Successfully updated {asset_key} with: {', '.join(updates_made)}")
                else:
                    logger.info(f"No AI insights to update for {asset_key}")
                
            except Exception as e:
                logger.error(f"Failed to update asset {asset_info['metadata']['key']} with AI insights: {str(e)}")
        
        logger.info("Completed updating assets with AI insights")
