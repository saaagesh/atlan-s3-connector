# modern_ui/app.py
"""
Modern Flask UI for Atlan S3 Connector with React Frontend
"""

import os
import asyncio
from flask import Flask, render_template, request, jsonify, send_from_directory
from dotenv import load_dotenv

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)

# All imports are now local to the modern_ui directory
from s3_connector import S3Connector
from atlan_client import get_atlan_client
from pyatlan.model.assets import S3Object, Table, Column
from pyatlan.model.fluent_search import FluentSearch
from config import S3Config, AIConfig, ConnectionConfig
from flask_ai_enhancer import AIEnhancer

app = Flask(__name__, static_folder='frontend/dist', static_url_path='')

# Serve React App
@app.route('/')
def serve_react_app():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/<path:path>')
def serve_react_assets(path):
    if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    else:
        return send_from_directory(app.static_folder, 'index.html')

# --- Configuration ---
s3_config = S3Config()
ai_config = AIConfig()
connection_config = ConnectionConfig()
atlan_client = get_atlan_client()

@app.route('/api/assets_by_source', methods=['POST'])
async def get_assets_by_source():
    """Fetch assets for a given source connection."""
    source_name = request.json.get('source')
    if not source_name:
        return jsonify({"success": False, "error": "Source name is required."}), 400

    try:
        if "s3" in source_name:
            s3_connector = S3Connector(s3_config)
            s3_objects = await s3_connector.discover_s3_objects()
            # Include existing metadata information in the response
            assets = []
            for obj in s3_objects:
                asset_info = {
                    "name": obj['key'], 
                    "qualified_name": obj['qualified_name'], 
                    "type": "s3"
                }
                
                # Add metadata information if available
                if 'existing_metadata' in obj and obj['existing_metadata']:
                    metadata = obj['existing_metadata']
                    asset_info.update({
                        "has_description": bool(metadata.get('description') or metadata.get('user_description')),
                        "description": metadata.get('user_description') or metadata.get('description') or "",
                        "user_description": metadata.get('user_description') or metadata.get('description') or "",
                        "has_owners": bool(metadata.get('owner_users') or metadata.get('owner_groups')),
                        "has_readme": bool(metadata.get('readme')),
                        "guid": metadata.get('guid', ""),
                        "has_columns": bool(metadata.get('columns'))
                    })
                
                assets.append(asset_info)
        else:
            # Handle DB connections (Postgres, Snowflake)
            connection_name = getattr(connection_config, f"{source_name.split('-')[0]}_connection_name")
            search_request = (
                FluentSearch()
                .where(FluentSearch.asset_type(Table))
                .where(FluentSearch.active_assets())
                .where(Table.CONNECTION_NAME.eq(connection_name))
                .include_on_results(Table.USER_DESCRIPTION)
                .include_on_results(Table.DESCRIPTION)
                .include_on_results(Table.OWNER_USERS)
                .include_on_results(Table.OWNER_GROUPS)
                .include_on_results(Table.README)
            ).to_request()
            
            results = atlan_client.asset.search(search_request)
            assets = []
            for r in results:
                asset_info = {
                    "name": r.name, 
                    "qualified_name": r.qualified_name, 
                    "type": "table",
                    "has_description": bool(r.user_description or r.description),
                    "description": r.user_description or r.description or "",
                    "has_owners": bool(getattr(r, 'owner_users', None) or getattr(r, 'owner_groups', None)),
                    "has_readme": bool(getattr(r, 'readme', None)),
                    "guid": r.guid
                }
                assets.append(asset_info)

        return jsonify({"success": True, "assets": assets})
    except Exception as e:
        app.logger.error(f"Error fetching assets for {source_name}: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/columns', methods=['POST'])
async def get_columns():
    """Get columns for a given asset."""
    asset_qn = request.json.get('asset_qualified_name')
    source_type = request.json.get('source_type')
    refresh = request.json.get('refresh', False)  # New parameter to force refresh

    try:
        # This logic works for both tables and S3 objects, as columns are linked via qualified_name
        asset_name = asset_qn.split('/')[-1]
        app.logger.info(f"Processing asset: {asset_name} with qualified name: {asset_qn}")
        
        # Explicitly search for columns related to the asset's qualified name.
        search_request = (
            FluentSearch()
            .where(FluentSearch.asset_type(Column))
            .where(Column.TABLE_QUALIFIED_NAME.eq(asset_qn)) # This works for S3 objects as well
            .include_on_results(Column.USER_DESCRIPTION)
            .include_on_results(Column.DESCRIPTION)
            .include_on_results(Column.DATA_TYPE)
        ).to_request()

        all_columns = list(atlan_client.asset.search(search_request))
        app.logger.info(f"Found {len(all_columns)} columns for asset {asset_qn}")
        
        columns = []
        for col in all_columns:
            if isinstance(col, Column):
                description = col.user_description or col.description or ""
                columns.append({
                    "name": col.name,
                    "description": description,
                    "has_description": bool(description),
                    "qualified_name": col.qualified_name,
                    "guid": col.guid,
                    "type": getattr(col, 'data_type', '')
                })
        
        columns.sort(key=lambda x: x["name"])

        app.logger.info(f"Returning {len(columns)} columns with descriptions")
        return jsonify({"success": True, "columns": columns})
    except Exception as e:
        app.logger.error(f"Error fetching columns for {asset_qn}: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/enhance_columns', methods=['POST'])
async def enhance_columns():
    """Generate AI descriptions for a list of columns."""
    try:
        data = request.json
        columns_to_enhance = data.get('columns', [])
        asset_qn = data.get('asset_qualified_name', 'Unknown Asset')
        
        app.logger.info(f"Starting AI description generation for {len(columns_to_enhance)} columns in asset: {asset_qn}.")
        
        ai_enhancer = AIEnhancer(ai_config)
        descriptions = await ai_enhancer.generate_column_level_descriptions(data)
        
        app.logger.info(f"Successfully generated {len(descriptions)} descriptions.")
        
        return jsonify({"success": True, "descriptions": descriptions})
    except Exception as e:
        app.logger.error(f"Error enhancing columns: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/save_descriptions', methods=['POST'])
async def save_descriptions():
    """Save updated column descriptions to Atlan."""
    data = request.json
    asset_qn = data.get('asset_qualified_name')
    columns_to_update = data.get('columns')
    
    try:
        # Fetch all column assets for the given table to get their qualified names
        search_request = (
            FluentSearch()
            .where(FluentSearch.asset_type(Column))
            .where(Column.TABLE_QUALIFIED_NAME.eq(asset_qn))
        ).to_request()
        
        # Get all columns in one go
        all_columns = list(atlan_client.asset.search(search_request))
        app.logger.info(f"Found {len(all_columns)} columns for table {asset_qn}")
        
        # Create a map of column name to column object
        atlan_columns = {}
        for c in all_columns:
            if isinstance(c, Column):
                atlan_columns[c.name] = c

        batch = []
        for col_data in columns_to_update:
            col_name = col_data.get('name')
            col_description = col_data.get('description')
            
            if col_name in atlan_columns and col_description:
                col = atlan_columns[col_name]
                app.logger.info(f"Updating column {col_name} with description: '{col_description}'")
                app.logger.info(f"  Current description: '{col.description or 'None'}'")
                app.logger.info(f"  Current user_description: '{col.user_description or 'None'}'")
                
                updater = Column.updater(
                    qualified_name=col.qualified_name,
                    name=col_name
                )
                updater.user_description = col_description
                batch.append(updater)

        if batch:
            app.logger.info(f"Saving {len(batch)} column descriptions to Atlan for asset {asset_qn}.")
            atlan_client.asset.save(batch)
            app.logger.info("Successfully saved descriptions.")
            return jsonify({"success": True, "message": f"Updated {len(batch)} columns."})
        else:
            return jsonify({"success": False, "error": "No columns with valid descriptions to update."})

    except Exception as e:
        app.logger.error(f"Error saving descriptions for {asset_qn}: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/fix_snowflake_columns', methods=['POST'])
async def fix_snowflake_columns():
    """Special endpoint to fix Snowflake column descriptions."""
    try:
        data = request.json
        table_name = data.get('table_name', 'CUSTOMERS')
        
        app.logger.info(f"Fixing column descriptions for Snowflake table: {table_name}")
        
        # Get the Snowflake connection name
        snowflake_connection = connection_config.snowflake_connection_name
        app.logger.info(f"Using Snowflake connection: {snowflake_connection}")
        
        # Find the table in the Snowflake connection
        table_request = (
            FluentSearch()
            .where(FluentSearch.asset_type(Table))
            .where(FluentSearch.active_assets())
            .where(Table.CONNECTION_NAME.eq(snowflake_connection))
            .where(Table.NAME.eq(table_name))
        ).to_request()
        
        table_results = list(atlan_client.asset.search(table_request))
        
        if not table_results:
            return jsonify({"success": False, "error": f"Table {table_name} not found in Snowflake connection."}), 404
        
        table = table_results[0]
        app.logger.info(f"Found table: {table.name} ({table.qualified_name})")
        
        # Search for columns of the table
        search_request = (
            FluentSearch()
            .where(FluentSearch.asset_type(Column))
            .where(Column.TABLE_QUALIFIED_NAME.eq(table.qualified_name))
        ).to_request()
        
        column_results = list(atlan_client.asset.search(search_request))
        app.logger.info(f"Found {len(column_results)} columns for table {table.name}")
        
        # Create a map of column descriptions based on the screenshot
        column_descriptions = {
            "ADDRESS": "Customer's street address.",
            "CITY": "Customer's city of residence.",
            "CONTACTNAME": "Name of the customer contact person.",
            "COUNTRY": "NEW COUNTRY",
            "CUSTOMERID": "Unique identifier for each customer.",
            "CUSTOMERNAME": "Customer's company or individual name.",
            "POSTALCODE": "Customer's postal code."
        }
        
        # Update columns with descriptions
        batch = []
        updated_columns = []
        
        for col in column_results:
            if isinstance(col, Column) and col.name in column_descriptions:
                description = column_descriptions[col.name]
                app.logger.info(f"Updating column {col.name} with description: '{description}'")
                app.logger.info(f"  Current description: '{col.description or 'None'}'")
                app.logger.info(f"  Current user_description: '{col.user_description or 'None'}'")
                
                updater = Column.updater(
                    qualified_name=col.qualified_name,
                    name=col.name
                )
                updater.user_description = description
                batch.append(updater)
                
                updated_columns.append({
                    "name": col.name,
                    "description": description,
                    "qualified_name": col.qualified_name
                })
        
        if batch:
            app.logger.info(f"Saving {len(batch)} column descriptions to Atlan.")
            atlan_client.asset.save(batch)
            app.logger.info("Successfully saved descriptions.")
            
            return jsonify({
                "success": True, 
                "message": f"Updated {len(batch)} columns.",
                "updated_columns": updated_columns
            })
        else:
            return jsonify({"success": False, "error": "No columns to update."})
        
    except Exception as e:
        app.logger.error(f"Error fixing Snowflake column descriptions: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/generate_asset_description', methods=['POST'])
async def generate_asset_description():
    """Generate AI description for an asset."""
    try:
        data = request.json
        asset_qn = data.get('asset_qualified_name')
        asset_name = data.get('asset_name')
        asset_type = data.get('asset_type')
        
        app.logger.info(f"Starting AI description generation for asset: {asset_name} ({asset_qn})")
        
        ai_enhancer = AIEnhancer(ai_config)
        
        # Create a payload with asset information
        asset_payload = {
            'name': asset_name,
            'qualified_name': asset_qn,
            'type': asset_type
        }
        
        # For S3 objects, we need additional context
        if asset_type == 's3':
            # Get the object metadata
            s3_connector = S3Connector(s3_config)
            key = asset_qn.split('/')[-1]
            head_response = s3_connector.s3_client.head_object(Bucket=s3_config.bucket_name, Key=key)
            s3_object_details = {'Key': key, 'Size': head_response['ContentLength'], 'LastModified': head_response['LastModified'], 'ETag': head_response['ETag']}
            metadata = await s3_connector._extract_object_metadata(s3_object_details)
            
            # Add schema information to the payload
            asset_payload['schema_info'] = metadata.get('schema_info', {})
        
        # Generate description
        description = await ai_enhancer.generate_asset_description(asset_payload)
        
        app.logger.info(f"Successfully generated description for {asset_name}")
        
        return jsonify({
            "success": True, 
            "description": description,
            "asset_name": asset_name,
            "asset_qualified_name": asset_qn
        })
    except Exception as e:
        app.logger.error(f"Error generating asset description: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/save_asset_description', methods=['POST'])
async def save_asset_description():
    """Save asset description to Atlan."""
    try:
        data = request.json
        asset_qn = data.get('asset_qualified_name')
        asset_type = data.get('asset_type')
        description = data.get('description')
        
        if not asset_qn or not description:
            return jsonify({"success": False, "error": "Asset qualified name and description are required."}), 400
        
        app.logger.info(f"Saving description for asset: {asset_qn}")
        
        # Update the asset based on its type
        if asset_type == 's3':
            # Update S3 object
            updater = S3Object.updater(
                qualified_name=asset_qn,
                name=asset_qn.split('/')[-1]
            )
            updater.user_description = description
            updater.description = description
        else:
            # Update table
            updater = Table.updater(
                qualified_name=asset_qn,
                name=asset_qn.split('/')[-1]
            )
            updater.user_description = description
            updater.description = description
        
        # Save the update
        atlan_client.asset.save(updater)
        
        app.logger.info(f"Successfully saved description for {asset_qn}")
        
        return jsonify({
            "success": True, 
            "message": "Description saved successfully."
        })
    except Exception as e:
        app.logger.error(f"Error saving asset description: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/generate_all_metadata', methods=['POST'])
async def generate_all_metadata():
    """Generate all metadata for an asset (description, column descriptions)."""
    try:
        data = request.json
        asset_qn = data.get('asset_qualified_name')
        asset_name = data.get('asset_name')
        asset_type = data.get('asset_type')
        
        app.logger.info(f"Starting comprehensive metadata generation for asset: {asset_name} ({asset_qn})")
        
        ai_enhancer = AIEnhancer(ai_config)
        
        # Step 1: Generate asset description
        asset_payload = {
            'name': asset_name,
            'qualified_name': asset_qn,
            'type': asset_type
        }
        
        # For S3 objects, we need additional context
        columns = []
        if asset_type == 's3':
            # Get the object metadata
            s3_connector = S3Connector(s3_config)
            key = asset_qn.split('/')[-1]
            head_response = s3_connector.s3_client.head_object(Bucket=s3_config.bucket_name, Key=key)
            s3_object_details = {'Key': key, 'Size': head_response['ContentLength'], 'LastModified': head_response['LastModified'], 'ETag': head_response['ETag']}
            metadata = await s3_connector._extract_object_metadata(s3_object_details)
            
            # Add schema information to the payload
            asset_payload['schema_info'] = metadata.get('schema_info', {})
            
            # Get columns for column-level descriptions
            columns_data = metadata.get('schema_info', {}).get('columns', [])
            columns = [{"name": c['name'], "type": c.get('type', '')} for c in columns_data]
        else:
            # Get columns for the table
            search_request = (
                FluentSearch()
                .where(FluentSearch.asset_type(Column))
                .where(Column.TABLE_QUALIFIED_NAME.eq(asset_qn))
            ).to_request()
            
            results = atlan_client.asset.search(search_request)
            columns = [{"name": c.name, "type": getattr(c, 'data_type', '')} for c in results if isinstance(c, Column)]
        
        # Generate asset description
        asset_description = await ai_enhancer.generate_asset_description(asset_payload)
        
        # Step 2: Generate column descriptions
        column_descriptions = []
        if columns:
            column_payload = {
                'asset_qualified_name': asset_qn,
                'asset_name': asset_name,
                'columns': columns
            }
            column_descriptions = await ai_enhancer.generate_column_level_descriptions(column_payload)
        
        app.logger.info(f"Successfully generated comprehensive metadata for {asset_name}")
        
        return jsonify({
            "success": True, 
            "asset_description": asset_description,
            "column_descriptions": column_descriptions,
            "asset_name": asset_name,
            "asset_qualified_name": asset_qn
        })
    except Exception as e:
        app.logger.error(f"Error generating comprehensive metadata: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/get_asset_details', methods=['POST'])
async def get_asset_details():
    """Get detailed information about an asset including README and owners."""
    try:
        data = request.json
        asset_qn = data.get('asset_qualified_name')
        asset_type = data.get('asset_type')
        
        if not asset_qn:
            return jsonify({"success": False, "error": "Asset qualified name is required."}), 400
        
        app.logger.info(f"Fetching detailed information for asset: {asset_qn}")
        
        # Get the asset based on its type
        if asset_type == 's3':
            # Get S3 object
            request_obj = (
                FluentSearch()
                .where(FluentSearch.asset_type(S3Object))
                .where(S3Object.QUALIFIED_NAME.eq(asset_qn))
                .where(FluentSearch.active_assets())
            ).to_request()
            
            results = list(atlan_client.asset.search(request_obj))
            if not results:
                return jsonify({"success": False, "error": "Asset not found."}), 404
            
            asset = results[0]
        else:
            # Get table
            request_obj = (
                FluentSearch()
                .where(FluentSearch.asset_type(Table))
                .where(Table.QUALIFIED_NAME.eq(asset_qn))
                .where(FluentSearch.active_assets())
            ).to_request()
            
            results = list(atlan_client.asset.search(request_obj))
            if not results:
                return jsonify({"success": False, "error": "Asset not found."}), 404
            
            asset = results[0]
        
        # Extract detailed information
        details = {
            "name": asset.name,
            "qualified_name": asset.qualified_name,
            "guid": asset.guid,
            "description": asset.user_description or asset.description or "",
            "readme": getattr(asset, 'readme', None) or "",
            "owner_users": getattr(asset, 'owner_users', []),
            "owner_groups": getattr(asset, 'owner_groups', []),
            "certificate_status": getattr(asset, 'certificate_status', None),
            "certificate_status_message": getattr(asset, 'certificate_status_message', None),
            "announcement_title": getattr(asset, 'announcement_title', None),
            "announcement_message": getattr(asset, 'announcement_message', None),
            "created_by": getattr(asset, 'created_by', None),
            "updated_by": getattr(asset, 'updated_by', None),
            "create_time": getattr(asset, 'create_time', None),
            "update_time": getattr(asset, 'update_time', None)
        }
        
        app.logger.info(f"Successfully retrieved details for {asset.name}")
        
        return jsonify({
            "success": True, 
            "details": details
        })
    except Exception as e:
        app.logger.error(f"Error retrieving asset details: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/update_asset_readme', methods=['POST'])
async def update_asset_readme():
    """Update README information for an asset."""
    try:
        data = request.json
        asset_qn = data.get('asset_qualified_name')
        asset_type = data.get('asset_type')
        readme = data.get('readme')
        
        if not asset_qn:
            return jsonify({"success": False, "error": "Asset qualified name is required."}), 400
        
        app.logger.info(f"Updating README for asset: {asset_qn}")
        
        # Update the asset based on its type
        if asset_type == 's3':
            # Update S3 object
            updater = S3Object.updater(
                qualified_name=asset_qn,
                name=asset_qn.split('/')[-1]
            )
            updater.readme = readme
        else:
            # Update table
            updater = Table.updater(
                qualified_name=asset_qn,
                name=asset_qn.split('/')[-1]
            )
            updater.readme = readme
        
        # Save the update
        atlan_client.asset.save(updater)
        
        app.logger.info(f"Successfully updated README for {asset_qn}")
        
        return jsonify({
            "success": True, 
            "message": "README updated successfully."
        })
    except Exception as e:
        app.logger.error(f"Error updating README: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/debug_asset', methods=['POST'])
async def debug_asset():
    """Debug endpoint to fetch and display asset metadata."""
    try:
        data = request.json
        asset_qn = data.get('asset_qualified_name')
        asset_type = data.get('asset_type', 'table')
        
        app.logger.info(f"Debugging asset: {asset_qn} (type: {asset_type})")
        
        if asset_type == 'table':
            # Get columns for the table
            search_request = (
                FluentSearch()
                .where(FluentSearch.asset_type(Column))
                .where(Column.TABLE_QUALIFIED_NAME.eq(asset_qn))
            ).to_request()
            
            results = list(atlan_client.asset.search(search_request))
            app.logger.info(f"Found {len(results)} columns for table {asset_qn}")
            
            columns_debug = []
            for col in results:
                if isinstance(col, Column):
                    columns_debug.append({
                        "name": col.name,
                        "qualified_name": col.qualified_name,
                        "guid": col.guid,
                        "description": col.description or "",
                        "user_description": col.user_description or "",
                        "data_type": getattr(col, 'data_type', ''),
                        "has_description": bool(col.user_description or col.description)
                    })
            
            # Get the table itself
            table_request = (
                FluentSearch()
                .where(FluentSearch.asset_type(Table))
                .where(Table.QUALIFIED_NAME.eq(asset_qn))
            ).to_request()
            
            table_results = list(atlan_client.asset.search(table_request))
            table_info = {}
            if table_results:
                table = table_results[0]
                table_info = {
                    "name": table.name,
                    "qualified_name": table.qualified_name,
                    "guid": table.guid,
                    "description": table.description or "",
                    "user_description": table.user_description or ""
                }
            
            return jsonify({
                "success": True,
                "table_info": table_info,
                "columns": columns_debug,
                "column_count": len(columns_debug)
            })
        else:
            # Create S3 connector
            s3_connector = S3Connector(s3_config)
            
            # Get existing metadata
            asset_name = asset_qn.split('/')[-1]
            metadata = await s3_connector._get_existing_asset_metadata(asset_name)
            
            # Try direct search for the asset
            request_obj = (
                FluentSearch()
                .where(FluentSearch.asset_type(S3Object))
                .where(FluentSearch.active_assets())
            ).to_request()
            
            all_s3_objects = list(atlan_client.asset.search(request_obj))
            app.logger.info(f"Found {len(all_s3_objects)} total S3 objects")
            
            matching_assets = []
            for asset in all_s3_objects:
                if asset_name.lower() in asset.name.lower():
                    matching_assets.append({
                        "name": asset.name,
                        "qualified_name": asset.qualified_name,
                        "guid": asset.guid,
                        "description": asset.description or "",
                        "user_description": asset.user_description or ""
                    })
            
            return jsonify({
                "success": True,
                "metadata": metadata,
                "matching_assets": matching_assets,
                "total_s3_objects": len(all_s3_objects)
            })
    except Exception as e:
        app.logger.error(f"Error in debug endpoint: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/debug_column_descriptions', methods=['POST'])
async def debug_column_descriptions():
    """Debug endpoint to check column descriptions for a specific table."""
    try:
        data = request.json
        table_qn = data.get('table_qualified_name')
        
        if not table_qn:
            return jsonify({"success": False, "error": "Table qualified name is required."}), 400
        
        app.logger.info(f"Debugging column descriptions for table: {table_qn}")
        
        # Search for columns of the table
        search_request = (
            FluentSearch()
            .where(FluentSearch.asset_type(Column))
            .where(Column.TABLE_QUALIFIED_NAME.eq(table_qn))
        ).to_request()
        
        results = list(atlan_client.asset.search(search_request))
        app.logger.info(f"Found {len(results)} columns for table {table_qn}")
        
        columns_debug = []
        for col in results:
            if isinstance(col, Column):
                # Get all non-empty attributes
                attributes = {}
                for attr_name in dir(col):
                    if not attr_name.startswith('_') and not callable(getattr(col, attr_name)):
                        try:
                            attr_value = getattr(col, attr_name)
                            if attr_value is not None and attr_value != "":
                                attributes[attr_name] = str(attr_value)
                        except:
                            pass
                
                columns_debug.append({
                    "name": col.name,
                    "qualified_name": col.qualified_name,
                    "guid": col.guid,
                    "description": col.description or "",
                    "user_description": col.user_description or "",
                    "data_type": getattr(col, 'data_type', ''),
                    "has_description": bool(col.user_description or col.description),
                    "attributes": attributes
                })
        
        return jsonify({
            "success": True,
            "columns": columns_debug,
            "column_count": len(columns_debug)
        })
    except Exception as e:
        app.logger.error(f"Error debugging column descriptions: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)
