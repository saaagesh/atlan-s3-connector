# flask_app/app.py
"""
Flask UI for Atlan S3 Connector
"""

import os
import asyncio
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)

# All imports are now local to the flask_app directory
from s3_connector import S3Connector
from atlan_client import get_atlan_client
from pyatlan.model.assets import S3Object, Table, Column
from pyatlan.model.fluent_search import FluentSearch
from config import S3Config, AIConfig, ConnectionConfig
from flask_ai_enhancer import AIEnhancer

app = Flask(__name__)

# --- Configuration ---
s3_config = S3Config()
ai_config = AIConfig()
connection_config = ConnectionConfig()
atlan_client = get_atlan_client()

@app.route('/')
def index():
    """Render the main UI page."""
    return render_template('index.html')

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
            # We only need a subset of info for the UI
            assets = [{"name": obj['key'], "qualified_name": obj['qualified_name'], "type": "s3"} for obj in s3_objects]
        else:
            # Handle DB connections (Postgres, Snowflake)
            connection_name = getattr(connection_config, f"{source_name.split('-')[0]}_connection_name")
            search_request = (
                FluentSearch()
                .where(FluentSearch.asset_type(Table))
                .where(FluentSearch.active_assets())
                .where(Table.CONNECTION_NAME.eq(connection_name))
            ).to_request()
            
            results = atlan_client.asset.search(search_request)
            assets = [{"name": r.name, "qualified_name": r.qualified_name, "type": "table"} for r in results]

        return jsonify({"success": True, "assets": assets})
    except Exception as e:
        app.logger.error(f"Error fetching assets for {source_name}: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/columns', methods=['POST'])
async def get_columns():
    """Get columns for a given asset."""
    asset_qn = request.json.get('asset_qualified_name')
    source_type = request.json.get('source_type')

    try:
        if source_type == "s3":
            # For S3, we need to re-discover the object to get schema info
            # This is inefficient but required by the current S3Connector design
            s3_connector = S3Connector(s3_config)
            key = asset_qn.split('/')[-1]
            head_response = s3_connector.s3_client.head_object(Bucket=s3_config.bucket_name, Key=key)
            s3_object_details = {'Key': key, 'Size': head_response['ContentLength'], 'LastModified': head_response['LastModified'], 'ETag': head_response['ETag']}
            metadata = await s3_connector._extract_object_metadata(s3_object_details)
            columns_data = metadata.get('schema_info', {}).get('columns', [])
            columns = [{"name": c['name'], "description": ""} for c in columns_data]
        else: # table
            # Explicitly search for columns related to the table's qualified name.
            # This is more reliable than relying on the table's relationship attribute.
            search_request = (
                FluentSearch()
                .where(FluentSearch.asset_type(Column))
                .where(Column.TABLE_QUALIFIED_NAME.eq(asset_qn))
            ).to_request()

            results = atlan_client.asset.search(search_request)
            
            columns = []
            for col in results:
                # Ensure col is a Column asset before accessing attributes
                if isinstance(col, Column):
                    columns.append({
                        "name": col.name,
                        "description": col.user_description or col.description or ""
                    })

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
        
        atlan_columns = {c.name: c.qualified_name for c in atlan_client.asset.search(search_request) if isinstance(c, Column)}

        batch = []
        for col_data in columns_to_update:
            col_name = col_data.get('name')
            col_description = col_data.get('description')
            
            if col_name in atlan_columns and col_description:
                updater = Column.updater(
                    qualified_name=atlan_columns[col_name],
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

if __name__ == '__main__':
    app.run(debug=True, port=5001)
