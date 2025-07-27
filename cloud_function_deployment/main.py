# main.py - Cloud Function Entry Point
"""
Google Cloud Function HTTP trigger for Atlan S3 Connector
This is the entry point that Cloud Functions will call
"""

import functions_framework
import asyncio
import json
import os
import logging
from datetime import datetime

# Import the pipeline
from atlan_s3_pipeline import AtlanS3Pipeline

# Configure logging for Cloud Functions
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@functions_framework.http
def atlan_s3_connector(request):
    """
    HTTP Cloud Function entry point for Atlan S3 Connector
    
    Expected request body (JSON):
    {
        "enable_ai": true/false,
        "bucket_name": "optional-override",
        "dry_run": true/false
    }
    """
    
    # Handle CORS preflight requests
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)
    
    # Set CORS headers for actual requests
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Content-Type': 'application/json'
    }
    
    try:
        logger.info(f"Cloud Function triggered at {datetime.utcnow()}")
        logger.info(f"Request method: {request.method}")
        
        # Parse request parameters
        request_json = request.get_json(silent=True) or {}
        
        # Extract parameters with defaults
        enable_ai = request_json.get('enable_ai', True)
        bucket_name = request_json.get('bucket_name', None)
        dry_run = request_json.get('dry_run', False)
        
        logger.info(f"Parameters - enable_ai: {enable_ai}, bucket_name: {bucket_name}, dry_run: {dry_run}")
        
        # Validate environment variables
        required_env_vars = ['ATLAN_BASE_URL', 'ATLAN_API_KEY']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        
        if missing_vars:
            error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
            logger.error(error_msg)
            return (
                json.dumps({'error': error_msg, 'success': False}),
                400,
                headers
            )
        
        # Initialize and run pipeline
        logger.info("Initializing Atlan S3 Pipeline...")
        pipeline = AtlanS3Pipeline(bucket_name_override=bucket_name)
        
        if dry_run:
            logger.info("Running in DRY RUN mode - no actual changes will be made")
            results = {
                'success': True,
                'dry_run': True,
                'message': 'Dry run completed - no actual operations performed',
                'timestamp': datetime.utcnow().isoformat(),
                'parameters': {
                    'enable_ai': enable_ai,
                    'bucket_name': bucket_name
                }
            }
        else:
            logger.info("Starting pipeline execution...")
            results = asyncio.run(pipeline.run_pipeline(enable_ai=enable_ai))
            
            # Add metadata to results
            results['timestamp'] = datetime.utcnow().isoformat()
            results['cloud_function'] = True
            results['parameters'] = {
                'enable_ai': enable_ai,
                'bucket_name': bucket_name
            }
        
        logger.info(f"Pipeline completed successfully: {results.get('success', False)}")
        
        # Return success response
        return (
            json.dumps(results, default=str, indent=2),
            200,
            headers
        )
        
    except Exception as e:
        error_msg = f"Cloud Function execution failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        error_response = {
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__,
            'timestamp': datetime.utcnow().isoformat(),
            'cloud_function': True
        }
        
        return (
            json.dumps(error_response, default=str, indent=2),
            500,
            headers
        )

# Health check endpoint
@functions_framework.http
def health_check(request):
    """Simple health check endpoint"""
    return {
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'service': 'atlan-s3-connector'
    }