#!/bin/bash

# deploy.sh - Deploy Atlan S3 Connector as Google Cloud Function

# Set default values
FUNCTION_NAME="atlan-s3-connector"
REGION="us-central1"
RUNTIME="python311"
MEMORY="2048MB"
TIMEOUT="540s"
PROJECT_ID=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --project)
      PROJECT_ID="$2"
      shift 2
      ;;
    --function-name)
      FUNCTION_NAME="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --memory)
      MEMORY="$2"
      shift 2
      ;;
    --timeout)
      TIMEOUT="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 --project PROJECT_ID [OPTIONS]"
      echo ""
      echo "Required:"
      echo "  --project PROJECT_ID    Google Cloud Project ID"
      echo ""
      echo "Optional:"
      echo "  --function-name NAME    Function name (default: atlan-s3-connector)"
      echo "  --region REGION         Deployment region (default: us-central1)"
      echo "  --memory MEMORY         Memory allocation (default: 2048MB)"
      echo "  --timeout TIMEOUT       Timeout (default: 540s)"
      echo "  -h, --help             Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option $1"
      exit 1
      ;;
  esac
done

# Check if project ID is provided
if [ -z "$PROJECT_ID" ]; then
    echo "Error: Project ID is required. Use --project PROJECT_ID"
    exit 1
fi

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "Error: gcloud CLI is not installed. Please install it first."
    exit 1
fi

# Set the project
echo "Setting Google Cloud project to: $PROJECT_ID"
gcloud config set project $PROJECT_ID

# Enable required APIs
echo "Enabling required Google Cloud APIs..."
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable artifactregistry.googleapis.com

# Deploy the function
echo "Deploying Cloud Function: $FUNCTION_NAME"
echo "Region: $REGION"
echo "Runtime: $RUNTIME"
echo "Memory: $MEMORY"
echo "Timeout: $TIMEOUT"

gcloud functions deploy $FUNCTION_NAME \
    --gen2 \
    --runtime=$RUNTIME \
    --region=$REGION \
    --source=. \
    --entry-point=atlan_s3_connector \
    --trigger=http \
    --allow-unauthenticated \
    --memory=$MEMORY \
    --timeout=$TIMEOUT \
    --set-env-vars="PYTHONPATH=/workspace" \
    --max-instances=10

# Check deployment status
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Deployment successful!"
    echo ""
    echo "Function URL:"
    gcloud functions describe $FUNCTION_NAME --region=$REGION --format="value(serviceConfig.uri)"
    echo ""
    echo "To test the function:"
    echo "curl -X POST \\"
    echo "  \$(gcloud functions describe $FUNCTION_NAME --region=$REGION --format='value(serviceConfig.uri)') \\"
    echo "  -H 'Content-Type: application/json' \\"
    echo "  -d '{\"enable_ai\": true, \"dry_run\": false}'"
    echo ""
    echo "Environment variables to set:"
    echo "- ATLAN_BASE_URL: Your Atlan instance URL"
    echo "- ATLAN_API_KEY: Your Atlan API key"
    echo "- GEMINI_API_KEY: Your Google Gemini API key (optional)"
    echo "- AWS_ACCESS_KEY_ID: AWS access key (optional if using IAM roles)"
    echo "- AWS_SECRET_ACCESS_KEY: AWS secret key (optional if using IAM roles)"
    echo ""
    echo "Set environment variables using:"
    echo "gcloud functions deploy $FUNCTION_NAME \\"
    echo "  --update-env-vars ATLAN_BASE_URL=your-url,ATLAN_API_KEY=your-key"
else
    echo "❌ Deployment failed!"
    exit 1
fi