#!/bin/bash

# deploy_fixed.sh - Deploy Cloud Function with fixed tag handling
# This script deploys the Atlan S3 Connector with the tag fixes

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Deploying Atlan S3 Connector with Tag Fixes${NC}"
echo "=================================================="

# Configuration
FUNCTION_NAME="atlan-s3-connector"
REGION="us-central1"
MEMORY="2GB"
TIMEOUT="540s"
RUNTIME="python311"

# Check for required environment variables
echo -e "${YELLOW}🔍 Checking required environment variables...${NC}"

# Check Atlan credentials
if [ -z "$ATLAN_BASE_URL" ]; then
    echo -e "${RED}❌ Error: ATLAN_BASE_URL environment variable is not set${NC}"
    echo "Please set it with: export ATLAN_BASE_URL=your_atlan_url"
    exit 1
fi

if [ -z "$ATLAN_API_KEY" ]; then
    echo -e "${RED}❌ Error: ATLAN_API_KEY environment variable is not set${NC}"
    echo "Please set it with: export ATLAN_API_KEY=your_api_key"
    exit 1
fi

# Check AWS credentials
if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo -e "${RED}❌ Error: AWS_ACCESS_KEY_ID environment variable is not set${NC}"
    echo "Please set it with: export AWS_ACCESS_KEY_ID=your_access_key"
    exit 1
fi

if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo -e "${RED}❌ Error: AWS_SECRET_ACCESS_KEY environment variable is not set${NC}"
    echo "Please set it with: export AWS_SECRET_ACCESS_KEY=your_secret_key"
    exit 1
fi

# Set default AWS region if not provided
if [ -z "$AWS_REGION" ]; then
    AWS_REGION="us-east-1"  # Default region
    echo -e "${YELLOW}⚠️  Using default AWS region: $AWS_REGION${NC}"
fi

# Optional: Check for Google API key
if [ -z "$GOOGLE_API_KEY" ]; then
    echo -e "${YELLOW}⚠️  Warning: GOOGLE_API_KEY is not set. AI features will be disabled.${NC}"
    GOOGLE_API_KEY="not-configured"
fi

echo -e "${GREEN}✅ All required environment variables are set${NC}"

echo -e "${YELLOW}🚀 Deploying Cloud Function with tag fixes...${NC}"

gcloud functions deploy $FUNCTION_NAME \
    --runtime $RUNTIME \
    --trigger-http \
    --allow-unauthenticated \
    --memory $MEMORY \
    --timeout $TIMEOUT \
    --region $REGION \
    --entry-point atlan_s3_connector \
    --set-env-vars ATLAN_BASE_URL="$ATLAN_BASE_URL",ATLAN_API_KEY="$ATLAN_API_KEY",GOOGLE_API_KEY="$GOOGLE_API_KEY",AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID",AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY",AWS_REGION="$AWS_REGION" \
    --source .

# Get the function URL
FUNCTION_URL=$(gcloud functions describe $FUNCTION_NAME --region=$REGION --format="value(httpsTrigger.url)")

echo ""
echo -e "${GREEN}🎉 Deployment completed successfully!${NC}"
echo "=================================================="
echo -e "${GREEN}Function URL:${NC} $FUNCTION_URL"
echo ""
echo -e "${GREEN}📝 Test the function:${NC}"
echo ""
echo "curl -X POST \"$FUNCTION_URL\" \\"
echo "     -H \"Content-Type: application/json\" \\"
echo "     -d '{\"enable_ai\": true}'"
echo ""
echo -e "${GREEN}📊 View logs:${NC}"
echo "gcloud functions logs read $FUNCTION_NAME --region=$REGION"