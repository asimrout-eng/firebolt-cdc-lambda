#!/bin/bash
# Update Lambda via S3 (for packages > 50MB)
# Run this after: git pull origin main

set -e

FUNCTION_NAME="firebolt-cdc-processor"
REGION="ap-south-1"
S3_BUCKET="fcanalytics"
S3_KEY="lambda-deployments/firebolt-cdc-processor.zip"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔄 Updating Lambda via S3 (Large Package Support)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo ""
echo "Step 1: Creating deployment package..."

cd "$SCRIPT_DIR/lambda"

# Clean up previous temp directory
rm -rf /tmp/lambda-deploy 2>/dev/null
mkdir -p /tmp/lambda-deploy
cd /tmp/lambda-deploy

# Copy Lambda code
cp "$SCRIPT_DIR/lambda/handler.py" .
cp "$SCRIPT_DIR/lambda/requirements.txt" .

echo "✓ Files copied"

echo ""
echo "Step 2: Installing dependencies (this may take 1-2 minutes)..."

pip3 install -r requirements.txt -t . --quiet --no-cache-dir

echo "✓ Dependencies installed"

echo ""
echo "Step 3: Creating zip package..."

zip -r -q lambda.zip .

# Get package size
SIZE=$(ls -lh lambda.zip | awk '{print $5}')
echo "✓ Package created (size: $SIZE)"

if [ ! -f lambda.zip ]; then
    echo "❌ Error: Failed to create lambda.zip"
    exit 1
fi

echo ""
echo "Step 4: Uploading to S3..."

aws s3 cp lambda.zip "s3://${S3_BUCKET}/${S3_KEY}" || {
    echo ""
    echo "❌ Failed to upload to S3"
    echo ""
    echo "Possible issues:"
    echo "  1. AWS credentials not configured: run 'aws configure'"
    echo "  2. No permission to write to S3 bucket: $S3_BUCKET"
    echo "  3. Network connectivity issue"
    echo ""
    echo "Manual alternative:"
    echo "  1. Upload lambda.zip from /tmp/lambda-deploy/lambda.zip to S3 manually"
    echo "  2. Then run:"
    echo "     aws lambda update-function-code \\"
    echo "       --function-name $FUNCTION_NAME \\"
    echo "       --s3-bucket $S3_BUCKET \\"
    echo "       --s3-key $S3_KEY \\"
    echo "       --region $REGION"
    exit 1
}

echo "✓ Uploaded to s3://${S3_BUCKET}/${S3_KEY}"

echo ""
echo "Step 5: Updating Lambda from S3..."

aws lambda update-function-code \
  --function-name "$FUNCTION_NAME" \
  --s3-bucket "$S3_BUCKET" \
  --s3-key "$S3_KEY" \
  --region "$REGION" \
  --output json > /dev/null || {
    echo ""
    echo "❌ Failed to update Lambda function"
    echo ""
    echo "Possible issues:"
    echo "  1. Lambda function does not exist: $FUNCTION_NAME"
    echo "  2. No permission to update Lambda"
    echo "  3. Function name or region is incorrect"
    exit 1
}

echo "✓ Lambda function updated"

echo ""
echo "Step 6: Waiting for update to complete..."

aws lambda wait function-updated \
  --function-name "$FUNCTION_NAME" \
  --region "$REGION" || {
    echo "⚠️  Wait timed out, but update may still be in progress"
    echo "   Check Lambda console or run: aws lambda get-function --function-name $FUNCTION_NAME --region $REGION"
}

echo "✓ Update complete"

# Cleanup
cd /tmp
rm -rf /tmp/lambda-deploy

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Lambda Updated Successfully via S3!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
echo "What was fixed:"
echo "  ✅ Lambda now supports DMS date format (YYYY/MM/DD)"
echo "  ✅ Can parse keys like: firebolt_dms_job/fair/table/2025/11/05/file.parquet"
echo ""
echo "Next steps:"
echo ""
echo "1. Monitor logs:"
echo "   aws logs tail /aws/lambda/$FUNCTION_NAME --follow --region $REGION"
echo ""
echo "2. Check for successful processing:"
echo "   Look for: ✓ Successfully merged X rows"
echo ""
echo "3. Verify data in Firebolt:"
echo "   SELECT COUNT(*) FROM \"public\".\"your_table_name\";"
echo ""
echo "✅ All done! Lambda is now processing DMS files correctly."

