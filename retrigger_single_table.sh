#!/bin/bash
# Re-trigger Lambda for a SINGLE TABLE

if [ $# -eq 0 ]; then
    echo "Usage: ./retrigger_single_table.sh <table_name>"
    echo ""
    echo "Example:"
    echo "  ./retrigger_single_table.sh cent_user"
    echo "  ./retrigger_single_table.sh sessions"
    exit 1
fi

TABLE_NAME=$1
S3_BUCKET="fcanalytics"
S3_PREFIX="firebolt_dms_job/fair/${TABLE_NAME}/"
LAMBDA_FUNCTION="firebolt-cdc-processor"
AWS_REGION="ap-south-1"

echo "════════════════════════════════════════════════════════════════════"
echo "  RE-TRIGGER LAMBDA FOR SINGLE TABLE"
echo "════════════════════════════════════════════════════════════════════"
echo ""
echo "📦 S3 Bucket: $S3_BUCKET"
echo "📂 S3 Prefix: $S3_PREFIX"
echo "📋 Table: $TABLE_NAME"
echo "λ  Lambda: $LAMBDA_FUNCTION"
echo "🌍 Region: $AWS_REGION"
echo ""

# Check if table path exists in S3
echo "🔍 Checking if table exists in S3..."
TABLE_EXISTS=$(aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX} --region ${AWS_REGION} 2>/dev/null | wc -l)

if [ "$TABLE_EXISTS" -eq 0 ]; then
    echo "❌ ERROR: Table '${TABLE_NAME}' not found in S3!"
    echo "   Path checked: s3://${S3_BUCKET}/${S3_PREFIX}"
    echo ""
    echo "💡 Available tables:"
    aws s3 ls s3://${S3_BUCKET}/firebolt_dms_job/fair/ --region ${AWS_REGION} | grep "PRE" | awk '{print "   - " $2}' | sed 's|/||g'
    exit 1
fi

# List all parquet files for this table
echo "📁 Listing parquet files for ${TABLE_NAME}..."
PARQUET_FILES=$(aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX} --recursive --region ${AWS_REGION} | grep '\.parquet$' | awk '{print $4}')

FILE_COUNT=$(echo "$PARQUET_FILES" | wc -l | xargs)
echo "✓ Found ${FILE_COUNT} parquet files for ${TABLE_NAME}"
echo ""

if [ "$FILE_COUNT" -eq 0 ]; then
    echo "❌ No parquet files found!"
    exit 1
fi

echo "🚀 Starting Lambda invocations..."
echo ""

SUCCESS=0
FAILED=0
COUNT=0

while IFS= read -r S3_KEY; do
    COUNT=$((COUNT + 1))
    
    # Create S3 event JSON payload
    PAYLOAD=$(cat <<EOF
{
  "Records": [{
    "eventVersion": "2.1",
    "eventSource": "aws:s3",
    "awsRegion": "${AWS_REGION}",
    "eventTime": "2025-11-11T00:00:00.000Z",
    "eventName": "ObjectCreated:Put",
    "s3": {
      "s3SchemaVersion": "1.0",
      "configurationId": "manual-retrigger",
      "bucket": {
        "name": "${S3_BUCKET}",
        "arn": "arn:aws:s3:::${S3_BUCKET}"
      },
      "object": {
        "key": "${S3_KEY}",
        "size": 1024,
        "eTag": "manual-retrigger"
      }
    }
  }]
}
EOF
    )
    
    # Invoke Lambda asynchronously
    if aws lambda invoke \
        --function-name ${LAMBDA_FUNCTION} \
        --invocation-type Event \
        --payload "${PAYLOAD}" \
        --region ${AWS_REGION} \
        /dev/null > /dev/null 2>&1; then
        SUCCESS=$((SUCCESS + 1))
        STATUS="✓"
    else
        FAILED=$((FAILED + 1))
        STATUS="✗"
        echo "  ✗ FAILED: ${S3_KEY}"
    fi
    
    # Progress update every 10 files
    if [ $((COUNT % 10)) -eq 0 ] || [ "$COUNT" -eq "$FILE_COUNT" ]; then
        echo "  [${COUNT}/${FILE_COUNT}] ${STATUS} Success: ${SUCCESS}, Failed: ${FAILED}"
    fi
    
done <<< "$PARQUET_FILES"

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "  SUMMARY - ${TABLE_NAME}"
echo "════════════════════════════════════════════════════════════════════"
echo "  ✓ Success: ${SUCCESS}"
echo "  ✗ Failed:  ${FAILED}"
echo "  📊 Total:   ${FILE_COUNT}"
echo "════════════════════════════════════════════════════════════════════"

if [ "$FAILED" -gt 0 ]; then
    echo ""
    echo "⚠️  Some invocations failed. Check CloudWatch logs for details."
else
    echo ""
    echo "✅ All Lambda invocations submitted successfully for ${TABLE_NAME}!"
fi

echo ""
echo "📊 Monitor Lambda executions:"
echo "   aws logs tail /aws/lambda/${LAMBDA_FUNCTION} --follow --region ${AWS_REGION}"

