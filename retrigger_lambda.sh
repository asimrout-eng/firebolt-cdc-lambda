#!/bin/bash
# Re-trigger Lambda for all S3 parquet files

S3_BUCKET="fcanalytics"
S3_PREFIX="firebolt_dms_job/"
LAMBDA_FUNCTION="firebolt-cdc-processor"
AWS_REGION="ap-south-1"

echo "════════════════════════════════════════════════════════════════════"
echo "  RE-TRIGGER LAMBDA FOR OLD S3 FILES"
echo "════════════════════════════════════════════════════════════════════"
echo ""
echo "📦 S3 Bucket: $S3_BUCKET"
echo "📂 S3 Prefix: $S3_PREFIX"
echo "λ  Lambda: $LAMBDA_FUNCTION"
echo "🌍 Region: $AWS_REGION"
echo ""

# List all parquet files
echo "📁 Listing parquet files..."
PARQUET_FILES=$(aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX} --recursive --region ${AWS_REGION} | grep '\.parquet$' | awk '{print $4}')

FILE_COUNT=$(echo "$PARQUET_FILES" | wc -l | xargs)
echo "✓ Found ${FILE_COUNT} parquet files"
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
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "${AWS_REGION}",
      "eventTime": "2025-11-10T00:00:00.000Z",
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
    }
  ]
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
    
    # Progress update every 50 files
    if [ $((COUNT % 50)) -eq 0 ] || [ "$COUNT" -eq "$FILE_COUNT" ]; then
        echo "  [${COUNT}/${FILE_COUNT}] ${STATUS} Success: ${SUCCESS}, Failed: ${FAILED}"
    fi
    
done <<< "$PARQUET_FILES"

echo ""
echo "════════════════════════════════════════════════════════════════════"
echo "  SUMMARY"
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
    echo "✅ All Lambda invocations submitted successfully!"
fi

echo ""
echo "📊 Monitor Lambda executions:"
echo "   aws logs tail /aws/lambda/${LAMBDA_FUNCTION} --follow --region ${AWS_REGION}"

