#!/usr/bin/env python3
"""
Re-trigger Lambda for all existing S3 parquet files
This will reload data into recreated Firebolt tables
"""

import boto3
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
S3_BUCKET = 'fcanalytics'
S3_PREFIX = 'firebolt_dms_job/'
LAMBDA_FUNCTION = 'firebolt-cdc-processor'
AWS_REGION = 'ap-south-1'
MAX_WORKERS = 10  # Parallel Lambda invocations

# Initialize AWS clients
s3_client = boto3.client('s3', region_name=AWS_REGION)
lambda_client = boto3.client('lambda', region_name=AWS_REGION)

def list_s3_parquet_files():
    """List all parquet files in S3"""
    print(f"📁 Listing parquet files in s3://{S3_BUCKET}/{S3_PREFIX}")
    
    parquet_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        if 'Contents' not in page:
            continue
        
        for obj in page['Contents']:
            key = obj['Key']
            if key.endswith('.parquet'):
                parquet_files.append(key)
    
    print(f"✓ Found {len(parquet_files)} parquet files")
    return parquet_files

def create_lambda_event(s3_key):
    """Create S3 event payload for Lambda"""
    return {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": AWS_REGION,
                "eventTime": "2025-11-10T00:00:00.000Z",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "manual-retrigger",
                    "bucket": {
                        "name": S3_BUCKET,
                        "arn": f"arn:aws:s3:::{S3_BUCKET}"
                    },
                    "object": {
                        "key": s3_key,
                        "size": 1024,
                        "eTag": "manual-retrigger"
                    }
                }
            }
        ]
    }

def invoke_lambda_for_file(s3_key):
    """Invoke Lambda for a single S3 file"""
    try:
        event = create_lambda_event(s3_key)
        
        response = lambda_client.invoke(
            FunctionName=LAMBDA_FUNCTION,
            InvocationType='Event',  # Async invocation
            Payload=json.dumps(event)
        )
        
        if response['StatusCode'] == 202:
            return {'status': 'success', 'key': s3_key}
        else:
            return {'status': 'failed', 'key': s3_key, 'error': f"Status {response['StatusCode']}"}
    
    except Exception as e:
        return {'status': 'error', 'key': s3_key, 'error': str(e)}

def main():
    print("═" * 70)
    print("  RE-TRIGGER LAMBDA FOR OLD S3 FILES")
    print("═" * 70)
    print(f"\n📦 S3 Bucket: {S3_BUCKET}")
    print(f"📂 S3 Prefix: {S3_PREFIX}")
    print(f"λ  Lambda: {LAMBDA_FUNCTION}")
    print(f"🌍 Region: {AWS_REGION}")
    print(f"⚡ Max Workers: {MAX_WORKERS}")
    print()
    
    # List all parquet files
    parquet_files = list_s3_parquet_files()
    
    if not parquet_files:
        print("❌ No parquet files found!")
        return
    
    print(f"\n🚀 Starting Lambda invocations for {len(parquet_files)} files...")
    print(f"   (Using {MAX_WORKERS} parallel workers)\n")
    
    # Invoke Lambda for each file in parallel
    success_count = 0
    failed_count = 0
    error_count = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_key = {
            executor.submit(invoke_lambda_for_file, key): key 
            for key in parquet_files
        }
        
        # Process results as they complete
        for i, future in enumerate(as_completed(future_to_key), 1):
            result = future.result()
            
            if result['status'] == 'success':
                success_count += 1
                status_icon = "✓"
            elif result['status'] == 'failed':
                failed_count += 1
                status_icon = "✗"
                print(f"  ✗ FAILED: {result['key']} - {result['error']}")
            else:  # error
                error_count += 1
                status_icon = "⚠"
                print(f"  ⚠ ERROR: {result['key']} - {result['error']}")
            
            # Progress update every 50 files
            if i % 50 == 0 or i == len(parquet_files):
                print(f"  [{i}/{len(parquet_files)}] {status_icon} Success: {success_count}, Failed: {failed_count}, Errors: {error_count}")
    
    # Summary
    print("\n" + "═" * 70)
    print("  SUMMARY")
    print("═" * 70)
    print(f"  ✓ Success: {success_count}")
    print(f"  ✗ Failed:  {failed_count}")
    print(f"  ⚠ Errors:  {error_count}")
    print(f"  📊 Total:   {len(parquet_files)}")
    print("═" * 70)
    
    if failed_count > 0 or error_count > 0:
        print("\n⚠️  Some invocations failed. Check CloudWatch logs for details.")
    else:
        print("\n✅ All Lambda invocations submitted successfully!")
    
    print("\n📊 Monitor Lambda executions:")
    print(f"   aws logs tail /aws/lambda/{LAMBDA_FUNCTION} --follow --region {AWS_REGION}")

if __name__ == "__main__":
    main()

