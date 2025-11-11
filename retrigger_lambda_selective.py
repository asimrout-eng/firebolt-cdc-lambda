#!/usr/bin/env python3
"""
Re-trigger Lambda for SPECIFIC TABLES only
Selective reload of tables from old S3 files
"""

import boto3
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

# Configuration
S3_BUCKET = 'fcanalytics'
S3_PREFIX = 'firebolt_dms_job/'
LAMBDA_FUNCTION = 'firebolt-cdc-processor'
AWS_REGION = 'ap-south-1'
MAX_WORKERS = 10  # Parallel Lambda invocations

# ═══════════════════════════════════════════════════════════════════
# SPECIFY TABLES TO RELOAD HERE
# ═══════════════════════════════════════════════════════════════════
TABLES_TO_RELOAD = [
    'cent_borrower_transaction',
    'cent_communications_log',
    'cent_emi',
    'users',
    'sessions',
    # Add more table names here
]

# Or set to None to reload ALL tables
# TABLES_TO_RELOAD = None

# Initialize AWS clients
s3_client = boto3.client('s3', region_name=AWS_REGION)
lambda_client = boto3.client('lambda', region_name=AWS_REGION)

def list_s3_parquet_files(table_filter=None):
    """List parquet files, optionally filtered by table names"""
    print(f"📁 Listing parquet files in s3://{S3_BUCKET}/{S3_PREFIX}")
    
    if table_filter:
        print(f"🔍 Filtering for tables: {', '.join(table_filter)}")
    
    parquet_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        if 'Contents' not in page:
            continue
        
        for obj in page['Contents']:
            key = obj['Key']
            if not key.endswith('.parquet'):
                continue
            
            # Key format: firebolt_dms_job/fair/<table_name>/YYYY/MM/DD/file.parquet
            parts = key.split('/')
            if len(parts) >= 3:
                table_name = parts[2]  # Extract table name
                
                # Filter by table if specified
                if table_filter and table_name not in table_filter:
                    continue
                
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
                "eventTime": "2025-11-11T00:00:00.000Z",
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
    print("  RE-TRIGGER LAMBDA FOR SPECIFIC TABLES")
    print("═" * 70)
    print(f"\n📦 S3 Bucket: {S3_BUCKET}")
    print(f"📂 S3 Prefix: {S3_PREFIX}")
    print(f"λ  Lambda: {LAMBDA_FUNCTION}")
    print(f"🌍 Region: {AWS_REGION}")
    print(f"⚡ Max Workers: {MAX_WORKERS}")
    
    if TABLES_TO_RELOAD:
        print(f"\n🔍 Tables to reload ({len(TABLES_TO_RELOAD)}):")
        for table in TABLES_TO_RELOAD:
            print(f"   - {table}")
    else:
        print(f"\n⚠️  No table filter - will reload ALL tables")
    
    print()
    
    # List parquet files (filtered by table if specified)
    parquet_files = list_s3_parquet_files(table_filter=TABLES_TO_RELOAD)
    
    if not parquet_files:
        print("❌ No parquet files found for the specified tables!")
        return
    
    # Show breakdown by table
    table_counts = {}
    for key in parquet_files:
        parts = key.split('/')
        if len(parts) >= 3:
            table_name = parts[2]
            table_counts[table_name] = table_counts.get(table_name, 0) + 1
    
    print(f"\n📊 Files per table:")
    for table, count in sorted(table_counts.items()):
        print(f"   {table}: {count} files")
    
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
    
    if TABLES_TO_RELOAD:
        print(f"\n✅ Reloaded {len(TABLES_TO_RELOAD)} tables: {', '.join(TABLES_TO_RELOAD)}")
    
    if failed_count > 0 or error_count > 0:
        print("\n⚠️  Some invocations failed. Check CloudWatch logs for details.")
    else:
        print("\n✅ All Lambda invocations submitted successfully!")
    
    print("\n📊 Monitor Lambda executions:")
    print(f"   aws logs tail /aws/lambda/{LAMBDA_FUNCTION} --follow --region {AWS_REGION}")

if __name__ == "__main__":
    # Allow table names to be passed as command-line arguments
    if len(sys.argv) > 1:
        TABLES_TO_RELOAD = sys.argv[1:]
        print(f"📝 Using table names from command line: {TABLES_TO_RELOAD}")
    
    main()

