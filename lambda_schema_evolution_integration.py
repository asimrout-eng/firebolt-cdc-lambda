"""
Schema Evolution Integration for Lambda Handler
Add these functions to your handler.py to track new tables and DDL changes

INTEGRATION POINTS:
1. After line 868 (after COPY succeeds) - Track schema from staging table
2. After line 724 (check for new table) - Auto-detect primary key
3. Save schema metadata to S3 for change tracking
"""

import os
import json
import boto3
import logging
from datetime import datetime
from typing import Dict, Optional, List

logger = logging.getLogger()

# Configuration (set via environment variables)
SCHEMA_EVOLUTION_ENABLED = os.environ.get('SCHEMA_EVOLUTION_ENABLED', 'false').lower() == 'true'
SCHEMA_METADATA_BUCKET = os.environ.get('SCHEMA_METADATA_BUCKET', '')  # Same as TABLE_KEYS_S3_BUCKET
SCHEMA_METADATA_PREFIX = 'firebolt_dms_job/schema_metadata/'
SCHEMA_ALERT_SNS_TOPIC = os.environ.get('SCHEMA_ALERT_SNS_TOPIC', '')  # Optional

s3_client = boto3.client('s3')

# ═══════════════════════════════════════════════════════════════════
# SCHEMA EVOLUTION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════

def load_schema_metadata(table_name: str, bucket: str) -> Optional[Dict]:
    """Load last known schema metadata from S3"""
    if not bucket:
        return None
    
    try:
        key = f"{SCHEMA_METADATA_PREFIX}{table_name}_schema.json"
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except s3_client.exceptions.NoSuchKey:
        return None
    except Exception as e:
        logger.warning(f"Error loading schema metadata for {table_name}: {e}")
        return None

def save_schema_metadata(table_name: str, schema: Dict[str, str], bucket: str, metadata: Dict = None):
    """Save schema metadata to S3"""
    if not bucket:
        return
    
    try:
        key = f"{SCHEMA_METADATA_PREFIX}{table_name}_schema.json"
        data = {
            'table_name': table_name,
            'schema': schema,
            'last_updated': datetime.utcnow().isoformat(),
            'metadata': metadata or {}
        }
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        logger.info(f"✅ Saved schema metadata for {table_name}")
    except Exception as e:
        logger.error(f"Error saving schema metadata for {table_name}: {e}")

def detect_schema_changes(current_schema: Dict[str, str], previous_schema: Optional[Dict]) -> Dict:
    """
    Detect schema changes between current and previous schema
    
    Returns:
        Dict with 'new_columns', 'removed_columns', 'type_changes', 'is_new_table'
    """
    if not previous_schema:
        return {
            'new_columns': [{'column': col, 'type': dtype} for col, dtype in current_schema.items()],
            'removed_columns': [],
            'type_changes': [],
            'is_new_table': True
        }
    
    changes = {
        'new_columns': [],
        'removed_columns': [],
        'type_changes': [],
        'is_new_table': False
    }
    
    # Find new columns
    for col, dtype in current_schema.items():
        if col not in previous_schema:
            changes['new_columns'].append({'column': col, 'type': dtype})
        elif previous_schema[col] != dtype:
            changes['type_changes'].append({
                'column': col,
                'old_type': previous_schema[col],
                'new_type': dtype
            })
    
    # Find removed columns
    for col in previous_schema:
        if col not in current_schema:
            changes['removed_columns'].append({'column': col, 'old_type': previous_schema[col]})
    
    return changes

def auto_detect_primary_key(table: str, fb_connector) -> Optional[str]:
    """
    Auto-detect primary key for a new table
    
    Returns:
        Primary key column name(s) if found, None otherwise
    """
    try:
        # Check if table has primary index in Firebolt
        query = f"""
        SELECT index_definition
        FROM information_schema.indexes
        WHERE table_name = '{table}'
          AND index_type = 'primary'
        """
        cursor = fb_connector.execute(query)
        result = cursor.fetchone()
        
        if result:
            # Parse primary key from index definition
            index_def = result[0]
            # Remove brackets and quotes, split by comma
            pk_cols = index_def.replace('[', '').replace(']', '').replace('(', '').replace(')', '').replace('"', '').split(',')
            pk_cols = [col.strip() for col in pk_cols if col.strip()]
            
            if len(pk_cols) == 1:
                return pk_cols[0]  # Single column
            else:
                return pk_cols  # Composite key (return as list)
        else:
            # No primary index - check for common PK column names
            query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table}'
              AND table_schema = 'public'
              AND column_name IN ('id', 'uuid', '{table}_id', 'pk_id')
            LIMIT 1
            """
            cursor = fb_connector.execute(query)
            result = cursor.fetchone()
            
            if result:
                return result[0]
            
            return None
            
    except Exception as e:
        logger.error(f"Error auto-detecting primary key for {table}: {e}")
        return None

def handle_new_table_detection(table: str, fb_connector, table_keys: Dict, bucket: str) -> Optional[str]:
    """
    Handle detection of a new table - auto-configure primary key
    
    Returns:
        Primary key if auto-configured, None otherwise
    """
    logger.info(f"🆕 New table detected: {table}")
    
    # Try to auto-detect primary key
    pk = auto_detect_primary_key(table, fb_connector)
    
    if pk:
        # Auto-configure in table_keys (in memory)
        table_keys[table] = pk
        logger.info(f"✅ Auto-configured {table} with PK: {pk}")
        
        # Optionally: Save updated table_keys to S3
        # (You'd need to add this function or do it in scheduled job)
        # save_table_keys_to_s3(table_keys, bucket)
        
        return pk
    else:
        logger.warning(f"⚠️  No primary key found for {table} - CDC will be skipped")
        return None

def track_schema_evolution(
    table: str, 
    staging_table: str, 
    fb_connector, 
    bucket: str,
    cols_staging: List[str],
    col_types_staging: Dict[str, str]
):
    """
    Track schema evolution after COPY to staging table
    
    Call this AFTER COPY succeeds (around line 868 in handler.py)
    
    Args:
        table: Production table name
        staging_table: Staging table name (has new schema from parquet)
        fb_connector: Firebolt connector
        bucket: S3 bucket for schema metadata
        cols_staging: List of columns in staging table
        col_types_staging: Dict of column_name -> data_type from staging table
    """
    if not SCHEMA_EVOLUTION_ENABLED or not bucket:
        return
    
    try:
        # Get current schema from staging (represents new data structure)
        current_schema = col_types_staging
        
        # Load previous schema metadata
        previous_metadata = load_schema_metadata(table, bucket)
        previous_schema = previous_metadata.get('schema') if previous_metadata else None
        
        # Detect changes
        changes = detect_schema_changes(current_schema, previous_schema)
        
        # Log changes
        if changes['is_new_table']:
            logger.info(f"🆕 First time seeing schema for {table}")
        elif changes['new_columns'] or changes['removed_columns'] or changes['type_changes']:
            logger.warning(f"⚠️  Schema changes detected for {table}:")
            if changes['new_columns']:
                new_cols = [c['column'] for c in changes['new_columns']]
                logger.warning(f"  ➕ New columns: {new_cols}")
            if changes['removed_columns']:
                removed_cols = [c['column'] for c in changes['removed_columns']]
                logger.warning(f"  ➖ Removed columns: {removed_cols}")
            if changes['type_changes']:
                type_changes = [f"{c['column']}: {c['old_type']} → {c['new_type']}" for c in changes['type_changes']]
                logger.warning(f"  🔄 Type changes: {type_changes}")
            
            # Send alert if configured
            if SCHEMA_ALERT_SNS_TOPIC:
                send_schema_alert(table, changes)
        
        # Save current schema as new baseline
        save_schema_metadata(table, current_schema, bucket, {
            'staging_table': staging_table,
            'changes_detected': changes,
            'columns_count': len(cols_staging)
        })
        
    except Exception as e:
        logger.error(f"Error tracking schema evolution for {table}: {e}")
        # Don't fail Lambda on schema tracking errors

def send_schema_alert(table_name: str, changes: Dict):
    """Send SNS alert about schema changes"""
    if not SCHEMA_ALERT_SNS_TOPIC:
        return
    
    try:
        sns = boto3.client('sns')
        message = {
            'alert_type': 'schema_change',
            'table_name': table_name,
            'timestamp': datetime.utcnow().isoformat(),
            'changes': changes
        }
        
        sns.publish(
            TopicArn=SCHEMA_ALERT_SNS_TOPIC,
            Subject=f"Schema Evolution Alert: {table_name}",
            Message=json.dumps(message, indent=2)
        )
        logger.info(f"✅ Sent schema alert for {table_name}")
    except Exception as e:
        logger.error(f"Error sending schema alert: {e}")

# ═══════════════════════════════════════════════════════════════════
# INTEGRATION INTO HANDLER
# ═══════════════════════════════════════════════════════════════════

"""
ADD THESE INTEGRATIONS TO YOUR handler() FUNCTION:

1. After line 724 (when checking for null primary key):
   
   # Check if this is a new table not in table_keys.json
   if table not in table_keys and SCHEMA_EVOLUTION_ENABLED:
       bucket = os.environ.get("TABLE_KEYS_S3_BUCKET", "")
       pk = handle_new_table_detection(table, fb_connector, table_keys, bucket)
       if pk:
           # Update keys for this Lambda execution
           keys = pk if isinstance(pk, str) else pk
           if isinstance(keys, str):
               keys = [keys]
           logger.info(f"✅ Auto-configured new table {table} with PK: {keys}")
       else:
           # No PK found - will be handled by existing null check
           pass

2. After line 868 (after COPY succeeds):
   
   # Track schema evolution (after staging table is created)
   if SCHEMA_EVOLUTION_ENABLED:
       bucket = os.environ.get("TABLE_KEYS_S3_BUCKET", "")
       track_schema_evolution(
           table=table,
           staging_table=staging_table,
           fb_connector=fb_connector,
           bucket=bucket,
           cols_staging=cols_staging,
           col_types_staging=col_types_staging
       )

3. Add environment variables to Lambda:
   - SCHEMA_EVOLUTION_ENABLED=true
   - SCHEMA_ALERT_SNS_TOPIC=arn:aws:sns:... (optional)
"""

