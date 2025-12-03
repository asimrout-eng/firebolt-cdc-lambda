"""
EXACT PATCH FOR handler.py - Schema Evolution Integration

Copy the functions below and add them to your handler.py
Then apply the integration points shown in the comments

REQUIRED: Add to existing imports (line 9):
    from typing import Optional, Any, Dict, List
"""

# ═══════════════════════════════════════════════════════════════════
# ADD THESE FUNCTIONS TO handler.py (after cleanup_old_processed_files, around line 660)
# ═══════════════════════════════════════════════════════════════════

def load_schema_metadata(table_name: str, bucket: str) -> Optional[Dict]:
    """Load last known schema metadata from S3"""
    if not bucket:
        return None
    try:
        s3 = boto3.client("s3")
        key = f"firebolt_dms_job/schema_metadata/{table_name}_schema.json"
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        logger.warning(f"Error loading schema metadata for {table_name}: {e}")
        return None

def save_schema_metadata(table_name: str, schema: Dict[str, str], bucket: str, metadata: Dict = None):
    """Save schema metadata to S3"""
    if not bucket:
        return
    try:
        s3 = boto3.client("s3")
        key = f"firebolt_dms_job/schema_metadata/{table_name}_schema.json"
        data = {
            'table_name': table_name,
            'schema': schema,
            'last_updated': datetime.utcnow().isoformat(),
            'metadata': metadata or {}
        }
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        logger.info(f"✅ Saved schema metadata for {table_name}")
    except Exception as e:
        logger.error(f"Error saving schema metadata for {table_name}: {e}")

def detect_schema_changes(current_schema: Dict[str, str], previous_schema: Optional[Dict]) -> Dict:
    """Detect schema changes between current and previous schema"""
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
    
    for col, dtype in current_schema.items():
        if col not in previous_schema:
            changes['new_columns'].append({'column': col, 'type': dtype})
        elif previous_schema[col] != dtype:
            changes['type_changes'].append({
                'column': col,
                'old_type': previous_schema[col],
                'new_type': dtype
            })
    
    for col in previous_schema:
        if col not in current_schema:
            changes['removed_columns'].append({'column': col, 'old_type': previous_schema[col]})
    
    return changes

def auto_detect_primary_key(table: str, fb_connector) -> Optional[str]:
    """Auto-detect primary key for a new table"""
    try:
        query = f"""
        SELECT index_definition
        FROM information_schema.indexes
        WHERE table_name = '{table}'
          AND index_type = 'primary'
        """
        cursor = fb_connector.execute(query)
        result = cursor.fetchone()
        
        if result:
            index_def = result[0]
            pk_cols = index_def.replace('[', '').replace(']', '').replace('(', '').replace(')', '').replace('"', '').split(',')
            pk_cols = [col.strip() for col in pk_cols if col.strip()]
            
            if len(pk_cols) == 1:
                return pk_cols[0]
            else:
                return pk_cols
        return None
    except Exception as e:
        logger.error(f"Error auto-detecting primary key for {table}: {e}")
        return None

def track_schema_evolution(table: str, staging_table: str, fb_connector, bucket: str, 
                           cols_staging: List[str], col_types_staging: Dict[str, str]):
    """Track schema evolution after COPY to staging table"""
    schema_enabled = os.environ.get('SCHEMA_EVOLUTION_ENABLED', 'false').lower() == 'true'
    if not schema_enabled or not bucket:
        return
    
    try:
        current_schema = col_types_staging
        previous_metadata = load_schema_metadata(table, bucket)
        previous_schema = previous_metadata.get('schema') if previous_metadata else None
        
        changes = detect_schema_changes(current_schema, previous_schema)
        
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
        
        save_schema_metadata(table, current_schema, bucket, {
            'staging_table': staging_table,
            'changes_detected': changes,
            'columns_count': len(cols_staging)
        })
    except Exception as e:
        logger.error(f"Error tracking schema evolution for {table}: {e}")

# ═══════════════════════════════════════════════════════════════════
# INTEGRATION POINT 1: After line 724 (new table detection)
# ═══════════════════════════════════════════════════════════════════

"""
REPLACE THIS SECTION (lines 723-783):

    # Get primary keys for this table
    keys = table_keys.get(table)
    
    # Handle tables with null primary keys (skip CDC)
    if keys is None:
        ...

WITH THIS:

    # Get primary keys for this table
    keys = table_keys.get(table)
    
    # ═══════════════════════════════════════════════════════════════════
    # SCHEMA EVOLUTION: Detect and auto-configure new tables
    # ═══════════════════════════════════════════════════════════════════
    schema_enabled = os.environ.get('SCHEMA_EVOLUTION_ENABLED', 'false').lower() == 'true'
    
    if keys is None and schema_enabled:
        # New table not in table_keys.json - try to auto-detect primary key
        logger.info(f"🆕 New table detected: {table} (not in table_keys.json)")
        
        # Connect to Firebolt to check for primary key
        fb_connector_temp = FireboltConnector()
        fb_connector_temp.connect()
        
        try:
            pk = auto_detect_primary_key(table, fb_connector_temp)
            if pk:
                # Auto-configure primary key
                keys = pk
                table_keys[table] = pk  # Update in-memory config
                logger.info(f"✅ Auto-configured {table} with PK: {pk}")
                logger.info(f"  Note: Update table_keys.json in S3 to persist this configuration")
            else:
                logger.warning(f"⚠️  No primary key found for {table} - CDC will be skipped")
        except Exception as e:
            logger.error(f"Error auto-detecting primary key for {table}: {e}")
        finally:
            fb_connector_temp.disconnect()
    
    # Handle tables with null primary keys (skip CDC)
    if keys is None:
        logger.warning(f"⚠️  Table '{table}' has null primary key - no CDC configured")
        ...
        (rest of existing null PK handling code)
"""

# ═══════════════════════════════════════════════════════════════════
# INTEGRATION POINT 2: After line 868 (after COPY succeeds)
# ═══════════════════════════════════════════════════════════════════

"""
ADD THIS CODE after line 868 (after fb_connector.execute(copy_sql)):

        # COPY single file to staging (AUTO_CREATE will infer schema from parquet)
        copy_sql = render_copy_single_file(staging_table, table, date_path, filename, location, database)
        fb_connector.execute(copy_sql)
        logger.info(f"✓ Copied {filename} to {staging_table} (auto-created from parquet schema)")
        
        # Get columns and data types from staging table (move these lines up)
        cols_staging = get_columns("public", staging_table, fb_connector)
        col_types_staging = get_column_types("public", staging_table, fb_connector)
        
        # ═══════════════════════════════════════════════════════════════════
        # SCHEMA EVOLUTION: Track schema changes
        # ═══════════════════════════════════════════════════════════════════
        if schema_enabled:
            bucket = os.environ.get("TABLE_KEYS_S3_BUCKET", "")
            track_schema_evolution(
                table=table,
                staging_table=staging_table,
                fb_connector=fb_connector,
                bucket=bucket,
                cols_staging=cols_staging,
                col_types_staging=col_types_staging
            )
        
        # Get column list from PRODUCTION table (not staging) to ensure schema compatibility
        # We only merge columns that exist in production
        cols_production = get_columns("public", table, fb_connector)
        col_types_production = get_column_types("public", table, fb_connector)
        
        # Use intersection of columns (only columns present in both tables)
        cols_initial = [c for c in cols_production if c in cols_staging]
"""

# ═══════════════════════════════════════════════════════════════════
# ADD IMPORT AT TOP OF FILE
# ═══════════════════════════════════════════════════════════════════

"""
ADD THIS IMPORT (after line 5):

from datetime import datetime
"""

