# Schema Evolution Integration Patch

## Overview

This patch adds schema evolution tracking to your Lambda handler to automatically detect and handle:
- **New tables** added to the source
- **New columns** added to existing tables
- **Column type changes**
- **Removed columns**

---

## Step 1: Add Schema Evolution Functions

Add these functions to your `handler.py` (after the existing helper functions, around line 460):

```python
# ═══════════════════════════════════════════════════════════════════
# SCHEMA EVOLUTION FUNCTIONS
# ═══════════════════════════════════════════════════════════════════

def load_schema_metadata(table_name: str, bucket: str) -> Optional[Dict]:
    """Load last known schema metadata from S3"""
    if not bucket:
        return None
    try:
        key = f"firebolt_dms_job/schema_metadata/{table_name}_schema.json"
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
        key = f"firebolt_dms_job/schema_metadata/{table_name}_schema.json"
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
                logger.warning(f"  ➕ New columns: {[c['column'] for c in changes['new_columns']]}")
            if changes['removed_columns']:
                logger.warning(f"  ➖ Removed columns: {[c['column'] for c in changes['removed_columns']]}")
            if changes['type_changes']:
                logger.warning(f"  🔄 Type changes: {[f\"{c['column']}: {c['old_type']} → {c['new_type']}\" for c in changes['type_changes']]}")
        
        save_schema_metadata(table, current_schema, bucket, {
            'staging_table': staging_table,
            'changes_detected': changes,
            'columns_count': len(cols_staging)
        })
    except Exception as e:
        logger.error(f"Error tracking schema evolution for {table}: {e}")
```

**Also add this import at the top:**
```python
from datetime import datetime
```

---

## Step 2: Add New Table Detection

**Location:** After line 724 (after `keys = table_keys.get(table)`)

**Add this code:**

```python
    # Get primary keys for this table
    keys = table_keys.get(table)
    
    # ═══════════════════════════════════════════════════════════════════
    # SCHEMA EVOLUTION: Detect new tables
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
        # ... existing null PK handling code ...
```

---

## Step 3: Add Schema Tracking After COPY

**Location:** After line 868 (after `fb_connector.execute(copy_sql)`)

**Add this code:**

```python
        # COPY single file to staging (AUTO_CREATE will infer schema from parquet)
        copy_sql = render_copy_single_file(staging_table, table, date_path, filename, location, database)
        fb_connector.execute(copy_sql)
        logger.info(f"✓ Copied {filename} to {staging_table} (auto-created from parquet schema)")
        
        # ═══════════════════════════════════════════════════════════════════
        # SCHEMA EVOLUTION: Track schema changes
        # ═══════════════════════════════════════════════════════════════════
        # (This will be called again after we get col_types_staging, but we can track here too)
        
        # Get columns and data types from staging table
        cols_staging = get_columns("public", staging_table, fb_connector)
        col_types_staging = get_column_types("public", staging_table, fb_connector)
        col_types_production = get_column_types("public", table, fb_connector)
        
        # Track schema evolution (after we have staging schema)
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
        
        # Use intersection of columns (only columns present in both tables)
        cols_initial = [c for c in cols_production if c in cols_staging]
```

**Note:** You'll need to move the `cols_staging` and `col_types_staging` lines up before the schema tracking call.

---

## Step 4: Set Environment Variables

Add to your Lambda environment variables:

```bash
SCHEMA_EVOLUTION_ENABLED=true
SCHEMA_ALERT_SNS_TOPIC=arn:aws:sns:us-east-1:123456789:schema-alerts  # Optional
```

---

## How It Works

### Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Lambda receives S3 event for CDC file                    │
│ 2. Check if table exists in table_keys.json                │
│    ├─ Yes → Use configured PK                              │
│    └─ No → Auto-detect PK from Firebolt                    │
│ 3. COPY parquet → staging table (AUTO_CREATE)              │
│ 4. Get schema from staging table                           │
│ 5. Compare with last known schema (from S3)                │
│ 6. Detect changes:                                         │
│    ├─ New columns?                                         │
│    ├─ Removed columns?                                     │
│    └─ Type changes?                                         │
│ 7. Save new schema to S3                                   │
│ 8. Continue with MERGE (existing logic)                    │
└─────────────────────────────────────────────────────────────┘
```

---

## Benefits

1. **Automatic Detection**: New tables detected when CDC files arrive
2. **Auto-Configuration**: Primary keys auto-detected when possible
3. **Change Tracking**: Schema changes logged and stored
4. **Non-Breaking**: Doesn't interfere with existing CDC logic
5. **Optional**: Can be enabled/disabled via environment variable

---

## Testing

1. **Test new table detection:**
   - Create a new table in source
   - Let DMS create parquet files
   - Lambda should auto-detect PK

2. **Test schema changes:**
   - Add a new column to existing table
   - Process CDC file
   - Check logs for schema change warning
   - Verify schema metadata in S3

---

## Files

- `lambda_schema_evolution_integration.py` - Integration functions
- `schema_evolution_tracker.py` - Standalone tracker script
- `LAMBDA_SCHEMA_EVOLUTION_PATCH.md` - This guide

