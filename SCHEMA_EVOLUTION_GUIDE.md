# Schema Evolution Tracking Guide

## Overview

This guide explains how to track and handle schema evolution (new tables, DDL changes) in your Firebolt CDC Lambda pipeline.

---

## Problem Statement

When customers add new tables or modify existing table schemas (DDL changes), your Lambda needs to:

1. **Detect new tables** automatically
2. **Track schema changes** (new columns, removed columns, type changes)
3. **Auto-configure primary keys** for new tables when possible
4. **Alert on schema mismatches** that could break CDC
5. **Handle schema evolution gracefully** without breaking existing pipelines

---

## Solution Architecture

### Components

1. **Schema Evolution Tracker** (`schema_evolution_tracker.py`)
   - Standalone script to scan Firebolt and detect changes
   - Can be run on schedule (e.g., daily via EventBridge)
   - Auto-updates `table_keys.json` with new tables

2. **Lambda Schema Handler** (`lambda_schema_evolution_handler.py`)
   - Integrated into your Lambda handler
   - Tracks schema changes in real-time during CDC processing
   - Detects new columns from staging table schema

3. **Schema Metadata Storage** (S3)
   - Stores last known schema for each table
   - Enables change detection over time
   - Location: `s3://fcanalytics/firebolt_dms_job/schema_metadata/{table_name}_schema.json`

---

## Implementation Steps

### Step 1: Add Schema Tracking to Lambda

Add this code to your Lambda handler (`firebolt-cdk-package/lambda/handler.py`):

```python
# At the top of handler.py, add:
from lambda_schema_evolution_handler import (
    track_schema_evolution,
    check_new_table,
    handle_new_table,
    SCHEMA_EVOLUTION_ENABLED
)

# In handler() function, after COPY to staging succeeds:
# (around line 850, after staging table is created)

# Track schema evolution
if SCHEMA_EVOLUTION_ENABLED:
    track_schema_evolution(table, staging_table, fb_connector, table_keys)
```

### Step 2: Handle New Tables

Add this check after loading `table_keys.json`:

```python
# After: keys = table_keys.get(table)

# Check if this is a new table
if check_new_table(table, table_keys):
    logger.info(f"🆕 New table detected: {table}")
    pk = handle_new_table(table, fb_connector, table_keys)
    
    if pk:
        # Auto-configure primary key
        table_keys[table] = pk
        logger.info(f"✅ Auto-configured {table} with PK: {pk}")
        
        # Optionally: Save updated table_keys to S3
        # (You'd need to add this function)
    else:
        logger.warning(f"⚠️  {table} has no primary key - CDC will be skipped")
```

### Step 3: Set Environment Variables

Add to your Lambda environment variables:

```bash
SCHEMA_EVOLUTION_ENABLED=true
SCHEMA_METADATA_BUCKET=fcanalytics
SCHEMA_ALERT_SNS_TOPIC=arn:aws:sns:us-east-1:123456789:schema-alerts  # Optional
```

### Step 4: Schedule Schema Tracker

Set up EventBridge rule to run schema tracker daily:

```bash
# Run daily at 2 AM UTC
aws events put-rule \
  --name schema-evolution-tracker-daily \
  --schedule-expression "cron(0 2 * * ? *)" \
  --state ENABLED

# Add Lambda as target
aws events put-targets \
  --rule schema-evolution-tracker-daily \
  --targets "Id=1,Arn=arn:aws:lambda:us-east-1:123456789:function:schema-tracker"
```

---

## How It Works

### 1. Real-Time Detection (During Lambda Execution)

When Lambda processes a CDC file:

```
┌─────────────────────────────────────────────────────────────┐
│ 1. COPY parquet → staging table                            │
│ 2. Get schema from staging table                            │
│ 3. Compare with last known schema (from S3)                │
│ 4. Detect changes:                                         │
│    - New columns?                                           │
│    - Removed columns?                                       │
│    - Type changes?                                          │
│ 5. Save new schema to S3                                   │
│ 6. Send alert if changes detected                           │
└─────────────────────────────────────────────────────────────┘
```

### 2. Batch Detection (Scheduled Tracker)

Daily scan of all tables:

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Get all tables from Firebolt                            │
│ 2. Compare with table_keys.json                            │
│ 3. Detect new tables                                        │
│ 4. Auto-detect primary keys                                 │
│ 5. Update table_keys.json                                   │
│ 6. Generate report                                          │
└─────────────────────────────────────────────────────────────┘
```

---

## Handling Different Scenarios

### Scenario 1: New Table Added

**What happens:**
- DMS creates parquet files for new table
- Lambda receives S3 event
- `check_new_table()` returns `True`
- `handle_new_table()` tries to detect primary key
- If PK found: Auto-configured in `table_keys.json`
- If no PK: Table marked as `null` in `table_keys.json` (CDC skipped)

**Action required:**
- None if PK auto-detected
- Manual configuration if no PK found

### Scenario 2: New Column Added

**What happens:**
- New column appears in parquet files
- Staging table has new column (via `AUTO_CREATE = TRUE`)
- Production table doesn't have new column yet
- Lambda detects mismatch during column comparison
- Alert sent (if SNS configured)
- Schema metadata updated

**Action required:**
- Option 1: Let Lambda handle it (new column ignored in MERGE)
- Option 2: Manually add column to production table:
  ```sql
  ALTER TABLE "cent_user" ADD COLUMN "new_column" TEXT;
  ```

### Scenario 3: Column Type Changed

**What happens:**
- Column type changes in source (e.g., INT → BIGINT)
- Parquet file has new type
- Staging table has new type
- Production table has old type
- Lambda detects type mismatch
- Alert sent
- MERGE may fail if types incompatible

**Action required:**
- Manually alter production table:
  ```sql
  ALTER TABLE "cent_user" ALTER COLUMN "id" TYPE BIGINT;
  ```

### Scenario 4: Column Removed

**What happens:**
- Column removed from source
- Parquet files no longer have column
- Production table still has column
- Lambda detects removed column
- Alert sent
- MERGE continues (missing columns handled gracefully)

**Action required:**
- Option 1: Keep column in Firebolt (harmless)
- Option 2: Remove column manually:
  ```sql
  ALTER TABLE "cent_user" DROP COLUMN "old_column";
  ```

---

## Monitoring & Alerts

### CloudWatch Metrics

Track these metrics:

- `SchemaEvolution/NewTables` - Count of new tables detected
- `SchemaEvolution/SchemaChanges` - Count of schema changes
- `SchemaEvolution/AutoConfigured` - Count of auto-configured tables

### SNS Alerts

Configure SNS topic for alerts:

```json
{
  "alert_type": "schema_change",
  "table_name": "cent_user",
  "timestamp": "2025-01-15T10:30:00Z",
  "changes": {
    "new_columns": [{"column": "new_field", "type": "TEXT"}],
    "removed_columns": [],
    "type_changes": []
  }
}
```

---

## Best Practices

### 1. Run Schema Tracker Regularly

- **Daily**: Detect new tables and major changes
- **Weekly**: Generate comprehensive reports
- **On-demand**: Before major deployments

### 2. Review Auto-Configurations

- Always review auto-configured primary keys
- Verify they're correct for your use case
- Update manually if needed

### 3. Handle Type Changes Proactively

- Monitor alerts for type changes
- Plan migration strategy for incompatible changes
- Test in staging first

### 4. Keep Schema Metadata

- Don't delete schema metadata files
- Use them for historical analysis
- Compare schemas over time

---

## Troubleshooting

### Issue: New table not detected

**Solution:**
- Check if table exists in Firebolt
- Verify `table_keys.json` permissions
- Run schema tracker manually

### Issue: Schema changes not detected

**Solution:**
- Verify `SCHEMA_EVOLUTION_ENABLED=true`
- Check S3 permissions for schema metadata
- Review Lambda logs for errors

### Issue: Auto-configuration fails

**Solution:**
- Check if table has primary index in Firebolt
- Verify table exists before processing
- Review Lambda logs for error details

---

## Files Created

1. **`schema_evolution_tracker.py`** - Standalone tracker script
2. **`lambda_schema_evolution_handler.py`** - Lambda integration code
3. **`SCHEMA_EVOLUTION_GUIDE.md`** - This guide

---

## Next Steps

1. ✅ Review the code
2. ✅ Test schema tracker locally
3. ✅ Integrate into Lambda handler
4. ✅ Set up scheduled tracker
5. ✅ Configure alerts
6. ✅ Monitor and iterate

---

## Questions?

- Check Lambda logs for schema tracking errors
- Review S3 schema metadata files
- Run schema tracker manually for debugging

