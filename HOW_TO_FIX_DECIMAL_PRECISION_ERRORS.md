# 🔧 How to Fix DECIMAL Precision Mismatch Errors

## 🚨 Error You're Seeing

```
numeric(38, 0) can't be assigned to column loan_id of the type numeric(20, 0)
```

**What this means:**
- Your **Parquet files** have `loan_id` as `numeric(38, 0)`
- Your **Firebolt production table** has `loan_id` as `numeric(20, 0)`
- Firebolt won't allow assignment between different DECIMAL precisions

---

## ✅ Quick Fix (For Single Table)

### **Table:** `cent_borrower_term_condition`

**Run this SQL in Firebolt:**

```sql
-- 1. Backup existing table
ALTER TABLE "public"."cent_borrower_term_condition" 
RENAME TO "cent_borrower_term_condition_backup_20251111";

-- 2. Create new table with correct schema from Parquet
CREATE TABLE "public"."cent_borrower_term_condition" AS
SELECT * EXCLUDE ("Op", "load_timestamp")
FROM READ_PARQUET(
    LOCATION => 's3_raw_dms',
    PATTERN => 'fair/cent_borrower_term_condition/**/*.parquet'
)
LIMIT 0;

-- 3. Verify schema
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'cent_borrower_term_condition'
ORDER BY ordinal_position;
```

**Done!** Lambda will now process new files successfully.

---

## 🔍 Find Other Tables With This Issue

**Run this SQL to identify all tables with DECIMAL mismatches:**

```sql
WITH parquet_schemas AS (
    -- Get schema from ONE parquet file per table
    SELECT 
        'users' as table_name,
        's3://fcanalytics/firebolt_dms_job/fair/users/**/*.parquet' as s3_path
    UNION ALL
    SELECT 'cent_borrower_term_condition', 
           's3://fcanalytics/firebolt_dms_job/fair/cent_borrower_term_condition/**/*.parquet'
    -- Add more tables as needed...
)
SELECT 
    ps.table_name,
    c.column_name,
    c.data_type as firebolt_type,
    'Check manually' as parquet_type
FROM information_schema.columns c
JOIN parquet_schemas ps ON ps.table_name = c.table_name
WHERE c.table_schema = 'public'
  AND (c.data_type LIKE '%numeric%' OR c.data_type LIKE '%decimal%')
ORDER BY ps.table_name, c.ordinal_position;
```

---

## 📋 Standard Fix Template (For Any Table)

**Replace `<TABLE_NAME>` with your table:**

```sql
-- Backup
ALTER TABLE "public"."<TABLE_NAME>" 
RENAME TO "<TABLE_NAME>_backup_$(date +%Y%m%d)";

-- Recreate with correct schema
CREATE TABLE "public"."<TABLE_NAME>" AS
SELECT * EXCLUDE ("Op", "load_timestamp")
FROM READ_PARQUET(
    LOCATION => 's3_raw_dms',
    PATTERN => 'fair/<TABLE_NAME>/**/*.parquet'
)
LIMIT 0;

-- Verify
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = '<TABLE_NAME>'
ORDER BY ordinal_position;
```

---

## 🤔 Should I Copy Old Data?

**Question:** Do I need to copy data from the backup table?

**Answer:** 
- ✅ **NO** - If you plan to retrigger Lambda for old files (recommended)
- ⚠️ **YES** - If you want to keep existing data and only process new files

**To copy old data (if needed):**
```sql
INSERT INTO "public"."<TABLE_NAME>"
SELECT * FROM "public"."<TABLE_NAME>_backup_$(date +%Y%m%d)";
```

---

## 🚀 What Lambda Will Do Now

**After the Lambda fix (already implemented):**

1. ✅ **Skip DECIMAL columns with mismatches** from MERGE
2. ✅ **Still process other columns** successfully
3. ⚠️ **Log warning** about skipped columns
4. ✅ **Continue processing** (won't crash)

**Log you'll see:**
```
⚠️  Skipping DECIMAL column 'loan_id' from MERGE due to type mismatch: prod=numeric(20,0), stg=numeric(38,0)
⚠️  1 DECIMAL columns removed from MERGE: ['loan_id (prod: numeric(20,0), stg: numeric(38,0))']
✓ MERGE completed for cent_borrower_term_condition (234 rows affected)
```

**But to fix properly:** Run the schema fix SQL above so ALL columns are included.

---

## 📊 Summary

| Scenario | Action |
|----------|--------|
| **Single table error** | Run SQL fix for that table |
| **Multiple tables** | Run fix for each table individually |
| **Want to preserve data** | Copy from backup after recreating |
| **Want fresh data** | Retrigger Lambda for old files |

---

## 🎯 Next Steps

1. ✅ **Run the SQL fix** for `cent_borrower_term_condition`
2. ✅ **Deploy latest Lambda** (already filters DECIMAL mismatches)
3. ✅ **Monitor logs** for other tables with similar issues
4. ✅ **Run schema fix** for any other affected tables

---

**File to run NOW:**
```
fix_cent_borrower_term_condition.sql
```

**Monitor for other tables:**
```bash
aws logs tail /aws/lambda/firebolt-cdc-processor \
  --since 10m \
  --region ap-south-1 \
  | grep "Skipping DECIMAL column"
```

