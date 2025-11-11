# Selective Lambda Re-trigger Guide

## 🎯 Can I Reload Specific Tables Only?

**YES!** You have 3 options to selectively reload tables.

---

## ✅ **Option 1: Python Script with Table List (RECOMMENDED)**

### Quick Start

Edit `retrigger_lambda_selective.py` and specify tables:

```python
TABLES_TO_RELOAD = [
    'cent_borrower_transaction',
    'cent_communications_log',
    'cent_emi',
    'users',
    'sessions',
]
```

Then run:

```bash
python3 retrigger_lambda_selective.py
```

### Or Pass Tables as Arguments

```bash
python3 retrigger_lambda_selective.py cent_user sessions cent_emi
```

### What It Does

- ✅ Lists ALL parquet files in S3
- ✅ Filters only files for specified tables
- ✅ Invokes Lambda for those files only
- ✅ Shows per-table file counts
- ✅ Progress tracking

### Example Output

```
📊 Files per table:
   cent_borrower_transaction: 1250 files
   cent_communications_log: 3400 files
   cent_emi: 2100 files
   users: 450 files
   sessions: 8900 files

🚀 Starting Lambda invocations for 16,100 files...
```

---

## ✅ **Option 2: Bash Script for Single Table**

### Quick Start

```bash
./retrigger_single_table.sh cent_user
```

### What It Does

- ✅ Lists parquet files for ONE table only
- ✅ Invokes Lambda for all files of that table
- ✅ Shows available tables if table not found
- ✅ Simple and fast

### Example

```bash
# Reload just the 'users' table
./retrigger_single_table.sh users

# Reload just 'sessions'
./retrigger_single_table.sh sessions

# Reload just 'cent_emi'
./retrigger_single_table.sh cent_emi
```

---

## ✅ **Option 3: Manual AWS CLI (For Testing)**

### Reload a Single File

```bash
# Find a file for specific table
aws s3 ls s3://fcanalytics/firebolt_dms_job/fair/cent_user/2025/11/11/ \
  --region ap-south-1

# Invoke Lambda for that specific file
aws lambda invoke \
  --function-name firebolt-cdc-processor \
  --invocation-type Event \
  --region ap-south-1 \
  --payload '{
    "Records": [{
      "s3": {
        "bucket": {"name": "fcanalytics"},
        "object": {"key": "firebolt_dms_job/fair/cent_user/2025/11/11/20251111-123456.parquet"}
      }
    }]
  }' \
  response.json
```

### Test First, Then Batch

```bash
# 1. Test with ONE file
./retrigger_single_table.sh cent_user  # (modify to process 1 file only)

# 2. Check logs
aws logs tail /aws/lambda/firebolt-cdc-processor --region ap-south-1

# 3. If successful, run full batch
./retrigger_single_table.sh cent_user
```

---

## 📊 **Comparison**

| Method | Use Case | Speed | Ease |
|--------|----------|-------|------|
| **Python Script (Multiple Tables)** | Reload 5-10 specific tables | Fast (parallel) | Easy ✅ |
| **Bash Script (Single Table)** | Reload 1 table | Medium | Very Easy ✅ |
| **Manual AWS CLI** | Test single file | N/A | Manual ⚠️ |
| **Full Reload** (`retrigger_lambda_for_old_files.py`) | Reload ALL 809 tables | Slow | Easy |

---

## 🎯 **Common Scenarios**

### Scenario 1: Fix DECIMAL Errors for 9 Tables

After running `FIX_9_DECIMAL_TABLES.sql`, reload those 9 tables:

```python
# Edit retrigger_lambda_selective.py
TABLES_TO_RELOAD = [
    'cent_borrower_transaction',
    'cent_communications_log',
    'cent_ekyc_verification_logs',
    'cent_emi',
    'cent_emi_err_log',
    'cent_inv_escrow_upi_transaction',
    'cent_pre_borrower_transaction',
    'cent_user_thirdparty_ekyc_details',
    'payment_source',
]
```

```bash
python3 retrigger_lambda_selective.py
```

**Time:** ~5 minutes (vs. 15 min for all tables)

---

### Scenario 2: Reload High-Contention Tables

Tables failing with transaction conflicts (`users`, `sessions`):

```bash
# Reload users first
./retrigger_single_table.sh users

# Wait 10 minutes, then reload sessions
./retrigger_single_table.sh sessions
```

**Why?** Avoids overwhelming Firebolt engine with concurrent writes to same tables.

---

### Scenario 3: Test Lambda Fix on 1 Table

Before full reload, test Lambda code changes:

```bash
# Test on small table first
./retrigger_single_table.sh cent_sms_status

# Check logs for errors
aws logs tail /aws/lambda/firebolt-cdc-processor --region ap-south-1

# If successful, reload all
python3 retrigger_lambda_for_old_files.py
```

---

### Scenario 4: Reload Only Tables Modified After Date

Reload tables that received new data after schema fix:

```bash
# Find tables with files after Nov 11
aws s3 ls s3://fcanalytics/firebolt_dms_job/fair/ \
  --recursive \
  --region ap-south-1 \
  | grep "2025/11/11" \
  | awk -F'/' '{print $3}' \
  | sort -u

# Use output to populate TABLES_TO_RELOAD in script
```

---

## 🔧 **How to Check Which Tables Need Reload**

### Option 1: Compare Row Counts

```sql
-- In Firebolt, check row count
SELECT COUNT(*) FROM "public"."cent_user";

-- Compare with old backup table
SELECT COUNT(*) FROM "public"."cent_user_old_20251110";

-- If new table has FEWER rows → needs reload
```

### Option 2: Check Lambda Logs for Failures

```bash
# Find tables that failed to process
aws logs filter-log-events \
  --log-group-name /aws/lambda/firebolt-cdc-processor \
  --filter-pattern "MERGE failed" \
  --region ap-south-1 \
  --start-time $(date -u -d '1 hour ago' +%s)000 \
  | grep -oP 'MERGE failed for \K\w+' \
  | sort -u
```

### Option 3: Check Firebolt for Empty Tables

```sql
-- Find tables with 0 rows (need reload)
SELECT 
    table_name,
    number_of_rows
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name LIKE 'cent_%'
  AND number_of_rows = 0
ORDER BY table_name;
```

---

## ⚠️ **Important Notes**

### Lambda Concurrency

When reloading specific tables, Lambda concurrency still applies:

- If reloading 5 tables with 1000 files each = 5000 Lambda invocations
- With concurrency = 10, max 10 files processed simultaneously
- High-contention tables (`users`, `sessions`) may still conflict

**Solution:** Reload high-contention tables sequentially (one at a time).

### Staging Tables

Lambda creates temporary staging tables per file:

```
stg_<table_name>_<random_id>
```

These are dropped after MERGE. If Lambda fails, staging tables may remain.

**Cleanup:**

```sql
-- Find orphaned staging tables
SHOW TABLES LIKE 'stg_%';

-- Drop them
DROP TABLE "public"."stg_cent_user_abc123";
```

### Cost Implications

Selective reload = fewer Lambda invocations = lower cost:

- **All 809 tables:** ~50,000 files = $X cost
- **9 specific tables:** ~5,000 files = $X/10 cost
- **1 table:** ~500 files = $X/100 cost

---

## 📝 **Complete Workflow Example**

### After Schema Fix for 36 Tables

```bash
# 1. Fix schema in Firebolt
# Run: FIX_36_TABLES_SCHEMA.sql in Firebolt editor

# 2. Edit selective reload script
# Edit retrigger_lambda_selective.py:
#   TABLES_TO_RELOAD = [list of 36 tables]

# 3. Test with 1 table first
./retrigger_single_table.sh cent_aadhar_address_data

# 4. Check logs (wait 2 min)
aws logs tail /aws/lambda/firebolt-cdc-processor \
  --since 2m \
  --region ap-south-1 \
  | grep -E "✓|✗|ERROR"

# 5. If successful, reload all 36 tables
python3 retrigger_lambda_selective.py

# 6. Monitor progress
aws logs tail /aws/lambda/firebolt-cdc-processor \
  --follow \
  --region ap-south-1

# 7. Verify data in Firebolt
# SELECT COUNT(*) FROM "cent_aadhar_address_data";
# Compare with old backup table

# 8. Drop old backup tables after verification
# DROP TABLE "cent_aadhar_address_data_old_20251110";
```

---

## 🆘 **Troubleshooting**

### Script Says "Table Not Found"

```bash
# Check available tables in S3
aws s3 ls s3://fcanalytics/firebolt_dms_job/fair/ --region ap-south-1

# Make sure table name matches exactly
```

### No Files Found for Table

```bash
# Check if table has parquet files
aws s3 ls s3://fcanalytics/firebolt_dms_job/fair/cent_user/ \
  --recursive --region ap-south-1

# If empty, table has no data in S3 (no reload needed)
```

### Lambda Invocations Fail

```bash
# Check Lambda exists
aws lambda get-function \
  --function-name firebolt-cdc-processor \
  --region ap-south-1

# Check Lambda concurrency
aws lambda get-function-concurrency \
  --function-name firebolt-cdc-processor \
  --region ap-south-1
```

---

## ✅ **Summary**

| Goal | Command |
|------|---------|
| **Reload 1 table** | `./retrigger_single_table.sh <table>` |
| **Reload 5-10 specific tables** | Edit & run `retrigger_lambda_selective.py` |
| **Reload ALL tables** | `python3 retrigger_lambda_for_old_files.py` |
| **Test 1 file** | Manual AWS CLI command |

---

**Bottom Line: You can absolutely reload specific tables selectively. Use the Python or Bash scripts above based on your needs!** 🎯

