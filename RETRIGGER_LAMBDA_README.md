# Re-trigger Lambda for Old S3 Files

## 🎯 Purpose

After fixing table schemas in Firebolt, re-trigger Lambda to reload data from existing S3 files.

---

## 📋 Prerequisites

1. ✅ Latest Lambda code deployed (`./scripts/deploy.sh`)
2. ✅ Schema fix SQL executed in Firebolt (`FIX_36_TABLES_SCHEMA.sql` or `FIX_9_DECIMAL_TABLES.sql`)
3. ✅ AWS credentials configured (`aws configure`)

---

## 🚀 Quick Start (One Command)

### **Option 1: Python Script (RECOMMENDED - Fast)**

```bash
pip3 install boto3
python3 retrigger_lambda_for_old_files.py
```

**Time:** ~15 minutes for 3000 files  
**Features:** Parallel processing, progress tracking, error handling

---

### **Option 2: Bash Script (Alternative)**

```bash
./retrigger_lambda.sh
```

**Time:** ~60 minutes for 3000 files  
**Features:** No Python dependencies, sequential processing

---

## 🧪 Test with One File First (RECOMMENDED)

Before running the full batch, test with a single file:

```bash
# 1. Find a test file
aws s3 ls s3://fcanalytics/firebolt_dms_job/fair/cent_user/2025/11/10/ \
  --region ap-south-1 | head -1

# Output example: 20251110-123456.parquet

# 2. Invoke Lambda for that file
aws lambda invoke \
  --function-name firebolt-cdc-processor \
  --invocation-type Event \
  --region ap-south-1 \
  --payload '{
    "Records": [{
      "s3": {
        "bucket": {"name": "fcanalytics"},
        "object": {"key": "firebolt_dms_job/fair/cent_user/2025/11/10/20251110-123456.parquet"}
      }
    }]
  }' \
  response.json

# 3. Monitor logs
aws logs tail /aws/lambda/firebolt-cdc-processor --follow --region ap-south-1

# 4. Check data in Firebolt
# Run in Firebolt SQL editor: SELECT COUNT(*) FROM "cent_user";
```

**If test succeeds → Run full batch (Option 1 or 2)**  
**If test fails → Fix issue, then retry**

---

## 📊 Monitor Progress

### Real-time CloudWatch Logs

```bash
aws logs tail /aws/lambda/firebolt-cdc-processor \
  --follow \
  --region ap-south-1 \
  | grep -E "✓|✗|ERROR"
```

### Check Data in Firebolt

Run this query every 5 minutes:

```sql
SELECT 
  COUNT(*) as row_count,
  MAX(created) as latest_timestamp
FROM "public"."cent_user";
```

---

## ⚙️ Configuration

Scripts use these defaults (modify in script if needed):

```python
S3_BUCKET = 'fcanalytics'
S3_PREFIX = 'firebolt_dms_job/'
LAMBDA_FUNCTION = 'firebolt-cdc-processor'
AWS_REGION = 'ap-south-1'
MAX_WORKERS = 10  # Parallel workers (Python script only)
```

---

## 🛠️ Troubleshooting

### "AccessDeniedException"
**Fix:** Configure AWS credentials
```bash
aws configure
# Enter Access Key ID, Secret Access Key, Region: ap-south-1
```

### Lambda Throttling (429 errors)
**Fix:** Reduce parallel workers in `retrigger_lambda_for_old_files.py`:
```python
MAX_WORKERS = 5  # Reduce from 10 to 5
```

### "Table not found" in Lambda logs
**Fix:** Ensure schema fix SQL was executed first:
- Run `FIX_36_TABLES_SCHEMA.sql` in Firebolt
- Verify tables exist: `SHOW TABLES;`

### DECIMAL errors still appear
**Fix:** Schema fix didn't work properly
```sql
-- Check if new tables were created
SHOW COLUMNS FROM "cent_user";

-- Verify old backup tables exist
SHOW TABLES LIKE '%_old_20251110';
```

---

## 📁 Files

- `retrigger_lambda_for_old_files.py` - Python script (recommended)
- `retrigger_lambda.sh` - Bash script (alternative)
- `FIX_36_TABLES_SCHEMA.sql` - Schema fix for 36 tables
- `FIX_9_DECIMAL_TABLES.sql` - Schema fix for 9 critical DECIMAL tables
- `RETRIGGER_LAMBDA_README.md` - This file

---

## 🔄 Complete Workflow

```bash
# 1. Deploy latest Lambda code
./scripts/deploy.sh

# 2. Fix schema in Firebolt (run in Firebolt SQL editor)
# Copy contents of FIX_36_TABLES_SCHEMA.sql and execute

# 3. Test with 1 file (see "Test with One File First" section above)

# 4. Re-trigger Lambda for all files
pip3 install boto3
python3 retrigger_lambda_for_old_files.py

# 5. Monitor progress
aws logs tail /aws/lambda/firebolt-cdc-processor --follow --region ap-south-1

# 6. Verify data in Firebolt
# SELECT COUNT(*) FROM "cent_user";
```

---

## ⏱️ Expected Timeline

| Step | Time |
|------|------|
| Deploy Lambda | 5 min |
| Fix schema SQL | 3 min |
| Test 1 file | 2 min |
| Re-trigger all files | 15 min (Python) |
| Monitor | 30 min |
| Verify data | 5 min |
| **Total** | **~60 min** |

---

## ✅ Success Criteria

After completion:

1. ✓ All Lambda invocations succeeded
2. ✓ No DECIMAL errors in CloudWatch logs
3. ✓ Data counts match:
   ```sql
   SELECT COUNT(*) FROM "cent_user_old_20251110";  -- Old table
   SELECT COUNT(*) FROM "cent_user";                -- New table (should match)
   ```

---

## 🆘 Need Help?

Check CloudWatch logs for detailed error messages:

```bash
aws logs tail /aws/lambda/firebolt-cdc-processor \
  --since 1h \
  --region ap-south-1 \
  | grep ERROR
```

---

**Happy Re-triggering!** 🚀

