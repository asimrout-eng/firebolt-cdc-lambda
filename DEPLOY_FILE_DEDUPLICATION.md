# Deploy File Deduplication - Complete Guide

## ✅ **What Was Implemented**

**File deduplication using Firebolt metadata table** to prevent duplicate processing of S3 files.

**Changes:**
1. ✅ Created `cdc_processed_files` table in Firebolt
2. ✅ Added deduplication functions to Lambda handler
3. ✅ Lambda now checks/marks files before/after processing

---

## 📋 **Deployment Steps**

### **Step 1: Verify Metadata Table (Already Done ✅)**

The table `cdc_processed_files` has been created in Firebolt.

**Verify:**
```sql
SELECT * FROM cdc_processed_files LIMIT 10;
```

---

### **Step 2: Test Lambda Code Locally (Optional)**

```bash
cd /Users/asimkumarrout/Documents/Firebolt/Python/firebolt-cdk-package

# Check for syntax errors
python3 -m py_compile lambda/handler.py

# If no errors, you're good to deploy!
```

---

### **Step 3: Deploy to AWS Lambda**

**Option A: Using CDK (Recommended)**

```bash
cd /Users/asimkumarrout/Documents/Firebolt/Python/firebolt-cdk-package

# Deploy
cdk deploy

# Or if you have a deploy script
./scripts/deploy.sh
```

**Option B: Manual Deployment**

```bash
# Package Lambda
cd lambda
zip -r ../lambda-deployment.zip .

# Upload to Lambda (replace with your function name)
aws lambda update-function-code \
  --function-name firebolt-cdc-processor \
  --zip-file fileb://../lambda-deployment.zip
```

---

### **Step 4: Test with a Real File**

**Trigger Lambda manually:**

```bash
# Get a recent S3 file
aws s3 ls s3://fcanalytics/firebolt_dms_job/fair/ --recursive | tail -5

# Trigger Lambda with that file
aws lambda invoke \
  --function-name firebolt-cdc-processor \
  --payload '{
    "detail": {
      "bucket": {"name": "fcanalytics"},
      "object": {"key": "firebolt_dms_job/fair/sessions/2024/11/24/20241124-123456.parquet"}
    }
  }' \
  response.json

# Check response
cat response.json
```

**Expected output (first time):**
```json
{
  "status": "success",
  "message": "File processed successfully"
}
```

**Expected output (second time - duplicate):**
```json
{
  "statusCode": 200,
  "body": "{\"message\": \"File already processed\", \"file_key\": \"fair/sessions/2024/11/24/20241124-123456.parquet\", \"status\": \"completed\"}"
}
```

---

### **Step 5: Monitor Logs**

```bash
# Watch Lambda logs
aws logs tail /aws/lambda/firebolt-cdc-processor --follow

# Look for these messages:
# ✓ "STEP 1: CHECK IF FILE ALREADY PROCESSED"
# ✓ "STEP 2: MARK FILE AS PROCESSING"
# ✓ "STEP 3: PROCESS FILE"
# ✓ "STEP 4: MARK FILE AS COMPLETED"
```

---

### **Step 6: Verify Deduplication is Working**

```sql
-- Check processed files
SELECT 
    file_key,
    status,
    processed_at,
    request_id
FROM cdc_processed_files
ORDER BY processed_at DESC
LIMIT 20;

-- Count by status
SELECT 
    status,
    COUNT(*) as count
FROM cdc_processed_files
GROUP BY status;

-- Find any failed files
SELECT 
    file_key,
    error_message,
    attempt_count,
    processed_at
FROM cdc_processed_files
WHERE status = 'failed'
ORDER BY processed_at DESC;
```

---

## 🔍 **How to Verify It's Working**

### **Test 1: Process a file twice**

```bash
# Process file first time
aws lambda invoke --function-name firebolt-cdc-processor \
  --payload '{"detail":{"bucket":{"name":"fcanalytics"},"object":{"key":"firebolt_dms_job/fair/test_table/2024/11/24/test.parquet"}}}' \
  response1.json

# Process same file again (should skip)
aws lambda invoke --function-name firebolt-cdc-processor \
  --payload '{"detail":{"bucket":{"name":"fcanalytics"},"object":{"key":"firebolt_dms_job/fair/test_table/2024/11/24/test.parquet"}}}' \
  response2.json

# Compare responses
cat response1.json  # Should say "processed successfully"
cat response2.json  # Should say "already processed"
```

### **Test 2: Check for duplicates in production tables**

```sql
-- Before deduplication (you had duplicates)
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT "sid") as unique_sids,
    COUNT(*) - COUNT(DISTINCT "sid") as duplicates
FROM sessions;

-- After a few hours of running with deduplication
-- Duplicates should stop increasing!
```

---

## 📊 **Monitoring Queries**

### **Query 1: Files processed today**

```sql
SELECT 
    COUNT(*) as files_processed_today,
    COUNT(DISTINCT LEFT(file_key, POSITION('/' IN file_key, POSITION('/' IN file_key) + 1))) as tables_affected
FROM cdc_processed_files
WHERE DATE(processed_at) = CURRENT_DATE;
```

### **Query 2: Processing rate**

```sql
SELECT 
    DATE(processed_at) as date,
    COUNT(*) as files_processed,
    COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful,
    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed
FROM cdc_processed_files
WHERE processed_at >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY DATE(processed_at)
ORDER BY date DESC;
```

### **Query 3: Duplicate prevention stats**

```sql
-- This query won't work directly, but conceptually:
-- Count how many times Lambda tried to process same file
SELECT 
    file_key,
    COUNT(*) as duplicate_attempts
FROM (
    -- You'd need to track this in Lambda logs
    -- For now, just check if files are marked as "already processed"
    SELECT file_key FROM cdc_processed_files WHERE status = 'completed'
)
GROUP BY file_key
HAVING COUNT(*) > 1;
```

---

## 🧹 **Maintenance**

### **Clean up old records (run monthly)**

```sql
-- Delete records older than 30 days
DELETE FROM cdc_processed_files
WHERE processed_at < CURRENT_TIMESTAMP - INTERVAL '30' DAY;

-- Check table size
SELECT 
    COUNT(*) as total_records,
    MIN(processed_at) as oldest_record,
    MAX(processed_at) as newest_record
FROM cdc_processed_files;
```

### **Retry failed files**

```sql
-- Find failed files
SELECT file_key, error_message
FROM cdc_processed_files
WHERE status = 'failed'
ORDER BY processed_at DESC;

-- Reset failed files to allow retry
UPDATE cdc_processed_files
SET status = 'pending', error_message = NULL
WHERE status = 'failed'
  AND processed_at < CURRENT_TIMESTAMP - INTERVAL '1' HOUR;
```

---

## 🚨 **Troubleshooting**

### **Problem: Lambda still creating duplicates**

**Check:**
1. Is the metadata table accessible?
   ```sql
   SELECT COUNT(*) FROM cdc_processed_files;
   ```

2. Are deduplication functions being called?
   ```bash
   aws logs filter-pattern "STEP 1: CHECK IF FILE" \
     --log-group-name /aws/lambda/firebolt-cdc-processor
   ```

3. Are there errors in deduplication logic?
   ```bash
   aws logs filter-pattern "Error checking if file processed" \
     --log-group-name /aws/lambda/firebolt-cdc-processor
   ```

### **Problem: Files marked as "processing" forever**

**Cause:** Lambda timed out or crashed

**Fix:**
```sql
-- Find stuck files (processing > 15 min)
SELECT file_key, processed_at
FROM cdc_processed_files
WHERE status = 'processing'
  AND processed_at < CURRENT_TIMESTAMP - INTERVAL '15' MINUTE;

-- Reset them
UPDATE cdc_processed_files
SET status = 'failed', error_message = 'Lambda timeout'
WHERE status = 'processing'
  AND processed_at < CURRENT_TIMESTAMP - INTERVAL '15' MINUTE;
```

### **Problem: Table growing too large**

**Check size:**
```sql
SELECT 
    COUNT(*) as row_count,
    COUNT(*) * 200 / 1024 / 1024 as estimated_size_mb
FROM cdc_processed_files;
```

**Fix:** Run cleanup (see Maintenance section above)

---

## 📈 **Success Metrics**

Track these metrics after deployment:

| Metric | Before | Target After |
|--------|--------|--------------|
| Duplicate files processed | 10-20% | < 0.1% |
| Duplicates in production tables | Growing | Stopped |
| Lambda failures due to duplicates | 5-10/day | 0 |
| Engineer time on cleanup | 2 hours/week | 0 |

---

## ✅ **Rollback Plan**

If something goes wrong:

### **Option 1: Disable deduplication (quick fix)**

```python
# In Lambda handler, comment out deduplication check:
# is_processed, status = is_file_processed(file_key, fb_connector)
# if is_processed:
#     return ...

# Redeploy Lambda
```

### **Option 2: Revert to previous Lambda version**

```bash
# List versions
aws lambda list-versions-by-function \
  --function-name firebolt-cdc-processor

# Revert to previous version
aws lambda update-alias \
  --function-name firebolt-cdc-processor \
  --name PROD \
  --function-version <previous-version>
```

---

## 🎉 **Deployment Checklist**

- [x] Metadata table created in Firebolt
- [ ] Lambda code deployed to AWS
- [ ] Test with duplicate file (should skip second time)
- [ ] Monitor logs for deduplication messages
- [ ] Verify no new duplicates in production tables
- [ ] Set up monthly cleanup job
- [ ] Document for team

---

## 📞 **Support**

If you encounter issues:

1. Check Lambda logs: `aws logs tail /aws/lambda/firebolt-cdc-processor --follow`
2. Check metadata table: `SELECT * FROM cdc_processed_files ORDER BY processed_at DESC LIMIT 10`
3. Check for duplicates: `SELECT COUNT(*) - COUNT(DISTINCT "sid") FROM sessions`

---

**Ready to deploy? Run:**

```bash
cd /Users/asimkumarrout/Documents/Firebolt/Python/firebolt-cdk-package
cdk deploy
```

**Or:**

```bash
./scripts/deploy.sh
```

🚀 **Good luck!**

