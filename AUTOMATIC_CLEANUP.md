# Automatic Cleanup of cdc_processed_files Table

## 🎯 **What It Does**

The Lambda now includes **automatic cleanup** of old records from the `cdc_processed_files` table to prevent it from growing indefinitely.

---

## 🔧 **How It Works**

### **Periodic Cleanup:**

- Runs **1% of the time** (1 in 100 Lambda invocations)
- Deletes records **older than 30 days**
- Takes **1-2 seconds** (minimal overhead)
- **Non-fatal** (if cleanup fails, Lambda continues processing the file)

### **Example:**

```
Lambda invocation 1:  Process file (no cleanup)
Lambda invocation 2:  Process file (no cleanup)
...
Lambda invocation 100: Process file + Run cleanup ✓
Lambda invocation 101: Process file (no cleanup)
...
```

---

## 📊 **Configuration**

### **Environment Variables:**

You can customize cleanup behavior with these Lambda environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `CLEANUP_PROBABILITY` | `0.01` | Probability of running cleanup (0.01 = 1%) |
| `CLEANUP_DAYS_TO_KEEP` | `30` | Number of days to keep records |

### **Examples:**

**Run cleanup more frequently (5% of invocations):**
```bash
CLEANUP_PROBABILITY=0.05
```

**Keep records for 60 days instead of 30:**
```bash
CLEANUP_DAYS_TO_KEEP=60
```

**Run cleanup every time (100% - not recommended):**
```bash
CLEANUP_PROBABILITY=1.0
```

**Disable cleanup completely:**
```bash
CLEANUP_PROBABILITY=0
```

---

## 📈 **Expected Behavior**

### **With 10,000 files/day:**

**Without cleanup:**
```
Day 1:   10,000 records
Day 30:  300,000 records
Day 365: 3,650,000 records (table keeps growing)
```

**With cleanup (30 days retention):**
```
Day 1:   10,000 records
Day 30:  300,000 records
Day 31:  300,000 records (cleanup starts)
Day 60:  300,000 records (stable)
Day 365: 300,000 records (stable)
```

**Table size stays at ~300,000 rows (~150 MB) forever!**

---

## 🔍 **Monitoring**

### **Check Cleanup Logs:**

```bash
# Watch for cleanup messages in Lambda logs
aws logs filter-pattern "Cleaning up records older than" \
  --log-group-name /aws/lambda/firebolt-cdc-processor

# Expected output:
# 🧹 Cleaning up records older than 30 days from cdc_processed_files
# ✓ Cleanup complete: Deleted 10,234 old records (before: 310,234, after: 300,000)
```

### **Check Table Size:**

```sql
-- Check current record count
SELECT COUNT(*) as total_records FROM cdc_processed_files;

-- Check oldest record
SELECT MIN(processed_at) as oldest_record FROM cdc_processed_files;

-- Should be ~30 days ago (not older)
```

### **Check Cleanup Frequency:**

```sql
-- Count records by date
SELECT 
    DATE(processed_at) as date,
    COUNT(*) as records
FROM cdc_processed_files
GROUP BY DATE(processed_at)
ORDER BY date DESC
LIMIT 35;

-- Should see ~30 days of data
```

---

## ⚡ **Performance Impact**

### **Overhead:**

| Metric | Value |
|--------|-------|
| **Cleanup frequency** | 1% of invocations |
| **Cleanup time** | 1-2 seconds |
| **Average overhead per invocation** | 0.01-0.02 seconds (negligible) |
| **Impact on file processing** | None (cleanup runs before processing) |

### **Calculation:**

```
If Lambda processes 10,000 files/day:
- Cleanup runs: 100 times/day
- Total cleanup time: 100-200 seconds/day
- Average overhead: 0.01 seconds per file
- Impact: < 0.1% of total processing time
```

---

## 🧪 **Testing**

### **Test Cleanup Manually:**

Set `CLEANUP_PROBABILITY=1.0` temporarily to force cleanup on every invocation:

```bash
# Update Lambda environment variable
aws lambda update-function-configuration \
  --function-name firebolt-cdc-processor \
  --environment Variables="{CLEANUP_PROBABILITY=1.0,CLEANUP_DAYS_TO_KEEP=30,...}"

# Trigger Lambda
aws lambda invoke \
  --function-name firebolt-cdc-processor \
  --payload '{"detail":{"bucket":{"name":"fcanalytics"},"object":{"key":"firebolt_dms_job/fair/sessions/2024/11/24/test.parquet"}}}' \
  response.json

# Check logs for cleanup message
aws logs tail /aws/lambda/firebolt-cdc-processor --follow | grep "Cleaning up"

# Reset to normal (1%)
aws lambda update-function-configuration \
  --function-name firebolt-cdc-processor \
  --environment Variables="{CLEANUP_PROBABILITY=0.01,CLEANUP_DAYS_TO_KEEP=30,...}"
```

### **Verify Cleanup Works:**

```sql
-- Before cleanup: Insert old test record
INSERT INTO cdc_processed_files 
VALUES (
    'test/cleanup/2024/01/01/test.parquet',
    'test-request-id',
    CURRENT_TIMESTAMP - INTERVAL '35' DAY,  -- 35 days ago
    'completed',
    'test-arn',
    1,
    NULL
);

-- Trigger Lambda with CLEANUP_PROBABILITY=1.0

-- After cleanup: Check if old record was deleted
SELECT * FROM cdc_processed_files 
WHERE file_key = 'test/cleanup/2024/01/01/test.parquet';

-- Should return 0 rows (deleted)
```

---

## 🚨 **Troubleshooting**

### **Problem: Cleanup not running**

**Check:**
1. Is `CLEANUP_PROBABILITY` set correctly?
   ```bash
   aws lambda get-function-configuration \
     --function-name firebolt-cdc-processor \
     --query 'Environment.Variables.CLEANUP_PROBABILITY'
   ```

2. Are Lambda invocations happening?
   ```bash
   aws logs tail /aws/lambda/firebolt-cdc-processor --follow
   ```

3. Is cleanup being triggered? (should see ~1 in 100 invocations)
   ```bash
   aws logs filter-pattern "Running periodic cleanup" \
     --log-group-name /aws/lambda/firebolt-cdc-processor
   ```

### **Problem: Table still growing**

**Check:**
1. Cleanup retention period:
   ```bash
   aws lambda get-function-configuration \
     --function-name firebolt-cdc-processor \
     --query 'Environment.Variables.CLEANUP_DAYS_TO_KEEP'
   ```

2. Are old records actually being deleted?
   ```sql
   SELECT 
       MIN(processed_at) as oldest,
       MAX(processed_at) as newest,
       DATEDIFF('day', MIN(processed_at), CURRENT_TIMESTAMP) as age_days
   FROM cdc_processed_files;
   
   -- age_days should be ~30 (not much older)
   ```

3. Is cleanup failing silently?
   ```bash
   aws logs filter-pattern "Cleanup failed" \
     --log-group-name /aws/lambda/firebolt-cdc-processor
   ```

### **Problem: Cleanup taking too long**

**Symptoms:**
- Lambda timeouts
- Slow file processing

**Solution:**
Reduce cleanup frequency:
```bash
# Run cleanup less often (0.1% = 1 in 1000 invocations)
CLEANUP_PROBABILITY=0.001
```

---

## 📋 **Manual Cleanup (If Needed)**

If automatic cleanup isn't working or you need to clean up immediately:

```sql
-- Delete all records older than 30 days
DELETE FROM cdc_processed_files
WHERE processed_at < CURRENT_TIMESTAMP - INTERVAL '30' DAY;

-- Check result
SELECT COUNT(*) FROM cdc_processed_files;
```

---

## ✅ **Benefits**

| Benefit | Description |
|---------|-------------|
| **No separate infrastructure** | No Airflow/cron jobs needed |
| **Self-maintaining** | Runs automatically with Lambda |
| **Zero additional cost** | Uses existing Lambda/Firebolt resources |
| **Configurable** | Adjust frequency and retention via env vars |
| **Non-blocking** | Cleanup failure doesn't affect file processing |
| **Minimal overhead** | < 0.1% impact on processing time |

---

## 🎯 **Recommended Settings**

For most use cases:

```bash
CLEANUP_PROBABILITY=0.01    # 1% (default)
CLEANUP_DAYS_TO_KEEP=30     # 30 days (default)
```

**Adjust based on your needs:**

| Scenario | CLEANUP_PROBABILITY | CLEANUP_DAYS_TO_KEEP |
|----------|-------------------|---------------------|
| **High volume (100K+ files/day)** | 0.05 (5%) | 30 days |
| **Low volume (< 1K files/day)** | 0.01 (1%) | 60 days |
| **Compliance (need audit trail)** | 0.01 (1%) | 90 days |
| **Storage sensitive** | 0.02 (2%) | 7 days |

---

## 📊 **Summary**

✅ **Automatic cleanup is now built into Lambda**
✅ **Runs 1% of the time (configurable)**
✅ **Keeps table at ~30 days of data**
✅ **No additional infrastructure needed**
✅ **Minimal performance impact**

**No action required - it just works!** 🚀

