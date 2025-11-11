# 🚨 Lambda Not Updated - Client Deployment Fix

## Issue

When you ran `./scripts/deploy.sh` at **11:31 AM IST**, CDK said:

```
✅  FireboltCdcStack (no changes)
```

This means **new Lambda code was NOT deployed**. You're still running old code with errors.

---

## ✅ Solution: Force Lambda Code Update

### **Step 1: Verify You Have Latest Code**

```bash
cd firebolt-cdk-package

# Check git commit
git log --oneline -1
# Should show: 311a943 Add selective table reload scripts

# Verify fix is in code
grep -n 'if "Cannot" in commit_msg:' lambda/handler.py
# Should show: 253:                    if "Cannot" in commit_msg:
```

**If both checks pass** → You have latest code, proceed to Step 2  
**If either fails** → Run `git pull origin main` first

---

### **Step 2: Clean CDK Cache**

```bash
cd firebolt-cdk-package

# Remove CDK cache
rm -rf cdk.out lambda-layer

echo "✓ Cache cleaned"
```

---

### **Step 3: Force Redeploy**

```bash
cd firebolt-cdk-package

# Run deployment script
./scripts/deploy.sh
```

**Expected output (last few lines):**
```
✅  FireboltCdcStack

✨  Deployment time: 45.2s

Outputs:
FireboltCdcStack.LambdaFunctionArn = arn:aws:lambda:ap-south-1:...
FireboltCdcStack.LambdaFunctionName = firebolt-cdc-processor
```

**Look for:** Deployment time > 30 seconds (indicates actual deployment)

**If still says "no changes":** Continue to Step 4

---

### **Step 4: Manual Lambda Update (If CDK Fails)**

```bash
cd firebolt-cdk-package

# Create deployment package
rm -rf lambda-package lambda-code.zip
mkdir lambda-package
cp lambda/handler.py lambda-package/

# Install dependencies
pip3 install -r lambda/requirements.txt -t lambda-package/ \
  --platform manylinux2014_x86_64 \
  --only-binary=:all: \
  --python-version 3.11

# Create zip
cd lambda-package
zip -r ../lambda-code.zip .
cd ..

# Update Lambda directly
aws lambda update-function-code \
  --function-name firebolt-cdc-processor \
  --zip-file fileb://lambda-code.zip \
  --region ap-south-1

# Clean up
rm -rf lambda-package lambda-code.zip

echo "✓ Lambda code updated directly"
```

---

### **Step 5: Force Lambda Restart**

After deployment, old Lambda containers may still run for 10-15 minutes. Force restart:

```bash
# Force Lambda to restart
aws lambda update-function-configuration \
  --function-name firebolt-cdc-processor \
  --description "Force restart at $(date -u +%Y-%m-%dT%H:%M:%S)" \
  --region ap-south-1

echo "✓ Lambda will restart in 30 seconds"
sleep 30
```

---

### **Step 6: Verify New Code is Running**

Wait **2 minutes** for new S3 events to trigger Lambda, then:

```bash
# Check recent logs
aws logs tail /aws/lambda/firebolt-cdc-processor \
  --since 2m \
  --region ap-south-1 \
  | grep -E "Processing S3|✓ MERGE|✗ MERGE|cannot COMMIT" \
  | head -30
```

**Look for:**

✅ **NEW CODE (Good):**
```
Processing S3 key: fair/cent_user/2025/11/11/20251111-123456.parquet
✓ Copied 150 rows to staging table
✓ MERGE completed for cent_user (150 rows affected)
✓ Dropped staging table
```

❌ **OLD CODE (Bad - still running):**
```
BEGIN
MERGE INTO "public"."cent_user" ...
COMMIT
Error: cannot COMMIT transaction: no transaction is in progress
```

---

### **Step 7: Monitor Ongoing**

```bash
# Real-time monitoring
aws logs tail /aws/lambda/firebolt-cdc-processor \
  --follow \
  --region ap-south-1 \
  | grep -E "Processing|✓ MERGE|✗ MERGE"
```

**Expected:** Should see mostly ✓ successes, very few ✗ errors

---

## 📊 Success Metrics

After fix is applied (wait 15 minutes):

| Metric | Before | After (Target) |
|--------|--------|----------------|
| **"cannot COMMIT" errors** | 100% | **0%** |
| **"cannot be retried" errors** | 30% | **< 5%** |
| **Success Rate** | 0-70% | **> 95%** |
| **Tables updating** | Few | **All 809 tables** |

---

## 🔍 Troubleshooting

### **Q: Still seeing "cannot COMMIT" errors after 15 minutes?**

**A:** Old Lambda containers are still running. Options:

1. **Wait another 5-10 minutes** (old containers will die)
2. **Disable and re-enable Lambda trigger:**
   ```bash
   # (This forces all containers to restart)
   aws lambda update-function-configuration \
     --function-name firebolt-cdc-processor \
     --description "Force all containers to restart" \
     --region ap-south-1
   ```

---

### **Q: CDK keeps saying "no changes"?**

**A:** Use Step 4 (Manual Lambda Update) instead.

---

### **Q: Getting "Query cannot be retried" errors?**

**A:** This is different from "cannot COMMIT". These are transaction conflicts.

**Solution:** Reduce Lambda concurrency to 5:

```bash
aws lambda put-function-concurrency \
  --function-name firebolt-cdc-processor \
  --reserved-concurrent-executions 5 \
  --region ap-south-1
```

---

## ⏱️ Timeline

| Time | Action | Duration |
|------|--------|----------|
| **Now** | Run Steps 1-3 (Force deploy) | 5 min |
| **+ 5 min** | Step 5 (Force restart) | 1 min |
| **+ 8 min** | Wait for old containers to die | 10 min |
| **+ 18 min** | Step 6-7 (Verify & monitor) | Ongoing |
| **Total** | **~20 minutes** to full resolution |

---

## 📞 Support

If after following all steps you still see errors:

1. Export logs:
   ```bash
   aws logs filter-log-events \
     --log-group-name /aws/lambda/firebolt-cdc-processor \
     --start-time $(date -u -d '30 minutes ago' +%s)000 \
     --region ap-south-1 \
     > lambda_logs_30min.json
   ```

2. Share the file for analysis

---

**Created:** 2025-11-11  
**Priority:** Critical  
**Est. Time to Fix:** 20 minutes

