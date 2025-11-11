# 🚨 URGENT: Lambda Not Updated - Quick Fix

## Problem

After running `./scripts/deploy.sh`, CDK said "no changes" and Lambda was **NOT updated**.

You're still running old code with errors:
```
cannot COMMIT transaction: no transaction is in progress
```

---

## ✅ Solution: Force Lambda Update

### **Option 1: Force CDK Redeploy**

```bash
# Navigate to your cloned repo folder
cd <your-repo-folder>  # e.g., cd firebolt-cdc-lambda

# Clean cache
rm -rf cdk.out lambda-layer

# Force deploy
./scripts/deploy.sh
```

**Look for:** Deployment time > 30 seconds (indicates actual deployment)

**If still says "no changes"** → Use Option 2 below

---

### **Option 2: Manual Lambda Update (Recommended)**

```bash
# Navigate to your cloned repo folder
cd <your-repo-folder>  # e.g., cd firebolt-cdc-lambda

# Create deployment package
mkdir -p lambda-package
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

# Update Lambda
aws lambda update-function-code \
  --function-name firebolt-cdc-processor \
  --zip-file fileb://lambda-code.zip \
  --region ap-south-1

# Verify (should show recent timestamp)
aws lambda get-function \
  --function-name firebolt-cdc-processor \
  --region ap-south-1 \
  --query 'Configuration.LastModified' \
  --output text

# Clean up
rm -rf lambda-package lambda-code.zip

echo "✅ Lambda updated!"
```

---

### **After Update: Force Lambda Restart**

```bash
# Force restart to kill old containers
aws lambda update-function-configuration \
  --function-name firebolt-cdc-processor \
  --description "Force restart at $(date -u +%Y-%m-%dT%H:%M:%S)" \
  --region ap-south-1

# Wait for new events
echo "⏳ Waiting 2 minutes for new Lambda invocations..."
sleep 120

# Check logs
aws logs tail /aws/lambda/firebolt-cdc-processor \
  --since 2m \
  --region ap-south-1 \
  | grep -E "Processing S3|✓ MERGE|✗ MERGE|cannot COMMIT" \
  | head -30
```

---

## 📊 Verify Success

**NEW CODE (Good) - You'll see:**
```
Processing S3 key: fair/cent_user/2025/11/11/20251111-123456.parquet
✓ Copied 150 rows to staging table
✓ MERGE completed for cent_user (150 rows affected)
✓ Dropped staging table
```

**OLD CODE (Bad) - You'll see:**
```
BEGIN
MERGE INTO "public"."cent_user" ...
COMMIT
Error: cannot COMMIT transaction: no transaction is in progress
```

---

## 📈 Monitor Ongoing

```bash
# Real-time monitoring
aws logs tail /aws/lambda/firebolt-cdc-processor \
  --follow \
  --region ap-south-1 \
  | grep -E "Processing|✓ MERGE|✗ MERGE"
```

**Expected:** Mostly ✓ successes, < 5% ✗ errors

---

## ⏱️ Timeline

| Step | Time |
|------|------|
| Manual Lambda update | 2 min |
| Force restart | 30 sec |
| Wait for new events | 2 min |
| Verify success | 1 min |
| **Total** | **~6 minutes** |

---

## 🆘 Still Not Working?

If after 10 minutes you still see "cannot COMMIT" errors:

```bash
# Export last 30 min of logs
aws logs filter-log-events \
  --log-group-name /aws/lambda/firebolt-cdc-processor \
  --start-time $(date -u -d '30 minutes ago' +%s)000 \
  --region ap-south-1 \
  > lambda_logs_issue.json

# Send lambda_logs_issue.json for analysis
```

---

**Recommendation:** Use **Option 2 (Manual Lambda Update)** - it's faster and always works.

