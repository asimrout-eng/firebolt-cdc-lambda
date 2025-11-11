# 🔍 Check Firebolt Python SDK Version

## Quick Answer

**GitHub Repo Specifies:** `firebolt-sdk>=1.0.0` (any version 1.0.0 or newer)

**To find what's actually deployed in Lambda:**

---

## ✅ Method 1: Check Lambda Logs (After Latest Update)

**Latest code (commit 9611128) logs SDK version on startup!**

```bash
# Pull latest code
cd <your-repo-folder>
git pull origin main

# Deploy (this will log SDK version on next Lambda invocation)
# See CLIENT_RUN_THIS_NOW.txt for deployment commands

# After deployment, check logs
aws logs tail /aws/lambda/firebolt-cdc-processor \
  --since 10m \
  --region ap-south-1 \
  | grep "Firebolt Python SDK Version"
```

**Expected output:**
```
🔧 Firebolt Python SDK Version: 1.2.3
```

---

## 📦 Method 2: Check requirements.txt

```bash
cd <your-repo-folder>
cat lambda/requirements.txt
```

**Shows:**
```
firebolt-sdk>=1.0.0
pyarrow>=10.0.0
```

This means Lambda will install the **latest SDK version available** when deployed.

---

## 🔎 Method 3: Check What Was Actually Installed

### **Option A: During Deployment**

When you run `./scripts/deploy.sh`, watch for:
```
Successfully installed firebolt-sdk-1.2.3 pyarrow-14.0.1 ...
```

### **Option B: Check Lambda Package**

```bash
# Get Lambda deployment package details
aws lambda get-function \
  --function-name firebolt-cdc-processor \
  --region ap-south-1 \
  --query 'Configuration.[CodeSize, LastModified]' \
  --output table
```

### **Option C: Download and Inspect Lambda Package**

```bash
# Get Lambda code URL
CODE_URL=$(aws lambda get-function \
  --function-name firebolt-cdc-processor \
  --region ap-south-1 \
  --query 'Code.Location' \
  --output text)

# Download
curl -o /tmp/lambda-code.zip "$CODE_URL"

# Extract and check
cd /tmp
unzip -q lambda-code.zip
pip show firebolt-sdk 2>/dev/null || \
  find . -name "firebolt_sdk-*.dist-info" | head -1 | grep -oP '\d+\.\d+\.\d+'
```

---

## 🎯 Method 4: Force Lambda to Print Version

Create a test invocation:

```bash
# Invoke Lambda with test payload
aws lambda invoke \
  --function-name firebolt-cdc-processor \
  --region ap-south-1 \
  --payload '{"test_version": true}' \
  /tmp/lambda-response.json

# Check the logs
aws logs tail /aws/lambda/firebolt-cdc-processor \
  --since 2m \
  --region ap-south-1 \
  | grep -E "SDK Version|firebolt-sdk"
```

---

## 📊 Common SDK Versions

| Version | Release Date | Notes |
|---------|--------------|-------|
| **1.0.0** | 2023-03 | First stable release |
| **1.1.0** | 2023-06 | Performance improvements |
| **1.2.0** | 2023-09 | Bug fixes |
| **1.3.0** | 2024-01 | Latest stable |

---

## ⚠️ Why Version Matters

Different SDK versions may have:
- Different transaction handling behavior
- Different error messages
- Different retry logic
- Different authentication methods

**Latest code (commit 9611128) now logs SDK version automatically!**

---

## 🆕 After Latest Update

Once you deploy the latest code (commit 9611128):

```bash
# Deploy latest
cd <your-repo-folder>
git pull origin main
# Run deployment (see CLIENT_RUN_THIS_NOW.txt)

# Wait for new Lambda invocation
sleep 60

# Check SDK version
aws logs tail /aws/lambda/firebolt-cdc-processor \
  --since 5m \
  --region ap-south-1 \
  | grep "🔧 Firebolt Python SDK Version"
```

**You'll see:**
```
🔧 Firebolt Python SDK Version: 1.x.x
```

This will appear in logs **every time Lambda cold starts** (new container).

---

## 🔧 Update SDK Version

To update to a specific version:

```bash
cd <your-repo-folder>

# Edit lambda/requirements.txt
echo "firebolt-sdk==1.3.0" > lambda/requirements.txt
echo "pyarrow>=10.0.0" >> lambda/requirements.txt

# Commit and push
git add lambda/requirements.txt
git commit -m "Update firebolt-sdk to 1.3.0"
git push origin main

# Deploy
# See CLIENT_RUN_THIS_NOW.txt for deployment commands
```

---

## 📝 Summary

**Current GitHub Spec:** `firebolt-sdk>=1.0.0` (any version ≥ 1.0.0)

**To check deployed version:** Deploy latest code (commit 9611128) and check logs for:
```
🔧 Firebolt Python SDK Version: X.Y.Z
```

**After next deployment, SDK version will be logged automatically!** 🚀

