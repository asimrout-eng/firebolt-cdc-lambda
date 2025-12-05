# ⚠️ Important: Lambda Environment Variables

## Issue Found

The CDK stack was **missing** `FIREBOLT_CLIENT_ID` and `FIREBOLT_CLIENT_SECRET` in the environment variables configuration.

**Impact**: If you deploy without these variables, the Lambda will:
1. Try to use `FIREBOLT_USERNAME` and `FIREBOLT_PASSWORD` (which may be empty)
2. Fail to connect to Firebolt
3. Overwrite any manually set `CLIENT_ID`/`CLIENT_SECRET` in the Lambda console

---

## ✅ Fix Applied

I've updated:
1. **`config/config.py`** - Added `firebolt_client_id` and `firebolt_client_secret` to config
2. **`stacks/firebolt_cdc_stack.py`** - Added `FIREBOLT_CLIENT_ID` and `FIREBOLT_CLIENT_SECRET` to Lambda environment variables

---

## 🔧 Before Deploying: Set Environment Variables

### Option 1: Set in `.env` file (Recommended)

Add to your `.env` file in `firebolt-cdk-package/`:

```bash
# Firebolt Client Credentials (Service Account)
FIREBOLT_CLIENT_ID=bDnqfj5uDfygACM7ByLH285LPd8ny4wI
FIREBOLT_CLIENT_SECRET=mRws0ftFkF1EiCJBvoc_K_vT_c77g6ySTIpc7aQywzoqS7WjfuVlu5W0hyMdy0ls

# OR use Username/Password (if preferred)
# FIREBOLT_USERNAME=your@email.com
# FIREBOLT_PASSWORD=your_password
```

### Option 2: Set in Lambda Console (Manual)

1. Go to Lambda → `firebolt-cdc-processor` → Configuration → Environment variables
2. Add:
   - `FIREBOLT_CLIENT_ID` = `bDnqfj5uDfygACM7ByLH285LPd8ny4wI`
   - `FIREBOLT_CLIENT_SECRET` = `mRws0ftFkF1EiCJBvoc_K_vT_c77g6ySTIpc7aQywzoqS7WjfuVlu5W0hyMdy0ls`

**⚠️ Warning**: If you deploy after setting manually, CDK will overwrite with values from `.env` file (or empty if not set).

---

## 🚀 Safe Deployment Steps

### Step 1: Update `.env` file

```bash
cd firebolt-cdk-package

# Edit .env file
nano .env  # or vim, code, etc.
```

Add:
```bash
FIREBOLT_CLIENT_ID=bDnqfj5uDfygACM7ByLH285LPd8ny4wI
FIREBOLT_CLIENT_SECRET=mRws0ftFkF1EiCJBvoc_K_vT_c77g6ySTIpc7aQywzoqS7WjfuVlu5W0hyMdy0ls
FIREBOLT_ACCOUNT=faircentindia
FIREBOLT_DATABASE=fair
FIREBOLT_ENGINE=support_test_db
```

### Step 2: Verify Config

```bash
# Test that config loads correctly
python3 -c "from config.config import get_config; import json; print(json.dumps(get_config(), indent=2, default=str))"
```

Check that `firebolt_client_id` and `firebolt_client_secret` are populated.

### Step 3: Deploy

```bash
./scripts/deploy.sh
```

**After deployment**, verify in Lambda console:
- Configuration → Environment variables
- Should see `FIREBOLT_CLIENT_ID` and `FIREBOLT_CLIENT_SECRET` set

---

## 🔍 How Lambda Chooses Authentication

The Lambda handler code (lines 49-76) uses this logic:

1. **If `FIREBOLT_CLIENT_ID` and `FIREBOLT_CLIENT_SECRET` are set** → Uses Client Credentials ✅
2. **Else if `FIREBOLT_USERNAME` and `FIREBOLT_PASSWORD` are set** → Uses Username/Password
3. **Else** → Raises error

**Priority**: Client Credentials > Username/Password

---

## ⚠️ Current State

**If you deploy now** (without updating `.env`):
- `FIREBOLT_CLIENT_ID` will be set to empty string `''`
- `FIREBOLT_CLIENT_SECRET` will be set to empty string `''`
- Lambda will fall back to `FIREBOLT_USERNAME`/`FIREBOLT_PASSWORD` (if set)
- **OR** Lambda will fail with authentication error

**Solution**: Set the values in `.env` file before deploying.

---

## 📝 Summary

✅ **Code updated** - CDK stack now includes CLIENT_ID and CLIENT_SECRET  
⚠️ **Action required** - Set values in `.env` file before deploying  
✅ **Safe to deploy** - After setting `.env`, deployment will preserve credentials

