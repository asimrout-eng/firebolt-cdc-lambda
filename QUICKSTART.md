# Quick Start Guide

## 🚀 Get Started in 3 Steps

### 1. Configure Credentials

```bash
cd firebolt-cdk-package
cp .env.example .env
nano .env  # Add your Firebolt credentials
```

Required fields in `.env`:
```
FIREBOLT_DATABASE=your_database
FIREBOLT_ENGINE=your_engine
FIREBOLT_USERNAME=your@email.com
FIREBOLT_PASSWORD=your_password
```

### 2. Deploy to AWS

```bash
./scripts/deploy.sh
```

This will:
- Install dependencies
- Build Lambda layer
- Deploy to AWS

### 3. Upload Table Keys

```bash
# Copy example
cp config/tables_keys.json.example config/tables_keys.json
nano config/tables_keys.json  # Edit with your tables

# Upload to S3
aws s3 cp config/tables_keys.json \
  s3://fcanalytics/firebolt-migration/config/tables_keys.json
```

## ✅ Done!

Your Lambda will now automatically process `.parquet` files from:
```
s3://fcanalytics/firebolt_dms_job/**/*.parquet
```

## 📊 Monitor

```bash
# View logs
aws logs tail /aws/lambda/firebolt-cdc-processor --follow
```

## 🌐 Create GitHub Page

```bash
./setup-github.sh YOUR_GITHUB_USERNAME
```

Then enable GitHub Pages:
1. Go to `https://github.com/YOUR_USERNAME/firebolt-cdc-lambda/settings/pages`
2. Source: Deploy from branch
3. Branch: main, Folder: / (root)
4. Save

Your page: `https://YOUR_USERNAME.github.io/firebolt-cdc-lambda/`

## 🗑️ Cleanup

```bash
./scripts/destroy.sh
```

