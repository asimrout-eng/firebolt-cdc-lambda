# 🚀 Firebolt CDC Lambda

[![AWS CDK](https://img.shields.io/badge/AWS-CDK-orange.svg)](https://aws.amazon.com/cdk/)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)

Automated AWS Lambda for processing CDC files from S3 into Firebolt using AWS CDK.

## ✨ Features

- ✅ **One-command deployment** - Deploy with `./scripts/deploy.sh`
- ✅ **S3 Auto-trigger** - Processes new `.parquet` files automatically  
- ✅ **Concurrent safe** - Multiple files processed simultaneously
- ✅ **Transaction safe** - MERGE with rollback support
- ✅ **Infrastructure as Code** - AWS CDK (Python)

## 📋 Prerequisites

- AWS CLI configured
- AWS CDK CLI: `npm install -g aws-cdk`
- Python 3.11+

## 🚀 Quick Start

```bash
# 1. Configure credentials
cp .env.example .env
nano .env  # Add your Firebolt credentials

# 2. Deploy
chmod +x scripts/*.sh
./scripts/deploy.sh

# 3. Upload table keys
aws s3 cp config/tables_keys.json \
  s3://fcanalytics/firebolt-migration/config/tables_keys.json

# Done! 🎉
```

## 📖 Documentation

See detailed docs in `/docs/` folder (coming soon).

## 🗑️ Cleanup

```bash
./scripts/destroy.sh
```

## 📄 License

MIT

---

**Made for Firebolt CDC Pipelines** ❤️

