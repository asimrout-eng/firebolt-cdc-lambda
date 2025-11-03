# 🚀 Firebolt CDC Lambda

[![AWS CDK](https://img.shields.io/badge/AWS-CDK-orange.svg)](https://aws.amazon.com/cdk/)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![Cross Platform](https://img.shields.io/badge/platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey.svg)](CROSS_PLATFORM_GUIDE.md)

Automated AWS Lambda for processing CDC files from S3 into Firebolt using AWS CDK.

**✅ Cross-platform support:** macOS, Linux, and Windows

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

**📖 Detailed setup guides:**
- [Linux Setup Guide](LINUX_SETUP.md) - Complete Linux installation guide
- [Cross-Platform Guide](CROSS_PLATFORM_GUIDE.md) - macOS, Linux, Windows

## 🚀 Quick Start

### macOS / Linux

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
```

### Windows (PowerShell)

```powershell
# 1. Configure credentials
Copy-Item .env.example .env
notepad .env  # Add your Firebolt credentials

# 2. Deploy
.\scripts\deploy.ps1

# 3. Upload table keys
aws s3 cp config\tables_keys.json s3://fcanalytics/firebolt-migration/config/tables_keys.json
```

### Windows (Command Prompt)

```cmd
REM 1. Configure credentials
copy .env.example .env
notepad .env

REM 2. Deploy
scripts\deploy.bat

REM 3. Upload table keys
aws s3 cp config\tables_keys.json s3://fcanalytics/firebolt-migration/config/tables_keys.json
```

**📖 Detailed guide:** See [CROSS_PLATFORM_GUIDE.md](CROSS_PLATFORM_GUIDE.md)

## 📖 Documentation

### Setup Guides
- [Linux Setup Guide](LINUX_SETUP.md) - Complete Linux installation & deployment
- [Cross-Platform Guide](CROSS_PLATFORM_GUIDE.md) - macOS, Linux, Windows
- [Configuration Setup](CONFIG_SETUP.md) - Table keys and S3 configuration

### Quick References
- [Quick Start](QUICKSTART.md) - Fast deployment
- [GitHub Setup](GITHUB_SETUP.md) - Create GitHub page
- [Important S3 Config](IMPORTANT_S3_CONFIG.txt) - S3 location setup

## 🗑️ Cleanup

**macOS / Linux:**
```bash
./scripts/destroy.sh
```

**Windows:**
```powershell
.\scripts\destroy.ps1    # PowerShell
scripts\destroy.bat      # Command Prompt
```

## 📄 License

MIT

---

**Made for Firebolt CDC Pipelines** ❤️

