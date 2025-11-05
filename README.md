# 🚀 Firebolt CDC Lambda

[![AWS CDK](https://img.shields.io/badge/AWS-CDK-orange.svg)](https://aws.amazon.com/cdk/)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![Cross Platform](https://img.shields.io/badge/platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey.svg)](CROSS_PLATFORM_GUIDE.md)

Automated AWS Lambda for processing CDC files from S3 into Firebolt using AWS CDK.

**Cross-platform support:** macOS, Linux, and Windows

##  Features

-  **One-command deployment** - Deploy with `./scripts/deploy.sh`
-  **S3 Auto-trigger** - Processes new `.parquet` files automatically  
-  **Concurrent safe** - Multiple files processed simultaneously
-  **Transaction safe** - MERGE with rollback support
-  **Infrastructure as Code** - AWS CDK (Python)

##  Prerequisites

⚠️ **IMPORTANT:** Python 3.9+ required (3.11+ recommended)
- `firebolt-sdk` requires Python 3.9 or higher
- If you have Python 3.8 or older, see [Python Upgrade Guide](#python-version-requirements)

🔐 **IAM Permissions Required:**
- See [IAM Permissions Guide](IAM_PERMISSIONS_REQUIRED.md) for complete list
- Quick option: Attach `AWSLambda_FullAccess`, `IAMFullAccess`, `AmazonS3FullAccess`
- Secure option: Use the custom least-privilege policy in the guide

**Required:**
- AWS CLI configured (`aws configure`)
- AWS CDK CLI: `npm install -g aws-cdk`
- Python 3.11+ (3.9 minimum)
- Node.js 18+ (for CDK)
- AWS IAM permissions (see above)

**Detailed setup guides:**
- [Linux Setup Guide](LINUX_SETUP.md) - Complete Linux installation guide
- [Cross-Platform Guide](CROSS_PLATFORM_GUIDE.md) - macOS, Linux, Windows
- [IAM Permissions Guide](IAM_PERMISSIONS_REQUIRED.md) - Required AWS permissions

##  Quick Start

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

** Detailed guide:** See [CROSS_PLATFORM_GUIDE.md](CROSS_PLATFORM_GUIDE.md)

##  Documentation

### Setup Guides
- [Linux Setup Guide](LINUX_SETUP.md) - Complete Linux installation & deployment
- [Cross-Platform Guide](CROSS_PLATFORM_GUIDE.md) - macOS, Linux, Windows
- [Configuration Setup](CONFIG_SETUP.md) - Table keys and S3 configuration
- [IAM Permissions Guide](IAM_PERMISSIONS_REQUIRED.md) - Required AWS permissions

### Quick References
- [Quick Start](QUICKSTART.md) - Fast deployment
- [GitHub Setup](GITHUB_SETUP.md) - Create GitHub page
- [Important S3 Config](IMPORTANT_S3_CONFIG.txt) - S3 location setup

##  Troubleshooting

### Python Version Requirements

**Error:** `firebolt-sdk requires python > 3.9`

**Solution:** Upgrade Python to 3.9 or higher (3.11+ recommended)

**Quick Fixes:**

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install python3.11 python3.11-venv -y
```

**macOS:**
```bash
brew install python@3.11
```

**Windows:**
- Download from: https://www.python.org/downloads/
- Or: `winget install Python.Python.3.11`

**Verify:**
```bash
python3 --version  # Should show 3.9+ (3.11+ recommended)
```

For detailed upgrade instructions, see the full [Python Upgrade Guide](../PYTHON_UPGRADE_GUIDE.md).

### Other Common Issues

**Error:** `pip: command not found`
- **Solution:** Script now uses `pip3` (fixed in latest version)
- Update: `git pull origin main`

**Error:** `cdk: command not found`
- **Solution:** Install AWS CDK: `npm install -g aws-cdk`

**Error:** `Access Denied` / IAM permissions
- **Solution:** Ensure IAM user has required permissions (see documentation)

---

##  Cleanup

**macOS / Linux:**
```bash
./scripts/destroy.sh
```

**Windows:**
```powershell
.\scripts\destroy.ps1    # PowerShell
scripts\destroy.bat      # Command Prompt
```
---

**Made for Firebolt CDC Pipelines** ❤️

