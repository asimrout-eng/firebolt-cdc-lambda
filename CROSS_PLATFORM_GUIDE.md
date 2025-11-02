# 🖥️ Cross-Platform Deployment Guide

Deploy Firebolt CDC Lambda on **macOS**, **Linux**, or **Windows**.

---

## 📋 Prerequisites (All Platforms)

✅ AWS CLI configured  
✅ AWS CDK CLI: `npm install -g aws-cdk`  
✅ Python 3.11+  
✅ pip installed  

---

## 🚀 Quick Start

### 1️⃣ Configure Credentials (All Platforms)

**macOS / Linux:**
```bash
cd firebolt-cdk-package
cp .env.example .env
nano .env  # or vim, code, etc.
```

**Windows (PowerShell):**
```powershell
cd firebolt-cdk-package
Copy-Item .env.example .env
notepad .env  # or code .env
```

**Windows (Command Prompt):**
```cmd
cd firebolt-cdk-package
copy .env.example .env
notepad .env
```

Fill in required values:
```bash
FIREBOLT_DATABASE=your_database
FIREBOLT_ENGINE=your_engine
FIREBOLT_USERNAME=your@email.com
FIREBOLT_PASSWORD=your_password
```

---

### 2️⃣ Deploy to AWS

Choose your platform:

#### 🍎 macOS / 🐧 Linux

```bash
chmod +x scripts/*.sh
./scripts/deploy.sh
```

#### 🪟 Windows (PowerShell)

```powershell
.\scripts\deploy.ps1
```

#### 🪟 Windows (Command Prompt)

```cmd
scripts\deploy.bat
```

---

### 3️⃣ Upload Table Keys (All Platforms)

**macOS / Linux:**
```bash
# Create your table keys config
cp config/tables_keys.json.example config/tables_keys.json
nano config/tables_keys.json

# Upload to S3
aws s3 cp config/tables_keys.json \
  s3://fcanalytics/firebolt-migration/config/tables_keys.json
```

**Windows:**
```powershell
# Create your table keys config
Copy-Item config\tables_keys.json.example config\tables_keys.json
notepad config\tables_keys.json

# Upload to S3
aws s3 cp config\tables_keys.json s3://fcanalytics/firebolt-migration/config/tables_keys.json
```

---

## 🗑️ Cleanup / Destroy

### 🍎 macOS / 🐧 Linux

```bash
./scripts/destroy.sh
```

### 🪟 Windows (PowerShell)

```powershell
.\scripts\destroy.ps1
```

### 🪟 Windows (Command Prompt)

```cmd
scripts\destroy.bat
```

---

## 📊 Monitor Logs (All Platforms)

**All platforms use AWS CLI (same command):**

```bash
# Tail logs in real-time
aws logs tail /aws/lambda/firebolt-cdc-processor --follow

# View recent logs
aws logs tail /aws/lambda/firebolt-cdc-processor --since 1h
```

---

## 🧪 Test Deployment (All Platforms)

**Upload a test file:**

**macOS / Linux:**
```bash
aws s3 cp test.parquet \
  s3://fcanalytics/firebolt_dms_job/mysql/customers/20251102/test.parquet
```

**Windows:**
```powershell
aws s3 cp test.parquet s3://fcanalytics/firebolt_dms_job/mysql/customers/20251102/test.parquet
```

---

## 🛠️ Platform-Specific Notes

### macOS / Linux

- Uses bash scripts (`.sh`)
- Requires execute permissions: `chmod +x scripts/*.sh`
- Text editors: `nano`, `vim`, `code`

### Windows PowerShell

- Uses PowerShell scripts (`.ps1`)
- May require execution policy change:
  ```powershell
  Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
  ```
- Text editors: `notepad`, `code`

### Windows Command Prompt

- Uses batch scripts (`.bat`)
- No special permissions needed
- Text editors: `notepad`, `code`

---

## 📂 Available Scripts

| Platform | Deploy | Destroy |
|----------|--------|---------|
| macOS / Linux | `./scripts/deploy.sh` | `./scripts/destroy.sh` |
| Windows PowerShell | `.\scripts\deploy.ps1` | `.\scripts\destroy.ps1` |
| Windows CMD | `scripts\deploy.bat` | `scripts\destroy.bat` |

---

## ⚠️ Troubleshooting

### Python not found (Windows)

Make sure Python is in your PATH:
```powershell
python --version
pip --version
```

If not found, reinstall Python and check "Add to PATH" during installation.

### AWS CLI not found

**macOS:**
```bash
brew install awscli
```

**Linux:**
```bash
sudo apt-get install awscli
```

**Windows:**
Download from: https://aws.amazon.com/cli/

### CDK not found

**All platforms:**
```bash
npm install -g aws-cdk
cdk --version
```

### Permission denied (macOS/Linux)

```bash
chmod +x scripts/*.sh
```

### Execution Policy Error (Windows PowerShell)

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

---

## ✅ Platform Compatibility Matrix

| Feature | macOS | Linux | Windows |
|---------|-------|-------|---------|
| Deployment | ✅ | ✅ | ✅ |
| Destroy | ✅ | ✅ | ✅ |
| AWS CLI | ✅ | ✅ | ✅ |
| CDK | ✅ | ✅ | ✅ |
| Python 3.11 | ✅ | ✅ | ✅ |

---

## 🎯 Recommended Setup by Platform

### 🍎 macOS
- **Terminal:** iTerm2 or default Terminal
- **Editor:** VS Code, nano, vim
- **Package Manager:** Homebrew

### 🐧 Linux
- **Terminal:** Default (GNOME Terminal, Konsole, etc.)
- **Editor:** VS Code, nano, vim
- **Package Manager:** apt, yum, dnf

### 🪟 Windows
- **Terminal:** PowerShell 7 (recommended) or Command Prompt
- **Editor:** VS Code, Notepad++, Notepad
- **Package Manager:** Chocolatey (optional)

---

## 🆘 Need Help?

- 📖 [Quick Start Guide](QUICKSTART.md)
- 🐛 [GitHub Issues](https://github.com/asimrout-eng/firebolt-cdc-lambda/issues)
- 📧 Contact support

---

**All platforms supported!** 🎉

