# 🐍 Python Upgrade Guide - Fix for Firebolt SDK

## ❌ Error

```
firebolt-sdk requires python > 3.9
```

**Your Python version:** 3.8.0  
**Required:** Python 3.9 or higher  
**Recommended:** Python 3.11 (tested and stable)

---

## ✅ Solution: Upgrade Python

Choose the guide for your operating system:

---

## 🐧 Linux (Ubuntu/Debian)

### Check Current Version

```bash
python3 --version
# Output: Python 3.8.0

which python3
# Note the path
```

### Option 1: Using deadsnakes PPA (Ubuntu/Debian)

```bash
# Add deadsnakes repository
sudo apt update
sudo apt install software-properties-common -y
sudo add-apt-repository ppa:deadsnakes/ppa -y

# Update package list
sudo apt update

# Install Python 3.11
sudo apt install python3.11 python3.11-venv python3.11-distutils python3-pip -y

# Verify installation
python3.11 --version
# Should show: Python 3.11.x

# Make Python 3.11 the default (optional)
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 2
sudo update-alternatives --config python3
# Select python3.11

# Verify
python3 --version
# Should now show: Python 3.11.x
```

### Option 2: Using pyenv (Recommended for Development)

```bash
# Install dependencies
sudo apt update
sudo apt install -y build-essential libssl-dev zlib1g-dev \
  libbz2-dev libreadline-dev libsqlite3-dev curl \
  libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev

# Install pyenv
curl https://pyenv.run | bash

# Add to ~/.bashrc or ~/.zshrc
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc

# Reload shell
source ~/.bashrc

# Install Python 3.11
pyenv install 3.11.5

# Set as global default
pyenv global 3.11.5

# Verify
python3 --version
# Should show: Python 3.11.5
```

---

## 🍎 macOS

### Check Current Version

```bash
python3 --version
# Output: Python 3.8.0
```

### Option 1: Using Homebrew (Recommended)

```bash
# Install Homebrew (if not installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# Update Homebrew
brew update

# Install Python 3.11
brew install python@3.11

# Verify
python3.11 --version
# Should show: Python 3.11.x

# Make it default (create alias)
echo 'alias python3="python3.11"' >> ~/.zshrc
echo 'alias pip3="pip3.11"' >> ~/.zshrc
source ~/.zshrc

# Or symlink (more permanent)
brew link --overwrite python@3.11
```

### Option 2: Using pyenv

```bash
# Install pyenv
brew install pyenv

# Add to ~/.zshrc
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.zshrc
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.zshrc
echo 'eval "$(pyenv init -)"' >> ~/.zshrc

# Reload shell
source ~/.zshrc

# Install Python 3.11
pyenv install 3.11.5

# Set as global default
pyenv global 3.11.5

# Verify
python3 --version
# Should show: Python 3.11.5
```

---

## 🪟 Windows

### Check Current Version

```powershell
python --version
# Output: Python 3.8.0
```

### Option 1: Download Official Installer (Easiest)

1. **Download Python 3.11:**
   - Go to: https://www.python.org/downloads/
   - Download "Python 3.11.x" (latest 3.11 version)

2. **Install:**
   - Run the installer
   - ☑️ **Check "Add Python 3.11 to PATH"** (IMPORTANT!)
   - Click "Install Now"

3. **Verify:**
   ```powershell
   python --version
   # Should show: Python 3.11.x
   
   pip --version
   # Should work
   ```

### Option 2: Using Windows Package Manager (winget)

```powershell
# In PowerShell (Admin)
winget install Python.Python.3.11

# Restart PowerShell

# Verify
python --version
# Should show: Python 3.11.x
```

### Option 3: Using Chocolatey

```powershell
# Install Chocolatey (if not installed)
Set-ExecutionPolicy Bypass -Scope Process -Force
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072
iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# Install Python 3.11
choco install python311 -y

# Restart PowerShell

# Verify
python --version
```

---

## 🔧 After Upgrading Python

### Step 1: Verify Python Version

```bash
python3 --version
# Should show: Python 3.9+ (preferably 3.11+)

which python3
# Verify correct path
```

### Step 2: Upgrade pip

```bash
python3 -m pip install --upgrade pip
```

### Step 3: Reinstall CDK Dependencies

```bash
cd firebolt-cdc-lambda

# Remove old virtual environment (if exists)
rm -rf .venv venv

# Create new virtual environment with Python 3.11
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate  # Linux/macOS
# Or: .venv\Scripts\activate  # Windows

# Install dependencies
pip3 install -r requirements.txt

# Verify firebolt-sdk installs
pip3 list | grep firebolt
# Should show: firebolt-sdk x.x.x
```

### Step 4: Retry Deployment

```bash
# Still in activated virtual environment
./scripts/deploy.sh

# Or deactivate and use system Python 3.11:
deactivate
python3 --version  # Verify it's 3.11+
./scripts/deploy.sh
```

---

## ⚠️ If You Can't Upgrade System Python

If you don't have sudo/admin access, use a virtual environment with specific Python version:

### Using pyenv (No sudo required)

```bash
# Install pyenv (no sudo)
curl https://pyenv.run | bash

# Add to shell config
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc
source ~/.bashrc

# Install Python 3.11 (user-local)
pyenv install 3.11.5

# Use it for this project
cd firebolt-cdc-lambda
pyenv local 3.11.5

# Verify
python3 --version
# Should show: Python 3.11.5

# Now deploy works
./scripts/deploy.sh
```

---

## 🧪 Troubleshooting

### Issue: "python3.11: command not found" after install

**Linux/macOS:**
```bash
# Find where Python 3.11 was installed
find /usr -name "python3.11" 2>/dev/null

# Create symlink
sudo ln -s /usr/bin/python3.11 /usr/local/bin/python3

# Or add to PATH
export PATH="/usr/local/bin:$PATH"
```

**Windows:**
```powershell
# Add to PATH manually:
# 1. Search for "Environment Variables"
# 2. Edit "Path"
# 3. Add: C:\Python311 and C:\Python311\Scripts
# 4. Restart terminal
```

### Issue: "pip3: No module named pip" after upgrade

```bash
# Reinstall pip for Python 3.11
python3.11 -m ensurepip --upgrade

# Or download get-pip.py
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3.11 get-pip.py

# Verify
python3.11 -m pip --version
```

### Issue: Virtual environment still uses old Python

```bash
# Remove old venv
rm -rf .venv venv

# Create with specific Python version
python3.11 -m venv .venv

# Or specify path explicitly
/usr/bin/python3.11 -m venv .venv

# Activate and verify
source .venv/bin/activate
python --version  # Should show 3.11+
```

---

## 📋 Quick Reference

| OS | Quick Install Command |
|----|----------------------|
| **Ubuntu/Debian** | `sudo apt install python3.11 python3.11-venv -y` |
| **macOS** | `brew install python@3.11` |
| **Windows** | Download from python.org or `winget install Python.Python.3.11` |
| **Any (pyenv)** | `pyenv install 3.11.5 && pyenv global 3.11.5` |

---

## ✅ Verification Checklist

After upgrading:

- [ ] `python3 --version` shows 3.9+ (preferably 3.11+)
- [ ] `pip3 --version` works
- [ ] `pip3 install firebolt-sdk` succeeds
- [ ] `./scripts/deploy.sh` runs without Python version error

---

## 🚀 After Python Upgrade - Full Deploy

```bash
# 1. Verify Python version
python3 --version
# Must show: Python 3.9+ (3.11+ recommended)

# 2. Navigate to project
cd firebolt-cdc-lambda

# 3. Pull latest code
git pull origin main

# 4. Configure .env
cp .env.example .env
nano .env  # Update credentials

# 5. Upload config
aws s3 cp config/tables_keys.json \
  s3://fcanalytics/firebolt_dms_job/config/tables_keys.json

# 6. Deploy
./scripts/deploy.sh

# Should work now! ✅
```

---

## 📞 Still Having Issues?

If you still face Python version issues:

1. **Show your Python setup:**
   ```bash
   python3 --version
   which python3
   ls -la $(which python3)
   ```

2. **Check alternatives:**
   ```bash
   # Linux
   update-alternatives --list python3
   
   # macOS
   ls -l /usr/local/bin/python*
   ```

3. **Try explicit version in deploy script:**
   Edit `scripts/deploy.sh`, change first line to:
   ```bash
   #!/usr/bin/env python3.11
   ```

---

**Python 3.11 is recommended for best compatibility with Firebolt SDK and AWS Lambda runtime.**

