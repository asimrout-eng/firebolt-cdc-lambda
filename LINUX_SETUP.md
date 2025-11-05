# 🐧 Linux Setup Guide

Complete setup guide for deploying Firebolt CDC Lambda on Linux.

---

## 📋 Prerequisites Installation

Your Linux machine needs the following tools installed:

---

### 1️⃣ Git

```bash
# Check if installed
git --version

# Install on Ubuntu/Debian
sudo apt update
sudo apt install git -y

# Install on RHEL/CentOS/Fedora
sudo yum install git -y
# OR
sudo dnf install git -y
```

---

### 2️⃣ Python 3.11+

```bash
# Check current version
python3 --version

# Install on Ubuntu/Debian
sudo apt update
sudo apt install python3.11 python3.11-venv python3-pip -y

# If Python 3.11 not available, add deadsnakes PPA (Ubuntu)
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.11 python3.11-venv python3-pip -y

# Install on RHEL/CentOS/Fedora
sudo dnf install python3.11 python3-pip -y

# Verify installation
python3.11 --version
```

---

### 3️⃣ Node.js and npm

```bash
# Check if installed
node --version
npm --version

# Install on Ubuntu/Debian (using NodeSource)
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt install -y nodejs

# Install on RHEL/CentOS/Fedora
curl -fsSL https://rpm.nodesource.com/setup_18.x | sudo bash -
sudo yum install -y nodejs

# Verify installation
node --version
npm --version
```

---

### 4️⃣ AWS CLI

```bash
# Check if installed
aws --version

# Install AWS CLI v2 (all Linux distributions)
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Cleanup
rm -rf aws awscliv2.zip

# Verify installation
aws --version
```

---

### 5️⃣ AWS CDK CLI

```bash
# Install AWS CDK globally
sudo npm install -g aws-cdk

# Verify installation
cdk --version
```

---

### 6️⃣ Configure AWS Credentials

```bash
# Run AWS configure
aws configure

# You'll be prompted for:
AWS Access Key ID: [YOUR_ACCESS_KEY]
AWS Secret Access Key: [YOUR_SECRET_KEY]
Default region name: ap-south-1
Default output format: json
```

**Verify configuration:**
```bash
aws sts get-caller-identity
```

You should see output like:
```json
{
    "UserId": "AIDAXXXXXXXXXXXXXXXXX",
    "Account": "123456789012",
    "Arn": "arn:aws:iam::123456789012:user/username"
}
```

---

## 🚀 Quick Installation Script

Copy and run this entire script:

```bash
#!/bin/bash
# Firebolt CDC Lambda - Prerequisites Installation Script
# For Ubuntu/Debian Linux

set -e

echo "🐧 Installing Firebolt CDC Lambda Prerequisites..."
echo ""

# Update package lists
echo "📦 Updating package lists..."
sudo apt update

# Install Git
echo "📦 Installing Git..."
sudo apt install git -y

# Install Python 3.11
echo "🐍 Installing Python 3.11..."
sudo add-apt-repository ppa:deadsnakes/ppa -y || true
sudo apt update
sudo apt install python3.11 python3.11-venv python3-pip -y

# Install Node.js
echo "📦 Installing Node.js..."
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt install -y nodejs

# Install AWS CLI
echo "☁️  Installing AWS CLI..."
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip -q awscliv2.zip
sudo ./aws/install
rm -rf aws awscliv2.zip

# Install AWS CDK
echo "📦 Installing AWS CDK..."
sudo npm install -g aws-cdk

echo ""
echo "✅ Installation Complete!"
echo ""
echo "Installed versions:"
git --version
python3.11 --version
node --version
npm --version
aws --version
cdk --version

echo ""
echo "📝 Next step: Configure AWS credentials"
echo "   Run: aws configure"
```

**Save and run:**
```bash
# Save the script
nano install-prerequisites.sh
# Paste the script above, then save (Ctrl+X, Y, Enter)

# Make executable
chmod +x install-prerequisites.sh

# Run
./install-prerequisites.sh
```

---

## 🚀 Deployment Steps

### Step 1: Clone Repository

```bash
git clone https://github.com/asimrout-eng/firebolt-cdc-lambda.git
cd firebolt-cdc-lambda
```

---

### Step 2: Configure Firebolt Credentials

```bash
# Copy environment template
cp .env.example .env

# Edit with your preferred editor
nano .env
# OR
vim .env
# OR
code .env
```

**Required fields:**
```bash
# AWS Configuration
AWS_REGION=ap-south-1
ENVIRONMENT=prod

# S3 Configuration
S3_BUCKET_NAME=fcanalytics
S3_PREFIX=firebolt_dms_job/

# Lambda Configuration
LAMBDA_FUNCTION_NAME=firebolt-cdc-processor
LAMBDA_TIMEOUT=300
LAMBDA_MEMORY_SIZE=512

# ============================================
# FIREBOLT CREDENTIALS (REQUIRED)
# ============================================
FIREBOLT_ACCOUNT=faircentindia
FIREBOLT_DATABASE=your_database_name
FIREBOLT_ENGINE=your_engine_name
FIREBOLT_USERNAME=your@email.com
FIREBOLT_PASSWORD=your_password

# Firebolt External Location
LOCATION_NAME=s3_raw_dms

# Table Keys Configuration
TABLE_KEYS_S3_BUCKET=fcanalytics
TABLE_KEYS_S3_KEY=firebolt_dms_job/config/tables_keys.json
```

**Save:** Ctrl+X, then Y, then Enter (for nano)

---

### Step 3: Create Table Keys Configuration

```bash
# Copy example
cp config/tables_keys.json.example config/tables_keys.json

# Edit with your tables
nano config/tables_keys.json
```

**Example:**
```json
{
  "customers": ["customer_id"],
  "orders": ["order_id"],
  "order_items": ["order_id", "item_id"],
  "payments": ["payment_id"]
}
```

**Upload to S3:**
```bash
aws s3 cp config/tables_keys.json \
  s3://fcanalytics/firebolt_dms_job/config/tables_keys.json

# Verify upload
aws s3 ls s3://fcanalytics/firebolt_dms_job/config/
```

---

### Step 4: Deploy to AWS

```bash
# Make scripts executable
chmod +x scripts/*.sh

# Deploy
./scripts/deploy.sh
```

**What happens during deployment:**
1. ✅ Installs Python dependencies (aws-cdk-lib, constructs)
2. ✅ Builds Lambda layer (firebolt-sdk, pyarrow)
3. ✅ Bootstraps CDK (first time only)
4. ✅ Creates Lambda function
5. ✅ Configures IAM roles
6. ✅ Sets up S3 event notifications
7. ✅ Creates CloudWatch log group

**Expected output:**
```
🚀 Deploying Firebolt CDC Lambda...
📦 Installing CDK dependencies...
🔧 Building Lambda layer...
🔐 Bootstrapping CDK...
🚀 Deploying stack...

✅ Deployment complete!

📝 Next: Upload tables_keys.json:
   aws s3 cp config/tables_keys.json s3://fcanalytics/firebolt_dms_job/config/tables_keys.json
```

---

### Step 5: Verify Deployment

```bash
# Check Lambda function
aws lambda get-function --function-name firebolt-cdc-processor

# List Lambda functions
aws lambda list-functions --query 'Functions[?FunctionName==`firebolt-cdc-processor`]'

# Check S3 trigger
aws lambda list-event-source-mappings --function-name firebolt-cdc-processor
```

---

### Step 6: Monitor Logs

```bash
# Tail logs in real-time
aws logs tail /aws/lambda/firebolt-cdc-processor --follow

# View recent logs
aws logs tail /aws/lambda/firebolt-cdc-processor --since 1h

# View logs with filter
aws logs tail /aws/lambda/firebolt-cdc-processor --filter-pattern "ERROR"
```

---

### Step 7: Test (Optional)

```bash
# Upload a test parquet file
aws s3 cp test.parquet \
  s3://fcanalytics/firebolt_dms_job/mysql/customers/20251102/test.parquet

# Watch processing in logs
aws logs tail /aws/lambda/firebolt-cdc-processor --follow
```

**Expected log output:**
```
Processing S3 key: firebolt_dms_job/mysql/customers/20251102/test.parquet
Database: mysql, Table: customers, Date: 20251102, File: test.parquet
✓ Staging table stg_customers_abc12345 ready
✓ Copied test.parquet to stg_customers_abc12345
✓ Retrieved 10 columns for customers
✓ MERGE completed for customers
✓ Cleaned up staging table stg_customers_abc12345
✓ Processing complete in 2.34s
```

---

## 📋 Quick Setup Checklist

```bash
# ✅ Complete Setup Checklist

# 1. Install prerequisites
./install-prerequisites.sh

# 2. Configure AWS
aws configure

# 3. Clone repository
git clone https://github.com/asimrout-eng/firebolt-cdc-lambda.git
cd firebolt-cdc-lambda

# 4. Configure Firebolt
cp .env.example .env
nano .env  # Fill in credentials

# 5. Create table keys
cp config/tables_keys.json.example config/tables_keys.json
nano config/tables_keys.json  # Add your tables

# 6. Upload table keys
aws s3 cp config/tables_keys.json \
  s3://fcanalytics/firebolt_dms_job/config/tables_keys.json

# 7. Deploy
chmod +x scripts/*.sh
./scripts/deploy.sh

# 8. Verify
aws lambda get-function --function-name firebolt-cdc-processor

# Done! 🎉
```

---

## 🗑️ Cleanup

To remove all AWS resources:

```bash
./scripts/destroy.sh
```

Type `destroy` when prompted to confirm.

---

## 🔍 Verification Commands

```bash
# Check all installations
echo "Git: $(git --version)"
echo "Python: $(python3.11 --version)"
echo "Node: $(node --version)"
echo "npm: $(npm --version)"
echo "AWS CLI: $(aws --version)"
echo "CDK: $(cdk --version)"

# Check AWS credentials
aws sts get-caller-identity

# Check deployed Lambda
aws lambda list-functions \
  --query 'Functions[?FunctionName==`firebolt-cdc-processor`].FunctionName'
```

---

## ⚠️ Troubleshooting

### Issue: Python 3.11 not available

**Solution:**
```bash
# Add deadsnakes PPA (Ubuntu)
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.11 python3.11-venv python3-pip -y
```

### Issue: Permission denied on scripts

**Solution:**
```bash
chmod +x scripts/*.sh
```

### Issue: AWS credentials not configured

**Solution:**
```bash
aws configure
# Enter your access key, secret key, region (ap-south-1)
```

### Issue: CDK bootstrap needed

**Error:**
```
This stack uses assets, so the toolkit stack must be deployed
```

**Solution:**
```bash
# Get your AWS account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Bootstrap CDK
cdk bootstrap aws://$ACCOUNT_ID/ap-south-1
```

### Issue: Insufficient IAM permissions

**Required IAM permissions:**
- Lambda: CreateFunction, UpdateFunctionCode, UpdateFunctionConfiguration
- S3: PutBucketNotification, GetObject, ListBucket
- IAM: CreateRole, AttachRolePolicy, PassRole
- CloudFormation: CreateStack, UpdateStack, DescribeStacks
- CloudWatch Logs: CreateLogGroup, CreateLogStream

### Issue: Lambda deployment timeout

**Solution:**
```bash
# Increase timeout in .env
LAMBDA_TIMEOUT=600

# Redeploy
./scripts/deploy.sh
```

---

## 📚 Additional Resources

- **Repository:** https://github.com/asimrout-eng/firebolt-cdc-lambda
- **Quick Start:** [QUICKSTART.md](QUICKSTART.md)
- **Cross-Platform Guide:** [CROSS_PLATFORM_GUIDE.md](CROSS_PLATFORM_GUIDE.md)
- **Configuration Setup:** [CONFIG_SETUP.md](CONFIG_SETUP.md)
- **GitHub Pages:** https://asimrout-eng.github.io/firebolt-cdc-lambda/

---

## 💡 Tips

1. **Use screen/tmux for deployment** - Keeps deployment running if SSH disconnects:
   ```bash
   screen -S cdk-deploy
   ./scripts/deploy.sh
   # Detach: Ctrl+A, D
   # Reattach: screen -r cdk-deploy
   ```

2. **Monitor multiple logs:**
   ```bash
   # Terminal 1: Lambda logs
   aws logs tail /aws/lambda/firebolt-cdc-processor --follow
   
   # Terminal 2: CloudFormation events
   watch -n 5 'aws cloudformation describe-stacks \
     --stack-name FireboltCdcStack --query Stacks[0].StackStatus'
   ```

3. **Backup your .env file:**
   ```bash
   cp .env .env.backup
   ```

---

## ✅ Success Indicators

After deployment, you should have:

- ✅ Lambda function: `firebolt-cdc-processor`
- ✅ IAM role: `firebolt-cdc-processor-role`
- ✅ Lambda layer: `firebolt-sdk-layer`
- ✅ CloudWatch log group: `/aws/lambda/firebolt-cdc-processor`
- ✅ S3 event notification on `fcanalytics` bucket
- ✅ CloudFormation stack: `FireboltCdcStack`

**Verify all:**
```bash
# Lambda
aws lambda get-function --function-name firebolt-cdc-processor

# IAM Role
aws iam get-role --role-name firebolt-cdc-processor-role

# CloudFormation
aws cloudformation describe-stacks --stack-name FireboltCdcStack
```

---

**🎉 Ready to process CDC data from S3 to Firebolt!**

