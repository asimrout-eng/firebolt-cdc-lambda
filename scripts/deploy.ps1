# PowerShell deployment script for Windows
# Firebolt CDC Lambda - AWS CDK Deployment

Write-Host "🚀 Deploying Firebolt CDC Lambda..." -ForegroundColor Green

# Check .env file
if (-not (Test-Path ".env")) {
    Write-Host "⚠️  .env not found! Copying from .env.example..." -ForegroundColor Yellow
    Copy-Item ".env.example" ".env"
    Write-Host "❗ Edit .env and add your Firebolt credentials!" -ForegroundColor Red
    exit 1
}

# Install CDK dependencies
Write-Host "📦 Installing CDK dependencies..." -ForegroundColor Cyan
pip install -r requirements.txt

# Build Lambda layer
Write-Host "🔧 Building Lambda layer..." -ForegroundColor Cyan
New-Item -ItemType Directory -Force -Path "lambda-layer\python" | Out-Null
pip install -r lambda\requirements.txt -t lambda-layer\python\ `
    --platform manylinux2014_x86_64 --only-binary=:all: --python-version 3.11

# Get AWS Account ID
$AWS_ACCOUNT = aws sts get-caller-identity --query Account --output text

# Bootstrap CDK (first time)
Write-Host "🔐 Bootstrapping CDK..." -ForegroundColor Cyan
cdk bootstrap "aws://$AWS_ACCOUNT/ap-south-1"

# Deploy
Write-Host "🚀 Deploying stack..." -ForegroundColor Cyan
cdk deploy --require-approval never

Write-Host ""
Write-Host "✅ Deployment complete!" -ForegroundColor Green
Write-Host ""
Write-Host "📝 Next: Upload tables_keys.json:" -ForegroundColor Yellow
Write-Host "   aws s3 cp config\tables_keys.json s3://fcanalytics/firebolt-migration/config/tables_keys.json" -ForegroundColor White



