#!/bin/bash
set -e

echo "🚀 Deploying Firebolt CDC Lambda..."

# Check .env
if [ ! -f .env ]; then
    echo "⚠️  .env not found! Copying from .env.example..."
    cp .env.example .env
    echo "❗ Edit .env and add your Firebolt credentials!"
    exit 1
fi

# Install dependencies
pip install -r requirements.txt

# Build Lambda layer
mkdir -p lambda-layer/python
pip install -r lambda/requirements.txt -t lambda-layer/python/ \
    --platform manylinux2014_x86_64 --only-binary=:all: --python-version 3.11

# Bootstrap (first time)
cdk bootstrap aws://$(aws sts get-caller-identity --query Account --output text)/ap-south-1 || true

# Deploy
cdk deploy --require-approval never

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📝 Next: Upload tables_keys.json:"
echo "   aws s3 cp config/tables_keys.json s3://fcanalytics/firebolt-migration/config/tables_keys.json"

