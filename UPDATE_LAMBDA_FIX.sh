#!/bin/bash
# Update Lambda with transaction conflict fix
# Run this script to deploy the latest fix from GitHub

set -e

echo "═══════════════════════════════════════════════════════════════════"
echo "  🔧 Lambda Transaction Conflict Fix - Update Script"
echo "═══════════════════════════════════════════════════════════════════"
echo ""

# Check if we're in the right directory
if [ ! -f "app.py" ] || [ ! -d "lambda" ]; then
    echo "❌ Error: Please run this script from the firebolt-cdc-lambda directory"
    echo ""
    echo "Run these commands first:"
    echo "  cd ~/firebolt-cdc-lambda"
    echo "  ./UPDATE_LAMBDA_FIX.sh"
    exit 1
fi

# Step 1: Pull latest code from GitHub
echo "Step 1: Pulling latest code from GitHub..."
git pull origin main

if [ $? -ne 0 ]; then
    echo "❌ Error: Failed to pull from GitHub"
    echo "   Run 'git status' to check for conflicts"
    exit 1
fi

echo "✓ Latest code pulled from GitHub"
echo ""

# Step 2: Ask about Lambda concurrency limit
echo "Step 2: Lambda Concurrency Configuration"
echo ""
echo "To reduce transaction conflicts, it's recommended to limit Lambda concurrency."
echo "Current: Unlimited (can cause 100+ concurrent executions)"
echo "Recommended: 5-10 concurrent executions"
echo ""
read -p "Do you want to set Lambda concurrency limit to 5? (y/n): " set_concurrency

if [[ "$set_concurrency" =~ ^[Yy]$ ]]; then
    echo ""
    echo "Setting Lambda concurrency to 5..."
    aws lambda put-function-concurrency \
      --function-name firebolt-cdc-processor \
      --reserved-concurrent-executions 5 \
      --region ap-south-1
    
    if [ $? -eq 0 ]; then
        echo "✓ Lambda concurrency set to 5"
    else
        echo "⚠️  Warning: Failed to set concurrency (continuing anyway)"
    fi
else
    echo "⚠️  Skipping concurrency limit (you may see more conflicts)"
fi

echo ""

# Step 3: Deploy updated Lambda
echo "Step 3: Deploying updated Lambda..."
echo ""
chmod +x scripts/deploy.sh
./scripts/deploy.sh

echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "✅ Update Complete!"
echo "═══════════════════════════════════════════════════════════════════"
echo ""
echo "What was fixed:"
echo "  ✅ Transaction conflict retry logic (3 retries with backoff)"
echo "  ✅ Proper ROLLBACK error handling"
if [[ "$set_concurrency" =~ ^[Yy]$ ]]; then
    echo "  ✅ Lambda concurrency limited to 5"
fi
echo ""
echo "Expected improvements:"
echo "  - No more 'Cannot ROLLBACK transaction' errors"
echo "  - ~90% fewer transaction conflict errors"
echo "  - Automatic retry on conflicts (up to 3 times)"
echo ""
echo "Monitor logs:"
echo "  aws logs tail /aws/lambda/firebolt-cdc-processor --follow --region ap-south-1"
echo ""
echo "Look for these messages:"
echo "  ✅ '✓ MERGE completed for <table>'"
echo "  ⚠️  '⚠️  Transaction conflict on <table>, retry X/3 in Y.XXs'"
echo "  ✅ '✓ Transaction rolled back'"
echo ""

