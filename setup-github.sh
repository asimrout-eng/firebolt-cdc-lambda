#!/bin/bash
set -e

REPO_NAME="firebolt-cdc-lambda"
GITHUB_USERNAME="${1}"

if [ -z "$GITHUB_USERNAME" ]; then
    echo "Usage: ./setup-github.sh YOUR_GITHUB_USERNAME"
    exit 1
fi

echo "📦 Setting up GitHub repository..."

# Initialize git
git init
git add .
git commit -m "🚀 Initial commit: Firebolt CDC Lambda with AWS CDK"

# Check if gh CLI is installed
if command -v gh &> /dev/null; then
    echo "Creating repo on GitHub..."
    gh repo create $REPO_NAME \
        --public \
        --source=. \
        --remote=origin \
        --description "Automated CDC pipeline: S3 → Lambda → Firebolt using AWS CDK" \
        --push
    
    echo ""
    echo "✅ Repository created!"
    echo "🌐 https://github.com/$GITHUB_USERNAME/$REPO_NAME"
    echo ""
    echo "📖 Enable GitHub Pages:"
    echo "   https://github.com/$GITHUB_USERNAME/$REPO_NAME/settings/pages"
    echo "   - Source: Deploy from branch"
    echo "   - Branch: main"
    echo "   - Folder: / (root)"
    echo ""
    echo "🎉 Your page: https://$GITHUB_USERNAME.github.io/$REPO_NAME/"
else
    echo "❌ GitHub CLI not found!"
    echo ""
    echo "Install: brew install gh"
    echo ""
    echo "Or create manually:"
    echo "1. Create repo: https://github.com/new"
    echo "2. Run: git remote add origin https://github.com/$GITHUB_USERNAME/$REPO_NAME.git"
    echo "3. Push: git push -u origin main"
fi



