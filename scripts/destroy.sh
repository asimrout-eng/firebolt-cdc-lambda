#!/bin/bash
set -e

echo "⚠️  Destroy all resources?"
read -p "Type 'destroy' to confirm: " CONFIRM

if [ "$CONFIRM" == "destroy" ]; then
    cdk destroy
    echo "✅ Resources destroyed"
else
    echo "❌ Cancelled"
fi



