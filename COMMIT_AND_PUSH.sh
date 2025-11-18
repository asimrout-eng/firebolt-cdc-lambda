#!/bin/bash
cd /Users/asimkumarrout/Documents/Firebolt/Python/firebolt-cdk-package

git add lambda/handler.py

git commit -m "🔥 CRITICAL FIX: Enable autocommit=False for proper transaction support

Per Firebolt Python SDK docs:
- autocommit=True (default): Each statement auto-commits, BEGIN is a no-op
- autocommit=False: Transactions start implicitly with first statement

Changes:
1. Set autocommit=False on connection (line 87)
2. Remove explicit BEGIN; statement (not needed, starts implicitly)
3. Keep COMMIT; and ROLLBACK; statements (required for explicit control)
4. Simplify transaction_started flag logic

This fixes 'cannot COMMIT transaction: no transaction is in progress' errors

Ref: https://python.docs.firebolt.io/sdk_documenation/latest/Connecting_and_queries.html#transaction-support"

git push origin main

echo "✅ Changes committed and pushed!"

