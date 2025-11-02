# Configuration Setup

## 📍 Table Keys Configuration

The Lambda function needs a `table_keys.json` file to know the primary keys for each table.

### ✅ Recommended: Use DMS S3 Location (Lambda Already Has Access)

Save to: `s3://fcanalytics/firebolt_dms_job/config/tables_keys.json`

**Why:**
- ✅ Lambda already has read access (no IAM changes needed)
- ✅ Only 1 environment variable to change
- ✅ Simplest setup

---

## 🔧 Setup Steps

### 1. Create `table_keys.json`

**Example:**
```json
{
  "customers": ["customer_id"],
  "orders": ["order_id"],
  "order_items": ["order_id", "item_id"],
  "payments": ["payment_id"],
  "transactions": ["transaction_id"]
}
```

**Format:**
- **Key:** Table name (as it appears in Firebolt)
- **Value:** Array of primary key column(s)
- **Composite keys:** Use array with multiple columns: `["col1", "col2"]`

### 2. Update `.env` File

Edit your `.env` file:

```bash
# Table Keys Configuration - Use DMS S3 location
TABLE_KEYS_S3_BUCKET=fcanalytics
TABLE_KEYS_S3_KEY=firebolt_dms_job/config/tables_keys.json
```

### 3. Upload to S3

**macOS / Linux:**
```bash
aws s3 cp config/tables_keys.json \
  s3://fcanalytics/firebolt_dms_job/config/tables_keys.json
```

**Windows:**
```powershell
aws s3 cp config\tables_keys.json s3://fcanalytics/firebolt_dms_job/config/tables_keys.json
```

### 4. Verify Upload

```bash
aws s3 ls s3://fcanalytics/firebolt_dms_job/config/
```

You should see:
```
2025-11-02 12:00:00        156 table_keys.json
```

### 5. Deploy Lambda

Deploy with updated environment variable:

**macOS / Linux:**
```bash
./scripts/deploy.sh
```

**Windows (PowerShell):**
```powershell
.\scripts\deploy.ps1
```

**Windows (Command Prompt):**
```cmd
scripts\deploy.bat
```

---

## 🔄 Updating Table Keys

To add/modify tables:

1. Edit local `config/tables_keys.json`
2. Re-upload to S3:
   ```bash
   aws s3 cp config/tables_keys.json \
     s3://fcanalytics/firebolt_dms_job/config/tables_keys.json
   ```
3. **No redeployment needed!** Lambda reads from S3 on each run.

---

## 📂 Final S3 Structure

```
s3://fcanalytics/firebolt_dms_job/
├── config/
│   └── table_keys.json              ← Configuration
│
├── mysql/                            ← DMS data files
│   ├── customers/
│   │   └── 20251102/
│   │       └── LOAD00000001.parquet
│   └── orders/
│       └── 20251102/
│           └── LOAD00000001.parquet
```

---

## 🆚 Alternative: Inline Environment Variable

For **very small configs** (< 5 tables), you can use inline JSON:

### Update `.env`:
```bash
# Inline JSON (no S3 upload needed)
TABLE_KEYS_JSON={"customers":["customer_id"],"orders":["order_id"]}

# Comment out S3 config:
# TABLE_KEYS_S3_BUCKET=fcanalytics
# TABLE_KEYS_S3_KEY=firebolt_dms_job/config/tables_keys.json
```

**Pros:**
- No S3 upload needed
- Works immediately

**Cons:**
- Hard to update (requires redeployment)
- Not scalable for many tables
- Less readable

**Recommendation:** Use S3 for production deployments.

---

## 🚨 Troubleshooting

### Lambda Can't Read table_keys.json

**Error:**
```
NoSuchKey: The specified key does not exist
```

**Solution:**
1. Verify file exists:
   ```bash
   aws s3 ls s3://fcanalytics/firebolt_dms_job/config/
   ```
2. Check `.env` has correct path:
   ```bash
   TABLE_KEYS_S3_KEY=firebolt_dms_job/config/tables_keys.json
   ```
3. Redeploy Lambda with updated env var

### Invalid JSON Format

**Error:**
```
JSONDecodeError: Expecting property name enclosed in double quotes
```

**Solution:**
Validate JSON syntax:
```bash
cat config/tables_keys.json | python -m json.tool
```

### Table Not Found in Config

**Error:**
```
RuntimeError: No keys configured for table customers
```

**Solution:**
Add table to `table_keys.json`:
```json
{
  "customers": ["customer_id"]
}
```

Then re-upload to S3.

---

## 📖 See Also

- [Quick Start](QUICKSTART.md)
- [Cross-Platform Guide](CROSS_PLATFORM_GUIDE.md)
- [Deployment Summary](../DEPLOYMENT_SUMMARY.md)

