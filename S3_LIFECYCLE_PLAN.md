# 📦 S3 Lifecycle Management Plan - Archive Old CDC Files

## 🎯 Objective

Automatically archive and delete old DMS CDC files from S3 after they've been processed by Lambda and loaded into Firebolt, reducing storage costs.

---

## 📊 Current Situation

**S3 Bucket:** `fcanalytics`  
**Prefix:** `firebolt_dms_job/`  
**File Pattern:** `fair/<table>/YYYY/MM/DD/*.parquet`

**Problem:**
- CDC files accumulate indefinitely
- Storage costs grow linearly with time
- Files are no longer needed after processing

---

## ✅ Three Recommended Strategies

### **Strategy 1: AGGRESSIVE (Recommended for Most Use Cases)**

**Timeline:**
- Day 0-7: Keep in S3 Standard (hot access)
- Day 8-30: Move to Intelligent-Tiering (automatic optimization)
- Day 31-90: Move to Glacier Instant Retrieval (cold archive, instant access)
- Day 91+: DELETE permanently

**Monthly Cost (for 1TB data/month):**
- Days 1-7: $23 (S3 Standard)
- Days 8-30: $4 (Intelligent-Tiering)
- Days 31-90: $4 (Glacier Instant)
- **Total: ~$31/month** (vs $150 without lifecycle)

**Use Case:** Production CDC where you're confident Lambda processed files successfully

---

### **Strategy 2: CONSERVATIVE (Extra Safety)**

**Timeline:**
- Day 0-14: Keep in S3 Standard
- Day 15-60: Move to Intelligent-Tiering
- Day 61-180: Move to Glacier Instant Retrieval
- Day 181-365: Move to Glacier Flexible Retrieval (3-5 hour retrieval)
- Day 366+: DELETE permanently

**Monthly Cost (for 1TB data/month):**
- **Total: ~$45/month** (vs $150 without lifecycle)

**Use Case:** Compliance requirements, cautious approach

---

### **Strategy 3: MINIMAL (Keep Files Forever, Just Archive)**

**Timeline:**
- Day 0-7: Keep in S3 Standard
- Day 8-30: Move to Intelligent-Tiering
- Day 31+: Move to Glacier Deep Archive (lowest cost, 12-hour retrieval)
- Never delete

**Monthly Cost (for 1TB data/month):**
- **Total: ~$10/month** (vs $150 without lifecycle)

**Use Case:** Regulatory requirements to keep all data, but don't need fast access

---

## 🚀 Recommended: AGGRESSIVE Strategy

### **S3 Lifecycle Policy JSON:**

```json
{
  "Rules": [
    {
      "Id": "ArchiveOldCDCFiles",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "firebolt_dms_job/"
      },
      "Transitions": [
        {
          "Days": 7,
          "StorageClass": "INTELLIGENT_TIERING"
        },
        {
          "Days": 30,
          "StorageClass": "GLACIER_IR"
        }
      ],
      "Expiration": {
        "Days": 90
      }
    }
  ]
}
```

---

## 📋 How to Implement (AWS Console - GUI Method)

### **Step 1: Navigate to S3 Lifecycle**

1. Open AWS Console → S3
2. Click on bucket: `fcanalytics`
3. Go to **"Management"** tab
4. Click **"Create lifecycle rule"**

---

### **Step 2: Configure Lifecycle Rule**

**Rule Name:** `Archive-Old-CDC-Files`

**Choose rule scope:**
- ✅ Select: **"Limit the scope of this rule using one or more filters"**
- **Prefix:** `firebolt_dms_job/`
- Click **"Add filter"**

---

### **Step 3: Lifecycle Rule Actions**

Check these boxes:
- ✅ **Transition current versions of objects between storage classes**
- ✅ **Expire current versions of objects**

---

### **Step 4: Transition Timeline**

**Transition 1:**
- **Days after object creation:** `7`
- **Storage class:** `Intelligent-Tiering`

Click **"Add transition"**

**Transition 2:**
- **Days after object creation:** `30`
- **Storage class:** `Glacier Instant Retrieval`

---

### **Step 5: Expiration**

**Expire current versions of objects:**
- **Days after object creation:** `90`

---

### **Step 6: Review and Create**

- Review all settings
- Acknowledge: "I acknowledge that this lifecycle rule will apply to all objects under the specified prefix"
- Click **"Create rule"**

---

## 🔍 Verify Lifecycle Rule

### **AWS Console:**
1. S3 → `fcanalytics` → **Management** tab
2. You should see: `Archive-Old-CDC-Files` rule enabled

### **AWS CLI:**
```bash
aws s3api get-bucket-lifecycle-configuration \
  --bucket fcanalytics \
  --region ap-south-1
```

---

## 📊 Cost Comparison (1TB data/month)

| Strategy | Month 1 | Month 3 | Month 6 | Annual |
|----------|---------|---------|---------|--------|
| **No Lifecycle** | $150 | $450 | $900 | $1,800 |
| **Aggressive** | $31 | $93 | $186 | $372 |
| **Conservative** | $45 | $135 | $270 | $540 |
| **Minimal** | $10 | $30 | $60 | $120 |

**Savings with Aggressive:** ~80% cost reduction ($1,428/year saved)

---

## ⚠️ Important Considerations

### **Before Enabling:**

1. ✅ **Verify Lambda is processing files successfully**
   ```bash
   aws logs tail /aws/lambda/firebolt-cdc-processor \
     --since 24h \
     --region ap-south-1 \
     | grep "✓ MERGE completed"
   ```

2. ✅ **Check Firebolt data is complete**
   ```sql
   SELECT table_name, COUNT(*) as row_count
   FROM information_schema.tables
   WHERE table_schema = 'public'
   ORDER BY table_name;
   ```

3. ✅ **Test retriggering Lambda for old files**
   - Use `retrigger_lambda_for_old_files.py` to verify you can reprocess if needed

---

### **Safety Tips:**

1. **Start with Conservative strategy** for first month
2. **Monitor for 2-4 weeks** before switching to Aggressive
3. **Keep detailed logs** of Lambda processing (CloudWatch retention 30+ days)
4. **Document table row counts** before enabling lifecycle

---

## 🎯 Recommended Implementation Plan

### **Week 1-2: Preparation**
```bash
# 1. Verify Lambda success rate
aws logs filter-pattern /aws/lambda/firebolt-cdc-processor \
  --filter-pattern "✓ MERGE completed" \
  --start-time $(date -d '7 days ago' +%s)000 \
  --region ap-south-1

# 2. Document current Firebolt row counts
# Run SQL: SELECT table_name, COUNT(*) FROM each table

# 3. Test retrigger script
python3 retrigger_lambda_selective.py --tables users,sessions
```

### **Week 3: Enable Conservative Lifecycle**
- Apply Conservative strategy (180-day deletion)
- Monitor for 2 weeks

### **Week 5: Switch to Aggressive**
- If no issues, switch to Aggressive strategy (90-day deletion)
- Monitor ongoing

---

## 📝 Alternative: CLI Implementation

### **Save Policy to File:**
```bash
cat > lifecycle-policy.json << 'EOF'
{
  "Rules": [
    {
      "Id": "ArchiveOldCDCFiles",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "firebolt_dms_job/"
      },
      "Transitions": [
        {
          "Days": 7,
          "StorageClass": "INTELLIGENT_TIERING"
        },
        {
          "Days": 30,
          "StorageClass": "GLACIER_IR"
        }
      ],
      "Expiration": {
        "Days": 90
      }
    }
  ]
}
EOF
```

### **Apply Policy:**
```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket fcanalytics \
  --lifecycle-configuration file://lifecycle-policy.json \
  --region ap-south-1
```

### **Verify:**
```bash
aws s3api get-bucket-lifecycle-configuration \
  --bucket fcanalytics \
  --region ap-south-1
```

---

## 🚨 Emergency: Disable Lifecycle Rule

### **AWS Console:**
1. S3 → `fcanalytics` → **Management** tab
2. Select `Archive-Old-CDC-Files` rule
3. Click **"Edit"**
4. Change **Status** to `Disabled`
5. Click **"Save changes"**

### **AWS CLI:**
```bash
# Download current policy
aws s3api get-bucket-lifecycle-configuration \
  --bucket fcanalytics \
  --region ap-south-1 > current-policy.json

# Edit policy: Change "Status": "Enabled" to "Status": "Disabled"

# Apply updated policy
aws s3api put-bucket-lifecycle-configuration \
  --bucket fcanalytics \
  --lifecycle-configuration file://current-policy.json \
  --region ap-south-1
```

---

## ✅ Summary

| Action | Timeline | Tool |
|--------|----------|------|
| **Implement Lifecycle** | Week 3 | AWS Console (GUI) |
| **Monitor Success Rate** | Ongoing | CloudWatch Logs |
| **Verify Data Integrity** | Weekly | Firebolt SQL queries |
| **Review Costs** | Monthly | AWS Cost Explorer |

**Recommended:** Start with **Aggressive (90-day)** strategy after verifying Lambda success rate for 1-2 weeks.

**Annual Savings:** ~$1,400 for 1TB data/month

---

## 📞 Questions?

**Q: What if I need to reprocess old files after they're deleted?**  
A: Keep CloudWatch logs for 90+ days, document table row counts, and test retrigger scripts before enabling lifecycle.

**Q: Can I retrieve files from Glacier?**  
A: Yes, Glacier Instant Retrieval provides instant access. Glacier Flexible takes 3-5 hours.

**Q: What if Lambda missed some files?**  
A: This is why Conservative strategy exists. Start there, verify data integrity, then switch to Aggressive.

---

**Ready to implement? Start with the GUI method (easiest) or CLI method (scriptable).** 🚀

