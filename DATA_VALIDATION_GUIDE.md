# 📊 Data Validation Guide: Redshift ↔ Firebolt

## 🎯 Goal

Validate that all data from Redshift has been correctly migrated to Firebolt.

---

## 🚀 Quick Start (3 Steps)

### **Step 1: Run Summary Query in Both Systems**

**Firebolt:**
```sql
SELECT 
    COUNT(DISTINCT table_name) as total_tables,
    SUM(table_rows) as total_rows
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE';
```

**Redshift:**
```sql
SELECT 
    COUNT(DISTINCT tablename) as total_tables,
    SUM(n_live_tup) as total_rows
FROM pg_stat_user_tables
WHERE schemaname = 'fair';  -- Your schema name
```

**Compare:** Do the numbers match?

---

### **Step 2: Get Table-by-Table Row Counts**

**Firebolt:**
```sql
SELECT 
    table_name,
    table_rows as row_count
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;
```

**Export to CSV:** `firebolt_counts.csv`

**Redshift:**
```sql
SELECT 
    tablename as table_name,
    n_live_tup as row_count
FROM pg_stat_user_tables
WHERE schemaname = 'fair'
ORDER BY tablename;
```

**Export to CSV:** `redshift_counts.csv`

---

### **Step 3: Compare in Excel**

1. Open both CSVs in Excel
2. Use `VLOOKUP` to compare:
   ```
   =IF(Redshift_Count = Firebolt_Count, "MATCH", "MISMATCH")
   ```
3. Filter for "MISMATCH" to find discrepancies

---

## 📋 Validation Methods

### **Method 1: SQL Queries (Manual)**

**Files:** `DATA_VALIDATION_QUERIES.sql`

**18 different validation queries:**
1. ✅ Row Count per Table
2. ✅ Row Count for Specific Table
3. ✅ Aggregate Statistics
4. ✅ Data Distribution by Date
5. ✅ NULL Value Counts
6. ✅ Duplicate Check
7. ✅ Sample Data Comparison
8. ✅ Checksum/Hash Validation
9. ✅ Record Count by Status
10. ✅ Data Freshness Check
11. ✅ Column Count Validation
12. ✅ Detailed Schema Comparison
13. ✅ Date Range Distribution
14. ✅ All 809 Tables Validation
15. ✅ TOP 100 Tables by Row Count
16. ✅ Empty Tables Check
17. ✅ Row Count Mismatch Finder
18. ✅ Summary Report

---

### **Method 2: Python Script (Automated)**

**File:** `validate_data_redshift_firebolt.py`

**Setup:**
```bash
# Install dependencies
pip install psycopg2-binary pandas firebolt-sdk

# Set environment variables
export FIREBOLT_CLIENT_ID="your_client_id"
export FIREBOLT_CLIENT_SECRET="your_client_secret"
export FIREBOLT_ACCOUNT="faircentindia"
export FIREBOLT_DATABASE="fair"
export FIREBOLT_ENGINE="general_purpose"

export REDSHIFT_HOST="your-redshift-cluster.region.redshift.amazonaws.com"
export REDSHIFT_PORT="5439"
export REDSHIFT_DATABASE="your_database"
export REDSHIFT_USER="your_user"
export REDSHIFT_PASSWORD="your_password"

# Run script
python3 validate_data_redshift_firebolt.py
```

**Output:**
```
Total Tables: 809
✓ Matches: 805 (99.5%)
✗ Mismatches: 4 (0.5%)

MISMATCHES (4 tables)
table_name           redshift_count  firebolt_count  difference  pct_diff
users                1000000         999999          1           0.00%
sessions             500000          500100          -100        -0.02%

💾 Report saved to: validation_report_20251114_120000.csv
```

---

## 🔍 Detailed Validation (For Mismatches)

### **Query 1: Row Count + Aggregates**

```sql
-- Run in BOTH systems, compare all values
SELECT 
    'users' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT id) as unique_ids,
    MIN(created) as earliest_record,
    MAX(created) as latest_record
FROM "public"."users";  -- Firebolt
-- FROM fair.users;    -- Redshift
```

**If values match:** Data is identical ✓

---

### **Query 2: Checksum Validation**

```sql
-- Quick integrity check
SELECT 
    COUNT(*) as row_count,
    SUM(CAST(id AS BIGINT)) as sum_ids,
    MD5(CAST(SUM(CAST(id AS BIGINT)) AS VARCHAR)) as checksum
FROM "public"."users";  -- Firebolt
-- FROM fair.users;    -- Redshift
```

**If checksum matches:** Data integrity verified ✓

---

### **Query 3: Sample Data Comparison**

```sql
-- First 10 rows
SELECT *
FROM "public"."users"
ORDER BY id ASC
LIMIT 10;
```

Compare manually between systems

---

## 📊 Handling 10k Row Limit

### **Problem:**
Firebolt limits query results to 10,000 rows

### **Solution:**
Use **aggregations** instead of raw data:

❌ **Don't do this:**
```sql
-- This will be truncated at 10k rows
SELECT * FROM users;
```

✅ **Do this instead:**
```sql
-- Aggregated, always under 10k
SELECT 
    table_name,
    COUNT(*) as row_count
FROM information_schema.tables
GROUP BY table_name;
```

---

### **For Large Tables (>10k rows):**

**Option 1: Aggregate by Date**
```sql
SELECT 
    DATE(created) as date,
    COUNT(*) as row_count
FROM "public"."users"
GROUP BY DATE(created)
ORDER BY date;
-- Will return 1 row per day (e.g., 365 rows for 1 year)
```

**Option 2: Aggregate by Month**
```sql
SELECT 
    DATE_TRUNC('month', created) as month,
    COUNT(*) as row_count
FROM "public"."users"
GROUP BY DATE_TRUNC('month', created)
ORDER BY month;
-- Will return 1 row per month (e.g., 12 rows for 1 year)
```

**Option 3: Use Checksum**
```sql
-- Single row result
SELECT 
    COUNT(*) as total,
    SUM(CAST(id AS BIGINT)) as checksum
FROM "public"."users";
```

---

## 🎯 Validation Checklist

### **Level 1: Quick Validation (5 minutes)**

- [ ] Run Query 18 (Summary Report) in both systems
- [ ] Compare total tables and total rows
- [ ] If match: ✓ Done!
- [ ] If mismatch: Continue to Level 2

---

### **Level 2: Table-Level Validation (30 minutes)**

- [ ] Run Query 1 (Row Count per Table) in both systems
- [ ] Export to CSV and compare in Excel
- [ ] Identify mismatched tables
- [ ] Continue to Level 3 for mismatches

---

### **Level 3: Detailed Validation (per table)**

For each mismatched table:

- [ ] Run Query 2 (Row Count + Aggregates)
- [ ] Run Query 8 (Checksum)
- [ ] Run Query 5 (NULL counts)
- [ ] Run Query 6 (Duplicate check)
- [ ] If still mismatched: Manual investigation

---

## 🔧 Common Issues & Solutions

### **Issue 1: Row Count Mismatch**

**Possible Causes:**
1. ✅ CDC still in progress (data still loading)
2. ✅ Deleted records (Redshift has soft deletes)
3. ✅ Duplicate records

**Solution:**
```sql
-- Check for soft deletes
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN deleted = 1 THEN 1 ELSE 0 END) as deleted_count,
    SUM(CASE WHEN deleted = 0 THEN 1 ELSE 0 END) as active_count
FROM "public"."users";
```

---

### **Issue 2: Data Freshness**

**Check latest records:**
```sql
SELECT 
    MAX(created) as latest_record,
    DATEDIFF('minute', MAX(created), CURRENT_TIMESTAMP) as minutes_old
FROM "public"."users";
```

**If > 30 minutes old:** DMS/Lambda may have stopped

---

### **Issue 3: Missing Tables**

**Find tables in Redshift but not in Firebolt:**
```sql
-- Redshift
SELECT tablename 
FROM pg_stat_user_tables 
WHERE schemaname = 'fair'
ORDER BY tablename;

-- Firebolt
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public'
ORDER BY table_name;
```

Compare lists manually

---

## 📈 Advanced Validation

### **Checksum by Date Range**

```sql
-- Validate specific date range
SELECT 
    DATE(created) as date,
    COUNT(*) as row_count,
    SUM(CAST(id AS BIGINT)) as checksum
FROM "public"."users"
WHERE created BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY DATE(created)
ORDER BY date;
```

Run in both systems, compare checksums

---

### **Schema Validation**

```sql
-- Get full schema
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position;
```

Export and compare schemas

---

## 💾 Export Results

### **Firebolt → CSV**

1. Run query in Firebolt UI
2. Click "Download" button
3. Choose CSV format
4. Save as `firebolt_results.csv`

### **Redshift → CSV**

**Option 1: Query Editor**
```
1. AWS Console → Redshift → Query Editor
2. Run query
3. Click "Export to CSV"
```

**Option 2: psql**
```bash
psql -h your-cluster.redshift.amazonaws.com \
     -U your_user \
     -d your_database \
     -c "COPY (SELECT * FROM pg_stat_user_tables WHERE schemaname='fair') TO STDOUT WITH CSV HEADER" \
     > redshift_results.csv
```

---

## ✅ Final Validation Report

### **Create Summary Document:**

```
═══════════════════════════════════════════════════════════════
  DATA VALIDATION REPORT
═══════════════════════════════════════════════════════════════
Date: 2024-11-14
Validator: [Your Name]

SUMMARY:
  Total Tables: 809
  Matched: 805 (99.5%)
  Mismatched: 4 (0.5%)
  
REDSHIFT:
  Total Rows: 50,000,000
  Total Tables: 809
  
FIREBOLT:
  Total Rows: 49,999,500
  Total Tables: 809
  
DIFFERENCE: 500 rows (0.001%)

MISMATCHED TABLES:
  1. users: -100 rows (soft deletes)
  2. sessions: +200 rows (duplicates in Redshift)
  3. cent_borrower_transaction: -500 rows (investigating)
  4. cent_user_login_log: +300 rows (CDC in progress)

CONCLUSION:
  ✓ Data migration 99.999% successful
  ⚠️ 4 tables require investigation
  ✓ No data integrity issues detected
  
═══════════════════════════════════════════════════════════════
```

---

## 🎯 Summary

| Method | Time | Accuracy | Best For |
|--------|------|----------|----------|
| **Query 18 (Summary)** | 1 min | High-level | Initial check |
| **Query 1 (Table Counts)** | 5 min | Table-level | Finding mismatches |
| **Python Script** | 10 min | Automated | Full validation |
| **Detailed Queries** | Per table | Row-level | Deep investigation |

**Recommended Workflow:**
1. Start with Summary (Query 18)
2. If mismatch → Table Counts (Query 1)
3. If specific mismatches → Detailed Queries
4. For automation → Python script

---

**Files:**
- `DATA_VALIDATION_QUERIES.sql` - 18 validation queries
- `validate_data_redshift_firebolt.py` - Automated validation script
- `DATA_VALIDATION_GUIDE.md` - This guide

**Ready to validate!** 🚀

