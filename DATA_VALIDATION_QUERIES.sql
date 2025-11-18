-- ═══════════════════════════════════════════════════════════════════
-- DATA VALIDATION: Redshift vs Firebolt
-- ═══════════════════════════════════════════════════════════════════
-- Note: Firebolt has 10k row limit, so we use aggregations
-- Run these queries in BOTH Redshift and Firebolt, then compare results
-- ═══════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 1: Row Count per Table (Most Important!)
-- ═══════════════════════════════════════════════════════════════════

-- Firebolt: Get row counts for all tables
SELECT 
    table_name,
    table_rows as row_count
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- Redshift: Get row counts for all tables (run this in Redshift)
/*
SELECT 
    schemaname || '.' || tablename as table_name,
    n_live_tup as row_count
FROM pg_stat_user_tables
WHERE schemaname = 'fair'  -- Your schema name
ORDER BY tablename;
*/

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 2: Row Count for Specific Table
-- ═══════════════════════════════════════════════════════════════════

-- Replace 'users' with your table name
SELECT 
    'users' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT id) as unique_ids,
    MIN(created) as earliest_record,
    MAX(created) as latest_record
FROM "public"."users";

-- Run same query in Redshift, compare all 5 values

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 3: Aggregate Statistics per Table
-- ═══════════════════════════════════════════════════════════════════

-- For numeric columns (e.g., loan amount, transaction amount)
SELECT 
    'cent_borrower_transaction' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT id) as unique_ids,
    SUM(txn_amount) as total_amount,
    AVG(txn_amount) as avg_amount,
    MIN(txn_amount) as min_amount,
    MAX(txn_amount) as max_amount,
    MIN(created) as earliest_date,
    MAX(created) as latest_date
FROM "public"."cent_borrower_transaction";

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 4: Data Distribution by Date
-- ═══════════════════════════════════════════════════════════════════

-- Check row counts by day (last 30 days)
SELECT 
    DATE(created) as date,
    COUNT(*) as row_count,
    COUNT(DISTINCT uid) as unique_users,
    SUM(CASE WHEN deleted = 1 THEN 1 ELSE 0 END) as deleted_count
FROM "public"."users"
WHERE created >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(created)
ORDER BY date DESC
LIMIT 10000;  -- Within 10k limit

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 5: NULL Value Counts
-- ═══════════════════════════════════════════════════════════════════

-- Check for NULL values in key columns
SELECT 
    'users' as table_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) as null_id,
    SUM(CASE WHEN emailid IS NULL THEN 1 ELSE 0 END) as null_email,
    SUM(CASE WHEN created IS NULL THEN 1 ELSE 0 END) as null_created,
    SUM(CASE WHEN deleted IS NULL THEN 1 ELSE 0 END) as null_deleted
FROM "public"."users";

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 6: Duplicate Check
-- ═══════════════════════════════════════════════════════════════════

-- Find tables with duplicate primary keys
SELECT 
    id,
    COUNT(*) as duplicate_count
FROM "public"."users"
GROUP BY id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 100;

-- If this returns 0 rows, no duplicates! ✓

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 7: Sample Data Comparison (First 10 rows)
-- ═══════════════════════════════════════════════════════════════════

-- Get first 10 rows ordered by ID
SELECT *
FROM "public"."users"
ORDER BY id ASC
LIMIT 10;

-- Run in both systems, compare values manually

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 8: Checksum/Hash Validation
-- ═══════════════════════════════════════════════════════════════════

-- Create a hash of aggregated data for quick comparison
SELECT 
    'users' as table_name,
    COUNT(*) as row_count,
    SUM(CAST(id AS BIGINT)) as sum_ids,
    MD5(CAST(SUM(CAST(id AS BIGINT)) AS VARCHAR)) as checksum
FROM "public"."users";

-- If row_count and checksum match between Redshift and Firebolt = DATA MATCHES!

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 9: Record Count by Status/Category
-- ═══════════════════════════════════════════════════════════════════

-- Count records by status (adjust column names)
SELECT 
    status,
    COUNT(*) as count,
    MIN(created) as first_seen,
    MAX(created) as last_seen
FROM "public"."cent_user_login_log"
GROUP BY status
ORDER BY count DESC;

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 10: Data Freshness Check
-- ═══════════════════════════════════════════════════════════════════

-- Check most recent records per table
SELECT 
    'users' as table_name,
    COUNT(*) as total_rows,
    MAX(created) as latest_record,
    DATEDIFF('minute', MAX(created), CURRENT_TIMESTAMP) as minutes_old
FROM "public"."users"

UNION ALL

SELECT 
    'sessions' as table_name,
    COUNT(*) as total_rows,
    MAX(timestamp) as latest_record,
    DATEDIFF('minute', MAX(timestamp), CURRENT_TIMESTAMP) as minutes_old
FROM "public"."sessions"

UNION ALL

SELECT 
    'cent_borrower_transaction' as table_name,
    COUNT(*) as total_rows,
    MAX(created) as latest_record,
    DATEDIFF('minute', MAX(created), CURRENT_TIMESTAMP) as minutes_old
FROM "public"."cent_borrower_transaction"

ORDER BY table_name;

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 11: Column Count and Schema Validation
-- ═══════════════════════════════════════════════════════════════════

-- Get column count per table
SELECT 
    table_name,
    COUNT(*) as column_count
FROM information_schema.columns
WHERE table_schema = 'public'
GROUP BY table_name
ORDER BY table_name;

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 12: Detailed Schema Comparison
-- ═══════════════════════════════════════════════════════════════════

-- Get schema for specific table
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'users'  -- Change table name
ORDER BY ordinal_position;

-- Compare this with Redshift schema

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 13: Date Range Distribution
-- ═══════════════════════════════════════════════════════════════════

-- Check data distribution by month
SELECT 
    DATE_TRUNC('month', created) as month,
    COUNT(*) as row_count,
    COUNT(DISTINCT uid) as unique_users
FROM "public"."users"
WHERE created >= '2024-01-01'
GROUP BY DATE_TRUNC('month', created)
ORDER BY month DESC;

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 14: Specific Tables Validation (All 809 Tables)
-- ═══════════════════════════════════════════════════════════════════

-- Quick validation for all tables at once
SELECT 
    t.table_name,
    t.table_rows as firebolt_rows,
    (SELECT COUNT(*) FROM information_schema.columns 
     WHERE table_schema = 'public' AND table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE t.table_schema = 'public'
  AND t.table_type = 'BASE TABLE'
ORDER BY t.table_name
LIMIT 10000;

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 15: TOP 100 Tables by Row Count
-- ═══════════════════════════════════════════════════════════════════

-- See which tables have most data
SELECT 
    table_name,
    table_rows as row_count
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE'
ORDER BY table_rows DESC
LIMIT 100;

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 16: Empty Tables Check
-- ═══════════════════════════════════════════════════════════════════

-- Find tables with 0 rows
SELECT 
    table_name,
    table_rows
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE'
  AND table_rows = 0
ORDER BY table_name;

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 17: Row Count Mismatch Finder
-- ═══════════════════════════════════════════════════════════════════

-- Export Firebolt counts to CSV, compare with Redshift
SELECT 
    table_name,
    table_rows as firebolt_count,
    0 as redshift_count,  -- Fill this manually from Redshift
    0 as difference       -- Calculate: redshift_count - firebolt_count
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;

-- ═══════════════════════════════════════════════════════════════════
-- VALIDATION 18: Summary Report (Run This First!)
-- ═══════════════════════════════════════════════════════════════════

-- Overall summary
SELECT 
    COUNT(DISTINCT table_name) as total_tables,
    SUM(table_rows) as total_rows,
    AVG(table_rows) as avg_rows_per_table,
    MAX(table_rows) as largest_table_rows,
    MIN(table_rows) as smallest_table_rows
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE';

-- ═══════════════════════════════════════════════════════════════════
-- HOW TO USE THESE QUERIES:
-- ═══════════════════════════════════════════════════════════════════
--
-- 1. Start with VALIDATION 18 (Summary Report) in both systems
-- 2. Run VALIDATION 1 (Row Count per Table) in both systems
-- 3. Export both results to Excel/CSV
-- 4. Use VLOOKUP or Python to compare:
--    =IF(Redshift_Count = Firebolt_Count, "MATCH", "MISMATCH")
-- 5. For mismatches, run detailed validations (2-17) on specific tables
--
-- ═══════════════════════════════════════════════════════════════════

