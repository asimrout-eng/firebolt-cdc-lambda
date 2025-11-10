-- ═══════════════════════════════════════════════════════════════════
-- URGENT: Fix 9 Tables with DECIMAL Precision Errors
-- ═══════════════════════════════════════════════════════════════════
--
-- These 9 tables are COMPLETELY BLOCKED due to DECIMAL precision mismatch
-- Run this SQL in Firebolt SQL editor: https://go.firebolt.io/
--
-- Time to run: ~3 minutes
-- Safety: Creates backups with "_old_20251110" suffix
--
-- Tables Fixed:
--   1. cent_borrower_transaction
--   2. cent_communications_log
--   3. cent_ekyc_verification_logs
--   4. cent_emi
--   5. cent_emi_err_log
--   6. cent_inv_escrow_upi_transaction
--   7. cent_pre_borrower_transaction
--   8. cent_user_thirdparty_ekyc_details
--   9. payment_source
--
-- ═══════════════════════════════════════════════════════════════════

BEGIN;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- [1/9] Fix: cent_borrower_transaction
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- Backup original table
ALTER TABLE "public"."cent_borrower_transaction" RENAME TO "cent_borrower_transaction_old_20251110";

-- Create new table with correct schema from parquet
CREATE TABLE "public"."cent_borrower_transaction" AS
SELECT * EXCLUDE ("Op", "load_timestamp")
FROM READ_PARQUET(
    LOCATION => 's3_raw_dms',
    PATTERN => 'fair/cent_borrower_transaction/2025/11/*/*.parquet'
)
LIMIT 0;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- [2/9] Fix: cent_communications_log
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- Backup original table
ALTER TABLE "public"."cent_communications_log" RENAME TO "cent_communications_log_old_20251110";

-- Create new table with correct schema from parquet
CREATE TABLE "public"."cent_communications_log" AS
SELECT * EXCLUDE ("Op", "load_timestamp")
FROM READ_PARQUET(
    LOCATION => 's3_raw_dms',
    PATTERN => 'fair/cent_communications_log/2025/11/*/*.parquet'
)
LIMIT 0;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- [3/9] Fix: cent_ekyc_verification_logs
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- Backup original table
ALTER TABLE "public"."cent_ekyc_verification_logs" RENAME TO "cent_ekyc_verification_logs_old_20251110";

-- Create new table with correct schema from parquet
CREATE TABLE "public"."cent_ekyc_verification_logs" AS
SELECT * EXCLUDE ("Op", "load_timestamp")
FROM READ_PARQUET(
    LOCATION => 's3_raw_dms',
    PATTERN => 'fair/cent_ekyc_verification_logs/2025/11/*/*.parquet'
)
LIMIT 0;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- [4/9] Fix: cent_emi
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- Backup original table
ALTER TABLE "public"."cent_emi" RENAME TO "cent_emi_old_20251110";

-- Create new table with correct schema from parquet
CREATE TABLE "public"."cent_emi" AS
SELECT * EXCLUDE ("Op", "load_timestamp")
FROM READ_PARQUET(
    LOCATION => 's3_raw_dms',
    PATTERN => 'fair/cent_emi/2025/11/*/*.parquet'
)
LIMIT 0;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- [5/9] Fix: cent_emi_err_log
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- Backup original table
ALTER TABLE "public"."cent_emi_err_log" RENAME TO "cent_emi_err_log_old_20251110";

-- Create new table with correct schema from parquet
CREATE TABLE "public"."cent_emi_err_log" AS
SELECT * EXCLUDE ("Op", "load_timestamp")
FROM READ_PARQUET(
    LOCATION => 's3_raw_dms',
    PATTERN => 'fair/cent_emi_err_log/2025/11/*/*.parquet'
)
LIMIT 0;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- [6/9] Fix: cent_inv_escrow_upi_transaction
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- Backup original table
ALTER TABLE "public"."cent_inv_escrow_upi_transaction" RENAME TO "cent_inv_escrow_upi_transaction_old_20251110";

-- Create new table with correct schema from parquet
CREATE TABLE "public"."cent_inv_escrow_upi_transaction" AS
SELECT * EXCLUDE ("Op", "load_timestamp")
FROM READ_PARQUET(
    LOCATION => 's3_raw_dms',
    PATTERN => 'fair/cent_inv_escrow_upi_transaction/2025/11/*/*.parquet'
)
LIMIT 0;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- [7/9] Fix: cent_pre_borrower_transaction
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- Backup original table
ALTER TABLE "public"."cent_pre_borrower_transaction" RENAME TO "cent_pre_borrower_transaction_old_20251110";

-- Create new table with correct schema from parquet
CREATE TABLE "public"."cent_pre_borrower_transaction" AS
SELECT * EXCLUDE ("Op", "load_timestamp")
FROM READ_PARQUET(
    LOCATION => 's3_raw_dms',
    PATTERN => 'fair/cent_pre_borrower_transaction/2025/11/*/*.parquet'
)
LIMIT 0;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- [8/9] Fix: cent_user_thirdparty_ekyc_details
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- Backup original table
ALTER TABLE "public"."cent_user_thirdparty_ekyc_details" RENAME TO "cent_user_thirdparty_ekyc_details_old_20251110";

-- Create new table with correct schema from parquet
CREATE TABLE "public"."cent_user_thirdparty_ekyc_details" AS
SELECT * EXCLUDE ("Op", "load_timestamp")
FROM READ_PARQUET(
    LOCATION => 's3_raw_dms',
    PATTERN => 'fair/cent_user_thirdparty_ekyc_details/2025/11/*/*.parquet'
)
LIMIT 0;

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- [9/9] Fix: payment_source
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- Backup original table
ALTER TABLE "public"."payment_source" RENAME TO "payment_source_old_20251110";

-- Create new table with correct schema from parquet
CREATE TABLE "public"."payment_source" AS
SELECT * EXCLUDE ("Op", "load_timestamp")
FROM READ_PARQUET(
    LOCATION => 's3_raw_dms',
    PATTERN => 'fair/payment_source/2025/11/*/*.parquet'
)
LIMIT 0;

COMMIT;

-- ═══════════════════════════════════════════════════════════════════
-- ✅ SCHEMA FIX COMPLETE FOR 9 TABLES!
-- ═══════════════════════════════════════════════════════════════════
--
-- NEXT STEPS:
--
-- 1. Verify schema looks correct for each table:
--    SELECT * FROM "cent_borrower_transaction" LIMIT 1;
--    SELECT * FROM "cent_communications_log" LIMIT 1;
--    SELECT * FROM "cent_ekyc_verification_logs" LIMIT 1;
--    SELECT * FROM "cent_emi" LIMIT 1;
--    SELECT * FROM "cent_emi_err_log" LIMIT 1;
--    SELECT * FROM "cent_inv_escrow_upi_transaction" LIMIT 1;
--    SELECT * FROM "cent_pre_borrower_transaction" LIMIT 1;
--    SELECT * FROM "cent_user_thirdparty_ekyc_details" LIMIT 1;
--    SELECT * FROM "payment_source" LIMIT 1;
--
-- 2. Copy data from old tables (if needed):
--    INSERT INTO "cent_borrower_transaction" SELECT * FROM "cent_borrower_transaction_old_20251110";
--    INSERT INTO "cent_communications_log" SELECT * FROM "cent_communications_log_old_20251110";
--    INSERT INTO "cent_ekyc_verification_logs" SELECT * FROM "cent_ekyc_verification_logs_old_20251110";
--    INSERT INTO "cent_emi" SELECT * FROM "cent_emi_old_20251110";
--    INSERT INTO "cent_emi_err_log" SELECT * FROM "cent_emi_err_log_old_20251110";
--    INSERT INTO "cent_inv_escrow_upi_transaction" SELECT * FROM "cent_inv_escrow_upi_transaction_old_20251110";
--    INSERT INTO "cent_pre_borrower_transaction" SELECT * FROM "cent_pre_borrower_transaction_old_20251110";
--    INSERT INTO "cent_user_thirdparty_ekyc_details" SELECT * FROM "cent_user_thirdparty_ekyc_details_old_20251110";
--    INSERT INTO "payment_source" SELECT * FROM "payment_source_old_20251110";
--
-- 3. Drop old tables (after verification):
--    DROP TABLE "cent_borrower_transaction_old_20251110";
--    DROP TABLE "cent_communications_log_old_20251110";
--    DROP TABLE "cent_ekyc_verification_logs_old_20251110";
--    DROP TABLE "cent_emi_old_20251110";
--    DROP TABLE "cent_emi_err_log_old_20251110";
--    DROP TABLE "cent_inv_escrow_upi_transaction_old_20251110";
--    DROP TABLE "cent_pre_borrower_transaction_old_20251110";
--    DROP TABLE "cent_user_thirdparty_ekyc_details_old_20251110";
--    DROP TABLE "payment_source_old_20251110";
--
-- 4. Monitor Lambda logs for 15 minutes:
--    aws logs tail /aws/lambda/firebolt-cdc-processor --follow --region ap-south-1
--
-- Expected: No more DECIMAL errors for these 9 tables!
--
-- ═══════════════════════════════════════════════════════════════════

