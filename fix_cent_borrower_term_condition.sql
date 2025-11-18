-- ═══════════════════════════════════════════════════════════════════
-- FIX SCHEMA: cent_borrower_term_condition
-- ═══════════════════════════════════════════════════════════════════
-- Issue: loan_id has precision mismatch
--   Production: numeric(20, 0)
--   Parquet:    numeric(38, 0)
-- Solution: Recreate table using READ_PARQUET to match source schema
-- ═══════════════════════════════════════════════════════════════════

-- Step 1: Rename existing table as backup
ALTER TABLE "public"."cent_borrower_term_condition" 
RENAME TO "cent_borrower_term_condition_backup_20251111";

-- Step 2: Create new table with correct schema from Parquet
CREATE TABLE "public"."cent_borrower_term_condition" AS
SELECT * EXCLUDE ("Op", "load_timestamp")
FROM READ_PARQUET(
    LOCATION => 's3_raw_dms',
    PATTERN => 'fair/cent_borrower_term_condition/**/*.parquet'
)
LIMIT 0;

-- Step 3: (Optional) Copy existing data from backup if needed
-- Uncomment if you want to preserve existing data:
-- INSERT INTO "public"."cent_borrower_term_condition"
-- SELECT * FROM "public"."cent_borrower_term_condition_backup_20251111";

-- Step 4: Verify new schema
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'cent_borrower_term_condition'
ORDER BY ordinal_position;

-- ═══════════════════════════════════════════════════════════════════
-- DONE! New table will match Parquet schema exactly.
-- Lambda will now process new files for this table successfully.
-- ═══════════════════════════════════════════════════════════════════

