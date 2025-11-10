-- ═══════════════════════════════════════════════════════════════════
-- SCHEMA FIX: 36 Tables with DECIMAL/Type Mismatches
-- ═══════════════════════════════════════════════════════════════════
-- NOTE: PRIMARY KEY will NOT be created (Firebolt limitation)
-- Lambda MERGE will still work (keys specified in ON clause)
--
-- Primary Keys (for reference, not applied):
--   - 32 tables: "id"
--   - cent_crif_log_data: "score"
--   - cent_user: "uid"
--   - sessions: "sid"
--   - users: "uid"
-- ═══════════════════════════════════════════════════════════════════

BEGIN;

-- [1/36] cent_aadhar_address_data (PK: id)
ALTER TABLE "public"."cent_aadhar_address_data" RENAME TO "cent_aadhar_address_data_old_20251110";
CREATE TABLE "public"."cent_aadhar_address_data" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_aadhar_address_data/2025/11/*/*.parquet') LIMIT 0;

-- [2/36] cent_bank_details (PK: id)
ALTER TABLE "public"."cent_bank_details" RENAME TO "cent_bank_details_old_20251110";
CREATE TABLE "public"."cent_bank_details" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_bank_details/2025/11/*/*.parquet') LIMIT 0;

-- [3/36] cent_ckyc_search (PK: id)
ALTER TABLE "public"."cent_ckyc_search" RENAME TO "cent_ckyc_search_old_20251110";
CREATE TABLE "public"."cent_ckyc_search" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_ckyc_search/2025/11/*/*.parquet') LIMIT 0;

-- [4/36] cent_crif_enquiry_logs (PK: id)
ALTER TABLE "public"."cent_crif_enquiry_logs" RENAME TO "cent_crif_enquiry_logs_old_20251110";
CREATE TABLE "public"."cent_crif_enquiry_logs" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_crif_enquiry_logs/2025/11/*/*.parquet') LIMIT 0;

-- [5/36] cent_crif_loan_details (PK: id)
ALTER TABLE "public"."cent_crif_loan_details" RENAME TO "cent_crif_loan_details_old_20251110";
CREATE TABLE "public"."cent_crif_loan_details" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_crif_loan_details/2025/11/*/*.parquet') LIMIT 0;

-- [6/36] cent_crif_log_data (PK: score)
ALTER TABLE "public"."cent_crif_log_data" RENAME TO "cent_crif_log_data_old_20251110";
CREATE TABLE "public"."cent_crif_log_data" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_crif_log_data/2025/11/*/*.parquet') LIMIT 0;

-- [7/36] cent_crif_log_information (PK: id)
ALTER TABLE "public"."cent_crif_log_information" RENAME TO "cent_crif_log_information_old_20251110";
CREATE TABLE "public"."cent_crif_log_information" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_crif_log_information/2025/11/*/*.parquet') LIMIT 0;

-- [8/36] cent_crif_variations (PK: id)
ALTER TABLE "public"."cent_crif_variations" RENAME TO "cent_crif_variations_old_20251110";
CREATE TABLE "public"."cent_crif_variations" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_crif_variations/2025/11/*/*.parquet') LIMIT 0;

-- [9/36] cent_emi_settlement (PK: id)
ALTER TABLE "public"."cent_emi_settlement" RENAME TO "cent_emi_settlement_old_20251110";
CREATE TABLE "public"."cent_emi_settlement" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_emi_settlement/2025/11/*/*.parquet') LIMIT 0;

-- [10/36] cent_employment (PK: id)
ALTER TABLE "public"."cent_employment" RENAME TO "cent_employment_old_20251110";
CREATE TABLE "public"."cent_employment" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_employment/2025/11/*/*.parquet') LIMIT 0;

-- [11/36] cent_fd_ploss_proposal (PK: id)
ALTER TABLE "public"."cent_fd_ploss_proposal" RENAME TO "cent_fd_ploss_proposal_old_20251110";
CREATE TABLE "public"."cent_fd_ploss_proposal" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_fd_ploss_proposal/2025/11/*/*.parquet') LIMIT 0;

-- [12/36] cent_inv (PK: id)
ALTER TABLE "public"."cent_inv" RENAME TO "cent_inv_old_20251110";
CREATE TABLE "public"."cent_inv" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_inv/2025/11/*/*.parquet') LIMIT 0;

-- [13/36] cent_loan (PK: id)
ALTER TABLE "public"."cent_loan" RENAME TO "cent_loan_old_20251110";
CREATE TABLE "public"."cent_loan" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_loan/2025/11/*/*.parquet') LIMIT 0;

-- [14/36] cent_loan_auto_execution_status (PK: id)
ALTER TABLE "public"."cent_loan_auto_execution_status" RENAME TO "cent_loan_auto_execution_status_old_20251110";
CREATE TABLE "public"."cent_loan_auto_execution_status" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_loan_auto_execution_status/2025/11/*/*.parquet') LIMIT 0;

-- [15/36] cent_loan_detail (PK: id)
ALTER TABLE "public"."cent_loan_detail" RENAME TO "cent_loan_detail_old_20251110";
CREATE TABLE "public"."cent_loan_detail" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_loan_detail/2025/11/*/*.parquet') LIMIT 0;

-- [16/36] cent_mail_track (PK: id)
ALTER TABLE "public"."cent_mail_track" RENAME TO "cent_mail_track_old_20251110";
CREATE TABLE "public"."cent_mail_track" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_mail_track/2025/11/*/*.parquet') LIMIT 0;

-- [17/36] cent_mobile_sms (PK: id)
ALTER TABLE "public"."cent_mobile_sms" RENAME TO "cent_mobile_sms_old_20251110";
CREATE TABLE "public"."cent_mobile_sms" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_mobile_sms/2025/11/*/*.parquet') LIMIT 0;

-- [18/36] cent_monitor_log (PK: id)
ALTER TABLE "public"."cent_monitor_log" RENAME TO "cent_monitor_log_old_20251110";
CREATE TABLE "public"."cent_monitor_log" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_monitor_log/2025/11/*/*.parquet') LIMIT 0;

-- [19/36] cent_notification (PK: id)
ALTER TABLE "public"."cent_notification" RENAME TO "cent_notification_old_20251110";
CREATE TABLE "public"."cent_notification" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_notification/2025/11/*/*.parquet') LIMIT 0;

-- [20/36] cent_req_encrypt_data (PK: id)
ALTER TABLE "public"."cent_req_encrypt_data" RENAME TO "cent_req_encrypt_data_old_20251110";
CREATE TABLE "public"."cent_req_encrypt_data" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_req_encrypt_data/2025/11/*/*.parquet') LIMIT 0;

-- [21/36] cent_request_validation (PK: id)
ALTER TABLE "public"."cent_request_validation" RENAME TO "cent_request_validation_old_20251110";
CREATE TABLE "public"."cent_request_validation" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_request_validation/2025/11/*/*.parquet') LIMIT 0;

-- [22/36] cent_sms_service_logs (PK: id)
ALTER TABLE "public"."cent_sms_service_logs" RENAME TO "cent_sms_service_logs_old_20251110";
CREATE TABLE "public"."cent_sms_service_logs" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_sms_service_logs/2025/11/*/*.parquet') LIMIT 0;

-- [23/36] cent_thirdparty_invoice (PK: id)
ALTER TABLE "public"."cent_thirdparty_invoice" RENAME TO "cent_thirdparty_invoice_old_20251110";
CREATE TABLE "public"."cent_thirdparty_invoice" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_thirdparty_invoice/2025/11/*/*.parquet') LIMIT 0;

-- [24/36] cent_transaction (PK: id)
ALTER TABLE "public"."cent_transaction" RENAME TO "cent_transaction_old_20251110";
CREATE TABLE "public"."cent_transaction" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_transaction/2025/11/*/*.parquet') LIMIT 0;

-- [25/36] cent_user (PK: uid)
ALTER TABLE "public"."cent_user" RENAME TO "cent_user_old_20251110";
CREATE TABLE "public"."cent_user" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_user/2025/11/*/*.parquet') LIMIT 0;

-- [26/36] cent_user_auth (PK: id)
ALTER TABLE "public"."cent_user_auth" RENAME TO "cent_user_auth_old_20251110";
CREATE TABLE "public"."cent_user_auth" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_user_auth/2025/11/*/*.parquet') LIMIT 0;

-- [27/36] cent_user_consent (PK: id)
ALTER TABLE "public"."cent_user_consent" RENAME TO "cent_user_consent_old_20251110";
CREATE TABLE "public"."cent_user_consent" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_user_consent/2025/11/*/*.parquet') LIMIT 0;

-- [28/36] cent_user_login_log (PK: id)
ALTER TABLE "public"."cent_user_login_log" RENAME TO "cent_user_login_log_old_20251110";
CREATE TABLE "public"."cent_user_login_log" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_user_login_log/2025/11/*/*.parquet') LIMIT 0;

-- [29/36] cent_user_verification (PK: id)
ALTER TABLE "public"."cent_user_verification" RENAME TO "cent_user_verification_old_20251110";
CREATE TABLE "public"."cent_user_verification" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_user_verification/2025/11/*/*.parquet') LIMIT 0;

-- [30/36] cent_verification_item (PK: id)
ALTER TABLE "public"."cent_verification_item" RENAME TO "cent_verification_item_old_20251110";
CREATE TABLE "public"."cent_verification_item" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_verification_item/2025/11/*/*.parquet') LIMIT 0;

-- [31/36] cent_virtual_account (PK: id)
ALTER TABLE "public"."cent_virtual_account" RENAME TO "cent_virtual_account_old_20251110";
CREATE TABLE "public"."cent_virtual_account" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_virtual_account/2025/11/*/*.parquet') LIMIT 0;

-- [32/36] cent_wallet_logs_data (PK: id)
ALTER TABLE "public"."cent_wallet_logs_data" RENAME TO "cent_wallet_logs_data_old_20251110";
CREATE TABLE "public"."cent_wallet_logs_data" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/cent_wallet_logs_data/2025/11/*/*.parquet') LIMIT 0;

-- [33/36] ftipl_dsa_upload_doc_mail_log (PK: id)
ALTER TABLE "public"."ftipl_dsa_upload_doc_mail_log" RENAME TO "ftipl_dsa_upload_doc_mail_log_old_20251110";
CREATE TABLE "public"."ftipl_dsa_upload_doc_mail_log" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/ftipl_dsa_upload_doc_mail_log/2025/11/*/*.parquet') LIMIT 0;

-- [34/36] ftpl_user (PK: id)
ALTER TABLE "public"."ftpl_user" RENAME TO "ftpl_user_old_20251110";
CREATE TABLE "public"."ftpl_user" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/ftpl_user/2025/11/*/*.parquet') LIMIT 0;

-- [35/36] sessions (PK: sid)
ALTER TABLE "public"."sessions" RENAME TO "sessions_old_20251110";
CREATE TABLE "public"."sessions" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/sessions/2025/11/*/*.parquet') LIMIT 0;

-- [36/36] users (PK: uid)
ALTER TABLE "public"."users" RENAME TO "users_old_20251110";
CREATE TABLE "public"."users" AS SELECT * EXCLUDE ("Op", "load_timestamp") FROM READ_PARQUET(LOCATION => 's3_raw_dms', PATTERN => 'fair/users/2025/11/*/*.parquet') LIMIT 0;

COMMIT;

-- ═══════════════════════════════════════════════════════════════════
-- COPY DATA FROM OLD TABLES (Run after verifying schema is correct)
-- ═══════════════════════════════════════════════════════════════════
-- INSERT INTO "public"."cent_aadhar_address_data" SELECT * FROM "public"."cent_aadhar_address_data_old_20251110";
-- INSERT INTO "public"."cent_bank_details" SELECT * FROM "public"."cent_bank_details_old_20251110";
-- INSERT INTO "public"."cent_ckyc_search" SELECT * FROM "public"."cent_ckyc_search_old_20251110";
-- INSERT INTO "public"."cent_crif_enquiry_logs" SELECT * FROM "public"."cent_crif_enquiry_logs_old_20251110";
-- INSERT INTO "public"."cent_crif_loan_details" SELECT * FROM "public"."cent_crif_loan_details_old_20251110";
-- INSERT INTO "public"."cent_crif_log_data" SELECT * FROM "public"."cent_crif_log_data_old_20251110";
-- INSERT INTO "public"."cent_crif_log_information" SELECT * FROM "public"."cent_crif_log_information_old_20251110";
-- INSERT INTO "public"."cent_crif_variations" SELECT * FROM "public"."cent_crif_variations_old_20251110";
-- INSERT INTO "public"."cent_emi_settlement" SELECT * FROM "public"."cent_emi_settlement_old_20251110";
-- INSERT INTO "public"."cent_employment" SELECT * FROM "public"."cent_employment_old_20251110";
-- INSERT INTO "public"."cent_fd_ploss_proposal" SELECT * FROM "public"."cent_fd_ploss_proposal_old_20251110";
-- INSERT INTO "public"."cent_inv" SELECT * FROM "public"."cent_inv_old_20251110";
-- INSERT INTO "public"."cent_loan" SELECT * FROM "public"."cent_loan_old_20251110";
-- INSERT INTO "public"."cent_loan_auto_execution_status" SELECT * FROM "public"."cent_loan_auto_execution_status_old_20251110";
-- INSERT INTO "public"."cent_loan_detail" SELECT * FROM "public"."cent_loan_detail_old_20251110";
-- INSERT INTO "public"."cent_mail_track" SELECT * FROM "public"."cent_mail_track_old_20251110";
-- INSERT INTO "public"."cent_mobile_sms" SELECT * FROM "public"."cent_mobile_sms_old_20251110";
-- INSERT INTO "public"."cent_monitor_log" SELECT * FROM "public"."cent_monitor_log_old_20251110";
-- INSERT INTO "public"."cent_notification" SELECT * FROM "public"."cent_notification_old_20251110";
-- INSERT INTO "public"."cent_req_encrypt_data" SELECT * FROM "public"."cent_req_encrypt_data_old_20251110";
-- INSERT INTO "public"."cent_request_validation" SELECT * FROM "public"."cent_request_validation_old_20251110";
-- INSERT INTO "public"."cent_sms_service_logs" SELECT * FROM "public"."cent_sms_service_logs_old_20251110";
-- INSERT INTO "public"."cent_thirdparty_invoice" SELECT * FROM "public"."cent_thirdparty_invoice_old_20251110";
-- INSERT INTO "public"."cent_transaction" SELECT * FROM "public"."cent_transaction_old_20251110";
-- INSERT INTO "public"."cent_user" SELECT * FROM "public"."cent_user_old_20251110";
-- INSERT INTO "public"."cent_user_auth" SELECT * FROM "public"."cent_user_auth_old_20251110";
-- INSERT INTO "public"."cent_user_consent" SELECT * FROM "public"."cent_user_consent_old_20251110";
-- INSERT INTO "public"."cent_user_login_log" SELECT * FROM "public"."cent_user_login_log_old_20251110";
-- INSERT INTO "public"."cent_user_verification" SELECT * FROM "public"."cent_user_verification_old_20251110";
-- INSERT INTO "public"."cent_verification_item" SELECT * FROM "public"."cent_verification_item_old_20251110";
-- INSERT INTO "public"."cent_virtual_account" SELECT * FROM "public"."cent_virtual_account_old_20251110";
-- INSERT INTO "public"."cent_wallet_logs_data" SELECT * FROM "public"."cent_wallet_logs_data_old_20251110";
-- INSERT INTO "public"."ftipl_dsa_upload_doc_mail_log" SELECT * FROM "public"."ftipl_dsa_upload_doc_mail_log_old_20251110";
-- INSERT INTO "public"."ftpl_user" SELECT * FROM "public"."ftpl_user_old_20251110";
-- INSERT INTO "public"."sessions" SELECT * FROM "public"."sessions_old_20251110";
-- INSERT INTO "public"."users" SELECT * FROM "public"."users_old_20251110";

