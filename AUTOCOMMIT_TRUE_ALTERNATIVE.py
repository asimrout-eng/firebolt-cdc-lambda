"""
ALTERNATIVE APPROACH: Using autocommit=True for simpler code

This removes the transaction wrapper since a single MERGE is already atomic.
However, this needs thorough testing to ensure conflict handling still works.
"""

# In FireboltConnector.__init__():
def __init__(self):
    # ...
    self.connection = fb_connect(
        auth=auth_obj,
        account_name=_clean(os.environ['FIREBOLT_ACCOUNT']),
        engine_name=_clean(os.environ['FIREBOLT_ENGINE']),
        database=_clean(os.environ['FIREBOLT_DATABASE']),
        disable_cache=True,
        autocommit=True  # ← Simpler approach
    )

# In perform_merge_with_retry():
def perform_merge_with_retry(fb_connector, table, staging_table, cols, keys, 
                             delete_expr=None, key_cols_safe=None, max_retries=3):
    """
    Simplified MERGE without explicit transaction wrapper.
    
    Args:
        fb_connector: Firebolt connector instance
        table: Target table name
        staging_table: Staging table name
        cols: List of columns to merge
        keys: Original primary key columns
        delete_expr: Optional delete expression for CDC
        key_cols_safe: Filtered primary keys for ON clause
        max_retries: Maximum number of retry attempts (default 3)
    """
    for attempt in range(max_retries):
        try:
            merge_sql = render_merge(table, staging_table, cols, keys, 
                                    delete_expr=delete_expr, key_cols_safe=key_cols_safe)
            logger.info(f"Generated MERGE SQL (first 500 chars): {merge_sql[:500]}")
            
            # Execute MERGE (auto-commits immediately with autocommit=True)
            fb_connector.execute(merge_sql)
            
            # Get row count if possible
            try:
                rows_affected = fb_connector.cursor.rowcount if hasattr(fb_connector.cursor, 'rowcount') else "unknown"
            except:
                rows_affected = "unknown"
            
            logger.info(f"✓ MERGE completed for {table} ({rows_affected} rows affected)")
            return  # Success!
            
        except Exception as e:
            error_msg = str(e)
            
            # Check if it's a retryable conflict
            if ("conflict" in error_msg.lower() or 
                "detected 1 conflicts" in error_msg or 
                "cannot be retried" in error_msg.lower()):
                
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"⚠️  Transaction conflict on {table}, retry {attempt + 1}/{max_retries} in {wait_time:.2f}s")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"✗ MERGE failed for {table} after {max_retries} retries: {error_msg}")
                    raise Exception(f"MERGE failed after {max_retries} retries: {error_msg}")
            else:
                # Non-retryable error
                logger.error(f"✗ MERGE failed for {table} with non-retryable error: {error_msg}")
                raise

# PROS:
# ✅ Simpler code (no COMMIT/ROLLBACK)
# ✅ No "cannot COMMIT" errors
# ✅ Single MERGE is already atomic
# ✅ ~30 fewer lines of code

# CONS:
# ❌ Need to test conflict detection still works
# ❌ No explicit ROLLBACK (may affect retry behavior)
# ❌ Different from Firebolt's documented transaction best practices

# RECOMMENDATION:
# Test this approach in a dev environment first to ensure:
# 1. Conflict detection still works correctly
# 2. Retry logic behaves as expected
# 3. No data loss or corruption occurs

