"""
Lambda: S3 → Firebolt CDC (Direct COPY + MERGE)
REFACTORED VERSION using Firebolt SDK
"""
import os, re, json, logging, hashlib, time, random
import boto3
from firebolt.db import connect as fb_connect
from firebolt.client.auth import UsernamePassword, ClientCredentials
from typing import Optional, Any
import firebolt

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Log SDK version on Lambda cold start
try:
    SDK_VERSION = getattr(firebolt, '__version__', 'unknown')
    logger.info(f"🔧 Firebolt Python SDK Version: {SDK_VERSION}")
except Exception as e:
    logger.warning(f"Could not determine Firebolt SDK version: {e}")

class FireboltConnector:
    """Handles Firebolt database connections and operations"""
    
    def __init__(self):
        self.connection: Optional[Any] = None
        self.cursor: Optional[Any] = None
        
    def connect(self) -> None:
        """Establish connection to Firebolt"""
        try:
            # Ensure writable cache/config directories in Lambda
            os.environ.setdefault('HOME', '/tmp')
            os.environ.setdefault('XDG_CACHE_HOME', '/tmp')
            os.environ.setdefault('XDG_CONFIG_HOME', '/tmp')
            os.environ.setdefault('FIREBOLT_CACHE_DIR', '/tmp')

            # Clean helper to strip accidental surrounding quotes
            def _clean(v: Any) -> Any:
                if isinstance(v, str) and len(v) >= 2:
                    if (v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'"):
                        return v[1:-1]
                return v

            # Validate required environment variables
            required_vars = ['FIREBOLT_ACCOUNT', 'FIREBOLT_DATABASE', 'FIREBOLT_ENGINE']
            missing_vars = [var for var in required_vars if not os.environ.get(var)]
            
            # Prefer Client Credentials if provided, else Username/Password (REQUIRED)
            client_id = _clean(os.environ.get('FIREBOLT_CLIENT_ID'))
            client_secret = _clean(os.environ.get('FIREBOLT_CLIENT_SECRET'))
            
            if client_id and client_secret:
                auth_obj = ClientCredentials(
                    client_id=client_id, 
                    client_secret=client_secret
                )
                logger.info("Using Client Credentials authentication")
            else:
                # Username/Password authentication (REQUIRED if no client credentials)
                username = _clean(os.environ.get('FIREBOLT_USERNAME'))
                password = _clean(os.environ.get('FIREBOLT_PASSWORD'))
                
                if not username or not password:
                    raise ValueError(
                        "Missing required authentication credentials! "
                        "You must provide either:\n"
                        "  1. FIREBOLT_USERNAME and FIREBOLT_PASSWORD, OR\n"
                        "  2. FIREBOLT_CLIENT_ID and FIREBOLT_CLIENT_SECRET"
                    )
                
                auth_obj = UsernamePassword(
                    username=username,
                    password=password
                )
                logger.info("Using Username/Password authentication")
            
            if missing_vars:
                raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

            self.connection = fb_connect(
                auth=auth_obj,
                account_name=_clean(os.environ['FIREBOLT_ACCOUNT']),
                engine_name=_clean(os.environ['FIREBOLT_ENGINE']),
                database=_clean(os.environ['FIREBOLT_DATABASE']),
                disable_cache=True
                # Using default autocommit=True - each statement is atomic
            )
            self.cursor = self.connection.cursor()
            logger.info("✓ Successfully connected to Firebolt")
            
        except Exception as e:
            logger.error(f"Failed to connect to Firebolt: {str(e)}")
            raise
    
    def execute(self, sql: str) -> Any:
        """Execute SQL and return results (raw SQL, no prepared statements)"""
        logger.info("SQL>> %s", sql[:200] + "..." if len(sql) > 200 else sql)
        try:
            # Force raw SQL execution (no prepared statements)
            # Firebolt doesn't support prepared statements for MERGE
            # Use string directly, no parameters
            self.cursor.execute(str(sql))
            return self.cursor
        except Exception as e:
            logger.error(f"Query failed: {e}")
            logger.error(f"SQL that failed: {sql[:1000]}")
            raise
    
    def disconnect(self) -> None:
        """Close Firebolt connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Disconnected from Firebolt")

def get_columns(schema, table, fb_connector):
    """Get column names for a table"""
    q = (
        "SELECT column_name "
        "FROM information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
        "ORDER BY ordinal_position;"
    )
    cursor = fb_connector.execute(q)
    rows = cursor.fetchall()
    
    if not rows:
        raise ValueError(f"Table {schema}.{table} not found or has no columns")
    
    # Extract column names from tuples
    return [row[0] for row in rows]

def get_column_types(schema, table, fb_connector):
    """Get column names and data types for a table"""
    q = (
        "SELECT column_name, data_type "
        "FROM information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
        "ORDER BY ordinal_position;"
    )
    cursor = fb_connector.execute(q)
    rows = cursor.fetchall()
    
    if not rows:
        raise ValueError(f"Table {schema}.{table} not found or has no columns")
    
    # Return dict {column_name: data_type}
    return {row[0]: row[1] for row in rows}

def ensure_staging_table_name(table, staging_suffix):
    """Generate unique staging table name for this Lambda invocation"""
    staging_table = f"stg_{table}_{staging_suffix}"
    
    # Note: Staging table will be auto-created by COPY command
    # This avoids schema mismatches between parquet and Firebolt table
    # (e.g., DMS writes BLOB as bytea, Firebolt expects text)
    
    logger.info(f"Staging table name: {staging_table}")
    return staging_table

def render_copy_single_file(staging_table, table, date_path, filename, location, database):
    """Generate COPY statement for single file - DMS format"""
    # DMS format: database/table/YYYY/MM/DD/filename
    pattern = f'{database}/{table}/{date_path}/{filename}'
    
    return (
        f'COPY "public"."{staging_table}"\n'
        f'FROM {location}\n'
        'WITH (\n'
        f"  PATTERN = '{pattern}',\n"
        '  TYPE = PARQUET,\n'
        '  AUTO_CREATE = TRUE,\n'  # Let Firebolt infer schema from parquet
        "  MAX_ERRORS_PER_FILE = '0%'\n"
        ');\n'
    )

def render_merge(table, staging_table, cols, key_cols, delete_expr=None, key_cols_safe=None):
    """Generate MERGE statement
    
    Args:
        table: Target table name
        staging_table: Staging table name
        cols: List of columns to merge (intersection of staging and production)
        key_cols: Original primary key columns
        delete_expr: Optional delete expression for CDC
        key_cols_safe: Filtered primary keys (excludes DECIMAL columns) for ON clause
    """
    # Use safe key columns for ON clause (without DECIMALs) if provided
    on_keys = key_cols_safe if key_cols_safe else key_cols
    on_clause = " AND ".join([f't."{k}" = s."{k}"' for k in on_keys])
    
    non_keys = [c for c in cols if c not in key_cols]
    set_clause = ",\n    ".join([f'"{c}" = s."{c}"' for c in non_keys]) if non_keys else None
    cols_csv = ", ".join([f'"{c}"' for c in cols])
    vals_csv = ", ".join([f's."{c}"' for c in cols])

    parts = [
        f'MERGE INTO "public"."{table}" AS t',
        f'USING "public"."{staging_table}" AS s',
        f'ON ({on_clause})'
    ]

    # Handle deletes (tombstones) if configured
    if delete_expr:
        parts.append(f'WHEN MATCHED AND ({delete_expr}) THEN DELETE')

    # Update existing rows
    if set_clause:
        parts.append(f'WHEN MATCHED THEN UPDATE SET\n    {set_clause}')

    # Insert new rows
    parts.append(f'WHEN NOT MATCHED THEN INSERT ({cols_csv}) VALUES ({vals_csv});')
    
    return "\n".join(parts)

def execute_merge_with_retry(fb_connector, table, staging_table, cols, keys, delete_expr=None, key_cols_safe=None, max_retries=3):
    """Execute MERGE with retry logic using Firebolt HTTP status codes
    
    Single MERGE statement is atomic - no explicit transaction wrapper needed.
    With autocommit=True (default), the MERGE auto-commits on success.
    
    Error handling follows Firebolt best practices:
    - 409 Conflict: Transaction conflict → Retry with backoff
    - 5xx Server Error: Transient failure → Retry with backoff
    - 4xx Client Error: Permanent error → Don't retry, fail immediately
    - 2xx Success: Proceed
    
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
            merge_sql = render_merge(table, staging_table, cols, keys, delete_expr=delete_expr, key_cols_safe=key_cols_safe)
            logger.info(f"Generated MERGE SQL (first 500 chars): {merge_sql[:500]}")
            logger.info(f"Staging table: {staging_table}, Production table: {table}")
            
            # Execute MERGE (auto-commits on success with autocommit=True)
            fb_connector.execute(merge_sql)
            
            # Get row count if possible
            try:
                rows_affected = fb_connector.cursor.rowcount if hasattr(fb_connector.cursor, 'rowcount') else "unknown"
            except:
                rows_affected = "unknown"
            
            logger.info(f"✓ MERGE completed for {table} ({rows_affected} rows affected)")
            return  # Success (2xx)!
            
        except Exception as e:
            error_msg = str(e)
            
            # Extract HTTP status code from exception (Firebolt SDK best practice)
            status_code = None
            if hasattr(e, 'status_code'):
                status_code = e.status_code
            elif hasattr(e, 'response') and hasattr(e.response, 'status_code'):
                status_code = e.response.status_code
            
            # Determine if error is retryable based on HTTP status code
            is_retryable = False
            error_category = "Unknown"
            
            if status_code:
                if status_code == 409:
                    # 409 Conflict: Transaction conflict (retryable)
                    is_retryable = True
                    error_category = "Conflict (409)"
                elif 500 <= status_code < 600:
                    # 5xx: Server error (retryable)
                    is_retryable = True
                    error_category = f"Server Error ({status_code})"
                elif 400 <= status_code < 500:
                    # 4xx: Client error (non-retryable, except 409 handled above)
                    is_retryable = False
                    error_category = f"Client Error ({status_code})"
                else:
                    # 2xx or other: Success or unexpected
                    is_retryable = False
                    error_category = f"Unexpected ({status_code})"
            else:
                # Fallback: No status code available, check error message and error code
                # (backwards compatibility for older SDK versions or network errors)
                logger.warning(f"⚠️  No HTTP status code available, falling back to text/code matching")
                
                # Check for Firebolt error code 9 (transaction conflict)
                has_conflict_code = "code: 9" in error_msg or "code:9" in error_msg
                
                # Check for conflict keywords in error message
                has_conflict_text = ("conflict" in error_msg.lower() or 
                                   "detected" in error_msg.lower() and "conflicts" in error_msg.lower() or
                                   "cannot be retried" in error_msg.lower())
                
                if has_conflict_code or has_conflict_text:
                    is_retryable = True
                    if has_conflict_code:
                        error_category = "Conflict (error code 9)"
                    else:
                        error_category = "Conflict (text match)"
                else:
                    is_retryable = False
                    error_category = "Non-retryable (text match)"
            
            # Handle based on retryability
            if is_retryable:
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter to avoid thundering herd
                    # Increased backoff for MVCC conflicts: 3^attempt instead of 2^attempt
                    base_wait = (3 ** attempt) if attempt <= 5 else 243  # Cap at ~4 minutes
                    wait_time = base_wait + random.uniform(0, 2)
                    logger.warning(
                        f"⚠️  {error_category} on {table}, "
                        f"retry {attempt + 1}/{max_retries} in {wait_time:.2f}s: {error_msg}"
                    )
                    time.sleep(wait_time)
                    continue  # Retry
                else:
                    # Max retries reached
                    logger.error(
                        f"✗ MERGE failed for {table} after {max_retries} retries "
                        f"({error_category}): {error_msg}"
                    )
                    logger.error(f"   Columns used: {cols}")
                    logger.error(f"   Primary keys: {keys}")
                    raise
            else:
                # Non-retryable error (permanent client error or syntax issue)
                logger.error(
                    f"✗ MERGE failed for {table} with non-retryable error "
                    f"({error_category}): {error_msg}"
                )
                logger.error(f"   Columns used: {cols}")
                logger.error(f"   Primary keys: {keys}")
                raise

def cleanup_staging_table(staging_table, fb_connector):
    """Drop temporary staging table"""
    try:
        drop_sql = f'DROP TABLE IF EXISTS "public"."{staging_table}"'
        fb_connector.execute(drop_sql)
        logger.info(f"✓ Cleaned up staging table {staging_table}")
    except Exception as e:
        logger.warning(f"Failed to cleanup staging table: {e}")

# Regex to extract database, table, date, filename from S3 key
# DMS format: firebolt_dms_job/<database>/<table>/YYYY/MM/DD/<filename>.parquet
RE_KEY = re.compile(r'firebolt_dms_job/([^/]+)/([^/]+)/(\d{4})/(\d{2})/(\d{2})/([^/]+\.parquet)$')

def handler(event, context):
    """
    Lambda handler for S3 → Firebolt CDC
    
    Triggered by S3 ObjectCreated event via EventBridge
    Processes ONE file at a time
    """
    start_time = time.time()
    
    # Extract S3 key from event
    key = ""
    if "detail" in event and "object" in event["detail"]:
        # EventBridge format
        key = event["detail"]["object"].get("key", "")
    elif "Records" in event:
        # Direct S3 event format
        key = event["Records"][0]["s3"]["object"]["key"]
    
    logger.info(f"Processing S3 key: {key}")
    
    # Parse database, table, date components, filename from key
    m = RE_KEY.search(key or "")
    if not m:
        raise RuntimeError(f"Cannot parse database/table/date/filename from key: {key}")
    
    database, table, year, month, day, filename = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6)
    date_yyyymmdd = f"{year}{month}{day}"  # For internal tracking
    date_path = f"{year}/{month}/{day}"    # For S3 COPY pattern
    logger.info(f"Database: {database}, Table: {table}, Date: {date_yyyymmdd}, File: {filename}")
    
    # Create unique suffix for staging table (prevents concurrency issues)
    unique_suffix = hashlib.md5(
        f"{table}_{date_yyyymmdd}_{filename}_{context.aws_request_id}".encode()
    ).hexdigest()[:8]
    
    # Load table keys configuration
    tk_inline = os.environ.get("TABLE_KEYS_JSON")
    if tk_inline:
        table_keys = json.loads(tk_inline)
    else:
        s3 = boto3.client("s3")
        k_bucket = os.environ["TABLE_KEYS_S3_BUCKET"]
        k_key = os.environ["TABLE_KEYS_S3_KEY"]
        obj = s3.get_object(Bucket=k_bucket, Key=k_key)
        table_keys = json.loads(obj["Body"].read().decode("utf-8"))
    
    # Get primary keys for this table
    keys = table_keys.get(table)
    if not keys:
        raise RuntimeError(f"No keys configured for table {table}")
    
    # Convert single key to list for uniform handling
    if isinstance(keys, str):
        keys = [keys]
    
    logger.info(f"Primary keys for {table}: {keys}")
    
    # Optional: Handle CDC deletes (tombstones)
    # Note: delete_expr will be validated later (after we know staging table columns)
    delete_col = os.environ.get("CDC_DELETE_COLUMN")
    delete_vals = os.environ.get("CDC_DELETE_VALUES")
    
    # Connect to Firebolt using SDK
    fb_connector = FireboltConnector()
    fb_connector.connect()
    
    # Get external location name
    location = os.environ["LOCATION_NAME"]
    
    staging_table = None
    try:
        # Generate unique staging table name (table will be auto-created by COPY)
        staging_table = ensure_staging_table_name(table, unique_suffix)
        
        # Drop staging table if it exists (from previous failed run)
        cleanup_staging_table(staging_table, fb_connector)
        
        # COPY single file to staging (AUTO_CREATE will infer schema from parquet)
        copy_sql = render_copy_single_file(staging_table, table, date_path, filename, location, database)
        fb_connector.execute(copy_sql)
        logger.info(f"✓ Copied {filename} to {staging_table} (auto-created from parquet schema)")
        
        # Get column list from PRODUCTION table (not staging) to ensure schema compatibility
        # We only merge columns that exist in production
        cols_production = get_columns("public", table, fb_connector)
        
        # Get columns and data types from staging table
        cols_staging = get_columns("public", staging_table, fb_connector)
        col_types_staging = get_column_types("public", staging_table, fb_connector)
        col_types_production = get_column_types("public", table, fb_connector)
        
        # Use intersection of columns (only columns present in both tables)
        cols_initial = [c for c in cols_production if c in cols_staging]
        
        if not cols_initial:
            raise RuntimeError(f"No common columns between staging and production table '{table}'")
        
        # Filter out DECIMAL/NUMERIC columns with mismatched precision from ALL columns
        # (Firebolt won't allow assignment between different DECIMAL precisions)
        cols = []
        decimal_cols_removed = []
        for c in cols_initial:
            prod_type = col_types_production.get(c, "")
            stg_type = col_types_staging.get(c, "")
            
            # Check if column is DECIMAL/NUMERIC with different precision
            if ("DECIMAL" in prod_type.upper() or "NUMERIC" in prod_type.upper()):
                if prod_type != stg_type:
                    decimal_cols_removed.append(f"{c} (prod: {prod_type}, stg: {stg_type})")
                    logger.warning(f"⚠️  Skipping DECIMAL column '{c}' from MERGE due to type mismatch: prod={prod_type}, stg={stg_type}")
                    continue
            
            cols.append(c)
        
        if not cols:
            raise RuntimeError(f"No compatible columns after filtering DECIMALs for table '{table}'")
        
        if decimal_cols_removed:
            logger.warning(f"⚠️  {len(decimal_cols_removed)} DECIMAL columns removed from MERGE: {decimal_cols_removed}")
        
        # Verify primary keys are in the filtered column list
        missing_keys = [k for k in keys if k not in cols]
        if missing_keys:
            # Check if missing keys were filtered due to DECIMAL mismatch
            decimal_key_issues = [d for d in decimal_cols_removed if any(k in d for k in missing_keys)]
            if decimal_key_issues:
                error_msg = (
                    f"⚠️  Cannot process table '{table}': Primary key(s) {missing_keys} have DECIMAL precision mismatch. "
                    f"This file will be skipped. Schema fix required:\n"
                    f"  Mismatched keys: {decimal_key_issues}\n"
                    f"  Solution: Run schema fix SQL to recreate table with correct data types."
                )
                logger.error(error_msg)
                raise RuntimeError(error_msg)
            else:
                raise RuntimeError(
                    f"Primary keys {missing_keys} not found in compatible columns for table '{table}'. "
                    f"Production columns: {cols_production}, Staging columns: {cols_staging}, Filtered: {cols}"
                )
        
        # Filter out DECIMAL columns from primary keys for ON clause
        # (Firebolt can't compare DECIMALs with different precision/scale)
        key_cols_safe = []
        decimal_keys_removed = []
        for k in keys:
            prod_type = col_types_production.get(k, "")
            stg_type = col_types_staging.get(k, "")
            
            # Skip DECIMAL columns in ON clause if types differ
            if "DECIMAL" in prod_type.upper() or "NUMERIC" in prod_type.upper():
                if prod_type != stg_type:
                    decimal_keys_removed.append(f"{k} (prod: {prod_type}, stg: {stg_type})")
                    logger.warning(f"Skipping DECIMAL key '{k}' from ON clause due to type mismatch")
                    continue
            
            key_cols_safe.append(k)
        
        # Fallback: if all keys are DECIMALs with different precision, use them anyway (will fail but with clear error)
        if not key_cols_safe:
            logger.warning(f"All primary keys are DECIMAL with different precision! Using original keys (MERGE may fail)")
            key_cols_safe = keys
        
        if decimal_keys_removed:
            logger.warning(f"DECIMAL keys removed from ON clause: {decimal_keys_removed}")
        
        logger.info(f"✓ Using {len(cols)} common columns for MERGE (production: {len(cols_production)}, staging: {len(cols_staging)})")
        logger.info(f"✓ Primary keys for ON clause: {key_cols_safe}")
        
        # Validate CDC delete expression (only if delete column exists in staging)
        delete_expr = None
        if delete_col and delete_vals:
            if delete_col in cols_staging:
                in_list = ", ".join([f"'{v.strip()}'" for v in delete_vals.split(",") if v.strip()])
                if in_list:
                    delete_expr = f's."{delete_col}" IN ({in_list})'
                    logger.info(f"✓ CDC delete expression: {delete_expr}")
            else:
                logger.warning(f"CDC delete column '{delete_col}' not found in staging table, skipping delete handling")
        
        # Execute MERGE with retry logic for transaction conflicts
        # Increased retries to handle high-contention tables
        execute_merge_with_retry(
            fb_connector=fb_connector,
            table=table,
            staging_table=staging_table,
            cols=cols,
            keys=keys,
            delete_expr=delete_expr,
            key_cols_safe=key_cols_safe,
            max_retries=10  # Increased from 3 to handle MVCC conflicts
        )
        
        # Cleanup staging table
        cleanup_staging_table(staging_table, fb_connector)
        
    except Exception as e:
        # Cleanup on error
        if staging_table:
            cleanup_staging_table(staging_table, fb_connector)
        raise
    finally:
        # Always disconnect
        fb_connector.disconnect()
    
    duration = time.time() - start_time
    
    result = {
        "status": "success",
        "database": database,
        "table": table,
        "date": date_yyyymmdd,
        "filename": filename,
        "staging_table": staging_table,
        "duration_seconds": round(duration, 2)
    }
    
    logger.info(f"✓ Processing complete in {duration:.2f}s")
    return result


