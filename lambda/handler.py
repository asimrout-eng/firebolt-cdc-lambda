"""
Lambda: S3 → Firebolt CDC (Direct COPY + MERGE)
REFACTORED VERSION using Firebolt SDK
"""
import os, re, json, logging, hashlib, time, random
import boto3
from firebolt.db import connect as fb_connect
from firebolt.client.auth import UsernamePassword, ClientCredentials
from typing import Optional, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
            )
            self.cursor = self.connection.cursor()
            logger.info("✓ Successfully connected to Firebolt")
            
        except Exception as e:
            logger.error(f"Failed to connect to Firebolt: {str(e)}")
            raise
    
    def execute(self, sql: str) -> Any:
        """Execute SQL and return results"""
        logger.info("SQL>> %s", sql[:200] + "..." if len(sql) > 200 else sql)
        try:
            self.cursor.execute(sql)
            return self.cursor
        except Exception as e:
            logger.error(f"Query failed: {e}")
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
    """Execute MERGE with retry logic for transaction conflicts
    
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
        transaction_started = False
        try:
            fb_connector.execute("BEGIN;")
            transaction_started = True
            
            merge_sql = render_merge(table, staging_table, cols, keys, delete_expr=delete_expr, key_cols_safe=key_cols_safe)
            fb_connector.execute(merge_sql)
            
            # Get row count if possible
            try:
                rows_affected = fb_connector.cursor.rowcount if hasattr(fb_connector.cursor, 'rowcount') else "unknown"
            except:
                rows_affected = "unknown"
            
            # COMMIT might fail if transaction was auto-rolled back by Firebolt
            try:
                fb_connector.execute("COMMIT;")
                transaction_started = False  # Transaction completed
                logger.info(f"✓ MERGE completed for {table} ({rows_affected} rows affected)")
                return  # Success!
            except Exception as commit_error:
                commit_msg = str(commit_error)
                if "no transaction is in progress" in commit_msg.lower():
                    # Transaction was auto-rolled back by Firebolt (timeout/conflict)
                    transaction_started = False
                    logger.warning(f"⚠️  Transaction was auto-rolled back by Firebolt for {table}: {commit_error}")
                    # Treat as conflict and retry
                    raise Exception(f"Transaction conflict: auto-rolled back by Firebolt")
                else:
                    raise
            
        except Exception as e:
            error_msg = str(e)
            
            # Rollback if transaction is active
            if transaction_started:
                try:
                    fb_connector.execute("ROLLBACK;")
                    logger.info("✓ Transaction rolled back")
                except Exception as rollback_error:
                    logger.warning(f"Failed to rollback transaction: {rollback_error}")
            
            # Check if it's a transaction conflict (Firebolt MVCC conflict)
            if "conflict" in error_msg.lower() or "detected 1 conflicts" in error_msg:
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter to avoid thundering herd
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"⚠️  Transaction conflict on {table}, retry {attempt + 1}/{max_retries} in {wait_time:.2f}s")
                    time.sleep(wait_time)
                    continue  # Retry
                else:
                    logger.error(f"✗ MERGE failed for {table} after {max_retries} retries: {e}")
                    logger.error(f"   Columns used: {cols}")
                    logger.error(f"   Primary keys: {keys}")
                    raise
            else:
                # Not a conflict error, don't retry (e.g., schema errors, syntax errors)
                logger.error(f"✗ MERGE failed for {table}: {e}")
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
        cols = [c for c in cols_production if c in cols_staging]
        
        if not cols:
            raise RuntimeError(f"No common columns between staging and production table '{table}'")
        
        # Verify primary keys are in the common column list
        missing_keys = [k for k in keys if k not in cols]
        if missing_keys:
            raise RuntimeError(
                f"Primary keys {missing_keys} not found in common columns for table '{table}'. "
                f"Production columns: {cols_production}, Staging columns: {cols_staging}"
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
        execute_merge_with_retry(
            fb_connector=fb_connector,
            table=table,
            staging_table=staging_table,
            cols=cols,
            keys=keys,
            delete_expr=delete_expr,
            key_cols_safe=key_cols_safe,
            max_retries=3
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


