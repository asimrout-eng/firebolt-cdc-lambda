"""
Lambda: S3 → Firebolt CDC (Direct COPY + MERGE)
REFACTORED VERSION using Firebolt SDK
"""
import os, re, json, logging, hashlib, time
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

def render_merge(table, staging_table, cols, key_cols, delete_expr=None):
    """Generate MERGE statement"""
    on_clause = " AND ".join([f't."{k}" = s."{k}"' for k in key_cols])
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
    delete_col = os.environ.get("CDC_DELETE_COLUMN")
    delete_vals = os.environ.get("CDC_DELETE_VALUES")
    delete_expr = None
    if delete_col and delete_vals:
        in_list = ", ".join([f"'{v.strip()}'" for v in delete_vals.split(",") if v.strip()])
        if in_list:
            delete_expr = f's."{delete_col}" IN ({in_list})'
            logger.info(f"Delete expression: {delete_expr}")
    
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
        
        # Get columns from staging to verify overlap
        cols_staging = get_columns("public", staging_table, fb_connector)
        
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
        
        logger.info(f"✓ Using {len(cols)} common columns for MERGE (production: {len(cols_production)}, staging: {len(cols_staging)})")
        
        # Execute MERGE in transaction
        transaction_started = False
        try:
            fb_connector.execute("BEGIN;")
            transaction_started = True
            
            merge_sql = render_merge(table, staging_table, cols, keys, delete_expr=delete_expr)
            fb_connector.execute(merge_sql)
            
            # Get row count if possible (optional, some DBs return this)
            try:
                rows_affected = fb_connector.cursor.rowcount if hasattr(fb_connector.cursor, 'rowcount') else "unknown"
            except:
                rows_affected = "unknown"
            
            fb_connector.execute("COMMIT;")
            transaction_started = False  # Transaction completed
            logger.info(f"✓ MERGE completed for {table} ({rows_affected} rows affected)")
            
        except Exception as e:
            # Only rollback if transaction was actually started
            if transaction_started:
                try:
                    fb_connector.execute("ROLLBACK;")
                    logger.info("✓ Transaction rolled back")
                except Exception as rollback_error:
                    logger.warning(f"Failed to rollback transaction: {rollback_error}")
                    # Don't raise - the original error is more important
            
            logger.error(f"✗ MERGE failed for {table}: {e}")
            logger.error(f"   Columns used: {cols}")
            logger.error(f"   Primary keys: {keys}")
            raise
        
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


