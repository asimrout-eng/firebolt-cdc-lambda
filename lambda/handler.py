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

def ensure_staging_like(table, staging_suffix, fb_connector):
    """Create unique staging table for this Lambda invocation"""
    staging_table = f"stg_{table}_{staging_suffix}"
    
    # Create staging table like production
    create_sql = f'''
    CREATE FACT TABLE IF NOT EXISTS "public"."{staging_table}" 
    LIKE "public"."{table}"
    '''
    fb_connector.execute(create_sql)
    
    # Truncate (in case it already exists from previous run)
    truncate_sql = f'TRUNCATE TABLE "public"."{staging_table}"'
    fb_connector.execute(truncate_sql)
    
    logger.info(f"✓ Staging table {staging_table} ready")
    return staging_table

def render_copy_single_file(staging_table, table, date_yyyymmdd, filename, location, database):
    """Generate COPY statement for single file - DMS format"""
    # DMS format: database/table/YYYYMMDD/filename
    pattern = f'{database}/{table}/{date_yyyymmdd}/{filename}'
    
    return (
        f'COPY "public"."{staging_table}"\n'
        f'FROM {location}\n'
        'WITH (\n'
        f"  PATTERN = '{pattern}',\n"
        '  TYPE = PARQUET,\n'
        '  AUTO_CREATE = FALSE,\n'
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
# DMS format: firebolt_dms_job/<database>/<table>/YYYYMMDD/<filename>.parquet
RE_KEY = re.compile(r'firebolt_dms_job/([^/]+)/([^/]+)/(\d{8})/([^/]+\.parquet)$')

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
    
    # Parse database, table, date, filename from key
    m = RE_KEY.search(key or "")
    if not m:
        raise RuntimeError(f"Cannot parse database/table/date/filename from key: {key}")
    
    database, table, date_yyyymmdd, filename = m.group(1), m.group(2), m.group(3), m.group(4)
    logger.info(f"Database: {database}, Table: {table}, Date: {date_yyyymmdd}, File: {filename}")
    
    # Create unique suffix for staging table (prevents concurrency issues)
    unique_suffix = hashlib.md5(
        f"{table}_{date_yyyymmdd}_{filename}_{context.request_id}".encode()
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
        # Create unique staging table
        staging_table = ensure_staging_like(table, unique_suffix, fb_connector)
        
        # COPY single file to staging
        copy_sql = render_copy_single_file(staging_table, table, date_yyyymmdd, filename, location, database)
        fb_connector.execute(copy_sql)
        logger.info(f"✓ Copied {filename} to {staging_table}")
        
        # Get column list from production table
        cols = get_columns("public", table, fb_connector)
        logger.info(f"✓ Retrieved {len(cols)} columns for {table}")
        
        # Execute MERGE in transaction
        fb_connector.execute("BEGIN;")
        try:
            merge_sql = render_merge(table, staging_table, cols, keys, delete_expr=delete_expr)
            fb_connector.execute(merge_sql)
            fb_connector.execute("COMMIT;")
            logger.info(f"✓ MERGE completed for {table}")
        except Exception as e:
            fb_connector.execute("ROLLBACK;")
            logger.error(f"✗ MERGE failed, rolled back: {e}")
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


