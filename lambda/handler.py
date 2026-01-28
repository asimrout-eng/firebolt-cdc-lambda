"""
Lambda: S3 → Firebolt CDC (Direct COPY + MERGE)
REFACTORED VERSION using Firebolt SDK

VERSION: 2.0.0 - Fixed Deduplication with ingestion_seq
CHANGES:
- Re-enabled deduplication with deterministic ordering
- Added ingestion_seq to capture Parquet row order
- Fixed edge case where identical timestamps caused wrong row selection
- Removed DELETE before MERGE (MERGE handles all operations)
"""
import os, re, json, logging, hashlib, time, random
import boto3
from firebolt.db import connect as fb_connect
from firebolt.client.auth import UsernamePassword, ClientCredentials
from typing import Optional, Any, List
import firebolt

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA EVOLUTION - Auto-add new columns from Parquet files
# ═══════════════════════════════════════════════════════════════════════════════

# MySQL/DMS to Firebolt type mapping
MYSQL_TO_FIREBOLT_TYPE_MAP = {
    # String types
    'STRING': 'TEXT',
    'VARCHAR': 'TEXT',
    'CHAR': 'TEXT',
    'TINYTEXT': 'TEXT',
    'MEDIUMTEXT': 'TEXT',
    'LONGTEXT': 'TEXT',
    'TEXT': 'TEXT',
    'ENUM': 'TEXT',
    'SET': 'TEXT',
    
    # Integer types
    'TINYINT': 'INTEGER',
    'SMALLINT': 'INTEGER',
    'MEDIUMINT': 'INTEGER',
    'INT': 'INTEGER',
    'INTEGER': 'INTEGER',
    'BIGINT': 'BIGINT',
    'INT64': 'BIGINT',
    'INT32': 'INTEGER',
    'INT16': 'INTEGER',
    'INT8': 'INTEGER',
    
    # Boolean
    'BOOLEAN': 'BOOLEAN',
    'BOOL': 'BOOLEAN',
    'BIT': 'BOOLEAN',
    
    # Decimal/Numeric - Use (38, 10) as safe default
    'DECIMAL': 'NUMERIC(38, 10)',
    'NUMERIC': 'NUMERIC(38, 10)',
    'NUMBER': 'NUMERIC(38, 10)',
    'FLOAT': 'DOUBLE',
    'DOUBLE': 'DOUBLE',
    'REAL': 'DOUBLE',
    'FLOAT64': 'DOUBLE',
    'FLOAT32': 'REAL',
    
    # Date/Time types
    'DATE': 'DATE',
    'DATETIME': 'TIMESTAMP',
    'TIMESTAMP': 'TIMESTAMPTZ',
    'TIMESTAMPTZ': 'TIMESTAMPTZ',
    'TIME': 'TEXT',  # Firebolt doesn't have TIME type
    'YEAR': 'INTEGER',
    
    # Binary types - convert to TEXT
    'BLOB': 'TEXT',
    'TINYBLOB': 'TEXT',
    'MEDIUMBLOB': 'TEXT',
    'LONGBLOB': 'TEXT',
    'BINARY': 'TEXT',
    'VARBINARY': 'TEXT',
    'BYTEA': 'TEXT',
    
    # JSON
    'JSON': 'TEXT',
    'JSONB': 'TEXT',
    
    # UUID
    'UUID': 'TEXT',
    
    # Spatial (cannot auto-convert)
    'GEOMETRY': None,
    'POINT': None,
    'LINESTRING': None,
    'POLYGON': None,
    'GEOGRAPHY': None,
}

# Types safe to auto-add
SAFE_AUTO_ADD_TYPES = {
    'TEXT', 'VARCHAR', 'STRING', 'CHAR',
    'INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT',
    'BOOLEAN', 'BOOL',
    'DATE', 'TIMESTAMP', 'TIMESTAMPTZ',
    'DOUBLE', 'FLOAT', 'REAL',
    'NUMERIC', 'DECIMAL', 'NUMBER'
}

# Types requiring manual intervention
MANUAL_INTERVENTION_TYPES = {'ARRAY', 'STRUCT', 'MAP', 'GEOMETRY', 'POINT', 'POLYGON'}


# ═══════════════════════════════════════════════════════════════════════════════
# FIREBOLT CONNECTOR CLASS
# ═══════════════════════════════════════════════════════════════════════════════

class FireboltConnector:
    """Handles Firebolt database connections"""
    
    def __init__(self, database: str, engine: str):
        self.database = database
        self.engine = engine
        self.connection = None
        self.cursor = None
        self._connect()
    
    def _connect(self):
        """Establish connection to Firebolt"""
        # Get credentials from environment
        client_id = os.environ.get('FIREBOLT_CLIENT_ID')
        client_secret = os.environ.get('FIREBOLT_CLIENT_SECRET')
        account_name = os.environ.get('FIREBOLT_ACCOUNT', 'faircentindia')
        
        if not client_id or not client_secret:
            raise ValueError("Missing FIREBOLT_CLIENT_ID or FIREBOLT_CLIENT_SECRET environment variables")
        
        auth = ClientCredentials(client_id, client_secret)
        
        self.connection = fb_connect(
            auth=auth,
            account_name=account_name,
            engine_name=self.engine,
            database=self.database,
            disable_cache=True
        )
        self.cursor = self.connection.cursor()
        logger.info(f"Connected to Firebolt: {self.database}/{self.engine}")
    
    def execute(self, sql: str):
        """Execute SQL statement"""
        try:
            self.cursor.execute(sql)
            return self.cursor
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            logger.error(f"SQL: {sql[:500]}...")
            raise
    
    def disconnect(self):
        """Close connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Disconnected from Firebolt")


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def get_columns(schema: str, table: str, fb_connector) -> list:
    """Get column names for a table"""
    q = f"""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = '{schema}' AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    cursor = fb_connector.execute(q)
    return [row[0] for row in cursor.fetchall()]


def get_column_types(schema: str, table: str, fb_connector) -> dict:
    """Get column names and data types for a table"""
    q = f"""
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_schema = '{schema}' AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    cursor = fb_connector.execute(q)
    return {row[0]: row[1] for row in cursor.fetchall()}


def get_column_details_for_evolution(schema: str, table: str, fb_connector) -> dict:
    """Get column names and data types for schema evolution"""
    return get_column_types(schema, table, fb_connector)


def normalize_type(data_type: str) -> str:
    """Extract base type without precision/scale"""
    if not data_type:
        return 'UNKNOWN'
    return data_type.upper().split('(')[0].strip()


def convert_to_firebolt_type(source_type: str) -> tuple:
    """
    Convert source type to Firebolt type.
    Returns: (firebolt_type, is_safe, message)
    """
    if not source_type:
        return None, False, "Empty source type"
    
    base_type = normalize_type(source_type)
    
    # Already valid Firebolt type?
    if base_type in SAFE_AUTO_ADD_TYPES:
        return source_type, True, "Already valid"
    
    # Lookup in mapping
    if base_type in MYSQL_TO_FIREBOLT_TYPE_MAP:
        firebolt_type = MYSQL_TO_FIREBOLT_TYPE_MAP[base_type]
        if firebolt_type is None:
            return None, False, f"Type {base_type} requires manual conversion"
        
        # Preserve precision for DECIMAL/NUMERIC
        if ('DECIMAL' in source_type.upper() or 'NUMERIC' in source_type.upper()) and '(' in source_type:
            precision = source_type[source_type.index('('):source_type.index(')')+1]
            firebolt_type = f'NUMERIC{precision}'
        
        return firebolt_type, True, f"Converted from {base_type}"
    
    # Manual intervention required?
    if base_type in MANUAL_INTERVENTION_TYPES:
        return None, False, f"Type {base_type} cannot be auto-converted"
    
    # Unknown - try TEXT as fallback
    logger.warning(f"Unknown type '{source_type}', using TEXT fallback")
    return 'TEXT', False, f"Unknown type {source_type} - using TEXT"


# ═══════════════════════════════════════════════════════════════════════════════
# SCHEMA EVOLUTION
# ═══════════════════════════════════════════════════════════════════════════════

def handle_schema_evolution(
    staging_table: str,
    production_table: str,
    fb_connector,
    auto_add: bool = True,
    sns_topic_arn: str = None
) -> dict:
    """
    Detect and handle schema evolution between staging and production tables.
    
    Returns dict with results.
    """
    result = {
        'new_columns': [],
        'columns_added': [],
        'columns_skipped': [],
        'requires_manual': []
    }
    
    # Get column details
    staging_cols = get_column_details_for_evolution("public", staging_table, fb_connector)
    prod_cols = get_column_details_for_evolution("public", production_table, fb_connector)
    
    # Exclude CDC metadata
    exclude_cols = {'Op', 'load_timestamp', 'rn', 'ingestion_seq'}
    
    # Find new columns
    for col_name, staging_type in staging_cols.items():
        if col_name in exclude_cols:
            continue
        
        if col_name not in prod_cols:
            result['new_columns'].append((col_name, staging_type))
            
            # Convert type
            firebolt_type, is_safe, message = convert_to_firebolt_type(staging_type)
            
            if firebolt_type and is_safe and auto_add:
                # Auto-add column
                try:
                    alter_sql = f'ALTER TABLE "public"."{production_table}" ADD COLUMN "{col_name}" {firebolt_type} NULL'
                    fb_connector.execute(alter_sql)
                    logger.info(f"Schema evolution: Added column '{col_name}' ({firebolt_type}) to {production_table}")
                    result['columns_added'].append(col_name)
                except Exception as e:
                    logger.warning(f"Failed to add column '{col_name}': {e}")
                    result['columns_skipped'].append((col_name, str(e)))
                    result['requires_manual'].append({
                        'column': col_name,
                        'source_type': staging_type,
                        'target_type': firebolt_type,
                        'reason': str(e)
                    })
            else:
                result['columns_skipped'].append((col_name, message))
                result['requires_manual'].append({
                    'column': col_name,
                    'source_type': staging_type,
                    'target_type': firebolt_type,
                    'reason': message
                })
                logger.warning(f"Manual action needed for column '{col_name}' ({staging_type}): {message}")
    
    # Send SNS notification for manual actions
    if result['requires_manual'] and sns_topic_arn:
        try:
            sns = boto3.client('sns')
            
            message_lines = [
                f"SCHEMA EVOLUTION - Manual Action Required",
                f"",
                f"Table: {production_table}",
                f"Columns: {len(result['requires_manual'])}",
                f""
            ]
            for item in result['requires_manual']:
                message_lines.append(f"  - {item['column']}: {item['source_type']} -> {item.get('target_type', 'N/A')}")
                message_lines.append(f"    Reason: {item['reason']}")
            
            sns.publish(
                TopicArn=sns_topic_arn,
                Subject=f"[Firebolt CDC] Schema Evolution - {production_table}",
                Message="\n".join(message_lines)
            )
            logger.info("Sent SNS notification for schema evolution")
        except Exception as e:
            logger.warning(f"Failed to send SNS notification: {e}")
    
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# DEDUPLICATION LOGIC (v2.0 - with ingestion_seq)
# ═══════════════════════════════════════════════════════════════════════════════

def build_dedup_order_by(cols_staging: List[str], keys: List[str]) -> str:
    """
    Build deterministic ORDER BY clause for deduplication.
    Works generically across all tables.
    
    CASCADING PRIORITY (each level only checked if previous levels are TIED):
    
    1. load_timestamp DESC (DMS S3 write timestamp)
       - If load_timestamps differ -> winner determined, STOP
       - If load_timestamps same -> continue to next level
       
    2. Op priority (D > U > I) - only if load_timestamp tied
       - Delete operations win (final state)
       - If Op differs -> winner determined, STOP
       - If Op same -> continue to next level
       
    3. updated DESC (MySQL timestamp) - only if above tied
       - If updated differs -> winner determined, STOP
       - If updated same or NULL -> continue to next level
       
    4. created DESC (MySQL timestamp) - only if above tied
       - If created differs -> winner determined, STOP
       - If created same or NULL -> continue to next level
       
    5. ingestion_seq DESC (Parquet file row order) - FINAL tie-breaker
       - Captures the natural row order from the Parquet file
       - Later rows in file = later changes in MySQL binlog
       - CRITICAL for edge cases with identical timestamps
    """
    order_parts = []
    
    # Level 1: load_timestamp (primary - checked first)
    if 'load_timestamp' in cols_staging:
        order_parts.append('load_timestamp DESC')
    
    # Level 2: Op column priority (only if load_timestamp tied)
    if 'Op' in cols_staging:
        order_parts.append("""CASE "Op" 
            WHEN 'D' THEN 3 
            WHEN 'U' THEN 2 
            WHEN 'I' THEN 1 
            ELSE 0 
        END DESC""")
    
    # Level 3: MySQL updated timestamp (only if above tied)
    # Handle both TIMESTAMP and BIGINT (epoch) types with COALESCE for NULLs
    if 'updated' in cols_staging:
        order_parts.append('COALESCE("updated", 0) DESC')
    
    # Level 4: MySQL created timestamp (only if above tied)
    if 'created' in cols_staging:
        order_parts.append('COALESCE("created", 0) DESC')
    
    # Level 5: Ingestion sequence number (captures Parquet file row order)
    # This is CRITICAL for edge cases where timestamps are identical
    # The LAST row in the Parquet file should win (higher ingestion_seq = later change)
    if 'ingestion_seq' in cols_staging:
        order_parts.append('"ingestion_seq" DESC')
    
    return ",\n            ".join(order_parts)


def deduplicate_staging_table(
    staging_table: str, 
    keys: List[str], 
    cols_staging: List[str], 
    fb_connector
) -> str:
    """
    Deduplicate staging table before MERGE.
    Keeps only one row per primary key based on deterministic ordering.
    
    Returns:
        str: Name of deduplicated table (either original or new dedup table)
    """
    key_cols_csv = ", ".join([f'"{k}"' for k in keys])
    
    # Check if deduplication is needed
    check_sql = f"""
    SELECT COUNT(*) as total_rows,
           COUNT(DISTINCT ({key_cols_csv})) as unique_keys
    FROM "public"."{staging_table}"
    """
    
    cursor = fb_connector.execute(check_sql)
    row = cursor.fetchone()
    total_rows = row[0]
    unique_keys = row[1]
    
    if total_rows == unique_keys:
        logger.info(f"No duplicates in staging ({total_rows} rows, {unique_keys} unique keys)")
        return staging_table  # No dedup needed
    
    duplicates = total_rows - unique_keys
    logger.info(f"Found {duplicates} duplicates ({total_rows} rows, {unique_keys} unique keys). Deduplicating...")
    
    # Build ORDER BY clause
    order_by = build_dedup_order_by(cols_staging, keys)
    
    # Create deduplicated table
    dedup_table = f"{staging_table}_dedup"
    partition_by = ", ".join([f'"{k}"' for k in keys])
    
    # Exclude ingestion_seq from output columns (it's just for ordering)
    output_cols = [c for c in cols_staging if c != 'ingestion_seq']
    all_cols = ", ".join([f'"{c}"' for c in output_cols])
    select_cols = ", ".join([f'"{c}"' for c in cols_staging])
    
    # Use subquery approach with ROW_NUMBER
    dedup_sql = f"""
    CREATE TABLE "public"."{dedup_table}" AS
    SELECT {all_cols}
    FROM (
        SELECT {select_cols},
               ROW_NUMBER() OVER (
                   PARTITION BY {partition_by}
                   ORDER BY 
                       {order_by}
               ) AS rn
        FROM "public"."{staging_table}"
    ) t
    WHERE rn = 1
    """
    
    logger.info(f"Creating deduplicated staging table: {dedup_table}")
    fb_connector.execute(dedup_sql)
    
    # Verify dedup worked
    verify_sql = f'SELECT COUNT(*) FROM "public"."{dedup_table}"'
    cursor = fb_connector.execute(verify_sql)
    dedup_count = cursor.fetchone()[0]
    
    logger.info(f"Deduplicated: {total_rows} -> {dedup_count} rows")
    
    return dedup_table


# ═══════════════════════════════════════════════════════════════════════════════
# STAGING TABLE CREATION (with ingestion_seq)
# ═══════════════════════════════════════════════════════════════════════════════

def create_staging_table_with_ingestion_seq(
    fb_connector,
    staging_table: str,
    location: str,
    pattern: str
) -> list:
    """
    Create staging table from Parquet with ingestion_seq column.
    
    The ingestion_seq captures the row order from the Parquet file,
    which preserves the MySQL binlog order for rows with identical timestamps.
    
    Returns:
        list: Column names in staging table
    """
    # First, create a temp table without ingestion_seq
    temp_table = f"{staging_table}_temp"
    
    create_temp_sql = f"""
    CREATE TABLE "public"."{temp_table}" AS
    SELECT * FROM READ_PARQUET(
        LOCATION => '{location}',
        PATTERN => '{pattern}'
    )
    """
    
    logger.info(f"Loading Parquet to temp table: {temp_table}")
    fb_connector.execute(create_temp_sql)
    
    # Get columns from temp table
    temp_cols = get_columns("public", temp_table, fb_connector)
    
    # Create staging table with ingestion_seq
    cols_csv = ", ".join([f'"{c}"' for c in temp_cols])
    
    create_staging_sql = f"""
    CREATE TABLE "public"."{staging_table}" AS
    SELECT 
        {cols_csv},
        ROW_NUMBER() OVER () AS ingestion_seq
    FROM "public"."{temp_table}"
    """
    
    logger.info(f"Creating staging table with ingestion_seq: {staging_table}")
    fb_connector.execute(create_staging_sql)
    
    # Drop temp table
    drop_temp_sql = f'DROP TABLE IF EXISTS "public"."{temp_table}"'
    fb_connector.execute(drop_temp_sql)
    
    # Return staging columns (including ingestion_seq)
    staging_cols = temp_cols + ['ingestion_seq']
    
    # Get row count
    count_sql = f'SELECT COUNT(*) FROM "public"."{staging_table}"'
    cursor = fb_connector.execute(count_sql)
    row_count = cursor.fetchone()[0]
    
    logger.info(f"Loaded {row_count:,} rows to staging table with ingestion_seq")
    
    return staging_cols


def cleanup_staging_table(staging_table: str, fb_connector):
    """Drop staging table"""
    try:
        drop_sql = f'DROP TABLE IF EXISTS "public"."{staging_table}"'
        fb_connector.execute(drop_sql)
        logger.info(f"Dropped staging table: {staging_table}")
    except Exception as e:
        logger.warning(f"Failed to drop staging table {staging_table}: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# MERGE EXECUTION
# ═══════════════════════════════════════════════════════════════════════════════

def render_merge(
    target_table: str,
    staging_table: str,
    cols: List[str],
    keys: List[str],
    delete_expr: str = None
) -> str:
    """
    Generate MERGE statement.
    
    Uses MERGE to:
    - INSERT new rows (not in target)
    - UPDATE existing rows (matching keys)
    - DELETE rows (when Op='D')
    """
    # ON clause
    on_clause = " AND ".join([f't."{k}" = s."{k}"' for k in keys])
    
    # UPDATE SET clause (exclude keys)
    update_cols = [c for c in cols if c not in keys and c not in ('Op', 'load_timestamp', 'ingestion_seq')]
    update_set = ", ".join([f'"{c}" = s."{c}"' for c in update_cols])
    
    # INSERT columns and values (exclude CDC metadata)
    insert_cols = [c for c in cols if c not in ('Op', 'load_timestamp', 'ingestion_seq')]
    insert_cols_csv = ", ".join([f'"{c}"' for c in insert_cols])
    insert_vals_csv = ", ".join([f's."{c}"' for c in insert_cols])
    
    # Build MERGE SQL
    merge_sql = f"""
    MERGE INTO "public"."{target_table}" AS t
    USING "public"."{staging_table}" AS s
    ON {on_clause}
    """
    
    # Add DELETE when matched (for Op='D')
    if delete_expr:
        merge_sql += f"""
    WHEN MATCHED AND {delete_expr} THEN DELETE
    """
    
    # Add UPDATE when matched (for non-delete operations)
    if update_set:
        merge_sql += f"""
    WHEN MATCHED THEN UPDATE SET {update_set}
    """
    
    # Add INSERT when not matched
    merge_sql += f"""
    WHEN NOT MATCHED THEN INSERT ({insert_cols_csv}) VALUES ({insert_vals_csv})
    """
    
    return merge_sql


def execute_merge_with_retry(
    fb_connector,
    table: str,
    staging_table: str,
    cols: List[str],
    keys: List[str],
    delete_expr: str = None,
    key_cols_safe: List[str] = None,
    max_retries: int = 10
):
    """Execute MERGE with retry logic for MVCC conflicts"""
    
    if key_cols_safe is None:
        key_cols_safe = keys
    
    merge_sql = render_merge(
        target_table=table,
        staging_table=staging_table,
        cols=cols,
        keys=key_cols_safe,
        delete_expr=delete_expr
    )
    
    for attempt in range(max_retries):
        try:
            fb_connector.execute(merge_sql)
            logger.info(f"MERGE completed successfully")
            return
        except Exception as e:
            error_str = str(e).lower()
            
            # Check if MVCC conflict
            if 'mvcc' in error_str or 'concurrent' in error_str or 'conflict' in error_str:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"MVCC conflict on attempt {attempt + 1}/{max_retries}, retrying in {wait_time:.2f}s...")
                time.sleep(wait_time)
            else:
                # Non-MVCC error, raise immediately
                raise
    
    raise RuntimeError(f"MERGE failed after {max_retries} retries due to MVCC conflicts")


# ═══════════════════════════════════════════════════════════════════════════════
# FILE TRACKING
# ═══════════════════════════════════════════════════════════════════════════════

def is_file_processed(file_key: str, fb_connector) -> bool:
    """Check if file has been processed"""
    check_sql = f"""
    SELECT COUNT(*) FROM cdc_processed_files 
    WHERE file_key = '{file_key}' AND status IN ('completed', 'batch_processed')
    """
    try:
        cursor = fb_connector.execute(check_sql)
        count = cursor.fetchone()[0]
        return count > 0
    except Exception as e:
        logger.warning(f"Failed to check file status: {e}")
        return False


def mark_file_completed(file_key: str, fb_connector):
    """Mark file as completed"""
    insert_sql = f"""
    INSERT INTO cdc_processed_files (file_key, status, processed_at)
    VALUES ('{file_key}', 'completed', CURRENT_TIMESTAMP)
    """
    try:
        fb_connector.execute(insert_sql)
        logger.info(f"Marked file as completed: {file_key}")
    except Exception as e:
        logger.warning(f"Failed to mark file as completed: {e}")


def mark_file_failed(file_key: str, error: str, fb_connector):
    """Mark file as failed"""
    error_escaped = error.replace("'", "''")[:500]
    insert_sql = f"""
    INSERT INTO cdc_processed_files (file_key, status, error_message, processed_at)
    VALUES ('{file_key}', 'failed', '{error_escaped}', CURRENT_TIMESTAMP)
    """
    try:
        fb_connector.execute(insert_sql)
        logger.info(f"Marked file as failed: {file_key}")
    except Exception as e:
        logger.warning(f"Failed to mark file as failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# TABLE KEYS CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

def get_table_keys(table_name: str) -> list:
    """Get primary keys for a table from configuration"""
    # Load from environment or default config
    table_keys_json = os.environ.get('TABLE_KEYS', '{}')
    try:
        table_keys = json.loads(table_keys_json)
    except:
        table_keys = {}
    
    # Default: 'id' for most tables
    key = table_keys.get(table_name, 'id')
    
    if key is None:
        return None
    
    # Handle composite keys (comma-separated)
    if isinstance(key, str):
        return [k.strip() for k in key.split(',')]
    
    return key


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN LAMBDA HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    """
    Lambda handler for S3 -> Firebolt CDC
    
    Triggered by S3 events when new Parquet files are uploaded.
    """
    start_time = time.time()
    
    # Get configuration from environment
    database = os.environ.get('FIREBOLT_DATABASE', 'fair')
    engine = os.environ.get('FIREBOLT_ENGINE', 'dm_engine')
    location = os.environ.get('FIREBOLT_LOCATION', 's3_raw_dms')
    delete_col = os.environ.get('CDC_DELETE_COLUMN', 'Op')
    delete_vals = os.environ.get('CDC_DELETE_VALUES', 'D')
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    
    # Parse S3 event
    try:
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        file_key = record['s3']['object']['key']
    except (KeyError, IndexError) as e:
        logger.error(f"Invalid S3 event: {e}")
        raise ValueError(f"Invalid S3 event structure: {e}")
    
    logger.info("=" * 80)
    logger.info(f"CDC Processing: {file_key}")
    logger.info("=" * 80)
    
    # Parse file path: fair/{table}/{year}/{month}/{day}/{filename}.parquet
    path_pattern = r'^fair/([^/]+)/(\d{4})/(\d{2})/(\d{2})/(.+\.parquet)$'
    match = re.match(path_pattern, file_key)
    
    if not match:
        logger.warning(f"Skipping non-CDC file: {file_key}")
        return {"status": "skipped", "reason": "Not a CDC file"}
    
    table = match.group(1)
    year = match.group(2)
    month = match.group(3)
    day = match.group(4)
    filename = match.group(5)
    date_yyyymmdd = f"{year}-{month}-{day}"
    
    # Skip LOAD files (full load)
    if filename.startswith('LOAD'):
        logger.info(f"Skipping LOAD file: {file_key}")
        return {"status": "skipped", "reason": "LOAD file"}
    
    # Get primary keys
    keys = get_table_keys(table)
    if not keys:
        logger.warning(f"No primary keys configured for table '{table}', skipping")
        return {"status": "skipped", "reason": "No primary keys configured"}
    
    logger.info(f"Table: {table}, Keys: {keys}, Date: {date_yyyymmdd}")
    
    # Connect to Firebolt
    fb_connector = None
    staging_table = None
    dedup_table = None
    
    try:
        fb_connector = FireboltConnector(database=database, engine=engine)
        
        # Check if file already processed
        if is_file_processed(file_key, fb_connector):
            logger.info(f"File already processed: {file_key}")
            return {"status": "skipped", "reason": "Already processed"}
        
        # Generate staging table name
        staging_table = f"stg_{table}_{int(time.time() * 1000)}"
        
        # Pattern for READ_PARQUET
        pattern = file_key
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 1: LOAD TO STAGING (with ingestion_seq)
        # ═══════════════════════════════════════════════════════════════════
        logger.info("=" * 80)
        logger.info("STEP 1: LOAD TO STAGING (with ingestion_seq)")
        logger.info("=" * 80)
        
        cols_staging = create_staging_table_with_ingestion_seq(
            fb_connector=fb_connector,
            staging_table=staging_table,
            location=location,
            pattern=pattern
        )
        
        # Get column types
        col_types_staging = get_column_types("public", staging_table, fb_connector)
        cols_production = get_columns("public", table, fb_connector)
        col_types_production = get_column_types("public", table, fb_connector)
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 2: SCHEMA EVOLUTION (check for new columns)
        # ═══════════════════════════════════════════════════════════════════
        logger.info("=" * 80)
        logger.info("STEP 2: SCHEMA EVOLUTION")
        logger.info("=" * 80)
        
        evolution_result = handle_schema_evolution(
            staging_table=staging_table,
            production_table=table,
            fb_connector=fb_connector,
            auto_add=True,
            sns_topic_arn=sns_topic_arn
        )
        
        if evolution_result['columns_added']:
            logger.info(f"Added {len(evolution_result['columns_added'])} new columns")
            # Refresh production columns
            cols_production = get_columns("public", table, fb_connector)
            col_types_production = get_column_types("public", table, fb_connector)
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 3: DEDUPLICATE STAGING (NEW - with ingestion_seq)
        # ═══════════════════════════════════════════════════════════════════
        logger.info("=" * 80)
        logger.info("STEP 3: DEDUPLICATE STAGING (deterministic ordering)")
        logger.info("=" * 80)
        
        # Deduplicate staging table
        dedup_table = deduplicate_staging_table(
            staging_table=staging_table,
            keys=keys,
            cols_staging=cols_staging,
            fb_connector=fb_connector
        )
        
        # Track if we created a separate dedup table
        dedup_table_created = (dedup_table != staging_table)
        
        # Get columns from dedup table (excludes ingestion_seq)
        cols_dedup = get_columns("public", dedup_table, fb_connector)
        col_types_dedup = get_column_types("public", dedup_table, fb_connector)
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 4: PREPARE MERGE COLUMNS
        # ═══════════════════════════════════════════════════════════════════
        
        # Use intersection of columns (only columns present in both tables)
        cols_initial = [c for c in cols_production if c in cols_dedup]
        
        if not cols_initial:
            raise RuntimeError(f"No common columns between staging and production table '{table}'")
        
        # Filter out DECIMAL/NUMERIC columns with mismatched precision
        cols = []
        decimal_cols_removed = []
        for c in cols_initial:
            prod_type = col_types_production.get(c, "")
            dedup_type = col_types_dedup.get(c, "")
            
            if ("DECIMAL" in prod_type.upper() or "NUMERIC" in prod_type.upper()):
                if prod_type != dedup_type:
                    decimal_cols_removed.append(f"{c} (prod: {prod_type}, stg: {dedup_type})")
                    logger.warning(f"Skipping DECIMAL column '{c}' from MERGE due to type mismatch")
                    continue
            
            cols.append(c)
        
        if not cols:
            raise RuntimeError(f"No compatible columns after filtering DECIMALs for table '{table}'")
        
        # Verify primary keys are in the filtered column list
        missing_keys = [k for k in keys if k not in cols]
        if missing_keys:
            decimal_key_issues = [d for d in decimal_cols_removed if any(k in d for k in missing_keys)]
            if decimal_key_issues:
                error_msg = (
                    f"Cannot process table '{table}': Primary key(s) {missing_keys} have DECIMAL precision mismatch. "
                    f"Mismatched keys: {decimal_key_issues}"
                )
                logger.error(error_msg)
                raise RuntimeError(error_msg)
            else:
                raise RuntimeError(
                    f"Primary keys {missing_keys} not found in compatible columns for table '{table}'"
                )
        
        logger.info(f"Using {len(cols)} common columns for MERGE")
        logger.info(f"Primary keys: {keys}")
        
        # Build delete expression for Op='D'
        delete_expr = None
        if delete_col and delete_vals:
            if delete_col in cols_dedup:
                in_list = ", ".join([f"'{v.strip()}'" for v in delete_vals.split(",") if v.strip()])
                if in_list:
                    delete_expr = f's."{delete_col}" IN ({in_list})'
                    logger.info(f"CDC delete expression: {delete_expr}")
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 5: EXECUTE MERGE
        # ═══════════════════════════════════════════════════════════════════
        logger.info("=" * 80)
        logger.info("STEP 5: EXECUTE MERGE")
        logger.info("=" * 80)
        
        execute_merge_with_retry(
            fb_connector=fb_connector,
            table=table,
            staging_table=dedup_table,  # Use deduplicated table
            cols=cols,
            keys=keys,
            delete_expr=delete_expr,
            key_cols_safe=keys,
            max_retries=10
        )
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 6: CLEANUP
        # ═══════════════════════════════════════════════════════════════════
        logger.info("=" * 80)
        logger.info("STEP 6: CLEANUP")
        logger.info("=" * 80)
        
        # Drop staging tables
        cleanup_staging_table(staging_table, fb_connector)
        if dedup_table_created:
            cleanup_staging_table(dedup_table, fb_connector)
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 7: MARK FILE AS COMPLETED
        # ═══════════════════════════════════════════════════════════════════
        logger.info("=" * 80)
        logger.info("STEP 7: MARK FILE AS COMPLETED")
        logger.info("=" * 80)
        
        mark_file_completed(file_key, fb_connector)
        
    except Exception as e:
        # Cleanup on error
        if staging_table:
            cleanup_staging_table(staging_table, fb_connector)
        if dedup_table and dedup_table != staging_table:
            cleanup_staging_table(dedup_table, fb_connector)
        
        # Mark file as failed
        logger.error(f"Error processing file: {e}")
        if fb_connector:
            mark_file_failed(file_key, str(e), fb_connector)
        
        raise
    finally:
        # Always disconnect
        if fb_connector:
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
    
    logger.info(f"Processing complete in {duration:.2f}s")
    return result

