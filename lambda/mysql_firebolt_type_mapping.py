#!/usr/bin/env python3
"""
MySQL to Firebolt Type Mapping
==============================

Comprehensive mapping of MySQL/MariaDB data types to Firebolt equivalents.
Used by schema evolution to automatically convert types.

Usage:
    from mysql_firebolt_type_mapping import convert_mysql_to_firebolt, MYSQL_TO_FIREBOLT_MAP
    
    firebolt_type, is_safe, message = convert_mysql_to_firebolt("VARCHAR(255)")
"""

from typing import Tuple, Optional
import re

# ═══════════════════════════════════════════════════════════════════════════════
# MYSQL TO FIREBOLT TYPE MAPPING
# ═══════════════════════════════════════════════════════════════════════════════

MYSQL_TO_FIREBOLT_MAP = {
    # ═══════════════════════════════════════════════════════════════════════════
    # STRING TYPES
    # ═══════════════════════════════════════════════════════════════════════════
    'CHAR': 'TEXT',
    'VARCHAR': 'TEXT',
    'TINYTEXT': 'TEXT',
    'TEXT': 'TEXT',
    'MEDIUMTEXT': 'TEXT',
    'LONGTEXT': 'TEXT',
    'STRING': 'TEXT',  # DMS/Parquet
    
    # Binary strings
    'BINARY': 'TEXT',
    'VARBINARY': 'TEXT',
    'TINYBLOB': 'TEXT',
    'BLOB': 'TEXT',
    'MEDIUMBLOB': 'TEXT',
    'LONGBLOB': 'TEXT',
    'BYTEA': 'TEXT',  # DMS writes BLOB as bytea
    
    # Special string types
    'ENUM': 'TEXT',
    'SET': 'TEXT',
    'JSON': 'TEXT',
    'JSONB': 'TEXT',
    'UUID': 'TEXT',
    'INET': 'TEXT',  # IP addresses
    'CIDR': 'TEXT',
    'MACADDR': 'TEXT',
    
    # ═══════════════════════════════════════════════════════════════════════════
    # INTEGER TYPES
    # ═══════════════════════════════════════════════════════════════════════════
    'BIT': 'BOOLEAN',  # BIT(1) is boolean
    'TINYINT': 'INTEGER',
    'BOOL': 'BOOLEAN',
    'BOOLEAN': 'BOOLEAN',
    'SMALLINT': 'INTEGER',
    'MEDIUMINT': 'INTEGER',
    'INT': 'INTEGER',
    'INTEGER': 'INTEGER',
    'BIGINT': 'BIGINT',
    
    # Unsigned variants (Firebolt doesn't have unsigned, use larger type)
    'TINYINT UNSIGNED': 'INTEGER',
    'SMALLINT UNSIGNED': 'INTEGER',
    'MEDIUMINT UNSIGNED': 'INTEGER',
    'INT UNSIGNED': 'BIGINT',
    'INTEGER UNSIGNED': 'BIGINT',
    'BIGINT UNSIGNED': 'NUMERIC(20, 0)',  # No unsigned BIGINT in Firebolt
    
    # Parquet/Arrow types
    'INT8': 'INTEGER',
    'INT16': 'INTEGER',
    'INT32': 'INTEGER',
    'INT64': 'BIGINT',
    'UINT8': 'INTEGER',
    'UINT16': 'INTEGER',
    'UINT32': 'BIGINT',
    'UINT64': 'NUMERIC(20, 0)',
    
    # ═══════════════════════════════════════════════════════════════════════════
    # DECIMAL/NUMERIC TYPES
    # ═══════════════════════════════════════════════════════════════════════════
    'DECIMAL': 'NUMERIC(38, 10)',  # Default precision
    'NUMERIC': 'NUMERIC(38, 10)',
    'DEC': 'NUMERIC(38, 10)',
    'FIXED': 'NUMERIC(38, 10)',
    'NUMBER': 'NUMERIC(38, 10)',
    'MONEY': 'NUMERIC(19, 4)',
    'SMALLMONEY': 'NUMERIC(10, 4)',
    
    # ═══════════════════════════════════════════════════════════════════════════
    # FLOATING POINT TYPES
    # ═══════════════════════════════════════════════════════════════════════════
    'FLOAT': 'REAL',
    'FLOAT4': 'REAL',
    'FLOAT8': 'DOUBLE',
    'REAL': 'REAL',
    'DOUBLE': 'DOUBLE',
    'DOUBLE PRECISION': 'DOUBLE',
    'FLOAT32': 'REAL',
    'FLOAT64': 'DOUBLE',
    
    # ═══════════════════════════════════════════════════════════════════════════
    # DATE/TIME TYPES
    # ═══════════════════════════════════════════════════════════════════════════
    'DATE': 'DATE',
    'DATETIME': 'TIMESTAMP',
    'TIMESTAMP': 'TIMESTAMPTZ',
    'TIMESTAMPTZ': 'TIMESTAMPTZ',
    'TIME': 'TEXT',  # Firebolt doesn't have TIME type
    'TIMETZ': 'TEXT',
    'YEAR': 'INTEGER',
    'INTERVAL': 'TEXT',  # Store as text
    
    # Parquet/Arrow date types
    'DATE32': 'DATE',
    'DATE64': 'DATE',
    'TIMESTAMP_S': 'TIMESTAMP',
    'TIMESTAMP_MS': 'TIMESTAMP',
    'TIMESTAMP_US': 'TIMESTAMP',
    'TIMESTAMP_NS': 'TIMESTAMP',
    
    # ═══════════════════════════════════════════════════════════════════════════
    # SPATIAL TYPES (Cannot auto-convert)
    # ═══════════════════════════════════════════════════════════════════════════
    'GEOMETRY': None,
    'POINT': None,
    'LINESTRING': None,
    'POLYGON': None,
    'MULTIPOINT': None,
    'MULTILINESTRING': None,
    'MULTIPOLYGON': None,
    'GEOMETRYCOLLECTION': None,
    'GEOGRAPHY': None,
    
    # ═══════════════════════════════════════════════════════════════════════════
    # COMPLEX TYPES (Cannot auto-convert)
    # ═══════════════════════════════════════════════════════════════════════════
    'ARRAY': None,
    'STRUCT': None,
    'MAP': None,
    'ROW': None,
    'TUPLE': None,
    
    # ═══════════════════════════════════════════════════════════════════════════
    # OTHER TYPES
    # ═══════════════════════════════════════════════════════════════════════════
    'XML': 'TEXT',
    'CLOB': 'TEXT',
    'NCLOB': 'TEXT',
    'NCHAR': 'TEXT',
    'NVARCHAR': 'TEXT',
    'NTEXT': 'TEXT',
    'IMAGE': 'TEXT',  # Binary image data
    'SERIAL': 'INTEGER',
    'BIGSERIAL': 'BIGINT',
    'SMALLSERIAL': 'INTEGER',
}

# Types that are safe to auto-add (won't cause data loss)
SAFE_AUTO_ADD_TYPES = {
    'TEXT', 'VARCHAR', 'STRING', 'CHAR',
    'INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT',
    'BOOLEAN', 'BOOL',
    'DATE', 'TIMESTAMP', 'TIMESTAMPTZ',
    'DOUBLE', 'FLOAT', 'REAL',
    'NUMERIC', 'DECIMAL', 'NUMBER', 'DEC',
}

# Types that require manual intervention
MANUAL_INTERVENTION_TYPES = {
    'ARRAY', 'STRUCT', 'MAP', 'ROW', 'TUPLE',
    'GEOMETRY', 'POINT', 'LINESTRING', 'POLYGON',
    'MULTIPOINT', 'MULTILINESTRING', 'MULTIPOLYGON',
    'GEOMETRYCOLLECTION', 'GEOGRAPHY'
}


def normalize_type(data_type: str) -> str:
    """
    Normalize a type string to base type (without precision/scale).
    
    Examples:
        VARCHAR(255) -> VARCHAR
        DECIMAL(10,2) -> DECIMAL
        INT UNSIGNED -> INT UNSIGNED
    """
    if not data_type:
        return 'UNKNOWN'
    
    # Uppercase and strip
    dtype = data_type.upper().strip()
    
    # Handle UNSIGNED suffix specially
    if ' UNSIGNED' in dtype:
        base = dtype.split('(')[0].strip()
        return base
    
    # Remove precision/scale
    base = dtype.split('(')[0].strip()
    return base


def extract_precision(data_type: str) -> Optional[Tuple[int, int]]:
    """
    Extract precision and scale from a type string.
    
    Returns:
        Tuple of (precision, scale) or None if not present
        
    Examples:
        DECIMAL(10,2) -> (10, 2)
        VARCHAR(255) -> (255, None)
        INT -> None
    """
    match = re.search(r'\((\d+)(?:\s*,\s*(\d+))?\)', data_type)
    if match:
        precision = int(match.group(1))
        scale = int(match.group(2)) if match.group(2) else None
        return (precision, scale)
    return None


def convert_mysql_to_firebolt(source_type: str) -> Tuple[Optional[str], bool, str]:
    """
    Convert a MySQL/DMS type to Firebolt type.
    
    Args:
        source_type: MySQL data type (e.g., "VARCHAR(255)", "DECIMAL(10,2)")
    
    Returns:
        Tuple of:
            - firebolt_type: The converted Firebolt type or None if cannot convert
            - is_safe: True if the conversion is safe for auto-adding
            - message: Explanation or error message
    
    Examples:
        >>> convert_mysql_to_firebolt("VARCHAR(255)")
        ('TEXT', True, 'Converted from VARCHAR')
        
        >>> convert_mysql_to_firebolt("DECIMAL(10,2)")
        ('NUMERIC(10, 2)', True, 'Converted from DECIMAL with preserved precision')
        
        >>> convert_mysql_to_firebolt("GEOMETRY")
        (None, False, 'Type GEOMETRY requires manual conversion (spatial type)')
    """
    if not source_type:
        return None, False, "Empty source type"
    
    base_type = normalize_type(source_type)
    
    # Check if already a valid Firebolt type
    if base_type in SAFE_AUTO_ADD_TYPES:
        # Handle precision for DECIMAL/NUMERIC
        if base_type in ('DECIMAL', 'NUMERIC', 'NUMBER', 'DEC'):
            precision = extract_precision(source_type)
            if precision:
                p, s = precision
                s = s if s is not None else 0
                return f'NUMERIC({p}, {s})', True, 'Already valid with preserved precision'
        return source_type, True, "Already valid Firebolt type"
    
    # Lookup in mapping
    if base_type in MYSQL_TO_FIREBOLT_MAP:
        firebolt_type = MYSQL_TO_FIREBOLT_MAP[base_type]
        
        if firebolt_type is None:
            if base_type in MANUAL_INTERVENTION_TYPES:
                return None, False, f"Type {base_type} requires manual conversion (complex/spatial type)"
            return None, False, f"Type {base_type} has no Firebolt equivalent"
        
        # Handle DECIMAL/NUMERIC with precision preservation
        if base_type in ('DECIMAL', 'NUMERIC', 'DEC', 'FIXED', 'NUMBER', 'MONEY', 'SMALLMONEY'):
            precision = extract_precision(source_type)
            if precision:
                p, s = precision
                s = s if s is not None else 0
                # Ensure precision fits in NUMERIC(38, x)
                p = min(p, 38)
                return f'NUMERIC({p}, {s})', True, f'Converted from {base_type} with preserved precision'
        
        return firebolt_type, True, f'Converted from {base_type}'
    
    # Check if it's a manual intervention type
    if base_type in MANUAL_INTERVENTION_TYPES:
        return None, False, f"Type {base_type} cannot be auto-converted (requires manual intervention)"
    
    # Unknown type - try TEXT as fallback (with warning)
    return 'TEXT', False, f"Unknown type {source_type} - using TEXT fallback (may need review)"


def is_type_compatible(source_type: str, target_type: str) -> bool:
    """
    Check if source type is compatible with target type (for MERGE operations).
    
    Compatible means data can be safely transferred without loss.
    """
    src_base = normalize_type(source_type)
    tgt_base = normalize_type(target_type)
    
    # Same base type is always compatible
    if src_base == tgt_base:
        return True
    
    # Define compatible type families
    compatible_groups = [
        {'TEXT', 'VARCHAR', 'STRING', 'CHAR', 'NCHAR', 'NVARCHAR'},
        {'INTEGER', 'INT', 'SMALLINT', 'TINYINT', 'MEDIUMINT'},
        {'BIGINT', 'INT64'},
        {'DOUBLE', 'FLOAT', 'REAL', 'FLOAT64', 'FLOAT32'},
        {'BOOLEAN', 'BOOL', 'BIT'},
        {'TIMESTAMP', 'TIMESTAMPTZ', 'DATETIME'},
        {'NUMERIC', 'DECIMAL', 'DEC', 'NUMBER'},
    ]
    
    for group in compatible_groups:
        if src_base in group and tgt_base in group:
            return True
    
    return False


def get_cast_expression(column: str, source_type: str, target_type: str) -> str:
    """
    Generate a CAST expression to convert between types.
    
    Args:
        column: Column name
        source_type: Source data type
        target_type: Target Firebolt type
    
    Returns:
        CAST expression string
        
    Example:
        >>> get_cast_expression("amount", "DECIMAL(10,2)", "NUMERIC(38, 10)")
        'CAST("amount" AS NUMERIC(38, 10))'
    """
    return f'CAST("{column}" AS {target_type})'


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def print_type_mapping():
    """Print all type mappings in a table format"""
    print("=" * 80)
    print("MySQL to Firebolt Type Mapping")
    print("=" * 80)
    print(f"{'MySQL Type':<25} {'Firebolt Type':<25} {'Safe?':<8}")
    print("-" * 80)
    
    for mysql_type, firebolt_type in sorted(MYSQL_TO_FIREBOLT_MAP.items()):
        safe = "✓" if firebolt_type else "✗"
        fb_type = firebolt_type if firebolt_type else "MANUAL"
        print(f"{mysql_type:<25} {fb_type:<25} {safe:<8}")


def test_conversions():
    """Test some common conversions"""
    test_types = [
        "VARCHAR(255)",
        "DECIMAL(10,2)",
        "BIGINT UNSIGNED",
        "DATETIME",
        "JSON",
        "GEOMETRY",
        "ARRAY<STRING>",
        "UNKNOWN_TYPE",
        "INT",
        "TIMESTAMP",
    ]
    
    print("\n" + "=" * 80)
    print("Type Conversion Tests")
    print("=" * 80)
    
    for t in test_types:
        firebolt_type, is_safe, message = convert_mysql_to_firebolt(t)
        safe_str = "✓" if is_safe else "⚠️"
        fb_type = firebolt_type if firebolt_type else "N/A"
        print(f"{safe_str} {t:<25} → {fb_type:<25} ({message})")


if __name__ == "__main__":
    print_type_mapping()
    test_conversions()

