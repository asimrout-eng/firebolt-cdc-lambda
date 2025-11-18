#!/usr/bin/env python3
"""
Data Validation Script: Redshift vs Firebolt
Compares row counts and aggregates between the two databases
"""

import os
import psycopg2
from firebolt.db import connect as fb_connect
from firebolt.client.auth import ClientCredentials
import pandas as pd
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════

# Firebolt Configuration
FIREBOLT_CLIENT_ID = os.getenv('FIREBOLT_CLIENT_ID')
FIREBOLT_CLIENT_SECRET = os.getenv('FIREBOLT_CLIENT_SECRET')
FIREBOLT_ACCOUNT = os.getenv('FIREBOLT_ACCOUNT')
FIREBOLT_DATABASE = os.getenv('FIREBOLT_DATABASE')
FIREBOLT_ENGINE = os.getenv('FIREBOLT_ENGINE')

# Redshift Configuration
REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = os.getenv('REDSHIFT_PORT', '5439')
REDSHIFT_DATABASE = os.getenv('REDSHIFT_DATABASE')
REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
REDSHIFT_SCHEMA = 'fair'  # Your schema name

# ═══════════════════════════════════════════════════════════════════
# Functions
# ═══════════════════════════════════════════════════════════════════

def get_firebolt_connection():
    """Connect to Firebolt"""
    auth = ClientCredentials(FIREBOLT_CLIENT_ID, FIREBOLT_CLIENT_SECRET)
    return fb_connect(
        auth=auth,
        account_name=FIREBOLT_ACCOUNT,
        engine_name=FIREBOLT_ENGINE,
        database=FIREBOLT_DATABASE
    )

def get_redshift_connection():
    """Connect to Redshift"""
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DATABASE,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )

def get_firebolt_table_counts():
    """Get row counts for all tables in Firebolt"""
    conn = get_firebolt_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        table_name,
        table_rows as row_count
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE'
    ORDER BY table_name
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    df = pd.DataFrame(results, columns=['table_name', 'firebolt_count'])
    
    conn.close()
    return df

def get_redshift_table_counts():
    """Get row counts for all tables in Redshift"""
    conn = get_redshift_connection()
    cursor = conn.cursor()
    
    query = f"""
    SELECT 
        tablename as table_name,
        n_live_tup as row_count
    FROM pg_stat_user_tables
    WHERE schemaname = '{REDSHIFT_SCHEMA}'
    ORDER BY tablename
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    df = pd.DataFrame(results, columns=['table_name', 'redshift_count'])
    
    conn.close()
    return df

def compare_table_counts(firebolt_df, redshift_df):
    """Compare row counts between Firebolt and Redshift"""
    # Merge dataframes
    merged = pd.merge(
        firebolt_df, 
        redshift_df, 
        on='table_name', 
        how='outer',
        indicator=True
    )
    
    # Fill NaN with 0 for missing tables
    merged['firebolt_count'] = merged['firebolt_count'].fillna(0).astype(int)
    merged['redshift_count'] = merged['redshift_count'].fillna(0).astype(int)
    
    # Calculate difference
    merged['difference'] = merged['redshift_count'] - merged['firebolt_count']
    merged['match'] = merged['firebolt_count'] == merged['redshift_count']
    merged['pct_diff'] = ((merged['difference'] / merged['redshift_count']) * 100).round(2)
    merged['pct_diff'] = merged['pct_diff'].replace([float('inf'), -float('inf')], 0)
    
    # Add status
    merged['status'] = merged.apply(lambda row: 
        '✓ MATCH' if row['match'] 
        else f'✗ MISMATCH ({row["pct_diff"]}%)', 
        axis=1
    )
    
    return merged[['table_name', 'redshift_count', 'firebolt_count', 'difference', 'pct_diff', 'status']]

def validate_specific_table(table_name):
    """Detailed validation for a specific table"""
    print(f"\n{'='*70}")
    print(f"Detailed Validation: {table_name}")
    print(f"{'='*70}")
    
    # Firebolt
    fb_conn = get_firebolt_connection()
    fb_cursor = fb_conn.cursor()
    
    fb_query = f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT id) as unique_ids,
        MIN(created) as earliest_record,
        MAX(created) as latest_record
    FROM "public"."{table_name}"
    """
    
    fb_cursor.execute(fb_query)
    fb_result = fb_cursor.fetchone()
    fb_conn.close()
    
    # Redshift
    rs_conn = get_redshift_connection()
    rs_cursor = rs_conn.cursor()
    
    rs_query = f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT id) as unique_ids,
        MIN(created) as earliest_record,
        MAX(created) as latest_record
    FROM {REDSHIFT_SCHEMA}.{table_name}
    """
    
    rs_cursor.execute(rs_query)
    rs_result = rs_cursor.fetchone()
    rs_conn.close()
    
    # Compare
    print(f"\nRedshift:")
    print(f"  Total Rows: {rs_result[0]:,}")
    print(f"  Unique IDs: {rs_result[1]:,}")
    print(f"  Earliest: {rs_result[2]}")
    print(f"  Latest: {rs_result[3]}")
    
    print(f"\nFirebolt:")
    print(f"  Total Rows: {fb_result[0]:,}")
    print(f"  Unique IDs: {fb_result[1]:,}")
    print(f"  Earliest: {fb_result[2]}")
    print(f"  Latest: {fb_result[3]}")
    
    print(f"\nMatch: {'✓ YES' if fb_result == rs_result else '✗ NO'}")

# ═══════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print("  DATA VALIDATION: Redshift vs Firebolt")
    print("=" * 70)
    
    # Get table counts
    print("\n📊 Fetching Firebolt table counts...")
    firebolt_df = get_firebolt_table_counts()
    print(f"   Found {len(firebolt_df)} tables in Firebolt")
    
    print("\n📊 Fetching Redshift table counts...")
    redshift_df = get_redshift_table_counts()
    print(f"   Found {len(redshift_df)} tables in Redshift")
    
    # Compare
    print("\n📊 Comparing table counts...")
    comparison_df = compare_table_counts(firebolt_df, redshift_df)
    
    # Summary
    matches = len(comparison_df[comparison_df['match'] == True])
    mismatches = len(comparison_df[comparison_df['match'] == False])
    
    print(f"\n{'='*70}")
    print(f"  SUMMARY")
    print(f"{'='*70}")
    print(f"Total Tables: {len(comparison_df)}")
    print(f"✓ Matches: {matches} ({matches/len(comparison_df)*100:.1f}%)")
    print(f"✗ Mismatches: {mismatches} ({mismatches/len(comparison_df)*100:.1f}%)")
    
    # Save report
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f'validation_report_{timestamp}.csv'
    comparison_df.to_csv(report_file, index=False)
    print(f"\n💾 Report saved to: {report_file}")
    
    # Show mismatches
    if mismatches > 0:
        print(f"\n{'='*70}")
        print(f"  MISMATCHES ({mismatches} tables)")
        print(f"{'='*70}")
        mismatched = comparison_df[comparison_df['match'] == False].head(20)
        print(mismatched.to_string(index=False))
        
        if mismatches > 20:
            print(f"\n... and {mismatches - 20} more (see CSV report)")
    
    # Detailed validation for first mismatch (optional)
    if mismatches > 0:
        first_mismatch = comparison_df[comparison_df['match'] == False].iloc[0]['table_name']
        user_input = input(f"\nRun detailed validation for '{first_mismatch}'? (y/n): ")
        if user_input.lower() == 'y':
            validate_specific_table(first_mismatch)

if __name__ == '__main__':
    main()

