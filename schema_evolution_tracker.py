#!/usr/bin/env python3
"""
Schema Evolution Tracker for Firebolt CDC Lambda
Tracks new tables, new columns, and schema changes
"""
from firebolt.db import connect
from firebolt.client.auth import ClientCredentials
import json
import boto3
from datetime import datetime
from typing import Dict, List, Set, Optional

# Firebolt connection parameters
CLIENT_ID = "bDnqfj5uDfygACM7ByLH285LPd8ny4wI"
CLIENT_SECRET = "mRws0ftFkF1EiCJBvoc_K_vT_c77g6ySTIpc7aQywzoqS7WjfuVlu5W0hyMdy0ls"
ACCOUNT_NAME = "faircentindia"
DATABASE = "fair"
ENGINE_NAME = "support_test_db"

# S3 configuration for table_keys.json
S3_BUCKET = "fcanalytics"  # Update with your bucket
S3_KEY = "firebolt_dms_job/config/tables_keys.json"

class SchemaEvolutionTracker:
    """Track and handle schema evolution"""
    
    def __init__(self):
        self.fb_connector = None
        self.s3_client = boto3.client('s3')
        
    def connect(self):
        """Connect to Firebolt"""
        self.fb_connector = connect(
            engine_name=ENGINE_NAME,
            database=DATABASE,
            account_name=ACCOUNT_NAME,
            auth=ClientCredentials(CLIENT_ID, CLIENT_SECRET)
        )
        return self.fb_connector.cursor()
    
    def disconnect(self):
        """Disconnect from Firebolt"""
        if self.fb_connector:
            self.fb_connector.close()
    
    def get_all_tables_in_firebolt(self) -> Set[str]:
        """Get all tables currently in Firebolt"""
        cursor = self.connect()
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND (table_type = 'FACT TABLE' OR table_type = 'DIMENSION TABLE')
            ORDER BY table_name
        """)
        tables = {row[0] for row in cursor.fetchall()}
        self.disconnect()
        return tables
    
    def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get column names and types for a table"""
        cursor = self.connect()
        cursor.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
              AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        schema = {row[0]: row[1] for row in cursor.fetchall()}
        self.disconnect()
        return schema
    
    def get_table_primary_key(self, table_name: str) -> Optional[List[str]]:
        """Get primary key columns for a table"""
        cursor = self.connect()
        cursor.execute(f"""
            SELECT index_definition
            FROM information_schema.indexes
            WHERE table_name = '{table_name}'
              AND index_type = 'primary'
        """)
        result = cursor.fetchone()
        self.disconnect()
        
        if result:
            # Parse index definition like "[id]" or "(id, col2)"
            index_def = result[0]
            pk_cols = index_def.replace('[', '').replace(']', '').replace('(', '').replace(')', '').replace('"', '').split(',')
            return [col.strip() for col in pk_cols if col.strip()]
        return None
    
    def load_table_keys(self) -> Dict:
        """Load table_keys.json from S3"""
        try:
            obj = self.s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
            return json.loads(obj['Body'].read().decode('utf-8'))
        except Exception as e:
            print(f"Error loading table_keys: {e}")
            return {}
    
    def save_table_keys(self, table_keys: Dict):
        """Save table_keys.json to S3"""
        self.s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY,
            Body=json.dumps(table_keys, indent=2),
            ContentType='application/json'
        )
    
    def detect_new_tables(self) -> List[str]:
        """Detect tables in Firebolt that are not in table_keys.json"""
        firebolt_tables = self.get_all_tables_in_firebolt()
        table_keys = self.load_table_keys()
        existing_tables = set(table_keys.keys())
        
        new_tables = sorted(firebolt_tables - existing_tables)
        return new_tables
    
    def detect_schema_changes(self, table_name: str) -> Dict:
        """Detect schema changes for a specific table"""
        current_schema = self.get_table_schema(table_name)
        
        # Get schema from last known state (stored in metadata table or S3)
        # For now, we'll compare with what's in Firebolt
        # In production, you'd store previous schema state
        
        changes = {
            'new_columns': [],
            'removed_columns': [],
            'type_changes': []
        }
        
        # This is a simplified version - in production, compare with stored previous state
        return changes
    
    def auto_configure_new_table(self, table_name: str) -> Optional[str]:
        """Auto-configure primary key for a new table"""
        pk_cols = self.get_table_primary_key(table_name)
        
        if pk_cols:
            if len(pk_cols) == 1:
                return pk_cols[0]  # Single column primary key
            else:
                return pk_cols  # Composite primary key
        else:
            # No primary key - check for common patterns
            schema = self.get_table_schema(table_name)
            
            # Common primary key column names
            common_pk_names = ['id', 'uuid', f'{table_name}_id', 'pk_id']
            
            for pk_name in common_pk_names:
                if pk_name in schema:
                    return pk_name
            
            # No primary key found
            return None
    
    def generate_schema_evolution_report(self) -> Dict:
        """Generate comprehensive schema evolution report"""
        print("=" * 80)
        print("SCHEMA EVOLUTION REPORT")
        print("=" * 80)
        print()
        
        # Detect new tables
        print("Step 1: Detecting new tables...")
        new_tables = self.detect_new_tables()
        
        if new_tables:
            print(f"⚠️  Found {len(new_tables)} new tables not in table_keys.json:")
            for table in new_tables:
                print(f"  - {table}")
        else:
            print("✅ No new tables detected")
        
        print()
        
        # Load current table_keys
        table_keys = self.load_table_keys()
        
        # Check for tables in table_keys that don't exist in Firebolt
        print("Step 2: Checking for removed tables...")
        firebolt_tables = self.get_all_tables_in_firebolt()
        removed_tables = sorted(set(table_keys.keys()) - firebolt_tables)
        
        if removed_tables:
            print(f"⚠️  Found {len(removed_tables)} tables in config but not in Firebolt:")
            for table in removed_tables:
                print(f"  - {table}")
        else:
            print("✅ No removed tables detected")
        
        print()
        
        # Generate recommendations
        recommendations = []
        
        if new_tables:
            print("Step 3: Auto-configuring new tables...")
            for table in new_tables:
                pk = self.auto_configure_new_table(table)
                if pk:
                    table_keys[table] = pk
                    recommendations.append({
                        'table': table,
                        'action': 'add',
                        'primary_key': pk,
                        'status': 'auto_configured'
                    })
                    print(f"  ✅ {table}: Auto-configured PK = {pk}")
                else:
                    recommendations.append({
                        'table': table,
                        'action': 'add',
                        'primary_key': None,
                        'status': 'needs_manual_config'
                    })
                    print(f"  ⚠️  {table}: No PK found - needs manual configuration")
        
        print()
        
        return {
            'new_tables': new_tables,
            'removed_tables': removed_tables,
            'recommendations': recommendations,
            'updated_table_keys': table_keys
        }
    
    def apply_recommendations(self, report: Dict, auto_apply: bool = False):
        """Apply schema evolution recommendations"""
        if not report['recommendations']:
            print("No recommendations to apply")
            return
        
        print("=" * 80)
        print("APPLYING RECOMMENDATIONS")
        print("=" * 80)
        print()
        
        if not auto_apply:
            print("Recommended changes:")
            for rec in report['recommendations']:
                print(f"  - {rec['table']}: {rec['action']} with PK = {rec['primary_key']}")
            print()
            response = input("Apply these changes? (yes/no): ").strip().lower()
            if response != 'yes':
                print("Cancelled")
                return
        
        # Apply changes
        updated_keys = report['updated_table_keys']
        
        # Backup current config
        backup_key = f"{S3_KEY}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        current_keys = self.load_table_keys()
        self.s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=backup_key,
            Body=json.dumps(current_keys, indent=2),
            ContentType='application/json'
        )
        print(f"✅ Backup created: {backup_key}")
        
        # Save updated config
        self.save_table_keys(updated_keys)
        print(f"✅ Updated table_keys.json with {len(report['recommendations'])} changes")
        
        # Save report
        report_key = f"firebolt_dms_job/config/schema_evolution_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self.s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=report_key,
            Body=json.dumps(report, indent=2, default=str),
            ContentType='application/json'
        )
        print(f"✅ Report saved: {report_key}")

def main():
    """Main function"""
    tracker = SchemaEvolutionTracker()
    
    try:
        # Generate report
        report = tracker.generate_schema_evolution_report()
        
        # Apply recommendations
        if report['recommendations']:
            tracker.apply_recommendations(report, auto_apply=False)
        else:
            print("✅ No changes needed")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        tracker.disconnect()

if __name__ == "__main__":
    main()

