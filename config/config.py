"""Configuration for Firebolt CDC Lambda"""
import os
from dotenv import load_dotenv

load_dotenv()

def get_config():
    """Get deployment configuration"""
    return {
        'aws_region': os.getenv('AWS_REGION', 'ap-south-1'),
        'environment': os.getenv('ENVIRONMENT', 'prod'),
        's3_bucket_name': os.getenv('S3_BUCKET_NAME', 'fcanalytics'),
        's3_prefix': os.getenv('S3_PREFIX', 'firebolt_dms_job/'),
        'lambda_function_name': os.getenv('LAMBDA_FUNCTION_NAME', 'firebolt-cdc-processor'),
        'lambda_timeout': int(os.getenv('LAMBDA_TIMEOUT', '300')),
        'lambda_memory_size': int(os.getenv('LAMBDA_MEMORY_SIZE', '512')),
        'firebolt_account': os.getenv('FIREBOLT_ACCOUNT', 'faircent'),
        'firebolt_database': os.getenv('FIREBOLT_DATABASE', ''),
        'firebolt_engine': os.getenv('FIREBOLT_ENGINE', ''),
        'firebolt_username': os.getenv('FIREBOLT_USERNAME', ''),
        'firebolt_password': os.getenv('FIREBOLT_PASSWORD', ''),
        'location_name': os.getenv('LOCATION_NAME', 's3_raw_dms'),
        'table_keys_s3_bucket': os.getenv('TABLE_KEYS_S3_BUCKET', 'fcanalytics'),
        'table_keys_s3_key': os.getenv('TABLE_KEYS_S3_KEY', 'firebolt-migration/config/tables_keys.json'),
        'cdc_delete_column': os.getenv('CDC_DELETE_COLUMN', ''),
        'cdc_delete_values': os.getenv('CDC_DELETE_VALUES', ''),
    }

