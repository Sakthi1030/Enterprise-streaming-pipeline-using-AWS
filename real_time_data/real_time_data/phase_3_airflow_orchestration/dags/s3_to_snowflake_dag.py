"""
DAG for loading data from S3 to Snowflake
Transforms and loads data into Snowflake data warehouse
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.models import Variable
import json
import boto3
from typing import List, Dict, Any
import pandas as pd
from io import StringIO

default_args = {
    'owner': 'sales-platform',
    'depends_on_past': False,
    'email': ['alerts@sales-platform.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    's3_to_snowflake_pipeline',
    default_args=default_args,
    description='Load data from S3 to Snowflake',
    schedule_interval=None,  # Manual runs only for local stability
    catchup=False,
    tags=['s3', 'snowflake', 'data-warehouse', 'etl'],
    max_active_runs=1,
)

def check_s3_new_data(**kwargs):
    """Check S3 for new data to process"""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
    ti = kwargs['ti']
    
    # Get configuration
    s3_bucket = Variable.get("S3_PROCESSED_BUCKET", "sales-platform-processed-data-dev")
    prefix = Variable.get("S3_PROCESSED_PREFIX", "processed/kinesis/")
    region = Variable.get("AWS_DEFAULT_REGION", "us-east-1")
    
    # Initialize hook
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    try:
        # Calculate time window (last 2 hours)
        cutoff_time = datetime.now() - timedelta(hours=2)
        
        # List files in S3
        all_files = s3_hook.list_keys(bucket_name=s3_bucket, prefix=prefix)
        
        if not all_files:
            ti.xcom_push(key='new_files', value=[])
            ti.xcom_push(key='files_count', value=0)
            return 'no_new_data'
        
        # Filter files by last modified time
        new_files = []
        total_size = 0
        
        for file_key in all_files:
            try:
                file_obj = s3_hook.get_key(file_key, s3_bucket)
                last_modified = file_obj.last_modified

                # Normalize timezone-aware datetimes to naive UTC for safe comparison.
                if isinstance(last_modified, str):
                    last_modified = datetime.strptime(last_modified, '%Y-%m-%d %H:%M:%S%z').replace(tzinfo=None)
                elif getattr(last_modified, "tzinfo", None) is not None:
                    last_modified = last_modified.replace(tzinfo=None)

                if last_modified > cutoff_time:
                    new_files.append({
                        'key': file_key,
                        'size': file_obj.content_length,
                        'last_modified': last_modified.isoformat(),
                        'type': file_key.split('.')[-1]
                    })
                    total_size += file_obj.content_length
            except Exception as e:
                print(f"Error processing file {file_key}: {str(e)}")
        
        ti.xcom_push(key='new_files', value=new_files)
        ti.xcom_push(key='files_count', value=len(new_files))
        ti.xcom_push(key='total_size_bytes', value=total_size)
        
        if new_files:
            return 'create_snowflake_stage'
        else:
            return 'no_new_data'
    
    except Exception as e:
        ti.xcom_push(key='error', value=str(e))
        raise Exception(f"Failed to check S3 for new data: {str(e)}")

def create_snowflake_stage(**kwargs):
    """Create or update Snowflake external stage"""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    ti = kwargs['ti']
    
    # Get configuration
    s3_bucket = Variable.get("S3_PROCESSED_BUCKET", "sales-platform-processed-data-dev")
    aws_region = Variable.get("AWS_DEFAULT_REGION", "us-east-1")
    snowflake_database = Variable.get("SNOWFLAKE_DATABASE", "SALES_DB")
    snowflake_schema = Variable.get("SNOWFLAKE_SCHEMA", "RAW")
    snowflake_warehouse = Variable.get("SNOWFLAKE_WAREHOUSE", "SALES_WH")
    
    # Get AWS credentials (assuming IAM role or stored in Airflow connections)
    s3_hook = S3Hook(aws_conn_id='aws_default')
    credentials = s3_hook.get_credentials()
    
    # Create stage SQL
    create_stage_sql = f"""
    CREATE OR REPLACE STAGE {snowflake_database}.{snowflake_schema}.S3_SALES_DATA_STAGE
    URL = 's3://{s3_bucket}/processed/kinesis/'
    STORAGE_INTEGRATION = AWS_S3_INTEGRATION
    FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
    COMMENT = 'Stage for S3 sales data';
    """
    
    # Execute using SnowflakeHook
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    try:
        connection = snowflake_hook.get_conn()
        cursor = connection.cursor()
        
        # Set warehouse
        cursor.execute(f"USE WAREHOUSE {snowflake_warehouse};")
        
        # Create stage
        cursor.execute(create_stage_sql)
        
        # Verify stage was created
        cursor.execute(f"DESC STAGE {snowflake_database}.{snowflake_schema}.S3_SALES_DATA_STAGE;")
        stage_info = cursor.fetchall()
        
        ti.xcom_push(key='stage_created', value=True)
        ti.xcom_push(key='stage_info', value=str(stage_info))
        
        return f"Snowflake stage created: {snowflake_database}.{snowflake_schema}.S3_SALES_DATA_STAGE"
    
    except Exception as e:
        ti.xcom_push(key='error', value=str(e))
        raise Exception(f"Failed to create Snowflake stage: {str(e)}")

def load_transactions_to_snowflake(**kwargs):
    """Load transaction data from S3 to Snowflake"""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    ti = kwargs['ti']
    
    # Get configuration
    snowflake_database = Variable.get("SNOWFLAKE_DATABASE", "SALES_DB")
    snowflake_schema = Variable.get("SNOWFLAKE_SCHEMA", "RAW")
    snowflake_warehouse = Variable.get("SNOWFLAKE_WAREHOUSE", "SALES_WH")
    
    # Get new files from previous task
    new_files = ti.xcom_pull(task_ids='check_s3_new_data', key='new_files')
    
    if not new_files:
        return "No new files to load"
    
    # Initialize Snowflake hook
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    try:
        connection = snowflake_hook.get_conn()
        cursor = connection.cursor()
        
        # Set warehouse and database
        cursor.execute(f"USE WAREHOUSE {snowflake_warehouse};")
        cursor.execute(f"USE DATABASE {snowflake_database};")
        cursor.execute(f"USE SCHEMA {snowflake_schema};")
        
        # Create transactions table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS TRANSACTIONS_RAW (
            RAW_DATA VARIANT,
            TRANSACTION_ID STRING AS (RAW_DATA:transaction_id::STRING),
            ORDER_ID STRING AS (RAW_DATA:order_id::STRING),
            TIMESTAMP TIMESTAMP_NTZ AS (RAW_DATA:timestamp::TIMESTAMP_NTZ),
            CUSTOMER_ID STRING AS (RAW_DATA:customer.customer_id::STRING),
            CUSTOMER_EMAIL STRING AS (RAW_DATA:customer.email::STRING),
            TOTAL_AMOUNT FLOAT AS (RAW_DATA:financials.total_amount::FLOAT),
            PAYMENT_METHOD STRING AS (RAW_DATA:payment.method::STRING),
            PAYMENT_STATUS STRING AS (RAW_DATA:payment.status::STRING),
            CHANNEL STRING AS (RAW_DATA:channel::STRING),
            LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        
        cursor.execute(create_table_sql)
        
        # List files to load
        file_patterns = []
        for file_info in new_files:
            # Extract date parts from S3 key for pattern matching
            parts = file_info['key'].split('/')
            if len(parts) >= 5:  # processed/kinesis/YYYY/MM/DD/HH/file.json
                date_pattern = '/'.join(parts[:5])  # processed/kinesis/YYYY/MM/DD/HH
                file_patterns.append(f"'{date_pattern}/*.json'")
        
        # Remove duplicates
        file_patterns = list(set(file_patterns))
        
        loaded_files = 0
        total_rows = 0
        
        for pattern in file_patterns:
            # Copy data from stage to table
            copy_sql = f"""
            COPY INTO {snowflake_database}.{snowflake_schema}.TRANSACTIONS_RAW (RAW_DATA)
            FROM @{snowflake_database}.{snowflake_schema}.S3_SALES_DATA_STAGE
            PATTERN = {pattern}
            FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
            ON_ERROR = 'CONTINUE'
            PURGE = FALSE;
            """
            
            cursor.execute(copy_sql)
            
            # Get copy results
            cursor.execute("SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME=>'TRANSACTIONS_RAW', START_TIME=> DATEADD(hour, -1, CURRENT_TIMESTAMP())));")
            copy_results = cursor.fetchall()
            
            for result in copy_results:
                loaded_files += result[6]  # FILES_LOADED
                total_rows += result[7]    # ROWS_LOADED
        
        # Create summary
        summary = {
            'loaded_files': loaded_files,
            'total_rows': total_rows,
            'file_patterns': file_patterns,
            'timestamp': datetime.now().isoformat()
        }
        
        ti.xcom_push(key='load_summary', value=summary)
        
        return f"Loaded {total_rows} rows from {loaded_files} files"
    
    except Exception as e:
        ti.xcom_push(key='error', value=str(e))
        raise Exception(f"Failed to load transactions to Snowflake: {str(e)}")

def transform_data_in_snowflake(**kwargs):
    """Transform raw data into structured tables"""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    ti = kwargs['ti']
    
    # Get configuration
    snowflake_database = Variable.get("SNOWFLAKE_DATABASE", "SALES_DB")
    snowflake_schema = Variable.get("SNOWFLAKE_SCHEMA", "ANALYTICS")
    snowflake_warehouse = Variable.get("SNOWFLAKE_WAREHOUSE", "SALES_WH")
    
    # Initialize Snowflake hook
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    try:
        connection = snowflake_hook.get_conn()
        cursor = connection.cursor()
        
        # Set warehouse and database
        cursor.execute(f"USE WAREHOUSE {snowflake_warehouse};")
        cursor.execute(f"USE DATABASE {snowflake_database};")
        
        # Create analytics schema if it doesn't exist
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {snowflake_schema};")
        cursor.execute(f"USE SCHEMA {snowflake_schema};")
        
        # Create fact table: SALES_FACT
        create_fact_table_sql = """
        CREATE OR REPLACE TABLE SALES_FACT (
            TRANSACTION_ID STRING PRIMARY KEY,
            ORDER_ID STRING,
            TRANSACTION_TIMESTAMP TIMESTAMP_NTZ,
            CUSTOMER_ID STRING,
            STORE_ID STRING,
            CAMPAIGN_ID STRING,
            SALES_REP_ID STRING,
            PRODUCT_ID STRING,
            QUANTITY INTEGER,
            UNIT_PRICE FLOAT,
            TOTAL_PRICE FLOAT,
            DISCOUNT_AMOUNT FLOAT,
            TAX_AMOUNT FLOAT,
            SHIPPING_COST FLOAT,
            FINAL_AMOUNT FLOAT,
            PAYMENT_METHOD STRING,
            PAYMENT_STATUS STRING,
            FRAUD_DETECTED BOOLEAN,
            FRAUD_SCORE INTEGER,
            CHANNEL STRING,
            FULFILLMENT_STATUS STRING,
            SEASON STRING,
            IS_WEEKEND BOOLEAN,
            LOAD_TIMESTAMP TIMESTAMP_NTZ
        );
        """
        cursor.execute(create_fact_table_sql)
        
        # Populate fact table from raw data
        insert_fact_sql = f"""
        INSERT INTO {snowflake_schema}.SALES_FACT
        SELECT
            r.RAW_DATA:transaction_id::STRING as TRANSACTION_ID,
            r.RAW_DATA:order_id::STRING as ORDER_ID,
            r.RAW_DATA:timestamp::TIMESTAMP_NTZ as TRANSACTION_TIMESTAMP,
            r.RAW_DATA:customer.customer_id::STRING as CUSTOMER_ID,
            r.RAW_DATA:store.store_id::STRING as STORE_ID,
            r.RAW_DATA:campaign.campaign_id::STRING as CAMPAIGN_ID,
            r.RAW_DATA:sales_rep.rep_id::STRING as SALES_REP_ID,
            p.value:product_id::STRING as PRODUCT_ID,
            p.value:quantity::INTEGER as QUANTITY,
            p.value:unit_price::FLOAT as UNIT_PRICE,
            p.value:total_price::FLOAT as TOTAL_PRICE,
            r.RAW_DATA:financials.discounts.total_discount::FLOAT as DISCOUNT_AMOUNT,
            r.RAW_DATA:financials.tax_amount::FLOAT as TAX_AMOUNT,
            r.RAW_DATA:financials.shipping_cost::FLOAT as SHIPPING_COST,
            r.RAW_DATA:financials.total_amount::FLOAT as FINAL_AMOUNT,
            r.RAW_DATA:payment.method::STRING as PAYMENT_METHOD,
            r.RAW_DATA:payment.status::STRING as PAYMENT_STATUS,
            r.RAW_DATA:payment.fraud_detected::BOOLEAN as FRAUD_DETECTED,
            r.RAW_DATA:payment.fraud_score::INTEGER as FRAUD_SCORE,
            r.RAW_DATA:channel::STRING as CHANNEL,
            r.RAW_DATA:fulfillment.status::STRING as FULFILLMENT_STATUS,
            r.RAW_DATA:metadata.season::STRING as SEASON,
            r.RAW_DATA:metadata.is_weekend::BOOLEAN as IS_WEEKEND,
            r.LOAD_TIMESTAMP
        FROM {snowflake_database}.RAW.TRANSACTIONS_RAW r,
        LATERAL FLATTEN(input => r.RAW_DATA:products) p;
        """
        
        cursor.execute(insert_fact_sql)
        fact_rows = cursor.rowcount
        
        # Create dimension tables
        
        # CUSTOMERS_DIM
        cursor.execute("""
        CREATE OR REPLACE TABLE CUSTOMERS_DIM AS
        SELECT DISTINCT
            r.RAW_DATA:customer.customer_id::STRING as CUSTOMER_ID,
            r.RAW_DATA:customer.first_name::STRING as FIRST_NAME,
            r.RAW_DATA:customer.last_name::STRING as LAST_NAME,
            r.RAW_DATA:customer.email::STRING as CUSTOMER_EMAIL,
            r.RAW_DATA:customer.segment::STRING as SEGMENT,
            r.RAW_DATA:customer.preferred_category::STRING as PREFERRED_CATEGORY,
            r.RAW_DATA:customer.customer_tier::STRING as TIER,
            r.RAW_DATA:customer.lifetime_value::INTEGER as LIFETIME_VALUE,
            CURRENT_TIMESTAMP() as LOAD_TIMESTAMP
        FROM SALES_DB.RAW.TRANSACTIONS_RAW r
        WHERE r.RAW_DATA:customer.customer_id::STRING IS NOT NULL;
        """)
        
        # PRODUCTS_DIM
        cursor.execute("""
        CREATE OR REPLACE TABLE PRODUCTS_DIM AS
        SELECT DISTINCT
            p.value:product_id::STRING as PRODUCT_ID,
            p.value:product_name::STRING as PRODUCT_NAME,
            p.value:category::STRING as CATEGORY,
            p.value:subcategory::STRING as SUBCATEGORY,
            p.value:brand::STRING as BRAND,
            p.value:sku::STRING as SKU,
            p.value:price::FLOAT as PRICE,
            CURRENT_TIMESTAMP() as LOAD_TIMESTAMP
        FROM SALES_DB.RAW.TRANSACTIONS_RAW r,
        LATERAL FLATTEN(input => r.RAW_DATA:products) p
        WHERE p.value:product_id::STRING IS NOT NULL;
        """)
        
        # Create view for reporting
        create_reporting_view_sql = """
        CREATE OR REPLACE VIEW SALES_REPORTING_VIEW AS
        SELECT
            f.TRANSACTION_TIMESTAMP,
            DATE(f.TRANSACTION_TIMESTAMP) as TRANSACTION_DATE,
            EXTRACT(HOUR FROM f.TRANSACTION_TIMESTAMP) as TRANSACTION_HOUR,
            c.FIRST_NAME || ' ' || c.LAST_NAME as CUSTOMER_NAME,
            c.SEGMENT as CUSTOMER_SEGMENT,
            c.TIER as CUSTOMER_TIER,
            p.PRODUCT_NAME,
            p.CATEGORY as PRODUCT_CATEGORY,
            p.BRAND as PRODUCT_BRAND,
            f.QUANTITY,
            f.UNIT_PRICE,
            f.TOTAL_PRICE,
            f.DISCOUNT_AMOUNT,
            f.TAX_AMOUNT,
            f.SHIPPING_COST,
            f.FINAL_AMOUNT,
            f.PAYMENT_METHOD,
            f.PAYMENT_STATUS,
            f.CHANNEL,
            f.FRAUD_DETECTED,
            f.SEASON,
            f.IS_WEEKEND
        FROM SALES_FACT f
        LEFT JOIN CUSTOMERS_DIM c ON f.CUSTOMER_ID = c.CUSTOMER_ID
        LEFT JOIN PRODUCTS_DIM p ON f.PRODUCT_ID = p.PRODUCT_ID;
        """
        cursor.execute(create_reporting_view_sql)
        
        # Get transformation metrics
        cursor.execute(f"SELECT COUNT(*) FROM {snowflake_schema}.SALES_FACT;")
        total_fact_rows = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM CUSTOMERS_DIM;")
        customer_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM PRODUCTS_DIM;")
        product_count = cursor.fetchone()[0]
        
        transformation_summary = {
            'fact_rows_inserted': fact_rows,
            'total_fact_rows': total_fact_rows,
            'customer_dim_count': customer_count,
            'product_dim_count': product_count,
            'timestamp': datetime.now().isoformat()
        }
        
        ti.xcom_push(key='transformation_summary', value=transformation_summary)
        
        return f"Transformation complete: {fact_rows} new fact rows, {customer_count} customers, {product_count} products"
    
    except Exception as e:
        ti.xcom_push(key='error', value=str(e))
        raise Exception(f"Failed to transform data in Snowflake: {str(e)}")

def verify_snowflake_data(**kwargs):
    """Verify data quality in Snowflake"""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    ti = kwargs['ti']
    
    # Get configuration
    snowflake_database = Variable.get("SNOWFLAKE_DATABASE", "SALES_DB")
    snowflake_schema = Variable.get("SNOWFLAKE_SCHEMA", "ANALYTICS")
    
    # Initialize Snowflake hook
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    try:
        connection = snowflake_hook.get_conn()
        cursor = connection.cursor()
        
        cursor.execute(f"USE SCHEMA {snowflake_database}.{snowflake_schema};")
        
        # Run data quality checks
        checks = []
        
        # Check 1: Fact table has data
        cursor.execute("SELECT COUNT(*) FROM SALES_FACT;")
        fact_count = cursor.fetchone()[0]
        checks.append({'check': 'fact_table_has_data', 'result': fact_count > 0, 'count': fact_count})
        
        # Check 2: No null transaction IDs
        cursor.execute("SELECT COUNT(*) FROM SALES_FACT WHERE TRANSACTION_ID IS NULL;")
        null_ids = cursor.fetchone()[0]
        checks.append({'check': 'no_null_transaction_ids', 'result': null_ids == 0, 'count': null_ids})
        
        # Check 3: Valid amounts (positive)
        cursor.execute("SELECT COUNT(*) FROM SALES_FACT WHERE FINAL_AMOUNT <= 0;")
        invalid_amounts = cursor.fetchone()[0]
        checks.append({'check': 'valid_amounts', 'result': invalid_amounts == 0, 'count': invalid_amounts})
        
        # Check 4: Recent data
        cursor.execute("SELECT MAX(TRANSACTION_TIMESTAMP) FROM SALES_FACT;")
        latest_timestamp = cursor.fetchone()[0]
        if latest_timestamp:
            hours_since_latest = (datetime.now() - latest_timestamp).total_seconds() / 3600
            checks.append({
                'check': 'recent_data',
                'result': hours_since_latest < (24 * 365),
                'hours_since_latest': hours_since_latest
            })
        
        # Check 5: Referential integrity
        cursor.execute("""
        SELECT COUNT(*) 
        FROM SALES_FACT f 
        LEFT JOIN CUSTOMERS_DIM c ON f.CUSTOMER_ID = c.CUSTOMER_ID 
        WHERE c.CUSTOMER_ID IS NULL AND f.CUSTOMER_ID IS NOT NULL;
        """)
        orphaned_customers = cursor.fetchone()[0]
        checks.append({'check': 'customer_referential_integrity', 'result': orphaned_customers == 0, 'count': orphaned_customers})
        
        # Calculate overall status
        all_passed = all(check['result'] for check in checks)
        
        verification = {
            'checks': checks,
            'all_passed': all_passed,
            'timestamp': datetime.now().isoformat(),
            'latest_data_timestamp': latest_timestamp.isoformat() if latest_timestamp else None
        }
        
        ti.xcom_push(key='data_verification', value=verification)
        
        if not all_passed:
            failed_checks = [check['check'] for check in checks if not check['result']]
            raise Exception(f"Data verification failed for checks: {failed_checks}")
        
        return f"Data verification passed: {len([c for c in checks if c['result']])}/{len(checks)} checks"
    
    except Exception as e:
        ti.xcom_push(key='error', value=str(e))
        raise Exception(f"Failed to verify Snowflake data: {str(e)}")

# Define tasks
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

check_s3_task = BranchPythonOperator(
    task_id='check_s3_new_data',
    python_callable=check_s3_new_data,
    dag=dag,
)

create_stage_task = PythonOperator(
    task_id='create_snowflake_stage',
    python_callable=create_snowflake_stage,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_transactions_to_snowflake',
    python_callable=load_transactions_to_snowflake,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data_in_snowflake',
    python_callable=transform_data_in_snowflake,
    dag=dag,
)

verify_data_task = PythonOperator(
    task_id='verify_snowflake_data',
    python_callable=verify_snowflake_data,
    dag=dag,
)

no_new_data_task = DummyOperator(
    task_id='no_new_data',
    dag=dag,
)

# Set up task dependencies
start >> check_s3_task

# Branch paths
check_s3_task >> create_stage_task
check_s3_task >> no_new_data_task

create_stage_task >> load_data_task >> transform_data_task >> verify_data_task >> end
no_new_data_task >> end


