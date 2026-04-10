"""
DAG for automating data generation (Phase 2 scripts)
Generates master data and transactions on a schedule
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import os
import sys

# Add project path to sys.path
sys.path.insert(0, '/opt/airflow/phase_2_data_generation')

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
    'data_generation_pipeline',
    default_args=default_args,
    description='Generate master data and transactions',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['generation', 'master-data', 'transactions'],
    max_active_runs=1,
)

def generate_master_data(**kwargs):
    """Generate master data using Phase 2 script"""
    import subprocess
    import json
    
    ti = kwargs['ti']
    
    # Get configuration from Airflow Variables
    output_dir = Variable.get("DATA_GEN_OUTPUT_DIR", "/opt/airflow/data")
    seed = int(Variable.get("DATA_GEN_SEED", "42"))
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(f"{output_dir}/logs", exist_ok=True)
    
    # Run master data generation
    script_path = '/opt/airflow/phase_2_data_generation/generate_master_data.py'
    
    cmd = [
        'python', script_path,
        '--output-dir', output_dir,
        '--seed', str(seed)
    ]
    
    # Check if S3 upload is enabled
    if Variable.get("UPLOAD_TO_S3", "false").lower() == "true":
        cmd.extend(['--upload-to-s3', '--s3-bucket', Variable.get("S3_RAW_BUCKET")])
    
    # Execute command
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        error_msg = f"Master data generation failed: {result.stderr}"
        ti.xcom_push(key='error', value=error_msg)
        raise Exception(error_msg)
    
    # Parse output files
    metadata_file = f"{output_dir}/metadata.yaml"
    if os.path.exists(metadata_file):
        import yaml
        with open(metadata_file, 'r') as f:
            metadata = yaml.safe_load(f)
        
        ti.xcom_push(key='master_data_generated', value=True)
        ti.xcom_push(key='metadata', value=metadata)
        ti.xcom_push(key='output_dir', value=output_dir)
        
        return f"Master data generated: {metadata}"
    
    return "Master data generation completed"

def generate_transactions(**kwargs):
    """Generate transaction data"""
    import subprocess
    import json
    from pathlib import Path
    
    ti = kwargs['ti']
    
    # Get configuration
    output_dir = ti.xcom_pull(task_ids='generate_master', key='output_dir')
    if not output_dir:
        output_dir = Variable.get("DATA_GEN_OUTPUT_DIR", "/opt/airflow/data")
    
    count = int(Variable.get("TRANSACTION_COUNT", "10000"))
    seed = int(Variable.get("DATA_GEN_SEED", "42"))
    master_data_dir = output_dir
    
    # Run transaction generation
    script_path = '/opt/airflow/phase_2_data_generation/generate_transactions.py'
    
    cmd = [
        'python', script_path,
        '--mode', 'batch',
        '--count', str(count),
        '--output-dir', output_dir,
        '--master-data-dir', master_data_dir,
        '--seed', str(seed),
        '--format', 'json'
    ]
    
    # Execute command
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        error_msg = f"Transaction generation failed: {result.stderr}"
        ti.xcom_push(key='error', value=error_msg)
        raise Exception(error_msg)
    
    # Find the generated transaction file
    output_path = Path(output_dir)
    transaction_files = list(output_path.glob('transactions_*.json'))
    
    if not transaction_files:
        raise Exception("No transaction files generated")
    
    latest_file = max(transaction_files, key=lambda x: x.stat().st_mtime)
    
    # Get file stats
    file_size = latest_file.stat().st_size
    
    # Parse a sample to get count
    with open(latest_file, 'r') as f:
        transactions = json.load(f)
    
    ti.xcom_push(key='transaction_file', value=str(latest_file))
    ti.xcom_push(key='transaction_count', value=len(transactions))
    ti.xcom_push(key='file_size_bytes', value=file_size)
    
    return f"Generated {len(transactions)} transactions in {latest_file.name} ({file_size/1024/1024:.2f} MB)"

def stream_to_kinesis(**kwargs):
    """Stream generated transactions to Kinesis"""
    import subprocess
    
    ti = kwargs['ti']
    
    # Get transaction file from previous task
    transaction_file = ti.xcom_pull(task_ids='generate_transactions', key='transaction_file')
    if not transaction_file:
        raise Exception("No transaction file found")
    
    # Get configuration
    stream_name = Variable.get("KINESIS_STREAM_NAME", "sales-data-stream-dev")
    records_per_sec = int(Variable.get("KINESIS_RECORDS_PER_SEC", "100"))
    duration = int(Variable.get("KINESIS_STREAM_DURATION", "5"))
    
    # Run streaming script
    script_path = '/opt/airflow/phase_2_data_generation/stream_to_kinesis.py'
    
    cmd = [
        'python', script_path,
        '--stream-name', stream_name,
        '--mode', 'file',
        '--input-file', transaction_file,
        '--records-per-second', str(records_per_sec),
        '--duration', str(duration),
        '--region', Variable.get("AWS_DEFAULT_REGION", "us-east-1"),
        '--batch-size', '500',
        '--max-retries', '3'
    ]
    
    # Execute command
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        error_msg = f"Kinesis streaming failed: {result.stderr}"
        ti.xcom_push(key='error', value=error_msg)
        raise Exception(error_msg)
    
    # Parse output for metrics
    lines = result.stdout.split('\n')
    metrics_line = next((line for line in lines if 'Metrics - Sent:' in line), "")
    
    ti.xcom_push(key='streaming_completed', value=True)
    ti.xcom_push(key='streaming_metrics', value=metrics_line)
    
    return f"Streaming completed: {metrics_line}"

def validate_data_generation(**kwargs):
    """Validate generated data"""
    import json
    from pathlib import Path
    
    ti = kwargs['ti']
    
    output_dir = ti.xcom_pull(task_ids='generate_master', key='output_dir')
    if not output_dir:
        output_dir = Variable.get("DATA_GEN_OUTPUT_DIR", "/opt/airflow/data")
    
    validation_results = {
        "master_data_valid": False,
        "transactions_valid": False,
        "files_present": []
    }
    
    # Check master data files
    required_master_files = [
        f"{output_dir}/products.json",
        f"{output_dir}/customers.json",
        f"{output_dir}/sales_reps.json",
        f"{output_dir}/campaigns.json",
        f"{output_dir}/stores.json"
    ]
    
    for file_path in required_master_files:
        if Path(file_path).exists():
            # Validate JSON structure
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                if isinstance(data, list) and len(data) > 0:
                    validation_results["files_present"].append(Path(file_path).name)
            except:
                pass
    
    if len(validation_results["files_present"]) >= 5:
        validation_results["master_data_valid"] = True
    
    # Check transaction files
    transaction_file = ti.xcom_pull(task_ids='generate_transactions', key='transaction_file')
    if transaction_file and Path(transaction_file).exists():
        try:
            with open(transaction_file, 'r') as f:
                transactions = json.load(f)
            if isinstance(transactions, list) and len(transactions) > 0:
                validation_results["transactions_valid"] = True
        except:
            pass
    
    ti.xcom_push(key='validation_results', value=validation_results)
    
    # Raise error if validation fails
    if not validation_results["master_data_valid"]:
        raise Exception(f"Master data validation failed. Files found: {validation_results['files_present']}")
    
    if not validation_results["transactions_valid"]:
        raise Exception("Transaction data validation failed")
    
    return f"Validation passed: {validation_results}"

def send_success_notification(**kwargs):
    """Send success notification"""
    ti = kwargs['ti']
    
    # Get metrics from previous tasks
    metadata = ti.xcom_pull(task_ids='generate_master', key='metadata')
    transaction_count = ti.xcom_pull(task_ids='generate_transactions', key='transaction_count')
    file_size = ti.xcom_pull(task_ids='generate_transactions', key='file_size_bytes')
    streaming_metrics = ti.xcom_pull(task_ids='stream_to_kinesis', key='streaming_metrics')
    
    # In production, this would send email/Slack notification
    notification_message = f"""
    ✅ Data Generation Pipeline Completed Successfully
    
    Summary:
    - Master Data: {metadata['datasets'] if metadata else 'Generated'}
    - Transactions: {transaction_count} records
    - File Size: {file_size/1024/1024:.2f} MB
    - Streaming: {streaming_metrics}
    
    Timestamp: {datetime.now().isoformat()}
    """
    
    print(notification_message)
    
    # Log to Airflow logs
    from airflow.utils.log.logging_mixin import LoggingMixin
    LoggingMixin().log.info(notification_message)
    
    return "Success notification sent"

def send_failure_notification(context):
    """Send failure notification"""
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    exception = context.get('exception', 'Unknown error')
    
    error_message = f"""
    ❌ Data Generation Pipeline Failed
    
    Details:
    - DAG: {dag_id}
    - Failed Task: {task_id}
    - Execution Time: {execution_date}
    - Error: {str(exception)}
    
    Please check Airflow logs for details.
    """
    
    print(error_message)
    
    # In production, send email/Slack
    # from airflow.utils.email import send_email
    # send_email(to=['team@sales-platform.com'], subject=f'Data Generation Failed: {dag_id}', html_content=error_message)

# Define tasks
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

generate_master_task = PythonOperator(
    task_id='generate_master',
    python_callable=generate_master_data,
    dag=dag,
)

generate_transactions_task = PythonOperator(
    task_id='generate_transactions',
    python_callable=generate_transactions,
    dag=dag,
)

stream_to_kinesis_task = PythonOperator(
    task_id='stream_to_kinesis',
    python_callable=stream_to_kinesis,
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data_generation,
    dag=dag,
)

send_success_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag,
    trigger_rule='all_success',
)

# Set up task dependencies
start >> generate_master_task >> generate_transactions_task
generate_transactions_task >> stream_to_kinesis_task >> validate_data_task
validate_data_task >> send_success_task >> end

# Add error handling
generate_master_task.on_failure_callback = send_failure_notification
generate_transactions_task.on_failure_callback = send_failure_notification
stream_to_kinesis_task.on_failure_callback = send_failure_notification
validate_data_task.on_failure_callback = send_failure_notification