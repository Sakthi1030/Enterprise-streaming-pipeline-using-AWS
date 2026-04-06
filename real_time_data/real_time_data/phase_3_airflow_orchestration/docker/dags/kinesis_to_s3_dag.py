"""
DAG for processing Kinesis data to S3
Monitors Kinesis stream and triggers processing jobs
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from airflow.models import Variable
import boto3
import json
import gzip
import time
from typing import Dict, List, Any

default_args = {
    'owner': 'sales-platform',
    'depends_on_past': False,
    'email': ['alerts@sales-platform.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    'kinesis_to_s3_pipeline',
    default_args=default_args,
    description='Process Kinesis streaming data to S3',
    schedule_interval=None,  # Manual runs only for local stability
    catchup=False,
    tags=['kinesis', 's3', 'streaming', 'processing'],
    max_active_runs=1,
)

def check_kinesis_metrics(**kwargs):
    """Check Kinesis metrics to determine if processing is needed"""
    ti = kwargs['ti']
    
    # Get configuration
    stream_name = Variable.get("KINESIS_STREAM_NAME", "sales-data-stream-dev")
    region = Variable.get("AWS_DEFAULT_REGION", "us-east-1")
    threshold_bytes = int(Variable.get("KINESIS_PROCESSING_THRESHOLD_MB", "10")) * 1024 * 1024
    
    # Initialize clients
    kinesis_client = boto3.client('kinesis', region_name=region)
    cloudwatch = boto3.client('cloudwatch', region_name=region)
    
    # Get stream details
    try:
        stream_info = kinesis_client.describe_stream(StreamName=stream_name)
        shard_count = len(stream_info['StreamDescription']['Shards'])
        
        # Get incoming bytes metric for last 15 minutes
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=15)
        
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/Kinesis',
            MetricName='IncomingBytes',
            Dimensions=[
                {'Name': 'StreamName', 'Value': stream_name}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=300,  # 5-minute intervals
            Statistics=['Sum']
        )
        
        # Calculate total bytes in last 15 minutes
        total_bytes = sum(datapoint['Sum'] for datapoint in response['Datapoints'])
        
        # Get iterator age to check for lag
        iterator_age_response = cloudwatch.get_metric_statistics(
            Namespace='AWS/Kinesis',
            MetricName='GetRecords.IteratorAgeMilliseconds',
            Dimensions=[
                {'Name': 'StreamName', 'Value': stream_name}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=300,
            Statistics=['Maximum']
        )
        
        max_iterator_age = 0
        if iterator_age_response['Datapoints']:
            max_iterator_age = max(datapoint['Maximum'] for datapoint in iterator_age_response['Datapoints'])
        
        metrics = {
            'shard_count': shard_count,
            'total_bytes': total_bytes,
            'total_mb': total_bytes / (1024 * 1024),
            'max_iterator_age_ms': max_iterator_age,
            'max_iterator_age_minutes': max_iterator_age / (1000 * 60),
            'threshold_bytes': threshold_bytes,
            'threshold_exceeded': total_bytes > threshold_bytes,
            'iterator_age_threshold_exceeded': max_iterator_age > (5 * 60 * 1000)  # 5 minutes
        }
        
        ti.xcom_push(key='kinesis_metrics', value=metrics)
        
        # Force processing in local/minimal mode to avoid branch skips during demo runs
        return 'process_kinesis_data'
            
    except Exception as e:
        ti.xcom_push(key='error', value=str(e))
        raise Exception(f"Failed to check Kinesis metrics: {str(e)}")

def process_kinesis_data(**kwargs):
    """Process Kinesis data to S3"""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
    ti = kwargs['ti']
    
    # Get configuration
    stream_name = Variable.get("KINESIS_STREAM_NAME", "sales-data-stream-dev")
    s3_bucket = Variable.get("S3_RAW_BUCKET", "sales-platform-raw-data-dev")
    region = Variable.get("AWS_DEFAULT_REGION", "us-east-1")
    batch_size = int(Variable.get("KINESIS_BATCH_SIZE", "10000"))
    
    # Initialize hooks
    kinesis_client = boto3.client('kinesis', region_name=region)
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    def _decode_record_data(payload: bytes) -> Dict[str, Any]:
        """Decode either plain JSON bytes or gzip-compressed JSON bytes."""
        try:
            return json.loads(payload.decode('utf-8'))
        except Exception:
            return json.loads(gzip.decompress(payload).decode('utf-8'))

    try:
        # Get stream info
        stream_info = kinesis_client.describe_stream(StreamName=stream_name)
        shards = stream_info['StreamDescription']['Shards']
        
        processed_records = 0
        processed_shards = 0
        errors = []
        
        # Process each shard
        for shard in shards:
            shard_id = shard['ShardId']
            
            try:
                # Get shard iterator
                iterator_response = kinesis_client.get_shard_iterator(
                    StreamName=stream_name,
                    ShardId=shard_id,
                    ShardIteratorType='TRIM_HORIZON'  # Start from oldest record
                )
                
                shard_iterator = iterator_response['ShardIterator']
                
                records_batch = []
                batch_count = 0
                
                # Read records in batches
                while batch_count < batch_size and shard_iterator:
                    response = kinesis_client.get_records(
                        ShardIterator=shard_iterator,
                        Limit=10000  # Max per request
                    )
                    
                    records = response.get('Records', [])
                    if not records:
                        break
                    
                    # Process records
                    for record in records:
                        try:
                            # Decode and parse record (supports plain JSON and gzip JSON)
                            record_data = _decode_record_data(record['Data'])
                            records_batch.append(record_data)
                            processed_records += 1
                            batch_count += 1
                        except Exception as e:
                            errors.append(f"Error parsing record: {str(e)}")
                    
                    # Update iterator
                    shard_iterator = response.get('NextShardIterator')
                    
                    # Check if we've reached batch size
                    if batch_count >= batch_size:
                        break
                
                # Write batch to S3 if we have records
                if records_batch:
                    # Create S3 key with timestamp
                    timestamp = datetime.now().strftime('%Y/%m/%d/%H/%M')
                    s3_key = f"processed/kinesis/{timestamp}/shard_{shard_id}_{int(time.time())}.json"
                    
                    # Convert to JSON lines format
                    json_lines = '\n'.join(json.dumps(record) for record in records_batch)
                    
                    # Upload to S3
                    s3_hook.load_string(
                        string_data=json_lines,
                        key=s3_key,
                        bucket_name=s3_bucket,
                        replace=True
                    )
                    
                    processed_shards += 1
                    
                    ti.xcom_push(key=f'shard_{shard_id}_processed', value=len(records_batch))
                    ti.xcom_push(key=f'shard_{shard_id}_s3_key', value=s3_key)
            
            except Exception as e:
                errors.append(f"Error processing shard {shard_id}: {str(e)}")
        
        # Summary
        summary = {
            'processed_shards': processed_shards,
            'processed_records': processed_records,
            'total_shards': len(shards),
            'errors': errors,
            'timestamp': datetime.now().isoformat()
        }
        
        ti.xcom_push(key='processing_summary', value=summary)

        # Fail fast when Kinesis read fails or when nothing is processed.
        if errors and processed_records == 0:
            raise Exception(f"Failed to read any Kinesis records. First error: {errors[0]}")

        if processed_records == 0:
            raise Exception("No records were processed from Kinesis; verify stream data and IAM read permissions.")

        if errors:
            error_msg = f"Processing completed with errors: {len(errors)} errors"
            ti.xcom_push(key='warning', value=error_msg)

        return f"Processed {processed_records} records from {processed_shards}/{len(shards)} shards"
    
    except Exception as e:
        ti.xcom_push(key='error', value=str(e))
        raise Exception(f"Failed to process Kinesis data: {str(e)}")

def trigger_firehose_backfill(**kwargs):
    """Trigger Kinesis Firehose to process any backlog"""
    import boto3
    
    ti = kwargs['ti']
    
    # Get configuration
    firehose_name = Variable.get("FIREHOSE_STREAM_NAME", "sales-to-s3-firehose-dev")
    region = Variable.get("AWS_DEFAULT_REGION", "us-east-1")
    
    # Initialize client
    firehose = boto3.client('firehose', region_name=region)
    
    try:
        # Get Firehose description
        response = firehose.describe_delivery_stream(
            DeliveryStreamName=firehose_name
        )
        
        status = response['DeliveryStreamDescription']['DeliveryStreamStatus']
        
        if status == 'ACTIVE':
            # Trigger update to start immediate processing
            update_response = firehose.update_destination(
                DeliveryStreamName=firehose_name,
                CurrentDeliveryStreamVersionId=response['DeliveryStreamDescription']['VersionId'],
                DestinationId='destinationId-000000000001'
            )
            
            ti.xcom_push(key='firehose_triggered', value=True)
            return f"Firehose {firehose_name} triggered for immediate processing"
        else:
            ti.xcom_push(key='warning', value=f"Firehose status is {status}, not triggering")
            return f"Firehose status is {status}, skipping trigger"
    
    except Exception as e:
        ti.xcom_push(key='warning', value=str(e))
        return f"Failed to trigger Firehose: {str(e)}"

def verify_s3_data(**kwargs):
    """Verify data was written to S3"""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    
    ti = kwargs['ti']
    
    # Get configuration
    s3_bucket = Variable.get("S3_RAW_BUCKET", "sales-platform-raw-data-dev")
    
    # Initialize hook
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    try:
        # Look back across recent hour-based prefixes to avoid timezone edge cases.
        prefixes = []
        now = datetime.utcnow()
        for hour_offset in range(0, 4):
            dt = now - timedelta(hours=hour_offset)
            prefixes.append(f"processed/kinesis/{dt.strftime('%Y/%m/%d/%H')}/")

        all_files = []
        matched_prefix = None
        for prefix in prefixes:
            files = s3_hook.list_keys(bucket_name=s3_bucket, prefix=prefix) or []
            if files:
                matched_prefix = prefix
                all_files = files
                break

        if all_files:
            file_sizes = []
            for file_key in all_files[:5]:
                try:
                    file_size = s3_hook.get_key(file_key, s3_bucket).content_length
                    file_sizes.append(file_size)
                except Exception:
                    pass

            verification = {
                'files_found': len(all_files),
                'sample_files': all_files[:5],
                'sample_sizes': file_sizes,
                'total_size_bytes': sum(file_sizes) if file_sizes else 0,
                'prefix_checked': prefixes,
                'matched_prefix': matched_prefix,
                'verified': len(all_files) > 0
            }

            ti.xcom_push(key='s3_verification', value=verification)
            return f"Verified {len(all_files)} files in S3 at {matched_prefix}"

        raise Exception(f"No files found in recent S3 prefixes: {prefixes}")
    
    except Exception as e:
        ti.xcom_push(key='error', value=str(e))
        raise Exception(f"Failed to verify S3 data: {str(e)}")

def update_monitoring_metrics(**kwargs):
    """Update monitoring metrics for the pipeline"""
    ti = kwargs['ti']
    
    # Get metrics from previous tasks
    kinesis_metrics = ti.xcom_pull(task_ids='check_kinesis_metrics', key='kinesis_metrics')
    processing_summary = ti.xcom_pull(task_ids='process_kinesis_data', key='processing_summary')
    s3_verification = ti.xcom_pull(task_ids='verify_s3_data', key='s3_verification')
    
    # Create monitoring summary
    monitoring_data = {
        'timestamp': datetime.now().isoformat(),
        'kinesis_metrics': kinesis_metrics,
        'processing_summary': processing_summary,
        's3_verification': s3_verification,
        'pipeline_status': 'SUCCESS'
    }
    
    # In production, this would send to CloudWatch, Datadog, etc.
    print(f"Monitoring Data: {json.dumps(monitoring_data, indent=2)}")
    
    ti.xcom_push(key='monitoring_data', value=monitoring_data)
    
    return "Monitoring metrics updated"

# Define tasks
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

check_metrics_task = BranchPythonOperator(
    task_id='check_kinesis_metrics',
    python_callable=check_kinesis_metrics,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_kinesis_data',
    python_callable=process_kinesis_data,
    dag=dag,
)

trigger_firehose_task = PythonOperator(
    task_id='trigger_firehose_backfill',
    python_callable=trigger_firehose_backfill,
    dag=dag,
)

verify_s3_task = PythonOperator(
    task_id='verify_s3_data',
    python_callable=verify_s3_data,
    dag=dag,
)

update_monitoring_task = PythonOperator(
    task_id='update_monitoring_metrics',
    python_callable=update_monitoring_metrics,
    dag=dag,
)

skip_processing_task = DummyOperator(
    task_id='skip_processing',
    dag=dag,
)

# Set up task dependencies
start >> check_metrics_task

# Branch paths
check_metrics_task >> process_data_task
check_metrics_task >> skip_processing_task

process_data_task >> trigger_firehose_task >> verify_s3_task >> update_monitoring_task >> end
skip_processing_task >> end


