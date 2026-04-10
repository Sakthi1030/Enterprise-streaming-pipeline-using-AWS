"""
Custom Airflow Operators for Sales Platform
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Optional, Dict, Any, List
import boto3
import json
import time
from datetime import datetime
import snowflake.connector

class KinesisBatchProcessorOperator(BaseOperator):
    """
    Custom operator for processing Kinesis data in batches
    """
    
    @apply_defaults
    def __init__(
        self,
        stream_name: str,
        shard_count: int = 2,
        batch_size: int = 10000,
        aws_conn_id: str = 'aws_default',
        s3_bucket: Optional[str] = None,
        s3_prefix: Optional[str] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.stream_name = stream_name
        self.shard_count = shard_count
        self.batch_size = batch_size
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        
    def execute(self, context):
        from airflow.providers.amazon.aws.hooks.kinesis import KinesisHook
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        self.log.info(f"Starting Kinesis batch processing for stream: {self.stream_name}")
        
        # Initialize hooks
        kinesis_hook = KinesisHook(aws_conn_id=self.aws_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id) if self.s3_bucket else None
        
        # Get stream details
        stream_info = kinesis_hook.conn.describe_stream(StreamName=self.stream_name)
        shards = stream_info['StreamDescription']['Shards']
        
        total_processed = 0
        processing_summary = {}
        
        for shard in shards:
            shard_id = shard['ShardId']
            self.log.info(f"Processing shard: {shard_id}")
            
            try:
                # Get shard iterator
                iterator_response = kinesis_hook.conn.get_shard_iterator(
                    StreamName=self.stream_name,
                    ShardId=shard_id,
                    ShardIteratorType='TRIM_HORIZON'
                )
                
                shard_iterator = iterator_response['ShardIterator']
                records_batch = []
                records_processed = 0
                
                # Read records
                while records_processed < self.batch_size and shard_iterator:
                    response = kinesis_hook.conn.get_records(
                        ShardIterator=shard_iterator,
                        Limit=10000
                    )
                    
                    records = response.get('Records', [])
                    if not records:
                        break
                    
                    for record in records:
                        try:
                            record_data = json.loads(record['Data'].decode('utf-8'))
                            records_batch.append(record_data)
                            records_processed += 1
                        except Exception as e:
                            self.log.warning(f"Error parsing record: {str(e)}")
                    
                    shard_iterator = response.get('NextShardIterator')
                    
                    if records_processed >= self.batch_size:
                        break
                
                # Process batch
                if records_batch:
                    if s3_hook and self.s3_bucket:
                        # Save to S3
                        timestamp = datetime.now().strftime('%Y/%m/%d/%H/%M')
                        s3_key = f"{self.s3_prefix}/{timestamp}/shard_{shard_id}_{int(time.time())}.json"
                        
                        json_lines = '\n'.join(json.dumps(record) for record in records_batch)
                        s3_hook.load_string(
                            string_data=json_lines,
                            key=s3_key,
                            bucket_name=self.s3_bucket,
                            replace=True
                        )
                        
                        self.log.info(f"Saved {len(records_batch)} records to S3: {s3_key}")
                    
                    total_processed += len(records_batch)
                    processing_summary[shard_id] = {
                        'records_processed': len(records_batch),
                        's3_key': s3_key if s3_hook else None
                    }
            
            except Exception as e:
                self.log.error(f"Error processing shard {shard_id}: {str(e)}")
                processing_summary[shard_id] = {
                    'error': str(e),
                    'records_processed': 0
                }
        
        # Push results to XCom
        context['ti'].xcom_push(key='kinesis_processing_summary', value={
            'total_records_processed': total_processed,
            'shard_summary': processing_summary,
            'timestamp': datetime.now().isoformat()
        })
        
        self.log.info(f"Kinesis batch processing completed: {total_processed} records processed")
        return total_processed

class SnowflakeDataValidatorOperator(BaseOperator):
    """
    Custom operator for validating data in Snowflake
    """
    
    @apply_defaults
    def __init__(
        self,
        snowflake_conn_id: str = 'snowflake_default',
        database: str = None,
        schema: str = None,
        table: str = None,
        validation_rules: Dict[str, Any] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema
        self.table = table
        self.validation_rules = validation_rules or {}
        
    def execute(self, context):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        
        self.log.info(f"Starting data validation for {self.database}.{self.schema}.{self.table}")
        
        # Initialize hook
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        
        try:
            connection = snowflake_hook.get_conn()
            cursor = connection.cursor()
            
            # Set context
            if self.database:
                cursor.execute(f"USE DATABASE {self.database};")
            if self.schema:
                cursor.execute(f"USE SCHEMA {self.schema};")
            
            validation_results = {}
            
            # Run default validations
            default_rules = {
                'row_count': f"SELECT COUNT(*) FROM {self.table}",
                'null_check': f"SELECT COUNT(*) FROM {self.table} WHERE {self._get_null_check_columns()}",
                'duplicate_check': f"SELECT COUNT(*) FROM (SELECT *, COUNT(*) as cnt FROM {self.table} GROUP BY 1 HAVING cnt > 1)"
            }
            
            # Merge with custom rules
            all_rules = {**default_rules, **self.validation_rules}
            
            for rule_name, rule_sql in all_rules.items():
                try:
                    cursor.execute(rule_sql)
                    result = cursor.fetchone()
                    validation_results[rule_name] = {
                        'sql': rule_sql,
                        'result': result[0] if result else None,
                        'passed': self._evaluate_rule(rule_name, result)
                    }
                except Exception as e:
                    validation_results[rule_name] = {
                        'sql': rule_sql,
                        'error': str(e),
                        'passed': False
                    }
            
            # Calculate overall status
            all_passed = all(result['passed'] for result in validation_results.values() 
                           if 'passed' in result)
            
            summary = {
                'table': f"{self.database}.{self.schema}.{self.table}",
                'validation_results': validation_results,
                'all_passed': all_passed,
                'timestamp': datetime.now().isoformat()
            }
            
            # Push to XCom
            context['ti'].xcom_push(key='data_validation_summary', value=summary)
            
            if not all_passed:
                failed_rules = [name for name, result in validation_results.items() 
                              if not result.get('passed', True)]
                error_msg = f"Data validation failed for rules: {', '.join(failed_rules)}"
                self.log.error(error_msg)
                raise Exception(error_msg)
            
            self.log.info(f"Data validation passed: {summary}")
            return summary
        
        except Exception as e:
            self.log.error(f"Data validation failed: {str(e)}")
            raise
    
    def _get_null_check_columns(self):
        """Get columns for null check based on table name"""
        # This is a simplified implementation
        # In production, you'd dynamically get primary key or important columns
        if 'TRANSACTIONS' in self.table.upper():
            return "TRANSACTION_ID IS NULL"
        elif 'CUSTOMERS' in self.table.upper():
            return "CUSTOMER_ID IS NULL"
        elif 'PRODUCTS' in self.table.upper():
            return "PRODUCT_ID IS NULL"
        else:
            return "1=0"  # No null check
    
    def _evaluate_rule(self, rule_name, result):
        """Evaluate if rule passed"""
        if rule_name == 'row_count':
            return result[0] > 0 if result else False
        elif rule_name == 'null_check':
            return result[0] == 0 if result else False
        elif rule_name == 'duplicate_check':
            return result[0] == 0 if result else False
        return True

class AITriggerOperator(BaseOperator):
    """
    Custom operator for triggering AI/ML processes
    """
    
    @apply_defaults
    def __init__(
        self,
        ai_service_endpoint: str,
        process_type: str,
        parameters: Dict[str, Any] = None,
        timeout_seconds: int = 300,
        retry_count: int = 3,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.ai_service_endpoint = ai_service_endpoint
        self.process_type = process_type
        self.parameters = parameters or {}
        self.timeout_seconds = timeout_seconds
        self.retry_count = retry_count
        
    def execute(self, context):
        import requests
        
        self.log.info(f"Triggering AI process: {self.process_type}")
        
        # Prepare payload
        payload = {
            "process_type": self.process_type,
            "parameters": self.parameters,
            "metadata": {
                "dag_id": context['dag'].dag_id,
                "task_id": self.task_id,
                "execution_date": context['execution_date'].isoformat(),
                "dag_run_id": context['dag_run'].run_id
            }
        }
        
        # Make request with retries
        for attempt in range(self.retry_count):
            try:
                response = requests.post(
                    f"{self.ai_service_endpoint}/trigger",
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=self.timeout_seconds
                )
                
                response.raise_for_status()
                result = response.json()
                
                # Push results to XCom
                context['ti'].xcom_push(key=f'ai_process_{self.process_type}', value=result)
                
                self.log.info(f"AI process triggered successfully: {result}")
                return result
                
            except requests.exceptions.RequestException as e:
                self.log.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == self.retry_count - 1:
                    self.log.error(f"All {self.retry_count} attempts failed")
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
            
            except Exception as e:
                self.log.error(f"Unexpected error: {str(e)}")
                raise

class S3DataMonitorOperator(BaseOperator):
    """
    Custom operator for monitoring S3 data quality and volume
    """
    
    @apply_defaults
    def __init__(
        self,
        bucket_name: str,
        prefix: str,
        aws_conn_id: str = 'aws_default',
        expected_file_count: Optional[int] = None,
        expected_total_size_mb: Optional[int] = None,
        time_window_hours: int = 24,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.aws_conn_id = aws_conn_id
        self.expected_file_count = expected_file_count
        self.expected_total_size_mb = expected_total_size_mb
        self.time_window_hours = time_window_hours
        
    def execute(self, context):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        self.log.info(f"Monitoring S3 data: {self.bucket_name}/{self.prefix}")
        
        # Initialize hook
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        
        try:
            # List all files with the prefix
            all_keys = s3_hook.list_keys(bucket_name=self.bucket_name, prefix=self.prefix)
            
            if not all_keys:
                self.log.warning(f"No files found in {self.bucket_name}/{self.prefix}")
                context['ti'].xcom_push(key='s3_monitoring_result', value={
                    'status': 'no_files',
                    'file_count': 0,
                    'total_size_bytes': 0
                })
                return {'status': 'no_files'}
            
            # Filter by time window
            cutoff_time = datetime.now() - timedelta(hours=self.time_window_hours)
            recent_files = []
            total_size = 0
            
            for key in all_keys:
                try:
                    file_obj = s3_hook.get_key(key, self.bucket_name)
                    last_modified = file_obj.last_modified
                    
                    if isinstance(last_modified, str):
                        last_modified = datetime.strptime(last_modified, '%Y-%m-%d %H:%M:%S%z').replace(tzinfo=None)
                    
                    if last_modified > cutoff_time:
                        recent_files.append({
                            'key': key,
                            'size': file_obj.content_length,
                            'last_modified': last_modified.isoformat()
                        })
                        total_size += file_obj.content_length
                except Exception as e:
                    self.log.warning(f"Error processing file {key}: {str(e)}")
            
            # Calculate metrics
            file_count = len(recent_files)
            total_size_mb = total_size / (1024 * 1024)
            
            # Check against expectations
            checks_passed = True
            check_results = {}
            
            if self.expected_file_count is not None:
                check_results['file_count_check'] = file_count >= self.expected_file_count
                checks_passed = checks_passed and check_results['file_count_check']
            
            if self.expected_total_size_mb is not None:
                check_results['size_check'] = total_size_mb >= self.expected_total_size_mb
                checks_passed = checks_passed and check_results['size_check']
            
            # Prepare monitoring result
            monitoring_result = {
                'bucket': self.bucket_name,
                'prefix': self.prefix,
                'file_count': file_count,
                'total_size_bytes': total_size,
                'total_size_mb': total_size_mb,
                'recent_files': recent_files[:10],  # Sample of recent files
                'checks_passed': checks_passed,
                'check_results': check_results,
                'time_window_hours': self.time_window_hours,
                'timestamp': datetime.now().isoformat()
            }
            
            # Push to XCom
            context['ti'].xcom_push(key='s3_monitoring_result', value=monitoring_result)
            
            if not checks_passed:
                failed_checks = [name for name, passed in check_results.items() if not passed]
                warning_msg = f"S3 monitoring checks failed: {failed_checks}"
                self.log.warning(warning_msg)
            
            self.log.info(f"S3 monitoring completed: {file_count} files, {total_size_mb:.2f} MB")
            return monitoring_result
        
        except Exception as e:
            self.log.error(f"S3 monitoring failed: {str(e)}")
            raise

class DataQualityAlertOperator(BaseOperator):
    """
    Custom operator for sending data quality alerts
    """
    
    @apply_defaults
    def __init__(
        self,
        alert_type: str,
        severity: str = 'warning',
        message: str = '',
        thresholds: Dict[str, Any] = None,
        notification_channels: List[str] = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.alert_type = alert_type
        self.severity = severity
        self.message = message
        self.thresholds = thresholds or {}
        self.notification_channels = notification_channels or ['log']
        
    def execute(self, context):
        self.log.info(f"Generating {self.severity} alert: {self.alert_type}")
        
        # Get relevant data from context
        ti = context['ti']
        
        # Check thresholds if provided
        should_alert = True
        
        if self.thresholds:
            for metric_name, threshold_value in self.thresholds.items():
                metric_value = ti.xcom_pull(key=metric_name)
                if metric_value is not None:
                    if self.alert_type == 'threshold_exceeded':
                        should_alert = metric_value > threshold_value
                    elif self.alert_type == 'threshold_below':
                        should_alert = metric_value < threshold_value
        
        if not should_alert:
            self.log.info(f"Alert conditions not met for {self.alert_type}")
            return {'status': 'skipped', 'reason': 'conditions_not_met'}
        
        # Prepare alert details
        alert_details = {
            'alert_id': f"{self.alert_type}_{int(datetime.now().timestamp())}",
            'alert_type': self.alert_type,
            'severity': self.severity,
            'message': self.message,
            'dag_id': context['dag'].dag_id,
            'task_id': self.task_id,
            'execution_date': context['execution_date'].isoformat(),
            'timestamp': datetime.now().isoformat(),
            'thresholds': self.thresholds
        }
        
        # Send to configured channels
        for channel in self.notification_channels:
            if channel == 'log':
                self.log.warning(f"ALERT [{self.severity.upper()}]: {self.message}")
            elif channel == 'xcom':
                ti.xcom_push(key='data_quality_alert', value=alert_details)
            elif channel == 'email':
                # In production, implement email sending
                self.log.info(f"Would send email alert: {self.message}")
            elif channel == 'slack':
                # In production, implement Slack webhook
                self.log.info(f"Would send Slack alert: {self.message}")
        
        # Push alert to XCom for downstream processing
        ti.xcom_push(key=f'alert_{self.alert_type}', value=alert_details)
        
        self.log.info(f"Alert generated: {alert_details['alert_id']}")
        return alert_details