"""
DAG for triggering Agentic AI processing
Orchestrates ML/AI workflows for sales intelligence
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
import json
import requests
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
    'agentic_ai_pipeline',
    default_args=default_args,
    description='Trigger Agentic AI processing for sales intelligence',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    catchup=False,
    tags=['ai', 'ml', 'agentic', 'analytics', 'intelligence'],
    max_active_runs=1,
)

def check_ai_processing_conditions(**kwargs):
    """Check conditions for triggering AI processing"""
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
        
        # Check 1: Sufficient new data
        cursor.execute("""
        SELECT 
            COUNT(*) as new_transactions,
            SUM(FINAL_AMOUNT) as new_revenue
        FROM SALES_FACT 
        WHERE TRANSACTION_TIMESTAMP >= DATEADD(hour, -6, CURRENT_TIMESTAMP());
        """)
        
        new_data = cursor.fetchone()
        new_transactions = new_data[0]
        new_revenue = new_data[1]
        
        # Check 2: Anomaly detection conditions
        cursor.execute("""
        SELECT 
            COUNT(*) as high_value_transactions
        FROM SALES_FACT 
        WHERE FINAL_AMOUNT > 5000 
        AND TRANSACTION_TIMESTAMP >= DATEADD(hour, -6, CURRENT_TIMESTAMP());
        """)
        
        high_value_tx = cursor.fetchone()[0]
        
        # Check 3: Customer segmentation update needed
        cursor.execute("""
        SELECT 
            MAX(TRANSACTION_TIMESTAMP) as latest_transaction,
            MIN(TRANSACTION_TIMESTAMP) as earliest_transaction
        FROM SALES_FACT 
        WHERE CUSTOMER_ID IN (
            SELECT CUSTOMER_ID 
            FROM CUSTOMERS_DIM 
            WHERE LOAD_TIMESTAMP <= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        );
        """)
        
        segmentation_data = cursor.fetchone()
        
        # Determine processing conditions
        conditions = {
            'new_transactions': new_transactions,
            'new_revenue': new_revenue,
            'high_value_transactions': high_value_tx,
            'latest_transaction': segmentation_data[0].isoformat() if segmentation_data[0] else None,
            'earliest_transaction': segmentation_data[1].isoformat() if segmentation_data[1] else None,
            'conditions_met': []
        }
        
        # Define thresholds
        if new_transactions >= 100:
            conditions['conditions_met'].append('sufficient_new_data')
        
        if high_value_tx >= 5:
            conditions['conditions_met'].append('high_value_transactions_present')
        
        if new_revenue and new_revenue >= 10000:
            conditions['conditions_met'].append('significant_revenue')
        
        # Always run customer segmentation every 24 hours
        cursor.execute("""
        SELECT MAX(LOAD_TIMESTAMP) FROM CUSTOMERS_DIM;
        """)
        last_segmentation = cursor.fetchone()[0]
        
        if last_segmentation:
            hours_since_segmentation = (datetime.now() - last_segmentation).total_seconds() / 3600
            if hours_since_segmentation >= 24:
                conditions['conditions_met'].append('segmentation_update_due')
        
        ti.xcom_push(key='ai_conditions', value=conditions)
        
        # Determine which AI processes to trigger
        processes_to_trigger = []
        
        if 'sufficient_new_data' in conditions['conditions_met']:
            processes_to_trigger.extend(['anomaly_detection', 'trend_analysis'])
        
        if 'high_value_transactions_present' in conditions['conditions_met']:
            processes_to_trigger.append('high_value_analysis')
        
        if 'significant_revenue' in conditions['conditions_met']:
            processes_to_trigger.append('revenue_forecasting')
        
        if 'segmentation_update_due' in conditions['conditions_met']:
            processes_to_trigger.append('customer_segmentation')
        
        # Remove duplicates
        processes_to_trigger = list(set(processes_to_trigger))
        
        ti.xcom_push(key='processes_to_trigger', value=processes_to_trigger)
        
        if processes_to_trigger:
            return 'trigger_ai_processing'
        else:
            return 'skip_ai_processing'
    
    except Exception as e:
        ti.xcom_push(key='error', value=str(e))
        raise Exception(f"Failed to check AI processing conditions: {str(e)}")

def trigger_anomaly_detection(**kwargs):
    """Trigger anomaly detection AI agent"""
    ti = kwargs['ti']
    
    # Get AI API endpoint from variables
    ai_api_endpoint = Variable.get("AI_API_ENDPOINT", "http://agentic-ai-service:8000")
    
    # Get conditions from previous task
    conditions = ti.xcom_pull(task_ids='check_ai_conditions', key='ai_conditions')
    
    # Prepare request payload
    payload = {
        "process": "anomaly_detection",
        "parameters": {
            "time_window_hours": 6,
            "min_transactions": 100,
            "anomaly_threshold": 3.0,
            "features": ["FINAL_AMOUNT", "QUANTITY", "DISCOUNT_AMOUNT", "FRAUD_SCORE"]
        },
        "metadata": {
            "dag_run_id": kwargs['dag_run'].run_id,
            "execution_date": kwargs['execution_date'].isoformat(),
            "conditions": conditions
        }
    }
    
    # Make API request to AI service
    try:
        response = requests.post(
            f"{ai_api_endpoint}/api/v1/process/anomaly-detection",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=300
        )
        
        if response.status_code == 200:
            result = response.json()
            
            # Store results
            ti.xcom_push(key='anomaly_detection_result', value=result)
            
            if result.get('status') == 'success':
                anomalies_found = result.get('anomalies_found', 0)
                return f"Anomaly detection completed: {anomalies_found} anomalies found"
            else:
                raise Exception(f"Anomaly detection failed: {result.get('error', 'Unknown error')}")
        else:
            raise Exception(f"API request failed: {response.status_code} - {response.text}")
    
    except Exception as e:
        ti.xcom_push(key='error', value=str(e))
        raise Exception(f"Failed to trigger anomaly detection: {str(e)}")

def trigger_customer_segmentation(**kwargs):
    """Trigger customer segmentation AI agent"""
    ti = kwargs['ti']
    
    # Get AI API endpoint
    ai_api_endpoint = Variable.get("AI_API_ENDPOINT", "http://agentic-ai-service:8000")
    
    # Prepare request payload
    payload = {
        "process": "customer_segmentation",
        "parameters": {
            "n_clusters": 5,
            "features": ["LIFETIME_VALUE", "AVG_ORDER_VALUE", "PURCHASE_FREQUENCY", "RECENCY"],
            "min_customers": 100
        },
        "metadata": {
            "dag_run_id": kwargs['dag_run'].run_id,
            "execution_date": kwargs['execution_date'].isoformat()
        }
    }
    
    # Make API request
    try:
        response = requests.post(
            f"{ai_api_endpoint}/api/v1/process/customer-segmentation",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=600  # Longer timeout for segmentation
        )
        
        if response.status_code == 200:
            result = response.json()
            
            # Store results
            ti.xcom_push(key='customer_segmentation_result', value=result)
            
            if result.get('status') == 'success':
                segments_created = result.get('segments_created', 0)
                customers_segmented = result.get('customers_segmented', 0)
                return f"Customer segmentation completed: {segments_created} segments for {customers_segmented} customers"
            else:
                raise Exception(f"Customer segmentation failed: {result.get('error', 'Unknown error')}")
        else:
            raise Exception(f"API request failed: {response.status_code} - {response.text}")
    
    except Exception as e:
        ti.xcom_push(key='error', value=str(e))
        raise Exception(f"Failed to trigger customer segmentation: {str(e)}")

def trigger_trend_analysis(**kwargs):
    """Trigger trend analysis AI agent"""
    ti = kwargs['ti']
    
    # Get AI API endpoint
    ai_api_endpoint = Variable.get("AI_API_ENDPOINT", "http://agentic-ai-service:8000")
    
    # Get conditions
    conditions = ti.xcom_pull(task_ids='check_ai_conditions', key='ai_conditions')
    
    # Prepare request payload
    payload = {
        "process": "trend_analysis",
        "parameters": {
            "time_window_days": 7,
            "analysis_types": ["hourly", "daily", "category", "geographic"],
            "confidence_level": 0.95
        },
        "metadata": {
            "dag_run_id": kwargs['dag_run'].run_id,
            "execution_date": kwargs['execution_date'].isoformat(),
            "new_transactions": conditions.get('new_transactions', 0)
        }
    }
    
    # Make API request
    try:
        response = requests.post(
            f"{ai_api_endpoint}/api/v1/process/trend-analysis",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=300
        )
        
        if response.status_code == 200:
            result = response.json()
            
            # Store results
            ti.xcom_push(key='trend_analysis_result', value=result)
            
            if result.get('status') == 'success':
                trends_identified = result.get('trends_identified', 0)
                return f"Trend analysis completed: {trends_identified} trends identified"
            else:
                raise Exception(f"Trend analysis failed: {result.get('error', 'Unknown error')}")
        else:
            raise Exception(f"API request failed: {response.status_code} - {response.text}")
    
    except Exception as e:
        ti.xcom_push(key='error', value=str(e))
        raise Exception(f"Failed to trigger trend analysis: {str(e)}")

def trigger_revenue_forecasting(**kwargs):
    """Trigger revenue forecasting AI agent"""
    ti = kwargs['ti']
    
    # Get AI API endpoint
    ai_api_endpoint = Variable.get("AI_API_ENDPOINT", "http://agentic-ai-service:8000")
    
    # Prepare request payload
    payload = {
        "process": "revenue_forecasting",
        "parameters": {
            "forecast_horizon_days": 30,
            "model_type": "prophet",
            "confidence_intervals": [0.8, 0.95],
            "include_seasonality": True,
            "include_holidays": True
        },
        "metadata": {
            "dag_run_id": kwargs['dag_run'].run_id,
            "execution_date": kwargs['execution_date'].isoformat()
        }
    }
    
    # Make API request
    try:
        response = requests.post(
            f"{ai_api_endpoint}/api/v1/process/revenue-forecasting",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=600  # Longer timeout for forecasting
        )
        
        if response.status_code == 200:
            result = response.json()
            
            # Store results
            ti.xcom_push(key='revenue_forecasting_result', value=result)
            
            if result.get('status') == 'success':
                forecast_created = result.get('forecast_created', False)
                mape = result.get('mape', 0)
                return f"Revenue forecasting completed: MAPE={mape:.2f}%"
            else:
                raise Exception(f"Revenue forecasting failed: {result.get('error', 'Unknown error')}")
        else:
            raise Exception(f"API request failed: {response.status_code} - {response.text}")
    
    except Exception as e:
        ti.xcom_push(key='error', value=str(e))
        raise Exception(f"Failed to trigger revenue forecasting: {str(e)}")

def update_ai_results_to_snowflake(**kwargs):
    """Update AI processing results to Snowflake"""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    ti = kwargs['ti']
    
    # Get results from all AI processes
    anomaly_result = ti.xcom_pull(task_ids='trigger_anomaly_detection', key='anomaly_detection_result')
    segmentation_result = ti.xcom_pull(task_ids='trigger_customer_segmentation', key='customer_segmentation_result')
    trend_result = ti.xcom_pull(task_ids='trigger_trend_analysis', key='trend_analysis_result')
    forecast_result = ti.xcom_pull(task_ids='trigger_revenue_forecasting', key='revenue_forecasting_result')
    
    # Get configuration
    snowflake_database = Variable.get("SNOWFLAKE_DATABASE", "SALES_DB")
    snowflake_schema = Variable.get("SNOWFLAKE_SCHEMA", "AI_RESULTS")
    
    # Initialize Snowflake hook
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    try:
        connection = snowflake_hook.get_conn()
        cursor = connection.cursor()
        
        # Create AI results schema if it doesn't exist
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {snowflake_database}.{snowflake_schema};")
        cursor.execute(f"USE SCHEMA {snowflake_database}.{snowflake_schema};")
        
        # Create AI_RESULTS table
        create_table_sql = """
        CREATE OR REPLACE TABLE AI_PROCESSING_RESULTS (
            PROCESS_ID STRING,
            PROCESS_TYPE STRING,
            EXECUTION_TIMESTAMP TIMESTAMP_NTZ,
            STATUS STRING,
            RESULTS VARIANT,
            METRICS VARIANT,
            DAG_RUN_ID STRING,
            LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
        cursor.execute(create_table_sql)
        
        # Insert results
        results_to_insert = []
        
        if anomaly_result:
            results_to_insert.append((
                anomaly_result.get('process_id', 'unknown'),
                'anomaly_detection',
                datetime.now(),
                anomaly_result.get('status', 'unknown'),
                json.dumps(anomaly_result.get('results', {})),
                json.dumps(anomaly_result.get('metrics', {})),
                kwargs['dag_run'].run_id
            ))
        
        if segmentation_result:
            results_to_insert.append((
                segmentation_result.get('process_id', 'unknown'),
                'customer_segmentation',
                datetime.now(),
                segmentation_result.get('status', 'unknown'),
                json.dumps(segmentation_result.get('results', {})),
                json.dumps(segmentation_result.get('metrics', {})),
                kwargs['dag_run'].run_id
            ))
        
        if trend_result:
            results_to_insert.append((
                trend_result.get('process_id', 'unknown'),
                'trend_analysis',
                datetime.now(),
                trend_result.get('status', 'unknown'),
                json.dumps(trend_result.get('results', {})),
                json.dumps(trend_result.get('metrics', {})),
                kwargs['dag_run'].run_id
            ))
        
        if forecast_result:
            results_to_insert.append((
                forecast_result.get('process_id', 'unknown'),
                'revenue_forecasting',
                datetime.now(),
                forecast_result.get('status', 'unknown'),
                json.dumps(forecast_result.get('results', {})),
                json.dumps(forecast_result.get('metrics', {})),
                kwargs['dag_run'].run_id
            ))
        
        # Insert all results
        if results_to_insert:
            insert_sql = """
            INSERT INTO AI_PROCESSING_RESULTS 
            (PROCESS_ID, PROCESS_TYPE, EXECUTION_TIMESTAMP, STATUS, RESULTS, METRICS, DAG_RUN_ID)
            VALUES (%s, %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s), %s);
            """
            
            cursor.executemany(insert_sql, results_to_insert)
            rows_inserted = cursor.rowcount
        
        # Create alerts for important findings
        alerts_created = 0
        
        if anomaly_result and anomaly_result.get('status') == 'success':
            anomalies = anomaly_result.get('results', {}).get('anomalies', [])
            high_severity_anomalies = [a for a in anomalies if a.get('severity') == 'high']
            
            if high_severity_anomalies:
                # Create alerts table if it doesn't exist
                cursor.execute("""
                CREATE OR REPLACE TABLE AI_ALERTS (
                    ALERT_ID STRING,
                    ALERT_TYPE STRING,
                    SEVERITY STRING,
                    DESCRIPTION STRING,
                    RELATED_DATA VARIANT,
                    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    ACKNOWLEDGED BOOLEAN DEFAULT FALSE
                );
                """)
                
                # Insert alerts
                for i, anomaly in enumerate(high_severity_anomalies):
                    alert_sql = """
                    INSERT INTO AI_ALERTS (ALERT_ID, ALERT_TYPE, SEVERITY, DESCRIPTION, RELATED_DATA)
                    VALUES (%s, %s, %s, %s, PARSE_JSON(%s));
                    """
                    
                    cursor.execute(alert_sql, (
                        f"alert_{int(datetime.now().timestamp())}_{i}",
                        'anomaly_detection',
                        anomaly.get('severity', 'high'),
                        anomaly.get('description', 'High severity anomaly detected'),
                        json.dumps(anomaly)
                    ))
                
                alerts_created = len(high_severity_anomalies)
        
        summary = {
            'results_inserted': len(results_to_insert),
            'alerts_created': alerts_created,
            'timestamp': datetime.now().isoformat()
        }
        
        ti.xcom_push(key='snowflake_update_summary', value=summary)
        
        return f"Updated {len(results_to_insert)} AI results to Snowflake, created {alerts_created} alerts"
    
    except Exception as e:
        ti.xcom_push(key='error', value=str(e))
        raise Exception(f"Failed to update AI results to Snowflake: {str(e)}")

def send_ai_summary_notification(**kwargs):
    """Send summary notification of AI processing"""
    ti = kwargs['ti']
    
    # Get results from all tasks
    conditions = ti.xcom_pull(task_ids='check_ai_conditions', key='ai_conditions')
    processes_to_trigger = ti.xcom_pull(task_ids='check_ai_conditions', key='processes_to_trigger')
    
    anomaly_result = ti.xcom_pull(task_ids='trigger_anomaly_detection')
    segmentation_result = ti.xcom_pull(task_ids='trigger_customer_segmentation')
    trend_result = ti.xcom_pull(task_ids='trigger_trend_analysis')
    forecast_result = ti.xcom_pull(task_ids='trigger_revenue_forecasting')
    snowflake_summary = ti.xcom_pull(task_ids='update_ai_results_to_snowflake', key='snowflake_update_summary')
    
    # Build notification message
    notification_message = f"""
    🤖 Agentic AI Processing Summary
    
    Execution Time: {datetime.now().isoformat()}
    DAG Run ID: {kwargs['dag_run'].run_id}
    
    Conditions Checked:
    - New Transactions: {conditions.get('new_transactions', 0)}
    - New Revenue: ${conditions.get('new_revenue', 0):,.2f}
    - High Value Transactions: {conditions.get('high_value_transactions', 0)}
    - Conditions Met: {', '.join(conditions.get('conditions_met', []))}
    
    Processes Triggered: {', '.join(processes_to_trigger) if processes_to_trigger else 'None'}
    
    Results:
    - Anomaly Detection: {anomaly_result if anomaly_result else 'Not triggered'}
    - Customer Segmentation: {segmentation_result if segmentation_result else 'Not triggered'}
    - Trend Analysis: {trend_result if trend_result else 'Not triggered'}
    - Revenue Forecasting: {forecast_result if forecast_result else 'Not triggered'}
    
    Snowflake Update:
    - Results Inserted: {snowflake_summary.get('results_inserted', 0) if snowflake_summary else 0}
    - Alerts Created: {snowflake_summary.get('alerts_created', 0) if snowflake_summary else 0}
    
    Status: ✅ COMPLETED
    """
    
    # In production, send via email/Slack/Teams
    print(notification_message)
    
    # Log to Airflow
    from airflow.utils.log.logging_mixin import LoggingMixin
    LoggingMixin().log.info(notification_message)
    
    return "AI processing summary notification sent"

# Define tasks
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

check_conditions_task = BranchPythonOperator(
    task_id='check_ai_conditions',
    python_callable=check_ai_processing_conditions,
    dag=dag,
)

anomaly_detection_task = PythonOperator(
    task_id='trigger_anomaly_detection',
    python_callable=trigger_anomaly_detection,
    dag=dag,
)

customer_segmentation_task = PythonOperator(
    task_id='trigger_customer_segmentation',
    python_callable=trigger_customer_segmentation,
    dag=dag,
)

trend_analysis_task = PythonOperator(
    task_id='trigger_trend_analysis',
    python_callable=trigger_trend_analysis,
    dag=dag,
)

revenue_forecasting_task = PythonOperator(
    task_id='trigger_revenue_forecasting',
    python_callable=trigger_revenue_forecasting,
    dag=dag,
)

update_snowflake_task = PythonOperator(
    task_id='update_ai_results_to_snowflake',
    python_callable=update_ai_results_to_snowflake,
    dag=dag,
)

send_summary_task = PythonOperator(
    task_id='send_ai_summary_notification',
    python_callable=send_ai_summary_notification,
    dag=dag,
)

skip_ai_task = DummyOperator(
    task_id='skip_ai_processing',
    dag=dag,
)

# Set up task dependencies
start >> check_conditions_task

# Branch paths
check_conditions_task >> [
    anomaly_detection_task, 
    customer_segmentation_task, 
    trend_analysis_task, 
    revenue_forecasting_task
]

check_conditions_task >> skip_ai_task

# Parallel AI tasks all feed into snowflake update
anomaly_detection_task >> update_snowflake_task
customer_segmentation_task >> update_snowflake_task
trend_analysis_task >> update_snowflake_task
revenue_forecasting_task >> update_snowflake_task

update_snowflake_task >> send_summary_task >> end
skip_ai_task >> end