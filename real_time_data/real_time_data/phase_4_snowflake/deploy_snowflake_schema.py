import os
import logging
from airflow_hooks.snowflake_hook import SnowflakeHook

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def deploy_schema():
    """
    Deploy Snowflake schema by executing SQL files in order.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    sql_dir = os.path.join(base_dir, 'sql')
    
    # Order of execution is important
    sql_files = [
        'create_warehouse.sql',
        'create_database.sql',
        'create_tables.sql',
        'views.sql'
    ]
    
    try:
        hook = SnowflakeHook()
        # Test connection first
        if not hook.test_connection():
            logger.error("Failed to connect to Snowflake. Please check your credentials in airflow.env")
            return

        logger.info("Successfully connected to Snowflake.")
        
        for sql_file in sql_files:
            file_path = os.path.join(sql_dir, sql_file)
            if not os.path.exists(file_path):
                logger.error(f"SQL file not found: {file_path}")
                continue
                
            logger.info(f"Executing {sql_file}...")
            # Use split_statements=True to handle multiple SQL commands in one file
            hook.execute_file(file_path, split_statements=True)
            logger.info(f"Successfully executed {sql_file}")
            
        logger.info("Snowflake schema deployment completed successfully!")
        
    except Exception as e:
        logger.error(f"Deployment failed: {str(e)}")

if __name__ == "__main__":
    deploy_schema()
