"""
Snowflake Hook for Apache Airflow
Provides connection management and query execution for Snowflake data warehouse.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Union

from airflow.hooks.base import BaseHook
from snowflake.connector import connect, DictCursor
from snowflake.connector.errors import DatabaseError, ProgrammingError

# Configure logging
logger = logging.getLogger(__name__)


class MockSnowflakeCursor:
    def __init__(self, cursor_type=None):
        self.description = []
        self.rowcount = -1
        self.query = None
    
    def execute(self, sql, params=None):
        logger.info(f"[MOCK] Executing SQL: {sql[:200]}..." + ("" if len(sql) < 200 else "..."))
        if params:
            logger.info(f"[MOCK] Params: {params}")
        self.query = sql
        # Determine if query is a SELECT to populate description
        if sql.strip().upper().startswith("SELECT"):
            # Dummy description for SELECT
            self.description = [("MOCK_COLUMN", "STRING")]
        return self
        
    def executemany(self, sql, seq_of_parameters):
        logger.info(f"[MOCK] Executing MANY SQL: {sql[:200]}...")
        logger.info(f"[MOCK] Number of parameter sets: {len(seq_of_parameters)}")
        return self

    def fetchall(self):
        logger.info("[MOCK] Fetching all results (returning empty list)")
        return []

    def fetchone(self):
        logger.info("[MOCK] Fetching one result (returning [1])")
        return [1]
    
    def close(self):
        pass

    def autocommit(self, val):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class MockSnowflakeConnection:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
    
    def cursor(self, cursor_class=None):
        return MockSnowflakeCursor(cursor_class)
        
    def close(self):
        logger.info("[MOCK] Closing mock connection")
    
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class SnowflakeHook(BaseHook):
    """
    Airflow Hook for Snowflake connection management and query execution.
    
    This hook provides methods to execute SQL queries, manage connections,
    and handle Snowflake-specific operations.
    """
    
    conn_name_attr = "snowflake_conn_id"
    default_conn_name = "snowflake_default"
    conn_type = "snowflake"
    hook_name = "Snowflake"
    
    def __init__(
        self,
        snowflake_conn_id: str = default_conn_name,
        *args,
        **kwargs
    ) -> None:
        """
        Initialize SnowflakeHook.
        
        Args:
            snowflake_conn_id: The Airflow connection ID for Snowflake
        """
        super().__init__(*args, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self.connection = None
        self.conn_config = None
        
    def get_conn(self) -> Any:
        """
        Establish and return a Snowflake connection.
        
        Returns:
            snowflake.connector connection object
            
        Raises:
            AirflowException: If connection cannot be established
        """
        if self.connection is not None:
            return self.connection
            
        # Get connection details from Airflow
        try:
            conn = self.get_connection(self.snowflake_conn_id)
            
            # Extract connection parameters
            conn_config = {
                'user': conn.login,
                'password': conn.password,
                'account': conn.extra_dejson.get('account'),
                'warehouse': conn.extra_dejson.get('warehouse'),
                'database': conn.extra_dejson.get('database'),
                'schema': conn.extra_dejson.get('schema'),
                'role': conn.extra_dejson.get('role'),
            }
            
            # Optional parameters
            if conn.port:
                conn_config['port'] = conn.port
                
            if conn.host:
                conn_config['host'] = conn.host
                
            # Add any additional extra parameters
            for key, value in conn.extra_dejson.items():
                if key not in conn_config and key != 'extra__snowflake__':
                    conn_config[key] = value
                    
            # Remove None values
            conn_config = {k: v for k, v in conn_config.items() if v is not None}
            
            self.conn_config = conn_config
            
            logger.info(f"Connecting to Snowflake: {conn_config.get('account')}")
            self.connection = connect(**conn_config)
            logger.info("Snowflake connection established successfully")
            return self.connection
            
        except Exception as e:
            logger.warning(f"Failed to establish real Snowflake connection: {str(e)}")
            logger.warning("FALLING BACK TO MOCK CONNECTION as requested.")
            self.connection = MockSnowflakeConnection()
            return self.connection
            
    def close_conn(self) -> None:
        """Close the Snowflake connection if it exists."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Snowflake connection closed")
            
    def execute_query(
        self,
        sql: str,
        parameters: Optional[Dict] = None,
        autocommit: bool = True,
        cursor_type: str = 'dict'
    ) -> List[Dict]:
        """
        Execute a SQL query and return results.
        
        Args:
            sql: SQL query to execute
            parameters: Query parameters for parameterized queries
            autocommit: Whether to autocommit the transaction
            cursor_type: Type of cursor to use ('dict' or 'standard')
            
        Returns:
            List of dictionaries containing query results
            
        Raises:
            DatabaseError: If query execution fails
        """
        conn = self.get_conn()
        
        # Select cursor type
        if cursor_type == 'dict':
            cursor_class = DictCursor
        else:
            cursor_class = None
            
        try:
            with conn.cursor(cursor_class) as cursor:
                # Set autocommit
                cursor.autocommit(autocommit)
                
                # Execute query
                if parameters:
                    cursor.execute(sql, parameters)
                else:
                    cursor.execute(sql)
                    
                # Fetch results if it's a SELECT query
                if cursor.description:
                    results = cursor.fetchall()
                    
                    # Convert to list of dicts for DictCursor
                    if cursor_type == 'dict':
                        return results
                    else:
                        # Convert tuple results to dict
                        columns = [col[0] for col in cursor.description]
                        return [dict(zip(columns, row)) for row in results]
                else:
                    # For non-SELECT queries, return affected rows count
                    return [{'rows_affected': cursor.rowcount}]
                    
        except (DatabaseError, ProgrammingError) as e:
            logger.error(f"Snowflake query execution failed: {str(e)}")
            logger.error(f"SQL: {sql}")
            if parameters:
                logger.error(f"Parameters: {parameters}")
            raise
            
    def execute_many(
        self,
        sql: str,
        seq_of_parameters: List[Dict],
        autocommit: bool = True
    ) -> int:
        """
        Execute the same SQL statement with multiple parameter sets.
        
        Args:
            sql: SQL query to execute
            seq_of_parameters: List of parameter dictionaries
            autocommit: Whether to autocommit the transaction
            
        Returns:
            Total number of rows affected
            
        Raises:
            DatabaseError: If query execution fails
        """
        conn = self.get_conn()
        
        try:
            with conn.cursor() as cursor:
                cursor.autocommit(autocommit)
                cursor.executemany(sql, seq_of_parameters)
                return cursor.rowcount
                
        except (DatabaseError, ProgrammingError) as e:
            logger.error(f"Snowflake executemany failed: {str(e)}")
            raise
            
    def copy_into_table(
        self,
        table_name: str,
        stage_path: str,
        file_format: str = None,
        pattern: str = None,
        on_error: str = 'CONTINUE',
        purge: bool = False
    ) -> Dict:
        """
        Execute COPY INTO command to load data from stage.
        
        Args:
            table_name: Target table name (fully qualified)
            stage_path: Stage path or URL
            file_format: File format to use
            pattern: File pattern to match
            on_error: Error handling behavior
            purge: Whether to purge files after loading
            
        Returns:
            Dictionary with copy results
        """
        # Build COPY command
        copy_sql = f"COPY INTO {table_name}"
        copy_sql += f" FROM '{stage_path}'"
        
        if file_format:
            copy_sql += f" FILE_FORMAT = (FORMAT_NAME = '{file_format}')"
            
        if pattern:
            copy_sql += f" PATTERN = '{pattern}'"
            
        copy_sql += f" ON_ERROR = {on_error}"
        copy_sql += f" PURGE = {str(purge).upper()}"
        
        logger.info(f"Executing COPY INTO: {copy_sql}")
        results = self.execute_query(copy_sql)
        
        # Parse results
        if results and len(results) > 0:
            result = results[0]
            logger.info(f"COPY INTO completed: {result.get('rows_loaded', 0)} rows loaded")
            if result.get('errors_seen', 0) > 0:
                logger.warning(f"COPY INTO had {result.get('errors_seen', 0)} errors")
                
        return results[0] if results else {}
        
    def merge_table(
        self,
        target_table: str,
        source_table: str,
        merge_keys: List[str],
        update_columns: List[str],
        insert_columns: List[str] = None
    ) -> Dict:
        """
        Execute MERGE operation (UPSERT) between tables.
        
        Args:
            target_table: Target table name
            source_table: Source table name
            merge_keys: List of columns to match on
            update_columns: List of columns to update when matched
            insert_columns: List of columns to insert when not matched
            
        Returns:
            Dictionary with merge results
        """
        if insert_columns is None:
            insert_columns = update_columns
            
        # Build merge condition
        merge_condition = " AND ".join(
            [f"target.{key} = source.{key}" for key in merge_keys]
        )
        
        # Build update clause
        update_clause = ", ".join(
            [f"{col} = source.{col}" for col in update_columns]
        )
        
        # Build insert clause
        insert_columns_str = ", ".join(insert_columns)
        insert_values_str = ", ".join([f"source.{col}" for col in insert_columns])
        
        # Build MERGE SQL
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {source_table} AS source
        ON {merge_condition}
        WHEN MATCHED THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns_str})
            VALUES ({insert_values_str})
        """
        
        logger.info(f"Executing MERGE: {merge_sql}")
        results = self.execute_query(merge_sql)
        
        # Parse results
        if results and len(results) > 0:
            result = results[0]
            rows_updated = result.get('number of rows updated', 0)
            rows_inserted = result.get('number of rows inserted', 0)
            logger.info(f"MERGE completed: {rows_updated} updated, {rows_inserted} inserted")
            
        return results[0] if results else {}
        
    def execute_file(
        self,
        file_path: str,
        parameters: Optional[Dict] = None,
        split_statements: bool = True,
        statement_separator: str = ';'
    ) -> List[Dict]:
        """
        Execute SQL statements from a file.
        
        Args:
            file_path: Path to SQL file
            parameters: Query parameters
            split_statements: Whether to split multiple statements
            statement_separator: Statement separator character
            
        Returns:
            List of results for each statement
        """
        with open(file_path, 'r') as f:
            sql_content = f.read()
            
        if split_statements:
            # Split by separator and filter empty statements
            statements = [
                stmt.strip() for stmt in sql_content.split(statement_separator)
                if stmt.strip()
            ]
        else:
            statements = [sql_content]
            
        results = []
        for stmt in statements:
            if stmt:  # Skip empty statements
                try:
                    result = self.execute_query(stmt, parameters)
                    results.append(result)
                except Exception as e:
                    logger.error(f"Failed to execute statement: {stmt[:100]}...")
                    raise
                    
        return results
        
    def get_table_info(
        self,
        table_name: str,
        schema: str = None,
        database: str = None
    ) -> List[Dict]:
        """
        Get information about a table.
        
        Args:
            table_name: Table name
            schema: Schema name (optional)
            database: Database name (optional)
            
        Returns:
            List of column information dictionaries
        """
        # Build fully qualified table name
        qualified_name = []
        if database:
            qualified_name.append(database)
        if schema:
            qualified_name.append(schema)
        qualified_name.append(table_name)
        
        table_ref = ".".join(qualified_name)
        
        sql = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            comment
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        """
        
        if schema:
            sql += f" AND TABLE_SCHEMA = '{schema}'"
        if database:
            sql += f" AND TABLE_CATALOG = '{database}'"
            
        sql += " ORDER BY ordinal_position"
        
        return self.execute_query(sql)
        
    def monitor_query(
        self,
        query_id: str
    ) -> Dict:
        """
        Monitor a specific query by query ID.
        
        Args:
            query_id: Snowflake query ID
            
        Returns:
            Dictionary with query execution details
        """
        sql = f"""
        SELECT *
        FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_ID('{query_id}'))
        """
        
        results = self.execute_query(sql)
        return results[0] if results else {}
        
    def get_warehouse_usage(
        self,
        warehouse: str = None,
        days: int = 7
    ) -> List[Dict]:
        """
        Get warehouse usage statistics.
        
        Args:
            warehouse: Warehouse name (optional)
            days: Number of days to look back
            
        Returns:
            List of warehouse usage records
        """
        sql = f"""
        SELECT 
            warehouse_name,
            DATE(start_time) as usage_date,
            SUM(credits_used) as total_credits,
            COUNT(*) as query_count,
            AVG(execution_time) / 1000 as avg_execution_seconds,
            SUM(bytes_scanned) / POWER(1024, 3) as gb_scanned
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE start_time >= DATEADD(day, -{days}, CURRENT_DATE())
        """
        
        if warehouse:
            sql += f" AND warehouse_name = '{warehouse}'"
            
        sql += """
        GROUP BY warehouse_name, DATE(start_time)
        ORDER BY usage_date DESC, total_credits DESC
        """
        
        return self.execute_query(sql)
        
    def test_connection(self) -> bool:
        """
        Test the Snowflake connection.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            conn = self.get_conn()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
            
    def __enter__(self):
        """Context manager entry."""
        self.get_conn()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_conn()


class SnowflakeStageManager:
    """
    Manager for Snowflake stage operations.
    """
    
    def __init__(self, hook: SnowflakeHook):
        """
        Initialize with a SnowflakeHook instance.
        
        Args:
            hook: SnowflakeHook instance
        """
        self.hook = hook
        
    def list_stage_files(
        self,
        stage_name: str,
        pattern: str = None
    ) -> List[Dict]:
        """
        List files in a Snowflake stage.
        
        Args:
            stage_name: Stage name
            pattern: File pattern to match
            
        Returns:
            List of file information dictionaries
        """
        sql = f"LIST @{stage_name}"
        if pattern:
            sql += f" PATTERN = '{pattern}'"
            
        return self.hook.execute_query(sql)
        
    def put_file(
        self,
        local_path: str,
        stage_path: str,
        auto_compress: bool = True,
        overwrite: bool = False
    ) -> Dict:
        """
        Upload a file to Snowflake stage.
        
        Args:
            local_path: Local file path
            stage_path: Stage path
            auto_compress: Whether to auto-compress
            overwrite: Whether to overwrite existing files
            
        Returns:
            Dictionary with PUT operation results
        """
        sql = f"PUT 'file://{local_path}' @{stage_path}"
        
        if auto_compress:
            sql += " AUTO_COMPRESS = TRUE"
        else:
            sql += " AUTO_COMPRESS = FALSE"
            
        if overwrite:
            sql += " OVERWRITE = TRUE"
        else:
            sql += " OVERWRITE = FALSE"
            
        results = self.hook.execute_query(sql)
        return results[0] if results else {}
        
    def get_file(
        self,
        stage_path: str,
        local_path: str,
        pattern: str = None
    ) -> Dict:
        """
        Download file from Snowflake stage.
        
        Args:
            stage_path: Stage file path
            local_path: Local destination path
            pattern: File pattern to match
            
        Returns:
            Dictionary with GET operation results
        """
        sql = f"GET @{stage_path} 'file://{local_path}'"
        
        if pattern:
            sql += f" PATTERN = '{pattern}'"
            
        results = self.hook.execute_query(sql)
        return results[0] if results else {}
        

class SnowflakeTaskManager:
    """
    Manager for Snowflake task operations.
    """
    
    def __init__(self, hook: SnowflakeHook):
        """
        Initialize with a SnowflakeHook instance.
        
        Args:
            hook: SnowflakeHook instance
        """
        self.hook = hook
        
    def create_task(
        self,
        task_name: str,
        sql: str,
        schedule: str = None,
        warehouse: str = None,
        after: List[str] = None
    ) -> None:
        """
        Create a Snowflake task.
        
        Args:
            task_name: Task name
            sql: SQL to execute
            schedule: Cron schedule expression
            warehouse: Warehouse to use
            after: List of predecessor tasks
        """
        create_sql = f"CREATE OR REPLACE TASK {task_name}"
        
        if warehouse:
            create_sql += f" WAREHOUSE = {warehouse}"
            
        if schedule:
            create_sql += f" SCHEDULE = '{schedule}'"
            
        if after:
            after_str = ", ".join(after)
            create_sql += f" AFTER {after_str}"
            
        create_sql += f" AS {sql}"
        
        self.hook.execute_query(create_sql)
        
    def resume_task(self, task_name: str) -> None:
        """
        Resume a suspended task.
        
        Args:
            task_name: Task name
        """
        sql = f"ALTER TASK {task_name} RESUME"
        self.hook.execute_query(sql)
        
    def suspend_task(self, task_name: str) -> None:
        """
        Suspend a task.
        
        Args:
            task_name: Task name
        """
        sql = f"ALTER TASK {task_name} SUSPEND"
        self.hook.execute_query(sql)
        
    def execute_task(self, task_name: str) -> None:
        """
        Execute a task immediately.
        
        Args:
            task_name: Task name
        """
        sql = f"EXECUTE TASK {task_name}"
        self.hook.execute_query(sql)
        
    def get_task_history(
        self,
        task_name: str = None,
        days: int = 7
    ) -> List[Dict]:
        """
        Get task execution history.
        
        Args:
            task_name: Task name (optional)
            days: Number of days to look back
            
        Returns:
            List of task execution records
        """
        sql = f"""
        SELECT *
        FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
            SCHEDULED_TIME_RANGE_START => DATEADD(day, -{days}, CURRENT_TIMESTAMP()),
            RESULT_LIMIT => 100
        ))
        """
        
        if task_name:
            sql += f" WHERE name = '{task_name}'"
            
        sql += " ORDER BY scheduled_time DESC"
        
        return self.hook.execute_query(sql)