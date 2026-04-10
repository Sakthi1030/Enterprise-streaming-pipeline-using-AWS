"""
Snowflake Query Tool - Tool for executing queries against Snowflake data warehouse.
Provides secure, parameterized query execution with error handling and result formatting.
"""

import json
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, date, timedelta
from decimal import Decimal
import asyncio
import pandas as pd

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.errors import DatabaseError, ProgrammingError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SnowflakeQueryTool:
    """
    Tool for executing queries against Snowflake with connection pooling,
    query optimization, and result caching.
    """
    
    def __init__(
        self,
        config: Optional[Dict] = None,
        max_connections: int = 10,
        cache_ttl: int = 300,  # 5 minutes cache TTL
        timeout: int = 30
    ):
        """
        Initialize Snowflake Query Tool.
        
        Args:
            config: Snowflake connection configuration
            max_connections: Maximum number of connections in pool
            cache_ttl: Cache time-to-live in seconds
            timeout: Query timeout in seconds
        """
        self.config = config or self._load_default_config()
        self.max_connections = max_connections
        self.cache_ttl = cache_ttl
        self.timeout = timeout
        
        # Connection pool
        self.connections = []
        self.connection_semaphore = asyncio.Semaphore(max_connections)
        
        # Query cache
        self.query_cache = {}
        self.cache_timestamps = {}
        
        logger.info(f"Snowflake Query Tool initialized with {max_connections} max connections")
    
    def _load_default_config(self) -> Dict:
        """Load default configuration from environment or defaults."""
        # In production, load from environment variables or config file
        return {
            "account": "your_account",
            "user": "your_username",
            "password": "your_password",
            "warehouse": "SALES_WH",
            "database": "SALES_DB",
            "schema": "ANALYTICS",
            "role": "SALES_ANALYST"
        }
    
    def _get_connection(self) -> snowflake.connector.SnowflakeConnection:
        """Get a Snowflake connection from pool or create new one."""
        try:
            # Try to get existing connection
            for conn in self.connections:
                if not conn.is_closed():
                    return conn
            
            # Create new connection
            conn = snowflake.connector.connect(**self.config)
            self.connections.append(conn)
            logger.debug("Created new Snowflake connection")
            return conn
            
        except Exception as e:
            logger.error(f"Failed to create Snowflake connection: {str(e)}")
            raise
    
    async def execute_query(
        self,
        query: str,
        params: Optional[Dict] = None,
        use_cache: bool = True,
        format_results: bool = True,
        max_rows: int = 10000
    ) -> Union[List[Dict], pd.DataFrame, str]:
        """
        Execute a query against Snowflake.
        
        Args:
            query: SQL query to execute
            params: Query parameters for parameterized queries
            use_cache: Whether to use query result caching
            format_results: Whether to format results as list of dicts
            max_rows: Maximum number of rows to return
            
        Returns:
            Query results in specified format
        """
        # Check cache first
        cache_key = self._create_cache_key(query, params)
        if use_cache and cache_key in self.query_cache:
            cached_time = self.cache_timestamps.get(cache_key)
            if cached_time and (datetime.now() - cached_time).seconds < self.cache_ttl:
                logger.debug(f"Using cached results for query: {query[:100]}...")
                return self.query_cache[cache_key]
        
        async with self.connection_semaphore:
            try:
                start_time = datetime.now()
                conn = await asyncio.to_thread(self._get_connection)
                
                # Execute query
                cursor = conn.cursor(DictCursor)
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Fetch results
                if cursor.description:  # SELECT query
                    results = cursor.fetchmany(max_rows)
                    
                    if format_results:
                        formatted_results = self._format_results(results)
                    else:
                        formatted_results = results
                    
                    # Get query metadata
                    query_id = cursor.sfqid
                    row_count = cursor.rowcount
                    columns = [col[0] for col in cursor.description] if cursor.description else []
                    
                    logger.info(
                        f"Query executed successfully: {query_id}, "
                        f"rows: {row_count}, "
                        f"time: {(datetime.now() - start_time).total_seconds():.2f}s"
                    )
                    
                    # Cache results
                    if use_cache:
                        self.query_cache[cache_key] = formatted_results
                        self.cache_timestamps[cache_key] = datetime.now()
                    
                    return formatted_results
                    
                else:  # Non-SELECT query (INSERT, UPDATE, DELETE)
                    row_count = cursor.rowcount
                    query_id = cursor.sfqid
                    
                    logger.info(
                        f"Non-SELECT query executed: {query_id}, "
                        f"rows affected: {row_count}, "
                        f"time: {(datetime.now() - start_time).total_seconds():.2f}s"
                    )
                    
                    return {"rows_affected": row_count, "query_id": query_id}
                
            except (DatabaseError, ProgrammingError) as e:
                logger.error(f"Snowflake query error: {str(e)}")
                logger.error(f"Query: {query}")
                if params:
                    logger.error(f"Parameters: {params}")
                raise
            
            except Exception as e:
                logger.error(f"Unexpected error executing query: {str(e)}")
                raise
    
    def _create_cache_key(self, query: str, params: Optional[Dict]) -> str:
        """Create cache key from query and parameters."""
        key_parts = [query]
        if params:
            key_parts.append(json.dumps(params, sort_keys=True))
        return hash("".join(key_parts))
    
    def _format_results(self, results: List[Dict]) -> List[Dict]:
        """Format query results for consistency."""
        formatted = []
        for row in results:
            formatted_row = {}
            for key, value in row.items():
                # Convert Snowflake-specific types to Python types
                if isinstance(value, (datetime, date)):
                    formatted_row[key] = value.isoformat()
                elif isinstance(value, (Decimal, float)):
                    formatted_row[key] = float(value)
                elif value is None:
                    formatted_row[key] = None
                else:
                    formatted_row[key] = str(value)
            formatted.append(formatted_row)
        return formatted
    
    async def execute_parameterized_query(
        self,
        query_template: str,
        param_sets: List[Dict],
        batch_size: int = 100
    ) -> List[Any]:
        """
        Execute parameterized query with multiple parameter sets.
        
        Args:
            query_template: SQL query template with placeholders
            param_sets: List of parameter dictionaries
            batch_size: Number of queries to execute per batch
            
        Returns:
            List of results for each parameter set
        """
        results = []
        
        # Process in batches
        for i in range(0, len(param_sets), batch_size):
            batch = param_sets[i:i + batch_size]
            batch_tasks = []
            
            for params in batch:
                task = self.execute_query(query_template, params, use_cache=False)
                batch_tasks.append(task)
            
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            results.extend(batch_results)
        
        return results
    
    async def execute_transaction(
        self,
        queries: List[Dict[str, Any]],
        rollback_on_error: bool = True
    ) -> Dict:
        """
        Execute multiple queries in a transaction.
        
        Args:
            queries: List of query dictionaries with 'sql' and 'params' keys
            rollback_on_error: Whether to rollback on error
            
        Returns:
            Transaction execution result
        """
        async with self.connection_semaphore:
            try:
                conn = await asyncio.to_thread(self._get_connection)
                cursor = conn.cursor()
                
                results = []
                start_time = datetime.now()
                
                # Begin transaction
                cursor.execute("BEGIN TRANSACTION")
                
                for query_info in queries:
                    sql = query_info['sql']
                    params = query_info.get('params')
                    
                    try:
                        if params:
                            cursor.execute(sql, params)
                        else:
                            cursor.execute(sql)
                        
                        if cursor.description:  # SELECT query
                            query_results = cursor.fetchall()
                            results.append({
                                "query": sql[:100] + "..." if len(sql) > 100 else sql,
                                "type": "select",
                                "row_count": len(query_results),
                                "results": query_results[:10]  # Include first 10 rows
                            })
                        else:
                            results.append({
                                "query": sql[:100] + "..." if len(sql) > 100 else sql,
                                "type": "dml",
                                "rows_affected": cursor.rowcount
                            })
                        
                    except Exception as e:
                        logger.error(f"Error in transaction query: {str(e)}")
                        if rollback_on_error:
                            cursor.execute("ROLLBACK")
                            raise
                        else:
                            continue
                
                # Commit transaction
                cursor.execute("COMMIT")
                
                return {
                    "success": True,
                    "results": results,
                    "total_queries": len(queries),
                    "execution_time": (datetime.now() - start_time).total_seconds()
                }
                
            except Exception as e:
                logger.error(f"Transaction failed: {str(e)}")
                return {
                    "success": False,
                    "error": str(e),
                    "results": results
                }
    
    async def get_table_info(
        self,
        table_name: str,
        schema: Optional[str] = None,
        database: Optional[str] = None
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
        if not schema:
            schema = self.config.get("schema", "ANALYTICS")
        if not database:
            database = self.config.get("database", "SALES_DB")
        
        query = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            comment
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{table_name}'
        AND table_schema = '{schema}'
        ORDER BY ordinal_position
        """
        
        return await self.execute_query(query)
    
    async def get_query_explain_plan(self, query: str) -> Dict:
        """
        Get query explain plan for optimization.
        
        Args:
            query: SQL query
            
        Returns:
            Explain plan information
        """
        explain_query = f"EXPLAIN USING TEXT {query}"
        
        try:
            result = await self.execute_query(explain_query, use_cache=False)
            return {
                "explain_plan": result,
                "original_query": query
            }
        except Exception as e:
            logger.error(f"Error getting explain plan: {str(e)}")
            return {"error": str(e)}
    
    async def get_query_history(
        self,
        user: Optional[str] = None,
        hours: int = 24,
        limit: int = 100
    ) -> List[Dict]:
        """
        Get query execution history.
        
        Args:
            user: Filter by user (optional)
            hours: Hours to look back
            limit: Maximum number of queries to return
            
        Returns:
            List of query history entries
        """
        query = f"""
        SELECT 
            query_id,
            query_text,
            user_name,
            warehouse_name,
            database_name,
            schema_name,
            execution_status,
            error_code,
            error_message,
            start_time,
            end_time,
            total_elapsed_time,
            bytes_scanned,
            rows_produced
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE start_time >= DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
        """
        
        if user:
            query += f" AND user_name = '{user}'"
        
        query += f" ORDER BY start_time DESC LIMIT {limit}"
        
        return await self.execute_query(query)
    
    async def monitor_warehouse_usage(
        self,
        warehouse: Optional[str] = None,
        hours: int = 24
    ) -> List[Dict]:
        """
        Monitor warehouse usage.
        
        Args:
            warehouse: Warehouse name (optional)
            hours: Hours to look back
            
        Returns:
            List of warehouse usage metrics
        """
        query = f"""
        SELECT 
            warehouse_name,
            DATE_TRUNC('hour', start_time) as usage_hour,
            SUM(credits_used) as credits_used,
            AVG(running) as avg_running_queries,
            AVG(queued) as avg_queued_queries,
            COUNT(*) as query_count
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE start_time >= DATEADD(hour, -{hours}, CURRENT_TIMESTAMP())
        """
        
        if warehouse:
            query += f" AND warehouse_name = '{warehouse}'"
        
        query += """
        GROUP BY warehouse_name, DATE_TRUNC('hour', start_time)
        ORDER BY usage_hour DESC, credits_used DESC
        """
        
        return await self.execute_query(query)
    
    async def get_data_quality_metrics(
        self,
        table_name: str,
        schema: Optional[str] = None
    ) -> Dict:
        """
        Get data quality metrics for a table.
        
        Args:
            table_name: Table name
            schema: Schema name (optional)
            
        Returns:
            Data quality metrics
        """
        if not schema:
            schema = self.config.get("schema", "ANALYTICS")
        
        queries = {
            "row_count": f"SELECT COUNT(*) as row_count FROM {schema}.{table_name}",
            "null_counts": f"""
            SELECT 
                column_name,
                SUM(CASE WHEN {table_name}.COLUMN_NAME IS NULL THEN 1 ELSE 0 END) as null_count,
                COUNT(*) as total_rows,
                (null_count / total_rows * 100) as null_percentage
            FROM {schema}.{table_name}
            CROSS JOIN (SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS 
                       WHERE table_name = '{table_name}' AND table_schema = '{schema}')
            GROUP BY column_name
            HAVING null_count > 0
            """,
            "date_range": f"""
            SELECT 
                MIN(transaction_timestamp) as min_date,
                MAX(transaction_timestamp) as max_date,
                COUNT(DISTINCT DATE(transaction_timestamp)) as unique_days
            FROM {schema}.{table_name}
            WHERE transaction_timestamp IS NOT NULL
            """
        }
        
        results = {}
        for metric_name, query in queries.items():
            try:
                result = await self.execute_query(query)
                results[metric_name] = result
            except Exception as e:
                results[metric_name] = {"error": str(e)}
        
        return results
    
    def clear_cache(self, pattern: Optional[str] = None):
        """
        Clear query cache.
        
        Args:
            pattern: Optional pattern to match cache keys
        """
        if pattern:
            keys_to_remove = [
                key for key in self.query_cache.keys()
                if pattern in str(key)
            ]
            for key in keys_to_remove:
                del self.query_cache[key]
                del self.cache_timestamps[key]
            logger.info(f"Cleared {len(keys_to_remove)} cache entries matching pattern: {pattern}")
        else:
            self.query_cache.clear()
            self.cache_timestamps.clear()
            logger.info("Cleared all cache entries")
    
    async def close_connections(self):
        """Close all Snowflake connections."""
        for conn in self.connections:
            try:
                conn.close()
            except:
                pass
        
        self.connections.clear()
        logger.info("Closed all Snowflake connections")
    
    def get_tool_status(self) -> Dict:
        """Get tool status and statistics."""
        return {
            "active_connections": len([c for c in self.connections if not c.is_closed()]),
            "cache_size": len(self.query_cache),
            "max_connections": self.max_connections,
            "cache_ttl": self.cache_ttl,
            "timeout": self.timeout,
            "config": {
                k: "***" if "password" in k.lower() else v 
                for k, v in self.config.items()
            }
        }