"""
Service for retrieving distinct values from different data sources.
Provides a unified interface for fetching distinct column values from various database types.
"""

from typing import Dict, Any, List, Optional, Tuple
import asyncio

# Import required dependencies with error handling
try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False
    asyncpg = None

from ..utils.logger import logger
from .connectors.delta_connector import get_spark_session


class DistinctValuesService:
    """Service to fetch distinct values from different data source types."""
    
    @staticmethod
    async def get_distinct_values(
        connection_type: str,
        connection_params: Dict[str, Any],
        schema_name: str,
        table_name: str,
        column_name: str,
        limit: int = 100,
        search: Optional[str] = None
    ) -> List[Any]:
        """
        Get distinct values for a column from any supported data source.
        
        Args:
            connection_type: Type of connection (postgres, delta, etc.)
            connection_params: Connection parameters
            schema_name: Schema name
            table_name: Table name  
            column_name: Column name
            limit: Maximum number of distinct values to return
            search: Optional search term to filter values (case insensitive partial match)
            
        Returns:
            List of distinct values
            
        Raises:
            ValueError: If connection type is not supported
            Exception: If there's an error fetching values
        """
        connection_type_lower = connection_type.lower()
        
        if connection_type_lower in ["postgresql", "postgres"]:
            return await DistinctValuesService._get_postgres_distinct_values(
                connection_params, schema_name, table_name, column_name, limit, search
            )
        elif connection_type_lower in ["deltalake", "delta"]:
            return await DistinctValuesService._get_delta_distinct_values(
                connection_params, schema_name, table_name, column_name, limit, search
            )
        else:
            raise ValueError(f"Unsupported connection type: {connection_type}")
    
    @staticmethod
    async def _get_postgres_distinct_values(
        connection_params: Dict[str, Any],
        schema_name: str,
        table_name: str,
        column_name: str,
        limit: int,
        search: Optional[str] = None
    ) -> List[Any]:
        """Get distinct values from PostgreSQL."""
        if not ASYNCPG_AVAILABLE:
            raise Exception("asyncpg is not available. Cannot connect to PostgreSQL.")
            
        pg_conn = None
        try:
            # Connect to PostgreSQL
            pg_conn = await asyncpg.connect(
                host=connection_params.get("host"),
                port=connection_params.get("port", 5432),
                database=connection_params.get("database"),
                user=connection_params.get("username"),
                password=connection_params.get("password"),
                ssl=connection_params.get("ssl", False)
            )
            
            # Build query with optional search filter
            if search:
                # Use parameterized query to prevent SQL injection
                query = f'''
                    SELECT DISTINCT "{column_name}" 
                    FROM "{schema_name}"."{table_name}" 
                    WHERE CAST("{column_name}" AS TEXT) ILIKE $1 
                    ORDER BY "{column_name}" 
                    LIMIT $2
                '''
                search_param = f'%{search}%'
                rows = await pg_conn.fetch(query, search_param, limit)
            else:
                query = f'''
                    SELECT DISTINCT "{column_name}" 
                    FROM "{schema_name}"."{table_name}" 
                    ORDER BY "{column_name}" 
                    LIMIT $1
                '''
                rows = await pg_conn.fetch(query, limit)
            
            # Extract values from rows
            values = [row[column_name] for row in rows]
            return values
            
        except Exception as e:
            logger.error(f"Error fetching distinct values from PostgreSQL: {str(e)}")
            raise Exception(f"Failed to fetch distinct values from PostgreSQL: {str(e)}")
        finally:
            if pg_conn:
                await pg_conn.close()
    
    @staticmethod
    async def _get_delta_distinct_values(
        connection_params: Dict[str, Any],
        schema_name: str,
        table_name: str,
        column_name: str,
        limit: int,
        search: Optional[str] = None
    ) -> List[Any]:
        """Get distinct values from Delta Lake."""
        spark = None
        try:
            # Get Spark session
            spark = get_spark_session(connection_params)
            
            # Determine table path
            bucket_name = connection_params.get('bucket_name')
            
            if schema_name == bucket_name:
                # S3 bucket table
                if table_name == bucket_name:
                    table_path = f"s3a://{bucket_name}/"
                else:
                    table_path = f"s3a://{bucket_name}/{table_name}"
            else:
                # Database table
                table_path = f"{schema_name}.{table_name}"
            
            # Read Delta table and get distinct values
            df = spark.read.format("delta").load(table_path)
            
            # Apply search filter if provided
            if search:
                # Add case-insensitive search filter
                from pyspark.sql.functions import col, lower
                df = df.filter(lower(col(column_name).cast("string")).contains(search.lower()))
            
            # Get distinct values for the column
            distinct_df = df.select(column_name).distinct().orderBy(column_name).limit(limit)
            rows = distinct_df.collect()
            
            # Extract values
            values = []
            for row in rows:
                value = row[column_name]
                # Convert complex types to string representation
                if value is not None and not isinstance(value, (str, int, float, bool)):
                    value = str(value)
                values.append(value)
            
            return values
            
        except Exception as e:
            logger.error(f"Error fetching distinct values from Delta Lake: {str(e)}")
            raise Exception(f"Failed to fetch distinct values from Delta Lake: {str(e)}")
        finally:
            if spark:
                try:
                    spark.stop()
                except Exception as e:
                    logger.warning(f"Error stopping Spark session: {str(e)}")
    
    @staticmethod
    async def is_connection_type_supported(connection_type: str) -> bool:
        """Check if a connection type is supported for distinct values queries."""
        supported_types = ["postgresql", "postgres", "deltalake", "delta"]
        return connection_type.lower() in supported_types
    
    @staticmethod
    def get_supported_connection_types() -> List[str]:
        """Get list of supported connection types."""
        return ["postgresql", "postgres", "deltalake", "delta"]


# Convenience functions for backwards compatibility
async def get_distinct_values_for_connection(
    connection_type: str,
    connection_params: Dict[str, Any],
    schema_name: str,
    table_name: str,
    column_name: str,
    limit: int = 100
) -> List[Any]:
    """
    Convenience function to get distinct values.
    Delegates to DistinctValuesService.get_distinct_values().
    """
    return await DistinctValuesService.get_distinct_values(
        connection_type, connection_params, schema_name, table_name, column_name, limit
    )


# ====================================================================================
# INSTRUCTIONS FOR ADDING NEW DATA SOURCE TYPES
# ====================================================================================
"""
To add support for a new data source type (e.g., MySQL, MongoDB, BigQuery, etc.):

1. Add the connection type to the main get_distinct_values() method:
   ```python
   elif connection_type_lower in ["mysql"]:
       return await DistinctValuesService._get_mysql_distinct_values(
           connection_params, schema_name, table_name, column_name, limit
       )
   ```

2. Implement the specific method for your data source:
   ```python
   @staticmethod
   async def _get_mysql_distinct_values(
       connection_params: Dict[str, Any],
       schema_name: str,
       table_name: str,
       column_name: str,
       limit: int
   ) -> List[Any]:
       # Implement MySQL-specific distinct values logic here
       pass
   ```

3. Update the get_supported_connection_types() method to include the new type:
   ```python
   return ["postgresql", "postgres", "deltalake", "delta", "mysql"]
   ```

4. Make sure to handle connection parameters appropriately for your data source
5. Add proper error handling and logging
6. Test the implementation thoroughly

Example implementations:
- PostgreSQL: Uses asyncpg with standard SQL DISTINCT query
- Delta Lake: Uses Spark with DataFrame distinct() operation
- For other SQL databases: Similar to PostgreSQL but with appropriate connector
- For NoSQL databases: Use database-specific aggregation/distinct operations
"""
