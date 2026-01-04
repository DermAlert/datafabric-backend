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

from .extractors.trino_extractor import TrinoExtractor
from ..utils.logger import logger
# from .connectors.delta_connector import get_spark_session


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
        search: Optional[str] = None,
        catalog_name: Optional[str] = None
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
            catalog_name: Catalog name (required for Delta Lake via Trino)
            
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
                connection_params, schema_name, table_name, column_name, limit, search, catalog_name
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
        search: Optional[str] = None,
        catalog_name: Optional[str] = None
    ) -> List[Any]:
        """Get distinct values from Delta Lake using Trino (async with aiotrino)."""
        try:
            # Use TrinoExtractor to get connection
            extractor = TrinoExtractor(
                connection_params=connection_params,
                connection_type="deltalake",
                connection_name="distinct_values_query"
            )
            
            # Use provided catalog_name or fall back to extractor's catalog
            catalog = catalog_name if catalog_name else extractor.catalog
            
            # Build query for Trino
            query = f'SELECT DISTINCT "{column_name}" FROM "{catalog}"."{schema_name}"."{table_name}"'
            
            if search:
                # Trino LIKE is case-sensitive, use lower() for case-insensitive
                # Prevent SQL injection by simple sanitization (basic, improvement needed for prod)
                safe_search = search.replace("'", "''")
                query += f' WHERE lower(cast("{column_name}" as varchar)) LIKE \'%{safe_search.lower()}%\''
            
            query += f' ORDER BY "{column_name}" LIMIT {limit}'
            
            logger.debug(f"Executing Trino query: {query}")
            
            # Execute query using async connection (aiotrino)
            conn = await extractor._get_connection()
            cur = await conn.cursor()
            await cur.execute(query)
            rows = await cur.fetchall()
            
            # Extract values
            values = []
            for row in rows:
                values.append(row[0])
            
            await conn.close()
            return values
            
        except Exception as e:
            logger.error(f"Error fetching distinct values from Delta Lake via Trino: {str(e)}")
            raise Exception(f"Failed to fetch distinct values from Delta Lake: {str(e)}")

    
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
