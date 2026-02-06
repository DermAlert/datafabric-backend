from typing import Dict, Any, List, Tuple, Optional, Union
import time
import asyncio
from datetime import datetime

from ...api.schemas.data_visualization_schemas import Filter, Sort

# Import specific visualizers
from .visualizers.postgres_visualizer import visualize_postgres_data, execute_postgres_query
from .visualizers.delta_visualizer import visualize_delta_data, execute_delta_query
# from .visualizers.mongodb_visualizer import visualize_mongodb_data, execute_mongodb_query
# from .visualizers.mysql_visualizer import visualize_mysql_data, execute_mysql_query
# from .visualizers.minio_visualizer import visualize_minio_data
# from .visualizers.bigquery_visualizer import visualize_bigquery_data, execute_bigquery_query

# Map connection types to their visualizer functions
DATA_VISUALIZERS = {
    "postgresql": visualize_postgres_data,
    "postgres": visualize_postgres_data,
    "deltalake": visualize_delta_data,
    "delta": visualize_delta_data,
    # "mongodb": visualize_mongodb_data,
    # "minio": visualize_minio_data,
    # "s3": visualize_minio_data,
    # "mysql": visualize_mysql_data,
    # "bigquery": visualize_bigquery_data,
    # Add more visualizers as needed
}

# Map connection types to their query execution functions
QUERY_EXECUTORS = {
    "postgresql": execute_postgres_query,
    "postgres": execute_postgres_query,
    "deltalake": execute_delta_query,
    "delta": execute_delta_query,
    # "mongodb": execute_mongodb_query,
    # "mysql": execute_mysql_query,
    # "bigquery": execute_bigquery_query,
    # Add more executors as needed
}

async def get_visualized_data(
    connection_type: str,
    connection_params: Dict[str, Any],
    schema_name: str,
    table_name: str,
    page: int = 1,
    page_size: int = 50,
    filters: Optional[List[Filter]] = None,
    sort_by: Optional[List[Sort]] = None,
    selected_columns: Optional[List[str]] = None,
    column_info: Optional[Dict[str, Any]] = None
) -> Tuple[int, List[List[Any]], List[str], float]:
    """
    Get virtualized data from a table with pagination, filtering, and sorting.
    
    Args:
        connection_type: Type of connection
        connection_params: Connection parameters
        schema_name: Schema name
        table_name: Table name
        page: Page number (1-based)
        page_size: Number of rows per page
        filters: Filters to apply
        sort_by: Sort specifications
        selected_columns: Columns to include (None for all)
        column_info: Information about table columns (for type validation)
        
    Returns:
        Tuple of (total_count, rows, column_names, execution_time_ms)
    """
    start_time = time.time()
    
    connection_type_lower = connection_type.lower()
    visualizer = DATA_VISUALIZERS.get(connection_type_lower)
    
    if not visualizer:
        raise ValueError(f"Unsupported connection type for visualization: {connection_type}")
    
    # Call the appropriate visualizer
    total_count, rows, column_names = await visualizer(
        connection_params=connection_params,
        schema_name=schema_name,
        table_name=table_name,
        page=page,
        page_size=page_size,
        filters=filters,
        sort_by=sort_by,
        selected_columns=selected_columns,
        column_info=column_info
    )
    
    execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
    
    return total_count, rows, column_names, execution_time

async def execute_custom_query(
    connection_type: str,
    connection_params: Dict[str, Any],
    query: str,
    max_rows: int = 1000
) -> Tuple[List[List[Any]], List[str], float]:
    """
    Execute a custom query on a data source.
    
    Args:
        connection_type: Type of connection
        connection_params: Connection parameters
        query: SQL or NoSQL query to execute
        max_rows: Maximum number of rows to return
        
    Returns:
        Tuple of (rows, column_names, execution_time_ms)
    """
    start_time = time.time()
    
    connection_type_lower = connection_type.lower()
    executor = QUERY_EXECUTORS.get(connection_type_lower)
    
    if not executor:
        raise ValueError(f"Unsupported connection type for custom queries: {connection_type}")
    
    # Call the appropriate query executor
    rows, column_names = await executor(
        connection_params=connection_params,
        query=query,
        max_rows=max_rows
    )
    
    execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
    
    return rows, column_names, execution_time