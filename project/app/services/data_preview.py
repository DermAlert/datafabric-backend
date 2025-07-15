from typing import Dict, Any, List, Tuple
import asyncio
import logging

# Import specific preview handlers
from .previewers.postgres_previewer import preview_postgres_data
from .previewers.delta_previewer import preview_delta_data
# from .previewers.mongodb_previewer import preview_mongodb_data
# from .previewers.mysql_previewer import preview_mysql_data
# from .previewers.minio_previewer import preview_minio_data
# from .previewers.bigquery_previewer import preview_bigquery_data

# Map connection types to their preview handlers
PREVIEWERS = {
    "postgresql": preview_postgres_data,
    "postgres": preview_postgres_data,
    "deltalake": preview_delta_data,
    "delta": preview_delta_data,
    # "mongodb": preview_mongodb_data,
    # "minio": preview_minio_data,
    # "s3": preview_minio_data,
    # "mysql": preview_mysql_data,
    # "bigquery": preview_bigquery_data,
    # Add more previewers as needed
}

async def get_data_preview(
    connection_type: str,
    connection_params: Dict[str, Any],
    schema_name: str,
    table_name: str,
    limit: int = 100
) -> Tuple[List[List[Any]], List[str]]:
    """
    Get a preview of data from a table.
    
    Args:
        connection_type: Type of connection (postgres, mongodb, etc.)
        connection_params: Connection parameters
        schema_name: Schema name
        table_name: Table name
        limit: Maximum number of rows to return
        
    Returns:
        Tuple of (rows, column_names)
        - rows is a list of lists, where each inner list represents a row
        - column_names is a list of column names
    """
    connection_type_lower = connection_type.lower()
    previewer = PREVIEWERS.get(connection_type_lower)
    
    if not previewer:
        raise ValueError(f"Unsupported connection type for preview: {connection_type}")
        
    return await previewer(connection_params, schema_name, table_name, limit)