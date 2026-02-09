from typing import Dict, Any, List, Tuple
import asyncio

from ...infrastructure.delta_connector import get_spark_session
from ....utils.logger import logger


async def preview_delta_data(
    connection_params: Dict[str, Any],
    schema_name: str,
    table_name: str,
    limit: int = 100
) -> Tuple[List[List[Any]], List[str]]:
    """
    Preview data from a Delta Lake table.
    
    Args:
        connection_params: Delta Lake connection parameters
        schema_name: Schema/database name or bucket path
        table_name: Table name
        limit: Maximum number of rows to return
        
    Returns:
        Tuple of (rows, column_names)
    """
    spark = None
    try:
        spark = get_spark_session(connection_params)
        
        # Determine the table path
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
        
        # Read the Delta table
        df = spark.read.format("delta").load(table_path)
        
        # Get column names
        column_names = df.columns
        
        # Get sample data
        sample_data = df.limit(limit).collect()
        
        # Convert to list of lists
        rows = []
        for row in sample_data:
            row_data = []
            for col in column_names:
                value = row[col]
                # Convert complex types to string representation
                if value is not None and not isinstance(value, (str, int, float, bool)):
                    value = str(value)
                row_data.append(value)
            rows.append(row_data)
        
        return rows, column_names
        
    except Exception as e:
        logger.error(f"Error previewing Delta Lake data from {schema_name}.{table_name}: {str(e)}")
        raise Exception(f"Failed to preview Delta Lake data: {str(e)}")
        
    finally:
        if spark:
            try:
                spark.stop()
            except Exception as e:
                logger.warning(f"Error stopping Spark session: {str(e)}")
