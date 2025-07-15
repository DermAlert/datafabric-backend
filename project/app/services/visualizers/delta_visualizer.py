from typing import Dict, Any, List, Tuple, Optional
import asyncio

from ..connectors.delta_connector import get_spark_session
from ...utils.logger import logger
from ...api.schemas.data_visualization_schemas import Filter, Sort


async def visualize_delta_data(
    connection_params: Dict[str, Any],
    schema_name: str,
    table_name: str,
    page: int = 1,
    page_size: int = 50,
    filters: Optional[List[Filter]] = None,
    sort_by: Optional[List[Sort]] = None,
    selected_columns: Optional[List[str]] = None,
    column_info: Optional[Dict[str, Any]] = None
) -> Tuple[int, List[List[Any]], List[str]]:
    """
    Get virtualized data from a Delta Lake table with pagination, filtering, and sorting.
    
    Args:
        connection_params: Delta Lake connection parameters
        schema_name: Schema/database name or bucket path
        table_name: Table name
        page: Page number (1-based)
        page_size: Number of rows per page
        filters: Filters to apply
        sort_by: Sort specifications
        selected_columns: Columns to include (None for all)
        column_info: Information about table columns
        
    Returns:
        Tuple of (total_count, rows, column_names)
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
        
        # Select specific columns if requested
        if selected_columns:
            available_columns = df.columns
            valid_columns = [col for col in selected_columns if col in available_columns]
            if valid_columns:
                df = df.select(*valid_columns)
        
        column_names = df.columns
        
        # Apply filters
        if filters:
            for filter_obj in filters:
                df = _apply_filter(df, filter_obj)
        
        # Get total count before pagination
        total_count = df.count()
        
        # Apply sorting
        if sort_by:
            for sort_obj in sort_by:
                if sort_obj.column in column_names:
                    if sort_obj.direction.lower() == "desc":
                        df = df.orderBy(df[sort_obj.column].desc())
                    else:
                        df = df.orderBy(df[sort_obj.column].asc())
        
        # Apply pagination
        offset = (page - 1) * page_size
        paginated_df = df.offset(offset).limit(page_size)
        
        # Collect data
        sample_data = paginated_df.collect()
        
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
        
        return total_count, rows, column_names
        
    except Exception as e:
        logger.error(f"Error visualizing Delta Lake data from {schema_name}.{table_name}: {str(e)}")
        raise Exception(f"Failed to visualize Delta Lake data: {str(e)}")
        
    finally:
        if spark:
            try:
                spark.stop()
            except Exception as e:
                logger.warning(f"Error stopping Spark session: {str(e)}")


async def execute_delta_query(
    connection_params: Dict[str, Any],
    query: str,
    max_rows: int = 1000
) -> Tuple[List[List[Any]], List[str]]:
    """
    Execute a custom Spark SQL query on Delta Lake.
    
    Args:
        connection_params: Delta Lake connection parameters
        query: Spark SQL query to execute
        max_rows: Maximum number of rows to return
        
    Returns:
        Tuple of (rows, column_names)
    """
    spark = None
    try:
        spark = get_spark_session(connection_params)
        
        # Execute the query
        df = spark.sql(query)
        
        # Limit results
        df_limited = df.limit(max_rows)
        
        # Get column names
        column_names = df.columns
        
        # Collect data
        data = df_limited.collect()
        
        # Convert to list of lists
        rows = []
        for row in data:
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
        logger.error(f"Error executing Delta Lake query: {str(e)}")
        raise Exception(f"Failed to execute Delta Lake query: {str(e)}")
        
    finally:
        if spark:
            try:
                spark.stop()
            except Exception as e:
                logger.warning(f"Error stopping Spark session: {str(e)}")


def _apply_filter(df, filter_obj: Filter):
    """Apply a filter to the DataFrame."""
    try:
        column = filter_obj.column
        operator = filter_obj.operator.lower()
        value = filter_obj.value
        
        if operator == "equals":
            return df.filter(df[column] == value)
        elif operator == "not_equals":
            return df.filter(df[column] != value)
        elif operator == "greater_than":
            return df.filter(df[column] > value)
        elif operator == "greater_than_equals":
            return df.filter(df[column] >= value)
        elif operator == "less_than":
            return df.filter(df[column] < value)
        elif operator == "less_than_equals":
            return df.filter(df[column] <= value)
        elif operator == "contains":
            return df.filter(df[column].contains(str(value)))
        elif operator == "starts_with":
            return df.filter(df[column].startswith(str(value)))
        elif operator == "ends_with":
            return df.filter(df[column].endswith(str(value)))
        elif operator == "is_null":
            return df.filter(df[column].isNull())
        elif operator == "is_not_null":
            return df.filter(df[column].isNotNull())
        elif operator == "in":
            if isinstance(value, list):
                return df.filter(df[column].isin(value))
            else:
                return df.filter(df[column].isin([value]))
        else:
            logger.warning(f"Unsupported filter operator: {operator}")
            return df
    except Exception as e:
        logger.error(f"Error applying filter: {str(e)}")
        return df
