from typing import Dict, Any, List, Tuple, Optional

from ...infrastructure.trino_extractor import TrinoExtractor
from ....utils.logger import logger
from ....api.schemas.data_visualization_schemas import Filter, Sort


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
    Using Trino as the query engine (async with aiotrino).
    """
    try:
        # Use TrinoExtractor to setup connection
        extractor = TrinoExtractor(
            connection_params=connection_params,
            connection_type="deltalake",
            connection_name="visualization_query"
        )
        
        # Construct Trino SQL query
        catalog = extractor.catalog
        
        # Basic Select
        select_clause = "*"
        if selected_columns:
            # Sanitize column names
            quoted_cols = [f'"{col}"' for col in selected_columns]
            select_clause = ", ".join(quoted_cols)
            
        base_query = f' FROM "{catalog}"."{schema_name}"."{table_name}"'
        
        # Apply filters
        where_clause = ""
        if filters:
            conditions = []
            for filter_obj in filters:
                condition = _build_trino_filter(filter_obj)
                if condition:
                    conditions.append(condition)
            
            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
        
        # Get total count
        count_query = f'SELECT count(*) {base_query} {where_clause}'
        
        # Use async connection (aiotrino)
        conn = await extractor._get_connection()
        cur = await conn.cursor()
        await cur.execute(count_query)
        result = await cur.fetchone()
        total_count = result[0]
        
        # Build final data query
        query = f'SELECT {select_clause} {base_query} {where_clause}'
        
        # Apply sorting
        if sort_by:
            sort_conditions = []
            for sort_obj in sort_by:
                direction = "DESC" if sort_obj.direction.lower() == "desc" else "ASC"
                sort_conditions.append(f'"{sort_obj.column}" {direction}')
            
            if sort_conditions:
                query += " ORDER BY " + ", ".join(sort_conditions)
        
        # Apply pagination
        offset = (page - 1) * page_size
        query += f" OFFSET {offset} LIMIT {page_size}"
        
        # Execute data query
        await cur.execute(query)
        rows = await cur.fetchall()
        
        # Get column names from aiotrino cursor using get_description() method
        column_names = []
        description = await cur.get_description() if hasattr(cur, 'get_description') else None
        if description:
            column_names = [col.name if hasattr(col, 'name') else (col.get('name') if isinstance(col, dict) else col[0]) for col in description]
        elif selected_columns:
            column_names = selected_columns
        
        # Convert rows to list of lists and handle types
        formatted_rows = []
        for row in rows:
            row_data = []
            for val in row:
                if val is not None and not isinstance(val, (str, int, float, bool)):
                    row_data.append(str(val))
                else:
                    row_data.append(val)
            formatted_rows.append(row_data)
        
        await conn.close()
        return total_count, formatted_rows, column_names
        
    except Exception as e:
        logger.error(f"Error visualizing Delta Lake data from {schema_name}.{table_name}: {str(e)}")
        raise Exception(f"Failed to visualize Delta Lake data: {str(e)}")


async def execute_delta_query(
    connection_params: Dict[str, Any],
    query: str,
    max_rows: int = 1000
) -> Tuple[List[List[Any]], List[str]]:
    """
    Execute a custom SQL query on Delta Lake via Trino (async with aiotrino).
    """
    try:
        # Use TrinoExtractor to setup connection
        extractor = TrinoExtractor(
            connection_params=connection_params,
            connection_type="deltalake",
            connection_name="custom_query"
        )
        
        # Note: The incoming query might be Spark SQL. Trino SQL is ANSI SQL compliant but might differ slightly.
        # We assume the user provides Trino-compatible SQL or simple SQL.
        # If the query assumes a specific catalog name that differs from our dynamic one, this might fail.
        # ideally we should inject the catalog name.
        
        conn = await extractor._get_connection()
        cur = await conn.cursor()
        
        # Limit results if not present
        if "limit" not in query.lower():
            query += f" LIMIT {max_rows}"
            
        await cur.execute(query)
        rows = await cur.fetchall()
        
        # Get column names from aiotrino cursor using get_description() method
        column_names = []
        description = await cur.get_description() if hasattr(cur, 'get_description') else None
        if description:
            column_names = [col.name if hasattr(col, 'name') else (col.get('name') if isinstance(col, dict) else col[0]) for col in description]
        
        formatted_rows = []
        for row in rows:
            row_data = []
            for val in row:
                if val is not None and not isinstance(val, (str, int, float, bool)):
                    row_data.append(str(val))
                else:
                    row_data.append(val)
            formatted_rows.append(row_data)
        
        await conn.close()
        return formatted_rows, column_names
        
    except Exception as e:
        logger.error(f"Error executing Delta Lake query: {str(e)}")
        raise Exception(f"Failed to execute Delta Lake query: {str(e)}")


def _build_trino_filter(filter_obj: Filter) -> Optional[str]:
    """Build Trino SQL filter condition."""
    try:
        column = f'"{filter_obj.column}"'
        operator = filter_obj.operator.lower()
        value = filter_obj.value
        
        # Helper to format value
        def fmt_val(v):
            if isinstance(v, str):
                return f"'{v}'"
            return str(v)

        if operator == "equals":
            return f"{column} = {fmt_val(value)}"
        elif operator == "not_equals":
            return f"{column} <> {fmt_val(value)}"
        elif operator == "greater_than":
            return f"{column} > {fmt_val(value)}"
        elif operator == "greater_than_equals":
            return f"{column} >= {fmt_val(value)}"
        elif operator == "less_than":
            return f"{column} < {fmt_val(value)}"
        elif operator == "less_than_equals":
            return f"{column} <= {fmt_val(value)}"
        elif operator == "contains":
            return f"CAST({column} AS VARCHAR) LIKE '%{value}%'"
        elif operator == "starts_with":
            return f"CAST({column} AS VARCHAR) LIKE '{value}%'"
        elif operator == "ends_with":
            return f"CAST({column} AS VARCHAR) LIKE '%{value}'"
        elif operator == "is_null":
            return f"{column} IS NULL"
        elif operator == "is_not_null":
            return f"{column} IS NOT NULL"
        elif operator == "in":
            if isinstance(value, list):
                vals = ", ".join([fmt_val(v) for v in value])
                return f"{column} IN ({vals})"
            else:
                return f"{column} IN ({fmt_val(value)})"
        else:
            logger.warning(f"Unsupported filter operator: {operator}")
            return None
    except Exception as e:
        logger.error(f"Error building filter: {str(e)}")
        return None
