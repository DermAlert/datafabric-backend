from typing import Dict, Any, List, Tuple, Optional
import asyncpg
import json
from datetime import datetime, date

from ...api.schemas.data_visualization_schemas import Filter, Sort, FilterOperator, SortDirection

async def visualize_postgres_data(
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
    Get virtualized data from a PostgreSQL table with pagination, filtering, and sorting.
    """
    conn = None
    try:
        # Extract connection parameters
        host = connection_params.get("host", "localhost")
        host = "postgres-backend"
        port = connection_params.get("port", 5432)
        database = connection_params.get("database")
        username = connection_params.get("username")
        password = connection_params.get("password")
        ssl = connection_params.get("ssl", False)
        
        # Connect to PostgreSQL
        conn = await asyncpg.connect(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password,
            ssl=ssl
        )
        
        # Sanitize identifiers to prevent SQL injection
        schema_name_sanitized = await conn.fetchval(
            "SELECT quote_ident($1)", schema_name
        )
        
        table_name_sanitized = await conn.fetchval(
            "SELECT quote_ident($1)", table_name
        )
        
        # Build column list
        columns_clause = "*"
        if selected_columns:
            # Sanitize column names
            sanitized_columns = []
            for col_name in selected_columns:
                sanitized_col = await conn.fetchval(
                    "SELECT quote_ident($1)", col_name
                )
                sanitized_columns.append(sanitized_col)
            
            if sanitized_columns:
                columns_clause = ", ".join(sanitized_columns)
        
        # Build WHERE clause for filters
        where_clause = ""
        where_values = []
        if filters:
            conditions = []
            param_index = 1
            
            for filter_item in filters:
                col_name = await conn.fetchval(
                    "SELECT quote_ident($1)", filter_item.column
                )
                
                if filter_item.operator == FilterOperator.eq:
                    conditions.append(f"{col_name} = ${param_index}")
                    where_values.append(filter_item.value)
                    param_index += 1
                    
                elif filter_item.operator == FilterOperator.neq:
                    conditions.append(f"{col_name} <> ${param_index}")
                    where_values.append(filter_item.value)
                    param_index += 1
                    
                elif filter_item.operator == FilterOperator.gt:
                    conditions.append(f"{col_name} > ${param_index}")
                    where_values.append(filter_item.value)
                    param_index += 1
                    
                elif filter_item.operator == FilterOperator.gte:
                    conditions.append(f"{col_name} >= ${param_index}")
                    where_values.append(filter_item.value)
                    param_index += 1
                    
                elif filter_item.operator == FilterOperator.lt:
                    conditions.append(f"{col_name} < ${param_index}")
                    where_values.append(filter_item.value)
                    param_index += 1
                    
                elif filter_item.operator == FilterOperator.lte:
                    conditions.append(f"{col_name} <= ${param_index}")
                    where_values.append(filter_item.value)
                    param_index += 1
                    
                elif filter_item.operator == FilterOperator.like:
                    conditions.append(f"{col_name} LIKE ${param_index}")
                    where_values.append(filter_item.value)
                    param_index += 1
                    
                elif filter_item.operator == FilterOperator.ilike:
                    conditions.append(f"{col_name} ILIKE ${param_index}")
                    where_values.append(filter_item.value)
                    param_index += 1
                    
                elif filter_item.operator == FilterOperator.in_list:
                    if filter_item.values:
                        placeholders = ", ".join(f"${i}" for i in range(param_index, param_index + len(filter_item.values)))
                        conditions.append(f"{col_name} IN ({placeholders})")
                        where_values.extend(filter_item.values)
                        param_index += len(filter_item.values)
                        
                elif filter_item.operator == FilterOperator.not_in:
                    if filter_item.values:
                        placeholders = ", ".join(f"${i}" for i in range(param_index, param_index + len(filter_item.values)))
                        conditions.append(f"{col_name} NOT IN ({placeholders})")
                        where_values.extend(filter_item.values)
                        param_index += len(filter_item.values)
                        
                elif filter_item.operator == FilterOperator.is_null:
                    conditions.append(f"{col_name} IS NULL")
                    
                elif filter_item.operator == FilterOperator.is_not_null:
                    conditions.append(f"{col_name} IS NOT NULL")
                    
                elif filter_item.operator == FilterOperator.between:
                    if filter_item.values and len(filter_item.values) == 2:
                        conditions.append(f"{col_name} BETWEEN ${param_index} AND ${param_index + 1}")
                        where_values.extend(filter_item.values[:2])
                        param_index += 2
            
            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
        
        # Build ORDER BY clause
        order_clause = ""
        if sort_by:
            order_terms = []
            for sort_item in sort_by:
                col_name = await conn.fetchval(
                    "SELECT quote_ident($1)", sort_item.column
                )
                direction = "ASC" if sort_item.direction == SortDirection.asc else "DESC"
                order_terms.append(f"{col_name} {direction}")
            
            if order_terms:
                order_clause = " ORDER BY " + ", ".join(order_terms)
        
        # Calculate offset from page and page_size
        offset = (page - 1) * page_size
        
        # First, get total count
        count_query = f"""
        SELECT COUNT(*) 
        FROM {schema_name_sanitized}.{table_name_sanitized}
        {where_clause}
        """
        
        total_count = await conn.fetchval(count_query, *where_values)
        
        # Now get actual data with pagination
        data_query = f"""
        SELECT {columns_clause}
        FROM {schema_name_sanitized}.{table_name_sanitized}
        {where_clause}
        {order_clause}
        LIMIT {page_size} OFFSET {offset}
        """
        
        results = await conn.fetch(data_query, *where_values)
        
        # Determine column names from results
        column_names = []
        if results:
            column_names = [col for col in results[0].keys()]
        elif selected_columns:
            column_names = selected_columns
        else:
            # If no results and no selected columns, fetch column names from the table
            columns_query = f"""
            SELECT column_name 
            FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = $2
            ORDER BY ordinal_position
            """
            columns = await conn.fetch(columns_query, schema_name, table_name)
            column_names = [col['column_name'] for col in columns]
        
        # Convert rows to list of lists
        rows = []
        for record in results:
            row = []
            for col_name in column_names:
                value = record[col_name]
                # Convert any complex types to string to ensure JSON serializability
                if isinstance(value, (datetime, date)):
                    value = value.isoformat()
                elif value is not None and not isinstance(value, (str, int, float, bool)):
                    value = str(value)
                row.append(value)
            rows.append(row)
        
        return total_count, rows, column_names
        
    except Exception as e:
        print(f"Error visualizing PostgreSQL data: {str(e)}")
        return 0, [], []
        
    finally:
        if conn:
            await conn.close()

async def execute_postgres_query(
    connection_params: Dict[str, Any],
    query: str,
    max_rows: int = 1000
) -> Tuple[List[List[Any]], List[str]]:
    """
    Execute a custom SQL query on PostgreSQL.
    """
    conn = None
    try:
        # Extract connection parameters
        host = connection_params.get("host", "localhost")
        host = "postgres-backend"
        port = connection_params.get("port", 5432)
        database = connection_params.get("database")
        username = connection_params.get("username")
        password = connection_params.get("password")
        ssl = connection_params.get("ssl", False)
        
        # Connect to PostgreSQL
        conn = await asyncpg.connect(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password,
            ssl=ssl
        )
        
        # Execute the query with a limit to prevent huge result sets
        # We use a subquery to safely apply the limit
        if not query.lower().strip().startswith("select"):
            raise ValueError("Only SELECT queries are allowed")
        
        # Add LIMIT clause to the query if it doesn't already have one
        query_lower = query.lower()
        if "limit" not in query_lower:
            query += f" LIMIT {max_rows}"
        
        results = await conn.fetch(query)
        
        # Determine column names from results
        column_names = []
        if results:
            column_names = [col for col in results[0].keys()]
        
        # Convert rows to list of lists
        rows = []
        for record in results:
            row = []
            for col_name in column_names:
                value = record[col_name]
                # Convert any complex types to string to ensure JSON serializability
                if isinstance(value, (datetime, date)):
                    value = value.isoformat()
                elif value is not None and not isinstance(value, (str, int, float, bool)):
                    value = str(value)
                row.append(value)
            rows.append(row)
        
        return rows, column_names
        
    except Exception as e:
        print(f"Error executing PostgreSQL query: {str(e)}")
        return [], []
        
    finally:
        if conn:
            await conn.close()