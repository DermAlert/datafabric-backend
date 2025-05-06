from typing import Dict, Any, List, Tuple
import asyncpg

async def preview_postgres_data(
    connection_params: Dict[str, Any],
    schema_name: str,
    table_name: str,
    limit: int = 100
) -> Tuple[List[List[Any]], List[str]]:
    """
    Get a preview of data from a PostgreSQL table.
    
    Args:
        connection_params: Connection parameters
        schema_name: Schema name
        table_name: Table name
        limit: Maximum number of rows to return
        
    Returns:
        Tuple of (rows, column_names)
    """
    conn = None
    try:
        # Extract connection parameters with defaults
        host = connection_params.get("host", "localhost")
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
        schema_name = await conn.fetch(
            "SELECT quote_ident($1) AS ident", schema_name
        )
        schema_ident = schema_name[0]['ident']
        
        table_name = await conn.fetch(
            "SELECT quote_ident($1) AS ident", table_name
        )
        table_ident = table_name[0]['ident']
        
        # Get column information
        column_query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = $1
        AND table_name = $2
        ORDER BY ordinal_position
        """
        
        columns = await conn.fetch(column_query, schema_name[0]['ident'], table_name[0]['ident'])
        column_names = [col['column_name'] for col in columns]
        
        # Build query for data preview
        query = f"SELECT * FROM {schema_ident}.{table_ident} LIMIT {min(limit, 1000)}"
        
        # Execute query
        results = await conn.fetch(query)
        
        # Convert rows to list of lists
        rows = []
        for record in results:
            row = []
            for column in column_names:
                # Convert any complex types to string to ensure JSON serializability
                value = record[column]
                if value is not None and not isinstance(value, (str, int, float, bool)):
                    value = str(value)
                row.append(value)
            rows.append(row)
        
        return rows, column_names
        
    except Exception as e:
        print(f"Error previewing PostgreSQL data: {str(e)}")
        return [], []
        
    finally:
        if conn:
            await conn.close()