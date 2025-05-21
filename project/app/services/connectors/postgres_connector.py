import asyncio
from typing import Dict, Any, Tuple, List
import asyncpg

async def test_postgres_connection(connection_params: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Test a PostgreSQL connection.
    
    Args:
        connection_params: Connection parameters including host, port, database, username, password
        
    Returns:
        Tuple of (success, message, details)
    """
    conn = None
    try:
        # Extract connection parameters with defaults
        print("DEBUG - Connection Params: ",connection_params)

        host = connection_params.get("host", "localhost")
        # host = "postgres-backend"
        port = connection_params.get("port", 5432)
        # port = 5432
        database = connection_params.get("database")
        username = connection_params.get("username")
        password = connection_params.get("password")
        ssl = False #connection_params.get("ssl", False)

        
        # Validate required parameters
        if not all([host, database, username, password]):
            missing = []
            if not host:
                missing.append("host")
            if not database:
                missing.append("database")
            if not username:
                missing.append("username")
            if not password:
                missing.append("password")
                
            return False, f"Missing required connection parameters: {', '.join(missing)}", {}
        
        print("DEBUG - Connection Params2: ",host, port, database, username, password, ssl)

        # Connect to PostgreSQL
        conn = await asyncpg.connect(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password,
            ssl=ssl
        )

        print("DEBUG - Connection: ",conn)
        
        # Test query
        row = await conn.fetchrow("SELECT version() AS version")
        version = row['version']
        
        # Get database size
        size_row = await conn.fetchrow("SELECT pg_database_size($1) AS size", database)
        db_size = size_row['size']
        
        return True, "Connection successful", {
            "version": version,
            "database_size": db_size
        }
        
    except Exception as e:
        return False, f"Connection failed: {str(e)}", {}
        
    finally:
        if conn:
            await conn.close()