import logging
from typing import Dict, Any, Tuple, List
import asyncio

from ..utils.logger import logger

# Import specific connectors
from .connectors.postgres_connector import test_postgres_connection
from .connectors.delta_connector import test_delta_connection
# from .connectors.mongodb_connector import test_mongodb_connection
# from .connectors.minio_connector import test_minio_connection
# from .connectors.mysql_connector import test_mysql_connection
# from .connectors.bigquery_connector import test_bigquery_connection
# from .connectors.minio_connector import test_minio_connection
# from .connectors.mysql_connector import test_mysql_connection
# from .connectors.bigquery_connector import test_bigquery_connection

# Map connection types to test functions
CONNECTION_TESTERS = {
    "postgresql": test_postgres_connection,
    "postgres": test_postgres_connection,
    "deltalake": test_delta_connection,
    "delta": test_delta_connection,
    # "mongodb": test_mongodb_connection,
    # "minio": test_minio_connection, 
    # "s3": test_minio_connection,
    # "mysql": test_mysql_connection,
    # "bigquery": test_bigquery_connection,
    # Add more testers as needed
}

async def test_connection(
    connection_type: str, 
    connection_params: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Test a connection to verify it works correctly.
    
    Args:
        connection_type: Type of connection (postgres, mongodb, etc.)
        connection_params: Connection parameters
        
    Returns:
        Tuple of (success, message, details)
    """
    try:
        connection_type_lower = connection_type.lower()
        tester = CONNECTION_TESTERS.get(connection_type_lower)
        
        if not tester:
            return False, f"Unsupported connection type: {connection_type}", {}
            
        return await tester(connection_params)
    except Exception as e:
        logger.error(f"Error testing connection: {str(e)}")
        return False, f"Connection test failed: {str(e)}", {}

