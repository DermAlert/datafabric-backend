import logging
from typing import Dict, Any, Tuple, List
import asyncio

from ..utils.logger import logger
from .trino_manager import TrinoManager

async def test_connection(
    connection_type: str, 
    connection_params: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Test a connection to verify it works correctly using Trino.
    
    Args:
        connection_type: Type of connection (postgres, delta, etc.)
        connection_params: Connection parameters
        
    Returns:
        Tuple of (success, message, details)
    """
    try:
        # Use a temporary test name
        import uuid
        test_catalog_name = f"test_conn_{uuid.uuid4().hex[:8]}"
        
        manager = TrinoManager()
        
        # Try to create catalog
        success = manager.ensure_catalog_exists(test_catalog_name, connection_type, connection_params)
        
        if not success:
             return False, "Failed to create Trino catalog for testing", {}
             
        # Try to query schemas to verify connectivity
        conn = manager.get_connection()
        try:
            cur = conn.cursor()
            # Sanitize name again just in case, though manager handles it
            safe_catalog = manager._sanitize_identifier(test_catalog_name)
            cur.execute(f"SHOW SCHEMAS FROM \"{safe_catalog}\"")
            schemas = cur.fetchall()
            
            # Clean up
            manager.drop_catalog(test_catalog_name)
            
            return True, f"Connection successful. Found {len(schemas)} schemas.", {"schemas": [s[0] for s in schemas]}
            
        except Exception as query_error:
            # Try to clean up
            try:
                manager.drop_catalog(test_catalog_name)
            except:
                pass
            return False, f"Connection created but query failed: {str(query_error)}", {}
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Error testing connection: {str(e)}")
        return False, f"Connection test failed: {str(e)}", {}
