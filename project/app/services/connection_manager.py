import logging
from typing import Dict, Any, Tuple, List
import uuid

from ..utils.logger import logger
from .trino_manager import TrinoManager


async def test_connection(
    connection_type: str, 
    connection_params: Dict[str, Any]
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Test a connection to verify it works correctly using Trino (async with aiotrino).
    
    Args:
        connection_type: Type of connection (postgres, delta, etc.)
        connection_params: Connection parameters
        
    Returns:
        Tuple of (success, message, details)
    """
    try:
        # Use a temporary test name
        test_catalog_name = f"test_conn_{uuid.uuid4().hex[:8]}"
        
        manager = TrinoManager()
        
        # Try to create catalog (using async version)
        success = await manager.ensure_catalog_exists_async(test_catalog_name, connection_type, connection_params)
        
        if not success:
             return False, "Failed to create Trino catalog for testing", {}
             
        # Try to query schemas to verify connectivity (async)
        conn = await manager.get_connection()
        try:
            cur = await conn.cursor()
            # Sanitize name again just in case, though manager handles it
            safe_catalog = manager._sanitize_identifier(test_catalog_name)
            await cur.execute(f"SHOW SCHEMAS FROM \"{safe_catalog}\"")
            schemas = await cur.fetchall()
            
            # Clean up (async)
            await manager.drop_catalog_async(test_catalog_name)
            
            return True, f"Connection successful. Found {len(schemas)} schemas.", {"schemas": [s[0] for s in schemas]}
            
        except Exception as query_error:
            # Try to clean up
            try:
                await manager.drop_catalog_async(test_catalog_name)
            except:
                pass
            return False, f"Connection created but query failed: {str(query_error)}", {}
        finally:
            await conn.close()
            
    except Exception as e:
        logger.error(f"Error testing connection: {str(e)}")
        return False, f"Connection test failed: {str(e)}", {}
