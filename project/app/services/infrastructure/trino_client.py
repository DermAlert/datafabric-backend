"""
Trino Client Provider

Provides a simple interface to get Trino client for executing queries.
"""

from .trino_manager import TrinoManager

# Singleton instance
_trino_manager = None


async def get_trino_client():
    """
    Get a Trino client instance.
    
    Returns:
        TrinoManager instance that can execute queries via Trino.
    """
    global _trino_manager
    if _trino_manager is None:
        _trino_manager = TrinoManager()
    return _trino_manager


class TrinoClient:
    """
    Simple Trino client wrapper for executing queries.
    
    Usage:
        client = await get_trino_client()
        result = await client.execute("SELECT * FROM catalog.schema.table")
    """
    
    def __init__(self):
        self.manager = TrinoManager()
    
    async def execute(self, sql: str) -> dict:
        """
        Execute a SQL query via Trino.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            Dict with 'columns' and 'data' keys
        """
        conn = await self.manager.get_connection()
        try:
            cursor = await conn.cursor()
            await cursor.execute(sql)
            rows = await cursor.fetchall()
            
            # Get column names
            description = cursor.description
            columns = [col[0] for col in description] if description else []
            
            return {
                'columns': columns,
                'data': rows
            }
        finally:
            await conn.close()



