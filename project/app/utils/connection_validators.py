from fastapi import HTTPException, status
from app.database.models.core import DataConnection

def validate_metadata_connection(connection: DataConnection, operation: str = "operation"):
    """
    Validates that a connection is of type 'metadata' and not 'image'.
    
    Args:
        connection: The DataConnection object to validate
        operation: The name of the operation being performed (for error messages)
        
    Raises:
        HTTPException: If the connection is of type 'image'
    """
    if connection.content_type == 'image':
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot perform {operation} on connections of type 'image'. "
                   f"Image connections are used only for dataset creation and image downloads."
        )

def validate_image_connection(connection: DataConnection, operation: str = "operation"):
    """
    Validates that a connection is of type 'image' and not 'metadata'.
    
    Args:
        connection: The DataConnection object to validate
        operation: The name of the operation being performed (for error messages)
        
    Raises:
        HTTPException: If the connection is of type 'metadata'
    """
    if connection.content_type == 'metadata':
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot perform {operation} on connections of type 'metadata'. "
                   f"This operation is only available for image connections."
        )
