from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from typing import List, Dict, Any, Optional
from datetime import datetime

from ...database.database import get_db
from ...database.core import core
from ...crud.token import get_current_user
from ..schemas.connection_schemas import (
    DataConnectionCreate,
    DataConnectionUpdate,
    DataConnectionResponse,
    ConnectionTestResult
)
from ...utils.logger import logger
from ...core.hierarchy import require_role, RoleEnum
from ...services.metadata_extraction import extract_metadata
from ...services.connection_manager import test_connection

router = APIRouter()

@router.post("/", response_model=DataConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_data_connection(
    connection: DataConnectionCreate,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Create a new data connection.
    """
    try:
        # Check if connection with same name already exists in the organization
        result = await db.execute(
            select(core.DataConnection).where(
                (core.DataConnection.name == connection.name) & 
                (core.DataConnection.organization_id == connection.organization_id)
            )
        )
        existing_connection = result.scalars().first()
        if existing_connection:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Connection with name '{connection.name}' already exists in this organization"
            )
        
        # Get connection type to validate against schema
        conn_type_result = await db.execute(
            select(core.ConnectionType).where(core.ConnectionType.id == connection.connection_type_id)
        )
        conn_type = conn_type_result.scalars().first()
        if not conn_type:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection type with ID {connection.connection_type_id} not found"
            )
        
        # TODO: Validate connection parameters against connection_params_schema

        # Create new data connection
        db_connection = core.DataConnection(
            organization_id=connection.organization_id,
            name=connection.name,
            description=connection.description,
            connection_type_id=connection.connection_type_id,
            connection_params=connection.connection_params,
            status='pending',
            sync_status='pending',
            cron_expression=connection.cron_expression,
            sync_settings=connection.sync_settings or {}
        )
        
        db.add(db_connection)
        await db.commit()
        await db.refresh(db_connection)
        
        # Schedule metadata extraction in the background
        background_tasks.add_task(
            extract_metadata,
            connection_id=db_connection.id
        )
        
        # logger.info(f"Data connection '{db_connection.name}' created by user {current_user.email}")
        return db_connection
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to create data connection: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create data connection: {str(e)}"
        )

@router.get("/", response_model=List[DataConnectionResponse])
async def list_data_connections(
    organization_id: Optional[int] = None,
    status: Optional[str] = None,
    connection_type_id: Optional[int] = None,
    skip: int = 0, 
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    List all available data connections with optional filters.
    """
    try:
        query = select(core.DataConnection)
        
        # Apply filters if provided
        if organization_id:
            query = query.where(core.DataConnection.organization_id == organization_id)
        if status:
            query = query.where(core.DataConnection.status == status)
        if connection_type_id:
            query = query.where(core.DataConnection.connection_type_id == connection_type_id)
            
        # Apply pagination
        query = query.order_by(core.DataConnection.id).offset(skip).limit(limit)
        
        result = await db.execute(query)
        connections = result.scalars().all()
        return connections
    except Exception as e:
        logger.error(f"Failed to retrieve data connections: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve data connections: {str(e)}"
        )

@router.get("/{connection_id}", response_model=DataConnectionResponse)
async def get_data_connection(
    connection_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Get details of a specific data connection.
    """
    try:
        result = await db.execute(
            select(core.DataConnection).where(core.DataConnection.id == connection_id)
        )
        connection = result.scalars().first()
        
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Data connection with ID {connection_id} not found"
            )
        
        # TODO: Check if user has access to this connection's organization
        
        return connection
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to retrieve data connection {connection_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve data connection: {str(e)}"
        )

@router.put("/{connection_id}", response_model=DataConnectionResponse)
async def update_data_connection(
    connection_id: int,
    connection: DataConnectionUpdate,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Update an existing data connection.
    """
    try:
        # Get existing connection
        result = await db.execute(
            select(core.DataConnection).where(core.DataConnection.id == connection_id)
        )
        db_connection = result.scalars().first()
        
        if not db_connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Data connection with ID {connection_id} not found"
            )
        
        # Check for name conflicts if name is being changed
        if connection.name is not None and connection.name != db_connection.name:
            name_result = await db.execute(
                select(core.DataConnection).where(
                    (core.DataConnection.name == connection.name) & 
                    (core.DataConnection.organization_id == db_connection.organization_id) &
                    (core.DataConnection.id != connection_id)
                )
            )
            existing_with_name = name_result.scalars().first()
            if existing_with_name:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Connection with name '{connection.name}' already exists in this organization"
                )
        
        # Check if connection parameters are being updated
        connection_params_updated = False
        if connection.connection_params is not None and connection.connection_params != db_connection.connection_params:
            connection_params_updated = True
        
        # Update fields
        for attr, value in connection.model_dump(exclude_unset=True).items():
            if value is not None:  # Only update non-None values
                setattr(db_connection, attr, value)
        
        await db.commit()
        await db.refresh(db_connection)
        
        # If connection parameters changed, schedule metadata extraction
        if connection_params_updated:
            background_tasks.add_task(
                extract_metadata,
                connection_id=db_connection.id
            )
        
        logger.info(f"Data connection ID {connection_id} updated by user {current_user.email}")
        return db_connection
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to update data connection {connection_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update data connection: {str(e)}"
        )

@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_data_connection(
    connection_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Delete a data connection and its associated metadata.
    """
    try:
        # Check if connection exists
        result = await db.execute(
            select(core.DataConnection).where(core.DataConnection.id == connection_id)
        )
        db_connection = result.scalars().first()
        
        if not db_connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Data connection with ID {connection_id} not found"
            )
        
        # TODO: Check if there are any datasets using this connection
        # If so, either prevent deletion or cascade delete
        
        # Delete the connection (cascading delete should handle metadata)
        await db.delete(db_connection)
        await db.commit()
        
        # logger.info(f"Data connection ID {connection_id} deleted by user {current_user.email}")
        return None
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to delete data connection {connection_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete data connection: {str(e)}"
        )

@router.post("/{connection_id}/test", response_model=ConnectionTestResult)
async def test_data_connection(
    connection_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Test a data connection to verify it works correctly.
    """
    try:
        # Get connection details
        result = await db.execute(
            select(core.DataConnection).where(core.DataConnection.id == connection_id)
        )
        connection = result.scalars().first()
        
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Data connection with ID {connection_id} not found"
            )
        
        # Get connection type
        conn_type_result = await db.execute(
            select(core.ConnectionType).where(core.ConnectionType.id == connection.connection_type_id)
        )
        conn_type = conn_type_result.scalars().first()
        if not conn_type:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection type with ID {connection.connection_type_id} not found"
            )
        
        # Test connection
        print("Testing connection...")
        print("Connection type:", conn_type.name)
        print("Connection params:", connection.connection_params)

        success, message, details = await test_connection(
            connection_type=conn_type.name,
            connection_params=connection.connection_params
        )
        
        # Update connection status based on test result
        connection.status = 'active' if success else 'error'
        await db.commit()
        
        return ConnectionTestResult(
            success=success,
            message=message,
            details=details
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to test data connection {connection_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to test data connection: {str(e)}"
        )

@router.post("/{connection_id}/sync", status_code=status.HTTP_202_ACCEPTED)
async def trigger_metadata_sync(
    connection_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Manually trigger metadata synchronization for a data connection.
    """
    try:
        # Get connection details
        result = await db.execute(
            select(core.DataConnection).where(core.DataConnection.id == connection_id)
        )
        connection = result.scalars().first()
        
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Data connection with ID {connection_id} not found"
            )
        
        # Update sync status
        connection.sync_status = 'pending'
        connection.last_sync_time = datetime.now()
        await db.commit()
        
        # Schedule metadata extraction in the background
        background_tasks.add_task(
            extract_metadata,
            connection_id=connection_id
        )
        
        return {"message": f"Metadata synchronization for connection '{connection.name}' has been initiated"}
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to trigger metadata sync for connection {connection_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger metadata sync: {str(e)}"
        )