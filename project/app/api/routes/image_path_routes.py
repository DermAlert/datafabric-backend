from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import update, and_
from typing import List, Optional
from datetime import datetime

from ...database.session import get_db
from ...database.models import core
from ...database.models import metadata
from ...core.auth import get_current_user
from ..schemas.image_path_schemas import (
    UpdateImagePathRequest,
    BulkUpdateImagePathRequest,
    ImagePathColumnResponse,
    ImageConnectionResponse,
    UpdateImagePathResponse,
    BulkUpdateImagePathResponse
)

router = APIRouter()

@router.put("/columns/{column_id}/image-path", response_model=UpdateImagePathResponse)
async def update_column_image_path(
    column_id: int,
    request: UpdateImagePathRequest,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Update a column's is_image_path status and optionally associate it with an image connection.
    """
    try:
        # Verify column exists
        column_result = await db.execute(
            select(metadata.ExternalColumn).where(metadata.ExternalColumn.id == column_id)
        )
        column = column_result.scalars().first()
        
        if not column:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Column with ID {column_id} not found"
            )
        
        # If setting is_image_path to True and image_connection_id is provided, verify the connection exists and is of type 'image'
        if request.is_image_path and request.image_connection_id:
            conn_result = await db.execute(
                select(core.DataConnection)
                .where(
                    and_(
                        core.DataConnection.id == request.image_connection_id,
                        core.DataConnection.content_type == 'image'
                    )
                )
            )
            connection = conn_result.scalars().first()
            
            if not connection:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Image connection with ID {request.image_connection_id} not found or not of type 'image'"
                )
        
        # Update the column
        update_data = {
            "is_image_path": request.is_image_path,
            "image_connection_id": request.image_connection_id if request.is_image_path else None
        }
        
        await db.execute(
            update(metadata.ExternalColumn)
            .where(metadata.ExternalColumn.id == column_id)
            .values(**update_data)
        )
        
        await db.commit()
        
        return UpdateImagePathResponse(
            success=True,
            message=f"Column {column_id} image path status updated successfully",
            column_id=column_id,
            is_image_path=request.is_image_path,
            image_connection_id=request.image_connection_id if request.is_image_path else None
        )
        
    except HTTPException:
        await db.rollback()
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating column image path: {str(e)}"
        )

@router.put("/columns/bulk/image-path", response_model=BulkUpdateImagePathResponse)
async def bulk_update_columns_image_path(
    request: BulkUpdateImagePathRequest,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Bulk update multiple columns' is_image_path status and optionally associate them with an image connection.
    """
    try:
        if not request.column_ids:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Column IDs list cannot be empty"
            )
        
        # Verify all columns exist
        columns_result = await db.execute(
            select(metadata.ExternalColumn.id)
            .where(metadata.ExternalColumn.id.in_(request.column_ids))
        )
        existing_column_ids = [row[0] for row in columns_result.all()]
        
        failed_column_ids = list(set(request.column_ids) - set(existing_column_ids))
        
        if failed_column_ids and len(failed_column_ids) == len(request.column_ids):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="None of the specified columns were found"
            )
        
        # If setting is_image_path to True and image_connection_id is provided, verify the connection
        if request.is_image_path and request.image_connection_id:
            conn_result = await db.execute(
                select(core.DataConnection)
                .where(
                    and_(
                        core.DataConnection.id == request.image_connection_id,
                        core.DataConnection.content_type == 'image'
                    )
                )
            )
            connection = conn_result.scalars().first()
            
            if not connection:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Image connection with ID {request.image_connection_id} not found or not of type 'image'"
                )
        
        # Update the columns
        update_data = {
            "is_image_path": request.is_image_path,
            "image_connection_id": request.image_connection_id if request.is_image_path else None
        }
        
        await db.execute(
            update(metadata.ExternalColumn)
            .where(metadata.ExternalColumn.id.in_(existing_column_ids))
            .values(**update_data)
        )
        
        await db.commit()
        
        return BulkUpdateImagePathResponse(
            success=True,
            message=f"Successfully updated {len(existing_column_ids)} columns",
            updated_count=len(existing_column_ids),
            failed_count=len(failed_column_ids),
            failed_column_ids=failed_column_ids
        )
        
    except HTTPException:
        await db.rollback()
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error bulk updating columns image path: {str(e)}"
        )

@router.get("/tables/{table_id}/image-path-columns", response_model=List[ImagePathColumnResponse])
async def list_table_image_path_columns(
    table_id: int,
    include_all: bool = Query(False, description="Include all columns or only those marked as image paths"),
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    List columns in a table that are marked as image paths, or optionally all columns.
    """
    try:
        # Verify table exists
        table_result = await db.execute(
            select(metadata.ExternalTables).where(metadata.ExternalTables.id == table_id)
        )
        table = table_result.scalars().first()
        
        if not table:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Table with ID {table_id} not found"
            )
        
        # Build query based on include_all parameter
        query = (
            select(
                metadata.ExternalColumn,
                metadata.ExternalTables.table_name,
                metadata.ExternalSchema.schema_name,
                core.DataConnection.name.label('image_connection_name')
            )
            .join(metadata.ExternalTables, metadata.ExternalColumn.table_id == metadata.ExternalTables.id)
            .join(metadata.ExternalSchema, metadata.ExternalTables.schema_id == metadata.ExternalSchema.id)
            .outerjoin(core.DataConnection, metadata.ExternalColumn.image_connection_id == core.DataConnection.id)
            .where(metadata.ExternalColumn.table_id == table_id)
        )
        
        if not include_all:
            query = query.where(metadata.ExternalColumn.is_image_path == True)
        
        result = await db.execute(query.order_by(metadata.ExternalColumn.column_position))
        rows = result.all()
        
        return [
            ImagePathColumnResponse(
                id=column.id,
                table_id=column.table_id,
                column_name=column.column_name,
                data_type=column.data_type,
                is_image_path=column.is_image_path,
                image_connection_id=column.image_connection_id,
                image_connection_name=image_connection_name,
                table_name=table_name,
                schema_name=schema_name
            ) for column, table_name, schema_name, image_connection_name in rows
        ]
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving table image path columns: {str(e)}"
        )