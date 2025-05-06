from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from typing import List, Optional, Dict, Any

from ...database.database import get_db
from ...database.core import core
from ...database.metadata import metadata
from ...crud.token import get_current_user
from ..schemas.metadata_schemas import (
    CatalogResponse,
    SchemaResponse,
    TableResponse,
    ColumnResponse,
    TableDetailsResponse,
    DataPreviewResponse
)
from ...services.data_preview import get_data_preview

router = APIRouter()

@router.get("/connections/{connection_id}/catalogs", response_model=List[CatalogResponse])
async def list_catalogs(
    connection_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    List all catalogs for a specific data connection.
    """
    try:
        # Verify connection exists and user has access
        conn_result = await db.execute(
            select(core.DataConnection).where(core.DataConnection.id == connection_id)
        )
        connection = conn_result.scalars().first()
        
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Data connection with ID {connection_id} not found"
            )
        
        # TODO: Check if user has access to this connection's organization
        
        # Get catalogs
        result = await db.execute(
            select(metadata.ExternalCatalogs)
            .where(metadata.ExternalCatalogs.connection_id == connection_id)
            .order_by(metadata.ExternalCatalogs.catalog_name)
        )
        catalogs = result.scalars().all()
        
        return catalogs
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve catalogs: {str(e)}"
        )

@router.get("/connections/{connection_id}/schemas", response_model=List[SchemaResponse])
async def list_schemas(
    connection_id: int,
    catalog_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    List all schemas for a specific data connection.
    """
    try:
        # Verify connection exists and user has access
        conn_result = await db.execute(
            select(core.DataConnection).where(core.DataConnection.id == connection_id)
        )
        connection = conn_result.scalars().first()
        
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Data connection with ID {connection_id} not found"
            )
        
        # TODO: Check if user has access to this connection's organization
        
        # Build query
        query = select(metadata.ExternalSchema).where(metadata.ExternalSchema.connection_id == connection_id)
        
        if catalog_id:
            query = query.where(metadata.ExternalSchema.catalog_id == catalog_id)
            
        query = query.order_by(metadata.ExternalSchema.schema_name)
        
        # Get schemas
        result = await db.execute(query)
        schemas = result.scalars().all()
        
        return schemas
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve schemas: {str(e)}"
        )

@router.get("/schemas/{schema_id}/tables", response_model=List[TableResponse])
async def list_tables(
    schema_id: int,
    table_type: Optional[str] = None,
    search: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    List all tables for a specific schema.
    """
    try:
        # Verify schema exists
        schema_result = await db.execute(
            select(metadata.ExternalSchema).where(metadata.ExternalSchema.id == schema_id)
        )
        schema = schema_result.scalars().first()
        
        if not schema:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schema with ID {schema_id} not found"
            )
        
        # TODO: Check if user has access to this connection's organization
        
        # Build query
        query = select(metadata.ExternalTables).where(metadata.ExternalTables.schema_id == schema_id)
        
        if table_type:
            query = query.where(metadata.ExternalTables.table_type == table_type)
            
        if search:
            query = query.where(metadata.ExternalTables.table_name.ilike(f"%{search}%"))
            
        query = query.order_by(metadata.ExternalTables.table_name)
        
        # Get tables
        result = await db.execute(query)
        tables = result.scalars().all()
        
        return tables
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve tables: {str(e)}"
        )

@router.get("/tables/{table_id}", response_model=TableDetailsResponse)
async def get_table_details(
    table_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Get detailed information about a specific table including its columns.
    """
    try:
        # Get table
        table_result = await db.execute(
            select(metadata.ExternalTables).where(metadata.ExternalTables.id == table_id)
        )
        table = table_result.scalars().first()
        
        if not table:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Table with ID {table_id} not found"
            )
        
        # TODO: Check if user has access to this connection's organization
        
        # Get schema
        schema_result = await db.execute(
            select(metadata.ExternalSchema).where(metadata.ExternalSchema.id == table.schema_id)
        )
        schema = schema_result.scalars().first()
        
        # Get columns
        columns_result = await db.execute(
            select(metadata.ExternalColumn)
            .where(metadata.ExternalColumn.table_id == table_id)
            .order_by(metadata.ExternalColumn.column_position)
        )
        columns_db = columns_result.scalars().all()
        
        # Convert ORM objects to Pydantic model instances with all required fields
        columns = [
            ColumnResponse(
                id=col.id,
                table_id=col.table_id,
                column_name=col.column_name,
                column_position=col.column_position,
                data_type=col.data_type,
                description=col.description,
                is_nullable=col.is_nullable,
                is_primary_key=col.is_primary_key,
                is_unique=col.is_unique,
                properties=col.properties,
                # Adding the missing required fields
                is_indexed=getattr(col, 'is_indexed', False),  # Default to False if not available
                statistics=getattr(col, 'statistics', {}),     # Default to empty dict if not available
                sample_values=getattr(col, 'sample_values', [])  # Default to empty list if not available
            ) for col in columns_db
        ]
        
        # Count primary keys
        primary_key_count = sum(1 for col in columns_db if col.is_primary_key)
        
        return TableDetailsResponse(
            id=table.id,
            schema_id=table.schema_id,
            connection_id=table.connection_id,
            table_name=table.table_name,
            external_reference=table.external_reference,
            table_type=table.table_type,
            estimated_row_count=table.estimated_row_count,
            total_size_bytes=table.total_size_bytes,
            last_analyzed=table.last_analyzed,
            properties=table.properties,
            description=table.description,
            schema_name=schema.schema_name if schema else None,
            columns=columns,
            primary_key_count=primary_key_count,
            column_count=len(columns)
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve table details: {str(e)}"
        )