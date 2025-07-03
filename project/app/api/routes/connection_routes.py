from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.future import select
from datetime import timedelta
from jose import JWTError, jwt
from typing import List
from ...database.database import get_db
from ...crud.token import authenticate_user, create_access_token, get_user_by_cpf, get_current_user
from ...core.config import ACCESS_TOKEN_EXPIRE_MINUTES, REFRESH_TOKEN_EXPIRE_MINUTES, SECRET_KEY, ALGORITHM
from ...database.core import core

from ..schemas.connection_routes import *
from ...utils.logger import logger
from ...core.hierarchy import require_role, RoleEnum


router = APIRouter()

@router.post("/", response_model=ConnectionTypeResponse, status_code=status.HTTP_201_CREATED)
async def create_connection_type(
    connection_type: ConnectionTypeCreate,
    db: AsyncSession = Depends(get_db),
):
  
    try:
        # Check if connection type with same name already exists
        result = await db.execute(
            select(core.ConnectionType).where(core.ConnectionType.name == connection_type.name)
        )
        existing_conn_type = result.scalars().first()
        if existing_conn_type:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Connection type with name '{connection_type.name}' already exists"
            )

        # Create new connection type
        db_connection_type = core.ConnectionType(
            name=connection_type.name,
            description=connection_type.description,
            icon=connection_type.icon,
            color_hex=connection_type.color_hex,
            connection_params_schema=connection_type.connection_params_schema,
            metadata_extraction_method=connection_type.metadata_extraction_method
        )
        
        db.add(db_connection_type)
        await db.commit()
        await db.refresh(db_connection_type)
        
        return db_connection_type
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to create connection type: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create connection type: {str(e)}"
        )

@router.get("/", response_model=List[ConnectionTypeResponse])
async def list_connection_types(
    skip: int = 0, 
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(require_role([RoleEnum.USER])),
):

    try:
        result = await db.execute(
            select(core.ConnectionType)
            .order_by(core.ConnectionType.name)
            .offset(skip)
            .limit(limit)
        )
        connection_types = result.scalars().all()
        return connection_types
    except Exception as e:
        logger.error(f"Failed to retrieve connection types: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve connection types: {str(e)}"
        )

@router.get("/{connection_type_id}", response_model=ConnectionTypeResponse)
async def get_connection_type(
    connection_type_id: int,
    db: AsyncSession = Depends(get_db),
):
    """
    Get details of a specific connection type.
    """
    try:
        result = await db.execute(
            select(core.ConnectionType).where(core.ConnectionType.id == connection_type_id)
        )
        connection_type = result.scalars().first()
        
        if not connection_type:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection type with ID {connection_type_id} not found"
            )
        
        return connection_type
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to retrieve connection type {connection_type_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve connection type: {str(e)}"
        )

@router.put("/{connection_type_id}", response_model=ConnectionTypeResponse)
async def update_connection_type(
    connection_type_id: int,
    connection_type: ConnectionTypeUpdate,
    db: AsyncSession = Depends(get_db),
):
   
    try:
        # Get existing connection type
        result = await db.execute(
            select(core.ConnectionType).where(core.ConnectionType.id == connection_type_id)
        )
        db_connection_type = result.scalars().first()
        
        if not db_connection_type:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection type with ID {connection_type_id} not found"
            )
        
        if connection_type.name is not None and connection_type.name != db_connection_type.name:
            name_result = await db.execute(
                select(core.ConnectionType).where(core.ConnectionType.name == connection_type.name)
            )
            existing_with_name = name_result.scalars().first()
            if existing_with_name:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Connection type with name '{connection_type.name}' already exists"
                )
        
        for attr, value in connection_type.model_dump(exclude_unset=True).items():
            if value is not None:  # Only update non-None values
                setattr(db_connection_type, attr, value)
        
        await db.commit()
        await db.refresh(db_connection_type)
        
        return db_connection_type
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to update connection type {connection_type_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update connection type: {str(e)}"
        )

@router.delete("/{connection_type_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection_type(
    connection_type_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(require_role(RoleEnum.ADMIN)),
):
    
    try:
        result = await db.execute(
            select(core.ConnectionType).where(core.ConnectionType.id == connection_type_id)
        )
        db_connection_type = result.scalars().first()
        
        if not db_connection_type:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection type with ID {connection_type_id} not found"
            )
        
        
        # Delete the connection type
        await db.delete(db_connection_type)
        await db.commit()
        
        # logger.info(f"Connection type ID {connection_type_id} deleted by user {current_user.email}")
        return None
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to delete connection type {connection_type_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete connection type: {str(e)}"
        )