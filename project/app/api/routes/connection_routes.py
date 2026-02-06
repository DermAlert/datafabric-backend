from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from app.database.session import get_db
from app.api.schemas.connection_type import (
    ConnectionTypeCreate, ConnectionTypeUpdate, ConnectionTypeResponse, SearchConnectionType
)
from app.api.schemas.search import SearchResult 
from app.services.connections.connection_type_service import ConnectionTypeService 

# router = APIRouter(prefix="/connection-types", tags=["connection-types"])
router = APIRouter()

@router.post("/", response_model=ConnectionTypeResponse, status_code=status.HTTP_201_CREATED)
async def create_connection_type(
    data: ConnectionTypeCreate,
    db: AsyncSession = Depends(get_db),
):
    return await ConnectionTypeService(db).create(data)

@router.post("/search", response_model=SearchResult[ConnectionTypeResponse])
async def search_connection_types(
    search: SearchConnectionType,
    db: AsyncSession = Depends(get_db),
):
    """Search connection types with pagination and filters"""
    service = ConnectionTypeService(db)

    if search.connection_type_id:
        result = await service.list_by_id(search)
    else:
        result = await service.list(search)

    return SearchResult(
        total=result.total,
        items=[ConnectionTypeResponse.model_validate(obj) for obj in result.items]
    )

@router.get("/{id}", response_model=ConnectionTypeResponse)
async def get_connection_type(
    id: int,
    db: AsyncSession = Depends(get_db),
):
    return await ConnectionTypeService(db).get(id)

@router.put("/{id}", response_model=ConnectionTypeResponse)
async def update_connection_type(
    id: int,
    data: ConnectionTypeUpdate,
    db: AsyncSession = Depends(get_db),
):
    return await ConnectionTypeService(db).update(id, data)

@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection_type(
    id: int,
    db: AsyncSession = Depends(get_db),
):
    await ConnectionTypeService(db).delete(id)