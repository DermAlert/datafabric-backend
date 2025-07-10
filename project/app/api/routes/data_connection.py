from fastapi import APIRouter, Depends, status, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from app.database.database import get_db
from app.api.schemas.data_connection import (
    DataConnectionCreate, DataConnectionUpdate, DataConnectionResponse, ConnectionTestResult
)
from app.api.schemas.search import BaseSearchRequest, SearchResult
from app.api.service.data_connection_service import DataConnectionService

router = APIRouter(prefix="/data-connections", tags=["data-connections"])

@router.post("/", response_model=DataConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_data_connection(
    data: DataConnectionCreate,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    return await DataConnectionService(db).create(data, background_tasks)

@router.post("/search", response_model=SearchResult[DataConnectionResponse])
async def search_data_connections(
    search: BaseSearchRequest,
    organization_id: Optional[int] = None,
    status: Optional[str] = None,
    connection_type_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
):
    result = await DataConnectionService(db).list(
        search,
        organization_id=organization_id,
        status=status,
        connection_type_id=connection_type_id
    )
    return SearchResult(
        total=result.total,
        items=[DataConnectionResponse.from_orm(obj) for obj in result.items]
    )

@router.get("/{id}", response_model=DataConnectionResponse)
async def get_data_connection(
    id: int,
    db: AsyncSession = Depends(get_db),
):
    return await DataConnectionService(db).get(id)

@router.put("/{id}", response_model=DataConnectionResponse)
async def update_data_connection(
    id: int,
    data: DataConnectionUpdate,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    return await DataConnectionService(db).update(id, data, background_tasks)

@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_data_connection(
    id: int,
    db: AsyncSession = Depends(get_db),
):
    await DataConnectionService(db).delete(id)

@router.post("/{id}/test", response_model=ConnectionTestResult)
async def test_data_connection(
    id: int,
    db: AsyncSession = Depends(get_db),
):
    return await DataConnectionService(db).test(id)

@router.post("/{id}/sync", status_code=status.HTTP_202_ACCEPTED)
async def trigger_metadata_sync(
    id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    return await DataConnectionService(db).sync(id, background_tasks)