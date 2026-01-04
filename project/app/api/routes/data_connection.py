from fastapi import APIRouter, Depends, status, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from datetime import datetime
import logging
from app.database.database import get_db
from app.api.schemas.data_connection import (
    DataConnectionCreate, DataConnectionUpdate, DataConnectionResponse, ConnectionTestResult, SearchDataConnection
)
from app.api.schemas.search import SearchResult
from app.api.service.data_connection_service import DataConnectionService
from fastapi import BackgroundTasks

# router = APIRouter(prefix="/data-connections", tags=["data-connections"])
router = APIRouter()

logger = logging.getLogger(__name__)

@router.post("/", response_model=DataConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_data_connection(
    data: DataConnectionCreate,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    return await DataConnectionService(db).create(data, background_tasks)

@router.post("/search", response_model=SearchResult[DataConnectionResponse])
async def search_data_connections(
    search: SearchDataConnection,
    db: AsyncSession = Depends(get_db),
):
    """Search data connections with pagination and filters"""
    result = await DataConnectionService(db).list(
        search,
        organization_id=search.organization_id,
        status=search.status,
        connection_type_id=search.connection_type_id,
        content_type=search.content_type,
        name=search.name
    )
    return SearchResult(
        total=result.total,
        items=[DataConnectionResponse.model_validate(obj) for obj in result.items]
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
    background_tasks: BackgroundTasks = None,
    db: AsyncSession = Depends(get_db),
):
    """
    Dispara sincronização de metadados via Airflow.
    
    Se já existe um sync em andamento (status 'running' ou 'pending'), retorna erro 409.
    """
    service = DataConnectionService(db)
    return await service.sync(id, background_tasks)