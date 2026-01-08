from sqlalchemy.future import select
from fastapi import HTTPException, status
from typing import Optional
from datetime import datetime
import logging
from app.api.schemas.data_connection import (
    DataConnectionCreate, DataConnectionUpdate, DataConnectionResponse, ConnectionTestResult
)
from app.api.schemas.search import BaseSearchRequest, SearchDataResult

from app.database.databaseUtils import DatabaseService
from app.database.core import core
from app.services.connection_manager import test_connection
from app.utils.connection_validators import validate_metadata_connection
from app.services.airflow_client import get_airflow_client

logger = logging.getLogger(__name__)

class DataConnectionService:
    def __init__(self, db):
        self.db = db
        self.db_service = DatabaseService(db)

    async def create(self, data: DataConnectionCreate, background_tasks=None):
        stmt = select(core.DataConnection).where(
            (core.DataConnection.name == data.name) &
            (core.DataConnection.organization_id == data.organization_id)
        )
        existing = await self.db_service.scalars_first(stmt)
        if existing:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                detail=f"Connection with name '{data.name}' already exists in this organization")
        stmt_ct = select(core.ConnectionType).where(core.ConnectionType.id == data.connection_type_id)
        ct = await self.db_service.scalars_first(stmt_ct)
        if not ct:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection type not found")
        obj = core.DataConnection(**data.model_dump())
        self.db.add(obj)
        await self.db.commit()
        await self.db.refresh(obj)
        return obj

    async def list(self, search: BaseSearchRequest, organization_id: Optional[int] = None,
                   status: Optional[str] = None, connection_type_id: Optional[int] = None,
                   content_type: Optional[str] = None, name: Optional[str] = None):
        stmt = select(core.DataConnection)
        if organization_id:
            stmt = stmt.where(core.DataConnection.organization_id == organization_id)
        if status:
            stmt = stmt.where(core.DataConnection.status == status)
        if connection_type_id:
            stmt = stmt.where(core.DataConnection.connection_type_id == connection_type_id)
        if content_type:
            stmt = stmt.where(core.DataConnection.content_type == content_type)
        if name:
            stmt = stmt.where(core.DataConnection.name.ilike(f"%{name}%"))
        stmt = stmt.order_by(core.DataConnection.id)
        result = await self.db_service.scalars_paginate(search, stmt)
        return result

    async def get(self, id: int):
        stmt = select(core.DataConnection).where(core.DataConnection.id == id)
        obj = await self.db_service.scalars_first(stmt)
        if not obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Data connection with ID {id} not found")
        return obj

    async def update(self, id: int, data: DataConnectionUpdate, background_tasks=None):
        obj = await self.get(id)
        # Name conflict check
        if data.name and data.name != obj.name:
            stmt = select(core.DataConnection).where(
                (core.DataConnection.name == data.name) &
                (core.DataConnection.organization_id == obj.organization_id) &
                (core.DataConnection.id != id)
            )
            existing = await self.db_service.scalars_first(stmt)
            if existing:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                    detail=f"Connection with name '{data.name}' already exists in this organization")
        for attr, value in data.model_dump(exclude_unset=True).items():
            if value is not None:
                setattr(obj, attr, value)
        await self.db.commit()
        await self.db.refresh(obj)
        return obj

    async def delete(self, id: int):
        obj = await self.get(id)
        # TODO: check if in use by datasets (implement if you have that logic)
        await self.db.delete(obj)
        await self.db.commit()

    async def test(self, id: int):
        obj = await self.get(id)
        
        # Get connection type
        stmt_ct = select(core.ConnectionType).where(core.ConnectionType.id == obj.connection_type_id)
        ct = await self.db_service.scalars_first(stmt_ct)
        if not ct:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection type not found")
        
        # Debug: log connection params
        from app.utils.logger import logger
        logger.info(f"[DataConnectionService] Testing connection id={id}")
        logger.info(f"[DataConnectionService] Connection type: {ct.name}")
        logger.info(f"[DataConnectionService] Params keys: {list(obj.connection_params.keys())}")
        # Mask sensitive values for logging
        safe_params = {}
        for k, v in obj.connection_params.items():
            if 'key' in k.lower() or 'secret' in k.lower() or 'password' in k.lower():
                safe_params[k] = f"{str(v)[:4]}***" if v else None
            else:
                safe_params[k] = v
        logger.info(f"[DataConnectionService] Params (masked): {safe_params}")
        
        # Test connection
        success, message, details = await test_connection(
            connection_type=ct.name,
            connection_params=obj.connection_params
        )
        obj.status = 'active' if success else 'error'
        await self.db.commit()
        return ConnectionTestResult(success=success, message=message, details=details)

    async def sync(self, id: int, background_tasks=None):
        obj = await self.get(id)
        
        # Validate that connection is not of type IMAGE
        validate_metadata_connection(obj, "metadata synchronization")
        
        # Check if sync is already in progress (prevent race conditions)
        if obj.sync_status in ('running', 'pending'):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Synchronization for connection '{obj.name}' is already in progress (status: {obj.sync_status}). "
                       f"Wait for it to complete before starting a new sync."
            )
        
        obj.sync_status = 'pending'
        obj.last_sync_time = datetime.now()
        await self.db.commit()
        
        # Use Airflow for job orchestration (no fallback)
        airflow = get_airflow_client()
        dag_run = await airflow.trigger_sync_dag(connection_id=id)
        
        logger.info(f"Triggered Airflow DAG for connection {id}. Run ID: {dag_run.get('dag_run_id')}")
        
        return {
            "message": f"Metadata synchronization for connection '{obj.name}' has been initiated via Airflow",
            "dag_run_id": dag_run.get("dag_run_id"),
            "orchestrator": "airflow"
        }