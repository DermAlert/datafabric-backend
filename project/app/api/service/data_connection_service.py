from sqlalchemy.future import select
from fastapi import HTTPException, status
from typing import Optional
from datetime import datetime
from app.api.schemas.data_connection import (
    DataConnectionCreate, DataConnectionUpdate, DataConnectionResponse, ConnectionTestResult
)
from app.api.schemas.search import BaseSearchRequest, SearchDataResult

from app.database.databaseUtils import DatabaseService
from app.database.core import core
from app.services.metadata_extraction import extract_metadata
from app.services.connection_manager import test_connection

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
        # Metadata extraction in background
        if background_tasks:
            background_tasks.add_task(extract_metadata, connection_id=obj.id)
        return obj

    async def list(self, search: BaseSearchRequest, organization_id: Optional[int] = None,
                   status: Optional[str] = None, connection_type_id: Optional[int] = None,
                   name: Optional[str] = None):
        stmt = select(core.DataConnection)
        if organization_id:
            stmt = stmt.where(core.DataConnection.organization_id == organization_id)
        if status:
            stmt = stmt.where(core.DataConnection.status == status)
        if connection_type_id:
            stmt = stmt.where(core.DataConnection.connection_type_id == connection_type_id)
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
        connection_params_updated = False
        if data.connection_params is not None and data.connection_params != obj.connection_params:
            connection_params_updated = True
        for attr, value in data.model_dump(exclude_unset=True).items():
            if value is not None:
                setattr(obj, attr, value)
        await self.db.commit()
        await self.db.refresh(obj)
        if connection_params_updated and background_tasks:
            background_tasks.add_task(extract_metadata, connection_id=obj.id)
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
        # Test connection
        success, message, details = await test_connection(
            connection_type=ct.name,
            connection_params=obj.connection_params
        )
        obj.status = 'active' if success else 'error'
        await self.db.commit()
        return ConnectionTestResult(success=success, message=message, details=details)

    async def sync(self, id: int, background_tasks):
        obj = await self.get(id)
        obj.sync_status = 'pending'
        obj.last_sync_time = datetime.now()
        await self.db.commit()
        background_tasks.add_task(extract_metadata, connection_id=id)
        return {"message": f"Metadata synchronization for connection '{obj.name}' has been initiated"}