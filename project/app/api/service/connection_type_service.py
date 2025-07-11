from sqlalchemy.future import select
from fastapi import HTTPException, status
from app.api.schemas.connection_type import (
    ConnectionTypeCreate, ConnectionTypeUpdate, ConnectionTypeResponse,SearchConnectionType
)
from app.api.schemas.search import BaseSearchRequest, SearchDataResult
from app.database.databaseUtils import DatabaseService
from app.database.core import core

class ConnectionTypeService:
    def __init__(self, db):
        self.db = db
        self.db_service = DatabaseService(db)

    async def create(self, data: ConnectionTypeCreate):
        # Check for name conflict
        stmt = select(core.ConnectionType).where(core.ConnectionType.name == data.name)
        existing = await self.db_service.scalars_first(stmt)
        if existing:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Connection type with name '{data.name}' already exists")
        obj = core.ConnectionType(**data.model_dump())
        self.db.add(obj)
        await self.db.commit()
        await self.db.refresh(obj)
        return obj

    async def list(self, search: BaseSearchRequest):
        stmt = select(core.ConnectionType).order_by(core.ConnectionType.name)
        result = await self.db_service.scalars_paginate(search, stmt)
        return result
    
    async def list_by_id(self, search: SearchConnectionType) -> SearchDataResult:
        stmt = select(core.ConnectionType)

        if search.connection_type_id:
            stmt = stmt.where(core.ConnectionType.id == search.connection_type_id)

        return await  self.db_service.scalars_paginate(search, stmt)


    async def get(self, id: int):
        stmt = select(core.ConnectionType).where(core.ConnectionType.id == id)
        obj = await self.db_service.scalars_first(stmt)
        if not obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Connection type with ID {id} not found")
        return obj

    async def update(self, id: int, data: ConnectionTypeUpdate):
        obj = await self.get(id)
        if data.name and data.name != obj.name:
            stmt = select(core.ConnectionType).where(core.ConnectionType.name == data.name)
            existing = await self.db_service.scalars_first(stmt)
            if existing:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Connection type with name '{data.name}' already exists")
        for attr, value in data.model_dump(exclude_unset=True).items():
            if value is not None:
                setattr(obj, attr, value)
        await self.db.commit()
        await self.db.refresh(obj)
        return obj

    async def delete(self, id: int):
        obj = await self.get(id)
        stmt = select(core.DataConnection).where(core.DataConnection.connection_type_id == id)
        in_use = await self.db_service.scalars_first(stmt)
        if in_use:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete: connection type is in use by a data connection")
        await self.db.delete(obj)
        await self.db.commit()