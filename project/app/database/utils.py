from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import Select, func, select
from pydantic import BaseModel
from typing import Type, TypeVar, Generic, Optional, List

from app.api.schemas.search import SearchDataResult, BaseSearchRequest

T = TypeVar("T")
M = TypeVar("M", bound=BaseModel)

class DatabaseService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def count_rows(self, stmt: Select) -> int:
        return (
            await self.db.scalars(
                select(func.count()).select_from(stmt.order_by(None).subquery())
            )
        ).first() or 0

    async def scalars_first(self, query: Select, params=None) -> Optional[T]:
        return (await self.db.scalars(query.limit(1), params)).first()

    async def scalars_all(self, query: Select, params=None) -> List[T]:
        return (await self.db.scalars(query, params)).all()

    async def execute_first(self, query: Select, params=None) -> Optional[T]:
        return (await self.db.execute(query.limit(1), params)).first()

    async def execute_all(self, query: Select, params=None) -> List[T]:
        return (await self.db.execute(query, params)).all()

    async def validate_all(
        self, model: Type[M], query: Select, params=None
    ) -> List[M]:
        result = await self.db.execute(query, params)
        return [model.model_validate(row) for row in result]

    async def scalars_paginate(
        self,
        search: BaseSearchRequest,
        query: Select,
    ) -> SearchDataResult:
        total = None
        if search.pagination.limit is not None or search.pagination.skip is not None:
            if search.pagination.query_total:
                total = await self.count_rows(query)
            if search.pagination.limit is not None:
                query = query.limit(search.pagination.limit)
            if search.pagination.skip is not None:
                query = query.offset(search.pagination.skip)
        results = await self.scalars_all(query)
        if total is None:
            total = len(results)
        return SearchDataResult(total=total, items=results)

    async def execute_paginate(
        self,
        search: "BaseSearchRequest",
        query: Select,
    ) -> "SearchDataResult":
        total = None
        if search.pagination.limit is not None or search.pagination.skip is not None:
            if search.pagination.query_total:
                total = await self.count_rows(query)
            if search.pagination.limit is not None:
                query = query.limit(search.pagination.limit)
            if search.pagination.skip is not None:
                query = query.offset(search.pagination.skip)
        results = await self.execute_all(query)
        if total is None:
            total = len(results)
        return SearchDataResult(total=total, items=results)