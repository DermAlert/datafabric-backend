from pydantic import BaseModel, Field
from typing import Generic, Optional, List, TypeVar
from dataclasses import dataclass

T = TypeVar("T")

class SearchRequestPagination(BaseModel):
    skip: Optional[int] = 0
    limit: Optional[int] = Field(15, examples=[15])
    query_total: bool = False

class BaseSearchRequest(BaseModel):
    pagination: SearchRequestPagination = Field(
        default_factory=SearchRequestPagination,
        examples=[
            {
                "skip": 0,
                "limit": 15,
                "query_total": False,
            }
        ],
    )

class SearchResult(BaseModel, Generic[T]):
    total: int
    items: List[T]

@dataclass
class SearchDataResult(Generic[T]):
    total: int
    items: List[T]
    def model_dump(self) -> dict:
        return {"total": self.total, "items": self.items}