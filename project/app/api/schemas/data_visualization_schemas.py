from typing import Dict, Any, List, Optional, Union
from pydantic import BaseModel, Field
from enum import Enum

class SortDirection(str, Enum):
    asc = "asc"
    desc = "desc"

class FilterOperator(str, Enum):
    eq = "eq"  # Equal
    neq = "neq"  # Not equal
    gt = "gt"  # Greater than
    gte = "gte"  # Greater than or equal
    lt = "lt"  # Less than
    lte = "lte"  # Less than or equal
    like = "like"  # LIKE in SQL
    ilike = "ilike"  # Case insensitive LIKE
    in_list = "in"  # IN operator
    not_in = "not_in"  # NOT IN operator
    is_null = "is_null"  # IS NULL
    is_not_null = "is_not_null"  # IS NOT NULL
    between = "between"  # BETWEEN

class Filter(BaseModel):
    column: str
    operator: FilterOperator
    value: Optional[Any] = None
    values: Optional[List[Any]] = None  # For 'in' and 'not_in' operators
    
class Sort(BaseModel):
    column: str
    direction: SortDirection = SortDirection.asc

class DataVisualizationRequest(BaseModel):
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=1000)
    filters: Optional[List[Filter]] = None
    sort_by: Optional[List[Sort]] = None
    columns: Optional[List[str]] = None  # If None, all columns will be selected

class DataVisualizationResponse(BaseModel):
    table_id: int
    table_name: str
    schema_name: str
    column_names: List[str]
    rows: List[List[Any]]
    page: int
    page_size: int
    total_rows: int
    total_pages: int
    execution_time_ms: float
    applied_filters: Optional[List[Filter]] = None
    applied_sorts: Optional[List[Sort]] = None

class QueryExecutionResponse(BaseModel):
    connection_id: int
    column_names: List[str]
    rows: List[List[Any]]
    row_count: int
    execution_time_ms: float
    query: str
    truncated: bool