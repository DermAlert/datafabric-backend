"""
Shared Data Query Schemas

Generic schemas for querying Delta Lake data with column filtering, sorting,
and pagination. Used by both Bronze and Silver data endpoints.

These schemas power an Excel-like filtering experience in the frontend,
allowing users to filter, sort, and paginate through dataset rows.

Operators are aligned with market-standard data grids (AG Grid, MUI DataGrid,
Ant Design Table) to ensure familiar UX for frontend developers.
"""

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Optional, Any
from enum import Enum


# ==================== ENUMS ====================

class ColumnFilterOperator(str, Enum):
    """
    Filter operators for column-level filtering.
    
    Covers the operators found in AG Grid, MUI DataGrid, Ant Design, and Excel:
    
    | Operator     | Frontend Label   | Applicable Types   | SQL Equivalent                       |
    |--------------|------------------|--------------------|--------------------------------------|
    | eq           | Equals           | all                | column = value                       |
    | neq          | Not equals       | all                | column != value                      |
    | contains     | Contains         | string             | column ILIKE '%value%'               |
    | not_contains | Does not contain | string             | column NOT ILIKE '%value%'           |
    | starts_with  | Starts with      | string             | column ILIKE 'value%'                |
    | ends_with    | Ends with        | string             | column ILIKE '%value'                |
    | gt           | Greater than     | number, date       | column > value                       |
    | gte          | Greater or equal | number, date       | column >= value                      |
    | lt           | Less than        | number, date       | column < value                       |
    | lte          | Less or equal    | number, date       | column <= value                      |
    | between      | Between          | number, date       | column BETWEEN value[0] AND value[1] |
    | is_null      | Is empty         | all                | column IS NULL                       |
    | is_not_null  | Is not empty     | all                | column IS NOT NULL                   |
    | in           | Is one of        | string, number     | column IN (v1, v2, ...)              |
    | not_in       | Is not one of    | string, number     | column NOT IN (v1, v2, ...)          |
    """
    eq = "eq"
    neq = "neq"
    contains = "contains"
    not_contains = "not_contains"
    starts_with = "starts_with"
    ends_with = "ends_with"
    gt = "gt"
    gte = "gte"
    lt = "lt"
    lte = "lte"
    between = "between"
    is_null = "is_null"
    is_not_null = "is_not_null"
    in_ = "in"
    not_in = "not_in"


class ColumnFilterLogic(str, Enum):
    """Logic for combining multiple column filters."""
    AND = "AND"
    OR = "OR"


class SortOrder(str, Enum):
    """Sort direction."""
    asc = "asc"
    desc = "desc"


# ==================== FILTER MODEL ====================

class ColumnFilter(BaseModel):
    """
    A single column filter condition.
    
    - For most operators, `value` is a single scalar (string, number, bool).
    - For `in` / `not_in`, `value` is a list of scalars.
    - For `between`, `value` is a list of exactly 2 elements: [min, max].
    - For `is_null` / `is_not_null`, `value` is not required.
    """
    column: str = Field(..., description="Column name to filter on")
    operator: ColumnFilterOperator = Field(..., description="Filter operator")
    value: Optional[Any] = Field(
        None, 
        description=(
            "Filter value. Required for all operators except is_null/is_not_null. "
            "For 'in'/'not_in', provide a list of values. "
            "For 'between', provide [min, max]."
        )
    )
    
    @model_validator(mode="after")
    def validate_value_for_operator(self):
        """Validate that value is appropriate for the chosen operator."""
        op = self.operator
        val = self.value
        
        no_value_ops = {ColumnFilterOperator.is_null, ColumnFilterOperator.is_not_null}
        if op in no_value_ops:
            return self
        
        if op == ColumnFilterOperator.between:
            if not isinstance(val, list) or len(val) != 2:
                raise ValueError(
                    f"'between' operator requires a list of exactly 2 values [min, max], got: {val}"
                )
        elif op in {ColumnFilterOperator.in_, ColumnFilterOperator.not_in}:
            if val is not None and not isinstance(val, list):
                self.value = [val]
        else:
            if val is None:
                raise ValueError(f"Operator '{op.value}' requires a value")
        
        return self
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {"column": "especialidade", "operator": "eq", "value": "HEMATOLOGIA"},
                {"column": "data_consulta", "operator": "gte", "value": "2024-01-01"},
                {"column": "descricao", "operator": "contains", "value": "AVC"},
                {"column": "has_avc", "operator": "eq", "value": True},
                {"column": "idade", "operator": "in", "value": [18, 25, 30]},
                {"column": "idade", "operator": "between", "value": [18, 65]},
                {"column": "status", "operator": "not_in", "value": ["CANCELLED", "EXPIRED"]},
                {"column": "observacao", "operator": "is_null"},
            ]
        }
    }


# ==================== REQUEST MODELS ====================

class DataQueryRequest(BaseModel):
    """
    Base request for querying Delta Lake data with filtering, sorting, and pagination.
    
    This is the shared request body for POST /data/query endpoints on both
    Bronze and Silver layers.
    """
    limit: int = Field(1000, ge=1, le=100000, description="Maximum rows to return")
    offset: int = Field(0, ge=0, description="Rows to skip for pagination")
    version: Optional[int] = Field(None, description="Query specific Delta version (time travel)")
    as_of_timestamp: Optional[str] = Field(None, description="Query as of timestamp in ISO format (time travel)")
    
    column_filters: Optional[List[ColumnFilter]] = Field(
        None, 
        description="List of column filter conditions. If empty or null, no filtering is applied."
    )
    column_filters_logic: ColumnFilterLogic = Field(
        ColumnFilterLogic.AND,
        description="Logic for combining filters: AND (all must match) or OR (any must match)"
    )
    
    sort_by: Optional[str] = Field(None, description="Column name to sort by")
    sort_order: SortOrder = Field(SortOrder.asc, description="Sort direction: asc or desc")
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "summary": "Simple filter with sorting",
                    "value": {
                        "limit": 15,
                        "offset": 0,
                        "column_filters": [
                            {"column": "especialidade", "operator": "eq", "value": "HEMATOLOGIA"},
                            {"column": "data_consulta", "operator": "gte", "value": "2024-01-01"},
                            {"column": "descricao", "operator": "contains", "value": "AVC"}
                        ],
                        "column_filters_logic": "AND",
                        "sort_by": "data_consulta",
                        "sort_order": "desc"
                    }
                },
                {
                    "summary": "No filters (just pagination)",
                    "value": {
                        "limit": 50,
                        "offset": 100
                    }
                },
                {
                    "summary": "Time travel with between filter",
                    "value": {
                        "limit": 1000,
                        "offset": 0,
                        "version": 2,
                        "column_filters": [
                            {"column": "idade", "operator": "between", "value": [18, 65]},
                            {"column": "status", "operator": "not_in", "value": ["CANCELLED", "EXPIRED"]}
                        ],
                        "column_filters_logic": "AND"
                    }
                },
                {
                    "summary": "OR logic - match any condition",
                    "value": {
                        "limit": 100,
                        "offset": 0,
                        "column_filters": [
                            {"column": "cidade", "operator": "eq", "value": "Brasilia"},
                            {"column": "cidade", "operator": "eq", "value": "Goiania"}
                        ],
                        "column_filters_logic": "OR",
                        "sort_by": "nome",
                        "sort_order": "asc"
                    }
                }
            ]
        }
    }


class BronzeDataQueryRequest(DataQueryRequest):
    """
    Request for querying Bronze Delta Lake data.
    
    Extends the base query with Bronze-specific fields (path_index for
    non-federated configs with multiple output paths).
    """
    path_index: int = Field(
        0, ge=0, 
        description=(
            "Index of output path to query. For federated configs use 0. "
            "For non-federated configs with multiple paths, specify which path (0, 1, 2...)"
        )
    )
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "summary": "Filter Bronze data by source table",
                    "value": {
                        "limit": 50,
                        "offset": 0,
                        "path_index": 0,
                        "column_filters": [
                            {"column": "_source_table", "operator": "eq", "value": "patients"},
                            {"column": "name", "operator": "contains", "value": "Silva"}
                        ],
                        "column_filters_logic": "AND",
                        "sort_by": "name",
                        "sort_order": "asc"
                    }
                }
            ]
        }
    }


class SilverDataQueryRequest(DataQueryRequest):
    """
    Request for querying Silver Delta Lake data.
    
    Same as base DataQueryRequest (Silver has a single output path per config).
    """
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "summary": "Filter Silver data with multiple conditions",
                    "value": {
                        "limit": 15,
                        "offset": 0,
                        "column_filters": [
                            {"column": "especialidade", "operator": "eq", "value": "HEMATOLOGIA"},
                            {"column": "data_consulta", "operator": "between", "value": ["2024-01-01", "2024-12-31"]},
                            {"column": "descricao", "operator": "contains", "value": "AVC"},
                            {"column": "observacao", "operator": "is_not_null"}
                        ],
                        "column_filters_logic": "AND",
                        "sort_by": "data_consulta",
                        "sort_order": "desc"
                    }
                }
            ]
        }
    }
