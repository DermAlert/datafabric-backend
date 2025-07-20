from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

class DatasetStorageType(str, Enum):
    VIRTUAL_VIEW = "virtual_view"
    MATERIALIZED = "materialized"
    COPY_TO_MINIO = "copy_to_minio"

class DatasetRefreshType(str, Enum):
    ON_DEMAND = "on_demand"
    SCHEDULED = "scheduled"
    REAL_TIME = "real_time"

class DatasetStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"

class JoinType(str, Enum):
    PRIMARY = "primary"
    LEFT = "left"
    INNER = "inner"
    RIGHT = "right"
    FULL = "full"

class TransformationType(str, Enum):
    DIRECT = "direct"
    CALCULATED = "calculated"
    AGGREGATED = "aggregated"

# Dataset Creation Schemas
class DatasetSourceCreate(BaseModel):
    table_id: int
    join_type: JoinType = JoinType.PRIMARY
    join_condition: Optional[str] = None
    filter_condition: Optional[str] = None

class DatasetColumnCreate(BaseModel):
    name: str
    description: Optional[str] = None
    data_type: str
    is_nullable: bool = True
    column_position: int
    transformation_expression: Optional[str] = None
    is_visible: bool = True
    format_pattern: Optional[str] = None
    properties: Dict[str, Any] = {}

class DatasetColumnSourceCreate(BaseModel):
    source_column_id: Optional[int] = None
    transformation_type: TransformationType = TransformationType.DIRECT
    transformation_expression: Optional[str] = None
    is_primary_source: bool = True

class DatasetCreate(BaseModel):
    name: str
    description: Optional[str] = None
    storage_type: DatasetStorageType = DatasetStorageType.VIRTUAL_VIEW
    refresh_type: DatasetRefreshType = DatasetRefreshType.ON_DEMAND
    refresh_schedule: Optional[str] = None
    version: str = "1.0"
    properties: Dict[str, Any] = {}
    sources: List[DatasetSourceCreate]
    columns: List[DatasetColumnCreate]
    column_sources: List[DatasetColumnSourceCreate]

class DatasetUnifiedCreate(BaseModel):
    """Schema for creating datasets with automatic column mapping unification"""
    name: str
    description: Optional[str] = None
    storage_type: DatasetStorageType = DatasetStorageType.VIRTUAL_VIEW
    refresh_type: DatasetRefreshType = DatasetRefreshType.ON_DEMAND
    refresh_schedule: Optional[str] = None
    version: str = "1.0"
    properties: Dict[str, Any] = {}
    
    # Selected connections and tables
    selected_tables: List[int] = Field(..., description="List of table IDs to include in the dataset")
    
    # Column mapping options
    auto_include_mapped_columns: bool = Field(True, description="Automatically include columns with existing mappings")
    apply_value_mappings: bool = Field(True, description="Apply value mappings to standardize values")
    
    # Storage configuration
    storage_properties: Dict[str, Any] = {}

class SelectedTableInfo(BaseModel):
    table_id: int
    table_name: str
    schema_name: str
    connection_name: str
    selected_columns: List[int]

class MappingInfo(BaseModel):
    group_id: int
    group_name: str
    mapped_columns: List[int]
    standard_column_name: str
    data_type: str

class DatasetUnificationPreview(BaseModel):
    """Preview of what will be created before actual dataset creation"""
    selected_tables: List[SelectedTableInfo]
    unified_columns: List[Dict[str, Any]]
    mapping_groups: List[MappingInfo]
    value_mappings_count: int
    estimated_columns_count: int

# Response Schemas
class DatasetColumnResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    data_type: str
    is_nullable: bool
    column_position: int
    transformation_expression: Optional[str]
    is_visible: bool
    format_pattern: Optional[str]
    properties: Dict[str, Any]

class DatasetSourceResponse(BaseModel):
    id: int
    table_id: int
    join_type: str
    join_condition: Optional[str]
    filter_condition: Optional[str]

class DatasetResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    storage_type: str
    refresh_type: str
    refresh_schedule: Optional[str]
    status: str
    version: str
    properties: Dict[str, Any]
    sources: List[DatasetSourceResponse] = []
    columns: List[DatasetColumnResponse] = []
    data_criacao: datetime
    data_atualizacao: datetime

class DatasetUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    storage_type: Optional[DatasetStorageType] = None
    refresh_type: Optional[DatasetRefreshType] = None
    refresh_schedule: Optional[str] = None
    status: Optional[DatasetStatus] = None
    properties: Optional[Dict[str, Any]] = None

# Search Schemas
class SearchDataset(BaseModel):
    page: int = 1
    size: int = 10
    name: Optional[str] = None
    status: Optional[DatasetStatus] = None
    storage_type: Optional[DatasetStorageType] = None
    organization_id: Optional[int] = None
