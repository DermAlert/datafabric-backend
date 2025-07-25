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

class SelectionMode(str, Enum):
    TABLES = "tables"
    COLUMNS = "columns"

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

class SelectionMode(str, Enum):
    TABLES = "tables"
    COLUMNS = "columns"

class DatasetUnifiedCreate(BaseModel):
    """Schema for creating datasets with automatic column mapping unification"""
    name: str
    description: Optional[str] = None
    storage_type: DatasetStorageType = DatasetStorageType.VIRTUAL_VIEW
    refresh_type: DatasetRefreshType = DatasetRefreshType.ON_DEMAND
    refresh_schedule: Optional[str] = None
    version: str = "1.0"
    properties: Dict[str, Any] = {}
    
    # Selection mode: choose between selecting tables or specific columns
    selection_mode: SelectionMode = Field(..., description="Whether to select entire tables or specific columns")
    
    # Table-based selection (when selection_mode = "tables")
    selected_tables: Optional[List[int]] = Field(None, description="List of table IDs to include in the dataset")
    
    # Column-based selection (when selection_mode = "columns") 
    selected_columns: Optional[List[int]] = Field(None, description="List of specific column IDs to include in the dataset")
    
    # Column mapping options
    auto_include_mapped_columns: bool = Field(True, description="Automatically include columns with existing mappings")
    apply_value_mappings: bool = Field(True, description="Apply value mappings to standardize values")
    
    # Storage configuration
    storage_properties: Dict[str, Any] = {}
    
    @validator('selected_tables')
    def validate_tables_selection(cls, v, values):
        selection_mode = values.get('selection_mode')
        if selection_mode == SelectionMode.TABLES and not v:
            raise ValueError("selected_tables is required when selection_mode is 'tables'")
        if selection_mode == SelectionMode.COLUMNS and v:
            raise ValueError("selected_tables should not be provided when selection_mode is 'columns'")
        return v
    
    @validator('selected_columns')
    def validate_columns_selection(cls, v, values):
        selection_mode = values.get('selection_mode')
        if selection_mode == SelectionMode.COLUMNS and not v:
            raise ValueError("selected_columns is required when selection_mode is 'columns'")
        if selection_mode == SelectionMode.TABLES and v:
            raise ValueError("selected_columns should not be provided when selection_mode is 'tables'")
        return v

class DatasetUnificationPreviewRequest(BaseModel):
    """Request schema for preview endpoint"""
    selection_mode: SelectionMode = Field(..., description="Whether to select entire tables or specific columns")
    selected_tables: Optional[List[int]] = Field(None, description="List of table IDs (when selection_mode='tables')")
    selected_columns: Optional[List[int]] = Field(None, description="List of column IDs (when selection_mode='columns')")
    auto_include_mapped_columns: bool = Field(True, description="Automatically include related mapped columns/tables")
    apply_value_mappings: bool = Field(True, description="Apply value mappings to standardize values")
    
    @validator('selected_tables')
    def validate_tables_selection(cls, v, values):
        selection_mode = values.get('selection_mode')
        if selection_mode == SelectionMode.TABLES and not v:
            raise ValueError("selected_tables is required when selection_mode is 'tables'")
        if selection_mode == SelectionMode.COLUMNS and v:
            raise ValueError("selected_tables should not be provided when selection_mode is 'columns'")
        return v
    
    @validator('selected_columns')
    def validate_columns_selection(cls, v, values):
        selection_mode = values.get('selection_mode')
        if selection_mode == SelectionMode.COLUMNS and not v:
            raise ValueError("selected_columns is required when selection_mode is 'columns'")
        if selection_mode == SelectionMode.TABLES and v:
            raise ValueError("selected_columns should not be provided when selection_mode is 'tables'")
        return v

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
    selection_mode: SelectionMode
    selected_tables: List[SelectedTableInfo]
    selected_columns_info: Optional[List[Dict[str, Any]]] = None  # For column-based selection
    unified_columns: List[Dict[str, Any]]
    mapping_groups: List[MappingInfo]
    value_mappings_count: int
    estimated_columns_count: int
    additional_tables_included: List[int] = []  # Tables auto-included due to mappings
    additional_columns_included: List[int] = []  # Columns auto-included due to mappings

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

# Dataset Images Schemas
class DatasetImageMetadata(BaseModel):
    """Metadata for a dataset image"""
    image_id: str
    image_name: str
    file_path: str
    file_size: Optional[int] = None
    content_type: Optional[str] = None
    data_criacao: Optional[datetime] = None
    data_atualizacao: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None

class DatasetImageResponse(BaseModel):
    """Response for dataset image with presigned URL"""
    image_id: str
    image_name: str
    presigned_url: str
    expires_in: int  # seconds
    metadata: DatasetImageMetadata

class SearchDatasetImages(BaseModel):
    """Search parameters for dataset images"""
    page: int = Field(default=1, ge=1)
    size: int = Field(default=20, ge=1, le=100)
    image_name: Optional[str] = None
    content_type: Optional[str] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None

class DatasetImageStats(BaseModel):
    """Statistics for dataset images"""
    total_images: int
    total_size_bytes: int
    content_types: Dict[str, int]
    size_distribution: Dict[str, int]  # small, medium, large
    date_range: Dict[str, Optional[datetime]]  # earliest, latest

class DatasetImageStats(BaseModel):
    """Statistics for dataset images"""
    total_images: int
    total_size_bytes: int
    content_types: Dict[str, int]
    latest_upload: Optional[datetime] = None
    oldest_upload: Optional[datetime] = None
    average_file_size: Optional[float] = None
