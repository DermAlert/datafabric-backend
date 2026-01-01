"""
Pydantic Schemas for Smart Bronze Architecture

These schemas define the API contracts for creating and managing
Bronze layer datasets using the Smart Bronze architecture.
"""

from pydantic import BaseModel, Field, validator, root_validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


# ==================== ENUMS ====================

class JoinStrategyEnum(str, Enum):
    """Join strategy for relationships"""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"


class OutputFormatEnum(str, Enum):
    """Output format for Bronze layer"""
    PARQUET = "parquet"
    DELTA = "delta"


class IngestionStatusEnum(str, Enum):
    """Status of ingestion execution"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


# ==================== TABLE SELECTION ====================

class TableColumnSelection(BaseModel):
    """
    Defines which columns to select from a table.
    
    If select_all is True, all columns will be included.
    Otherwise, only the columns in column_ids will be included.
    """
    table_id: int = Field(..., description="ID of the table to include")
    select_all: bool = Field(False, description="If True, select all columns from this table")
    column_ids: Optional[List[int]] = Field(None, description="Specific column IDs to include (ignored if select_all=True)")
    
    @validator('column_ids')
    def validate_columns(cls, v, values):
        if not values.get('select_all') and not v:
            raise ValueError("column_ids is required when select_all is False")
        return v


# Internal schemas used by the service (not exposed in API)
class IntraDBRelationship(BaseModel):
    """Internal: Join relationship between tables in the SAME database."""
    left_table_id: int
    left_column_id: int
    right_table_id: int
    right_column_id: int
    join_strategy: JoinStrategyEnum = JoinStrategyEnum.FULL
    custom_condition: Optional[str] = None


class InterDBRelationship(BaseModel):
    """Internal: Link between tables in DIFFERENT databases (for Silver)."""
    left_table_id: int
    left_column_id: int
    right_table_id: int
    right_column_id: int
    join_strategy: JoinStrategyEnum = JoinStrategyEnum.FULL
    description: Optional[str] = None


# Internal schema used by service
class DatasetBronzeCreate(BaseModel):
    """Internal schema - use DatasetBronzeCreateRequest for API"""
    name: str
    description: Optional[str] = None
    tables: List[TableColumnSelection]
    intra_db_relationships: Optional[List[IntraDBRelationship]] = None
    inter_db_relationships: Optional[List[InterDBRelationship]] = None
    output_format: OutputFormatEnum = OutputFormatEnum.PARQUET
    output_bucket: Optional[str] = None
    partition_columns: Optional[List[str]] = None
    enable_federated_joins: bool = Field(
        default=False,
        description="If True, performs JOINs between different databases using Trino federated queries in Bronze layer"
    )
    properties: Dict[str, Any] = Field(default_factory=dict)


# ==================== PREVIEW ====================

class IngestionGroupPreview(BaseModel):
    """Preview of a single ingestion group"""
    group_name: str
    connection_id: int
    connection_name: str
    tables: List[Dict[str, Any]]  # Table info with selected columns
    has_joins: bool
    estimated_sql: Optional[str] = None
    output_path: str


class BronzeIngestionPreview(BaseModel):
    """
    Preview of the Bronze ingestion plan before execution.
    
    Shows how the dataset will be split into ingestion groups
    and what SQL will be generated for each group.
    """
    dataset_name: str
    total_tables: int
    total_columns: int
    ingestion_groups: List[IngestionGroupPreview]
    inter_db_links: List[Dict[str, Any]]  # Links for Silver layer
    estimated_output_paths: List[str]
    warnings: List[str] = []
    federated_joins_enabled: bool = Field(
        default=False,
        description="Indicates if federated joins between databases are enabled"
    )


# ==================== EXECUTION ====================

class IngestionGroupResult(BaseModel):
    """Result of executing a single ingestion group"""
    group_name: str
    connection_name: str
    status: IngestionStatusEnum
    output_path: Optional[str] = None
    rows_ingested: Optional[int] = None
    execution_time_seconds: Optional[float] = None
    error_message: Optional[str] = None


class BronzeIngestionResult(BaseModel):
    """Result of executing the full Bronze ingestion"""
    dataset_id: int
    dataset_name: str
    status: IngestionStatusEnum
    groups: List[IngestionGroupResult]
    total_rows_ingested: int
    total_execution_time_seconds: float
    bronze_paths: List[str]
    message: str


# ==================== RESPONSE SCHEMAS ====================

class SourceRelationshipResponse(BaseModel):
    """Response schema for a source relationship"""
    id: int
    left_table_id: int
    left_table_name: str
    left_column_id: int
    left_column_name: str
    right_table_id: int
    right_table_name: str
    right_column_id: int
    right_column_name: str
    relationship_type: str
    join_strategy: str
    is_verified: bool


class DatasetBronzeConfigResponse(BaseModel):
    """Response schema for Bronze configuration"""
    id: int
    dataset_id: int
    name: str
    description: Optional[str]
    bronze_bucket: str
    bronze_path_prefix: str
    output_format: str
    partition_columns: Optional[List[str]]
    last_ingestion_time: Optional[datetime]
    last_ingestion_status: Optional[str]
    ingestion_groups_count: int


class IngestionGroupResponse(BaseModel):
    """Response schema for an ingestion group"""
    id: int
    group_name: str
    connection_id: int
    connection_name: str
    output_path: str
    status: str
    last_execution_time: Optional[datetime]
    rows_ingested: Optional[int]
    tables_count: int
    generated_sql: Optional[str] = None


# ==================== SEARCH/LIST ====================

class SearchBronzeDatasets(BaseModel):
    """Search parameters for Bronze datasets"""
    page: int = Field(default=1, ge=1)
    size: int = Field(default=20, ge=1, le=100)
    name: Optional[str] = None
    status: Optional[IngestionStatusEnum] = None
    connection_id: Optional[int] = None


# ==================== BRONZE DATASET CREATION (API Schema) ====================

class DatasetBronzeCreateRequest(BaseModel):
    """
    Schema for creating a Bronze layer dataset.
    
    **Important:** Relationships are NOT defined here. They must be defined
    beforehand in the metadata layer using the `/relationships` endpoints.
    
    The system will automatically:
    1. Look up existing relationships between the selected tables
    2. Use intra-connection relationships for Trino pushdown joins
    3. Store inter-connection relationships for Silver layer (Spark)
    
    **Federated Joins (Optional):**
    - Set `enable_federated_joins=True` to perform JOINs between different databases
      directly in Bronze layer using Trino federated queries
    - When enabled, all tables are joined into a single unified output
    - When disabled (default), tables from different connections are stored separately
      and inter-connection relationships are saved for Silver layer processing
    
    **Workflow:**
    1. Connect your databases (POST /data-connections)
    2. Define relationships (POST /relationships or POST /relationships/discover)
    3. Create Bronze dataset (this endpoint) - just select tables/columns
    """
    # Basic info
    name: str = Field(..., min_length=1, max_length=255, description="Name of the dataset")
    description: Optional[str] = Field(None, description="Description of the dataset")
    
    # Table and column selection
    tables: List[TableColumnSelection] = Field(
        ..., 
        min_items=1,
        description="Tables to include in the dataset with their column selections"
    )
    
    # Relationship filtering (optional - auto-discovered if not provided)
    relationship_ids: Optional[List[int]] = Field(
        None,
        description="Optional. If not provided, automatically uses all applicable relationships between selected tables."
    )
    
    # Federated joins configuration
    enable_federated_joins: bool = Field(
        default=False,
        description=(
            "Enable Trino federated JOINs between different databases in Bronze layer. "
            "If False (default), inter-database relationships are stored for Silver layer processing."
        )
    )
    
    # Output configuration
    output_format: OutputFormatEnum = Field(
        OutputFormatEnum.PARQUET,
        description="Output format for the Bronze data"
    )
    output_bucket: Optional[str] = Field(
        None,
        description="MinIO bucket for output (defaults to system bronze bucket)"
    )
    partition_columns: Optional[List[str]] = Field(
        None,
        description="Columns to partition by (optional)"
    )
    
    # Additional properties
    properties: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional properties for the dataset"
    )


# Keep for backwards compatibility
DatasetBronzeCreateSimplified = DatasetBronzeCreateRequest


class RelationshipUsagePreview(BaseModel):
    """Preview of how a relationship will be used in ingestion"""
    relationship_id: int
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    scope: str  # intra_connection or inter_connection
    usage: str  # "pushdown_join" or "silver_layer_link"
    join_type: str


# ==================== VIRTUALIZED QUERY ====================

class BronzeVirtualizedRequest(BaseModel):
    """
    Request for virtualized Bronze query (without saving to Delta).
    
    Same structure as DatasetBronzeCreateRequest but used for 
    executing queries and returning data directly.
    """
    # Table and column selection
    tables: List[TableColumnSelection] = Field(
        ..., 
        min_items=1,
        description="Tables to include with their column selections"
    )
    
    # Relationship filtering (optional - auto-discovered if not provided)
    relationship_ids: Optional[List[int]] = Field(
        None,
        description="Optional. If not provided, automatically uses all applicable relationships between selected tables."
    )
    
    # Federated joins configuration
    enable_federated_joins: bool = Field(
        default=False,
        description="Enable Trino federated JOINs between different databases."
    )


class BronzeVirtualizedGroupResult(BaseModel):
    """Result of one virtualized query group"""
    group_name: str
    connection_name: str
    columns: List[str]
    data: List[Dict[str, Any]]
    row_count: int
    sql_executed: Optional[str] = None


class BronzeVirtualizedResponse(BaseModel):
    """
    Response for virtualized Bronze query.
    
    Returns the data directly without saving to Delta/Parquet.
    Useful for exploration, preview with data, and light API consumption.
    """
    total_tables: int
    total_columns: int
    groups: List[BronzeVirtualizedGroupResult]
    total_rows: int
    execution_time_seconds: float
    warnings: List[str] = []
    message: str

