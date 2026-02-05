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
    column_mappings: Optional[List[Dict[str, Any]]] = None  # Maps external_column_id → bronze_column_name


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


# ==================== BRONZE CONFIG ARCHITECTURE ====================
# These schemas mirror the Silver layer architecture for consistency.
# - VirtualizedConfig: Query original sources (no persistence)
# - PersistentConfig: Materialize raw data to Delta Lake

class BronzeExecutionStatusEnum(str, Enum):
    """Status of a Bronze execution"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


class WriteModeEnum(str, Enum):
    """
    Write mode for Delta Lake operations.
    
    - **overwrite**: Replace all data each execution
    - **append**: Add to existing data (may create duplicates)
    - **merge**: Upsert based on merge_keys - insert new, update existing (recommended)
    """
    OVERWRITE = "overwrite"
    APPEND = "append"
    MERGE = "merge"


# ==================== VIRTUALIZED CONFIG ====================

class BronzeVirtualizedConfigCreate(BaseModel):
    """
    Create a virtualized config for querying original data sources via Trino.
    Data is NOT saved - returned as JSON (use for exploration, APIs, etc.).
    
    This is like a "saved query" that can be executed multiple times.
    
    ---
    
    ## **Request Fields:**
    
    | Field | Type | Required | Description |
    |-------|------|----------|-------------|
    | `name` | string | ✅ | Unique name for the config |
    | `description` | string | ❌ | Optional description |
    | `tables` | object[] | ✅ | Tables and columns to include |
    | `relationship_ids` | int[] | ❌ | Relationships for JOINs. `null`=auto-discover, `[]`=CROSS JOIN |
    | `enable_federated_joins` | bool | ❌ | Enable cross-database JOINs (default: false) |
    
    ---
    
    ## **tables Structure:**
    
    ```json
    "tables": [
      {"table_id": 1, "select_all": true},
      {"table_id": 2, "column_ids": [10, 11, 12]}
    ]
    ```
    
    | Field | Type | Description |
    |-------|------|-------------|
    | `table_id` | int | ID from GET /api/metadata/tables |
    | `select_all` | bool | If true, includes all columns (default: false) |
    | `column_ids` | int[] | Specific columns (required if select_all=false) |
    
    ---
    
    ## **Example 1: Minimal (all columns from one table)**
    ```json
    {
      "name": "users_exploration",
      "tables": [{"table_id": 1, "select_all": true}]
    }
    ```
    
    ## **Example 2: Multiple tables with auto-discovered relationships**
    ```json
    {
      "name": "users_orders_view",
      "description": "Users with their orders",
      "tables": [
        {"table_id": 1, "select_all": true},
        {"table_id": 2, "column_ids": [10, 11, 12]}
      ]
    }
    ```
    
    ## **Example 3: With federated joins (cross-database)**
    ```json
    {
      "name": "unified_clinical_view",
      "tables": [
        {"table_id": 1, "select_all": true},
        {"table_id": 10, "select_all": true}
      ],
      "enable_federated_joins": true
    }
    ```
    """
    name: str = Field(..., min_length=1, max_length=255, description="Unique name for the config")
    description: Optional[str] = Field(None, description="Optional description")
    
    tables: List[TableColumnSelection] = Field(
        ..., 
        min_items=1,
        description="Tables and columns to include"
    )
    
    relationship_ids: Optional[List[int]] = Field(
        None,
        description="Relationship IDs for JOINs. null=auto-discover, []=CROSS JOIN (not recommended)"
    )
    
    enable_federated_joins: bool = Field(
        False,
        description="Enable Trino federated JOINs between different databases"
    )


class BronzeVirtualizedConfigUpdate(BaseModel):
    """Update a virtualized config."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    tables: Optional[List[TableColumnSelection]] = None
    relationship_ids: Optional[List[int]] = None
    enable_federated_joins: Optional[bool] = None
    is_active: Optional[bool] = None


class BronzeVirtualizedConfigResponse(BaseModel):
    """Response for a virtualized config."""
    id: int
    name: str
    description: Optional[str]
    tables: List[Dict[str, Any]]
    relationship_ids: Optional[List[int]]
    enable_federated_joins: bool
    generated_sql: Optional[str]
    is_active: bool
    last_query_time: Optional[datetime]
    last_query_rows: Optional[int]
    data_criacao: Optional[datetime] = None
    data_atualizacao: Optional[datetime] = None
    
    model_config = {"from_attributes": True}


class BronzeVirtualizedQueryResponse(BaseModel):
    """Response for executing a virtualized config query."""
    config_id: int
    config_name: str
    total_tables: int
    total_columns: int
    groups: List[BronzeVirtualizedGroupResult]
    total_rows: int
    execution_time_seconds: float
    warnings: List[str] = []


# ==================== PERSISTENT CONFIG ====================

class BronzePersistentConfigCreate(BaseModel):
    """
    Create a persistent config for materializing raw data to Bronze Delta Lake.
    
    This is like a "saved ETL job" that can be executed multiple times.
    On each execution, data is extracted from sources and saved to Delta Lake.
    
    ---
    
    ## **Versioning with Delta Lake:**
    
    | write_mode | Behavior | Duplicates | Use Case |
    |------------|----------|------------|----------|
    | `overwrite` | Replace all data | N/A | Full snapshot |
    | `append` | Add to existing | ⚠️ May duplicate | Event logs |
    | `merge` | Upsert (insert/update) | ✅ No duplicates | **Recommended** |
    
    **merge_keys**: Columns used to identify unique records (like a primary key).
    - If not provided with `write_mode=merge`, auto-detects from source table PKs
    - If no PK found, falls back to `overwrite` mode
    
    ---
    
    ## **Request Fields:**
    
    | Field | Type | Required | Description |
    |-------|------|----------|-------------|
    | `name` | string | ✅ | Unique name for the config |
    | `description` | string | ❌ | Optional description |
    | `tables` | object[] | ✅ | Tables and columns to include |
    | `relationship_ids` | int[] | ❌ | Relationships for JOINs. `null`=auto-discover |
    | `enable_federated_joins` | bool | ❌ | Enable cross-database JOINs (default: false) |
    | `output_format` | string | ❌ | "parquet" or "delta" (default: "delta") |
    | `write_mode` | string | ❌ | "overwrite", "append", or "merge" (default: "merge") |
    | `merge_keys` | string[] | ❌ | Columns for deduplication (auto-detected if null) |
    | `output_bucket` | string | ❌ | MinIO bucket (defaults to system bronze bucket) |
    | `output_path_prefix` | string | ❌ | Custom path prefix |
    | `partition_columns` | string[] | ❌ | Columns to partition by |
    
    ---
    
    ## **Example 1: Minimal (uses merge with auto-detected PK)**
    ```json
    {
      "name": "users_bronze",
      "tables": [{"table_id": 1, "select_all": true}]
    }
    ```
    
    ## **Example 2: Explicit merge keys**
    ```json
    {
      "name": "patients_bronze",
      "tables": [{"table_id": 1, "select_all": true}],
      "write_mode": "merge",
      "merge_keys": ["patient_id"]
    }
    ```
    
    ## **Example 3: Composite merge keys**
    ```json
    {
      "name": "visits_bronze",
      "tables": [{"table_id": 1, "select_all": true}],
      "write_mode": "merge",
      "merge_keys": ["hospital_id", "patient_id", "visit_id"]
    }
    ```
    
    ## **Example 4: Overwrite mode (full refresh)**
    ```json
    {
      "name": "snapshot_daily",
      "tables": [{"table_id": 1, "select_all": true}],
      "write_mode": "overwrite"
    }
    ```
    
    ## **Example 5: Complete with all options**
    ```json
    {
      "name": "unified_clinical_bronze",
      "description": "All clinical data unified from multiple sources",
      "tables": [
        {"table_id": 1, "select_all": true},
        {"table_id": 10, "select_all": true}
      ],
      "relationship_ids": [1, 2],
      "enable_federated_joins": true,
      "output_format": "delta",
      "write_mode": "merge",
      "merge_keys": ["patient_id"],
      "output_bucket": "my-bronze-bucket",
      "output_path_prefix": "clinical/v1",
      "partition_columns": ["hospital_id"]
    }
    ```
    
    ---
    
    ## **Workflow:**
    1. **Create config** → POST /api/bronze/configs/persistent
    2. **Preview** → POST /api/bronze/configs/persistent/{id}/preview
    3. **Execute** → POST /api/bronze/configs/persistent/{id}/execute (creates version 0)
    4. **Execute again** → POST /api/bronze/configs/persistent/{id}/execute (creates version 1, merges data)
    5. **View history** → GET /api/bronze/configs/persistent/{id}/versions
    6. **Time travel** → GET /api/bronze/configs/persistent/{id}/data?version=0
    """
    name: str = Field(..., min_length=1, max_length=255, description="Unique name for the config")
    description: Optional[str] = Field(None, description="Optional description")
    
    tables: List[TableColumnSelection] = Field(
        ..., 
        min_items=1,
        description="Tables and columns to include"
    )
    
    relationship_ids: Optional[List[int]] = Field(
        None,
        description="Relationship IDs for JOINs. null=auto-discover"
    )
    
    enable_federated_joins: bool = Field(
        False,
        description="Enable Trino federated JOINs between different databases"
    )
    
    output_format: OutputFormatEnum = Field(
        OutputFormatEnum.DELTA,
        description="Output format (default: delta for versioning support)"
    )
    
    # === VERSIONING FIELDS ===
    write_mode: WriteModeEnum = Field(
        WriteModeEnum.MERGE,
        description=(
            "Write mode: 'overwrite' (replace all), 'append' (add without checking), "
            "'merge' (upsert - recommended, no duplicates)"
        )
    )
    
    merge_keys: Optional[List[str]] = Field(
        None,
        description=(
            "Columns used for MERGE deduplication (like a primary key). "
            "If null with write_mode='merge', auto-detects from source table PKs. "
            "If no PK found, falls back to 'overwrite'."
        )
    )
    
    output_bucket: Optional[str] = Field(
        None,
        description="MinIO bucket for output (defaults to system bronze bucket)"
    )
    
    output_path_prefix: Optional[str] = Field(
        None,
        description="Custom path prefix for output"
    )
    
    partition_columns: Optional[List[str]] = Field(
        None,
        description="Columns to partition by"
    )
    
    properties: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional properties"
    )


class BronzePersistentConfigUpdate(BaseModel):
    """Update a persistent config."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    tables: Optional[List[TableColumnSelection]] = None
    relationship_ids: Optional[List[int]] = None
    enable_federated_joins: Optional[bool] = None
    output_format: Optional[OutputFormatEnum] = None
    write_mode: Optional[WriteModeEnum] = None
    merge_keys: Optional[List[str]] = None
    output_bucket: Optional[str] = None
    output_path_prefix: Optional[str] = None
    partition_columns: Optional[List[str]] = None
    properties: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None


class BronzePersistentConfigResponse(BaseModel):
    """Response for a persistent config."""
    id: int
    name: str
    description: Optional[str]
    tables: List[Dict[str, Any]]
    relationship_ids: Optional[List[int]]
    enable_federated_joins: bool
    output_format: str
    
    # Versioning fields
    write_mode: str = "merge"
    merge_keys: Optional[List[str]] = None
    merge_keys_source: Optional[str] = None  # 'user_defined', 'auto_detected_pk', or null
    current_delta_version: Optional[int] = None
    
    output_bucket: Optional[str]
    output_path_prefix: Optional[str]
    partition_columns: Optional[List[str]]
    properties: Optional[Dict[str, Any]]
    is_active: bool
    last_execution_time: Optional[datetime]
    last_execution_status: Optional[str]
    last_execution_rows: Optional[int]
    dataset_id: Optional[int]
    data_criacao: Optional[datetime] = None
    data_atualizacao: Optional[datetime] = None
    
    model_config = {"from_attributes": True}


class BronzePersistentPreviewResponse(BaseModel):
    """Preview of persistent config execution."""
    config_id: int
    config_name: str
    ingestion_groups: List[IngestionGroupPreview]
    inter_db_links: List[Dict[str, Any]]
    estimated_output_paths: List[str]
    total_tables: int
    total_columns: int
    warnings: List[str] = []
    
    # Versioning info
    write_mode: str = "merge"
    merge_keys: Optional[List[str]] = None
    merge_keys_source: Optional[str] = None


# ==================== EXECUTION HISTORY ====================

class BronzeExecutionResponse(BaseModel):
    """Response for a Bronze execution history entry."""
    id: int
    config_id: int
    status: str
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    group_results: Optional[List[Dict[str, Any]]]
    rows_ingested: Optional[int]
    output_paths: Optional[List[str]]
    error_message: Optional[str]
    
    # Delta Lake versioning info
    delta_version: Optional[int] = None
    write_mode_used: Optional[str] = None
    merge_keys_used: Optional[List[str]] = None
    
    # MERGE statistics
    rows_inserted: Optional[int] = None
    rows_updated: Optional[int] = None
    rows_deleted: Optional[int] = None
    
    # Config snapshot at execution time (for reproducibility)
    config_snapshot: Optional[Dict[str, Any]] = None
    
    model_config = {"from_attributes": True}


class BronzePersistentExecuteResponse(BaseModel):
    """Response for persistent config execution."""
    config_id: int
    config_name: str
    execution_id: int
    status: BronzeExecutionStatusEnum
    groups: List[IngestionGroupResult]
    total_rows_ingested: int
    bronze_paths: List[str]
    execution_time_seconds: float
    message: str
    
    # Delta Lake versioning info
    delta_version: Optional[int] = None
    write_mode_used: str = "merge"
    merge_keys_used: Optional[List[str]] = None
    
    # MERGE statistics (when write_mode=merge)
    rows_inserted: Optional[int] = None
    rows_updated: Optional[int] = None
    
    # Config snapshot at execution time (for reproducibility)
    config_snapshot: Optional[Dict[str, Any]] = None


# ==================== VERSION HISTORY ====================

class DeltaVersionInfo(BaseModel):
    """Information about a single Delta Lake version."""
    version: int
    timestamp: datetime
    operation: str  # WRITE, MERGE, DELETE, etc.
    execution_id: Optional[int] = None
    
    # Statistics
    rows_inserted: Optional[int] = None
    rows_updated: Optional[int] = None
    rows_deleted: Optional[int] = None
    total_rows: Optional[int] = None
    
    # Size info
    num_files: Optional[int] = None
    size_bytes: Optional[int] = None
    
    # Config snapshot at this version (for diff comparison)
    config_snapshot: Optional[Dict[str, Any]] = None


class BronzeVersionHistoryResponse(BaseModel):
    """Response for version history of a Bronze config."""
    config_id: int
    config_name: str
    current_version: Optional[int]
    output_paths: List[str]  # All output paths (for non-federated configs with multiple sources)
    versions: List[DeltaVersionInfo]  # Aggregated metrics across all paths


class BronzeDataQueryResponse(BaseModel):
    """Response for querying Bronze data with optional time travel."""
    config_id: int
    config_name: str
    version: Optional[int] = None  # null = latest
    as_of_timestamp: Optional[datetime] = None
    columns: List[str]
    data: List[Dict[str, Any]]
    row_count: int
    total_rows: Optional[int] = None
    execution_time_seconds: float

