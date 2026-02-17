from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from enum import Enum

# ===================== Protocol Models =====================

class DeltaSharingProtocol(BaseModel):
    """Protocol information for Delta Sharing"""
    minReaderVersion: int = Field(default=1, description="Minimum reader version supported")
    minWriterVersion: Optional[int] = Field(default=None, description="Minimum writer version (if applicable)")

class TableMetadata(BaseModel):
    """Table metadata for Delta Sharing protocol"""
    id: str = Field(description="Unique table identifier")
    name: Optional[str] = Field(default=None, description="Table name")
    description: Optional[str] = Field(default=None, description="Table description")
    format: Dict[str, Any] = Field(default={"provider": "parquet"}, description="Table format information")
    schemaString: str = Field(description="JSON schema string")
    partitionColumns: List[str] = Field(default=[], description="Partition columns")
    configuration: Dict[str, str] = Field(default={}, description="Table configuration")
    version: Optional[int] = Field(default=None, description="Table version")
    size: Optional[int] = Field(default=None, description="Table size in bytes")
    numFiles: Optional[int] = Field(default=None, description="Number of files")

class FileAction(BaseModel):
    """File action in Delta protocol"""
    url: str = Field(description="Presigned URL for file access")
    id: str = Field(description="File identifier")
    partitionValues: Dict[str, str] = Field(default={}, description="Partition values")
    size: int = Field(description="File size in bytes")
    stats: Optional[str] = Field(default=None, description="File statistics as JSON string")
    version: Optional[int] = Field(default=None, description="Version when file was added")
    timestamp: Optional[int] = Field(default=None, description="Timestamp in milliseconds")

class RemoveFile(BaseModel):
    """Remove file action in Delta protocol"""
    url: str = Field(description="Presigned URL for removed file")
    id: str = Field(description="File identifier")
    partitionValues: Dict[str, str] = Field(default={}, description="Partition values")
    size: int = Field(description="File size in bytes")
    timestamp: int = Field(description="Timestamp when file was removed in milliseconds")
    version: int = Field(description="Version when file was removed")

# ===================== Query Models =====================

class QueryTableRequest(BaseModel):
    """Request for querying a table"""
    predicateHints: Optional[List[str]] = Field(default=None, description="Predicate hints for optimization")
    jsonPredicateHints: Optional[Union[str, Dict[str, Any]]] = Field(default=None, description="JSON predicate hints for structured filtering")
    limitHint: Optional[int] = Field(default=None, description="Limit hint for optimization")
    version: Optional[int] = Field(default=None, description="Specific version to query")
    timestamp: Optional[str] = Field(default=None, description="Timestamp for time travel queries")

# ===================== Response Models =====================

class ShareResponse(BaseModel):
    """Response model for a share"""
    name: str = Field(description="Share name")
    id: Optional[str] = Field(default=None, description="Share ID")

class ListSharesResponse(BaseModel):
    """Response for listing shares"""
    items: List[ShareResponse] = Field(description="List of shares")
    nextPageToken: Optional[str] = Field(default=None, description="Token for next page")

class SchemaResponse(BaseModel):
    """Response model for a schema"""
    name: str = Field(description="Schema name")
    share: str = Field(description="Share name this schema belongs to")

class ListSchemasResponse(BaseModel):
    """Response for listing schemas"""
    items: List[SchemaResponse] = Field(description="List of schemas")
    nextPageToken: Optional[str] = Field(default=None, description="Token for next page")

class TableResponse(BaseModel):
    """Response model for a table"""
    name: str = Field(description="Table name")
    schema: str = Field(description="Schema name")
    share: str = Field(description="Share name")
    shareId: Optional[str] = Field(default=None, description="Share ID")
    id: Optional[str] = Field(default=None, description="Table ID")

class ListTablesResponse(BaseModel):
    """Response for listing tables"""
    items: List[TableResponse] = Field(description="List of tables")
    nextPageToken: Optional[str] = Field(default=None, description="Token for next page")

class TableVersionResponse(BaseModel):
    """Response for table version"""
    version: int = Field(description="Table version")
    timestamp: Optional[str] = Field(default=None, description="Version timestamp")

# ===================== Management API Models =====================

class ShareStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"

class TableShareStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"

class ShareCreate(BaseModel):
    """Create share request"""
    name: str = Field(description="Share name", min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, description="Share description")
    owner_email: Optional[str] = Field(default=None, description="Owner email")
    contact_info: Optional[Dict[str, Any]] = Field(default=None, description="Contact information")
    terms_of_use: Optional[str] = Field(default=None, description="Terms of use")

    @validator('name')
    def validate_name(cls, v):
        # Delta Sharing name validation - alphanumeric, hyphens, underscores only
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Share name must contain only alphanumeric characters, hyphens, and underscores')
        return v

class ShareUpdate(BaseModel):
    """Update share request"""
    description: Optional[str] = Field(default=None, description="Share description")
    status: Optional[ShareStatus] = Field(default=None, description="Share status")
    owner_email: Optional[str] = Field(default=None, description="Owner email")
    contact_info: Optional[Dict[str, Any]] = Field(default=None, description="Contact information")
    terms_of_use: Optional[str] = Field(default=None, description="Terms of use")

class RecipientBasic(BaseModel):
    """Basic recipient info for listing in shares"""
    id: int = Field(description="Recipient ID")
    identifier: str = Field(description="Unique identifier")
    name: str = Field(description="Recipient name")
    email: Optional[str] = Field(default=None, description="Recipient email")
    is_active: bool = Field(description="Whether recipient is active")

class ShareBasic(BaseModel):
    """Basic share info for listing in recipients"""
    id: int = Field(description="Share ID")
    name: str = Field(description="Share name")
    description: Optional[str] = Field(default=None, description="Share description")
    status: ShareStatus = Field(description="Share status")

class ShareDetail(BaseModel):
    """Detailed share information"""
    id: int = Field(description="Share ID")
    name: str = Field(description="Share name")
    description: Optional[str] = Field(default=None, description="Share description")
    organization_id: int = Field(description="Organization ID")
    status: ShareStatus = Field(description="Share status")
    owner_email: Optional[str] = Field(default=None, description="Owner email")
    contact_info: Optional[Dict[str, Any]] = Field(default=None, description="Contact information")
    terms_of_use: Optional[str] = Field(default=None, description="Terms of use")
    data_criacao: datetime = Field(description="Creation timestamp")
    data_atualizacao: datetime = Field(description="Last update timestamp")
    schemas_count: int = Field(description="Number of schemas in share")
    tables_count: int = Field(description="Number of tables in share")
    recipients_count: int = Field(description="Number of recipients with access")
    recipients: List[RecipientBasic] = Field(default=[], description="Recipients with access to this share")

class SchemaCreate(BaseModel):
    """Create schema request"""
    name: str = Field(description="Schema name", min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, description="Schema description")

    @validator('name')
    def validate_name(cls, v):
        # Delta Sharing name validation
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Schema name must contain only alphanumeric characters, hyphens, and underscores')
        return v

class SchemaUpdate(BaseModel):
    """Update schema request"""
    description: Optional[str] = Field(default=None, description="Schema description")

class SchemaDetail(BaseModel):
    """Detailed schema information"""
    id: int = Field(description="Schema ID")
    name: str = Field(description="Schema name")
    description: Optional[str] = Field(default=None, description="Schema description")
    share_id: int = Field(description="Share ID")
    share_name: str = Field(description="Share name")
    data_criacao: datetime = Field(description="Creation timestamp")
    data_atualizacao: datetime = Field(description="Last update timestamp")
    tables_count: int = Field(description="Number of tables in schema")

class ShareTableCreate(BaseModel):
    """Create shared table request"""
    name: str = Field(description="Table name", min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, description="Table description")
    dataset_id: int = Field(description="Dataset ID to share")
    share_mode: str = Field(default="full", description="Share mode: full, filtered, aggregated")
    filter_condition: Optional[str] = Field(default=None, description="SQL WHERE clause for filtered sharing")

    @validator('name')
    def validate_name(cls, v):
        # Delta Sharing name validation
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Table name must contain only alphanumeric characters, hyphens, and underscores')
        return v

    @validator('share_mode')
    def validate_share_mode(cls, v):
        if v not in ['full', 'filtered', 'aggregated']:
            raise ValueError('Share mode must be one of: full, filtered, aggregated')
        return v

class ShareTableUpdate(BaseModel):
    """Update shared table request"""
    description: Optional[str] = Field(default=None, description="Table description")
    status: Optional[TableShareStatus] = Field(default=None, description="Table status")
    share_mode: Optional[str] = Field(default=None, description="Share mode: full, filtered, aggregated")
    filter_condition: Optional[str] = Field(default=None, description="SQL WHERE clause for filtered sharing")

    @validator('share_mode')
    def validate_share_mode(cls, v):
        if v is not None and v not in ['full', 'filtered', 'aggregated']:
            raise ValueError('Share mode must be one of: full, filtered, aggregated')
        return v

class ShareTableDetail(BaseModel):
    """Detailed shared table information"""
    id: int = Field(description="Table ID")
    name: str = Field(description="Table name")
    description: Optional[str] = Field(default=None, description="Table description")
    schema_id: int = Field(description="Schema ID")
    schema_name: str = Field(description="Schema name")
    share_id: int = Field(description="Share ID")
    share_name: str = Field(description="Share name")
    dataset_id: Optional[int] = Field(default=None, description="Dataset ID (null for virtualized)")
    dataset_name: Optional[str] = Field(default=None, description="Dataset name (null for virtualized)")
    status: TableShareStatus = Field(description="Table status")
    share_mode: str = Field(description="Share mode")
    filter_condition: Optional[str] = Field(default=None, description="Filter condition")
    current_version: int = Field(description="Current version")
    table_format: str = Field(description="Table format")
    partition_columns: Optional[List[str]] = Field(default=None, description="Partition columns")
    storage_location: Optional[str] = Field(default=None, description="Storage location")
    source_type: Optional[str] = Field(default="delta", description="Source type: delta, bronze_virtualized, silver_virtualized")
    bronze_virtualized_config_id: Optional[int] = Field(default=None, description="Bronze virtualized config ID")
    silver_virtualized_config_id: Optional[int] = Field(default=None, description="Silver virtualized config ID")
    data_criacao: datetime = Field(description="Creation timestamp")
    data_atualizacao: datetime = Field(description="Last update timestamp")

class RecipientCreate(BaseModel):
    """Create recipient request"""
    identifier: Optional[str] = Field(default=None, description="Unique recipient identifier (auto-generated if not provided)", max_length=255)
    name: str = Field(description="Recipient name", min_length=1, max_length=255)
    email: Optional[str] = Field(default=None, description="Recipient email")
    organization_name: Optional[str] = Field(default=None, description="Organization name")
    authentication_type: str = Field(default="bearer_token", description="Authentication type")
    max_requests_per_hour: Optional[int] = Field(default=None, description="Rate limit - requests per hour", ge=1)
    max_downloads_per_day: Optional[int] = Field(default=None, description="Rate limit - downloads per day", ge=1)
    contact_info: Optional[Dict[str, Any]] = Field(default=None, description="Contact information")
    notes: Optional[str] = Field(default=None, description="Additional notes")

    @validator('identifier')
    def validate_identifier(cls, v):
        if v is None:
            return v
        # Identifier validation
        import re
        if not re.match(r'^[a-zA-Z0-9_.-]+$', v):
            raise ValueError('Identifier must contain only alphanumeric characters, hyphens, underscores, and dots')
        return v

class RecipientUpdate(BaseModel):
    """Update recipient request"""
    name: Optional[str] = Field(default=None, description="Recipient name", min_length=1, max_length=255)
    email: Optional[str] = Field(default=None, description="Recipient email")
    organization_name: Optional[str] = Field(default=None, description="Organization name")
    is_active: Optional[bool] = Field(default=None, description="Whether recipient is active")
    max_requests_per_hour: Optional[int] = Field(default=None, description="Rate limit - requests per hour", ge=1)
    max_downloads_per_day: Optional[int] = Field(default=None, description="Rate limit - downloads per day", ge=1)
    contact_info: Optional[Dict[str, Any]] = Field(default=None, description="Contact information")
    notes: Optional[str] = Field(default=None, description="Additional notes")

class RecipientDetail(BaseModel):
    """Detailed recipient information"""
    id: int = Field(description="Recipient ID")
    identifier: str = Field(description="Unique identifier")
    name: str = Field(description="Recipient name")
    email: Optional[str] = Field(default=None, description="Recipient email")
    organization_name: Optional[str] = Field(default=None, description="Organization name")
    authentication_type: str = Field(description="Authentication type")
    bearer_token: Optional[str] = Field(default=None, description="Bearer token (masked for security)")
    token_expiry: Optional[datetime] = Field(default=None, description="Token expiry")
    is_active: bool = Field(description="Whether recipient is active")
    max_requests_per_hour: Optional[int] = Field(default=None, description="Rate limit - requests per hour")
    max_downloads_per_day: Optional[int] = Field(default=None, description="Rate limit - downloads per day")
    access_logged: bool = Field(description="Whether access is logged")
    data_usage_agreement_accepted: bool = Field(description="Whether data usage agreement is accepted")
    agreement_accepted_at: Optional[datetime] = Field(default=None, description="Agreement acceptance timestamp")
    contact_info: Optional[Dict[str, Any]] = Field(default=None, description="Contact information")
    notes: Optional[str] = Field(default=None, description="Additional notes")
    data_criacao: datetime = Field(description="Creation timestamp")
    data_atualizacao: datetime = Field(description="Last update timestamp")
    shares_count: int = Field(description="Number of shares this recipient has access to")
    shares: List[ShareBasic] = Field(default=[], description="Shares this recipient has access to")

class RecipientShareAssignment(BaseModel):
    """Assign recipient to share"""
    share_ids: List[int] = Field(description="List of share IDs to assign")

class ShareRecipientAssignment(BaseModel):
    """Assign recipients to share"""
    recipient_ids: List[int] = Field(description="List of recipient IDs to assign")

# ===================== Search and Pagination Models =====================

class SearchShares(BaseModel):
    """Search shares request"""
    page: int = Field(default=1, ge=1, description="Page number")
    size: int = Field(default=20, ge=1, le=100, description="Page size")
    search: Optional[str] = Field(default=None, description="Search term")
    status: Optional[ShareStatus] = Field(default=None, description="Filter by status")

class SearchSchemas(BaseModel):
    """Search schemas request"""
    page: int = Field(default=1, ge=1, description="Page number")
    size: int = Field(default=20, ge=1, le=100, description="Page size")
    search: Optional[str] = Field(default=None, description="Search term")

class SearchTables(BaseModel):
    """Search tables request"""
    page: int = Field(default=1, ge=1, description="Page number")
    size: int = Field(default=20, ge=1, le=100, description="Page size")
    search: Optional[str] = Field(default=None, description="Search term")
    status: Optional[TableShareStatus] = Field(default=None, description="Filter by status")

class SearchRecipients(BaseModel):
    """Search recipients request"""
    page: int = Field(default=1, ge=1, description="Page number")
    size: int = Field(default=20, ge=1, le=100, description="Page size")
    search: Optional[str] = Field(default=None, description="Search term")
    is_active: Optional[bool] = Field(default=None, description="Filter by active status")

# ===================== Error Models =====================

class DeltaSharingError(BaseModel):
    """Delta Sharing error response"""
    errorCode: str = Field(description="Error code")
    message: str = Field(description="Error message")
    details: Optional[Dict[str, Any]] = Field(default=None, description="Additional error details")


# ===================== Bronze/Silver Integration Models =====================

class DatasetSourceType(str, Enum):
    """Type of dataset source"""
    BRONZE = "bronze"
    SILVER = "silver"

class AvailableDataset(BaseModel):
    """Dataset available for sharing"""
    config_id: int = Field(description="Config ID (Bronze or Silver)")
    name: str = Field(description="Dataset name")
    description: Optional[str] = Field(default=None, description="Dataset description")
    source_type: DatasetSourceType = Field(description="Source type: bronze or silver")
    output_path: Optional[str] = Field(default=None, description="Delta Lake path")
    current_version: Optional[int] = Field(default=None, description="Current Delta version")
    last_execution_status: Optional[str] = Field(default=None, description="Last execution status")
    last_execution_at: Optional[datetime] = Field(default=None, description="Last execution timestamp")
    total_rows: Optional[int] = Field(default=None, description="Total rows in latest version")

class CreateTableFromBronze(BaseModel):
    """Create Delta Sharing table from Bronze config"""
    bronze_config_id: int = Field(description="Bronze Persistent Config ID")
    name: str = Field(description="Table name for Delta Sharing", min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, description="Table description")
    share_mode: str = Field(default="full", description="Share mode: full, filtered, aggregated")
    filter_condition: Optional[str] = Field(default=None, description="SQL WHERE clause for filtered sharing")
    path_index: int = Field(default=0, description="Path index for non-federated configs with multiple outputs")

    @validator('name')
    def validate_name(cls, v):
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Table name must contain only alphanumeric characters, hyphens, and underscores')
        return v.lower().replace('-', '_')

class CreateTableFromSilver(BaseModel):
    """Create Delta Sharing table from Silver config"""
    silver_config_id: int = Field(description="Silver Transform Config ID")
    name: str = Field(description="Table name for Delta Sharing", min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, description="Table description")
    share_mode: str = Field(default="full", description="Share mode: full, filtered, aggregated")
    filter_condition: Optional[str] = Field(default=None, description="SQL WHERE clause for filtered sharing")

    @validator('name')
    def validate_name(cls, v):
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Table name must contain only alphanumeric characters, hyphens, and underscores')
        return v.lower().replace('-', '_')

class IntegrationTableDetail(BaseModel):
    """Detail of table created from Bronze/Silver integration"""
    table_id: int = Field(description="Delta Sharing Table ID")
    table_name: str = Field(description="Table name")
    share_id: int = Field(description="Share ID")
    share_name: str = Field(description="Share name")
    schema_id: int = Field(description="Schema ID")
    schema_name: str = Field(description="Schema name")
    source_type: DatasetSourceType = Field(description="Source type: bronze or silver")
    source_config_id: int = Field(description="Source config ID")
    source_config_name: str = Field(description="Source config name")
    storage_location: Optional[str] = Field(default=None, description="Delta Lake storage location (null for virtualized)")
    current_version: int = Field(description="Current table version")
    share_mode: str = Field(description="Share mode")
    status: str = Field(description="Table status")


# ===================== Virtualized Table Integration Models =====================

class VirtualizedSourceType(str, Enum):
    """Source type for virtualized tables"""
    BRONZE_VIRTUALIZED = "bronze_virtualized"
    SILVER_VIRTUALIZED = "silver_virtualized"

class CreateTableFromBronzeVirtualized(BaseModel):
    """Create Delta Sharing table from a Bronze Virtualized Config"""
    bronze_virtualized_config_id: int = Field(description="Bronze Virtualized Config ID")
    name: str = Field(description="Table name for Delta Sharing", min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, description="Table description")

    @validator('name')
    def validate_name(cls, v):
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Table name must contain only alphanumeric characters, hyphens, and underscores')
        return v.lower().replace('-', '_')

class CreateTableFromSilverVirtualized(BaseModel):
    """Create Delta Sharing table from a Silver Virtualized Config"""
    silver_virtualized_config_id: int = Field(description="Silver Virtualized Config ID")
    name: str = Field(description="Table name for Delta Sharing", min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, description="Table description")

    @validator('name')
    def validate_name(cls, v):
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', v):
            raise ValueError('Table name must contain only alphanumeric characters, hyphens, and underscores')
        return v.lower().replace('-', '_')


# ===================== Data API Models (REST access for virtualized tables) =====================

class DataAPIFormat(str, Enum):
    """Output format for Data API"""
    JSON = "json"
    CSV = "csv"
    NDJSON = "ndjson"

class DataAPIRequest(BaseModel):
    """Request for querying data via the Data API"""
    format: DataAPIFormat = Field(default=DataAPIFormat.JSON, description="Output format")
    limit: int = Field(default=1000, ge=1, le=50000, description="Maximum rows to return")
    offset: int = Field(default=0, ge=0, description="Rows to skip (pagination)")

    @validator('limit')
    def cap_limit(cls, v):
        if v > 50000:
            raise ValueError('Limit cannot exceed 50000 rows per request')
        return v

class DataAPIColumnInfo(BaseModel):
    """Column info in data API response"""
    name: str = Field(description="Column name")
    type: Optional[str] = Field(default=None, description="Column data type")

class DataAPIResponse(BaseModel):
    """Response for Data API queries"""
    share: str = Field(description="Share name")
    schema_name: str = Field(description="Schema name")  # 'schema' is reserved in Pydantic
    table: str = Field(description="Table name")
    source_type: str = Field(description="Source type (delta, bronze_virtualized, silver_virtualized)")
    columns: List[DataAPIColumnInfo] = Field(description="Column metadata")
    data: List[Dict[str, Any]] = Field(description="Query results as list of row dicts")
    row_count: int = Field(description="Number of rows returned")
    has_more: bool = Field(description="Whether more rows exist beyond limit+offset")
    execution_time_seconds: float = Field(description="Query execution time")

class DataAPIErrorResponse(BaseModel):
    """Error response for Data API"""
    error: str = Field(description="Error type")
    message: str = Field(description="Human-readable error message")
    detail: Optional[str] = Field(default=None, description="Technical detail")
