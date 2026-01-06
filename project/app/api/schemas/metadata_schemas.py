from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from datetime import datetime

class CatalogResponse(BaseModel):
    id: int
    connection_id: int
    catalog_name: Optional[str] = None
    catalog_type: Optional[str] = None
    external_reference: Optional[str] = None
    properties: Dict[str, Any]
    fl_ativo: bool
    
    model_config = {
        "from_attributes": True
    }

class SchemaResponse(BaseModel):
    id: int
    catalog_id: Optional[int] = None
    connection_id: int
    schema_name: str
    external_reference: Optional[str] = None
    properties: Dict[str, Any]
    fl_ativo: bool
    
    model_config = {
        "from_attributes": True
    }

class TableResponse(BaseModel):
    id: int
    schema_id: int
    connection_id: int
    table_name: str
    external_reference: Optional[str] = None
    table_type: str
    estimated_row_count: Optional[int] = None
    total_size_bytes: Optional[int] = None
    last_analyzed: Optional[datetime] = None
    properties: Dict[str, Any]
    description: Optional[str] = None
    fl_ativo: bool
    
    model_config = {
        "from_attributes": True
    }

class ColumnResponse(BaseModel):
    id: int
    table_id: int
    column_name: str
    external_reference: Optional[str] = None
    data_type: str
    is_nullable: bool
    column_position: int
    max_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None
    is_primary_key: bool
    is_unique: bool
    is_indexed: bool
    
    # Foreign Key metadata
    is_foreign_key: bool = False
    fk_referenced_table_id: Optional[int] = None
    fk_referenced_column_id: Optional[int] = None
    fk_constraint_name: Optional[str] = None
    
    default_value: Optional[str] = None
    description: Optional[str] = None
    statistics: Dict[str, Any]
    sample_values: List[Any]
    properties: Dict[str, Any]
    fl_ativo: bool
    
    model_config = {
        "from_attributes": True
    }

class TableDetailsResponse(TableResponse):
    schema_name: Optional[str] = None
    columns: List[ColumnResponse]
    primary_key_count: int
    column_count: int

class DataPreviewResponse(BaseModel):
    table_id: int
    table_name: str
    schema_name: str
    column_names: List[str]
    rows: List[List[Any]]
    row_count: int
    preview_limit: int
    truncated: bool

class UpdateFlAtivoRequest(BaseModel):
    fl_ativo: bool
    
    model_config = {
        "from_attributes": True
    }

class BulkUpdateFlAtivoRequest(BaseModel):
    ids: List[int]
    fl_ativo: bool
    
    model_config = {
        "from_attributes": True
    }

class UpdateFlAtivoResponse(BaseModel):
    id: int
    fl_ativo: bool
    data_atualizacao: datetime
    
    model_config = {
        "from_attributes": True
    }

class BulkUpdateFlAtivoResponse(BaseModel):
    updated_count: int
    updated_ids: List[int]
    fl_ativo: bool
    
    model_config = {
        "from_attributes": True
    }


# ============================================================================
# CONSTRAINT EXTRACTION SCHEMAS
# ============================================================================

class ConstraintExtractionRequest(BaseModel):
    """Request to extract PK/FK constraints for a connection."""
    schemas: Optional[List[str]] = None  # If None, extracts from all schemas
    
    model_config = {
        "json_schema_extra": {
            "examples": [{
                "schemas": ["public", "defaultdb"]
            }]
        }
    }

class ConstraintExtractionResponse(BaseModel):
    """Response from constraint extraction."""
    connection_id: int
    catalog: str
    db_type: str
    schemas_processed: List[str]
    primary_keys_found: int
    foreign_keys_found: int
    errors: List[Dict[str, Any]]
    
    model_config = {
        "from_attributes": True
    }

class TableCardinalityInfo(BaseModel):
    """Information about PK/FK structure for cardinality inference."""
    table_id: int
    primary_keys: List[Dict[str, Any]]
    foreign_keys: List[Dict[str, Any]]
    has_composite_pk: bool
    fk_count: int
    
    model_config = {
        "from_attributes": True
    }

class JoinCardinalityResponse(BaseModel):
    """Inferred cardinality between two tables."""
    left_table_id: int
    right_table_id: int
    join_column_name: str
    cardinality: str  # ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ONE, MANY_TO_MANY
    
    model_config = {
        "from_attributes": True
    }