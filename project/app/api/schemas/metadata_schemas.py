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
    default_value: Optional[str] = None
    description: Optional[str] = None
    statistics: Dict[str, Any]
    sample_values: List[Any]
    properties: Dict[str, Any]
    
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