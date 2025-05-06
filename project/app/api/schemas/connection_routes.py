from pydantic import BaseModel, Field, validator
from typing import Dict, Optional, Any
from enum import Enum

class MetadataExtractionMethod(str, Enum):
    """Define os métodos possíveis de extração de metadados."""
    AIRFLOW_JOB = "airflow_job"
    DIRECT_QUERY = "direct_query"
    API_CALL = "api_call"

class ConnectionTypeBase(BaseModel):
    name: str = Field(..., description="Unique name of the connection type (e.g., 'PostgreSQL', 'MinIO')")
    description: Optional[str] = Field(None, description="Description of the connection type")
    icon: Optional[str] = Field(None, description="Icon identifier or URL for the connection type")
    color_hex: Optional[str] = Field(None, description="Color hex code (e.g., '#FF5733') for visual representation")
    connection_params_schema: Dict[str, Any] = Field(
        ..., 
        description="JSON Schema defining the required parameters for this connection type"
    )
    metadata_extraction_method: str = Field(
        ..., 
        description="Method to extract metadata (e.g., 'airflow_job', 'direct_query', 'api_call')"
    )

    @validator('color_hex')
    def validate_color_hex(cls, v):
        if v is not None and (not v.startswith('#') or len(v) != 7):
            raise ValueError('color_hex must be in format #RRGGBB')
        return v

class ConnectionTypeCreate(ConnectionTypeBase):
    pass

class ConnectionTypeUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    icon: Optional[str] = None
    color_hex: Optional[str] = None
    connection_params_schema: Optional[Dict[str, Any]] = None
    metadata_extraction_method: Optional[str] = None

    @validator('color_hex')
    def validate_color_hex(cls, v):
        if v is not None and (not v.startswith('#') or len(v) != 7):
            raise ValueError('color_hex must be in format #RRGGBB')
        return v

class ConnectionTypeResponse(ConnectionTypeBase):
    id: int
    
    class Config:
        orm_mode = True