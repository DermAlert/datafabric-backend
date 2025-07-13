from pydantic import BaseModel, Field, validator
from typing import Dict, Any, Optional
from app.api.schemas.search import BaseSearchRequest

class ConnectionTypeBase(BaseModel):
    name: str = Field(..., description="Unique name of the connection type (e.g., 'PostgreSQL', 'MinIO')")
    description: Optional[str] = None
    icon: Optional[str] = None
    color_hex: Optional[str] = None
    connection_params_schema: Dict[str, Any]
    metadata_extraction_method: str

    @validator('color_hex')
    def validate_color_hex(cls, v):
        if v is not None and (not v.startswith('#') or len(v) != 7):
            raise ValueError('color_hex must be in format #RRGGBB')
        return v

class ConnectionTypeCreate(ConnectionTypeBase):
    pass


class SearchConnectionType(BaseSearchRequest):
    connection_type_id: Optional[int] = None
    name: Optional[str] = None
    metadata_extraction_method: Optional[str] = None
  

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
    model_config = {
        "from_attributes": True
    }