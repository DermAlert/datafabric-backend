from pydantic import BaseModel
from typing import Dict, Any, Optional
from datetime import datetime
from enum import Enum
from app.api.schemas.search import BaseSearchRequest

class ContentType(str, Enum):
    METADATA = "metadata"
    IMAGE = "image"

class DataConnectionBase(BaseModel):
    name: str
    description: Optional[str] = None
    connection_type_id: int
    connection_params: Dict[str, Any]
    content_type: ContentType = ContentType.METADATA
    cron_expression: Optional[str] = None
    sync_settings: Optional[Dict[str, Any]] = None

class DataConnectionCreate(DataConnectionBase):
    organization_id: int

class DataConnectionUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    connection_params: Optional[Dict[str, Any]] = None
    content_type: Optional[ContentType] = None
    status: Optional[str] = None
    cron_expression: Optional[str] = None
    sync_settings: Optional[Dict[str, Any]] = None

class SearchDataConnection(BaseSearchRequest):
    organization_id: Optional[int] = None
    status: Optional[str] = None
    connection_type_id: Optional[int] = None
    content_type: Optional[ContentType] = None
    name: Optional[str] = None

class DataConnectionResponse(DataConnectionBase):
    id: int
    organization_id: int
    content_type: ContentType
    status: str
    sync_status: str
    last_sync_time: Optional[datetime] = None
    next_sync_time: Optional[datetime] = None
    
    model_config = {
        "from_attributes": True
    }

class ConnectionTestResult(BaseModel):
    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None