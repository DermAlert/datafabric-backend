from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field
from datetime import datetime

class DataConnectionBase(BaseModel):
    name: str
    description: Optional[str] = None
    connection_type_id: int
    connection_params: Dict[str, Any]
    cron_expression: Optional[str] = None
    sync_settings: Optional[Dict[str, Any]] = None

class DataConnectionCreate(DataConnectionBase):
    organization_id: int

class DataConnectionUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    connection_params: Optional[Dict[str, Any]] = None
    status: Optional[str] = None
    cron_expression: Optional[str] = None
    sync_settings: Optional[Dict[str, Any]] = None

class DataConnectionResponse(DataConnectionBase):
    id: int
    organization_id: int
    status: str
    sync_status: str
    last_sync_time: Optional[datetime] = None
    next_sync_time: Optional[datetime] = None
    
    class Config:
        orm_mode = True

class ConnectionTestResult(BaseModel):
    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None