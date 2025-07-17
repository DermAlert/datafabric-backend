from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from datetime import datetime

class UpdateImagePathRequest(BaseModel):
    """Request schema for updating a column's is_image_path status"""
    is_image_path: bool
    image_connection_id: Optional[int] = None
    
    model_config = {
        "from_attributes": True
    }

class BulkUpdateImagePathRequest(BaseModel):
    """Request schema for bulk updating multiple columns' is_image_path status"""
    column_ids: List[int]
    is_image_path: bool
    image_connection_id: Optional[int] = None
    
    model_config = {
        "from_attributes": True
    }

class ImagePathColumnResponse(BaseModel):
    """Response schema for image path column information"""
    id: int
    table_id: int
    column_name: str
    data_type: str
    is_image_path: bool
    image_connection_id: Optional[int] = None
    image_connection_name: Optional[str] = None
    table_name: Optional[str] = None
    schema_name: Optional[str] = None
    
    model_config = {
        "from_attributes": True
    }

class ImageConnectionResponse(BaseModel):
    """Response schema for image connections"""
    id: int
    name: str
    description: Optional[str] = None
    status: str
    connection_params: Dict[str, Any]
    
    model_config = {
        "from_attributes": True
    }

class UpdateImagePathResponse(BaseModel):
    """Response schema for updating image path"""
    success: bool
    message: str
    column_id: int
    is_image_path: bool
    image_connection_id: Optional[int] = None
    
    model_config = {
        "from_attributes": True
    }

class BulkUpdateImagePathResponse(BaseModel):
    """Response schema for bulk updating image paths"""
    success: bool
    message: str
    updated_count: int
    failed_count: int
    failed_column_ids: List[int] = []
    
    model_config = {
        "from_attributes": True
    }
