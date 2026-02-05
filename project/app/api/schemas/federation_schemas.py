"""
Pydantic Schemas for Federation API

Federations are logical groupings of data connections and tables
for organizing cross-database relationships.
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


# ==================== CONNECTION INFO ====================

class FederationConnectionInfo(BaseModel):
    """Connection information within a federation"""
    id: int
    name: str
    type: str = Field(..., description="Connection type (postgresql, mysql, mongodb, etc.)")
    color: Optional[str] = Field(None, description="Hex color from connection type")
    icon: Optional[str] = Field(None, description="Icon name from connection type")
    
    class Config:
        from_attributes = True


class FederationTableInfo(BaseModel):
    """Table information within a federation"""
    id: int
    table_name: str
    schema_name: str
    connection_id: int
    connection_name: str
    
    class Config:
        from_attributes = True


# ==================== CREATE/UPDATE SCHEMAS ====================

class FederationCreate(BaseModel):
    """Schema for creating a new federation"""
    name: str = Field(..., max_length=255, description="Name of the federation")
    description: Optional[str] = Field(None, description="Description of the federation")


class FederationUpdate(BaseModel):
    """Schema for updating a federation"""
    name: Optional[str] = Field(None, max_length=255, description="Name of the federation")
    description: Optional[str] = Field(None, description="Description of the federation")


class FederationAddConnections(BaseModel):
    """Schema for adding connections to a federation"""
    connection_ids: List[int] = Field(..., min_length=1, description="List of connection IDs to add")


class FederationAddTables(BaseModel):
    """Schema for adding specific tables to a federation"""
    table_ids: List[int] = Field(..., min_length=1, description="List of table IDs to add")


# ==================== RESPONSE SCHEMAS ====================

class FederationListItem(BaseModel):
    """Federation item for list responses"""
    id: int
    name: str
    description: Optional[str]
    connections: List[FederationConnectionInfo]
    tables_count: int
    relationships_count: int
    data_criacao: Optional[datetime] = None
    data_atualizacao: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class FederationResponse(BaseModel):
    """Full federation response with details"""
    id: int
    name: str
    description: Optional[str]
    connections: List[FederationConnectionInfo]
    tables: List[FederationTableInfo]
    tables_count: int
    relationships_count: int
    data_criacao: Optional[datetime] = None
    data_atualizacao: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class FederationConnectionsResponse(BaseModel):
    """Response after modifying connections"""
    id: int
    name: str
    connections: List[FederationConnectionInfo]
    tables_count: int
    
    class Config:
        from_attributes = True


class FederationTablesResponse(BaseModel):
    """Response after modifying tables"""
    id: int
    name: str
    tables: List[FederationTableInfo]
    tables_count: int
    
    class Config:
        from_attributes = True
