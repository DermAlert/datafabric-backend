"""
Pydantic Schemas for Table Relationships API

These schemas define the API contracts for managing permanent relationships
between tables in the metadata layer.
"""

from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


# ==================== ENUMS ====================

class RelationshipScopeEnum(str, Enum):
    """Scope of the relationship"""
    INTRA_CONNECTION = "intra_connection"
    INTER_CONNECTION = "inter_connection"


class RelationshipSourceEnum(str, Enum):
    """How the relationship was discovered/created"""
    AUTO_FK = "auto_fk"
    AUTO_NAME = "auto_name"
    AUTO_DATA = "auto_data"
    MANUAL = "manual"
    SUGGESTED = "suggested"


class RelationshipCardinalityEnum(str, Enum):
    """Cardinality of the relationship"""
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


class JoinTypeEnum(str, Enum):
    """Default join type for the relationship"""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"


class SuggestionStatusEnum(str, Enum):
    """Status of a relationship suggestion"""
    PENDING = "pending"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


# ==================== CREATE SCHEMAS ====================

class TableRelationshipCreate(BaseModel):
    """Schema for creating a new table relationship"""
    
    # Left side (parent/one side)
    left_table_id: int = Field(..., description="ID of the left table")
    left_column_id: int = Field(..., description="ID of the join column in the left table")
    
    # Right side (child/many side)
    right_table_id: int = Field(..., description="ID of the right table")
    right_column_id: int = Field(..., description="ID of the join column in the right table")
    
    # Optional metadata
    name: Optional[str] = Field(None, max_length=255, description="Friendly name for the relationship")
    description: Optional[str] = Field(None, description="Description of the relationship")
    
    # Classification
    cardinality: Optional[RelationshipCardinalityEnum] = Field(None, description="Cardinality of the relationship")
    default_join_type: JoinTypeEnum = Field(JoinTypeEnum.FULL, description="Default join type (FULL keeps all data from both sides)")
    
    # Additional properties
    properties: Dict[str, Any] = Field(default_factory=dict)


class TableRelationshipUpdate(BaseModel):
    """Schema for updating a table relationship"""
    name: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = None
    cardinality: Optional[RelationshipCardinalityEnum] = None
    default_join_type: Optional[JoinTypeEnum] = None
    is_verified: Optional[bool] = None
    is_active: Optional[bool] = None
    properties: Optional[Dict[str, Any]] = None


class SemanticColumnLinkCreate(BaseModel):
    """Schema for creating a semantic link between columns"""
    column_id: int = Field(..., description="ID of the first column")
    linked_column_id: int = Field(..., description="ID of the second column")
    semantic_type: Optional[str] = Field(None, max_length=100, description="Semantic type (e.g., 'patient_id')")
    notes: Optional[str] = None


# ==================== RESPONSE SCHEMAS ====================

class ColumnInfo(BaseModel):
    """Column information for relationship responses"""
    column_id: int
    column_name: str
    data_type: str
    table_id: int
    table_name: str
    schema_name: str
    connection_id: int
    connection_name: str


class TableRelationshipResponse(BaseModel):
    """Response schema for a table relationship"""
    id: int
    
    # Left side
    left_column: ColumnInfo
    
    # Right side
    right_column: ColumnInfo
    
    # Metadata
    name: Optional[str]
    description: Optional[str]
    scope: RelationshipScopeEnum
    source: RelationshipSourceEnum
    cardinality: Optional[RelationshipCardinalityEnum]
    default_join_type: JoinTypeEnum
    
    # Status
    confidence_score: Optional[float]
    is_verified: bool
    is_active: bool
    
    # Timestamps
    data_criacao: datetime
    data_atualizacao: datetime


class RelationshipSuggestionResponse(BaseModel):
    """Response schema for a relationship suggestion"""
    id: int
    
    # Columns involved
    left_column: ColumnInfo
    right_column: ColumnInfo
    
    # Suggestion details
    suggestion_reason: str
    confidence_score: float
    status: SuggestionStatusEnum
    details: Dict[str, Any]
    
    # Timestamps
    generated_at: datetime


class SemanticColumnLinkResponse(BaseModel):
    """Response schema for a semantic column link"""
    id: int
    column: ColumnInfo
    linked_column: ColumnInfo
    semantic_type: Optional[str]
    source: RelationshipSourceEnum
    confidence_score: Optional[float]
    is_verified: bool
    is_active: bool
    notes: Optional[str]


# ==================== DISCOVERY SCHEMAS ====================

class DiscoverRelationshipsRequest(BaseModel):
    """Request to discover relationships from database FK constraints"""
    
    # Scope of discovery
    connection_ids: Optional[List[int]] = Field(
        None, 
        description="Limit discovery to these connections. If None, discovers across all connections."
    )
    table_ids: Optional[List[int]] = Field(
        None,
        description="Limit discovery to these tables. If None, discovers across all tables in scope."
    )
    
    # Options
    auto_accept: bool = Field(False, description="Automatically create relationships (if false, creates as suggestions for review)")


class DiscoverRelationshipsResponse(BaseModel):
    """Response from relationship discovery"""
    
    # Summary
    tables_analyzed: int
    columns_analyzed: int
    
    # Results
    relationships_found: int
    relationships_created: int  # Auto-accepted (e.g., FK-based)
    suggestions_created: int    # Needs user confirmation
    
    # Details
    new_relationships: List[TableRelationshipResponse]
    new_suggestions: List[RelationshipSuggestionResponse]
    
    # Warnings/info
    warnings: List[str]


class AcceptSuggestionRequest(BaseModel):
    """Request to accept a relationship suggestion"""
    suggestion_id: int
    
    # Optional overrides
    cardinality: Optional[RelationshipCardinalityEnum] = None
    default_join_type: Optional[JoinTypeEnum] = Field(
        None, 
        description="Join type for the relationship. Defaults to FULL if not provided."
    )
    name: Optional[str] = None
    description: Optional[str] = None


class BulkAcceptSuggestionsRequest(BaseModel):
    """Request to accept multiple suggestions at once"""
    suggestion_ids: List[int] = Field(..., min_items=1, examples=[[1, 2, 3]])
    default_join_type: Optional[JoinTypeEnum] = Field(
        None, 
        description="Apply this join type to all accepted relationships. Defaults to FULL if not provided.",
        examples=["full"]
    )
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "suggestion_ids": [1, 2, 3],
                    "default_join_type": "full"
                }
            ]
        }
    }


# ==================== SEARCH/LIST SCHEMAS ====================

class SearchRelationships(BaseModel):
    """Search parameters for relationships"""
    page: int = Field(default=1, ge=1)
    size: int = Field(default=20, ge=1, le=100)
    
    # Filters
    connection_id: Optional[int] = None
    table_id: Optional[int] = None
    scope: Optional[RelationshipScopeEnum] = None
    source: Optional[RelationshipSourceEnum] = None
    is_verified: Optional[bool] = None
    is_active: Optional[bool] = None
    
    # Search
    search: Optional[str] = Field(None, description="Search in name, description, table names")


class SearchSuggestions(BaseModel):
    """Search parameters for relationship suggestions"""
    page: int = Field(default=1, ge=1)
    size: int = Field(default=20, ge=1, le=100)
    
    # Filters
    connection_id: Optional[int] = None
    table_id: Optional[int] = None
    status: Optional[SuggestionStatusEnum] = None
    min_confidence: Optional[float] = Field(None, ge=0.0, le=1.0)
    suggestion_reason: Optional[str] = None


# ==================== GRAPH/VISUALIZATION SCHEMAS ====================

class RelationshipGraphNode(BaseModel):
    """Node in the relationship graph (represents a table)"""
    id: str  # "table_{table_id}"
    table_id: int
    table_name: str
    schema_name: str
    connection_id: int
    connection_name: str
    column_count: int


class RelationshipGraphEdge(BaseModel):
    """Edge in the relationship graph (represents a relationship)"""
    id: str  # "rel_{relationship_id}"
    source: str  # Node ID
    target: str  # Node ID
    relationship_id: int
    left_column: str
    right_column: str
    scope: RelationshipScopeEnum
    cardinality: Optional[RelationshipCardinalityEnum]
    is_verified: bool


class RelationshipGraph(BaseModel):
    """Graph representation of relationships for visualization"""
    nodes: List[RelationshipGraphNode]
    edges: List[RelationshipGraphEdge]
    
    # Metadata
    total_tables: int
    total_relationships: int
    connections_involved: List[Dict[str, Any]]

