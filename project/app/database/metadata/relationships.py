"""
Table Relationships Metadata Models

This module defines the database models for storing permanent relationships
between tables across data connections. These relationships are:

1. **Auto-discovered**: System detects FK constraints, naming patterns, etc.
2. **Manually defined**: User explicitly creates relationships
3. **Suggested**: System suggests based on heuristics, user confirms

These relationships are stored at the metadata level (not dataset level)
and can be reused across multiple datasets.
"""

from sqlalchemy import Column, Integer, String, ForeignKey, JSON, Boolean, Text, Enum as SQLEnum, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy import UniqueConstraint, TIMESTAMP
from ..database import Base
from ..baseMixin import AuditMixin
import enum


class RelationshipScope(enum.Enum):
    """Scope of the relationship"""
    INTRA_CONNECTION = "intra_connection"  # Same connection (same database)
    INTER_CONNECTION = "inter_connection"  # Different connections (different databases)


class RelationshipSource(enum.Enum):
    """How the relationship was discovered/created"""
    AUTO_FK = "auto_fk"              # Discovered from foreign key constraint
    AUTO_NAME = "auto_name"          # Discovered from column name similarity
    AUTO_DATA = "auto_data"          # Discovered from data analysis (value matching)
    MANUAL = "manual"                # Manually defined by user
    SUGGESTED = "suggested"          # System suggested, not yet confirmed


class RelationshipCardinality(enum.Enum):
    """Cardinality of the relationship"""
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


class JoinType(enum.Enum):
    """Default join type for the relationship"""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"


class TableRelationship(AuditMixin, Base):
    """
    Permanent relationship definition between two tables.
    
    This is stored at the metadata level and can be reused across datasets.
    The relationship can be:
    - Within the same connection (intra-connection): Can be pushed down to source DB
    - Across different connections (inter-connection): Requires federated processing
    
    Relationships can be auto-discovered or manually defined.
    """
    __tablename__ = "table_relationships"
    __table_args__ = (
        UniqueConstraint('left_column_id', 'right_column_id', name='uq_table_relationship'),
        {'schema': 'metadata'}
    )

    id = Column(Integer, primary_key=True)
    
    # Left side of the relationship (typically the "parent" or "one" side)
    left_table_id = Column(Integer, ForeignKey('metadata.external_tables.id', ondelete='CASCADE'), nullable=False)
    left_column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=False)
    
    # Right side of the relationship (typically the "child" or "many" side)
    right_table_id = Column(Integer, ForeignKey('metadata.external_tables.id', ondelete='CASCADE'), nullable=False)
    right_column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=False)
    
    # Relationship metadata
    name = Column(String(255), nullable=True)  # Optional friendly name
    description = Column(Text, nullable=True)
    
    # Classification
    scope = Column(SQLEnum(RelationshipScope), nullable=False)
    source = Column(SQLEnum(RelationshipSource), nullable=False, default=RelationshipSource.MANUAL)
    cardinality = Column(SQLEnum(RelationshipCardinality), nullable=True)
    default_join_type = Column(SQLEnum(JoinType), nullable=False, default=JoinType.FULL)
    
    # Confidence and validation
    confidence_score = Column(Float, nullable=True)  # 0.0 to 1.0 for auto-discovered
    is_verified = Column(Boolean, default=False)  # User has confirmed this relationship
    is_active = Column(Boolean, default=True)  # Can be deactivated without deleting
    
    # Additional metadata
    properties = Column(JSON, nullable=False, default={})
    
    # Discovery metadata (for auto-discovered relationships)
    discovery_details = Column(JSON, nullable=True)  # Details about how it was discovered


class RelationshipSuggestion(AuditMixin, Base):
    """
    Suggested relationships that haven't been confirmed yet.
    
    The system generates these based on various heuristics:
    - Column name similarity
    - Data type compatibility
    - Sample value matching
    - Naming conventions (e.g., user_id -> users.id)
    
    Users can accept (convert to TableRelationship) or reject these suggestions.
    """
    __tablename__ = "relationship_suggestions"
    __table_args__ = (
        UniqueConstraint('left_column_id', 'right_column_id', name='uq_relationship_suggestion'),
        {'schema': 'metadata'}
    )

    id = Column(Integer, primary_key=True)
    
    # Left side
    left_table_id = Column(Integer, ForeignKey('metadata.external_tables.id', ondelete='CASCADE'), nullable=False)
    left_column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=False)
    
    # Right side
    right_table_id = Column(Integer, ForeignKey('metadata.external_tables.id', ondelete='CASCADE'), nullable=False)
    right_column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=False)
    
    # Suggestion metadata
    suggestion_reason = Column(String(100), nullable=False)  # e.g., "name_similarity", "fk_pattern", "data_match"
    confidence_score = Column(Float, nullable=False)  # 0.0 to 1.0
    
    # Status
    status = Column(String(50), nullable=False, default='pending')  # pending, accepted, rejected
    
    # Details about the suggestion
    details = Column(JSON, nullable=False, default={})
    
    # When was this suggestion generated
    generated_at = Column(TIMESTAMP(timezone=True), server_default=func.now())


class SemanticColumnLink(AuditMixin, Base):
    """
    Links columns that represent the same semantic concept across tables.
    
    This is different from a join relationship - it indicates that two columns
    contain the same type of data (e.g., both are "patient_id" even if named differently).
    
    This is useful for:
    - Value mapping (standardizing values across sources)
    - Understanding data lineage
    - Suggesting relationships
    
    Note: This complements the existing ColumnGroup/ColumnMapping in equivalence schema,
    but is specifically for cross-table semantic linking.
    """
    __tablename__ = "semantic_column_links"
    __table_args__ = (
        UniqueConstraint('column_id', 'linked_column_id', name='uq_semantic_link'),
        {'schema': 'metadata'}
    )

    id = Column(Integer, primary_key=True)
    
    # The two columns being linked
    column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=False)
    linked_column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=False)
    
    # Semantic information
    semantic_type = Column(String(100), nullable=True)  # e.g., "patient_identifier", "diagnosis_code"
    
    # How was this link discovered
    source = Column(SQLEnum(RelationshipSource), nullable=False, default=RelationshipSource.MANUAL)
    confidence_score = Column(Float, nullable=True)
    
    # Status
    is_verified = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)
    
    # Notes
    notes = Column(Text, nullable=True)

