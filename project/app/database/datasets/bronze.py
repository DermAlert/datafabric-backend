"""
Bronze Layer Models for Smart Bronze Architecture

This module defines the database models for the Bronze layer of the Medallion architecture.
The Bronze layer focuses on raw data ingestion without transformations.

Key concepts:
- SourceRelationship: Defines joins between tables (intra-DB and inter-DB)
- DatasetIngestionGroup: Groups tables by connection for optimized ingestion
- DatasetBronzeConfig: Stores the Bronze layer configuration for a dataset
"""

from sqlalchemy import Column, Integer, String, ForeignKey, JSON, Boolean, Text, Enum as SQLEnum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy import UniqueConstraint, TIMESTAMP
from ..database import Base
from ..baseMixin import AuditMixin
import enum


class RelationshipType(enum.Enum):
    """Type of relationship between tables"""
    INTRA_DB = "intra_db"      # Same database (can be pushed down)
    INTER_DB = "inter_db"      # Different databases (federated - for Silver layer)


class JoinStrategy(enum.Enum):
    """Join strategy for the relationship"""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"


class IngestionStatus(enum.Enum):
    """Status of an ingestion group execution"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


class SourceRelationship(AuditMixin, Base):
    """
    Defines a relationship (join) between two tables.
    
    This can be:
    - Intra-DB: Both tables in the same connection (join pushed to source DB)
    - Inter-DB: Tables in different connections (federated join - used in Silver layer)
    
    The relationship is stored as metadata and used to:
    1. Generate optimized SQL for Bronze ingestion (intra-DB joins)
    2. Define the join logic for Silver layer unification (inter-DB joins)
    """
    __tablename__ = "source_relationships"
    __table_args__ = (
        UniqueConstraint('left_column_id', 'right_column_id'),
        {'schema': 'datasets'}
    )

    id = Column(Integer, primary_key=True)
    
    # Left side of the join
    left_table_id = Column(Integer, ForeignKey('metadata.external_tables.id', ondelete='CASCADE'), nullable=False)
    left_column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=False)
    
    # Right side of the join
    right_table_id = Column(Integer, ForeignKey('metadata.external_tables.id', ondelete='CASCADE'), nullable=False)
    right_column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=False)
    
    # Relationship metadata
    relationship_type = Column(SQLEnum(RelationshipType), nullable=False)
    join_strategy = Column(SQLEnum(JoinStrategy), nullable=False, default=JoinStrategy.INNER)
    
    # Optional: custom join condition (for complex joins beyond simple equality)
    custom_condition = Column(Text, nullable=True)
    
    # Confidence/validation
    is_verified = Column(Boolean, default=False)
    notes = Column(Text, nullable=True)


class DatasetBronzeConfig(AuditMixin, Base):
    """
    Stores the Bronze layer configuration for a dataset.
    
    This is the "definition" of how the dataset should be ingested,
    including which tables, columns, and relationships are involved.
    """
    __tablename__ = "dataset_bronze_configs"
    __table_args__ = {'schema': 'datasets'}

    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey('core.datasets.id', ondelete='CASCADE'), nullable=False, unique=True)
    
    # Configuration
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    
    # Storage location in MinIO
    bronze_bucket = Column(String(255), nullable=False)
    bronze_path_prefix = Column(String(500), nullable=False)  # e.g., "bronze/dataset_100/"
    
    # Output format
    output_format = Column(String(50), nullable=False, default='parquet')  # parquet, delta
    
    # Partitioning strategy (optional)
    partition_columns = Column(JSON, nullable=True)  # e.g., ["year", "month"]
    
    # Full configuration snapshot (for reproducibility)
    config_snapshot = Column(JSON, nullable=False, default={})
    
    # Status
    last_ingestion_time = Column(TIMESTAMP(timezone=True), nullable=True)
    last_ingestion_status = Column(SQLEnum(IngestionStatus), nullable=True)
    
    # Relationships
    ingestion_groups = relationship("DatasetIngestionGroup", back_populates="bronze_config", cascade="all, delete-orphan")


class DatasetIngestionGroup(AuditMixin, Base):
    """
    Groups tables by connection for optimized ingestion.
    
    Each group represents a single "extraction unit" that will be processed
    by Trino in one query. Tables within the same group (same connection)
    can have their joins pushed down to the source database.
    """
    __tablename__ = "dataset_ingestion_groups"
    __table_args__ = {'schema': 'datasets'}

    id = Column(Integer, primary_key=True)
    bronze_config_id = Column(Integer, ForeignKey('datasets.dataset_bronze_configs.id', ondelete='CASCADE'), nullable=False)
    
    # Connection this group belongs to
    connection_id = Column(Integer, ForeignKey('core.data_connections.id', ondelete='CASCADE'), nullable=False)
    
    # Group identifier within the dataset
    group_name = Column(String(255), nullable=False)  # e.g., "part_postgres_clinic"
    group_order = Column(Integer, nullable=False, default=0)  # Execution order
    
    # Output path for this group
    output_path = Column(String(500), nullable=False)  # e.g., "bronze/dataset_100/part_01_postgres/"
    
    # The generated SQL for this group (stored for debugging/audit)
    generated_sql = Column(Text, nullable=True)
    
    # Execution status
    status = Column(SQLEnum(IngestionStatus), nullable=False, default=IngestionStatus.PENDING)
    last_execution_time = Column(TIMESTAMP(timezone=True), nullable=True)
    rows_ingested = Column(Integer, nullable=True)
    error_message = Column(Text, nullable=True)
    
    # Relationships
    bronze_config = relationship("DatasetBronzeConfig", back_populates="ingestion_groups")
    selected_tables = relationship("IngestionGroupTable", back_populates="ingestion_group", cascade="all, delete-orphan")


class IngestionGroupTable(Base):
    """
    Links tables to their ingestion group with specific column selection.
    """
    __tablename__ = "ingestion_group_tables"
    __table_args__ = (
        UniqueConstraint('ingestion_group_id', 'table_id'),
        {'schema': 'datasets'}
    )

    id = Column(Integer, primary_key=True)
    ingestion_group_id = Column(Integer, ForeignKey('datasets.dataset_ingestion_groups.id', ondelete='CASCADE'), nullable=False)
    table_id = Column(Integer, ForeignKey('metadata.external_tables.id', ondelete='CASCADE'), nullable=False)
    
    # Table alias for SQL generation
    table_alias = Column(String(50), nullable=False)
    
    # Column selection mode
    select_all_columns = Column(Boolean, default=False)
    
    # If select_all_columns is False, this stores the specific column IDs
    selected_column_ids = Column(JSON, nullable=True)  # List of column IDs
    
    # Join info (if this table is joined to another in the group)
    is_primary_table = Column(Boolean, default=False)  # The "driving" table of the group
    join_to_table_id = Column(Integer, ForeignKey('metadata.external_tables.id'), nullable=True)
    join_type = Column(SQLEnum(JoinStrategy), nullable=True)
    join_condition = Column(Text, nullable=True)  # e.g., "t1.id = t2.user_id"
    
    # Relationships
    ingestion_group = relationship("DatasetIngestionGroup", back_populates="selected_tables")


class InterSourceLink(AuditMixin, Base):
    """
    Defines the logical link between ingestion groups from different sources.
    
    This is metadata-only in Bronze layer. The actual join happens in Silver.
    It tells the Silver layer how to unify the parts.
    """
    __tablename__ = "inter_source_links"
    __table_args__ = (
        UniqueConstraint('bronze_config_id', 'left_group_id', 'right_group_id'),
        {'schema': 'datasets'}
    )

    id = Column(Integer, primary_key=True)
    bronze_config_id = Column(Integer, ForeignKey('datasets.dataset_bronze_configs.id', ondelete='CASCADE'), nullable=False)
    
    # Left group (source)
    left_group_id = Column(Integer, ForeignKey('datasets.dataset_ingestion_groups.id', ondelete='CASCADE'), nullable=False)
    left_column_name = Column(String(255), nullable=False)  # Column name in the output
    
    # Right group (target)
    right_group_id = Column(Integer, ForeignKey('datasets.dataset_ingestion_groups.id', ondelete='CASCADE'), nullable=False)
    right_column_name = Column(String(255), nullable=False)  # Column name in the output
    
    # Join strategy for Silver layer
    join_strategy = Column(SQLEnum(JoinStrategy), nullable=False, default=JoinStrategy.INNER)
    
    # Notes
    description = Column(Text, nullable=True)


class BronzeColumnMapping(Base):
    """
    Maps external columns to their Bronze output names.
    
    This table persists the mapping between:
    - external_column.id (source metadata)
    - bronze_column_name (actual column name in Delta Lake)
    
    This enables Silver layer to:
    1. Use ColumnGroup from Equivalence (which references external_column.id)
    2. Resolve to actual Bronze column names for transformations
    
    Example:
    - external_column_id: 100 (cpf in pacientes table)
    - bronze_column_name: "cpf" (if unique) or "pacientes_cpf" (if duplicated)
    """
    __tablename__ = "bronze_column_mappings"
    __table_args__ = (
        UniqueConstraint('ingestion_group_id', 'external_column_id'),
        {'schema': 'datasets'}
    )

    id = Column(Integer, primary_key=True)
    
    # Which ingestion group this mapping belongs to
    ingestion_group_id = Column(Integer, ForeignKey('datasets.dataset_ingestion_groups.id', ondelete='CASCADE'), nullable=False)
    
    # Source: external column metadata
    external_column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=False)
    external_table_id = Column(Integer, ForeignKey('metadata.external_tables.id', ondelete='CASCADE'), nullable=False)
    
    # Original names (for reference)
    original_column_name = Column(String(255), nullable=False)
    original_table_name = Column(String(255), nullable=False)
    original_schema_name = Column(String(255), nullable=True)
    
    # Target: Bronze Delta Lake column name
    bronze_column_name = Column(String(255), nullable=False)
    
    # Data type info
    data_type = Column(String(100), nullable=True)
    
    # Position in output
    column_position = Column(Integer, nullable=True)
    
    # Flags
    is_prefixed = Column(Boolean, default=False)  # True if column name was prefixed with table name
    
    # Relationship
    ingestion_group = relationship("DatasetIngestionGroup")



