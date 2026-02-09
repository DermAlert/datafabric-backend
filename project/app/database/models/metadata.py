from sqlalchemy import Column, Integer, String, ForeignKey, JSON, TIMESTAMP, UniqueConstraint, Boolean, Numeric, Enum
from sqlalchemy.sql import func
from ..session import Base
from ..base import AuditMixin

class ExternalTables (AuditMixin, Base):
    __tablename__ = "external_tables"
    __table_args__ = (
        UniqueConstraint('schema_id', 'table_name'),
        {'schema': 'metadata'}
    )
    id = Column(Integer, primary_key=True)
    schema_id = Column(Integer, ForeignKey('metadata.external_schemas.id', ondelete='CASCADE'), nullable=False)
    connection_id = Column(Integer, ForeignKey('core.data_connections.id', ondelete='CASCADE'), nullable=False)
    table_name = Column(String(255), nullable=False)
    external_reference = Column(String(255), nullable=True)
    table_type = Column(String(50), nullable=False)
    estimated_row_count = Column(Integer, nullable=True)
    total_size_bytes = Column(Integer, nullable=True)
    last_analyzed = Column(TIMESTAMP(timezone=True), nullable=True)
    properties = Column(JSON, nullable=False, default={})
    description = Column(String, nullable=True)

class ExternalCatalogs(AuditMixin, Base):
    __tablename__ = "external_catalogs"
    __table_args__ = {'schema': 'metadata'}
    id = Column(Integer, primary_key=True)  # substitui catalog_id UUID
    connection_id = Column(Integer, ForeignKey('core.data_connections.id', ondelete='CASCADE'), nullable=False)
    catalog_name = Column(String(255), nullable=True)
    catalog_type = Column(String(50), nullable=True)
    external_reference = Column(String(255), nullable=True)  # external identifier in source system
    properties = Column(JSON, nullable=False, default={})

class ExternalSchema(AuditMixin, Base):
    __tablename__ = "external_schemas"
    __table_args__ = (
        UniqueConstraint('connection_id', 'schema_name'),
        {'schema': 'metadata'}
    )

    id = Column(Integer, primary_key=True)  # schema_id
    catalog_id = Column(Integer, ForeignKey('metadata.external_catalogs.id', ondelete='CASCADE'), nullable=True)
    connection_id = Column(Integer, ForeignKey('core.data_connections.id', ondelete='CASCADE'), nullable=False)
    schema_name = Column(String(255), nullable=False)
    external_reference = Column(String(255), nullable=True)
    properties = Column(JSON, nullable=False, default={})

class ExternalColumn(AuditMixin, Base):
    __tablename__ = "external_columns"
    __table_args__ = (
        UniqueConstraint('table_id', 'column_name'),
        {'schema': 'metadata'}
    )

    id = Column(Integer, primary_key=True)  # column_id
    table_id = Column(Integer, ForeignKey('metadata.external_tables.id', ondelete='CASCADE'), nullable=False)
    column_name = Column(String(255), nullable=False)
    external_reference = Column(String(255), nullable=True)
    data_type = Column(String(100), nullable=False)
    is_nullable = Column(Boolean, nullable=False, default=True)
    column_position = Column(Integer, nullable=False)
    max_length = Column(Integer, nullable=True)
    numeric_precision = Column(Integer, nullable=True)
    numeric_scale = Column(Integer, nullable=True)
    is_primary_key = Column(Boolean, nullable=False, default=False)
    is_unique = Column(Boolean, nullable=False, default=False)
    is_indexed = Column(Boolean, nullable=False, default=False)
    
    # Foreign Key metadata - extracted via Trino query passthrough
    is_foreign_key = Column(Boolean, nullable=False, default=False)
    fk_referenced_table_id = Column(Integer, ForeignKey('metadata.external_tables.id'), nullable=True)
    fk_referenced_column_id = Column(Integer, ForeignKey('metadata.external_columns.id'), nullable=True)
    fk_constraint_name = Column(String(255), nullable=True)
    
    default_value = Column(String, nullable=True)
    description = Column(String, nullable=True)
    is_image_path = Column(Boolean, nullable=False, default=False)
    image_connection_id = Column(Integer, ForeignKey('core.data_connections.id'), nullable=True)
    statistics = Column(JSON, nullable=False, default={})
    sample_values = Column(JSON, nullable=False, default=[])
    properties = Column(JSON, nullable=False, default={})

class ColumnTag(AuditMixin, Base):
    __tablename__ = "column_tags"
    __table_args__ = (
        UniqueConstraint('column_id', 'tag_name', 'tag_type'),
        {'schema': 'metadata'}
    )

    id = Column(Integer, primary_key=True)  # column_tag_id
    column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=False)
    tag_name = Column(String(100), nullable=False)
    tag_type = Column(String(50), nullable=False)  # semantic_type, data_domain, quality, etc.
    confidence = Column(Numeric(5, 4), nullable=True)  # optional
    # assigned_by = Column(Integer, ForeignKey('security.users.id'), nullable=True)  # NULL if auto-assigned  -> ainda n implementado


