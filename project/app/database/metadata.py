from sqlalchemy import Column, Integer, String, ForeignKey, JSON, TIMESTAMP, UniqueConstraint
from sqlalchemy.sql import func
from .database import Base

class EntityxternalTables (Base):
    __tablename__ = "external_tables "
    __table_args__ = (
        {'schema': 'metadata'},
        UniqueConstraint('schema_id', 'table_name')
    )
    id = Column(Integer, primary_key=True)
    schema_id = Column(Integer, ForeignKey('metadata.external_schemas.schema_id', ondelete='CASCADE'), nullable=False)
    connection_id = Column(Integer, ForeignKey('core.data_connections.connection_id', ondelete='CASCADE'), nullable=False)
    table_name = Column(String(255), nullable=False)
    external_reference = Column(String(255), nullable=True)
    table_type = Column(String(50), nullable=False)
    estimated_row_count = Column(Integer, nullable=True)
    total_size_bytes = Column(Integer, nullable=True)
    last_analyzed = Column(TIMESTAMP(timezone=True), nullable=True)
    properties = Column(JSON, nullable=False, default={})
    description = Column(String, nullable=True)
