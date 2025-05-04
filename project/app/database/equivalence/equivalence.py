from sqlalchemy import Column, Integer, String, ForeignKey, JSON, TIMESTAMP, UniqueConstraint, Boolean, Numeric, ARRAY, Text
from sqlalchemy.sql import func
from ..database import Base

class SemanticDomain(Base):
    __tablename__ = "semantic_domains"
    __table_args__ = {'schema': 'equivalence'}

    id = Column(Integer, primary_key=True)  # Replaced domain_id UUID
    name = Column(String(100), nullable=False, unique=True)
    description = Column(Text) # Using Text for potentially longer descriptions
    parent_domain_id = Column(Integer, ForeignKey('equivalence.semantic_domains.id'))
    domain_rules = Column(JSON, default={}) # Changed from JSONB to JSON for broader compatibility, use JSONB if needed


class DataDictionary(Base):
    __tablename__ = "data_dictionary"
    __table_args__ = {'schema': 'equivalence'}

    id = Column(Integer, primary_key=True)  # Replaced term_id UUID
    name = Column(String(100), nullable=False, unique=True)
    display_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=False) # Using Text
    semantic_domain_id = Column(Integer, ForeignKey('equivalence.semantic_domains.id'))
    data_type = Column(String(50), nullable=False)
    validation_rules = Column(JSON, default={}) # Changed from JSONB
    example_values = Column(JSON, default={}) # Changed from JSONB
    synonyms = Column(ARRAY(String))

class ColumnGroup(Base):
    __tablename__ = "column_groups"
    __table_args__ = {'schema': 'equivalence'}

    id = Column(Integer, primary_key=True)  # Replaced group_id UUID
    name = Column(String(255), nullable=False)
    description = Column(Text) # Using Text
    semantic_domain_id = Column(Integer, ForeignKey('equivalence.semantic_domains.id'))
    data_dictionary_term_id = Column(Integer, ForeignKey('equivalence.data_dictionary.id'))
    properties = Column(JSON, default={}) # Changed from JSONB


class ColumnMapping(Base):
    __tablename__ = "column_mappings"
    __table_args__ = (
        UniqueConstraint('group_id', 'column_id'),
        {'schema': 'equivalence'}
    )

    id = Column(Integer, primary_key=True)  # Replaced mapping_id UUID
    group_id = Column(Integer, ForeignKey('equivalence.column_groups.id', ondelete='CASCADE'), nullable=False)
    column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=False)
    transformation_rule = Column(Text) # Using Text
    confidence_score = Column(Numeric(5, 4), default=1.0)
    notes = Column(Text) # Using Text

    # Validate vai virar uma coluna base como o baseMixin.py
    # validated_by_id = Column("validated_by", Integer, ForeignKey('security.users.id')) # Renamed field slightly for clarity
    # validated_at = Column(TIMESTAMP(timezone=True))

class ValueMapping(Base):
    __tablename__ = "value_mappings"
    __table_args__ = (
        UniqueConstraint('group_id', 'source_column_id', 'source_value'),
        {'schema': 'equivalence'}
    )

    id = Column(Integer, primary_key=True)  # Replaced value_mapping_id UUID
    group_id = Column(Integer, ForeignKey('equivalence.column_groups.id', ondelete='CASCADE'), nullable=False)
    source_column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=False)
    source_value = Column(Text, nullable=False) # Using Text
    standard_value = Column(Text, nullable=False) # Using Text
    description = Column(Text) # Using Text