from sqlalchemy import Column, Integer, String, ForeignKey, JSON, TIMESTAMP, UniqueConstraint, Boolean, Numeric, ARRAY, Text
from sqlalchemy.sql import func
from ..database import Base
from ..baseMixin import AuditMixin


class SemanticDomain(AuditMixin, Base):
    """
    Domínios semânticos para categorização de termos.
    Ex: Demographics, Operations, Location, Contact, Financial
    """
    __tablename__ = "semantic_domains"
    __table_args__ = {'schema': 'equivalence'}

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    description = Column(Text)
    parent_domain_id = Column(Integer, ForeignKey('equivalence.semantic_domains.id'))
    color = Column(String(7))  # Hex color como "#3b82f6"
    domain_rules = Column(JSON, default={})


class DataDictionary(AuditMixin, Base):
    """
    Dicionário de dados com termos padronizados.
    Ex: Sex/Gender, Date of Birth, Country, Email, Amount
    """
    __tablename__ = "data_dictionary"
    __table_args__ = {'schema': 'equivalence'}

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    display_name = Column(String(255), nullable=False)
    description = Column(Text, nullable=False)
    semantic_domain_id = Column(Integer, ForeignKey('equivalence.semantic_domains.id'))
    data_type = Column(String(50), nullable=False)  # STRING, INTEGER, DATE, ENUM, DECIMAL, etc.
    standard_values = Column(ARRAY(String))  # Valores padrão permitidos: ["M", "F", "O"]
    validation_rules = Column(JSON, default={})
    example_values = Column(JSON, default={})
    synonyms = Column(ARRAY(String))  # Sinônimos: ["gender", "sexo", "genero", "sex"]


class ColumnGroup(AuditMixin, Base):
    """
    Grupos de colunas equivalentes de diferentes fontes.
    Ex: sex_unified agrupa patients.sex, users.gender, pacientes.sexo_paciente
    """
    __tablename__ = "column_groups"
    __table_args__ = {'schema': 'equivalence'}

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    semantic_domain_id = Column(Integer, ForeignKey('equivalence.semantic_domains.id'))
    data_dictionary_term_id = Column(Integer, ForeignKey('equivalence.data_dictionary.id'))
    properties = Column(JSON, default={})


class ColumnMapping(AuditMixin, Base):
    """
    Mapeamento de colunas externas para grupos de equivalência.
    Vincula uma coluna específica (ex: patients.sex) a um grupo (ex: sex_unified)
    """
    __tablename__ = "column_mappings"
    __table_args__ = (
        UniqueConstraint('group_id', 'column_id'),
        {'schema': 'equivalence'}
    )

    id = Column(Integer, primary_key=True)
    group_id = Column(Integer, ForeignKey('equivalence.column_groups.id', ondelete='CASCADE'), nullable=False)
    column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=False)
    transformation_rule = Column(Text)  # Regra de transformação opcional
    confidence_score = Column(Numeric(5, 4), default=1.0)
    notes = Column(Text)


class ValueMapping(AuditMixin, Base):
    """
    Mapeamento de valores de origem para valores padronizados.
    Ex: "male" -> "M", "female" -> "F", "masculino" -> "M"
    """
    __tablename__ = "value_mappings"
    __table_args__ = (
        UniqueConstraint('group_id', 'source_column_id', 'source_value'),
        {'schema': 'equivalence'}
    )

    id = Column(Integer, primary_key=True)
    group_id = Column(Integer, ForeignKey('equivalence.column_groups.id', ondelete='CASCADE'), nullable=False)
    source_column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=False)
    source_value = Column(Text, nullable=False)  # Valor original: "male", "masculino"
    standard_value = Column(Text, nullable=False)  # Valor padronizado: "M"
    description = Column(Text)
    record_count = Column(Integer, default=0)  # Contagem de registros com esse valor
