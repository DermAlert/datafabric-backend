"""
Silver Layer Models for Data Transformation

This module defines the database models for the Silver layer of the Medallion architecture.
The Silver layer focuses on data transformation, normalization, and semantic unification.

Key concepts:
- NormalizationRule: Reusable rules for formatting data (CPF, phone, etc.)
- SemanticMapping: Maps columns from different sources to unified names
- SilverFilter: Reusable filter conditions
- VirtualizedConfig: Config for virtualized queries (sources → JSON)
- TransformConfig: Config for materialization (Bronze → Silver Delta)
"""

from sqlalchemy import Column, Integer, String, ForeignKey, JSON, Boolean, Text, Enum as SQLEnum, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy import UniqueConstraint, TIMESTAMP
from ..database import Base
from ..baseMixin import AuditMixin
import enum


# ==================== ENUMS ====================

class NormalizationType(enum.Enum):
    """Type of normalization to apply"""
    TEMPLATE = "template"          # Uses template like {d3}.{d3}.{d3}-{d2}
    SQL_REGEX = "sql_regex"        # Uses SQL regexp_replace
    CASE_MAPPING = "case_mapping"  # CASE WHEN value mappings
    PYTHON_RULE = "python_rule"    # Uses Python Normalizer (only for transform)


class FilterOperator(enum.Enum):
    """Operators for filter conditions"""
    EQ = "="
    NEQ = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    LIKE = "LIKE"
    ILIKE = "ILIKE"
    IN = "IN"
    NOT_IN = "NOT IN"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"
    BETWEEN = "BETWEEN"


class FilterLogic(enum.Enum):
    """Logic for combining filter conditions"""
    AND = "AND"
    OR = "OR"


class TransformStatus(enum.Enum):
    """Status of a transformation execution"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


# ==================== NORMALIZATION RULES ====================

class NormalizationRule(AuditMixin, Base):
    """
    Reusable normalization rule.
    
    Can be used with templates (like {d3}.{d3}.{d3}-{d2} for CPF)
    or with regex patterns for SQL-based transformations.
    """
    __tablename__ = "normalization_rules"
    __table_args__ = (
        UniqueConstraint('name'),
        {'schema': 'datasets'}
    )

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    
    # Template-based (for Python Normalizer)
    template = Column(String(500), nullable=True)  # e.g., "{d3}.{d3}.{d3}-{d2}"
    
    # Regex-based (for SQL)
    regex_pattern = Column(Text, nullable=True)     # Input pattern
    regex_replacement = Column(Text, nullable=True) # Output pattern
    
    # Pre-processing
    pre_process = Column(String(100), nullable=True)  # e.g., "digits_only|uppercase"
    
    # Generated fields (from template parsing)
    regex_entrada = Column(Text, nullable=True)      # Generated input regex
    template_saida = Column(Text, nullable=True)     # Generated output template
    case_transforms = Column(JSON, nullable=True)    # Case transformations per group
    
    # Metadata
    is_builtin = Column(Boolean, default=False)      # System-provided rule
    is_active = Column(Boolean, default=True)
    
    # Examples for documentation
    example_input = Column(String(255), nullable=True)
    example_output = Column(String(255), nullable=True)


# ==================== SEMANTIC MAPPINGS ====================
# NOTE: Semantic mappings are managed by the Equivalence module.
# Use equivalence.ColumnGroup and equivalence.ColumnMapping for semantic unification.
# The Silver layer references column_group_ids from Equivalence.


# ==================== FILTERS ====================

class SilverFilter(AuditMixin, Base):
    """
    Reusable filter definition.
    """
    __tablename__ = "silver_filters"
    __table_args__ = (
        UniqueConstraint('name'),
        {'schema': 'datasets'}
    )

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    logic = Column(SQLEnum(FilterLogic), default=FilterLogic.AND)  # How to combine conditions
    
    is_active = Column(Boolean, default=True)


class SilverFilterCondition(AuditMixin, Base):
    """
    A single condition in a filter.
    """
    __tablename__ = "silver_filter_conditions"
    __table_args__ = {'schema': 'datasets'}

    id = Column(Integer, primary_key=True)
    filter_id = Column(Integer, ForeignKey('datasets.silver_filters.id', ondelete='CASCADE'), nullable=False)
    
    # Column reference (can be column_id or column_name depending on config type)
    column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=True)
    column_name = Column(String(255), nullable=True)  # For Bronze-based filters
    
    operator = Column(SQLEnum(FilterOperator), nullable=False)
    value = Column(Text, nullable=True)  # JSON-encoded for complex values (arrays, etc.)
    
    # For BETWEEN operator
    value_min = Column(Text, nullable=True)
    value_max = Column(Text, nullable=True)
    
    # Order within filter
    condition_order = Column(Integer, default=0)


# ==================== VIRTUALIZED CONFIG ====================

class VirtualizedConfig(AuditMixin, Base):
    """
    Configuration for virtualized queries (sources → JSON).
    
    Uses original data sources via Trino, applies transformations via SQL.
    Does NOT materialize data.
    
    Semantic unification and value mappings are loaded automatically from
    the Equivalence module when column_group_ids is provided:
    - ColumnMappings: which columns to unify
    - ValueMappings: how to normalize values (male→Masculino, etc.)
    
    column_transformations is for text transformations (lowercase, trim, etc.)
    and normalization rules (template). For value mappings, use /api/equivalence.
    """
    __tablename__ = "virtualized_configs"
    __table_args__ = (
        UniqueConstraint('name'),
        {'schema': 'datasets'}
    )

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    
    # Tables and columns selection (same structure as Bronze)
    # [{"table_id": 1, "select_all": true}, {"table_id": 2, "column_ids": [10, 11]}]
    tables = Column(JSON, nullable=False)
    
    # Column groups from Equivalence module (for semantic unification + value mappings)
    # When provided, automatically loads ColumnMappings and ValueMappings from Equivalence
    column_group_ids = Column(JSON, nullable=True)  # List of equivalence.column_groups IDs
    
    # Filters to apply
    filter_ids = Column(JSON, nullable=True)  # List of silver_filter IDs
    
    # Column transformations: template, lowercase, uppercase, trim, normalize_spaces, remove_accents
    # [{column_id: 100, type: "template", rule_id: 1}]
    # [{column_id: 101, type: "lowercase"}]
    column_transformations = Column(JSON, nullable=True)
    
    is_active = Column(Boolean, default=True)
    
    # Cached SQL (for performance)
    generated_sql = Column(Text, nullable=True)
    sql_generated_at = Column(TIMESTAMP(timezone=True), nullable=True)


# ==================== TRANSFORM CONFIG ====================

class TransformConfig(AuditMixin, Base):
    """
    Configuration for materialization (Bronze → Silver Delta).
    
    Uses Bronze dataset as source, applies transformations (SQL + Python).
    Materializes to Silver Delta Lake.
    """
    __tablename__ = "transform_configs"
    __table_args__ = (
        UniqueConstraint('name'),
        {'schema': 'datasets'}
    )

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    
    # Source: Bronze dataset
    source_bronze_dataset_id = Column(Integer, ForeignKey('core.datasets.id', ondelete='CASCADE'), nullable=False)
    
    # Output configuration
    silver_bucket = Column(String(255), nullable=True)  # Defaults to system silver bucket
    silver_path_prefix = Column(String(500), nullable=True)
    
    # Column groups from Equivalence (for semantic unification)
    # Uses BronzeColumnMapping to resolve external_column.id → bronze_column_name
    column_group_ids = Column(JSON, nullable=True)  # List of equivalence.column_groups IDs
    
    # Semantic column unification (JSON array) - for direct column name mapping
    # [{unified_name: "patient_id", source_columns: ["cpf", "cpf_paciente"], normalization_rule_id: 1}]
    semantic_columns = Column(JSON, nullable=True)
    
    # Filters to apply
    filter_ids = Column(JSON, nullable=True)
    
    # Column-specific normalizations (JSON array)
    # [{column_name: "cpf", type: "rule", rule_id: 1}]
    # [{column_name: "tel", type: "template", template: "({d2}) {d5}-{d4}"}]
    column_normalizations = Column(JSON, nullable=True)
    
    # Value mappings (JSON array)
    # [{column_name: "sexo", output_column: "gender", mappings: {...}, default: "unknown"}]
    value_mappings = Column(JSON, nullable=True)
    
    # Image labeling config (optional)
    # {column_name: "image_path", model: "classifier", output_column: "label"}
    image_labeling_config = Column(JSON, nullable=True)
    
    # Execution status
    last_execution_time = Column(TIMESTAMP(timezone=True), nullable=True)
    last_execution_status = Column(SQLEnum(TransformStatus), nullable=True)
    last_execution_rows = Column(Integer, nullable=True)
    last_execution_error = Column(Text, nullable=True)
    
    is_active = Column(Boolean, default=True)
    
    # Full config snapshot (for reproducibility)
    config_snapshot = Column(JSON, nullable=True)


class TransformExecution(AuditMixin, Base):
    """
    Execution history for transform configs.
    """
    __tablename__ = "transform_executions"
    __table_args__ = {'schema': 'datasets'}

    id = Column(Integer, primary_key=True)
    config_id = Column(Integer, ForeignKey('datasets.transform_configs.id', ondelete='CASCADE'), nullable=False)
    
    status = Column(SQLEnum(TransformStatus), nullable=False, default=TransformStatus.PENDING)
    started_at = Column(TIMESTAMP(timezone=True), nullable=True)
    finished_at = Column(TIMESTAMP(timezone=True), nullable=True)
    
    rows_processed = Column(Integer, nullable=True)
    rows_output = Column(Integer, nullable=True)
    
    output_path = Column(String(500), nullable=True)
    
    error_message = Column(Text, nullable=True)
    
    # Execution details
    execution_details = Column(JSON, nullable=True)

