"""
Silver Layer Models for Data Transformation

This module defines the database models for the Silver layer of the Medallion architecture.
The Silver layer focuses on data transformation, normalization, and semantic unification.

Key concepts:
- NormalizationRule: Reusable rules for formatting data (CPF, phone, etc.)
- SemanticMapping: Maps columns from different sources to unified names
- VirtualizedConfig: Config for virtualized queries (sources → JSON) with inline filters
- TransformConfig: Config for materialization (Bronze → Silver Delta) with inline filters
"""

from sqlalchemy import Column, Integer, String, ForeignKey, JSON, Boolean, Text, Enum as SQLEnum, Float
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy import UniqueConstraint, TIMESTAMP
from ..session import Base
from ..base import AuditMixin
import enum


# ==================== ENUMS ====================

class NormalizationType(enum.Enum):
    """Type of normalization to apply"""
    TEMPLATE = "template"          # Uses template like {d3}.{d3}.{d3}-{d2}
    SQL_REGEX = "sql_regex"        # Uses SQL regexp_replace
    CASE_MAPPING = "case_mapping"  # CASE WHEN value mappings
    PYTHON_RULE = "python_rule"    # Uses Python Normalizer (only for transform)


class TransformStatus(enum.Enum):
    """Status of a transformation execution"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class SilverWriteMode(enum.Enum):
    """Write mode for Silver Delta Lake operations"""
    OVERWRITE = "overwrite"  # Replace all data (default behavior before versioning)
    APPEND = "append"        # Add to existing data (may duplicate)
    MERGE = "merge"          # Upsert: insert new, update existing (no duplicates)


# ==================== NORMALIZATION RULES ====================

class NormalizationRule(AuditMixin, Base):
    """
    Reusable normalization rule.
    
    Can be used with templates (like {d3}.{d3}.{d3}-{d2} for CPF)
    or with regex patterns for SQL-based transformations.
    """
    __tablename__ = "silver_normalization_rules"
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
    
    Filters are defined inline in the config (not referenced by ID).
    column_transformations is for text transformations (lowercase, trim, etc.)
    and normalization rules (template). For value mappings, use /api/equivalence.
    """
    __tablename__ = "silver_virtualized_configs"
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
    
    # Relationship IDs from metadata.table_relationships
    # When provided, uses these relationships for JOINs instead of CROSS JOIN
    # If None/empty, auto-discovers applicable relationships for selected tables
    relationship_ids = Column(JSON, nullable=True)  # List of table_relationship IDs
    
    # Inline filters (conditions applied to the query)
    # {
    #   "logic": "AND",  # AND or OR
    #   "conditions": [
    #     {"column_id": 10, "operator": "=", "value": "active"},
    #     {"column_name": "age", "operator": ">=", "value": 18}
    #   ]
    # }
    filters = Column(JSON, nullable=True)
    
    # Column transformations: template, lowercase, uppercase, trim, normalize_spaces, remove_accents
    # [{column_id: 100, type: "template", rule_id: 1}]
    # [{column_id: 101, type: "lowercase"}]
    column_transformations = Column(JSON, nullable=True)
    
    # When True, excludes original source columns after semantic unification
    # E.g., if sex_group unifies clinical_sex and sexo, only sex_group will appear
    exclude_unified_source_columns = Column(Boolean, default=False, nullable=False)
    
    is_active = Column(Boolean, default=True)
    
    # Cached SQL (for performance)
    generated_sql = Column(Text, nullable=True)
    sql_generated_at = Column(TIMESTAMP(timezone=True), nullable=True)


# ==================== TRANSFORM CONFIG ====================

class TransformConfig(AuditMixin, Base):
    """
    Configuration for materialization (Bronze → Silver Delta).
    
    Uses Bronze Persistent Config as source, applies transformations.
    Materializes to Silver Delta Lake with OVERWRITE mode.
    
    Source Options:
    - source_bronze_config_id: Reference to Bronze Persistent Config (RECOMMENDED)
    - source_bronze_version: Optional specific Delta version to use from Bronze
    - source_bronze_dataset_id: LEGACY - deprecated, use source_bronze_config_id
    
    Write mode is always OVERWRITE for simplicity and reliability.
    Delta Lake maintains version history automatically.
    """
    __tablename__ = "silver_persistent_configs"
    __table_args__ = (
        UniqueConstraint('name'),
        {'schema': 'datasets'}
    )

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    
    # Source: Bronze Persistent Config (NEW - RECOMMENDED)
    source_bronze_config_id = Column(Integer, ForeignKey('datasets.bronze_persistent_configs.id', ondelete='CASCADE'), nullable=True)
    source_bronze_version = Column(Integer, nullable=True)  # Optional: specific Delta version to read
    
    # Source: Bronze dataset (LEGACY - deprecated)
    source_bronze_dataset_id = Column(Integer, ForeignKey('core.datasets.id', ondelete='CASCADE'), nullable=True)
    
    # Output configuration
    silver_bucket = Column(String(255), nullable=True)  # Defaults to system silver bucket
    silver_path_prefix = Column(String(500), nullable=True)
    
    # Column groups from Equivalence (for semantic unification)
    # Uses BronzeColumnMapping to resolve external_column.id → bronze_column_name
    column_group_ids = Column(JSON, nullable=True)  # List of equivalence.column_groups IDs
    
    # Inline filters (conditions applied to the data)
    # {
    #   "logic": "AND",  # AND or OR
    #   "conditions": [
    #     {"column_id": 10, "operator": "=", "value": "active"},
    #     {"column_name": "status", "operator": "IN", "value": ["active", "pending"]}
    #   ]
    # }
    filters = Column(JSON, nullable=True)
    
    # Column transformations (same format as VirtualizedConfig)
    # [{column_id: 100, type: "template", rule_id: 1}]
    # [{column_id: 101, type: "lowercase"}]
    # Uses BronzeColumnMapping to resolve external_column_id → bronze_column_name
    # For regex/complex rules, create via POST /api/silver/normalization-rules
    column_transformations = Column(JSON, nullable=True)
    
    # Image labeling config (optional)
    # {column_name: "image_path", model: "classifier", output_column: "label"}
    image_labeling_config = Column(JSON, nullable=True)
    
    # When True, excludes original source columns after semantic unification
    # E.g., if sex_group unifies clinical_sex and sexo, only sex_group will appear
    exclude_unified_source_columns = Column(Boolean, default=False, nullable=False)
    
    # === VERSIONING CONFIGURATION ===
    # Write mode is always OVERWRITE for simplicity (column kept for compatibility)
    write_mode = Column(SQLEnum(SilverWriteMode), nullable=False, default=SilverWriteMode.OVERWRITE)
    
    # DEPRECATED: merge_keys no longer used (always overwrite)
    merge_keys = Column(JSON, nullable=True)
    merge_keys_source = Column(String(50), nullable=True)
    
    # Delta Lake versioning info
    current_delta_version = Column(Integer, nullable=True)  # Latest Delta version
    
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
    
    Tracks each execution including versioning info and merge statistics.
    """
    __tablename__ = "silver_executions"
    __table_args__ = {'schema': 'datasets'}

    id = Column(Integer, primary_key=True)
    config_id = Column(Integer, ForeignKey('datasets.silver_persistent_configs.id', ondelete='CASCADE'), nullable=False)
    
    status = Column(SQLEnum(TransformStatus), nullable=False, default=TransformStatus.PENDING)
    started_at = Column(TIMESTAMP(timezone=True), nullable=True)
    finished_at = Column(TIMESTAMP(timezone=True), nullable=True)
    
    rows_processed = Column(Integer, nullable=True)
    rows_output = Column(Integer, nullable=True)
    
    output_path = Column(String(500), nullable=True)
    
    error_message = Column(Text, nullable=True)
    
    # Execution details
    execution_details = Column(JSON, nullable=True)
    
    # === DELTA LAKE VERSIONING ===
    # Delta version created by this execution
    delta_version = Column(Integer, nullable=True)
    
    # Write mode used in this execution
    write_mode_used = Column(String(20), nullable=True)  # overwrite, append, merge
    
    # Merge keys used (if merge mode)
    merge_keys_used = Column(JSON, nullable=True)
    
    # MERGE operation statistics
    rows_inserted = Column(Integer, nullable=True)  # New rows inserted
    rows_updated = Column(Integer, nullable=True)   # Existing rows updated
    rows_deleted = Column(Integer, nullable=True)   # Rows deleted (if applicable)
    
    # Schema tracking (internal - for auditing schema changes)
    schema_hash = Column(String(64), nullable=True)  # Hash of config
    schema_changed = Column(Boolean, default=False)  # True if schema changed from previous execution
    
    # Full config snapshot at execution time (for reproducibility)
    # Stores exactly what config was used to generate this version
    config_snapshot = Column(JSON, nullable=True)