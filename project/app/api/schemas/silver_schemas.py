"""
Pydantic Schemas for Silver Layer

These schemas define the API contracts for the Silver layer transformation system.
"""

from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


# ==================== ENUMS ====================

class NormalizationTypeEnum(str, Enum):
    """
    Available transformation types for column_transformations.
    
    Rule-based:
        - template: Use a normalization rule created via POST /normalization-rules
        - python_rule: Python rule (Transform only, not Virtualized)
    
    SQL Text Transformations (no additional params needed):
        - lowercase: Convert to lowercase → LOWER(column)
        - uppercase: Convert to uppercase → UPPER(column)
        - trim: Remove leading/trailing spaces → TRIM(column)
        - normalize_spaces: Collapse multiple spaces → TRIM(regexp_replace(col, '\\s+', ' '))
        - remove_accents: Remove accents → translate(col, 'áàâ...', 'aaa...')
    
    Note: For value mappings (M→Masculino), use the Equivalence module (/api/equivalence).
    """
    # Rule-based transformations
    TEMPLATE = "template"
    PYTHON_RULE = "python_rule"
    
    # SQL text transformations
    LOWERCASE = "lowercase"
    UPPERCASE = "uppercase"
    TRIM = "trim"
    NORMALIZE_SPACES = "normalize_spaces"
    REMOVE_ACCENTS = "remove_accents"


class FilterOperatorEnum(str, Enum):
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


# ==================== TABLE SELECTION ====================

class TableColumnSelection(BaseModel):
    """
    Defines which columns to select from a table.
    
    If select_all is True, all columns will be included.
    Otherwise, only the columns in column_ids will be included.
    
    Examples:
        {"table_id": 1, "select_all": true}
        {"table_id": 2, "column_ids": [10, 11, 12]}
    """
    table_id: int = Field(..., description="ID of the table (from GET /api/metadata/tables)")
    select_all: bool = Field(True, description="If True, select all columns from this table")
    column_ids: Optional[List[int]] = Field(None, description="Specific column IDs to include (ignored if select_all=True)")
    
    @validator('column_ids')
    def validate_columns(cls, v, values):
        if not values.get('select_all') and not v:
            raise ValueError("column_ids is required when select_all is False")
        return v


class FilterLogicEnum(str, Enum):
    AND = "AND"
    OR = "OR"


class TransformStatusEnum(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


# ==================== NORMALIZATION RULES ====================

class NormalizationRuleCreate(BaseModel):
    """Create a normalization rule using template syntax."""
    name: str = Field(..., min_length=1, max_length=100, description="Unique rule name")
    description: Optional[str] = Field(None, description="Description of the rule")
    template: str = Field(..., description="Template like {d3}.{d3}.{d3}-{d2} for CPF")
    pre_process: Optional[str] = Field(None, description="Pre-processing: digits_only, uppercase, etc.")
    example_input: Optional[str] = Field(None, description="Example input value")
    example_output: Optional[str] = Field(None, description="Expected output")


class NormalizationRuleUpdate(BaseModel):
    """Update a normalization rule."""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    template: Optional[str] = None
    pre_process: Optional[str] = None
    example_input: Optional[str] = None
    example_output: Optional[str] = None
    is_active: Optional[bool] = None


class NormalizationRuleResponse(BaseModel):
    """Response for a normalization rule."""
    id: int
    name: str
    description: Optional[str]
    template: Optional[str]
    regex_pattern: Optional[str]
    regex_replacement: Optional[str]
    pre_process: Optional[str]
    regex_entrada: Optional[str]
    template_saida: Optional[str]
    example_input: Optional[str]
    example_output: Optional[str]
    is_builtin: bool
    is_active: bool
    
    model_config = {"from_attributes": True}


class NormalizationRuleTest(BaseModel):
    """Test a normalization rule with a value."""
    rule_id: Optional[int] = Field(None, description="ID of existing rule")
    rule_name: Optional[str] = Field(None, description="Name of existing rule")
    template: Optional[str] = Field(None, description="Or provide template inline")
    value: str = Field(..., description="Value to normalize")


class NormalizationRuleTestResult(BaseModel):
    """Result of testing a normalization rule."""
    success: bool
    original_value: str
    normalized_value: Optional[str] = None
    rule_applied: Optional[str] = None
    error: Optional[str] = None


# ==================== SEMANTIC MAPPINGS ====================
# NOTE: Semantic mappings are managed by the Equivalence module.
# Use equivalence.ColumnGroup and equivalence.ColumnMapping via /api/equivalence endpoints.
# VirtualizedConfig and TransformConfig reference column_group_ids from Equivalence.


# ==================== SEMANTIC COLUMN (for Transform) ====================

class SemanticColumnConfig(BaseModel):
    """
    Semantic column unification config for Transform (Bronze → Silver).
    
    Maps multiple Bronze column names to a unified name.
    Uses column names (strings) since Bronze Delta Lake doesn't have external_column.id.
    """
    unified_name: str = Field(..., description="Output column name in Silver")
    source_columns: List[str] = Field(..., min_items=1, description="Bronze column names to unify")
    normalization_rule_id: Optional[int] = Field(None, description="Optional rule to apply")
    aggregation: Optional[str] = Field(None, description="How to aggregate: 'coalesce', 'first', 'concat'")


# ==================== FILTERS ====================

class FilterConditionCreate(BaseModel):
    """A single filter condition."""
    column_id: Optional[int] = Field(None, description="external_column.id (for virtualized)")
    column_name: Optional[str] = Field(None, description="Column name (for transform)")
    operator: FilterOperatorEnum
    value: Optional[Any] = Field(None, description="Value to compare")
    value_min: Optional[Any] = Field(None, description="Min value for BETWEEN")
    value_max: Optional[Any] = Field(None, description="Max value for BETWEEN")


class SilverFilterCreate(BaseModel):
    """Create a reusable filter."""
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    logic: FilterLogicEnum = Field(FilterLogicEnum.AND, description="How to combine conditions")
    conditions: List[FilterConditionCreate] = Field(..., min_items=1)


class SilverFilterUpdate(BaseModel):
    """Update a filter."""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    logic: Optional[FilterLogicEnum] = None
    is_active: Optional[bool] = None


class FilterConditionResponse(BaseModel):
    """Response for a filter condition."""
    id: int
    column_id: Optional[int]
    column_name: Optional[str]
    operator: str
    value: Optional[Any]
    value_min: Optional[Any]
    value_max: Optional[Any]
    
    model_config = {"from_attributes": True}


class SilverFilterResponse(BaseModel):
    """Response for a filter."""
    id: int
    name: str
    description: Optional[str]
    logic: str
    is_active: bool
    conditions: List[FilterConditionResponse] = []
    
    model_config = {"from_attributes": True}


# ==================== VIRTUALIZED CONFIG ====================

class VirtualizedColumnTransformation(BaseModel):
    """
    Column transformation for virtualized queries (SQL only).
    
    Available Types:
        Rule-based:
        - "template": Use rule from POST /normalization-rules → {"column_id": 10, "type": "template", "rule_id": 1}
        
        SQL Text (no extra params):
        - "lowercase": LOWER() → {"column_id": 10, "type": "lowercase"}
        - "uppercase": UPPER() → {"column_id": 10, "type": "uppercase"}
        - "trim": TRIM() → {"column_id": 10, "type": "trim"}
        - "normalize_spaces": Collapse spaces → {"column_id": 10, "type": "normalize_spaces"}
        - "remove_accents": Remove accents → {"column_id": 10, "type": "remove_accents"}
    
    For custom regex: create a rule via POST /api/silver/normalization-rules first.
    For value mappings (M→Masculino): use /api/equivalence (ValueMappings).
    """
    column_id: int = Field(..., description="external_column.id (from GET /api/metadata/tables/{id}/columns)")
    type: NormalizationTypeEnum = Field(
        ..., 
        description="Transformation type: template, lowercase, uppercase, trim, normalize_spaces, remove_accents"
    )
    # For template - reference a created rule
    rule_id: Optional[int] = Field(None, description="normalization_rule.id (required for type=template)")


class VirtualizedConfigCreate(BaseModel):
    """
    Create a virtualized config.
    
    When column_group_ids is provided, the system automatically loads from Equivalence:
    - ColumnMappings: which columns to unify under the same name
    - ValueMappings: how to normalize values (e.g., male→Masculino)
    
    column_transformations is for text transformations (lowercase, trim, etc.) and 
    normalization rules (template). For value mappings, use /api/equivalence.
    """
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    
    tables: List[TableColumnSelection] = Field(
        ..., 
        min_items=1, 
        description="Tables and columns to include (same structure as Bronze)"
    )
    
    column_group_ids: Optional[List[int]] = Field(
        None, 
        description="Column groups from Equivalence. Auto-loads ColumnMappings and ValueMappings."
    )
    filter_ids: Optional[List[int]] = Field(None, description="Filter IDs (from POST /api/silver/filters)")
    
    column_transformations: Optional[List[VirtualizedColumnTransformation]] = Field(
        None,
        description="Text transformations: template, lowercase, uppercase, trim, normalize_spaces, remove_accents"
    )


class VirtualizedConfigUpdate(BaseModel):
    """Update a virtualized config."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    tables: Optional[List[TableColumnSelection]] = None
    column_group_ids: Optional[List[int]] = None
    filter_ids: Optional[List[int]] = None
    column_transformations: Optional[List[VirtualizedColumnTransformation]] = None
    is_active: Optional[bool] = None


class VirtualizedConfigResponse(BaseModel):
    """Response for a virtualized config."""
    id: int
    name: str
    description: Optional[str]
    tables: List[Dict[str, Any]]
    column_group_ids: Optional[List[int]]
    filter_ids: Optional[List[int]]
    column_transformations: Optional[List[Dict[str, Any]]]
    is_active: bool
    generated_sql: Optional[str]
    
    model_config = {"from_attributes": True}


class VirtualizedPreviewResponse(BaseModel):
    """Preview of virtualized query."""
    config_id: int
    config_name: str
    sql: str
    columns: List[str]
    estimated_tables: int
    warnings: List[str] = []


class VirtualizedQueryResponse(BaseModel):
    """Response for virtualized query execution."""
    config_id: int
    config_name: str
    columns: List[str]
    data: List[Dict[str, Any]]
    row_count: int
    total_rows_available: Optional[int] = None
    execution_time_seconds: float
    sql_executed: Optional[str] = None
    warnings: List[str] = []


# ==================== TRANSFORM CONFIG ====================

class TransformColumnNormalization(BaseModel):
    """Column normalization for transform (Bronze → Silver)."""
    column_name: str = Field(..., description="Column name in Bronze dataset")
    type: NormalizationTypeEnum = Field(..., description="Type of normalization")
    # For rule type
    rule_id: Optional[int] = Field(None, description="normalization_rule.id")
    # For template type
    template: Optional[str] = Field(None, description="Template inline")
    # For sql_regex
    pattern: Optional[str] = None
    replacement: Optional[str] = None
    # Output column name (optional, defaults to same)
    output_column: Optional[str] = None


class TransformValueMapping(BaseModel):
    """Value mapping for transform."""
    column_name: str = Field(..., description="Column name in Bronze dataset")
    output_column: Optional[str] = Field(None, description="Output column name")
    mappings: Dict[str, str] = Field(..., description="Value mappings")
    default_value: Optional[str] = Field("", description="Default if no match")


class TransformImageLabeling(BaseModel):
    """Image labeling configuration."""
    column_name: str = Field(..., description="Column with image paths")
    model: str = Field(..., description="Model to use for labeling")
    output_column: str = Field(..., description="Column for labels")
    confidence_column: Optional[str] = Field(None, description="Column for confidence scores")


class TransformConfigCreate(BaseModel):
    """Create a transform config."""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    
    source_bronze_dataset_id: int = Field(..., description="Bronze dataset ID")
    
    silver_bucket: Optional[str] = Field(None, description="Output bucket (optional)")
    silver_path_prefix: Optional[str] = Field(None, description="Output path prefix")
    
    column_group_ids: Optional[List[int]] = Field(None, description="Column groups from Equivalence (uses BronzeColumnMapping)")
    semantic_columns: Optional[List[SemanticColumnConfig]] = Field(None, description="Direct column name mapping")
    filter_ids: Optional[List[int]] = None
    
    column_normalizations: Optional[List[TransformColumnNormalization]] = None
    value_mappings: Optional[List[TransformValueMapping]] = None
    
    image_labeling_config: Optional[TransformImageLabeling] = None


class TransformConfigUpdate(BaseModel):
    """Update a transform config."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    source_bronze_dataset_id: Optional[int] = None
    silver_bucket: Optional[str] = None
    silver_path_prefix: Optional[str] = None
    column_group_ids: Optional[List[int]] = None
    semantic_columns: Optional[List[SemanticColumnConfig]] = None
    filter_ids: Optional[List[int]] = None
    column_normalizations: Optional[List[TransformColumnNormalization]] = None
    value_mappings: Optional[List[TransformValueMapping]] = None
    image_labeling_config: Optional[TransformImageLabeling] = None
    is_active: Optional[bool] = None


class TransformConfigResponse(BaseModel):
    """Response for a transform config."""
    id: int
    name: str
    description: Optional[str]
    source_bronze_dataset_id: int
    source_bronze_dataset_name: Optional[str] = None
    silver_bucket: Optional[str]
    silver_path_prefix: Optional[str]
    column_group_ids: Optional[List[int]]
    semantic_columns: Optional[List[Dict[str, Any]]]
    filter_ids: Optional[List[int]]
    column_normalizations: Optional[List[Dict[str, Any]]]
    value_mappings: Optional[List[Dict[str, Any]]]
    image_labeling_config: Optional[Dict[str, Any]]
    last_execution_time: Optional[datetime]
    last_execution_status: Optional[str]
    last_execution_rows: Optional[int]
    is_active: bool
    
    model_config = {"from_attributes": True}


class TransformPreviewResponse(BaseModel):
    """Preview of transform execution."""
    config_id: int
    config_name: str
    source_bronze_dataset: str
    sql_transformations: str
    python_transformations: List[str]
    output_columns: List[str]
    estimated_rows: Optional[int]
    output_path: str
    warnings: List[str] = []


class TransformExecuteResponse(BaseModel):
    """Response for transform execution."""
    config_id: int
    config_name: str
    execution_id: int
    status: TransformStatusEnum
    rows_processed: int
    rows_output: int
    output_path: str
    execution_time_seconds: float
    message: str


class TransformExecutionResponse(BaseModel):
    """Response for a transform execution history entry."""
    id: int
    config_id: int
    status: str
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    rows_processed: Optional[int]
    rows_output: Optional[int]
    output_path: Optional[str]
    error_message: Optional[str]
    
    model_config = {"from_attributes": True}

