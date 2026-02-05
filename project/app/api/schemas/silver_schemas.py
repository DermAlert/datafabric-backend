"""
Pydantic Schemas for Silver Layer

These schemas define the API contracts for the Silver layer transformation system.
"""

from pydantic import BaseModel, Field, field_validator
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
    """
    Available operators for filter conditions.
    
    Comparison:
        - = : Equal to
        - != : Not equal to
        - > : Greater than
        - >= : Greater than or equal
        - < : Less than
        - <= : Less than or equal
    
    Pattern Matching:
        - LIKE : Case-sensitive pattern match (use % as wildcard)
        - ILIKE : Case-insensitive pattern match
    
    Set Operations:
        - IN : Value is in a list
        - NOT IN : Value is not in a list
    
    Null Checks:
        - IS NULL : Value is null
        - IS NOT NULL : Value is not null
    
    Range:
        - BETWEEN : Value is between min and max (inclusive)
    """
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


class FilterLogicEnum(str, Enum):
    """Logic to combine multiple filter conditions."""
    AND = "AND"
    OR = "OR"


class TransformStatusEnum(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class SilverWriteModeEnum(str, Enum):
    """
    Write mode for Silver Delta Lake operations.
    
    - **overwrite**: Replace all data each execution (legacy behavior)
    - **append**: Add to existing data (may create duplicates)
    - **merge**: Upsert based on merge_keys - insert new, update existing (recommended)
    """
    OVERWRITE = "overwrite"
    APPEND = "append"
    MERGE = "merge"


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
    
    @field_validator('column_ids')
    @classmethod
    def validate_columns(cls, v, info):
        if not info.data.get('select_all') and not v:
            raise ValueError("column_ids is required when select_all is False")
        return v


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


# ==================== INLINE FILTERS ====================

class FilterCondition(BaseModel):
    """
    A single filter condition for inline filters.
    
    RECOMMENDED: Use column_id (external_column.id from metadata) for unambiguous filtering.
    This avoids issues when multiple sources have columns with the same name.
    
    ALTERNATIVE: Use column_name for direct reference (may be ambiguous if columns have same name).
    
    Examples (using column_id - RECOMMENDED):
        {"column_id": 75, "operator": "IS NOT NULL"}
        {"column_id": 71, "operator": "=", "value": "João Silva"}
        {"column_id": 10, "operator": ">=", "value": 18}
        {"column_id": 15, "operator": "IN", "value": ["active", "pending"]}
        {"column_id": 20, "operator": "BETWEEN", "value_min": 10, "value_max": 100}
    
    Examples (using column_name - alternative):
        {"column_name": "cpf_paciente", "operator": "IS NOT NULL"}
        {"column_name": "status", "operator": "=", "value": "active"}
    
    Note: column_id is resolved to the actual Bronze column name automatically,
    handling any prefixing that may have occurred during ingestion.
    """
    column_id: Optional[int] = Field(None, description="external_column.id (RECOMMENDED - unambiguous)")
    column_name: Optional[str] = Field(None, description="Column name (alternative - may be ambiguous)")
    operator: FilterOperatorEnum = Field(..., description="Comparison operator")
    value: Optional[Any] = Field(None, description="Value to compare (not needed for IS NULL/IS NOT NULL)")
    value_min: Optional[Any] = Field(None, description="Min value for BETWEEN operator")
    value_max: Optional[Any] = Field(None, description="Max value for BETWEEN operator")
    
    @field_validator('value_min', 'value_max')
    @classmethod
    def validate_between_values(cls, v, info):
        if info.field_name in ('value_min', 'value_max'):
            operator = info.data.get('operator')
            if operator == FilterOperatorEnum.BETWEEN and v is None:
                raise ValueError(f"{info.field_name} is required for BETWEEN operator")
        return v


class InlineFilter(BaseModel):
    """
    Inline filter configuration for VirtualizedConfig and TransformConfig.
    
    Filters are applied as WHERE conditions in the query.
    Multiple conditions are combined using the specified logic (AND/OR).
    
    RECOMMENDED: Use column_id for unambiguous filtering across multiple sources.
    
    Example (using column_id - RECOMMENDED):
    ```json
    {
        "logic": "AND",
        "conditions": [
            {"column_id": 75, "operator": "IS NOT NULL"},
            {"column_id": 71, "operator": "=", "value": "João Silva"},
            {"column_id": 80, "operator": ">=", "value": 18}
        ]
    }
    ```
    
    Example (using column_name - alternative):
    ```json
    {
        "logic": "AND",
        "conditions": [
            {"column_name": "cpf_paciente", "operator": "IS NOT NULL"},
            {"column_name": "nome_paciente", "operator": "=", "value": "João Silva"}
        ]
    }
    ```
    
    Operators:
        - `=`, `!=`, `>`, `>=`, `<`, `<=` : Comparison
        - `LIKE`, `ILIKE` : Pattern matching (use % as wildcard)
        - `IN`, `NOT IN` : Set membership (value should be a list)
        - `IS NULL`, `IS NOT NULL` : Null checks
        - `BETWEEN` : Range check (requires value_min and value_max)
    """
    logic: FilterLogicEnum = Field(FilterLogicEnum.AND, description="How to combine conditions (AND/OR)")
    conditions: List[FilterCondition] = Field(..., min_length=1, description="List of filter conditions")


# ==================== COLUMN TRANSFORMATIONS ====================

class ColumnTransformation(BaseModel):
    """
    Column transformation for queries.
    
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


# ==================== VIRTUALIZED CONFIG ====================

class VirtualizedConfigCreate(BaseModel):
    """
    Create a virtualized config for querying original data sources via Trino.
    Data is NOT saved - returned as JSON (use for exploration, APIs, etc.).
    
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
        min_length=1, 
        description="Tables and columns to include (same structure as Bronze)"
    )
    
    column_group_ids: Optional[List[int]] = Field(
        None, 
        description="Column groups from Equivalence. Auto-loads ColumnMappings and ValueMappings."
    )
    relationship_ids: Optional[List[int]] = Field(
        None, 
        description="Relationship IDs from metadata.table_relationships for JOINs. "
                    "If None, auto-discovers relationships for selected tables. "
                    "If empty list [], uses CROSS JOIN (not recommended)."
    )
    
    filters: Optional[InlineFilter] = Field(
        None, 
        description="Inline filter conditions applied as WHERE clause"
    )
    
    column_transformations: Optional[List[ColumnTransformation]] = Field(
        None,
        description="Text transformations: template, lowercase, uppercase, trim, normalize_spaces, remove_accents"
    )
    
    exclude_unified_source_columns: bool = Field(
        False,
        description="When True, excludes original source columns after semantic unification. "
                    "E.g., if sex_group unifies clinical_sex and sexo, only sex_group will appear in output."
    )


class VirtualizedConfigUpdate(BaseModel):
    """Update a virtualized config."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    tables: Optional[List[TableColumnSelection]] = None
    column_group_ids: Optional[List[int]] = None
    relationship_ids: Optional[List[int]] = None
    filters: Optional[InlineFilter] = None
    column_transformations: Optional[List[ColumnTransformation]] = None
    exclude_unified_source_columns: Optional[bool] = None
    is_active: Optional[bool] = None


class VirtualizedConfigResponse(BaseModel):
    """Response for a virtualized config."""
    id: int
    name: str
    description: Optional[str]
    tables: List[Dict[str, Any]]
    column_group_ids: Optional[List[int]]
    relationship_ids: Optional[List[int]]
    filters: Optional[Dict[str, Any]]
    column_transformations: Optional[List[Dict[str, Any]]]
    exclude_unified_source_columns: bool = False
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

class TransformImageLabeling(BaseModel):
    """Image labeling configuration."""
    column_name: str = Field(..., description="Column with image paths")
    model: str = Field(..., description="Model to use for labeling")
    output_column: str = Field(..., description="Column for labels")
    confidence_column: Optional[str] = Field(None, description="Column for confidence scores")


class TransformConfigCreate(BaseModel):
    """
    Create a transform config for Bronze → Silver transformation.
    
    Supports the same transformations as VirtualizedConfig:
    - column_transformations: Same format (column_id + type)
    - column_group_ids: Semantic unification + value mappings from Equivalence
    - filters: Inline filter conditions
    
    Source Options:
    - source_bronze_config_id: Reference to Bronze Persistent Config (RECOMMENDED)
    - source_bronze_version: Optional specific Delta version to use from Bronze
    
    Write mode is always OVERWRITE. Delta Lake maintains version history automatically.
    """
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    
    source_bronze_config_id: int = Field(..., description="Bronze Persistent Config ID")
    source_bronze_version: Optional[int] = Field(None, description="Specific Bronze Delta version to read (null = latest)")
    
    silver_bucket: Optional[str] = Field(None, description="Output bucket (optional)")
    silver_path_prefix: Optional[str] = Field(None, description="Output path prefix")
    
    # Same as VirtualizedConfig
    column_group_ids: Optional[List[int]] = Field(
        None, 
        description="Column groups from Equivalence. Auto-loads ColumnMappings (column unification) and ValueMappings (value normalization)."
    )
    
    filters: Optional[InlineFilter] = Field(
        None, 
        description="Inline filter conditions applied to the data"
    )
    
    # column_transformations - SAME FORMAT AS VIRTUALIZED
    column_transformations: Optional[List[ColumnTransformation]] = Field(
        None,
        description="Text transformations (same as Virtualized): template, lowercase, uppercase, trim, normalize_spaces, remove_accents. Uses external_column_id, auto-resolved to bronze_column_name. For regex/complex rules, create via POST /api/silver/normalization-rules first."
    )
    
    image_labeling_config: Optional[TransformImageLabeling] = None
    
    exclude_unified_source_columns: bool = Field(
        False,
        description="When True, excludes original source columns after semantic unification. "
                    "E.g., if sex_group unifies clinical_sex and sexo, only sex_group will appear in output."
    )
    
    # Write mode is always OVERWRITE (versioning fields deprecated)


class TransformConfigUpdate(BaseModel):
    """Update a transform config."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    source_bronze_config_id: Optional[int] = Field(None, description="Bronze Persistent Config ID")
    source_bronze_version: Optional[int] = Field(None, description="Specific Bronze Delta version to read")
    silver_bucket: Optional[str] = None
    silver_path_prefix: Optional[str] = None
    column_group_ids: Optional[List[int]] = None
    filters: Optional[InlineFilter] = None
    column_transformations: Optional[List[ColumnTransformation]] = None
    image_labeling_config: Optional[TransformImageLabeling] = None
    exclude_unified_source_columns: Optional[bool] = None
    is_active: Optional[bool] = None


class TransformConfigResponse(BaseModel):
    """Response for a transform config."""
    id: int
    name: str
    description: Optional[str]
    
    # Source Bronze (new fields)
    source_bronze_config_id: Optional[int] = None
    source_bronze_config_name: Optional[str] = None
    source_bronze_version: Optional[int] = None  # null = latest
    
    # LEGACY (deprecated)
    source_bronze_dataset_id: Optional[int] = None
    source_bronze_dataset_name: Optional[str] = None
    
    silver_bucket: Optional[str]
    silver_path_prefix: Optional[str]
    column_group_ids: Optional[List[int]]
    filters: Optional[Dict[str, Any]]
    column_transformations: Optional[List[Dict[str, Any]]]
    image_labeling_config: Optional[Dict[str, Any]]
    exclude_unified_source_columns: bool = False
    
    # Versioning (always overwrite)
    write_mode: str = "overwrite"
    current_delta_version: Optional[int] = None
    
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
    
    # Delta Lake versioning info
    delta_version: Optional[int] = None
    write_mode_used: str = "overwrite"
    
    # Statistics
    rows_inserted: Optional[int] = None
    
    # Config snapshot at execution time (for reproducibility)
    config_snapshot: Optional[Dict[str, Any]] = None


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
    
    # Delta Lake versioning info
    delta_version: Optional[int] = None
    write_mode_used: Optional[str] = None
    merge_keys_used: Optional[List[str]] = None
    
    # MERGE statistics
    rows_inserted: Optional[int] = None
    rows_updated: Optional[int] = None
    rows_deleted: Optional[int] = None
    
    # Config snapshot at execution time (for reproducibility)
    config_snapshot: Optional[Dict[str, Any]] = None
    
    model_config = {"from_attributes": True}


# ==================== VERSION HISTORY ====================

class SilverVersionInfo(BaseModel):
    """Information about a single Silver Delta Lake version."""
    version: int
    timestamp: datetime
    operation: str  # WRITE, MERGE, DELETE, etc.
    execution_id: Optional[int] = None
    
    # Statistics
    rows_inserted: Optional[int] = None
    rows_updated: Optional[int] = None
    rows_deleted: Optional[int] = None
    total_rows: Optional[int] = None
    
    # Size info
    num_files: Optional[int] = None
    size_bytes: Optional[int] = None
    
    # Config snapshot at this version (for diff comparison)
    config_snapshot: Optional[Dict[str, Any]] = None


class SilverVersionHistoryResponse(BaseModel):
    """Response for version history of a Silver config."""
    config_id: int
    config_name: str
    current_version: Optional[int]
    output_paths: List[str]  # All output paths (for consistency with Bronze)
    versions: List[SilverVersionInfo]


class SilverDataQueryResponse(BaseModel):
    """Response for querying Silver data with optional time travel."""
    config_id: int
    config_name: str
    version: Optional[int] = None  # null = latest
    as_of_timestamp: Optional[datetime] = None
    columns: List[str]
    data: List[Dict[str, Any]]
    row_count: int
    total_rows: Optional[int] = None
    execution_time_seconds: float
