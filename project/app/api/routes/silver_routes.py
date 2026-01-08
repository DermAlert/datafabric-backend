"""
Silver Layer API Routes

This module provides endpoints for:
- Normalization rules management
- Filters management
- Virtualized configs (sources → JSON)
- Transform configs (Bronze → Silver Delta)

NOTE: Semantic mappings are managed by the Equivalence module (/api/equivalence).
Use ColumnGroup and ColumnMapping from Equivalence for semantic unification.
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional

from ..schemas.silver_schemas import (
    # Normalization Rules
    NormalizationRuleCreate,
    NormalizationRuleUpdate,
    NormalizationRuleResponse,
    NormalizationRuleTest,
    NormalizationRuleTestResult,
    NormalizationTypeEnum,
    # Filters
    SilverFilterCreate,
    SilverFilterUpdate,
    SilverFilterResponse,
    # Virtualized
    VirtualizedConfigCreate,
    VirtualizedConfigUpdate,
    VirtualizedConfigResponse,
    VirtualizedPreviewResponse,
    VirtualizedQueryResponse,
    # Transform
    TransformConfigCreate,
    TransformConfigUpdate,
    TransformConfigResponse,
    TransformPreviewResponse,
    TransformExecuteResponse,
)
from ...services.silver_transformation_service import SilverTransformationService
from ...database.database import get_db

router = APIRouter(prefix="/silver", tags=["Silver Layer"])


# ==============================================================================
# TRANSFORMATION TYPES REFERENCE
# ==============================================================================

@router.get(
    "/transformation-types",
    summary="List available transformation types",
    description="Returns all available transformation types for column_transformations with examples."
)
async def list_transformation_types():
    """
    List all available transformation types for use in column_transformations.
    """
    return {
        "transformation_types": [
            {
                "type": "template",
                "category": "rule_based",
                "description": "Use a normalization rule created via POST /normalization-rules",
                "required_fields": ["rule_id"],
                "sql_generated": "regexp_replace(col, 'pattern', 'replacement')",
                "example": {"column_id": 10, "type": "template", "rule_id": 1},
                "note": "Create rules first via POST /api/silver/normalization-rules with a template like {d3}.{d3}.{d3}-{d2}"
            },
            {
                "type": "lowercase",
                "category": "text_transformation",
                "description": "Convert text to lowercase",
                "required_fields": [],
                "sql_generated": "LOWER(col)",
                "example": {"column_id": 10, "type": "lowercase"}
            },
            {
                "type": "uppercase",
                "category": "text_transformation",
                "description": "Convert text to uppercase",
                "required_fields": [],
                "sql_generated": "UPPER(col)",
                "example": {"column_id": 10, "type": "uppercase"}
            },
            {
                "type": "trim",
                "category": "text_transformation",
                "description": "Remove leading and trailing spaces",
                "required_fields": [],
                "sql_generated": "TRIM(col)",
                "example": {"column_id": 10, "type": "trim"}
            },
            {
                "type": "normalize_spaces",
                "category": "text_transformation",
                "description": "Collapse multiple spaces into one and trim",
                "required_fields": [],
                "sql_generated": "TRIM(regexp_replace(col, '\\s+', ' '))",
                "example": {"column_id": 10, "type": "normalize_spaces"}
            },
            {
                "type": "remove_accents",
                "category": "text_transformation",
                "description": "Remove accents from characters (á→a, é→e, etc.)",
                "required_fields": [],
                "sql_generated": "translate(col, 'áàâãä...', 'aaaaa...')",
                "example": {"column_id": 10, "type": "remove_accents"}
            }
        ],
        "categories": {
            "rule_based": "Transformations that require rule_id (from /normalization-rules)",
            "text_transformation": "Simple SQL text transformations (no additional params needed)"
        },
        "notes": [
            "column_id is required for all transformations",
            "Get column_ids from GET /api/metadata/tables/{table_id}/columns",
            "For custom regex patterns, create a normalization rule first via POST /api/silver/normalization-rules",
            "For value mappings (M→Masculino), use /api/equivalence (ValueMappings)",
            "Multiple transformations on same column are applied in order"
        ]
    }


def get_service(db: AsyncSession = Depends(get_db)) -> SilverTransformationService:
    return SilverTransformationService(db)


# ==============================================================================
# NORMALIZATION RULES
# ==============================================================================

@router.get(
    "/normalization-rules",
    response_model=List[NormalizationRuleResponse],
    summary="List normalization rules",
    description="List all normalization rules. Rules define how to format data (e.g., CPF, phone numbers)."
)
async def list_normalization_rules(
    include_inactive: bool = Query(False, description="Include inactive rules"),
    service: SilverTransformationService = Depends(get_service)
):
    return await service.list_normalization_rules(include_inactive=include_inactive)


@router.post(
    "/normalization-rules",
    response_model=NormalizationRuleResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create normalization rule",
    description="""
Create a normalization rule using template syntax.

**Template placeholders:**
- `{d}` - single digit
- `{d3}` - exactly 3 digits
- `{D}` - one or more digits
- `{l}` - single letter
- `{l2}` - exactly 2 letters
- `{L}` - one or more letters
- `{w}` - single alphanumeric
- `{W}` - one or more alphanumerics
- Add `?` for optional: `{d?}`, `{d3?}`

**Examples:**
- CPF: `{d3}.{d3}.{d3}-{d2}` → `123.456.789-01`
- Phone: `({d2}) {d5}-{d4}` → `(61) 99999-8888`
- CEP: `{d5}-{d3}` → `70000-000`
"""
)
async def create_normalization_rule(
    data: NormalizationRuleCreate,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.create_normalization_rule(data)


@router.get(
    "/normalization-rules/templates",
    summary="Get template help",
    description="Get help on template placeholders and examples."
)
async def get_template_help():
    return {
        "placeholders": {
            "{d}": "Single digit (0-9)",
            "{d3}": "Exactly 3 digits",
            "{D}": "One or more digits (variable length)",
            "{l}": "Single letter (A-Z, a-z)",
            "{l2}": "Exactly 2 letters",
            "{L}": "One or more letters (variable length)",
            "{w}": "Single alphanumeric (A-Z, a-z, 0-9, _)",
            "{W}": "One or more alphanumerics",
            "{d?}": "Optional single digit",
            "{d3?}": "Optional 3 digits",
        },
        "case_modifiers": {
            "{l:upper}": "Letter converted to uppercase",
            "{l:lower}": "Letter converted to lowercase",
        },
        "examples": [
            {
                "name": "CPF",
                "template": "{d3}.{d3}.{d3}-{d2}",
                "input": "12345678901",
                "output": "123.456.789-01"
            },
            {
                "name": "Phone (Brazil)",
                "template": "({d2}) {d5}-{d4}",
                "input": "61999998888",
                "output": "(61) 99999-8888"
            },
            {
                "name": "CEP",
                "template": "{d5}-{d3}",
                "input": "70000000",
                "output": "70000-000"
            },
            {
                "name": "License Plate",
                "template": "{l3:upper}-{d}{l}{d2}",
                "input": "abc1d23",
                "output": "ABC-1D23"
            }
        ]
    }


@router.get(
    "/normalization-rules/{rule_id}",
    response_model=NormalizationRuleResponse,
    summary="Get normalization rule",
    description="Get details of a specific normalization rule."
)
async def get_normalization_rule(
    rule_id: int,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.get_normalization_rule(rule_id)


@router.put(
    "/normalization-rules/{rule_id}",
    response_model=NormalizationRuleResponse,
    summary="Update normalization rule",
    description="Update an existing normalization rule."
)
async def update_normalization_rule(
    rule_id: int,
    data: NormalizationRuleUpdate,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.update_normalization_rule(rule_id, data)


@router.delete(
    "/normalization-rules/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete normalization rule",
    description="Delete a normalization rule."
)
async def delete_normalization_rule(
    rule_id: int,
    service: SilverTransformationService = Depends(get_service)
):
    await service.delete_normalization_rule(rule_id)


@router.post(
    "/normalization-rules/test",
    response_model=NormalizationRuleTestResult,
    summary="Test normalization rule",
    description="Test a normalization rule with a sample value. You can test by rule_id, rule_name, or provide a template inline."
)
async def test_normalization_rule(
    data: NormalizationRuleTest,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.test_normalization_rule(data)


# ==============================================================================
# SEMANTIC MAPPINGS
# ==============================================================================
# NOTE: Semantic mappings are managed by the Equivalence module.
# Use /api/equivalence endpoints for ColumnGroup and ColumnMapping.
#
# For VirtualizedConfig and TransformConfig, use column_group_ids 
# to reference ColumnGroups from Equivalence.


# ==============================================================================
# FILTERS
# ==============================================================================

@router.get(
    "/filters",
    response_model=List[SilverFilterResponse],
    summary="List filters",
    description="List all reusable filters."
)
async def list_filters(
    include_inactive: bool = Query(False, description="Include inactive filters"),
    service: SilverTransformationService = Depends(get_service)
):
    return await service.list_filters(include_inactive=include_inactive)


@router.post(
    "/filters",
    response_model=SilverFilterResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create filter",
    description="""
Create a reusable filter with conditions.

**Operators:**
`=`, `!=`, `>`, `>=`, `<`, `<=`, `LIKE`, `ILIKE`, `IN`, `NOT IN`, `IS NULL`, `IS NOT NULL`, `BETWEEN`

**Example:**
```json
{
  "name": "adults_active",
  "description": "Adult active patients",
  "logic": "AND",
  "conditions": [
    {"column_name": "age", "operator": ">=", "value": 18},
    {"column_name": "status", "operator": "=", "value": "active"}
  ]
}
```
"""
)
async def create_filter(
    data: SilverFilterCreate,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.create_filter(data)


@router.get(
    "/filters/{filter_id}",
    response_model=SilverFilterResponse,
    summary="Get filter",
    description="Get details of a specific filter with all conditions."
)
async def get_filter(
    filter_id: int,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.get_filter(filter_id)


@router.put(
    "/filters/{filter_id}",
    response_model=SilverFilterResponse,
    summary="Update filter",
    description="Update a filter's metadata."
)
async def update_filter(
    filter_id: int,
    data: SilverFilterUpdate,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.update_filter(filter_id, data)


@router.delete(
    "/filters/{filter_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete filter",
    description="Delete a filter."
)
async def delete_filter(
    filter_id: int,
    service: SilverTransformationService = Depends(get_service)
):
    await service.delete_filter(filter_id)


# ==============================================================================
# VIRTUALIZED CONFIGS
# ==============================================================================

@router.get(
    "/virtualized/configs",
    response_model=List[VirtualizedConfigResponse],
    summary="List virtualized configs",
    description="List all virtualized configs. These configs query original data sources via Trino."
)
async def list_virtualized_configs(
    include_inactive: bool = Query(False, description="Include inactive configs"),
    service: SilverTransformationService = Depends(get_service)
):
    return await service.list_virtualized_configs(include_inactive=include_inactive)


@router.post(
    "/virtualized/configs",
    response_model=VirtualizedConfigResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create virtualized config",
    description="""
Create a virtualized config for querying original data sources via Trino.
Data is NOT saved - returned as JSON (use for exploration, APIs, etc.).

---

## **Request Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | ✅ | Unique name for the config |
| `description` | string | ❌ | Optional description |
| `tables` | object[] | ✅ | Tables and columns (same structure as Bronze) |
| `column_group_ids` | int[] | ❌ | Semantic groups from Equivalence (auto-loads ColumnMappings + ValueMappings) |
| `relationship_ids` | int[] | ❌ | Relationship IDs for JOINs. `null`=auto-discover, `[]`=CROSS JOIN (not recommended) |
| `filter_ids` | int[] | ❌ | Filter IDs from POST /api/silver/filters |
| `column_transformations` | object[] | ❌ | Text transformations per column |
| `exclude_unified_source_columns` | bool | ❌ | When `true`, excludes original source columns after semantic unification. Default: `false` |

---

## **tables Structure (same as Bronze):**

```json
"tables": [
  {"table_id": 1, "select_all": true},
  {"table_id": 2, "column_ids": [10, 11, 12]}
]
```

| Field | Type | Description |
|-------|------|-------------|
| `table_id` | int | ID from GET /api/metadata/tables |
| `select_all` | bool | If true, includes all columns (default: true) |
| `column_ids` | int[] | Specific columns (required if select_all=false) |

---

## **column_transformations Types:**

| type | Description | Required Fields |
|------|-------------|-----------------|
| `template` | Use normalization rule | `rule_id` (from POST /normalization-rules) |
| `lowercase` | LOWER() | - |
| `uppercase` | UPPER() | - |
| `trim` | TRIM() | - |
| `normalize_spaces` | Remove extra spaces | - |
| `remove_accents` | Remove accents (á→a) | - |

See GET /api/silver/transformation-types for full documentation.

**For value mappings (M→Masculino):** use /api/equivalence (ValueMappings + ColumnGroups).

---

## **Example 1: Minimal (all columns from one table)**
```json
{
  "name": "patient_data",
  "tables": [{"table_id": 14, "select_all": true}]
}
```

---

## **Example 2: Specific columns**
```json
{
  "name": "patient_subset",
  "tables": [{"table_id": 14, "column_ids": [160, 161, 162]}]
}
```

---

## **Example 3: Multiple tables with Semantic Equivalence**
```json
{
  "name": "unified_patients",
  "tables": [
    {"table_id": 2, "select_all": true},
    {"table_id": 14, "select_all": true}
  ],
  "column_group_ids": [1]
}
```
*column_group_ids* loads from Equivalence module:
- **ColumnMappings**: unify columns (e.g., "sexo" + "clinical_sex" → "sex_group")
- **ValueMappings**: normalize values (e.g., "M"→"Masculino", "male"→"Masculino")

---

## **Example 4: With Filters**
```json
{
  "name": "young_patients",
  "tables": [
    {"table_id": 2, "select_all": true},
    {"table_id": 14, "select_all": true}
  ],
  "filter_ids": [1]
}
```
*filter_ids* apply WHERE conditions created via POST /api/silver/filters.

---

## **Example 5: With Column Transformations**
```json
{
  "name": "normalized_patients",
  "tables": [{"table_id": 14, "select_all": true}],
  "column_transformations": [
    {"column_id": 160, "type": "uppercase"},
    {"column_id": 161, "type": "trim"},
    {"column_id": 162, "type": "template", "rule_id": 1},
    {"column_id": 163, "type": "normalize_spaces"},
    {"column_id": 164, "type": "remove_accents"},
    {"column_id": 165, "type": "lowercase"}
  ]
}
```

---

## **Example 6: Complete (all features)**
```json
{
  "name": "full_patient_exploration",
  "description": "Unified patient data with all transformations",
  "tables": [
    {"table_id": 2, "column_ids": [50, 51, 52]},
    {"table_id": 14, "column_ids": [160, 161, 162]}
  ],
  "column_group_ids": [1, 2],
  "relationship_ids": [4, 5],
  "filter_ids": [1, 2],
  "column_transformations": [
    {"column_id": 162, "type": "template", "rule_id": 1},
    {"column_id": 160, "type": "uppercase"},
    {"column_id": 161, "type": "trim"}
  ],
  "exclude_unified_source_columns": true
}
```

*exclude_unified_source_columns*: when `true`, only the unified column appears (e.g., only `sex_group` instead of `clinical_sex` + `sexo` + `sex_group`)

*relationship_ids*: specify which relationships to use for JOINs. If `null`, auto-discovers. If `[]`, forces CROSS JOIN.

---

## **Notes:**
- Use GET /api/metadata/tables to find `table_id`
- Use GET /api/metadata/tables/{id}/columns to find `column_id` for column_ids and column_transformations
- Create rules first via POST /api/silver/normalization-rules for `template` type
- Create filters first via POST /api/silver/filters for `filter_ids`
- For value mappings (M→Masculino), use /api/equivalence (not column_transformations)
- Python-based transformations are NOT available (use Transform for that)
"""
)
async def create_virtualized_config(
    data: VirtualizedConfigCreate,
    service: SilverTransformationService = Depends(get_service)
):
    """Create a new virtualized config for querying original data sources."""
    return await service.create_virtualized_config(data)


@router.get(
    "/virtualized/configs/{config_id}",
    response_model=VirtualizedConfigResponse,
    summary="Get virtualized config",
    description="Get details of a specific virtualized config."
)
async def get_virtualized_config(
    config_id: int,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.get_virtualized_config(config_id)


@router.put(
    "/virtualized/configs/{config_id}",
    response_model=VirtualizedConfigResponse,
    summary="Update virtualized config",
    description="Update a virtualized config."
)
async def update_virtualized_config(
    config_id: int,
    data: VirtualizedConfigUpdate,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.update_virtualized_config(config_id, data)


@router.delete(
    "/virtualized/configs/{config_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete virtualized config",
    description="Delete a virtualized config."
)
async def delete_virtualized_config(
    config_id: int,
    service: SilverTransformationService = Depends(get_service)
):
    await service.delete_virtualized_config(config_id)


@router.post(
    "/virtualized/configs/{config_id}/preview",
    response_model=VirtualizedPreviewResponse,
    summary="Preview virtualized config",
    description="Generate and preview the SQL that will be executed without running it."
)
async def preview_virtualized_config(
    config_id: int,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.preview_virtualized_config(config_id)


@router.post(
    "/virtualized/configs/{config_id}/query",
    response_model=VirtualizedQueryResponse,
    summary="Execute virtualized query",
    description="""
Execute a virtualized query and return data as JSON.

**Note:** This does NOT save data. For large datasets, use pagination (limit/offset).
"""
)
async def query_virtualized_config(
    config_id: int,
    limit: int = Query(1000, ge=1, le=100000, description="Maximum rows to return"),
    offset: int = Query(0, ge=0, description="Rows to skip"),
    service: SilverTransformationService = Depends(get_service)
):
    return await service.query_virtualized_config(config_id, limit=limit, offset=offset)


# ==============================================================================
# TRANSFORM CONFIGS
# ==============================================================================

@router.get(
    "/transform/configs",
    response_model=List[TransformConfigResponse],
    summary="List transform configs",
    description="List all transform configs. These configs transform Bronze data to Silver Delta."
)
async def list_transform_configs(
    include_inactive: bool = Query(False, description="Include inactive configs"),
    service: SilverTransformationService = Depends(get_service)
):
    return await service.list_transform_configs(include_inactive=include_inactive)


@router.post(
    "/transform/configs",
    response_model=TransformConfigResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create transform config",
    description="""
Create a transform config to materialize Bronze data into Silver Delta Lake using **Apache Spark**.

Unlike VirtualizedConfig (which queries data on-demand via Trino), TransformConfig 
**persists** transformed data to Silver Delta Lake for efficient downstream consumption.

---

## **Virtualized vs Transform (Materialized):**

| Aspect | Virtualized | Transform |
|--------|-------------|-----------|
| **Engine** | Trino (SQL) | Spark (DataFrame) |
| **Output** | JSON (in memory) | Delta Lake (persisted) |
| **Use Case** | Exploration, APIs | Data pipelines, curated datasets |
| **Performance** | Fast for small queries | Optimized for large ETL |
| **Python UDFs** | ❌ Not supported | ✅ Supported |

---

## **Request Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | ✅ | Unique name for the config |
| `description` | string | ❌ | Optional description |
| `source_bronze_dataset_id` | int | ✅ | Bronze dataset ID (from GET /api/bronze/datasets) |
| `silver_bucket` | string | ❌ | Output bucket (defaults to system silver bucket) |
| `silver_path_prefix` | string | ❌ | Output path prefix |
| `column_group_ids` | int[] | ❌ | Semantic groups from Equivalence (column unification + value mappings) |
| `filter_ids` | int[] | ❌ | Filter IDs from POST /api/silver/filters |
| `column_transformations` | object[] | ❌ | **Same format as Virtualized** (uses `column_id`) |
| `exclude_unified_source_columns` | bool | ❌ | When `true`, excludes original source columns after semantic unification. Default: `false` |

---

## **column_transformations (same as Virtualized):**

Uses `column_id` (external_column.id) - automatically resolved to Bronze column name.

```json
"column_transformations": [
  {"column_id": 160, "type": "lowercase"},
  {"column_id": 161, "type": "template", "rule_id": 1}
]
```

| type | Description | Required Fields |
|------|-------------|-----------------|
| `template` | Use normalization rule (regex or Python) | `rule_id` |
| `lowercase` | Convert to lowercase | - |
| `uppercase` | Convert to uppercase | - |
| `trim` | Remove spaces | - |
| `normalize_spaces` | Collapse multiple spaces | - |
| `remove_accents` | Remove accents (á→a) | - |

**For regex or complex transformations:** Create a rule via `POST /api/silver/normalization-rules` first, then reference via `rule_id`.

---

## **Example 1: Minimal**
```json
{
  "name": "patients_silver",
  "source_bronze_dataset_id": 15
}
```

---

## **Example 2: With column_transformations (same as Virtualized)**
```json
{
  "name": "patients_normalized",
  "source_bronze_dataset_id": 15,
  "column_transformations": [
    {"column_id": 160, "type": "lowercase"},
    {"column_id": 161, "type": "trim"},
    {"column_id": 162, "type": "template", "rule_id": 1}
  ]
}
```

---

## **Example 3: With Semantic Equivalence**
```json
{
  "name": "unified_patients_silver",
  "source_bronze_dataset_id": 15,
  "column_group_ids": [1, 2]
}
```
*column_group_ids* loads from Equivalence module:
- **ColumnMappings**: unify columns (e.g., "sexo" + "gender" → unified column)
- **ValueMappings**: normalize values (e.g., "M"→"Masculino")

---

## **Example 4: With Filters**
```json
{
  "name": "active_patients_silver",
  "source_bronze_dataset_id": 15,
  "filter_ids": [1, 2]
}
```

---

## **Example 5: Complete (all features)**
```json
{
  "name": "full_silver_pipeline",
  "description": "Complete patient transformation with all features",
  "source_bronze_dataset_id": 15,
  "column_group_ids": [1],
  "filter_ids": [1],
  "column_transformations": [
    {"column_id": 160, "type": "uppercase"},
    {"column_id": 162, "type": "template", "rule_id": 1}
  ],
  "exclude_unified_source_columns": true
}
```
*column_group_ids* loads from Equivalence module both:
- **ColumnMappings**: unify columns under same name (COALESCE)
- **ValueMappings**: normalize values (M→Masculino, F→Feminino)

*exclude_unified_source_columns*: when `true`, only the unified column appears (e.g., only `sex_group` instead of `clinical_sex` + `sexo` + `sex_group`)

---

## **Workflow:**
1. **Create config** → POST /api/silver/transform/configs
2. **Preview** → POST /api/silver/transform/configs/{id}/preview
3. **Execute** → POST /api/silver/transform/configs/{id}/execute
4. **Query** → GET /api/silver/transform/configs/{id}/query

---

## **Notes:**
- Use GET /api/bronze/datasets to find `source_bronze_dataset_id`
- Use GET /api/metadata/tables/{id}/columns to find `column_id` for column_transformations
- Create rules first via POST /api/silver/normalization-rules for `template` type (regex, formatting)
- Create filters first via POST /api/silver/filters for `filter_ids`
- For semantic unification + value mappings, use /api/equivalence to create ColumnGroups
- After execute, the Silver table is auto-registered in Trino for SQL queries
"""
)
async def create_transform_config(
    data: TransformConfigCreate,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.create_transform_config(data)


@router.get(
    "/transform/configs/{config_id}",
    response_model=TransformConfigResponse,
    summary="Get transform config",
    description="Get details of a specific transform config."
)
async def get_transform_config(
    config_id: int,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.get_transform_config(config_id)


@router.put(
    "/transform/configs/{config_id}",
    response_model=TransformConfigResponse,
    summary="Update transform config",
    description="Update a transform config."
)
async def update_transform_config(
    config_id: int,
    data: TransformConfigUpdate,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.update_transform_config(config_id, data)


@router.delete(
    "/transform/configs/{config_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete transform config",
    description="Delete a transform config."
)
async def delete_transform_config(
    config_id: int,
    service: SilverTransformationService = Depends(get_service)
):
    await service.delete_transform_config(config_id)


@router.post(
    "/transform/configs/{config_id}/preview",
    response_model=TransformPreviewResponse,
    summary="Preview transform config",
    description="Preview the transformation plan without executing it."
)
async def preview_transform_config(
    config_id: int,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.preview_transform_config(config_id)


@router.post(
    "/transform/configs/{config_id}/execute",
    response_model=TransformExecuteResponse,
    summary="Execute transform (Bronze → Silver)",
    description="""
Execute a transform to materialize Bronze data to Silver Delta Lake using **Apache Spark**.

---

## **What Happens:**

| Step | Description |
|------|-------------|
| 1️⃣ | **Read Bronze**: Load data from Bronze Delta Lake via Spark |
| 2️⃣ | **Apply column_transformations**: Same as Virtualized (lowercase, uppercase, trim, etc.) |
| 3️⃣ | **Apply column_group_ids**: Semantic unification + value mappings from Equivalence |
| 4️⃣ | **Apply filter_ids**: Filter rows based on conditions |
| 5️⃣ | **Write Silver**: Persist to Silver Delta Lake with ACID transactions |
| 6️⃣ | **Register in Trino**: Auto-register table for SQL queries |

---

## **Supported Transformations:**

### column_transformations (same format as Virtualized)
Uses `column_id` (external_column.id), auto-resolved to bronze_column_name.

| Type | Description | Example |
|------|-------------|---------|
| `lowercase` | Convert to lowercase | `{"column_id": 10, "type": "lowercase"}` |
| `uppercase` | Convert to uppercase | `{"column_id": 10, "type": "uppercase"}` |
| `trim` | Remove leading/trailing spaces | `{"column_id": 10, "type": "trim"}` |
| `normalize_spaces` | Collapse multiple spaces | `{"column_id": 10, "type": "normalize_spaces"}` |
| `remove_accents` | Remove diacritics | `{"column_id": 10, "type": "remove_accents"}` |
| `template` | Apply normalization rule (regex or Python) | `{"column_id": 10, "type": "template", "rule_id": 1}` |

**For regex/complex transformations:** Create a rule via `POST /api/silver/normalization-rules` first, then use `type: "template"` with `rule_id`.

### column_group_ids (Semantic Unification + Value Mappings)
Loads from Equivalence module:
- **ColumnMappings**: Unify columns under same name (COALESCE)
- **ValueMappings**: Normalize values (M→Masculino, F→Feminino)

Use `/api/equivalence` to create ColumnGroups, ColumnMappings, and ValueMappings.

### filter_ids (Row Filtering)
Apply pre-created filters (POST /api/silver/filters).

---

## **Response:**

```json
{
  "config_id": 1,
  "config_name": "patients_silver",
  "execution_id": 42,
  "status": "success",
  "rows_processed": 50000,
  "rows_output": 48500,
  "output_path": "s3a://datafabric-silver/1-patients_silver/",
  "execution_time_seconds": 12.5,
  "message": "Silver transformation completed. 48500 rows written. Table registered in Trino as silver.default.1_patients_silver"
}
```

---

## **Why Spark (not Trino)?**

| Aspect | Spark | Trino |
|--------|-------|-------|
| **ETL Write** | ✅ Native Delta Lake writes | ❌ Limited write support |
| **ACID** | ✅ Full transaction support | ⚠️ Partial |
| **UDFs** | ✅ Python UDFs (normalization rules) | ❌ Limited |
| **Scale** | ✅ Optimized for large batch jobs | ✅ Optimized for queries |
| **Parallelism** | ✅ Partition-level parallelism | ⚠️ Query-level |

---

## **After Execution:**

The Silver table is automatically registered in Trino. You can:

1. **Query via API**: `GET /api/silver/transform/configs/{id}/query?limit=1000`
2. **Query via Trino SQL**: 
   ```sql
   SELECT * FROM silver.default.{config_id}_{config_name} LIMIT 100
   ```

---

## **Notes:**
- This is a **long-running operation** for large datasets
- Use `/preview` first to validate transformations
- Output overwrites previous execution (mode=overwrite)
- Check `/executions` endpoint for execution history
"""
)
async def execute_transform_config(
    config_id: int,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.execute_transform_config(config_id)


@router.get(
    "/transform/configs/{config_id}/table-info",
    summary="Get Silver table info",
    description="Get information about the Silver Delta table including schema and row count."
)
async def get_silver_table_info(
    config_id: int,
    service: SilverTransformationService = Depends(get_service)
):
    info = await service.get_silver_table_info(config_id)
    if not info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Silver table for config {config_id} not found. Execute the transform first."
        )
    return info


@router.get(
    "/transform/configs/{config_id}/query",
    summary="Query Silver table (after execute)",
    description="""
Query the materialized Silver Delta table via Trino.

---

## **Prerequisites:**

⚠️ **You must execute the transform first!** Use `POST /api/silver/transform/configs/{id}/execute`

---

## **How it Works:**

| Step | Description |
|------|-------------|
| 1️⃣ | After `/execute`, the Silver Delta table is created in S3 |
| 2️⃣ | The table is auto-registered in Trino catalog `silver.default.{id}_{name}` |
| 3️⃣ | This endpoint queries the registered table via Trino SQL |

---

## **Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int | 1000 | Maximum rows to return (1-100000) |
| `offset` | int | 0 | Rows to skip for pagination |

---

## **Response:**

```json
{
  "config_id": 1,
  "config_name": "patients_silver",
  "columns": ["id", "name", "cpf_normalizado", "gender_unified", "_silver_timestamp"],
  "data": [
    {"id": 1, "name": "João Silva", "cpf_normalizado": "123.456.789-00", "gender_unified": "Masculino", "_silver_timestamp": "2024-01-15T10:30:00"},
    ...
  ],
  "row_count": 1000,
  "total_rows": 50000,
  "execution_time_seconds": 0.5
}
```

---

## **Special Columns:**

Silver tables include metadata columns added during transformation:

| Column | Description |
|--------|-------------|
| `_silver_timestamp` | When the row was transformed |
| `_transform_config_id` | ID of the transform config |

---

## **Alternative: Direct Trino SQL:**

You can also query directly via Trino:
```sql
SELECT * FROM silver.default.{config_id}_{config_name} 
WHERE gender_unified = 'Masculino'
LIMIT 100
```

---

## **Errors:**

| Code | Description |
|------|-------------|
| 404 | Config not found |
| 400 | Transform not executed yet (no Silver table) |
| 500 | Query execution error |
"""
)
async def query_silver_table(
    config_id: int,
    limit: int = Query(1000, ge=1, le=100000, description="Maximum rows to return"),
    offset: int = Query(0, ge=0, description="Rows to skip"),
    service: SilverTransformationService = Depends(get_service)
):
    result = await service.query_silver_table(config_id, limit=limit, offset=offset)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Silver table for config {config_id} not found. Execute the transform first."
        )
    return result

