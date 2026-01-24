"""
Silver Layer API Routes

This module provides endpoints for:
- Normalization rules management
- Virtualized configs (sources → JSON) with inline filters
- Transform configs (Bronze → Silver Delta) with inline filters

NOTE: Semantic mappings are managed by the Equivalence module (/api/equivalence).
Use ColumnGroup and ColumnMapping from Equivalence for semantic unification.

NOTE: Filters are now defined inline within each config (not as separate entities).
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
    # Filter schemas (for documentation)
    FilterOperatorEnum,
    FilterLogicEnum,
    InlineFilter,
    FilterCondition,
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
    SilverWriteModeEnum,
    TransformExecutionResponse,
    # Versioning
    SilverVersionHistoryResponse,
    SilverDataQueryResponse,
)
from ...services.silver_transformation_service import SilverTransformationService
from ...services.silver_versioning_service import SilverVersioningService
from ...database.database import get_db
from ...database.datasets.silver import TransformConfig, TransformStatus
import logging

logger = logging.getLogger(__name__)

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


@router.get(
    "/filter-operators",
    summary="List available filter operators",
    description="Returns all available operators for inline filter conditions with examples."
)
async def list_filter_operators():
    """
    List all available filter operators for use in inline filters.
    """
    return {
        "operators": [
            {
                "operator": "=",
                "category": "comparison",
                "description": "Equal to",
                "example": {"column_id": 10, "operator": "=", "value": "active"}
            },
            {
                "operator": "!=",
                "category": "comparison",
                "description": "Not equal to",
                "example": {"column_id": 10, "operator": "!=", "value": "deleted"}
            },
            {
                "operator": ">",
                "category": "comparison",
                "description": "Greater than",
                "example": {"column_id": 15, "operator": ">", "value": 18}
            },
            {
                "operator": ">=",
                "category": "comparison",
                "description": "Greater than or equal to",
                "example": {"column_id": 15, "operator": ">=", "value": 18}
            },
            {
                "operator": "<",
                "category": "comparison",
                "description": "Less than",
                "example": {"column_id": 20, "operator": "<", "value": 100}
            },
            {
                "operator": "<=",
                "category": "comparison",
                "description": "Less than or equal to",
                "example": {"column_id": 20, "operator": "<=", "value": 100}
            },
            {
                "operator": "LIKE",
                "category": "pattern",
                "description": "Case-sensitive pattern match (use % as wildcard)",
                "example": {"column_id": 25, "operator": "LIKE", "value": "John%"}
            },
            {
                "operator": "ILIKE",
                "category": "pattern",
                "description": "Case-insensitive pattern match",
                "example": {"column_id": 30, "operator": "ILIKE", "value": "%@gmail.com"}
            },
            {
                "operator": "IN",
                "category": "set",
                "description": "Value is in a list",
                "example": {"column_id": 10, "operator": "IN", "value": ["active", "pending"]}
            },
            {
                "operator": "NOT IN",
                "category": "set",
                "description": "Value is not in a list",
                "example": {"column_id": 10, "operator": "NOT IN", "value": ["deleted", "archived"]}
            },
            {
                "operator": "IS NULL",
                "category": "null",
                "description": "Value is null",
                "example": {"column_id": 35, "operator": "IS NULL"}
            },
            {
                "operator": "IS NOT NULL",
                "category": "null",
                "description": "Value is not null",
                "example": {"column_id": 30, "operator": "IS NOT NULL"}
            },
            {
                "operator": "BETWEEN",
                "category": "range",
                "description": "Value is between min and max (inclusive)",
                "example": {"column_id": 20, "operator": "BETWEEN", "value_min": 10, "value_max": 100}
            }
        ],
        "logic_options": ["AND", "OR"],
        "example_filter": {
            "logic": "AND",
            "conditions": [
                {"column_id": 15, "operator": ">=", "value": 18},
                {"column_id": 10, "operator": "=", "value": "active"},
                {"column_id": 35, "operator": "IS NULL"}
            ]
        },
        "notes": [
            "Filters are defined inline within VirtualizedConfig and TransformConfig",
            "RECOMMENDED: Use column_id (external_column.id from metadata) - unambiguous across sources",
            "ALTERNATIVE: Use column_name for direct reference (may be ambiguous if columns have same name)",
            "Multiple conditions are combined using the specified logic (AND/OR)",
            "For BETWEEN, use value_min and value_max instead of value",
            "For IS NULL and IS NOT NULL, value is not required",
            "Get column_id via GET /api/metadata/tables/{table_id}/columns"
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
| `filters` | object | ❌ | Inline filter conditions (see below) |
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

## **filters Structure (inline):**

Filters are defined directly in the config, not as separate entities.

**RECOMMENDED:** Use `column_id` (from GET /api/metadata/tables/{table_id}/columns) for unambiguous filtering.

**ALTERNATIVE:** Use `column_name` for direct reference (may be ambiguous if columns have same name across sources).

```json
"filters": {
  "logic": "AND",
  "conditions": [
    {"column_id": 15, "operator": ">=", "value": 18},
    {"column_id": 10, "operator": "=", "value": "active"},
    {"column_id": 35, "operator": "IS NULL"}
  ]
}
```

**Operators:** `=`, `!=`, `>`, `>=`, `<`, `<=`, `LIKE`, `ILIKE`, `IN`, `NOT IN`, `IS NULL`, `IS NOT NULL`, `BETWEEN`

See GET /api/silver/filter-operators for full documentation.

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

## **Example 2: With Inline Filters**
```json
{
  "name": "active_adult_patients",
  "tables": [{"table_id": 14, "select_all": true}],
  "filters": {
    "logic": "AND",
    "conditions": [
      {"column_id": 15, "operator": ">=", "value": 18},
      {"column_id": 10, "operator": "=", "value": "active"}
    ]
  }
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

## **Example 4: With Column Transformations**
```json
{
  "name": "normalized_patients",
  "tables": [{"table_id": 14, "select_all": true}],
  "column_transformations": [
    {"column_id": 160, "type": "uppercase"},
    {"column_id": 161, "type": "trim"},
    {"column_id": 162, "type": "template", "rule_id": 1}
  ]
}
```

---

## **Example 5: Complete (all features)**
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
  "filters": {
    "logic": "AND",
    "conditions": [
      {"column_id": 51, "operator": ">=", "value": 18},
      {"column_id": 50, "operator": "IN", "value": ["active", "pending"]}
    ]
  },
  "column_transformations": [
    {"column_id": 162, "type": "template", "rule_id": 1},
    {"column_id": 160, "type": "uppercase"}
  ],
  "exclude_unified_source_columns": true
}
```

---

## **Notes:**
- Use GET /api/metadata/tables to find `table_id`
- Use GET /api/metadata/tables/{id}/columns to find `column_id` (RECOMMENDED for filters)
- Create rules first via POST /api/silver/normalization-rules for `template` type
- Filters: prefer `column_id` over `column_name` for unambiguous references
- For value mappings (M→Masculino), use /api/equivalence (not column_transformations)
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
    "/persistent/configs",
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
    "/persistent/configs",
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
| `filters` | object | ❌ | Inline filter conditions (same structure as Virtualized) |
| `column_transformations` | object[] | ❌ | **Same format as Virtualized** (uses `column_id`) |
| `exclude_unified_source_columns` | bool | ❌ | When `true`, excludes original source columns after semantic unification. Default: `false` |

---

## **filters Structure (inline):**

Filters are defined directly in the config, not as separate entities.

**RECOMMENDED:** Use `column_id` (from GET /api/metadata/tables/{table_id}/columns) for unambiguous filtering.

**ALTERNATIVE:** Use `column_name` for direct reference (may be ambiguous if columns have same name across sources).

```json
"filters": {
  "logic": "AND",
  "conditions": [
    {"column_id": 15, "operator": ">=", "value": 18},
    {"column_id": 10, "operator": "=", "value": "active"}
  ]
}
```

**Operators:** `=`, `!=`, `>`, `>=`, `<`, `<=`, `LIKE`, `ILIKE`, `IN`, `NOT IN`, `IS NULL`, `IS NOT NULL`, `BETWEEN`

See GET /api/silver/filter-operators for full documentation.

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

---

## **Example 1: Minimal**
```json
{
  "name": "patients_silver",
  "source_bronze_dataset_id": 15
}
```

---

## **Example 2: With Inline Filters**
```json
{
  "name": "active_patients_silver",
  "source_bronze_dataset_id": 15,
  "filters": {
    "logic": "AND",
    "conditions": [
      {"column_id": 10, "operator": "=", "value": "active"},
      {"column_id": 35, "operator": "IS NULL"}
    ]
  }
}
```

---

## **Example 3: With column_transformations**
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

## **Example 4: With Semantic Equivalence**
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

## **Example 5: Complete (all features)**
```json
{
  "name": "full_silver_pipeline",
  "description": "Complete patient transformation with all features",
  "source_bronze_dataset_id": 15,
  "column_group_ids": [1],
  "filters": {
    "logic": "AND",
    "conditions": [
      {"column_id": 51, "operator": ">=", "value": 18},
      {"column_id": 50, "operator": "IN", "value": ["active", "pending"]}
    ]
  },
  "column_transformations": [
    {"column_id": 160, "type": "uppercase"},
    {"column_id": 162, "type": "template", "rule_id": 1}
  ],
  "exclude_unified_source_columns": true
}
```

---

## **Workflow:**
1. **Create config** → POST /api/silver/persistent/configs
2. **Preview** → POST /api/silver/persistent/configs/{id}/preview
3. **Execute** → POST /api/silver/persistent/configs/{id}/execute
4. **Query** → GET /api/silver/persistent/configs/{id}/query

---

## **Notes:**
- Use GET /api/bronze/datasets to find `source_bronze_dataset_id`
- Use GET /api/metadata/tables/{id}/columns to find `column_id` for column_transformations
- Create rules first via POST /api/silver/normalization-rules for `template` type
- Filters are now inline (not separate entities)
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
    "/persistent/configs/{config_id}",
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
    "/persistent/configs/{config_id}",
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
    "/persistent/configs/{config_id}",
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
    "/persistent/configs/{config_id}/preview",
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
    "/persistent/configs/{config_id}/execute",
    response_model=TransformExecuteResponse,
    summary="Execute transform (Bronze → Silver)",
    description="""
Execute a transform to materialize Bronze data to Silver Delta Lake using **Apache Spark**.

---

## **What Happens:**

| Step | Description |
|------|-------------|
| 1️⃣ | **Read Bronze**: Load data from Bronze Delta Lake via Spark |
| 2️⃣ | **Apply filters**: Filter rows based on inline conditions |
| 3️⃣ | **Apply column_transformations**: Same as Virtualized (lowercase, uppercase, trim, etc.) |
| 4️⃣ | **Apply column_group_ids**: Semantic unification + value mappings from Equivalence |
| 5️⃣ | **Write Silver**: Persist to Silver Delta Lake with ACID transactions |
| 6️⃣ | **Register in Trino**: Auto-register table for SQL queries |

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

## **After Execution:**

The Silver table is automatically registered in Trino. You can:

1. **Query via API**: `GET /api/silver/persistent/configs/{id}/query?limit=1000`
2. **Query via Trino SQL**: 
   ```sql
   SELECT * FROM silver.default.{config_id}_{config_name} LIMIT 100
   ```

---

## **Notes:**
- This is a **long-running operation** for large datasets
- Use `/preview` first to validate transformations
- Output overwrites previous execution (mode=overwrite)
"""
)
async def execute_transform_config(
    config_id: int,
    service: SilverTransformationService = Depends(get_service)
):
    return await service.execute_transform_config(config_id)


@router.get(
    "/persistent/configs/{config_id}/table-info",
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
    "/persistent/configs/{config_id}/query",
    summary="Query Silver table (after execute)",
    description="""
Query the materialized Silver Delta table via Trino.

---

## **Prerequisites:**

⚠️ **You must execute the transform first!** Use `POST /api/silver/persistent/configs/{id}/execute`

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

## **Alternative: Direct Trino SQL:**

You can also query directly via Trino:
```sql
SELECT * FROM silver.default.{config_id}_{config_name} 
WHERE gender_unified = 'Masculino'
LIMIT 100
```
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


# ==================== VERSIONING & TIME TRAVEL ====================

@router.get(
    "/persistent/configs/{config_id}/versions",
    response_model=SilverVersionHistoryResponse,
    summary="Get Delta Lake version history",
    description="""
Get the version history of a Silver Delta Lake table.

Each execution creates a new version. You can use version numbers
for time travel queries.

---

## **Response:**

```json
{
  "config_id": 1,
  "config_name": "patients_silver",
  "current_version": 5,
  "output_path": "s3a://datafabric-silver/1-patients_silver/",
  "versions": [
    {
      "version": 5,
      "timestamp": "2024-01-20T14:30:00Z",
      "operation": "MERGE",
      "rows_inserted": 150,
      "rows_updated": 23,
      "total_rows": 10523
    },
    {
      "version": 4,
      "timestamp": "2024-01-15T10:00:00Z",
      "operation": "MERGE",
      "rows_inserted": 500,
      "rows_updated": 0,
      "total_rows": 10350
    }
  ]
}
```

---

## **Use Cases:**
- See all versions available for time travel
- Track changes over time
- Audit data modifications
"""
)
async def get_silver_version_history(
    config_id: int,
    limit: int = Query(100, ge=1, le=1000, description="Maximum versions to return"),
    db: AsyncSession = Depends(get_db),
):
    """Get Delta Lake version history for a Silver config."""
    from sqlalchemy.future import select
    
    try:
        # Get config
        result = await db.execute(
            select(TransformConfig).where(
                TransformConfig.id == config_id
            )
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Transform config {config_id} not found"
            )
        
        # Build output path
        silver_bucket = config.silver_bucket or "datafabric-silver"
        if config.silver_path_prefix:
            output_path = f"s3a://{silver_bucket}/{config.silver_path_prefix}"
        else:
            output_path = f"s3a://{silver_bucket}/{config.id}-{config.name}"
        
        # Get version history using Spark
        from ...services.spark_manager import SparkManager
        spark_manager = SparkManager()
        
        versioning_service = SilverVersioningService(db)
        
        with spark_manager.session_scope() as spark:
            history = await versioning_service.get_version_history(
                spark=spark,
                output_path=output_path,
                config_id=config_id,
                config_name=config.name,
                limit=limit
            )
        
        return history
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get version history: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get version history: {str(e)}"
        )


@router.get(
    "/persistent/configs/{config_id}/data",
    response_model=SilverDataQueryResponse,
    summary="Query Silver data with time travel",
    description="""
Query the Silver Delta Lake table with optional time travel.

You can query:
- **Latest version** (default): No parameters needed
- **Specific version**: Use `version` parameter
- **Point in time**: Use `as_of_timestamp` parameter

---

## **Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `version` | int | Query specific version (0, 1, 2, ...) |
| `as_of_timestamp` | string | Query as of timestamp (ISO format) |
| `limit` | int | Maximum rows to return (default: 1000) |
| `offset` | int | Rows to skip for pagination |

---

## **Examples:**

```bash
# Latest data
GET /api/silver/persistent/configs/1/data

# Specific version
GET /api/silver/persistent/configs/1/data?version=2

# Point in time
GET /api/silver/persistent/configs/1/data?as_of_timestamp=2024-01-15T10:00:00Z

# With pagination
GET /api/silver/persistent/configs/1/data?version=2&limit=100&offset=500
```

---

## **Response:**

```json
{
  "config_id": 1,
  "config_name": "patients_silver",
  "version": 2,
  "columns": ["patient_id", "name", "cpf_normalizado", "gender_unified", "_silver_timestamp"],
  "data": [
    {"patient_id": 1, "name": "João Silva", "cpf_normalizado": "123.456.789-00", ...},
    ...
  ],
  "row_count": 100,
  "total_rows": 10350,
  "execution_time_seconds": 0.5
}
```
"""
)
async def query_silver_data_with_time_travel(
    config_id: int,
    version: Optional[int] = Query(None, description="Query specific Delta version"),
    as_of_timestamp: Optional[str] = Query(None, description="Query as of timestamp (ISO format)"),
    limit: int = Query(1000, ge=1, le=100000, description="Maximum rows to return"),
    offset: int = Query(0, ge=0, description="Rows to skip"),
    db: AsyncSession = Depends(get_db),
):
    """Query Silver data with optional time travel."""
    import time
    from sqlalchemy.future import select
    
    start_time = time.time()
    
    try:
        # Get config
        result = await db.execute(
            select(TransformConfig).where(
                TransformConfig.id == config_id
            )
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Transform config {config_id} not found"
            )
        
        # Check if config has been executed
        if not config.last_execution_status or config.last_execution_status == TransformStatus.FAILED:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Config has not been successfully executed yet. Run execute first."
            )
        
        # Build output path
        silver_bucket = config.silver_bucket or "datafabric-silver"
        if config.silver_path_prefix:
            output_path = f"s3a://{silver_bucket}/{config.silver_path_prefix}"
        else:
            output_path = f"s3a://{silver_bucket}/{config.id}-{config.name}"
        
        # Query using Spark
        from ...services.spark_manager import SparkManager
        spark_manager = SparkManager()
        
        versioning_service = SilverVersioningService(db)
        
        with spark_manager.session_scope() as spark:
            columns, data, total_rows = await versioning_service.query_at_version(
                spark=spark,
                output_path=output_path,
                version=version,
                timestamp=as_of_timestamp,
                limit=limit,
                offset=offset
            )
        
        execution_time = time.time() - start_time
        
        # Parse timestamp if provided
        parsed_timestamp = None
        if as_of_timestamp:
            try:
                from datetime import datetime
                parsed_timestamp = datetime.fromisoformat(as_of_timestamp.replace('Z', '+00:00'))
            except:
                pass
        
        return SilverDataQueryResponse(
            config_id=config_id,
            config_name=config.name,
            version=version,
            as_of_timestamp=parsed_timestamp,
            columns=columns,
            data=data,
            row_count=len(data),
            total_rows=total_rows,
            execution_time_seconds=round(execution_time, 3)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to query Silver data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to query Silver data: {str(e)}"
        )
