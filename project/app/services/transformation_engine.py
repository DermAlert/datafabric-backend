"""
Transformation Engine - Shared transformation logic for Silver layer.

This module provides shared transformation functionality used by both:
- VirtualizedConfig (Trino SQL generation)
- TransformConfig (Spark DataFrame operations)

The module handles:
- Transformation validation
- SQL generation for Trino (Virtualized)
- Transformation config normalization

NOTE: Spark operations are NOT in this module to keep clear separation.
      Spark transformations remain in silver_transformation_service.py
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


# ==============================================================================
# TRANSFORMATION TYPES
# ==============================================================================

class TransformationType(str, Enum):
    """Available transformation types."""
    # Text transformations (SQL-compatible)
    LOWERCASE = "lowercase"
    UPPERCASE = "uppercase"
    TRIM = "trim"
    NORMALIZE_SPACES = "normalize_spaces"
    REMOVE_ACCENTS = "remove_accents"
    
    # Rule-based transformations
    TEMPLATE = "template"
    PYTHON_RULE = "python_rule"  # Transform only (Spark UDF)
    SQL_REGEX = "sql_regex"


# SQL-compatible transformations (can be used in both Trino and Spark)
SQL_COMPATIBLE_TYPES = {
    TransformationType.LOWERCASE,
    TransformationType.UPPERCASE,
    TransformationType.TRIM,
    TransformationType.NORMALIZE_SPACES,
    TransformationType.REMOVE_ACCENTS,
    TransformationType.TEMPLATE,  # If rule has regex
    TransformationType.SQL_REGEX,
}

# Transformations that require Spark UDF (Transform only)
SPARK_ONLY_TYPES = {
    TransformationType.PYTHON_RULE,
    TransformationType.TEMPLATE,  # If rule uses Python normalizer
}


# ==============================================================================
# TRANSFORMATION VALIDATION
# ==============================================================================

class TransformationValidator:
    """Validates transformation configurations."""
    
    @staticmethod
    def validate_column_transformation(
        transformation: Dict[str, Any],
        available_columns: Optional[List[int]] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate a column_transformation entry.
        
        Args:
            transformation: {"column_id": 10, "type": "lowercase", "rule_id": 1}
            available_columns: List of valid column IDs (optional)
            
        Returns:
            (is_valid, error_message)
        """
        if not transformation:
            return False, "Transformation is empty"
        
        column_id = transformation.get('column_id')
        trans_type = transformation.get('type')
        
        if column_id is None:
            return False, "Missing column_id"
        
        if not trans_type:
            return False, "Missing type"
        
        # Validate type
        valid_types = [t.value for t in TransformationType]
        if trans_type not in valid_types:
            return False, f"Invalid type '{trans_type}'. Valid: {valid_types}"
        
        # Validate column_id exists
        if available_columns is not None and column_id not in available_columns:
            return False, f"Column ID {column_id} not found in available columns"
        
        # Validate rule_id for template type
        if trans_type == 'template' and not transformation.get('rule_id'):
            return False, "type='template' requires rule_id"
        
        return True, None
    
    @staticmethod
    def validate_value_mapping(
        mapping: Dict[str, Any],
        available_columns: Optional[List[str]] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate a value_mapping entry.
        
        Args:
            mapping: {"column_name": "gender", "mappings": {"M": "Male"}, "default_value": ""}
            available_columns: List of valid column names (optional)
            
        Returns:
            (is_valid, error_message)
        """
        if not mapping:
            return False, "Mapping is empty"
        
        column_name = mapping.get('column_name')
        mappings = mapping.get('mappings')
        
        if not column_name:
            return False, "Missing column_name"
        
        if not mappings or not isinstance(mappings, dict):
            return False, "Missing or invalid 'mappings' (must be object)"
        
        if available_columns is not None and column_name not in available_columns:
            return False, f"Column '{column_name}' not found in available columns"
        
        return True, None


# ==============================================================================
# SQL GENERATION FOR TRINO (VIRTUALIZED)
# ==============================================================================

class TrinoSQLGenerator:
    """
    Generates Trino SQL expressions for transformations.
    
    Used by VirtualizedConfig to build SQL queries.
    """
    
    # Accent mapping for remove_accents (Trino translate function)
    ACCENT_FROM = "áàâãäéèêëíìîïóòôõöúùûüñçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÑÇ"
    ACCENT_TO = "aaaaaeeeeiiiiooooouuuuncAAAAAEEEEIIIIOOOOOUUUUNC"
    
    @classmethod
    def generate_transformation_sql(
        cls,
        column_expr: str,
        transformation_type: str,
        rule: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Generate SQL expression for a transformation.
        
        Args:
            column_expr: Column expression (e.g., "t1.column_name")
            transformation_type: Type of transformation
            rule: Normalization rule data (for template type)
            
        Returns:
            SQL expression string
        """
        if transformation_type == 'lowercase':
            return f"LOWER({column_expr})"
        
        elif transformation_type == 'uppercase':
            return f"UPPER({column_expr})"
        
        elif transformation_type == 'trim':
            return f"TRIM({column_expr})"
        
        elif transformation_type == 'normalize_spaces':
            return f"TRIM(regexp_replace({column_expr}, '\\s+', ' '))"
        
        elif transformation_type == 'remove_accents':
            return f"translate({column_expr}, '{cls.ACCENT_FROM}', '{cls.ACCENT_TO}')"
        
        elif transformation_type == 'template' and rule:
            if rule.get('regex_pattern') and rule.get('regex_replacement'):
                pattern = rule['regex_pattern'].replace("'", "''")
                replacement = rule['regex_replacement'].replace("'", "''")
                return f"regexp_replace({column_expr}, '{pattern}', '{replacement}')"
            else:
                # Template without regex - return as-is (Python rule not supported in SQL)
                logger.warning(f"Template rule without regex pattern - returning column as-is")
                return column_expr
        
        elif transformation_type == 'sql_regex':
            # sql_regex should have pattern in transformation config, not rule
            # This is handled differently - caller should pass pattern/replacement
            return column_expr
        
        else:
            # Unknown type - return as-is
            logger.warning(f"Unknown transformation type '{transformation_type}' - returning column as-is")
            return column_expr
    
    @classmethod
    def generate_value_mapping_sql(
        cls,
        column_expr: str,
        mappings: Dict[str, str],
        default_value: Optional[str] = None
    ) -> str:
        """
        Generate SQL CASE expression for value mapping.
        
        Args:
            column_expr: Column expression
            mappings: {"from_value": "to_value", ...}
            default_value: Value if no match (uses original if None)
            
        Returns:
            SQL CASE expression
        """
        if not mappings:
            return column_expr
        
        cases = []
        for from_val, to_val in mappings.items():
            from_escaped = from_val.replace("'", "''")
            to_escaped = to_val.replace("'", "''")
            cases.append(f"WHEN {column_expr} = '{from_escaped}' THEN '{to_escaped}'")
        
        case_expr = " ".join(cases)
        
        if default_value is not None:
            default_escaped = default_value.replace("'", "''")
            return f"CASE {case_expr} ELSE '{default_escaped}' END"
        else:
            return f"CASE {case_expr} ELSE {column_expr} END"
    
    @classmethod
    def generate_filter_sql(
        cls,
        conditions: List[Dict[str, Any]],
        logic: str = 'AND'
    ) -> Optional[str]:
        """
        Generate SQL WHERE clause from filter conditions.
        
        Args:
            conditions: List of filter conditions
            logic: 'AND' or 'OR'
            
        Returns:
            SQL WHERE clause (without WHERE keyword) or None
        """
        if not conditions:
            return None
        
        sql_conditions = []
        
        for cond in conditions:
            column_name = cond.get('column_name')
            operator = cond.get('operator')
            value = cond.get('value')
            value_min = cond.get('value_min')
            value_max = cond.get('value_max')
            
            if not column_name or not operator:
                continue
            
            # Escape column name
            col_expr = f'"{column_name}"'
            
            if operator == 'IS NULL':
                sql_conditions.append(f"{col_expr} IS NULL")
            elif operator == 'IS NOT NULL':
                sql_conditions.append(f"{col_expr} IS NOT NULL")
            elif operator == 'BETWEEN' and value_min is not None and value_max is not None:
                sql_conditions.append(f"{col_expr} BETWEEN {cls._escape_value(value_min)} AND {cls._escape_value(value_max)}")
            elif operator in ('IN', 'NOT IN') and isinstance(value, list):
                values_str = ", ".join(cls._escape_value(v) for v in value)
                sql_conditions.append(f"{col_expr} {operator} ({values_str})")
            elif operator in ('LIKE', 'NOT LIKE', 'ILIKE'):
                sql_conditions.append(f"{col_expr} {operator} {cls._escape_value(value)}")
            elif operator in ('=', '!=', '<>', '>', '<', '>=', '<='):
                sql_conditions.append(f"{col_expr} {operator} {cls._escape_value(value)}")
        
        if not sql_conditions:
            return None
        
        joiner = f" {logic} "
        return joiner.join(sql_conditions)
    
    @staticmethod
    def _escape_value(value: Any) -> str:
        """Escape a value for SQL."""
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"


# ==============================================================================
# TRANSFORMATION CONFIG NORMALIZER
# ==============================================================================

class TransformationConfigNormalizer:
    """
    Normalizes transformation configurations.
    
    Handles conversion: column_transformations (column_id based) → bronze column name
    """
    
    @staticmethod
    def resolve_column_transformations(
        transformations: List[Dict[str, Any]],
        column_id_to_name: Dict[int, str]
    ) -> List[Dict[str, Any]]:
        """
        Resolve column_transformations column_id to column_name.
        
        Args:
            transformations: List of {"column_id": 10, "type": "lowercase", ...}
            column_id_to_name: Mapping of column_id → column_name
            
        Returns:
            List with column_name added to each transformation
        """
        result = []
        
        for trans in transformations:
            column_id = trans.get('column_id')
            if column_id is None:
                continue
            
            column_name = column_id_to_name.get(column_id)
            if not column_name:
                logger.warning(f"Column ID {column_id} not found in mapping - skipping")
                continue
            
            resolved = {
                **trans,
                'column_name': column_name
            }
            result.append(resolved)
        
        return result


# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def is_sql_compatible(transformation_type: str) -> bool:
    """Check if a transformation type can be expressed in pure SQL."""
    try:
        t = TransformationType(transformation_type)
        return t in SQL_COMPATIBLE_TYPES
    except ValueError:
        return False


def requires_spark_udf(transformation_type: str) -> bool:
    """Check if a transformation type requires Spark UDF."""
    try:
        t = TransformationType(transformation_type)
        return t in SPARK_ONLY_TYPES
    except ValueError:
        return False


def get_supported_transformation_types() -> List[str]:
    """Get list of all supported transformation types."""
    return [t.value for t in TransformationType]

