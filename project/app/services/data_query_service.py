"""
Data Query Service - Generic filtering and sorting for Delta Lake DataFrames

Applies column filters and sorting to PySpark DataFrames before pagination.
This enables Excel-like filtering for both Bronze and Silver data endpoints.

Filters are pushed down to Delta Lake via PySpark, leveraging Delta's
data skipping and predicate pushdown for efficient queries on large datasets.

Date handling:
- Columns with TimestampType/DateType work natively with comparison operators
- String columns containing dates (dd/mm/yyyy, ISO, etc.) are auto-detected
  and parsed to proper dates before comparison, so filters work correctly
"""

import re
import logging
from typing import List, Optional, Tuple, Dict
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Column
from pyspark.sql.types import (
    StringType,
    TimestampType,
    TimestampNTZType,
    DateType,
)

from ..api.schemas.data_query_schemas import (
    ColumnFilter,
    ColumnFilterOperator,
    ColumnFilterLogic,
    SortOrder,
)

logger = logging.getLogger(__name__)

# Operators that involve ordering/range comparison (need date-aware handling)
_COMPARISON_OPS = {
    ColumnFilterOperator.gt,
    ColumnFilterOperator.gte,
    ColumnFilterOperator.lt,
    ColumnFilterOperator.lte,
    ColumnFilterOperator.between,
    ColumnFilterOperator.eq,
    ColumnFilterOperator.neq,
}

# Date format patterns: (regex, parse_mode, label)
# parse_mode is either a PySpark format string or "ISO_DATETIME_SUBSTR" for special handling
_DATE_PATTERNS: List[Tuple[re.Pattern, str, str]] = [
    # dd/mm/yyyy or dd-mm-yyyy (BR standard)
    (re.compile(r"^\d{2}[/\-]\d{2}[/\-]\d{4}$"), "dd/MM/yyyy", "dd/mm/yyyy"),
    # yyyy-mm-dd (ISO date only)
    (re.compile(r"^\d{4}-\d{2}-\d{2}$"), "yyyy-MM-dd", "yyyy-mm-dd"),
    # yyyy-mm-dd HH:MM:SS or yyyy-mm-ddTHH:MM:SS (ISO datetime with optional
    # fractional seconds, timezone, etc - we just extract the date portion)
    (re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}"), "ISO_DATETIME_SUBSTR", "ISO datetime"),
    # dd/mm/yy
    (re.compile(r"^\d{2}[/\-]\d{2}[/\-]\d{2}$"), "dd/MM/yy", "dd/mm/yy"),
]

# Cache: column_name -> detected spark format string (within a single query)
_format_cache: Dict[str, Optional[str]] = {}


def _detect_date_format_from_samples(df: DataFrame, col_name: str) -> Optional[str]:
    """
    Detect the date format of a string column by sampling non-null values.
    
    Samples up to 20 non-null values and matches against known patterns.
    If >50% of samples match a pattern, returns the PySpark format string.
    
    For dd/mm/yyyy vs mm/dd/yyyy ambiguity:
    - If any value has the first segment > 12, it's dd/mm (day-first)
    - Default: dd/mm/yyyy (Brazilian/European standard)
    
    Returns:
        PySpark date format string (e.g. "dd/MM/yyyy") or None if not a date column
    """
    try:
        samples = (
            df.select(col_name)
            .where(F.col(col_name).isNotNull() & (F.trim(F.col(col_name)) != ""))
            .limit(20)
            .collect()
        )
        
        if not samples:
            return None
        
        values = [str(row[0]).strip() for row in samples if row[0] is not None]
        if not values:
            return None
        
        for pattern, spark_fmt, label in _DATE_PATTERNS:
            matches = sum(1 for v in values if pattern.match(v))
            if matches / len(values) >= 0.5:
                logger.info(
                    f"Column '{col_name}': detected date format '{label}' "
                    f"({matches}/{len(values)} samples matched)"
                )
                
                # For dd/mm vs mm/dd disambiguation on slash/dash patterns
                if spark_fmt in ("dd/MM/yyyy", "dd/MM/yy"):
                    first_segments = []
                    for v in values:
                        parts = re.split(r"[/\-]", v)
                        if parts:
                            try:
                                first_segments.append(int(parts[0]))
                            except ValueError:
                                pass
                    
                    # If all first segments are <= 12, it's ambiguous
                    # but we default to dd/mm (BR standard)
                    # If any first segment > 12, it confirms dd/mm
                    has_day_gt_12 = any(s > 12 for s in first_segments)
                    if has_day_gt_12:
                        logger.info(f"Column '{col_name}': confirmed day-first (dd/mm) format")
                    else:
                        logger.info(
                            f"Column '{col_name}': ambiguous dd/mm vs mm/dd, "
                            f"defaulting to dd/mm (BR standard)"
                        )
                
                # Normalize separator for the format string
                if spark_fmt == "dd/MM/yyyy" and "-" in values[0]:
                    spark_fmt = "dd-MM-yyyy"
                elif spark_fmt == "dd/MM/yy" and "-" in values[0]:
                    spark_fmt = "dd-MM-yy"
                
                return spark_fmt
        
        return None
    
    except Exception as e:
        logger.warning(f"Failed to detect date format for column '{col_name}': {e}")
        return None


def _get_date_parsed_col(df: DataFrame, col_name: str) -> Tuple[Column, bool]:
    """
    Get a column expression suitable for date comparison.
    
    - If the column is already DateType/TimestampType, casts to date for comparison
    - If it's a StringType with detected date format, returns to_date() expression
    - For ISO datetime strings (with timezone, microseconds, etc.), extracts
      the first 10 chars (yyyy-mm-dd) and parses that
    - Otherwise returns the raw column (string comparison fallback)
    
    Returns:
        Tuple of (column_expression, is_date_parsed)
    """
    schema_field = df.schema[col_name]
    col_type = schema_field.dataType
    
    # Already a proper date type - return as-is
    if isinstance(col_type, DateType):
        return F.col(col_name), True
    
    # Timestamp type - cast to date for clean date comparison
    if isinstance(col_type, (TimestampType, TimestampNTZType)):
        return F.to_date(F.col(col_name)), True
    
    # String type - try to detect date format
    if isinstance(col_type, StringType):
        # Check cache first
        cache_key = f"{id(df)}:{col_name}"
        if cache_key in _format_cache:
            fmt = _format_cache[cache_key]
        else:
            fmt = _detect_date_format_from_samples(df, col_name)
            _format_cache[cache_key] = fmt
        
        if fmt:
            if fmt == "ISO_DATETIME_SUBSTR":
                # For complex ISO datetimes (with timezone, microseconds, etc.),
                # extract the date portion (first 10 chars: "yyyy-mm-dd")
                return F.to_date(F.substring(F.col(col_name), 1, 10), "yyyy-MM-dd"), True
            else:
                return F.to_date(F.col(col_name), fmt), True
    
    # Not a date column
    return F.col(col_name), False


def _parse_filter_value_as_date(value: str) -> Column:
    """
    Parse a filter value string to a date literal.
    
    The frontend should always send dates in ISO format (yyyy-mm-dd).
    This function also accepts dd/mm/yyyy for convenience.
    
    Returns:
        PySpark date literal Column
    """
    val = str(value).strip()
    
    # ISO format: yyyy-mm-dd
    if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
        return F.to_date(F.lit(val), "yyyy-MM-dd")
    
    # ISO datetime: yyyy-mm-ddTHH:MM:SS...
    if re.match(r"^\d{4}-\d{2}-\d{2}[T ]", val):
        # Truncate to date portion
        date_part = val[:10]
        return F.to_date(F.lit(date_part), "yyyy-MM-dd")
    
    # BR format: dd/mm/yyyy
    if re.match(r"^\d{2}/\d{2}/\d{4}$", val):
        return F.to_date(F.lit(val), "dd/MM/yyyy")
    
    # BR format with dash: dd-mm-yyyy
    if re.match(r"^\d{2}-\d{2}-\d{4}$", val):
        return F.to_date(F.lit(val), "dd-MM-yyyy")
    
    # Fallback: let Spark try to parse it
    return F.to_date(F.lit(val))


def build_filter_condition(df: DataFrame, filter_: ColumnFilter) -> Column:
    """
    Build a PySpark Column condition from a single ColumnFilter.
    
    Smart date handling:
    - For comparison operators (gt, gte, lt, lte, between, eq, neq) on
      string columns that contain dates, auto-detects the format and
      parses to proper date before comparing.
    - Accepts filter values in ISO format (yyyy-mm-dd) or dd/mm/yyyy.
    
    String operations (contains, starts_with, etc.) are case-insensitive.
    
    Args:
        df: DataFrame (used to validate column existence and detect types)
        filter_: ColumnFilter with column, operator, and value
        
    Returns:
        PySpark Column condition
        
    Raises:
        ValueError: If column doesn't exist or operator/value is invalid
    """
    col_name = filter_.column
    op = filter_.operator
    value = filter_.value
    
    if col_name not in df.columns:
        raise ValueError(f"Column '{col_name}' not found. Available columns: {df.columns}")
    
    # For comparison operators, use date-aware column if applicable
    if op in _COMPARISON_OPS:
        parsed_col, is_date = _get_date_parsed_col(df, col_name)
    else:
        parsed_col = F.col(col_name)
        is_date = False
    
    # Helper: parse a single filter value for date comparison
    def _val(v):
        if is_date and isinstance(v, str):
            return _parse_filter_value_as_date(v)
        return v
    
    if op == ColumnFilterOperator.eq:
        return parsed_col == _val(value)
    
    elif op == ColumnFilterOperator.neq:
        return parsed_col != _val(value)
    
    elif op == ColumnFilterOperator.contains:
        if value is None:
            raise ValueError("'contains' operator requires a value")
        return F.lower(F.col(col_name).cast("string")).contains(str(value).lower())
    
    elif op == ColumnFilterOperator.not_contains:
        if value is None:
            raise ValueError("'not_contains' operator requires a value")
        return ~F.lower(F.col(col_name).cast("string")).contains(str(value).lower())
    
    elif op == ColumnFilterOperator.starts_with:
        if value is None:
            raise ValueError("'starts_with' operator requires a value")
        return F.lower(F.col(col_name).cast("string")).startswith(str(value).lower())
    
    elif op == ColumnFilterOperator.ends_with:
        if value is None:
            raise ValueError("'ends_with' operator requires a value")
        return F.lower(F.col(col_name).cast("string")).endswith(str(value).lower())
    
    elif op == ColumnFilterOperator.gt:
        return parsed_col > _val(value)
    
    elif op == ColumnFilterOperator.gte:
        return parsed_col >= _val(value)
    
    elif op == ColumnFilterOperator.lt:
        return parsed_col < _val(value)
    
    elif op == ColumnFilterOperator.lte:
        return parsed_col <= _val(value)
    
    elif op == ColumnFilterOperator.is_null:
        return F.col(col_name).isNull()
    
    elif op == ColumnFilterOperator.is_not_null:
        return F.col(col_name).isNotNull()
    
    elif op == ColumnFilterOperator.between:
        if not isinstance(value, list) or len(value) != 2:
            raise ValueError("'between' operator requires a list of exactly 2 values [min, max]")
        return (parsed_col >= _val(value[0])) & (parsed_col <= _val(value[1]))
    
    elif op == ColumnFilterOperator.in_:
        if value is None:
            raise ValueError("'in' operator requires a list of values")
        if not isinstance(value, list):
            value = [value]
        if is_date:
            # For date columns with 'in', parse each value
            conditions = [parsed_col == _val(v) for v in value]
            return reduce(lambda a, b: a | b, conditions)
        return F.col(col_name).isin(value)
    
    elif op == ColumnFilterOperator.not_in:
        if value is None:
            raise ValueError("'not_in' operator requires a list of values")
        if not isinstance(value, list):
            value = [value]
        if is_date:
            conditions = [parsed_col == _val(v) for v in value]
            combined = reduce(lambda a, b: a | b, conditions)
            return ~combined
        return ~F.col(col_name).isin(value)
    
    else:
        raise ValueError(f"Unknown operator: {op}")


def apply_column_filters(
    df: DataFrame,
    filters: Optional[List[ColumnFilter]],
    logic: ColumnFilterLogic = ColumnFilterLogic.AND,
) -> DataFrame:
    """
    Apply a list of column filters to a PySpark DataFrame.
    
    Filters are combined using AND or OR logic. PySpark will push predicates
    down to Delta Lake for efficient data skipping.
    
    Args:
        df: Source DataFrame
        filters: List of ColumnFilter conditions (or None to skip)
        logic: How to combine filters (AND = all must match, OR = any must match)
        
    Returns:
        Filtered DataFrame
        
    Raises:
        ValueError: If a filter references a non-existent column or has invalid params
    """
    if not filters:
        return df
    
    # Clear format cache for this query
    _format_cache.clear()
    
    conditions = []
    for f in filters:
        try:
            condition = build_filter_condition(df, f)
            conditions.append(condition)
        except ValueError as e:
            logger.warning(f"Invalid filter on column '{f.column}': {e}")
            raise
    
    if not conditions:
        return df
    
    if logic == ColumnFilterLogic.AND:
        combined = reduce(lambda a, b: a & b, conditions)
    else:
        combined = reduce(lambda a, b: a | b, conditions)
    
    return df.filter(combined)


def apply_sorting(
    df: DataFrame,
    sort_by: Optional[str],
    sort_order: SortOrder = SortOrder.asc,
) -> DataFrame:
    """
    Apply sorting to a PySpark DataFrame.
    
    For string columns containing dates, sorting uses the parsed date
    so that chronological order is correct.
    
    Args:
        df: Source DataFrame
        sort_by: Column name to sort by (None to skip sorting)
        sort_order: Sort direction (asc or desc)
        
    Returns:
        Sorted DataFrame
        
    Raises:
        ValueError: If sort column doesn't exist
    """
    if not sort_by:
        return df
    
    if sort_by not in df.columns:
        raise ValueError(f"Sort column '{sort_by}' not found. Available columns: {df.columns}")
    
    # Use date-aware column for sorting too
    parsed_col, is_date = _get_date_parsed_col(df, sort_by)
    
    if sort_order == SortOrder.desc:
        return df.orderBy(parsed_col.desc())
    else:
        return df.orderBy(parsed_col.asc())


def apply_filters_and_sorting(
    df: DataFrame,
    filters: Optional[List[ColumnFilter]] = None,
    filters_logic: ColumnFilterLogic = ColumnFilterLogic.AND,
    sort_by: Optional[str] = None,
    sort_order: SortOrder = SortOrder.asc,
) -> DataFrame:
    """
    Apply column filters and sorting to a DataFrame (convenience wrapper).
    
    Order of operations:
    1. Apply column filters (with smart date detection)
    2. Apply sorting (also date-aware)
    
    Pagination (limit/offset) is NOT applied here - that's handled by
    the versioning service after counting total filtered rows.
    
    Args:
        df: Source DataFrame
        filters: Column filter conditions
        filters_logic: AND or OR logic for combining filters
        sort_by: Column name to sort by
        sort_order: asc or desc
        
    Returns:
        Filtered and sorted DataFrame
    """
    df = apply_column_filters(df, filters, filters_logic)
    df = apply_sorting(df, sort_by, sort_order)
    return df
