"""
Silver Transformation Service

This service handles all Silver layer operations:
1. Normalization rules management
2. Semantic mappings management
3. Filters management
4. Virtualized queries (sources → JSON)
5. Transform execution (Bronze → Silver Delta)
"""

import os
import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from collections import defaultdict
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import and_, or_, func
from fastapi import HTTPException, status

from ..database.core.core import DataConnection, Dataset
from ..database.metadata.metadata import ExternalTables, ExternalColumn, ExternalSchema
from ..database.metadata.relationships import TableRelationship, RelationshipScope, JoinType
from ..database.datasets.bronze import DatasetBronzeConfig, BronzeColumnMapping
from ..database.datasets.silver import (
    NormalizationRule,
    VirtualizedConfig,
    TransformConfig,
    TransformExecution,
    TransformStatus,
    NormalizationType,
)
from ..database.equivalence.equivalence import ColumnGroup, ColumnMapping
from ..api.schemas.silver_schemas import (
    NormalizationRuleCreate,
    NormalizationRuleUpdate,
    NormalizationRuleResponse,
    NormalizationRuleTest,
    NormalizationRuleTestResult,
    VirtualizedConfigCreate,
    VirtualizedConfigUpdate,
    VirtualizedConfigResponse,
    VirtualizedPreviewResponse,
    VirtualizedQueryResponse,
    TransformConfigCreate,
    TransformConfigUpdate,
    TransformConfigResponse,
    TransformPreviewResponse,
    TransformExecuteResponse,
    TransformStatusEnum,
)
from .trino_manager import TrinoManager
from .spark_manager import SparkManager

# Import the data normalizer
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
try:
    from .data_normalizer import Normalizer, RuleBuilder, NormalizationResult
except ImportError:
    Normalizer = None
    RuleBuilder = None

logger = logging.getLogger(__name__)


class SilverTransformationService:
    """
    Service for Silver layer transformations.
    """
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.trino = TrinoManager()
        self.spark_manager = SparkManager()
        self.silver_bucket = os.getenv("SILVER_BUCKET", "datafabric-silver")
        self.silver_catalog = "silver"
        self.silver_schema = "default"
        
        # Initialize Python normalizer if available
        self.normalizer = Normalizer() if Normalizer else None
        if self.normalizer:
            self.normalizer.load_default_rules()
        
        # Ensure Silver bucket exists
        self._ensure_silver_bucket()
    
    def _ensure_silver_bucket(self):
        """Create the Silver bucket if it doesn't exist."""
        from minio import Minio
        from urllib.parse import urlparse
        
        try:
            endpoint = os.getenv("INTERNAL_S3_ENDPOINT", "http://minio:9000")
            access_key = os.getenv("INTERNAL_S3_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minio"))
            secret_key = os.getenv("INTERNAL_S3_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123"))
            
            parsed = urlparse(endpoint)
            endpoint_host = parsed.netloc if parsed.netloc else parsed.path
            secure = parsed.scheme == "https"
            
            client = Minio(
                endpoint=endpoint_host,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
            
            if not client.bucket_exists(self.silver_bucket):
                client.make_bucket(self.silver_bucket)
                logger.info(f"Created Silver bucket: {self.silver_bucket}")
            else:
                logger.debug(f"Silver bucket already exists: {self.silver_bucket}")
        except Exception as e:
            logger.warning(f"Could not ensure Silver bucket exists: {e}")
    
    # ==================== NORMALIZATION RULES ====================
    
    async def create_normalization_rule(
        self,
        data: NormalizationRuleCreate
    ) -> NormalizationRuleResponse:
        """Create a normalization rule from template."""
        
        # Check if name already exists
        existing = await self.db.execute(
            select(NormalizationRule).where(NormalizationRule.name == data.name)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Rule with name '{data.name}' already exists"
            )
        
        # Parse template to generate regex
        regex_entrada = None
        template_saida = None
        case_transforms = None
        
        if RuleBuilder and data.template:
            try:
                parsed_rule = RuleBuilder.from_template(
                    nome=data.name,
                    template=data.template,
                    descricao=data.description or "",
                    pre_process=data.pre_process
                )
                regex_entrada = parsed_rule.regex_entrada
                template_saida = parsed_rule.template_saida
                case_transforms = parsed_rule.case_transforms
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid template: {str(e)}"
                )
        
        # Convert template to SQL regex pattern
        sql_pattern, sql_replacement = self._template_to_sql_regex(data.template)
        
        rule = NormalizationRule(
            name=data.name,
            description=data.description,
            template=data.template,
            regex_pattern=sql_pattern,
            regex_replacement=sql_replacement,
            pre_process=data.pre_process,
            regex_entrada=regex_entrada,
            template_saida=template_saida,
            case_transforms=case_transforms,
            example_input=data.example_input,
            example_output=data.example_output,
            is_builtin=False,
            is_active=True
        )
        
        self.db.add(rule)
        await self.db.commit()
        await self.db.refresh(rule)
        
        return NormalizationRuleResponse.model_validate(rule)
    
    async def get_normalization_rule(self, rule_id: int) -> NormalizationRuleResponse:
        """Get a normalization rule by ID."""
        result = await self.db.execute(
            select(NormalizationRule).where(NormalizationRule.id == rule_id)
        )
        rule = result.scalar_one_or_none()
        
        if not rule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Rule with ID {rule_id} not found"
            )
        
        return NormalizationRuleResponse.model_validate(rule)
    
    async def list_normalization_rules(
        self,
        include_inactive: bool = False
    ) -> List[NormalizationRuleResponse]:
        """List all normalization rules."""
        query = select(NormalizationRule)
        if not include_inactive:
            query = query.where(NormalizationRule.is_active == True)
        query = query.order_by(NormalizationRule.name)
        
        result = await self.db.execute(query)
        rules = result.scalars().all()
        
        return [NormalizationRuleResponse.model_validate(r) for r in rules]
    
    async def update_normalization_rule(
        self,
        rule_id: int,
        data: NormalizationRuleUpdate
    ) -> NormalizationRuleResponse:
        """Update a normalization rule."""
        result = await self.db.execute(
            select(NormalizationRule).where(NormalizationRule.id == rule_id)
        )
        rule = result.scalar_one_or_none()
        
        if not rule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Rule with ID {rule_id} not found"
            )
        
        # Update fields
        update_data = data.model_dump(exclude_unset=True)
        
        # If template changed, regenerate regex
        if 'template' in update_data and update_data['template']:
            if RuleBuilder:
                try:
                    parsed_rule = RuleBuilder.from_template(
                        nome=rule.name,
                        template=update_data['template'],
                        descricao=rule.description or "",
                        pre_process=update_data.get('pre_process', rule.pre_process)
                    )
                    update_data['regex_entrada'] = parsed_rule.regex_entrada
                    update_data['template_saida'] = parsed_rule.template_saida
                    update_data['case_transforms'] = parsed_rule.case_transforms
                except Exception as e:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid template: {str(e)}"
                    )
            
            sql_pattern, sql_replacement = self._template_to_sql_regex(update_data['template'])
            update_data['regex_pattern'] = sql_pattern
            update_data['regex_replacement'] = sql_replacement
        
        for key, value in update_data.items():
            setattr(rule, key, value)
        
        await self.db.commit()
        await self.db.refresh(rule)
        
        return NormalizationRuleResponse.model_validate(rule)
    
    async def delete_normalization_rule(self, rule_id: int) -> bool:
        """Delete a normalization rule."""
        result = await self.db.execute(
            select(NormalizationRule).where(NormalizationRule.id == rule_id)
        )
        rule = result.scalar_one_or_none()
        
        if not rule:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Rule with ID {rule_id} not found"
            )
        
        await self.db.delete(rule)
        await self.db.commit()
        return True
    
    async def test_normalization_rule(
        self,
        data: NormalizationRuleTest
    ) -> NormalizationRuleTestResult:
        """Test a normalization rule with a value."""
        
        try:
            rule = None
            template = data.template
            
            # Get rule by ID or name
            if data.rule_id:
                result = await self.db.execute(
                    select(NormalizationRule).where(NormalizationRule.id == data.rule_id)
                )
                rule = result.scalar_one_or_none()
                if rule:
                    template = rule.template
                else:
                    return NormalizationRuleTestResult(
                        success=False,
                        original_value=data.value,
                        normalized_value=None,
                        error=f"Rule with ID {data.rule_id} not found"
                    )
            elif data.rule_name:
                result = await self.db.execute(
                    select(NormalizationRule).where(NormalizationRule.name == data.rule_name)
                )
                rule = result.scalar_one_or_none()
                if rule:
                    template = rule.template
                else:
                    return NormalizationRuleTestResult(
                        success=False,
                        original_value=data.value,
                        normalized_value=None,
                        error=f"Rule with name '{data.rule_name}' not found"
                    )
            
            if not template:
                return NormalizationRuleTestResult(
                    success=False,
                    original_value=data.value,
                    normalized_value=None,
                    error="No template provided and rule has no template"
                )
            
            # Test with Python normalizer
            if self.normalizer and RuleBuilder:
                try:
                    test_rule = RuleBuilder.from_template(
                        nome="test",
                        template=template,
                        descricao=""
                    )
                    self.normalizer.add_rule(test_rule)
                    normalized = self.normalizer.normalize(data.value, "test")
                    self.normalizer.remove_rule("test")
                    
                    if normalized:
                        return NormalizationRuleTestResult(
                            success=True,
                            original_value=data.value,
                            normalized_value=normalized,
                            rule_applied=rule.name if rule else template
                        )
                    else:
                        return NormalizationRuleTestResult(
                            success=False,
                            original_value=data.value,
                            normalized_value=None,
                            rule_applied=rule.name if rule else template,
                            error="Value does not match expected pattern"
                        )
                except Exception as e:
                    logger.error(f"Normalizer error: {str(e)}")
                    return NormalizationRuleTestResult(
                        success=False,
                        original_value=data.value,
                        normalized_value=None,
                        error=f"Normalizer error: {str(e)}"
                    )
            
            return NormalizationRuleTestResult(
                success=False,
                original_value=data.value,
                normalized_value=None,
                error="Normalizer not available (data_normalizer.py not found or has errors)"
            )
        except Exception as e:
            logger.error(f"Test normalization rule error: {str(e)}")
            return NormalizationRuleTestResult(
                success=False,
                original_value=data.value,
                normalized_value=None,
                error=f"Unexpected error: {str(e)}"
            )
    
    def _template_to_sql_regex(self, template: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Convert a template like {d3}.{d3}.{d3}-{d2} to SQL regex patterns.
        
        The template defines the OUTPUT format. The INPUT pattern captures
        only the data (without separators), then the output adds the formatting.
        
        Example:
            Template: {d3}.{d3}.{d3}-{d2}
            Input:  ^(\d{3})(\d{3})(\d{3})(\d{2})$  (captures: 12345678901)
            Output: $1.$2.$3-$4                     (produces: 123.456.789-01)
        
        Returns:
            (input_pattern, output_pattern) for regexp_replace
        """
        if not template:
            return None, None
        
        pattern = r'\{([^}]+)\}'
        
        input_parts = []
        output_parts = []
        group_num = 1
        last_end = 0
        
        for match in re.finditer(pattern, template):
            # Get literal before this placeholder (for OUTPUT only)
            literal_before = template[last_end:match.start()]
            if literal_before:
                # Literals go to OUTPUT pattern only (for formatting)
                output_parts.append(literal_before)
            
            placeholder = match.group(1)
            last_end = match.end()
            
            # Parse placeholder
            is_optional = placeholder.endswith('?')
            clean_ph = placeholder.rstrip('?')
            tipo = clean_ph[0].lower()
            
            quantidade = None
            if len(clean_ph) > 1 and clean_ph[1:].isdigit():
                quantidade = int(clean_ph[1:])
            
            # Determine char pattern
            if tipo == 'd':
                char_pattern = "\\d"
            elif tipo == 'l':
                char_pattern = "[A-Za-z]"
            elif tipo == 'w':
                char_pattern = "\\w"
            else:
                continue
            
            # Build quantifier
            is_variavel = placeholder[0].isupper() and placeholder[0] in 'DLW'
            if is_variavel:
                quant = "*" if is_optional else "+"
            elif quantidade:
                quant = f"{{{quantidade}}}"
            else:
                quant = "?" if is_optional else ""
            
            # INPUT pattern: capture groups without separators
            input_parts.append(f"({char_pattern}{quant})")
            # OUTPUT pattern: reference to captured group
            output_parts.append(f"${group_num}")
            group_num += 1
        
        # Remaining literal (for OUTPUT only)
        if last_end < len(template):
            literal_after = template[last_end:]
            output_parts.append(literal_after)
        
        input_pattern = "^" + "".join(input_parts) + "$"
        output_pattern = "".join(output_parts)
        
        return input_pattern, output_pattern
    
    # ==================== SEMANTIC MAPPINGS ====================
    # NOTE: Semantic mappings are managed by the Equivalence module.
    # Use /api/equivalence endpoints for ColumnGroup and ColumnMapping.
    # This Silver service uses column_group_ids from Equivalence and resolves
    # to Bronze column names via BronzeColumnMapping.
    
    async def resolve_column_group_to_bronze_names(
        self,
        column_group_id: int,
        bronze_dataset_id: int
    ) -> List[Dict[str, str]]:
        """
        Resolve a ColumnGroup from Equivalence to Bronze column names.
        
        Uses BronzeColumnMapping to translate external_column.id to bronze_column_name.
        
        Returns:
            List of {external_column_id, bronze_column_name, unified_name}
        """
        # Get column mappings from the group
        column_mappings_result = await self.db.execute(
            select(ColumnMapping).where(ColumnMapping.group_id == column_group_id)
        )
        column_mappings = column_mappings_result.scalars().all()
        
        if not column_mappings:
            return []
        
        # Get the ColumnGroup to get the unified name
        group_result = await self.db.execute(
            select(ColumnGroup).where(ColumnGroup.id == column_group_id)
        )
        group = group_result.scalar_one_or_none()
        unified_name = group.name if group else f"group_{column_group_id}"
        
        # Get the bronze dataset config
        bronze_config_result = await self.db.execute(
            select(DatasetBronzeConfig).where(DatasetBronzeConfig.dataset_id == bronze_dataset_id)
        )
        bronze_config = bronze_config_result.scalar_one_or_none()
        
        if not bronze_config:
            return []
        
        # Get ingestion groups for this bronze config
        from ..database.datasets.bronze import DatasetIngestionGroup
        ingestion_groups_result = await self.db.execute(
            select(DatasetIngestionGroup.id).where(
                DatasetIngestionGroup.bronze_config_id == bronze_config.id
            )
        )
        ingestion_group_ids = [r[0] for r in ingestion_groups_result.fetchall()]
        
        if not ingestion_group_ids:
            return []
        
        # Map external_column_ids to bronze column names
        resolved = []
        for cm in column_mappings:
            bronze_mapping_result = await self.db.execute(
                select(BronzeColumnMapping).where(
                    and_(
                        BronzeColumnMapping.external_column_id == cm.column_id,
                        BronzeColumnMapping.ingestion_group_id.in_(ingestion_group_ids)
                    )
                )
            )
            bronze_mapping = bronze_mapping_result.scalar_one_or_none()
            
            if bronze_mapping:
                resolved.append({
                    'external_column_id': cm.column_id,
                    'bronze_column_name': bronze_mapping.bronze_column_name,
                    'unified_name': unified_name
                })
        
        return resolved
    
    async def get_column_group_info(
        self,
        column_group_id: int
    ) -> Dict[str, Any]:
        """
        Get full ColumnGroup info including ColumnMappings and ValueMappings.
        
        Returns:
            {
                'id': group_id,
                'name': 'unified_name',
                'column_mappings': [{column_id, column_name, table_name}],
                'value_mappings': [{source_column_id, source_value, standard_value}]
            }
        """
        from ..database.equivalence.equivalence import ValueMapping
        
        # Get the ColumnGroup
        group_result = await self.db.execute(
            select(ColumnGroup).where(ColumnGroup.id == column_group_id)
        )
        group = group_result.scalar_one_or_none()
        
        if not group:
            return None
        
        # Get ColumnMappings with column details
        column_mappings_result = await self.db.execute(
            select(
                ColumnMapping,
                ExternalColumn.column_name,
                ExternalTables.table_name,
                DataConnection.name.label('connection_name')
            ).join(
                ExternalColumn, ColumnMapping.column_id == ExternalColumn.id
            ).join(
                ExternalTables, ExternalColumn.table_id == ExternalTables.id
            ).join(
                DataConnection, ExternalTables.connection_id == DataConnection.id
            ).where(
                ColumnMapping.group_id == column_group_id
            )
        )
        column_mappings_rows = column_mappings_result.fetchall()
        
        column_mappings = [
            {
                'column_id': row[0].column_id,
                'column_name': row[1],
                'table_name': row[2],
                'connection_name': row[3]
            }
            for row in column_mappings_rows
        ]
        
        # Get ValueMappings
        value_mappings_result = await self.db.execute(
            select(
                ValueMapping,
                ExternalColumn.column_name
            ).join(
                ExternalColumn, ValueMapping.source_column_id == ExternalColumn.id
            ).where(
                ValueMapping.group_id == column_group_id
            )
        )
        value_mappings_rows = value_mappings_result.fetchall()
        
        value_mappings = [
            {
                'source_column_id': row[0].source_column_id,
                'source_column_name': row[1],
                'source_value': row[0].source_value,
                'standard_value': row[0].standard_value
            }
            for row in value_mappings_rows
        ]
        
        return {
            'id': group.id,
            'name': group.name,
            'description': group.description,
            'column_mappings': column_mappings,
            'value_mappings': value_mappings
        }
    
    async def load_equivalence_for_virtualized(
        self,
        column_group_ids: List[int]
    ) -> Dict[str, Any]:
        """
        Load all ColumnGroups info with their ColumnMappings and ValueMappings
        for use in virtualized queries.
        
        Returns:
            {
                'groups': [group_info...],
                'unified_columns': {external_column_id: unified_name},
                'value_mappings_by_column': {
                    external_column_id: {source_value: standard_value}
                }
            }
        """
        groups = []
        unified_columns = {}
        value_mappings_by_column = {}
        
        for group_id in column_group_ids:
            group_info = await self.get_column_group_info(group_id)
            if not group_info:
                continue
            
            groups.append(group_info)
            
            # Map column_ids to unified name
            for cm in group_info['column_mappings']:
                unified_columns[cm['column_id']] = group_info['name']
            
            # Organize value mappings by column
            for vm in group_info['value_mappings']:
                col_id = vm['source_column_id']
                if col_id not in value_mappings_by_column:
                    value_mappings_by_column[col_id] = {}
                value_mappings_by_column[col_id][vm['source_value']] = vm['standard_value']
        
        return {
            'groups': groups,
            'unified_columns': unified_columns,
            'value_mappings_by_column': value_mappings_by_column
        }
    
    # ==================== INLINE FILTER HELPERS ====================
    
    def _inline_filter_to_sql(
        self,
        filters: Dict[str, Any],
        column_metadata: Dict[int, Dict[str, Any]],
        table_aliases: Optional[Dict[int, str]] = None
    ) -> Optional[str]:
        """
        Convert inline filter to SQL WHERE clause.
        
        Args:
            filters: Inline filter dict with 'logic' and 'conditions'
            column_metadata: Dict mapping column_id to column info
            table_aliases: Dict mapping table_id to alias
            
        Returns:
            SQL WHERE clause string or None
        """
        if not filters:
            return None
        
        conditions = filters.get('conditions', [])
        if not conditions:
            return None
        
        def format_value(val: Any) -> str:
            """Format value for SQL - no quotes for numbers, quotes for strings."""
            if val is None:
                return "NULL"
            # Check if it's a number
            try:
                float(val)
                return str(val)  # No quotes for numeric values
            except (ValueError, TypeError):
                # Escape single quotes and wrap in quotes
                escaped = str(val).replace("'", "''")
                return f"'{escaped}'"
        
        # Build reverse lookup: column_name -> [(table_id, column_id)]
        columns_by_name = {}
        for col_id, col_meta in column_metadata.items():
            col_name = col_meta["column_name"]
            table_id = col_meta.get("table_id")
            if col_name not in columns_by_name:
                columns_by_name[col_name] = []
            columns_by_name[col_name].append((table_id, col_id))
        
        condition_sqls = []
        for cond in conditions:
            col_ref = None
            column_id = cond.get('column_id')
            column_name = cond.get('column_name')
            
            if column_id and column_id in column_metadata:
                col_meta = column_metadata[column_id]
                col_name = col_meta["column_name"]
                table_id = col_meta.get("table_id")
                
                # Use table alias if available to avoid ambiguous column errors
                if table_aliases and table_id and table_id in table_aliases:
                    alias = table_aliases[table_id]
                    col_ref = f'{alias}."{col_name}"'
                else:
                    col_ref = f'"{col_name}"'
            elif column_name:
                # Column referenced by name - check if it exists in multiple tables
                col_name = column_name
                if col_name in columns_by_name and table_aliases:
                    matching = columns_by_name[col_name]
                    if len(matching) > 1:
                        # Column exists in multiple tables - use COALESCE to match unified column
                        coalesce_parts = []
                        for t_id, c_id in matching:
                            if t_id in table_aliases:
                                coalesce_parts.append(f'{table_aliases[t_id]}."{col_name}"')
                        if coalesce_parts:
                            col_ref = f'COALESCE({", ".join(coalesce_parts)})'
                        else:
                            col_ref = f'"{col_name}"'
                    elif len(matching) == 1:
                        # Column exists in one table - use alias
                        t_id, c_id = matching[0]
                        if t_id in table_aliases:
                            col_ref = f'{table_aliases[t_id]}."{col_name}"'
                        else:
                            col_ref = f'"{col_name}"'
                    else:
                        col_ref = f'"{col_name}"'
                else:
                    col_ref = f'"{col_name}"'
            else:
                continue
            
            op = cond.get('operator', '=')
            value = cond.get('value')
            value_min = cond.get('value_min')
            value_max = cond.get('value_max')
            
            if op in ('IS NULL', 'IS NOT NULL'):
                condition_sqls.append(f"{col_ref} {op}")
            elif op == 'BETWEEN':
                val_min = format_value(value_min)
                val_max = format_value(value_max)
                condition_sqls.append(f"{col_ref} BETWEEN {val_min} AND {val_max}")
            elif op in ('IN', 'NOT IN'):
                # Value should be a list
                import json
                try:
                    values_list = json.loads(value) if isinstance(value, str) else value
                    if isinstance(values_list, list):
                        values_str = ", ".join([format_value(v) for v in values_list])
                        condition_sqls.append(f"{col_ref} {op} ({values_str})")
                except Exception:
                    pass
            else:
                formatted_val = format_value(value)
                condition_sqls.append(f"{col_ref} {op} {formatted_val}")
        
        if not condition_sqls:
            return None
        
        logic = filters.get('logic', 'AND')
        return f" {logic} ".join(condition_sqls)
    
    # ==================== VIRTUALIZED CONFIG ====================
    
    async def create_virtualized_config(
        self,
        data: VirtualizedConfigCreate
    ) -> VirtualizedConfigResponse:
        """Create a virtualized config."""
        
        # Check if name exists
        existing = await self.db.execute(
            select(VirtualizedConfig).where(VirtualizedConfig.name == data.name)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Config with name '{data.name}' already exists"
            )
        
        config = VirtualizedConfig(
            name=data.name,
            description=data.description,
            tables=[t.model_dump() for t in data.tables],
            column_group_ids=data.column_group_ids,
            relationship_ids=data.relationship_ids,
            filters=data.filters.model_dump() if data.filters else None,
            column_transformations=[t.model_dump() for t in data.column_transformations] if data.column_transformations else None,
            exclude_unified_source_columns=data.exclude_unified_source_columns,
            is_active=True
        )
        
        self.db.add(config)
        await self.db.commit()
        await self.db.refresh(config)
        
        return VirtualizedConfigResponse.model_validate(config)
    
    async def get_virtualized_config(self, config_id: int) -> VirtualizedConfigResponse:
        """Get a virtualized config."""
        result = await self.db.execute(
            select(VirtualizedConfig).where(VirtualizedConfig.id == config_id)
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Config with ID {config_id} not found"
            )
        
        return VirtualizedConfigResponse.model_validate(config)
    
    async def list_virtualized_configs(
        self,
        include_inactive: bool = False
    ) -> List[VirtualizedConfigResponse]:
        """List all virtualized configs."""
        query = select(VirtualizedConfig)
        if not include_inactive:
            query = query.where(VirtualizedConfig.is_active == True)
        query = query.order_by(VirtualizedConfig.name)
        
        result = await self.db.execute(query)
        configs = result.scalars().all()
        
        return [VirtualizedConfigResponse.model_validate(c) for c in configs]
    
    async def update_virtualized_config(
        self,
        config_id: int,
        data: VirtualizedConfigUpdate
    ) -> VirtualizedConfigResponse:
        """Update a virtualized config."""
        result = await self.db.execute(
            select(VirtualizedConfig).where(VirtualizedConfig.id == config_id)
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Config with ID {config_id} not found"
            )
        
        update_data = data.model_dump(exclude_unset=True)
        
        # Convert nested objects to dicts
        if 'column_transformations' in update_data and update_data['column_transformations']:
            update_data['column_transformations'] = [
                t.model_dump() if hasattr(t, 'model_dump') else t 
                for t in update_data['column_transformations']
            ]
        
        # Convert filters to dict
        if 'filters' in update_data and update_data['filters']:
            if hasattr(update_data['filters'], 'model_dump'):
                update_data['filters'] = update_data['filters'].model_dump()
        
        # Clear cached SQL
        update_data['generated_sql'] = None
        update_data['sql_generated_at'] = None
        
        for key, value in update_data.items():
            setattr(config, key, value)
        
        await self.db.commit()
        await self.db.refresh(config)
        
        return VirtualizedConfigResponse.model_validate(config)
    
    async def delete_virtualized_config(self, config_id: int) -> bool:
        """Delete a virtualized config."""
        result = await self.db.execute(
            select(VirtualizedConfig).where(VirtualizedConfig.id == config_id)
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Config with ID {config_id} not found"
            )
        
        await self.db.delete(config)
        await self.db.commit()
        return True
    
    async def preview_virtualized_config(
        self,
        config_id: int
    ) -> VirtualizedPreviewResponse:
        """Preview a virtualized config (generate SQL)."""
        config = await self.get_virtualized_config(config_id)
        
        sql, columns, warnings = await self._generate_virtualized_sql(config)
        
        return VirtualizedPreviewResponse(
            config_id=config.id,
            config_name=config.name,
            sql=sql,
            columns=columns,
            estimated_tables=len(config.tables),
            warnings=warnings
        )
    
    async def query_virtualized_config(
        self,
        config_id: int,
        limit: int = 1000,
        offset: int = 0
    ) -> VirtualizedQueryResponse:
        """Execute a virtualized query."""
        config = await self.get_virtualized_config(config_id)
        
        start_time = datetime.now()
        
        sql, columns, warnings = await self._generate_virtualized_sql(config)
        
        # Add LIMIT/OFFSET
        sql_with_limit = f"{sql} LIMIT {limit}"
        if offset > 0:
            sql_with_limit += f" OFFSET {offset}"
        
        # Execute (async with aiotrino)
        try:
            conn = await self.trino.get_connection()
            cur = await conn.cursor()
            await cur.execute(sql_with_limit)
            
            rows = await cur.fetchall()
            
            # Get column names from aiotrino cursor using get_description() method
            result_columns = columns  # Use pre-computed columns as default
            description = await cur.get_description() if hasattr(cur, 'get_description') else None
            if description:
                result_columns = [col.name if hasattr(col, 'name') else (col.get('name') if isinstance(col, dict) else col[0]) for col in description]
            
            data = []
            for row in rows:
                row_dict = {}
                for i, col_name in enumerate(result_columns):
                    value = row[i]
                    if value is not None and not isinstance(value, (str, int, float, bool, list, dict)):
                        value = str(value)
                    row_dict[col_name] = value
                data.append(row_dict)
            
            await conn.close()
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return VirtualizedQueryResponse(
                config_id=config.id,
                config_name=config.name,
                columns=result_columns,
                data=data,
                row_count=len(data),
                execution_time_seconds=execution_time,
                sql_executed=sql_with_limit,
                warnings=warnings
            )
            
        except Exception as e:
            logger.error(f"Virtualized query failed: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Query execution failed: {str(e)}"
            )
    
    async def _generate_virtualized_sql(
        self,
        config: VirtualizedConfigResponse
    ) -> Tuple[str, List[str], List[str]]:
        """Generate SQL for a virtualized config."""
        warnings = []
        
        # ==================== EXTRACT TABLE AND COLUMN IDS ====================
        # Extract from new 'tables' structure (same as Bronze)
        table_ids = []
        column_ids = []
        
        for table_sel in config.tables:
            table_id = table_sel['table_id']
            table_ids.append(table_id)
            
            if table_sel.get('select_all', True):
                # Get all columns for this table
                cols_result = await self.db.execute(
                    select(ExternalColumn.id).where(ExternalColumn.table_id == table_id)
                )
                column_ids.extend([c[0] for c in cols_result.fetchall()])
            else:
                # Use specific columns
                column_ids.extend(table_sel.get('column_ids', []))
        
        # Get table metadata
        table_metadata = await self._fetch_table_metadata(table_ids)
        
        # Get column metadata
        column_metadata = await self._fetch_column_metadata(column_ids)
        
        # ==================== BUILD TABLE ALIASES ====================
        # Create aliases FIRST so we can use them in column expressions
        table_aliases = {}
        for idx, table_id in enumerate(table_ids):
            table_aliases[table_id] = f"t{idx}"
        
        # ==================== LOAD EQUIVALENCE DATA ====================
        # Load ColumnGroups and ValueMappings from Equivalence module
        equivalence_data = None
        unified_columns = {}  # col_id -> unified_name
        value_mappings_from_equiv = {}  # col_id -> {source_value: standard_value}
        
        if config.column_group_ids:
            equivalence_data = await self.load_equivalence_for_virtualized(config.column_group_ids)
            unified_columns = equivalence_data.get('unified_columns', {})
            value_mappings_from_equiv = equivalence_data.get('value_mappings_by_column', {})
            logger.info(f"Loaded {len(unified_columns)} unified columns and {len(value_mappings_from_equiv)} value mappings from Equivalence")
        
        # ==================== LOAD NORMALIZATION RULES ====================
        # Load all referenced normalization rules from column_transformations
        norm_rules = {}
        all_rule_ids = set()
        
        # Collect rule_ids from column_transformations
        if config.column_transformations:
            for t in config.column_transformations:
                if t.get('rule_id'):
                    all_rule_ids.add(t['rule_id'])
        
        # Load all rules
        if all_rule_ids:
            rules_result = await self.db.execute(
                select(NormalizationRule).where(NormalizationRule.id.in_(all_rule_ids))
            )
            for rule in rules_result.scalars():
                norm_rules[rule.id] = {
                    'name': rule.name,
                    'pattern': rule.regex_pattern,
                    'replacement': rule.regex_replacement,
                    'template': rule.template
                }
            logger.info(f"Loaded {len(norm_rules)} normalization rules")
        
        # ==================== GET RELATIONSHIPS FOR JOIN COLUMN UNIFICATION ====================
        # Fetch relationships early so we can identify JOIN columns to unify
        relationships = await self._get_applicable_relationships(
            table_ids,
            config.relationship_ids
        )
        
        # Build a map of column_id -> unified_name for JOIN columns
        # This automatically unifies columns used in JOINs (e.g., isic_id from multiple tables)
        join_column_unification = {}  # col_id -> unified_name
        join_column_ids = set()  # Track all column IDs used in JOINs
        
        for rel in relationships:
            left_col_name = await self._get_column_name_by_id(rel.left_column_id)
            right_col_name = await self._get_column_name_by_id(rel.right_column_id)
            
            if left_col_name and right_col_name:
                # Use the left column name as the unified name
                # (typically the "primary" side of the relationship)
                unified_name = left_col_name
                
                # Mark both columns for unification under the same name
                join_column_unification[rel.left_column_id] = unified_name
                join_column_unification[rel.right_column_id] = unified_name
                join_column_ids.add(rel.left_column_id)
                join_column_ids.add(rel.right_column_id)
                
        if join_column_unification:
            unique_unified = set(join_column_unification.values())
            logger.info(f"Auto-unifying {len(join_column_ids)} JOIN columns into {len(unique_unified)} unified columns: {unique_unified}")
        
        # ==================== BUILD SELECT COLUMNS ====================
        select_columns = []
        output_columns = []
        processed_unified_names = set()  # Track unified names already added
        columns_for_unification = {}  # unified_name -> [col_expressions]
        unified_source_column_names = set()  # Track original column names that are sources of unification
        
        # Check if we should exclude source columns after unification
        exclude_source_columns = getattr(config, 'exclude_unified_source_columns', False)
        
        # Transformations lookup (from config) - support multiple transforms per column
        transformations = defaultdict(list)
        if config.column_transformations:
            for t in config.column_transformations:
                transformations[t['column_id']].append(t)
        
        # Helper to build column expression with value mapping (from Equivalence only)
        def build_col_expr_with_mapping(col_expr: str, col_id: int) -> str:
            """Apply value mapping from Equivalence to column expression."""
            if col_id in value_mappings_from_equiv:
                mappings = value_mappings_from_equiv[col_id]
                if mappings:
                    case_parts = []
                    for from_val, to_val in mappings.items():
                        case_parts.append(f"WHEN {col_expr} = '{from_val}' THEN '{to_val}'")
                    # Default to original value if no match
                    return f"CASE {' '.join(case_parts)} ELSE CAST({col_expr} AS VARCHAR) END"
            
            return col_expr
        
        # Helper to apply normalization rule
        def apply_normalization_rule(col_expr: str, rule_id: int) -> str:
            """Apply a normalization rule to column expression."""
            if rule_id not in norm_rules:
                warnings.append(f"Normalization rule {rule_id} not found")
                return col_expr
            
            rule = norm_rules[rule_id]
            if rule['pattern'] and rule['replacement']:
                return f"regexp_replace({col_expr}, '{rule['pattern']}', '{rule['replacement']}')"
            
            warnings.append(f"Rule {rule_id} ({rule['name']}) has no pattern/replacement")
            return col_expr
        
        for col_id, col_info in column_metadata.items():
            table_id = col_info['table_id']
            table_info = table_metadata.get(table_id, {})
            
            # Use table alias instead of full catalog path
            alias = table_aliases.get(table_id, 't0')
            col_expr = f'{alias}."{col_info["column_name"]}"'
            output_name = col_info['column_name']
            
            # Apply transformations (from config) - supports multiple transforms per column
            if col_id in transformations:
                for t in transformations[col_id]:
                    trans_type = t.get('type', '')
                    
                    # Rule-based transformations (template)
                    if trans_type == 'template' and t.get('rule_id'):
                        col_expr = apply_normalization_rule(col_expr, t['rule_id'])
                    elif t.get('rule_id'):
                        # Fallback: if rule_id provided, use it
                        col_expr = apply_normalization_rule(col_expr, t['rule_id'])
                    
                    # SQL text transformations
                    elif trans_type == 'lowercase':
                        col_expr = f"LOWER({col_expr})"
                    elif trans_type == 'uppercase':
                        col_expr = f"UPPER({col_expr})"
                    elif trans_type == 'trim':
                        col_expr = f"TRIM({col_expr})"
                    elif trans_type == 'normalize_spaces':
                        # Replace multiple spaces with single space, then trim
                        col_expr = f"TRIM(regexp_replace({col_expr}, '\\s+', ' '))"
                    elif trans_type == 'remove_accents':
                        # Trino translate() for common accents
                        col_expr = f"translate({col_expr}, 'áàâãäéèêëíìîïóòôõöúùûüçñÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ', 'aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN')"
            
            # Check if this column should be unified
            # Priority: 1) Equivalence groups (explicit), 2) JOIN columns (automatic)
            if col_id in unified_columns:
                # Explicit unification from Equivalence module
                unified_name = unified_columns[col_id]
                
                # Track that this column is a source of unification
                unified_source_column_names.add(output_name)
                
                # Apply value mapping before unification (from Equivalence)
                col_expr_with_mapping = build_col_expr_with_mapping(col_expr, col_id)
                
                if unified_name not in columns_for_unification:
                    columns_for_unification[unified_name] = []
                columns_for_unification[unified_name].append(col_expr_with_mapping)
                
                # If NOT excluding source columns, also add the original column
                if not exclude_source_columns:
                    select_columns.append(f'{col_expr_with_mapping} AS "{output_name}"')
                    output_columns.append(output_name)
                    
            elif col_id in join_column_unification:
                # Automatic unification for JOIN columns
                unified_name = join_column_unification[col_id]
                
                # Track that this column is a source of unification
                unified_source_column_names.add(output_name)
                
                # Apply value mapping if any
                col_expr_with_mapping = build_col_expr_with_mapping(col_expr, col_id)
                
                if unified_name not in columns_for_unification:
                    columns_for_unification[unified_name] = []
                columns_for_unification[unified_name].append(col_expr_with_mapping)
                
                # JOIN columns are always unified (no separate column for each source)
                # This is the expected behavior - we don't want t0.isic_id, t1.isic_id separately
                
            else:
                # Regular column (not unified)
                col_expr = build_col_expr_with_mapping(col_expr, col_id)
                
                select_columns.append(f'{col_expr} AS "{output_name}"')
                output_columns.append(output_name)
        
        # Add unified columns with COALESCE
        for unified_name, col_expressions in columns_for_unification.items():
            if unified_name not in processed_unified_names:
                if len(col_expressions) > 1:
                    # Multiple columns to unify - use COALESCE
                    coalesce_expr = f"COALESCE({', '.join(col_expressions)})"
                else:
                    # Single column
                    coalesce_expr = col_expressions[0]
                
                select_columns.append(f'{coalesce_expr} AS "{unified_name}"')
                output_columns.append(unified_name)
                processed_unified_names.add(unified_name)
        
        # Log if source columns were excluded
        if exclude_source_columns and unified_source_column_names:
            logger.info(f"Excluded {len(unified_source_column_names)} source columns after unification: {unified_source_column_names}")
        
        # ==================== BUILD FROM CLAUSE WITH JOINS ====================
        # Note: relationships already fetched above for JOIN column unification
        
        # Build table full names
        table_full_names = {}
        for table_id in table_ids:
            table_info = table_metadata.get(table_id, {})
            catalog = self.trino.generate_catalog_name(
                table_info.get('connection_name', ''),
                table_info.get('connection_id', 0)
            )
            alias = table_aliases.get(table_id, f"t{table_ids.index(table_id)}")
            table_full_names[table_id] = {
                'full_name': f'"{catalog}"."{table_info.get("schema_name", "")}"."{table_info.get("table_name", "")}"',
                'alias': alias,
                'table_name': table_info.get('table_name', '')
            }
        
        # Build WHERE clause from inline filters
        where_clauses = []
        if config.filters:
            filter_sql = self._inline_filter_to_sql(config.filters, column_metadata, table_aliases)
            if filter_sql:
                where_clauses.append(f"({filter_sql})")
        
        # ==================== ASSEMBLE SQL ====================
        if len(table_ids) == 1:
            # Single table - simple FROM
            t = table_full_names[table_ids[0]]
            sql = f"SELECT {', '.join(select_columns)}"
            sql += f" FROM {t['full_name']} {t['alias']}"
        elif not relationships:
            # Multiple tables but no relationships - CROSS JOIN with warning
            from_parts = [f"{t['full_name']} {t['alias']}" for t in table_full_names.values()]
            sql = f"SELECT {', '.join(select_columns)}"
            sql += f" FROM {' CROSS JOIN '.join(from_parts)}"
            warnings.append("No relationships found between selected tables. Using CROSS JOIN which produces cartesian product. "
                          "Define relationships via POST /api/relationships/ or use Bronze with enable_federated_joins.")
        else:
            # Multiple tables with relationships - build JOINs
            sql = f"SELECT {', '.join(select_columns)}"
            sql += await self._build_join_clause(table_ids, relationships, table_full_names, table_aliases)
            logger.info(f"Generated JOIN clause using {len(relationships)} relationships")
        
        if where_clauses:
            sql += f" WHERE {' AND '.join(where_clauses)}"
        
        # Debug: log the full SQL
        logger.info(f"Generated virtualized SQL:\n{sql}")
        
        return sql, output_columns, warnings
    
    async def _build_join_clause(
        self,
        table_ids: List[int],
        relationships: List[TableRelationship],
        table_full_names: Dict[int, Dict[str, str]],
        table_aliases: Dict[int, str]
    ) -> str:
        """
        Build FROM clause with JOINs based on relationships.
        
        Uses the same algorithm as Bronze federated joins to ensure consistency.
        """
        # Determine primary table (first table or the one with most relationships on left side)
        left_counts = defaultdict(int)
        for rel in relationships:
            left_counts[rel.left_table_id] += 1
        primary_table_id = max(table_ids, key=lambda t: left_counts.get(t, 0))
        
        # Start FROM clause with primary table
        primary = table_full_names[primary_table_id]
        from_clause = f" FROM {primary['full_name']} {primary['alias']}"
        
        # Track which tables have been joined
        joined_tables = {primary_table_id}
        
        # Build JOIN clauses
        join_type_map = {
            JoinType.INNER: 'INNER JOIN',
            JoinType.LEFT: 'LEFT JOIN',
            JoinType.RIGHT: 'RIGHT JOIN',
            JoinType.FULL: 'FULL OUTER JOIN'
        }
        
        # Sort relationships to join tables in logical order
        remaining_rels = list(relationships)
        max_iterations = len(remaining_rels) * 2
        iteration = 0
        
        while remaining_rels and iteration < max_iterations:
            iteration += 1
            made_progress = False
            
            for rel in list(remaining_rels):
                left_in = rel.left_table_id in joined_tables
                right_in = rel.right_table_id in joined_tables
                
                if left_in and right_in:
                    # Both already joined, skip
                    remaining_rels.remove(rel)
                    made_progress = True
                    continue
                elif left_in:
                    join_table_id = rel.right_table_id
                elif right_in:
                    join_table_id = rel.left_table_id
                else:
                    # Neither joined yet, skip for now
                    continue
                
                if join_table_id not in table_full_names:
                    remaining_rels.remove(rel)
                    made_progress = True
                    continue
                
                join_table = table_full_names[join_table_id]
                
                # Get column names for join condition
                left_col_name = await self._get_column_name_by_id(rel.left_column_id)
                right_col_name = await self._get_column_name_by_id(rel.right_column_id)
                
                if not left_col_name or not right_col_name:
                    logger.warning(f"Could not resolve columns for relationship {rel.id}")
                    remaining_rels.remove(rel)
                    made_progress = True
                    continue
                
                # Build join type
                join_type = join_type_map.get(rel.default_join_type, 'FULL OUTER JOIN')
                
                # Build join condition
                left_alias = table_aliases[rel.left_table_id]
                right_alias = table_aliases[rel.right_table_id]
                condition = f'{left_alias}."{left_col_name}" = {right_alias}."{right_col_name}"'
                
                # Add JOIN clause
                from_clause += f"\n{join_type} {join_table['full_name']} {join_table['alias']} ON {condition}"
                joined_tables.add(join_table_id)
                
                remaining_rels.remove(rel)
                made_progress = True
            
            if not made_progress:
                if remaining_rels:
                    logger.warning(f"Could not apply {len(remaining_rels)} relationships - disconnected tables")
                break
        
        # Handle tables that couldn't be joined (add as CROSS JOIN with warning)
        unjoined = set(table_ids) - joined_tables
        for table_id in unjoined:
            t = table_full_names[table_id]
            from_clause += f"\nCROSS JOIN {t['full_name']} {t['alias']}"
            logger.warning(f"Table {t['table_name']} has no relationship to other tables, using CROSS JOIN")
        
        return from_clause
    
    async def _get_applicable_relationships(
        self,
        table_ids: List[int],
        relationship_ids: Optional[List[int]] = None
    ) -> List[TableRelationship]:
        """
        Get relationships that apply to the selected tables.
        
        Args:
            table_ids: List of table IDs in the query
            relationship_ids: Optional specific relationship IDs to use.
                             If None, auto-discovers all applicable relationships.
                             If empty list [], returns empty (forces CROSS JOIN).
        
        Returns:
            List of TableRelationship objects
        """
        # If explicitly empty list, return empty (user wants CROSS JOIN)
        if relationship_ids is not None and len(relationship_ids) == 0:
            return []
        
        # Build query for relationships involving the selected tables
        query = select(TableRelationship).where(
            and_(
                TableRelationship.is_active == True,
                or_(
                    TableRelationship.left_table_id.in_(table_ids),
                    TableRelationship.right_table_id.in_(table_ids)
                )
            )
        )
        
        # Filter by specific relationship IDs if provided
        if relationship_ids:
            query = query.where(TableRelationship.id.in_(relationship_ids))
        
        result = await self.db.execute(query)
        relationships = result.scalars().all()
        
        # Filter to only include relationships where BOTH tables are selected
        table_id_set = set(table_ids)
        applicable = [
            r for r in relationships 
            if r.left_table_id in table_id_set and r.right_table_id in table_id_set
        ]
        
        logger.info(f"Found {len(applicable)} applicable relationships for tables {table_ids}")
        return applicable
    
    async def _get_column_name_by_id(self, column_id: int) -> Optional[str]:
        """Get column name by ID."""
        query = select(ExternalColumn.column_name).where(ExternalColumn.id == column_id)
        result = await self.db.execute(query)
        row = result.fetchone()
        return row[0] if row else None

    async def _fetch_table_metadata(
        self,
        table_ids: List[int]
    ) -> Dict[int, Dict[str, Any]]:
        """Fetch metadata for tables."""
        query = select(
            ExternalTables.id,
            ExternalTables.table_name,
            ExternalTables.connection_id,
            ExternalSchema.schema_name,
            DataConnection.name.label('connection_name')
        ).join(
            ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
        ).join(
            DataConnection, ExternalTables.connection_id == DataConnection.id
        ).where(
            ExternalTables.id.in_(table_ids)
        )
        
        result = await self.db.execute(query)
        rows = result.fetchall()
        
        return {
            row.id: {
                'table_id': row.id,
                'table_name': row.table_name,
                'schema_name': row.schema_name,
                'connection_id': row.connection_id,
                'connection_name': row.connection_name
            }
            for row in rows
        }
    
    async def _fetch_column_metadata(
        self,
        column_ids: List[int]
    ) -> Dict[int, Dict[str, Any]]:
        """Fetch metadata for columns."""
        query = select(
            ExternalColumn.id,
            ExternalColumn.column_name,
            ExternalColumn.table_id,
            ExternalColumn.data_type
        ).where(
            ExternalColumn.id.in_(column_ids)
        )
        
        result = await self.db.execute(query)
        rows = result.fetchall()
        
        return {
            row.id: {
                'column_id': row.id,
                'column_name': row.column_name,
                'table_id': row.table_id,
                'data_type': row.data_type
            }
            for row in rows
        }
    
    # ==================== TRANSFORM CONFIG ====================
    
    async def create_transform_config(
        self,
        data: TransformConfigCreate
    ) -> TransformConfigResponse:
        """Create a transform config."""
        
        # Check if name exists
        existing = await self.db.execute(
            select(TransformConfig).where(TransformConfig.name == data.name)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Config with name '{data.name}' already exists"
            )
        
        # Verify Bronze dataset exists
        bronze_result = await self.db.execute(
            select(Dataset).where(Dataset.id == data.source_bronze_dataset_id)
        )
        bronze_dataset = bronze_result.scalar_one_or_none()
        if not bronze_dataset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Bronze dataset with ID {data.source_bronze_dataset_id} not found"
            )
        
        config = TransformConfig(
            name=data.name,
            description=data.description,
            source_bronze_dataset_id=data.source_bronze_dataset_id,
            silver_bucket=data.silver_bucket or self.silver_bucket,
            silver_path_prefix=data.silver_path_prefix,
            column_group_ids=data.column_group_ids,
            filters=data.filters.model_dump() if data.filters else None,
            column_transformations=[t.model_dump() for t in data.column_transformations] if data.column_transformations else None,
            image_labeling_config=data.image_labeling_config.model_dump() if data.image_labeling_config else None,
            exclude_unified_source_columns=data.exclude_unified_source_columns,
            config_snapshot=data.model_dump(),
            is_active=True
        )
        
        self.db.add(config)
        await self.db.commit()
        await self.db.refresh(config)
        
        return await self.get_transform_config(config.id)
    
    async def get_transform_config(self, config_id: int) -> TransformConfigResponse:
        """Get a transform config."""
        result = await self.db.execute(
            select(TransformConfig, Dataset.name.label('bronze_name')).join(
                Dataset, TransformConfig.source_bronze_dataset_id == Dataset.id
            ).where(TransformConfig.id == config_id)
        )
        row = result.first()
        
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Config with ID {config_id} not found"
            )
        
        config, bronze_name = row
        
        return TransformConfigResponse(
            id=config.id,
            name=config.name,
            description=config.description,
            source_bronze_dataset_id=config.source_bronze_dataset_id,
            source_bronze_dataset_name=bronze_name,
            silver_bucket=config.silver_bucket,
            silver_path_prefix=config.silver_path_prefix,
            column_group_ids=config.column_group_ids,
            filters=config.filters,
            column_transformations=config.column_transformations,
            image_labeling_config=config.image_labeling_config,
            exclude_unified_source_columns=config.exclude_unified_source_columns,
            last_execution_time=config.last_execution_time,
            last_execution_status=config.last_execution_status.value if config.last_execution_status else None,
            last_execution_rows=config.last_execution_rows,
            is_active=config.is_active
        )
    
    async def list_transform_configs(
        self,
        include_inactive: bool = False
    ) -> List[TransformConfigResponse]:
        """List all transform configs."""
        query = select(TransformConfig)
        if not include_inactive:
            query = query.where(TransformConfig.is_active == True)
        query = query.order_by(TransformConfig.name)
        
        result = await self.db.execute(query)
        configs = result.scalars().all()
        
        responses = []
        for c in configs:
            resp = await self.get_transform_config(c.id)
            responses.append(resp)
        
        return responses
    
    async def update_transform_config(
        self,
        config_id: int,
        data: TransformConfigUpdate
    ) -> TransformConfigResponse:
        """Update a transform config."""
        result = await self.db.execute(
            select(TransformConfig).where(TransformConfig.id == config_id)
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Config with ID {config_id} not found"
            )
        
        update_data = data.model_dump(exclude_unset=True)
        
        # Convert nested objects
        if 'column_transformations' in update_data and update_data['column_transformations']:
            update_data['column_transformations'] = [
                t.model_dump() if hasattr(t, 'model_dump') else t 
                for t in update_data['column_transformations']
            ]
        if 'filters' in update_data and update_data['filters']:
            if hasattr(update_data['filters'], 'model_dump'):
                update_data['filters'] = update_data['filters'].model_dump()
        if 'image_labeling_config' in update_data and update_data['image_labeling_config']:
            if hasattr(update_data['image_labeling_config'], 'model_dump'):
                update_data['image_labeling_config'] = update_data['image_labeling_config'].model_dump()
        
        for key, value in update_data.items():
            setattr(config, key, value)
        
        await self.db.commit()
        
        return await self.get_transform_config(config_id)
    
    async def delete_transform_config(self, config_id: int) -> bool:
        """Delete a transform config."""
        result = await self.db.execute(
            select(TransformConfig).where(TransformConfig.id == config_id)
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Config with ID {config_id} not found"
            )
        
        await self.db.delete(config)
        await self.db.commit()
        return True
    
    async def preview_transform_config(
        self,
        config_id: int
    ) -> TransformPreviewResponse:
        """Preview a transform config."""
        config = await self.get_transform_config(config_id)
        
        # Get Bronze dataset info
        bronze_result = await self.db.execute(
            select(DatasetBronzeConfig).where(
                DatasetBronzeConfig.dataset_id == config.source_bronze_dataset_id
            )
        )
        bronze_config = bronze_result.scalar_one_or_none()
        
        # Generate SQL transformations
        sql_transforms = []
        python_transforms = []
        output_columns = []
        
        if config.column_transformations:
            for trans in config.column_transformations:
                col_id = trans.get('column_id', '')
                trans_type = trans.get('type', '')
                rule_id = trans.get('rule_id')
                
                output_columns.append(f"column_{col_id}")
                
                if trans_type in ('lowercase', 'uppercase', 'trim', 'normalize_spaces', 'remove_accents'):
                    sql_transforms.append(f"-- column_id {col_id}: {trans_type}")
                elif trans_type == 'template' and rule_id:
                    python_transforms.append(f"column_id {col_id}: template (rule_id={rule_id})")
        
        # Build output path
        output_path = f"s3a://{config.silver_bucket or self.silver_bucket}/"
        if config.silver_path_prefix:
            output_path += config.silver_path_prefix
        else:
            output_path += f"{config.id}-{config.name}/"
        
        return TransformPreviewResponse(
            config_id=config.id,
            config_name=config.name,
            source_bronze_dataset=config.source_bronze_dataset_name or str(config.source_bronze_dataset_id),
            sql_transformations="\n".join(sql_transforms) if sql_transforms else "No SQL transformations",
            python_transformations=python_transforms,
            output_columns=output_columns,
            estimated_rows=None,
            output_path=output_path,
            warnings=[]
        )
    
    async def execute_transform_config(
        self,
        config_id: int
    ) -> TransformExecuteResponse:
        """
        Execute a transform config (Bronze → Silver) using Spark.
        
        This method:
        1. Reads Bronze Delta Lake tables using Spark
        2. Applies column normalizations (Spark UDFs + SQL)
        3. Applies value mappings (Spark expressions)
        4. Applies filters
        5. Writes to Silver Delta Lake
        
        Spark is used instead of Trino because:
        - Better for large-scale ETL operations
        - Native Delta Lake support with full ACID
        - UDF support for Python transformations
        - More efficient write operations
        """
        config = await self.get_transform_config(config_id)
        
        start_time = datetime.now()
        
        # Create execution record
        execution = TransformExecution(
            config_id=config_id,
            status=TransformStatus.RUNNING,
            started_at=start_time
        )
        self.db.add(execution)
        await self.db.flush()
        
        try:
            # Get Bronze dataset info
            bronze_info = await self._get_bronze_dataset_info(config.source_bronze_dataset_id)
            
            if not bronze_info:
                raise ValueError(f"Bronze dataset {config.source_bronze_dataset_id} not found")
            
            # Build output path
            output_bucket = config.silver_bucket or self.silver_bucket
            if config.silver_path_prefix:
                output_path = f"s3a://{output_bucket}/{config.silver_path_prefix}"
            else:
                output_path = f"s3a://{output_bucket}/{config.id}-{config.name}"
            
            # Execute transformation with Spark
            rows_processed, rows_output = await self._execute_spark_transform(
                bronze_info=bronze_info,
                config=config,
                output_path=output_path
            )
            
            # Update execution record
            execution.status = TransformStatus.SUCCESS
            execution.finished_at = datetime.now()
            execution.rows_processed = rows_processed
            execution.rows_output = rows_output
            execution.output_path = output_path
            execution.execution_details = {
                "bronze_tables": bronze_info.get('tables', []),
                "transformations_applied": {
                    "column_transformations": len(config.column_transformations or []),
                    "column_groups": len(config.column_group_ids or []),
                    "filters": 1 if config.filters else 0
                }
            }
            
            # Update config status
            result = await self.db.execute(
                select(TransformConfig).where(TransformConfig.id == config_id)
            )
            config_obj = result.scalar_one()
            config_obj.last_execution_time = datetime.now()
            config_obj.last_execution_status = TransformStatus.SUCCESS
            config_obj.last_execution_rows = rows_output
            config_obj.last_execution_error = None
            
            await self.db.commit()
            
            # Register table in Trino for SQL queries
            trino_registered = await self.register_silver_table_in_trino(
                config_id=config_id,
                config_name=config.name,
                output_path=output_path
            )
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            message = f"Silver transformation completed. {rows_output} rows written to {output_path}"
            if trino_registered:
                table_name = self.trino._sanitize_identifier(f"{config_id}_{config.name}")
                message += f". Table registered in Trino as {self.silver_catalog}.{self.silver_schema}.{table_name}"
            
            return TransformExecuteResponse(
                config_id=config_id,
                config_name=config.name,
                execution_id=execution.id,
                status=TransformStatusEnum.SUCCESS,
                rows_processed=rows_processed,
                rows_output=rows_output,
                output_path=output_path,
                execution_time_seconds=execution_time,
                message=message
            )
            
        except Exception as e:
            logger.error(f"Transform execution failed: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            execution.status = TransformStatus.FAILED
            execution.finished_at = datetime.now()
            execution.error_message = str(e)
            
            result = await self.db.execute(
                select(TransformConfig).where(TransformConfig.id == config_id)
            )
            config_obj = result.scalar_one()
            config_obj.last_execution_time = datetime.now()
            config_obj.last_execution_status = TransformStatus.FAILED
            config_obj.last_execution_error = str(e)
            
            await self.db.commit()
            
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Transform execution failed: {str(e)}"
            )
    
    async def _get_bronze_dataset_info(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        """
        Get Bronze dataset information including paths and column mappings.
        
        Returns:
            Dict with bronze_path, tables, column_mappings, etc.
        """
        # Get Bronze config
        bronze_config_result = await self.db.execute(
            select(DatasetBronzeConfig).where(DatasetBronzeConfig.dataset_id == dataset_id)
        )
        bronze_config = bronze_config_result.scalar_one_or_none()
        
        if not bronze_config:
            return None
        
        # Get ingestion groups
        from ..database.datasets.bronze import DatasetIngestionGroup, InterSourceLink
        
        groups_result = await self.db.execute(
            select(DatasetIngestionGroup).where(
                DatasetIngestionGroup.bronze_config_id == bronze_config.id
            )
        )
        ingestion_groups = groups_result.scalars().all()
        
        # Build table paths
        tables = []
        group_id_to_index = {}  # Map group_id to index for JOIN references
        for idx, group in enumerate(ingestion_groups):
            tables.append({
                'group_id': group.id,
                'group_name': group.group_name,
                'output_path': group.output_path,
                'rows_ingested': group.rows_ingested
            })
            group_id_to_index[group.id] = idx
        
        # Get column mappings for all groups
        group_ids = [g.id for g in ingestion_groups]
        if group_ids:
            mappings_result = await self.db.execute(
                select(BronzeColumnMapping).where(
                    BronzeColumnMapping.ingestion_group_id.in_(group_ids)
                )
            )
            column_mappings = mappings_result.scalars().all()
        else:
            column_mappings = []
        
        # Build column mapping dict: external_column_id -> bronze_column_name
        column_map = {}
        for m in column_mappings:
            column_map[m.external_column_id] = {
                'bronze_column_name': m.bronze_column_name,
                'original_column_name': m.original_column_name,
                'original_table_name': m.original_table_name,
                'data_type': m.data_type
            }
        
        # Get InterSourceLinks for JOIN between groups
        inter_source_links = []
        if group_ids:
            links_result = await self.db.execute(
                select(InterSourceLink).where(
                    InterSourceLink.bronze_config_id == bronze_config.id
                )
            )
            links = links_result.scalars().all()
            
            for link in links:
                inter_source_links.append({
                    'id': link.id,
                    'left_group_id': link.left_group_id,
                    'left_group_index': group_id_to_index.get(link.left_group_id),
                    'left_column_name': link.left_column_name,
                    'right_group_id': link.right_group_id,
                    'right_group_index': group_id_to_index.get(link.right_group_id),
                    'right_column_name': link.right_column_name,
                    'join_strategy': link.join_strategy.value if link.join_strategy else 'full'
                })
            
            if inter_source_links:
                logger.info(f"Found {len(inter_source_links)} InterSourceLinks for Bronze config {bronze_config.id}")
        
        return {
            'config_id': bronze_config.id,
            'dataset_id': dataset_id,
            'bronze_bucket': bronze_config.bronze_bucket,
            'bronze_path_prefix': bronze_config.bronze_path_prefix,
            'tables': tables,
            'column_mappings': column_map,
            'inter_source_links': inter_source_links
        }
    
    async def _execute_spark_transform(
        self,
        bronze_info: Dict[str, Any],
        config: TransformConfigResponse,
        output_path: str
    ) -> Tuple[int, int]:
        """
        Execute the transformation using PySpark.
        
        This runs in a separate thread to avoid blocking the async event loop.
        
        Returns:
            Tuple of (rows_processed, rows_output)
        """
        import asyncio
        from concurrent.futures import ThreadPoolExecutor
        
        # Get transformation config for Spark
        transform_config = {
            'config_id': config.id,
            'column_transformations': config.column_transformations,  # Same format as Virtualized
            'filters': config.filters,  # Inline filters
            'column_group_ids': config.column_group_ids,
            'exclude_unified_source_columns': getattr(config, 'exclude_unified_source_columns', False)
        }
        
        # Load normalization rules from column_transformations
        norm_rules = await self._load_transformation_rules(transform_config.get('column_transformations', []))
        transform_config['normalization_rules'] = norm_rules
        
        # Load column group info from Equivalence
        if transform_config.get('column_group_ids'):
            equiv_data = await self.load_equivalence_for_virtualized(transform_config['column_group_ids'])
            transform_config['equivalence_data'] = equiv_data
        
        # Execute Spark transformation in thread pool
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            rows_processed, rows_output = await loop.run_in_executor(
                executor,
                self._run_spark_transform,
                bronze_info,
                transform_config,
                output_path
            )
        
        return rows_processed, rows_output
    
    def _run_spark_transform(
        self,
        bronze_info: Dict[str, Any],
        transform_config: Dict[str, Any],
        output_path: str
    ) -> Tuple[int, int]:
        """
        Run the Spark transformation (blocking, runs in thread pool).
        
        This is the core transformation logic using PySpark.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType
        
        spark = self.spark_manager.get_or_create_session()
        
        # Read all Bronze tables and union them
        dfs = []
        total_rows = 0
        
        for table in bronze_info.get('tables', []):
            table_path = table['output_path']
            logger.info(f"Reading Bronze table from: {table_path}")
            
            try:
                df = spark.read.format("delta").load(table_path)
                row_count = df.count()
                total_rows += row_count
                dfs.append(df)
                logger.info(f"Read {row_count} rows from {table_path}")
            except Exception as e:
                logger.error(f"Error reading Bronze table {table_path}: {e}")
                continue
        
        if not dfs:
            logger.warning("No Bronze tables found to transform")
            return 0, 0
        
        # Combine DataFrames using JOINs (if InterSourceLinks exist) or UNION
        inter_source_links = bronze_info.get('inter_source_links', [])
        
        if len(dfs) == 1:
            df = dfs[0]
        elif inter_source_links:
            # Use JOINs based on InterSourceLinks
            df = self._join_dataframes_with_links(dfs, inter_source_links, bronze_info)
            logger.info(f"Joined {len(dfs)} DataFrames using {len(inter_source_links)} InterSourceLinks")
        else:
            # Fallback to UNION if no InterSourceLinks defined
            # This preserves backward compatibility but may not be correct semantically
            logger.warning("No InterSourceLinks found - using UNION (may produce incorrect results)")
            df = dfs[0]
            for other_df in dfs[1:]:
                df = df.unionByName(other_df, allowMissingColumns=True)
        
        logger.info(f"Total rows to process: {total_rows}")
        
        # Apply filters FIRST (before transformations) - consistent with Virtualized behavior
        # In SQL (Virtualized), WHERE clause filters original data before SELECT transforms
        # We do the same here: filter on original values, then transform
        df = self._apply_spark_filters(df, transform_config)
        
        # Apply column_transformations (same format as Virtualized, uses column_id)
        # This resolves column_id → bronze_column_name using BronzeColumnMapping
        df = self._apply_column_transformations(df, transform_config, bronze_info)
        
        # Apply column_group_ids from Equivalence module
        # This handles both column unification and value mappings from Equivalence
        df = self._apply_equivalence_groups(df, transform_config, bronze_info)
        
        # Add Silver metadata columns
        df = df.withColumn("_silver_timestamp", F.current_timestamp())
        df = df.withColumn("_transform_config_id", F.lit(transform_config.get('config_id')))
        
        # Write to Silver Delta Lake
        logger.info(f"Writing Silver Delta to: {output_path}")
        
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("mergeSchema", "true") \
            .save(output_path)
        
        # Get output row count
        output_df = spark.read.format("delta").load(output_path)
        rows_output = output_df.count()
        
        logger.info(f"Silver transformation complete: {total_rows} rows processed, {rows_output} rows written")
        
        return total_rows, rows_output
    
    def _join_dataframes_with_links(
        self,
        dfs: List,
        inter_source_links: List[Dict[str, Any]],
        bronze_info: Dict[str, Any]
    ):
        """
        Join DataFrames using InterSourceLinks.
        
        This method:
        1. Uses the InterSourceLinks to determine JOIN conditions
        2. Applies the correct join type (inner, left, right, full)
        3. Unifies JOIN columns automatically (drops duplicates, keeps one)
        
        Args:
            dfs: List of Spark DataFrames (one per ingestion group)
            inter_source_links: List of link definitions from Bronze
            bronze_info: Bronze dataset info including tables
        
        Returns:
            Joined DataFrame
        """
        from pyspark.sql import functions as F
        
        if len(dfs) == 0:
            return None
        if len(dfs) == 1:
            return dfs[0]
        
        # Build a map of group_index -> DataFrame
        # (dfs are in order of bronze_info['tables'])
        df_map = {i: df for i, df in enumerate(dfs)}
        
        # Track which DataFrames have been joined
        joined_indices = set()
        
        # Track columns to drop after join (duplicates from JOIN keys)
        columns_to_unify = []  # [(left_col, right_col), ...]
        
        # Start with the first DataFrame
        result_df = dfs[0]
        joined_indices.add(0)
        
        # Sort links to ensure we can build the join graph
        remaining_links = list(inter_source_links)
        max_iterations = len(remaining_links) * 2
        iteration = 0
        
        while remaining_links and iteration < max_iterations:
            iteration += 1
            made_progress = False
            
            for link in list(remaining_links):
                left_idx = link.get('left_group_index')
                right_idx = link.get('right_group_index')
                
                if left_idx is None or right_idx is None:
                    remaining_links.remove(link)
                    made_progress = True
                    continue
                
                # Check if we can apply this link
                left_in = left_idx in joined_indices
                right_in = right_idx in joined_indices
                
                if left_in and right_in:
                    # Both already joined, skip
                    remaining_links.remove(link)
                    made_progress = True
                    continue
                elif left_in:
                    join_df = df_map.get(right_idx)
                    join_idx = right_idx
                elif right_in:
                    join_df = df_map.get(left_idx)
                    join_idx = left_idx
                else:
                    # Neither in joined set yet, skip for now
                    continue
                
                if join_df is None:
                    remaining_links.remove(link)
                    made_progress = True
                    continue
                
                # Get join columns
                left_col = link['left_column_name']
                right_col = link['right_column_name']
                
                # Determine join type
                join_strategy = link.get('join_strategy', 'full')
                join_type_map = {
                    'inner': 'inner',
                    'left': 'left',
                    'right': 'right',
                    'full': 'full'
                }
                spark_join_type = join_type_map.get(join_strategy, 'full')
                
                # Check if columns exist in DataFrames
                result_cols = [c.lower() for c in result_df.columns]
                join_cols = [c.lower() for c in join_df.columns]
                
                if left_col.lower() not in result_cols and right_col.lower() not in result_cols:
                    logger.warning(f"Neither {left_col} nor {right_col} found in result DataFrame")
                    remaining_links.remove(link)
                    made_progress = True
                    continue
                
                if left_col.lower() not in join_cols and right_col.lower() not in join_cols:
                    logger.warning(f"Neither {left_col} nor {right_col} found in join DataFrame")
                    remaining_links.remove(link)
                    made_progress = True
                    continue
                
                # Build join condition
                # Find actual column names (case-sensitive)
                result_join_col = None
                for c in result_df.columns:
                    if c.lower() == left_col.lower() or c.lower() == right_col.lower():
                        result_join_col = c
                        break
                
                join_df_join_col = None
                for c in join_df.columns:
                    if c.lower() == left_col.lower() or c.lower() == right_col.lower():
                        join_df_join_col = c
                        break
                
                if not result_join_col or not join_df_join_col:
                    logger.warning(f"Could not find join columns for link {link['id']}")
                    remaining_links.remove(link)
                    made_progress = True
                    continue
                
                # Rename join column in joining DF to avoid ambiguity
                temp_col_name = f"_join_temp_{join_df_join_col}"
                join_df_renamed = join_df.withColumnRenamed(join_df_join_col, temp_col_name)
                
                # Perform the join
                logger.info(f"Joining on {result_join_col} = {temp_col_name} ({spark_join_type})")
                result_df = result_df.join(
                    join_df_renamed,
                    result_df[result_join_col] == join_df_renamed[temp_col_name],
                    spark_join_type
                )
                
                # Unify the join columns using COALESCE (important for FULL OUTER JOIN)
                # This ensures we don't lose data when one side is NULL
                unified_col_name = result_join_col  # Keep the original name
                result_df = result_df.withColumn(
                    unified_col_name,
                    F.coalesce(F.col(result_join_col), F.col(temp_col_name))
                )
                
                # Drop the temporary join column (now unified)
                result_df = result_df.drop(temp_col_name)
                
                joined_indices.add(join_idx)
                remaining_links.remove(link)
                made_progress = True
                logger.info(f"Joined DataFrame {join_idx}, now have {len(joined_indices)} DataFrames joined")
            
            if not made_progress:
                if remaining_links:
                    logger.warning(f"Could not apply {len(remaining_links)} InterSourceLinks - disconnected groups")
                break
        
        # Handle DataFrames that couldn't be joined (add as CROSS JOIN with warning)
        unjoined = set(range(len(dfs))) - joined_indices
        for idx in unjoined:
            logger.warning(f"DataFrame {idx} has no InterSourceLink - using CROSS JOIN")
            result_df = result_df.crossJoin(df_map[idx])
        
        return result_df
    
    def _apply_equivalence_groups(
        self, 
        df, 
        transform_config: Dict[str, Any],
        bronze_info: Dict[str, Any]
    ):
        """
        Apply column_group_ids from Equivalence module.
        
        This method:
        1. Resolves external_column_id to bronze_column_name using BronzeColumnMapping
        2. Applies column unification (COALESCE columns with same unified name)
        3. Applies value mappings from Equivalence (M→Masculino, etc.)
        
        This is the same logic as Virtualized but adapted for Bronze column names.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType
        
        equiv_data = transform_config.get('equivalence_data')
        if not equiv_data:
            return df
        
        # Get bronze column mappings: external_column_id -> bronze_column_name
        bronze_column_map = bronze_info.get('column_mappings', {})
        
        # Get Equivalence data
        unified_columns = equiv_data.get('unified_columns', {})  # col_id -> unified_name
        value_mappings_by_column = equiv_data.get('value_mappings_by_column', {})  # col_id -> {from: to}
        groups = equiv_data.get('groups', [])
        
        logger.info(f"Applying Equivalence: {len(unified_columns)} unified columns, "
                    f"{len(value_mappings_by_column)} columns with value mappings")
        
        # Build mapping: unified_name -> [bronze_column_names]
        columns_by_unified_name = {}
        
        for external_col_id, unified_name in unified_columns.items():
            # Convert external_column_id to int if string
            ext_col_id = int(external_col_id) if isinstance(external_col_id, str) else external_col_id
            
            # Resolve external_column_id to bronze_column_name
            bronze_mapping = bronze_column_map.get(ext_col_id)
            if bronze_mapping:
                bronze_col_name = bronze_mapping.get('bronze_column_name')
                if bronze_col_name and bronze_col_name in df.columns:
                    if unified_name not in columns_by_unified_name:
                        columns_by_unified_name[unified_name] = []
                    columns_by_unified_name[unified_name].append({
                        'bronze_col': bronze_col_name,
                        'external_col_id': ext_col_id
                    })
        
        # Apply value mappings first (before unification)
        for external_col_id, mappings in value_mappings_by_column.items():
            ext_col_id = int(external_col_id) if isinstance(external_col_id, str) else external_col_id
            
            bronze_mapping = bronze_column_map.get(ext_col_id)
            if not bronze_mapping:
                continue
            
            bronze_col_name = bronze_mapping.get('bronze_column_name')
            if not bronze_col_name or bronze_col_name not in df.columns:
                continue
            
            if mappings:
                # Build CASE expression for value mapping
                case_expr = F.col(bronze_col_name)
                for from_val, to_val in mappings.items():
                    case_expr = F.when(
                        F.col(bronze_col_name) == from_val,
                        F.lit(to_val)
                    ).otherwise(case_expr)
                
                # Apply mapping to the column
                df = df.withColumn(bronze_col_name, case_expr)
                logger.info(f"Applied Equivalence value mapping to {bronze_col_name} ({len(mappings)} mappings)")
        
        # Check if we should exclude source columns after unification
        exclude_source_columns = transform_config.get('exclude_unified_source_columns', False)
        columns_to_drop = set()  # Track source columns to drop
        
        # Apply column unification (COALESCE)
        for unified_name, col_infos in columns_by_unified_name.items():
            if len(col_infos) > 1:
                # Multiple columns to unify - use COALESCE
                bronze_cols = [c['bronze_col'] for c in col_infos]
                coalesce_expr = F.coalesce(*[F.col(c) for c in bronze_cols])
                df = df.withColumn(unified_name, coalesce_expr)
                logger.info(f"Unified columns {bronze_cols} into '{unified_name}'")
                
                # Mark source columns for potential removal
                if exclude_source_columns:
                    for bronze_col in bronze_cols:
                        if bronze_col != unified_name:  # Don't drop if same name
                            columns_to_drop.add(bronze_col)
                            
            elif len(col_infos) == 1:
                # Single column - just alias
                bronze_col = col_infos[0]['bronze_col']
                if bronze_col != unified_name:
                    df = df.withColumn(unified_name, F.col(bronze_col))
                    logger.info(f"Aliased column {bronze_col} as '{unified_name}'")
                    
                    # Mark source column for potential removal
                    if exclude_source_columns:
                        columns_to_drop.add(bronze_col)
        
        # Drop source columns if configured
        if exclude_source_columns and columns_to_drop:
            # Only drop columns that exist in the DataFrame
            cols_to_actually_drop = [c for c in columns_to_drop if c in df.columns]
            if cols_to_actually_drop:
                df = df.drop(*cols_to_actually_drop)
                logger.info(f"Excluded {len(cols_to_actually_drop)} source columns after unification: {cols_to_actually_drop}")
        
        return df
    
    def _apply_spark_filters(self, df, transform_config: Dict[str, Any]):
        """Apply inline filters to the DataFrame."""
        from pyspark.sql import functions as F
        
        filters = transform_config.get('filters')
        
        if not filters:
            return df
        
        # Inline filter is a single object with logic and conditions
        logic = filters.get('logic', 'AND')
        conditions = filters.get('conditions', [])
        
        if not conditions:
            return df
        
        # Build filter conditions
        spark_conditions = []
        
        for cond in conditions:
            col_name = cond.get('column_name')
            if not col_name or col_name not in df.columns:
                continue
            
            operator = cond.get('operator', '=')
            value = cond.get('value')
            value_min = cond.get('value_min')
            value_max = cond.get('value_max')
            
            col_expr = F.col(col_name)
            
            if operator == '=':
                spark_conditions.append(col_expr == value)
            elif operator == '!=':
                spark_conditions.append(col_expr != value)
            elif operator == '>':
                spark_conditions.append(col_expr > value)
            elif operator == '>=':
                spark_conditions.append(col_expr >= value)
            elif operator == '<':
                spark_conditions.append(col_expr < value)
            elif operator == '<=':
                spark_conditions.append(col_expr <= value)
            elif operator == 'LIKE':
                spark_conditions.append(col_expr.like(value))
            elif operator == 'ILIKE':
                spark_conditions.append(F.lower(col_expr).like(value.lower() if value else ''))
            elif operator in ('IN', 'NOT IN'):
                import json
                try:
                    values_list = json.loads(value) if isinstance(value, str) else value
                    if operator == 'IN':
                        spark_conditions.append(col_expr.isin(values_list))
                    else:
                        spark_conditions.append(~col_expr.isin(values_list))
                except Exception:
                    pass
            elif operator == 'IS NULL':
                spark_conditions.append(col_expr.isNull())
            elif operator == 'IS NOT NULL':
                spark_conditions.append(col_expr.isNotNull())
            elif operator == 'BETWEEN':
                if value_min is not None and value_max is not None:
                    spark_conditions.append(col_expr.between(value_min, value_max))
        
        if not spark_conditions:
            return df
        
        # Combine conditions with AND or OR
        if logic == 'OR':
            combined = spark_conditions[0]
            for cond in spark_conditions[1:]:
                combined = combined | cond
        else:
            combined = spark_conditions[0]
            for cond in spark_conditions[1:]:
                combined = combined & cond
        
        df = df.filter(combined)
        logger.info(f"Applied inline filter with {len(conditions)} conditions using {logic} logic")
        
        return df
    
    async def _load_transformation_rules(self, transformations: Optional[List]) -> Dict[int, Dict]:
        """
        Load normalization rules from column_transformations.
        
        Same as _load_normalization_rules but for column_transformations format.
        """
        if not transformations:
            return {}
        
        rule_ids = set()
        for trans in transformations:
            if trans.get('rule_id'):
                rule_ids.add(trans['rule_id'])
        
        if not rule_ids:
            return {}
        
        rules_result = await self.db.execute(
            select(NormalizationRule).where(NormalizationRule.id.in_(rule_ids))
        )
        
        rules = {}
        for rule in rules_result.scalars():
            rules[rule.id] = {
                'name': rule.name,
                'template': rule.template,
                'regex_pattern': rule.regex_pattern,
                'regex_replacement': rule.regex_replacement,
                'pre_process': rule.pre_process
            }
        
        return rules
    
    def _apply_column_transformations(
        self,
        df,
        transform_config: Dict[str, Any],
        bronze_info: Dict[str, Any]
    ):
        """
        Apply column_transformations (same format as Virtualized).
        
        This method:
        1. Resolves column_id (external_column.id) to bronze_column_name
        2. Applies the transformation (template, lowercase, uppercase, etc.)
        
        This enables using the same format as VirtualizedConfig.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType
        
        transformations = transform_config.get('column_transformations', [])
        if not transformations:
            return df
        
        # Get bronze column mappings: external_column_id -> bronze_column_name
        bronze_column_map = bronze_info.get('column_mappings', {})
        norm_rules = transform_config.get('normalization_rules', {})
        
        for trans in transformations:
            column_id = trans.get('column_id')
            trans_type = trans.get('type', '')
            rule_id = trans.get('rule_id')
            
            if not column_id:
                continue
            
            # Resolve column_id to bronze_column_name
            bronze_mapping = bronze_column_map.get(column_id)
            if not bronze_mapping:
                logger.warning(f"Column ID {column_id} not found in BronzeColumnMapping")
                continue
            
            col_name = bronze_mapping.get('bronze_column_name')
            if not col_name or col_name not in df.columns:
                logger.warning(f"Bronze column {col_name} not found in DataFrame")
                continue
            
            logger.info(f"Applying {trans_type} transformation to column_id {column_id} -> {col_name}")
            
            # Apply transformation based on type
            if trans_type == 'template' and rule_id and rule_id in norm_rules:
                rule = norm_rules[rule_id]
                if rule.get('regex_pattern') and rule.get('regex_replacement'):
                    df = df.withColumn(
                        col_name,
                        F.regexp_replace(
                            F.col(col_name),
                            rule['regex_pattern'],
                            rule['regex_replacement']
                        )
                    )
                elif rule.get('template') and self.normalizer:
                    normalizer = self.normalizer
                    rule_name = rule['name']
                    
                    def apply_rule(value):
                        if value is None:
                            return None
                        try:
                            return normalizer.normalize(str(value), rule_name)
                        except Exception:
                            return value
                    
                    apply_rule_udf = F.udf(apply_rule, StringType())
                    df = df.withColumn(col_name, apply_rule_udf(F.col(col_name)))
            
            elif trans_type == 'lowercase':
                df = df.withColumn(col_name, F.lower(F.col(col_name)))
            
            elif trans_type == 'uppercase':
                df = df.withColumn(col_name, F.upper(F.col(col_name)))
            
            elif trans_type == 'trim':
                df = df.withColumn(col_name, F.trim(F.col(col_name)))
            
            elif trans_type == 'normalize_spaces':
                df = df.withColumn(
                    col_name,
                    F.trim(F.regexp_replace(F.col(col_name), r'\s+', ' '))
                )
            
            elif trans_type == 'remove_accents':
                def remove_accents(text):
                    if text is None:
                        return None
                    import unicodedata
                    return ''.join(
                        c for c in unicodedata.normalize('NFD', text)
                        if unicodedata.category(c) != 'Mn'
                    )
                
                remove_accents_udf = F.udf(remove_accents, StringType())
                df = df.withColumn(col_name, remove_accents_udf(F.col(col_name)))
            
            logger.info(f"Applied {trans_type} to {col_name}")
        
        return df
    
    async def register_silver_table_in_trino(
        self,
        config_id: int,
        config_name: str,
        output_path: str
    ) -> bool:
        """
        Register a Silver Delta table in Trino's silver catalog.
        
        This allows querying the Silver table via Trino SQL.
        
        Args:
            config_id: Transform config ID
            config_name: Transform config name
            output_path: S3A path to the Silver Delta table
            
        Returns:
            True if successful, False otherwise
        """
        # Generate a valid table name
        table_name = self.trino._sanitize_identifier(f"{config_id}_{config_name}")
        
        try:
            conn = await self.trino.get_connection()
            cur = await conn.cursor()
            
            # First, ensure silver catalog/schema exists
            await self._ensure_silver_schema_async()
            
            # Check if table already exists
            await cur.execute(
                f"SHOW TABLES FROM {self.silver_catalog}.{self.silver_schema} LIKE '{table_name}'"
            )
            existing = await cur.fetchall()
            
            if existing:
                # Drop existing table to recreate with updated data
                logger.info(f"Dropping existing Silver table: {table_name}")
                await cur.execute(
                    f'DROP TABLE IF EXISTS "{self.silver_catalog}"."{self.silver_schema}"."{table_name}"'
                )
                await cur.fetchall()
            
            # Register the Delta table using call procedure
            # Note: Delta Lake connector supports registering external tables
            register_sql = f"""
                CALL {self.silver_catalog}.system.register_table(
                    schema_name => '{self.silver_schema}',
                    table_name => '{table_name}',
                    table_location => '{output_path}'
                )
            """
            
            logger.info(f"Registering Silver table: {register_sql}")
            await cur.execute(register_sql)
            await cur.fetchall()
            
            await conn.close()
            
            logger.info(f"Successfully registered Silver table: {self.silver_catalog}.{self.silver_schema}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register Silver table in Trino: {e}")
            return False
    
    async def _ensure_silver_schema_async(self):
        """Create the silver schema in Trino if it doesn't exist."""
        try:
            conn = await self.trino.get_connection()
            cur = await conn.cursor()
            
            # Check if silver catalog exists
            await cur.execute(f"SHOW CATALOGS LIKE '{self.silver_catalog}'")
            catalogs = await cur.fetchall()
            
            if not catalogs:
                logger.warning(
                    f"Silver catalog '{self.silver_catalog}' does not exist in Trino. "
                    f"Make sure silver.properties is configured in trino/catalog/"
                )
                await conn.close()
                return
            
            # Check if schema exists
            await cur.execute(f"SHOW SCHEMAS FROM {self.silver_catalog} LIKE '{self.silver_schema}'")
            schemas = await cur.fetchall()
            
            if not schemas:
                # Create schema with location pointing to silver bucket
                create_schema_sql = f"""
                    CREATE SCHEMA IF NOT EXISTS {self.silver_catalog}.{self.silver_schema}
                    WITH (location = 's3a://{self.silver_bucket}/')
                """
                logger.info(f"Creating silver schema with SQL: {create_schema_sql}")
                await cur.execute(create_schema_sql)
                await cur.fetchall()
                logger.info(f"Created silver schema: {self.silver_catalog}.{self.silver_schema}")
            
            await conn.close()
        except Exception as e:
            logger.error(f"Failed to ensure silver schema exists: {e}")
    
    async def get_silver_table_info(self, config_id: int) -> Optional[Dict[str, Any]]:
        """
        Get information about a registered Silver table.
        
        Returns table metadata including path, schema, and row count.
        """
        config = await self.get_transform_config(config_id)
        
        table_name = self.trino._sanitize_identifier(f"{config_id}_{config.name}")
        
        try:
            conn = await self.trino.get_connection()
            cur = await conn.cursor()
            
            # Get table columns
            await cur.execute(
                f'DESCRIBE "{self.silver_catalog}"."{self.silver_schema}"."{table_name}"'
            )
            columns = await cur.fetchall()
            
            # Get row count
            await cur.execute(
                f'SELECT COUNT(*) FROM "{self.silver_catalog}"."{self.silver_schema}"."{table_name}"'
            )
            count_result = await cur.fetchone()
            row_count = count_result[0] if count_result else 0
            
            await conn.close()
            
            return {
                'catalog': self.silver_catalog,
                'schema': self.silver_schema,
                'table_name': table_name,
                'full_path': f'{self.silver_catalog}.{self.silver_schema}.{table_name}',
                'columns': [
                    {'name': col[0], 'type': col[1]}
                    for col in columns
                ],
                'row_count': row_count
            }
            
        except Exception as e:
            logger.error(f"Error getting Silver table info: {e}")
            return None
    
    async def query_silver_table(
        self,
        config_id: int,
        limit: int = 1000,
        offset: int = 0
    ) -> Optional[Dict[str, Any]]:
        """
        Query a Silver table via Trino.
        
        Returns data from the Silver Delta Lake table.
        """
        config = await self.get_transform_config(config_id)
        table_name = self.trino._sanitize_identifier(f"{config_id}_{config.name}")
        
        try:
            conn = await self.trino.get_connection()
            cur = await conn.cursor()
            
            # Execute query
            query = f"""
                SELECT * FROM "{self.silver_catalog}"."{self.silver_schema}"."{table_name}"
                LIMIT {limit}
            """
            if offset > 0:
                query += f" OFFSET {offset}"
            
            await cur.execute(query)
            rows = await cur.fetchall()
            
            # Get column names
            description = await cur.get_description()
            columns = [
                col.name if hasattr(col, 'name') else col[0]
                for col in description
            ]
            
            # Convert to dicts
            data = []
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    col_name = columns[i] if i < len(columns) else f'col_{i}'
                    if value is not None and not isinstance(value, (str, int, float, bool, list, dict)):
                        value = str(value)
                    row_dict[col_name] = value
                data.append(row_dict)
            
            await conn.close()
            
            return {
                'columns': columns,
                'data': data,
                'row_count': len(data)
            }
            
        except Exception as e:
            logger.error(f"Error querying Silver table: {e}")
            return None

