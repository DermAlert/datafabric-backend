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
from ..database.datasets.bronze import DatasetBronzeConfig, BronzeColumnMapping
from ..database.datasets.silver import (
    NormalizationRule,
    SilverFilter,
    SilverFilterCondition,
    VirtualizedConfig,
    TransformConfig,
    TransformExecution,
    TransformStatus,
    FilterOperator,
    FilterLogic,
    NormalizationType,
)
from ..database.equivalence.equivalence import ColumnGroup, ColumnMapping
from ..api.schemas.silver_schemas import (
    NormalizationRuleCreate,
    NormalizationRuleUpdate,
    NormalizationRuleResponse,
    NormalizationRuleTest,
    NormalizationRuleTestResult,
    SilverFilterCreate,
    SilverFilterUpdate,
    SilverFilterResponse,
    FilterConditionResponse,
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
        self.silver_bucket = os.getenv("SILVER_BUCKET", "datafabric-silver")
        
        # Initialize Python normalizer if available
        self.normalizer = Normalizer() if Normalizer else None
        if self.normalizer:
            self.normalizer.load_default_rules()
    
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
    
    # ==================== FILTERS ====================
    
    async def create_filter(
        self,
        data: SilverFilterCreate
    ) -> SilverFilterResponse:
        """Create a reusable filter."""
        
        # Check if name exists
        existing = await self.db.execute(
            select(SilverFilter).where(SilverFilter.name == data.name)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Filter with name '{data.name}' already exists"
            )
        
        # Create filter
        filter_obj = SilverFilter(
            name=data.name,
            description=data.description,
            logic=FilterLogic(data.logic.value),
            is_active=True
        )
        self.db.add(filter_obj)
        await self.db.flush()
        
        # Create conditions
        for i, cond_data in enumerate(data.conditions):
            condition = SilverFilterCondition(
                filter_id=filter_obj.id,
                column_id=cond_data.column_id,
                column_name=cond_data.column_name,
                operator=FilterOperator(cond_data.operator.value),
                value=str(cond_data.value) if cond_data.value is not None else None,
                value_min=str(cond_data.value_min) if cond_data.value_min is not None else None,
                value_max=str(cond_data.value_max) if cond_data.value_max is not None else None,
                condition_order=i
            )
            self.db.add(condition)
        
        await self.db.commit()
        
        return await self.get_filter(filter_obj.id)
    
    async def get_filter(self, filter_id: int) -> SilverFilterResponse:
        """Get a filter with conditions."""
        result = await self.db.execute(
            select(SilverFilter).where(SilverFilter.id == filter_id)
        )
        filter_obj = result.scalar_one_or_none()
        
        if not filter_obj:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Filter with ID {filter_id} not found"
            )
        
        # Get conditions
        cond_result = await self.db.execute(
            select(SilverFilterCondition).where(
                SilverFilterCondition.filter_id == filter_id
            ).order_by(SilverFilterCondition.condition_order)
        )
        conditions = cond_result.scalars().all()
        
        return SilverFilterResponse(
            id=filter_obj.id,
            name=filter_obj.name,
            description=filter_obj.description,
            logic=filter_obj.logic.value,
            is_active=filter_obj.is_active,
            conditions=[
                FilterConditionResponse(
                    id=c.id,
                    column_id=c.column_id,
                    column_name=c.column_name,
                    operator=c.operator.value,
                    value=c.value,
                    value_min=c.value_min,
                    value_max=c.value_max
                )
                for c in conditions
            ]
        )
    
    async def list_filters(
        self,
        include_inactive: bool = False
    ) -> List[SilverFilterResponse]:
        """List all filters."""
        query = select(SilverFilter)
        if not include_inactive:
            query = query.where(SilverFilter.is_active == True)
        query = query.order_by(SilverFilter.name)
        
        result = await self.db.execute(query)
        filters = result.scalars().all()
        
        responses = []
        for f in filters:
            resp = await self.get_filter(f.id)
            responses.append(resp)
        
        return responses
    
    async def update_filter(
        self,
        filter_id: int,
        data: SilverFilterUpdate
    ) -> SilverFilterResponse:
        """Update a filter."""
        result = await self.db.execute(
            select(SilverFilter).where(SilverFilter.id == filter_id)
        )
        filter_obj = result.scalar_one_or_none()
        
        if not filter_obj:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Filter with ID {filter_id} not found"
            )
        
        update_data = data.model_dump(exclude_unset=True)
        if 'logic' in update_data:
            update_data['logic'] = FilterLogic(update_data['logic'])
        
        for key, value in update_data.items():
            setattr(filter_obj, key, value)
        
        await self.db.commit()
        
        return await self.get_filter(filter_id)
    
    async def delete_filter(self, filter_id: int) -> bool:
        """Delete a filter."""
        result = await self.db.execute(
            select(SilverFilter).where(SilverFilter.id == filter_id)
        )
        filter_obj = result.scalar_one_or_none()
        
        if not filter_obj:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Filter with ID {filter_id} not found"
            )
        
        await self.db.delete(filter_obj)
        await self.db.commit()
        return True
    
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
            filter_ids=data.filter_ids,
            column_transformations=[t.model_dump() for t in data.column_transformations] if data.column_transformations else None,
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
        
        # ==================== BUILD SELECT COLUMNS ====================
        select_columns = []
        output_columns = []
        processed_unified_names = set()  # Track unified names already added
        columns_for_unification = {}  # unified_name -> [col_expressions]
        
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
            
            # Check if this column should be unified (from Equivalence)
            if col_id in unified_columns:
                unified_name = unified_columns[col_id]
                
                # Apply value mapping before unification (from Equivalence)
                col_expr_with_mapping = build_col_expr_with_mapping(col_expr, col_id)
                
                if unified_name not in columns_for_unification:
                    columns_for_unification[unified_name] = []
                columns_for_unification[unified_name].append(col_expr_with_mapping)
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
        
        # Build FROM clause (using table_aliases created earlier)
        from_parts = []
        for table_id in table_ids:
            table_info = table_metadata.get(table_id, {})
            catalog = self.trino.generate_catalog_name(
                table_info.get('connection_name', ''),
                table_info.get('connection_id', 0)
            )
            alias = table_aliases.get(table_id, f"t{table_ids.index(table_id)}")
            from_parts.append(
                f'"{catalog}"."{table_info.get("schema_name", "")}"."{table_info.get("table_name", "")}" {alias}'
            )
        
        # Build WHERE clause from filters
        where_clauses = []
        if config.filter_ids:
            for filter_id in config.filter_ids:
                filter_sql = await self._filter_to_sql(filter_id, column_metadata)
                if filter_sql:
                    where_clauses.append(f"({filter_sql})")
        
        # Assemble SQL
        if len(from_parts) == 1:
            sql = f"SELECT {', '.join(select_columns)}"
            sql += f" FROM {from_parts[0]}"
        else:
            # Multiple tables - use CROSS JOIN for now (user should define relationships)
            sql = f"SELECT {', '.join(select_columns)}"
            sql += f" FROM {' CROSS JOIN '.join(from_parts)}"
            warnings.append("Multiple tables without relationships result in CROSS JOIN. Consider using Bronze with relationships instead.")
        
        if where_clauses:
            sql += f" WHERE {' AND '.join(where_clauses)}"
        
        return sql, output_columns, warnings
    
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
    
    async def _filter_to_sql(
        self,
        filter_id: int,
        column_metadata: Dict[int, Dict[str, Any]]
    ) -> Optional[str]:
        """Convert a filter to SQL WHERE clause."""
        filter_resp = await self.get_filter(filter_id)
        
        if not filter_resp.conditions:
            return None
        
        def format_value(val: str) -> str:
            """Format value for SQL - no quotes for numbers, quotes for strings."""
            if val is None:
                return "NULL"
            # Check if it's a number
            try:
                float(val)
                return val  # No quotes for numeric values
            except (ValueError, TypeError):
                # Escape single quotes and wrap in quotes
                escaped = val.replace("'", "''")
                return f"'{escaped}'"
        
        condition_sqls = []
        for cond in filter_resp.conditions:
            col_ref = None
            if cond.column_id and cond.column_id in column_metadata:
                col_ref = f'"{column_metadata[cond.column_id]["column_name"]}"'
            elif cond.column_name:
                col_ref = f'"{cond.column_name}"'
            else:
                continue
            
            op = cond.operator
            if op in ('IS NULL', 'IS NOT NULL'):
                condition_sqls.append(f"{col_ref} {op}")
            elif op == 'BETWEEN':
                val_min = format_value(cond.value_min)
                val_max = format_value(cond.value_max)
                condition_sqls.append(f"{col_ref} BETWEEN {val_min} AND {val_max}")
            elif op in ('IN', 'NOT IN'):
                # Parse value as list
                import json
                try:
                    values = json.loads(cond.value) if cond.value else []
                    values_str = ", ".join([format_value(str(v)) for v in values])
                    condition_sqls.append(f"{col_ref} {op} ({values_str})")
                except:
                    pass
            else:
                formatted_val = format_value(cond.value)
                condition_sqls.append(f"{col_ref} {op} {formatted_val}")
        
        if not condition_sqls:
            return None
        
        logic = filter_resp.logic
        return f" {logic} ".join(condition_sqls)
    
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
            semantic_columns=[s.model_dump() for s in data.semantic_columns] if data.semantic_columns else None,
            filter_ids=data.filter_ids,
            column_normalizations=[n.model_dump() for n in data.column_normalizations] if data.column_normalizations else None,
            value_mappings=[v.model_dump() for v in data.value_mappings] if data.value_mappings else None,
            image_labeling_config=data.image_labeling_config.model_dump() if data.image_labeling_config else None,
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
            semantic_columns=config.semantic_columns,
            filter_ids=config.filter_ids,
            column_normalizations=config.column_normalizations,
            value_mappings=config.value_mappings,
            image_labeling_config=config.image_labeling_config,
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
        if 'column_normalizations' in update_data and update_data['column_normalizations']:
            update_data['column_normalizations'] = [
                n.model_dump() if hasattr(n, 'model_dump') else n 
                for n in update_data['column_normalizations']
            ]
        if 'value_mappings' in update_data and update_data['value_mappings']:
            update_data['value_mappings'] = [
                v.model_dump() if hasattr(v, 'model_dump') else v 
                for v in update_data['value_mappings']
            ]
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
        
        if config.column_normalizations:
            for norm in config.column_normalizations:
                col = norm.get('column_name', '')
                output_col = norm.get('output_column', col)
                output_columns.append(output_col)
                
                norm_type = norm.get('type', '')
                if norm_type in ('sql_regex', 'case_mapping'):
                    sql_transforms.append(f"-- {col} → {output_col}: {norm_type}")
                elif norm_type in ('rule', 'template', 'python_rule'):
                    python_transforms.append(f"{col} → {output_col}: {norm_type}")
        
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
        """Execute a transform config (Bronze → Silver)."""
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
            # TODO: Implement actual transformation logic
            # 1. Read from Bronze Delta
            # 2. Apply SQL transformations via Trino
            # 3. Apply Python transformations (normalization rules)
            # 4. Write to Silver Delta
            
            # For now, return placeholder
            rows_processed = 0
            rows_output = 0
            output_path = f"s3a://{config.silver_bucket or self.silver_bucket}/{config.id}-{config.name}/"
            
            # Update execution
            execution.status = TransformStatus.SUCCESS
            execution.finished_at = datetime.now()
            execution.rows_processed = rows_processed
            execution.rows_output = rows_output
            execution.output_path = output_path
            
            # Update config
            result = await self.db.execute(
                select(TransformConfig).where(TransformConfig.id == config_id)
            )
            config_obj = result.scalar_one()
            config_obj.last_execution_time = datetime.now()
            config_obj.last_execution_status = TransformStatus.SUCCESS
            config_obj.last_execution_rows = rows_output
            
            await self.db.commit()
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return TransformExecuteResponse(
                config_id=config_id,
                config_name=config.name,
                execution_id=execution.id,
                status=TransformStatusEnum.SUCCESS,
                rows_processed=rows_processed,
                rows_output=rows_output,
                output_path=output_path,
                execution_time_seconds=execution_time,
                message="Transform execution completed (placeholder - full implementation pending)"
            )
            
        except Exception as e:
            logger.error(f"Transform execution failed: {str(e)}")
            
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

