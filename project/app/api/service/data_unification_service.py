from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from sqlalchemy import and_, or_, func
from typing import List, Dict, Any, Set, Tuple
from fastapi import HTTPException, status

from ...database.core.core import DataConnection
from ...database.metadata.metadata import ExternalTables, ExternalColumn, ExternalSchema
from ...database.equivalence.equivalence import ColumnMapping, ValueMapping, ColumnGroup, DataDictionary
from ..schemas.dataset_schemas import DatasetUnifiedCreate

class DataUnificationService:
    """Service for handling data unification logic with column mappings and value mappings"""
    
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_unified_tables_with_mappings(self, selected_table_ids: List[int], auto_include_mapped_columns: bool = True) -> Dict[str, Any]:
        """
        Get all tables that should be included based on column mappings.
        If a table has columns that are mapped to the same group as columns from selected tables,
        it should be included automatically.
        """
        # Get all columns from selected tables
        columns_query = select(ExternalColumn).where(
            ExternalColumn.table_id.in_(selected_table_ids)
        )
        columns_result = await self.db.execute(columns_query)
        selected_columns = columns_result.scalars().all()
        selected_column_ids = [col.id for col in selected_columns]
        
        # Get all column mappings for these columns
        mappings_query = select(
            ColumnMapping.group_id,
            ColumnMapping.column_id,
            ColumnGroup.name,
            ColumnGroup.description,
            ColumnGroup.semantic_domain_id,
            ColumnGroup.data_dictionary_term_id
        ).join(
            ColumnGroup, ColumnMapping.group_id == ColumnGroup.id
        ).where(
            ColumnMapping.column_id.in_(selected_column_ids)
        )
        
        mappings_result = await self.db.execute(mappings_query)
        mappings = mappings_result.fetchall()
        
        # Get all group IDs that have columns from selected tables
        relevant_group_ids = set(mapping.group_id for mapping in mappings)
        
        if not relevant_group_ids:
            # No mappings found, return only selected tables
            return {
                'table_ids': selected_table_ids,
                'mapping_groups': [],
                'additional_tables': []
            }
        
        # Get ALL columns that belong to these groups
        all_group_mappings_query = select(
            ColumnMapping.column_id,
            ColumnMapping.group_id,
            ExternalColumn.table_id,
            ExternalColumn.column_name,
            ExternalColumn.data_type,
            ExternalTables.table_name,
            ExternalSchema.schema_name
        ).join(
            ExternalColumn, ColumnMapping.column_id == ExternalColumn.id
        ).join(
            ExternalTables, ExternalColumn.table_id == ExternalTables.id
        ).join(
            ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
        ).where(
            ColumnMapping.group_id.in_(relevant_group_ids)
        )
        
        all_mappings_result = await self.db.execute(all_group_mappings_query)
        all_mappings = all_mappings_result.fetchall()
        
        # Organize by groups
        mapping_groups = {}
        all_table_ids = set(selected_table_ids)
        
        for mapping in all_mappings:
            group_id = mapping.group_id
            if group_id not in mapping_groups:
                mapping_groups[group_id] = {
                    'group_id': group_id,
                    'columns': [],
                    'tables': set()
                }
            
            mapping_groups[group_id]['columns'].append({
                'column_id': mapping.column_id,
                'column_name': mapping.column_name,
                'data_type': mapping.data_type,
                'table_id': mapping.table_id,
                'table_name': mapping.table_name,
                'schema_name': mapping.schema_name
            })
            mapping_groups[group_id]['tables'].add(mapping.table_id)
            
            # Only add additional tables if auto_include_mapped_columns is True
            if auto_include_mapped_columns:
                all_table_ids.add(mapping.table_id)
        
        # Identify additional tables that were added due to mappings
        additional_table_ids = all_table_ids - set(selected_table_ids)
        
        return {
            'table_ids': list(all_table_ids),
            'mapping_groups': list(mapping_groups.values()),
            'additional_tables': list(additional_table_ids)
        }

    async def generate_unified_columns(self, unified_tables_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate the unified column structure for the dataset"""
        unified_columns = []
        processed_column_ids = set()
        
        # Process mapped columns first
        for group in unified_tables_info['mapping_groups']:
            group_id = group['group_id']
            
            # Get group information
            group_query = select(
                ColumnGroup.name,
                ColumnGroup.description,
                DataDictionary.display_name,
                DataDictionary.data_type,
                DataDictionary.description.label('dict_description')
            ).outerjoin(
                DataDictionary, ColumnGroup.data_dictionary_term_id == DataDictionary.id
            ).where(
                ColumnGroup.id == group_id
            )
            
            group_result = await self.db.execute(group_query)
            group_info = group_result.first()
            
            # Create unified column for this group
            unified_column = {
                'name': group_info.display_name or group_info.name,
                'description': group_info.dict_description or group_info.description,
                'data_type': group_info.data_type or 'string',
                'source_type': 'mapped',
                'group_id': group_id,
                'source_columns': [],
                'transformation_rules': []
            }
            
            # Add all columns from this group
            for col in group['columns']:
                unified_column['source_columns'].append({
                    'column_id': col['column_id'],
                    'column_name': col['column_name'],
                    'table_name': col['table_name'],
                    'schema_name': col['schema_name'],
                    'data_type': col['data_type']
                })
                processed_column_ids.add(col['column_id'])
            
            unified_columns.append(unified_column)
        
        # Process unmapped columns
        all_table_ids = unified_tables_info['table_ids']
        unmapped_columns_query = select(
            ExternalColumn.id,
            ExternalColumn.column_name,
            ExternalColumn.data_type,
            ExternalColumn.description,
            ExternalColumn.table_id,
            ExternalTables.table_name,
            ExternalSchema.schema_name
        ).join(
            ExternalTables, ExternalColumn.table_id == ExternalTables.id
        ).join(
            ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
        ).where(
            and_(
                ExternalColumn.table_id.in_(all_table_ids),
                ~ExternalColumn.id.in_(processed_column_ids)
            )
        ).order_by(
            ExternalColumn.table_id,
            ExternalColumn.column_position
        )
        
        unmapped_result = await self.db.execute(unmapped_columns_query)
        unmapped_columns = unmapped_result.fetchall()
        
        for col in unmapped_columns:
            # Handle name conflicts for unmapped columns
            base_name = col.column_name
            column_name = base_name
            counter = 1
            
            # Check if this column name already exists in unified_columns
            existing_names = {uc['name'] for uc in unified_columns}
            while column_name in existing_names:
                column_name = f"{base_name}_{col.table_name}"
                if column_name in existing_names:
                    column_name = f"{base_name}_{col.table_name}_{counter}"
                    counter += 1
            
            unified_column = {
                'name': column_name,
                'description': col.description,
                'data_type': col.data_type,
                'source_type': 'individual',
                'group_id': None,
                'source_columns': [{
                    'column_id': col.id,
                    'column_name': col.column_name,
                    'table_name': col.table_name,
                    'schema_name': col.schema_name,
                    'data_type': col.data_type
                }],
                'transformation_rules': []
            }
            unified_columns.append(unified_column)
        
        return unified_columns

    async def get_value_mappings_for_groups(self, group_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
        """Get all value mappings for the specified groups"""
        if not group_ids:
            return {}
        
        value_mappings_query = select(
            ValueMapping.group_id,
            ValueMapping.source_column_id,
            ValueMapping.source_value,
            ValueMapping.standard_value,
            ValueMapping.description,
            ExternalColumn.column_name,
            ExternalTables.table_name
        ).join(
            ExternalColumn, ValueMapping.source_column_id == ExternalColumn.id
        ).join(
            ExternalTables, ExternalColumn.table_id == ExternalTables.id
        ).where(
            ValueMapping.group_id.in_(group_ids)
        ).order_by(
            ValueMapping.group_id,
            ValueMapping.source_column_id,
            ValueMapping.source_value
        )
        
        result = await self.db.execute(value_mappings_query)
        value_mappings = result.fetchall()
        
        # Group by group_id
        mappings_by_group = {}
        for mapping in value_mappings:
            group_id = mapping.group_id
            if group_id not in mappings_by_group:
                mappings_by_group[group_id] = []
            
            mappings_by_group[group_id].append({
                'source_column_id': mapping.source_column_id,
                'source_value': mapping.source_value,
                'standard_value': mapping.standard_value,
                'description': mapping.description,
                'column_name': mapping.column_name,
                'table_name': mapping.table_name
            })
        
        return mappings_by_group

    async def generate_transformation_sql(self, unified_columns: List[Dict[str, Any]], value_mappings: Dict[int, List[Dict[str, Any]]]) -> str:
        """Generate SQL transformation logic for the unified dataset"""
        sql_parts = []
        
        for col in unified_columns:
            if col['source_type'] == 'mapped':
                # Handle mapped columns with potential value mappings
                group_id = col['group_id']
                mappings = value_mappings.get(group_id, [])
                
                if mappings:
                    # Generate CASE statement for value mapping
                    case_parts = []
                    for mapping in mappings:
                        case_parts.append(
                            f"WHEN source_col = '{mapping['source_value']}' THEN '{mapping['standard_value']}'"
                        )
                    
                    case_sql = f"CASE {' '.join(case_parts)} ELSE source_col END"
                    sql_parts.append(f"{case_sql} AS {col['name']}")
                else:
                    # Simple COALESCE for multiple source columns
                    source_cols = [f"t{i}.{src_col['column_name']}" for i, src_col in enumerate(col['source_columns'])]
                    sql_parts.append(f"COALESCE({', '.join(source_cols)}) AS {col['name']}")
            else:
                # Handle individual columns
                src_col = col['source_columns'][0]
                sql_parts.append(f"t0.{src_col['column_name']} AS {col['name']}")
        
        return "SELECT\n  " + ",\n  ".join(sql_parts) + "\n"

    async def validate_unification_compatibility(self, table_ids: List[int]) -> Dict[str, Any]:
        """Validate if the selected tables can be unified"""
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'recommendations': []
        }
        
        # Check if tables exist
        tables_query = select(ExternalTables.id).where(ExternalTables.id.in_(table_ids))
        tables_result = await self.db.execute(tables_query)
        existing_tables = [row[0] for row in tables_result.fetchall()]
        
        missing_tables = set(table_ids) - set(existing_tables)
        if missing_tables:
            validation_result['valid'] = False
            validation_result['errors'].append(f"Tables not found: {list(missing_tables)}")
        
        # Check if tables have columns
        columns_query = select(
            ExternalColumn.table_id,
            func.count(ExternalColumn.id).label('column_count')
        ).where(
            ExternalColumn.table_id.in_(table_ids)
        ).group_by(
            ExternalColumn.table_id
        )
        
        columns_result = await self.db.execute(columns_query)
        column_counts = {row.table_id: row.column_count for row in columns_result.fetchall()}
        
        tables_without_columns = []
        for table_id in table_ids:
            if table_id not in column_counts or column_counts[table_id] == 0:
                tables_without_columns.append(table_id)
        
        if tables_without_columns:
            validation_result['warnings'].append(f"Tables without columns: {tables_without_columns}")
        
        # Check for potential join keys
        # This is a simplified check - in a real implementation, you'd want more sophisticated logic
        if len(table_ids) > 1:
            validation_result['recommendations'].append(
                "Consider defining join conditions between tables for better data integration"
            )
        
        return validation_result
