from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload
from sqlalchemy import and_, or_, func
from typing import List, Dict, Any, Set, Tuple
from fastapi import HTTPException, status

from ..database.models.core import DataConnection
from ..database.models.metadata import ExternalTables, ExternalColumn, ExternalSchema
from ..database.models.equivalence import ColumnMapping, ValueMapping, ColumnGroup, DataDictionary
from ..api.schemas.dataset_schemas import DatasetUnifiedCreate

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

    async def get_unified_columns_from_selected_columns(self, selected_column_ids: List[int], auto_include_mapped_columns: bool = True) -> Dict[str, Any]:
        """
        Get unified structure based on specific selected columns.
        Only includes the mapped columns within groups, not all columns from tables.
        """
        # Get information about selected columns
        columns_query = select(
            ExternalColumn.id,
            ExternalColumn.column_name,
            ExternalColumn.data_type,
            ExternalColumn.table_id,
            ExternalTables.table_name,
            ExternalSchema.schema_name
        ).join(
            ExternalTables, ExternalColumn.table_id == ExternalTables.id
        ).join(
            ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
        ).where(
            ExternalColumn.id.in_(selected_column_ids)
        )
        
        columns_result = await self.db.execute(columns_query)
        selected_columns = columns_result.fetchall()
        
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
        
        # Get all group IDs that have selected columns
        relevant_group_ids = set(mapping.group_id for mapping in mappings)
        mapped_column_ids = set(mapping.column_id for mapping in mappings)
        
        # Get additional mapped columns from the same groups (if auto_include_mapped_columns is True)
        additional_column_ids = set()
        if auto_include_mapped_columns and relevant_group_ids:
            additional_mappings_query = select(
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
                and_(
                    ColumnMapping.group_id.in_(relevant_group_ids),
                    ~ColumnMapping.column_id.in_(selected_column_ids)
                )
            )
            
            additional_mappings_result = await self.db.execute(additional_mappings_query)
            additional_mappings = additional_mappings_result.fetchall()
            
            for mapping in additional_mappings:
                additional_column_ids.add(mapping.column_id)
        
        # Get all mappings for final column set
        final_column_ids = set(selected_column_ids) | additional_column_ids
        
        if relevant_group_ids:
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
                ColumnMapping.column_id.in_(final_column_ids)
            )
            
            all_mappings_result = await self.db.execute(all_group_mappings_query)
            all_mappings = all_mappings_result.fetchall()
        else:
            all_mappings = []
        
        # Organize by groups
        mapping_groups = {}
        table_ids = set()
        
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
            table_ids.add(mapping.table_id)
        
        # Add unmapped selected columns
        for col in selected_columns:
            if col.id not in mapped_column_ids:
                table_ids.add(col.table_id)
        
        return {
            'column_ids': list(final_column_ids),
            'table_ids': list(table_ids),
            'mapping_groups': list(mapping_groups.values()),
            'selected_column_ids': selected_column_ids,
            'additional_column_ids': list(additional_column_ids)
        }

    async def generate_unified_columns(self, unified_tables_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate the unified column structure for the dataset (original table-based method)"""
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
                    'table_id': col['table_id'],
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
                    'table_id': col.table_id,
                    'column_name': col.column_name,
                    'table_name': col.table_name,
                    'schema_name': col.schema_name,
                    'data_type': col.data_type
                }],
                'transformation_rules': []
            }
            unified_columns.append(unified_column)
        
        return unified_columns

    async def generate_unified_columns_from_selected_columns(self, unified_columns_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate the unified column structure for the dataset based on selected columns"""
        unified_columns = []
        processed_column_ids = set()
        
        # Process mapped columns first
        for group in unified_columns_info['mapping_groups']:
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
                    'table_id': col['table_id'],
                    'column_name': col['column_name'],
                    'table_name': col['table_name'],
                    'schema_name': col['schema_name'],
                    'data_type': col['data_type']
                })
                processed_column_ids.add(col['column_id'])
            
            unified_columns.append(unified_column)
        
        # Process unmapped columns (only those that were originally selected)
        selected_column_ids = unified_columns_info.get('selected_column_ids', [])
        unmapped_selected_ids = set(selected_column_ids) - processed_column_ids
        
        if unmapped_selected_ids:
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
                ExternalColumn.id.in_(unmapped_selected_ids)
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
                        'table_id': col.table_id,
                        'column_name': col.column_name,
                        'table_name': col.table_name,
                        'schema_name': col.schema_name,
                        'data_type': col.data_type
                    }],
                    'transformation_rules': []
                }
                unified_columns.append(unified_column)
        
        return unified_columns
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
                    'table_id': col['table_id'],
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
                    'table_id': col.table_id,
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

    async def generate_federated_query(
        self, 
        unified_columns: List[Dict[str, Any]], 
        unified_tables_info: Dict[str, Any],
        value_mappings: Dict[int, List[Dict[str, Any]]]
    ) -> str:
        """
        Generate a Trino federated SQL query to unify data from multiple sources.
        Returns a SELECT query that includes UNION ALL across all sources with correct column mapping and casting.
        """
        select_queries = []
        
        # We need to get the connection/catalog info for each table to build the fully qualified name
        # We'll need to fetch table details including connection info
        table_ids = unified_tables_info['table_ids']
        
        tables_query = select(
            ExternalTables.id,
            ExternalTables.table_name,
            ExternalSchema.schema_name,
            DataConnection.name.label('connection_name')
        ).join(
            ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
        ).join(
            DataConnection, ExternalTables.connection_id == DataConnection.id
        ).where(
            ExternalTables.id.in_(table_ids)
        )
        
        tables_result = await self.db.execute(tables_query)
        tables_data = {row.id: row for row in tables_result.fetchall()}
        
        from ...services.trino_manager import TrinoManager
        trino_manager = TrinoManager()
        
        import logging
        logger = logging.getLogger(__name__)
        
        for table_id in table_ids:
            if table_id not in tables_data:
                continue
                
            table_info = tables_data[table_id]
            # Sanitize connection name to get catalog name (using same logic as TrinoManager)
            catalog = trino_manager._sanitize_identifier(table_info.connection_name)
            schema = table_info.schema_name
            table = table_info.table_name
            
            logger.info(f"Generating query part for table {table_id} ({catalog}.{schema}.{table})")
            
            # Build column expressions for this table
            col_expressions = []
            
            for unified_col in unified_columns:
                unified_name = unified_col['name']
                target_type = unified_col['data_type']
                
                # Find if this table contributes to this unified column
                # Ensure strict type comparison for table_id
                source_col = None
                for src in unified_col['source_columns']:
                    # Debug log to check what's in src
                    # logger.info(f"Checking source col: {src.get('column_name')} - Table ID in src: {src.get('table_id')} (type: {type(src.get('table_id'))}) vs Current Table ID: {table_id} (type: {type(table_id)})")
                    
                    src_table_id = src.get('table_id')
                    if src_table_id is not None and int(src_table_id) == int(table_id):
                        source_col = src
                        break
                
                if source_col:
                    logger.info(f"  - [MATCH] Table {table_id} HAS column '{unified_name}' -> mapped to source '{source_col['column_name']}'")
                    
                    # Handle case sensitivity for column names (Trino is case-insensitive but preserves case in quotes)
                    # We'll use the exact name from the database metadata
                    source_expr = f'"{source_col["column_name"]}"'
                    
                    # Apply value mappings if they exist and this is a mapped column
                    if unified_col.get('group_id') and unified_col['group_id'] in value_mappings:
                        col_mappings = [
                            m for m in value_mappings[unified_col['group_id']] 
                            if m['source_column_id'] == source_col['column_id']
                        ]
                        
                        if col_mappings:
                            whens = []
                            for m in col_mappings:
                                # Escape single quotes in values
                                src_val = m['source_value'].replace("'", "''")
                                std_val = m['standard_value'].replace("'", "''")
                                whens.append(f"WHEN {source_expr} = '{src_val}' THEN '{std_val}'")
                            
                            if whens:
                                source_expr = f"CASE {' '.join(whens)} ELSE CAST({source_expr} AS VARCHAR) END"
                    
                    # Cast to target type if needed
                    # Always casting to ensure UNION compatibility
                    trino_type = self._map_to_trino_type(target_type)
                    col_expressions.append(f'CAST({source_expr} AS {trino_type}) AS "{unified_name}"')
                    
                else:
                    # Column missing in this source, add NULL casted to correct type
                    logger.info(f"  - [MISSING] Table {table_id} MISSING column '{unified_name}'. Using NULL.")
                    trino_type = self._map_to_trino_type(target_type)
                    col_expressions.append(f'CAST(NULL AS {trino_type}) AS "{unified_name}"')
            
            # Add metadata columns
            col_expressions.append(f"CAST('{table}' AS VARCHAR) AS _source_table")
            col_expressions.append(f"CAST({table_id} AS INTEGER) AS _source_table_id")
            
            select_query = f"""
                SELECT 
                    {', '.join(col_expressions)}
                FROM "{catalog}"."{schema}"."{table}"
            """
            select_queries.append(select_query)
            
        # Combine all with UNION ALL
        full_query = "\nUNION ALL\n".join(select_queries)
        
        return full_query

    def _map_to_trino_type(self, type_name: str) -> str:
        """Map application data types to Trino SQL types"""
        type_map = {
            'string': 'VARCHAR',
            'text': 'VARCHAR',
            'integer': 'INTEGER',
            'int': 'INTEGER',
            'bigint': 'BIGINT',
            'boolean': 'BOOLEAN',
            'bool': 'BOOLEAN',
            'float': 'DOUBLE',
            'double': 'DOUBLE',
            'decimal': 'DECIMAL',
            'date': 'DATE',
            'timestamp': 'TIMESTAMP',
            'datetime': 'TIMESTAMP'
        }
        return type_map.get(type_name.lower(), 'VARCHAR')

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

    # ==================== SMART BRONZE ARCHITECTURE ====================
    
    async def generate_bronze_ingestion_plan(self, dataset_data) -> List[Dict[str, Any]]:
        """
        Generate a Bronze ingestion plan using Smart Bronze architecture.
        
        This method:
        1. Groups tables by their source connection
        2. Generates optimized SQL with pushdown joins for each group
        3. Returns a list of ingestion tasks to be executed by Trino
        
        Note: This is a bridge method for backwards compatibility with the existing
        endpoint. For new implementations, use BronzeIngestionService directly.
        """
        from collections import defaultdict
        from ...services.trino_manager import TrinoManager
        import os
        
        trino_manager = TrinoManager()
        bronze_bucket = os.getenv("BRONZE_BUCKET", "datafabric-bronze")
        
        # Get table IDs based on selection mode
        if hasattr(dataset_data, 'selection_mode'):
            if dataset_data.selection_mode.value == 'tables':
                table_ids = dataset_data.selected_tables or []
            else:
                # For column-based selection, get unique table IDs from columns
                column_ids = dataset_data.selected_columns or []
                if column_ids:
                    cols_query = select(ExternalColumn.table_id).where(
                        ExternalColumn.id.in_(column_ids)
                    ).distinct()
                    result = await self.db.execute(cols_query)
                    table_ids = [row[0] for row in result.fetchall()]
                else:
                    table_ids = []
        else:
            table_ids = []
        
        if not table_ids:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No tables selected for Bronze ingestion"
            )
        
        # Fetch table metadata with connection info
        tables_query = select(
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
        
        tables_result = await self.db.execute(tables_query)
        tables_data = {row.id: row for row in tables_result.fetchall()}
        
        # Group tables by connection
        connection_groups = defaultdict(list)
        for table_id, table_info in tables_data.items():
            connection_groups[table_info.connection_id].append({
                'table_id': table_id,
                'table_name': table_info.table_name,
                'schema_name': table_info.schema_name,
                'connection_name': table_info.connection_name
            })
        
        # Generate ingestion tasks for each group
        ingestion_tasks = []
        group_idx = 0
        
        for conn_id, tables in connection_groups.items():
            group_idx += 1
            conn_name = tables[0]['connection_name']
            catalog = trino_manager._sanitize_identifier(conn_name)
            
            # For simplicity in this bridge method, we do a simple SELECT per table
            # The full BronzeIngestionService handles joins properly
            for table in tables:
                # Fetch columns for this table
                cols_query = select(
                    ExternalColumn.column_name,
                    ExternalColumn.data_type
                ).where(
                    ExternalColumn.table_id == table['table_id']
                ).order_by(
                    ExternalColumn.column_position
                )
                
                cols_result = await self.db.execute(cols_query)
                columns = cols_result.fetchall()
                
                # Build SELECT clause
                col_expressions = [f'"{col.column_name}"' for col in columns]
                col_expressions.append(f"'{table['table_name']}' AS _source_table")
                col_expressions.append(f"{table['table_id']} AS _source_table_id")
                col_expressions.append("CURRENT_TIMESTAMP AS _ingestion_timestamp")
                
                # Build the full SQL
                select_sql = f"""SELECT 
    {', '.join(col_expressions)}
FROM "{catalog}"."{table['schema_name']}"."{table['table_name']}"
"""
                
                # Target path in MinIO
                target_path = f"s3a://{bronze_bucket}/{dataset_data.name}/part_{catalog}_{table['table_name']}/"
                
                ingestion_tasks.append({
                    'group_id': group_idx,
                    'connection_id': conn_id,
                    'connection_name': conn_name,
                    'table_id': table['table_id'],
                    'table_name': table['table_name'],
                    'tables_involved': [table['table_name']],
                    'sql': select_sql,
                    'target_path': target_path
                })
        
        return ingestion_tasks
