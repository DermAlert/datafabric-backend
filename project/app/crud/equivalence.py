# """
# CRUD operations for equivalence management.
# """

# from sqlalchemy.ext.asyncio import AsyncSession
# from sqlalchemy.future import select
# from sqlalchemy import and_, or_, func
# from typing import List, Optional, Dict, Any
# from sqlalchemy import join

# from ..database.equivalence import equivalence
# from ..database.metadata import metadata


# class EquivalenceCRUD:
#     """CRUD operations for equivalence entities."""

#     @staticmethod
#     async def get_semantic_domain_by_name(db: AsyncSession, name: str) -> Optional[equivalence.SemanticDomain]:
#         """Get semantic domain by name."""
#         result = await db.execute(
#             select(equivalence.SemanticDomain)
#             .where(equivalence.SemanticDomain.name == name)
#         )
#         return result.scalars().first()

#     @staticmethod
#     async def get_data_dictionary_term_by_name(db: AsyncSession, name: str) -> Optional[equivalence.DataDictionary]:
#         """Get data dictionary term by name."""
#         result = await db.execute(
#             select(equivalence.DataDictionary)
#             .where(equivalence.DataDictionary.name == name)
#         )
#         return result.scalars().first()

#     @staticmethod
#     async def get_column_mappings_by_group(db: AsyncSession, group_id: int) -> List[equivalence.ColumnMapping]:
#         """Get all column mappings for a specific group."""
#         result = await db.execute(
#             select(equivalence.ColumnMapping)
#             .where(equivalence.ColumnMapping.group_id == group_id)
#         )
#         return result.scalars().all()

#     @staticmethod
#     async def get_value_mappings_by_group(
#         db: AsyncSession, 
#         group_id: int, 
#         source_column_id: Optional[int] = None
#     ) -> List[equivalence.ValueMapping]:
#         """Get value mappings for a group, optionally filtered by source column."""
#         query = select(equivalence.ValueMapping).where(
#             equivalence.ValueMapping.group_id == group_id
#         )
        
#         if source_column_id:
#             query = query.where(equivalence.ValueMapping.source_column_id == source_column_id)
            
#         result = await db.execute(query)
#         return result.scalars().all()

#     @staticmethod
#     async def get_unmapped_columns(
#         db: AsyncSession,
#         connection_id: Optional[int] = None,
#         schema_id: Optional[int] = None,
#         data_type: Optional[str] = None,
#         limit: int = 100
#     ) -> List[Dict[str, Any]]:
#         """Get columns that are not yet mapped to any group."""
        
#         # Subquery for mapped columns
#         mapped_columns_subquery = select(equivalence.ColumnMapping.column_id)
        
#         # Create explicit joins
#         column_table_join = join(
#             metadata.ExternalColumn,
#             metadata.ExternalTables,
#             metadata.ExternalColumn.table_id == metadata.ExternalTables.id
#         )
        
#         full_join = join(
#             column_table_join,
#             metadata.ExternalSchema,
#             metadata.ExternalTables.schema_id == metadata.ExternalSchema.id
#         )
        
#         # Main query for unmapped columns
#         query = select(
#             metadata.ExternalColumn,
#             metadata.ExternalTables.table_name,
#             metadata.ExternalSchema.schema_name
#         ).select_from(full_join).where(
#             metadata.ExternalColumn.id.notin_(mapped_columns_subquery)
#         )
        
#         # Apply filters
#         if connection_id:
#             query = query.where(metadata.ExternalTables.connection_id == connection_id)
#         if schema_id:
#             query = query.where(metadata.ExternalTables.schema_id == schema_id)
#         if data_type:
#             query = query.where(metadata.ExternalColumn.data_type == data_type)
        
#         query = query.limit(limit)
        
#         result = await db.execute(query)
#         rows = result.all()
        
#         columns = []
#         for row in rows:
#             column, table_name, schema_name = row
#             columns.append({
#                 "id": column.id,
#                 "column_name": column.column_name,
#                 "data_type": column.data_type,
#                 "table_name": table_name,
#                 "schema_name": schema_name,
#                 "table_id": column.table_id,
#                 "description": column.description,
#                 "sample_values": column.sample_values[:5] if column.sample_values else []
#             })
        
#         return columns

#     @staticmethod
#     async def find_similar_columns(
#         db: AsyncSession,
#         reference_column_id: int,
#         similarity_threshold: float = 0.3,
#         limit: int = 10
#     ) -> List[Dict[str, Any]]:
#         """Find columns similar to a reference column based on name and data type."""
#         # Get reference column
#         ref_result = await db.execute(
#             select(metadata.ExternalColumn)
#             .where(metadata.ExternalColumn.id == reference_column_id)
#         )
#         ref_column = ref_result.scalars().first()
        
#         if not ref_column:
#             return []
        
#         # Find similar columns
#         query = select(
#             metadata.ExternalColumn,
#             metadata.ExternalTables.table_name,
#             metadata.ExternalSchema.schema_name
#         ).select_from(
#             metadata.ExternalColumn
#             .join(metadata.ExternalTables, metadata.ExternalColumn.table_id == metadata.ExternalTables.id)
#             .join(metadata.ExternalSchema, metadata.ExternalTables.schema_id == metadata.ExternalSchema.id)
#         ).where(
#             and_(
#                 metadata.ExternalColumn.id != reference_column_id,
#                 or_(
#                     metadata.ExternalColumn.column_name.ilike(f"%{ref_column.column_name}%"),
#                     metadata.ExternalColumn.data_type == ref_column.data_type
#                 )
#             )
#         ).limit(limit * 2)  # Get more results to filter by similarity
        
#         result = await db.execute(query)
#         rows = result.all()
        
#         suggestions = []
#         for row in rows:
#             column, table_name, schema_name = row
            
#             # Calculate simple similarity score
#             score = 0.0
#             if column.column_name.lower() == ref_column.column_name.lower():
#                 score += 0.5
#             elif ref_column.column_name.lower() in column.column_name.lower():
#                 score += 0.3
#             elif column.column_name.lower() in ref_column.column_name.lower():
#                 score += 0.2
            
#             if column.data_type == ref_column.data_type:
#                 score += 0.3
            
#             # Only include if above threshold
#             if score >= similarity_threshold:
#                 suggestions.append({
#                     "column": {
#                         "id": column.id,
#                         "column_name": column.column_name,
#                         "data_type": column.data_type,
#                         "table_name": table_name,
#                         "schema_name": schema_name,
#                         "description": column.description
#                     },
#                     "similarity_score": score
#                 })
        
#         # Sort by similarity score and limit results
#         suggestions.sort(key=lambda x: x["similarity_score"], reverse=True)
#         return suggestions[:limit]

#     @staticmethod
#     async def get_mapping_statistics(
#         db: AsyncSession,
#         connection_id: Optional[int] = None
#     ) -> Dict[str, Any]:
#         """Get mapping coverage statistics."""
#         # Count total columns
#         total_columns_query = select(func.count(metadata.ExternalColumn.id))
#         if connection_id:
#             total_columns_query = total_columns_query.select_from(
#                 metadata.ExternalColumn
#                 .join(metadata.ExternalTables, metadata.ExternalColumn.table_id == metadata.ExternalTables.id)
#             ).where(metadata.ExternalTables.connection_id == connection_id)
        
#         # Count mapped columns
#         mapped_columns_query = select(func.count(equivalence.ColumnMapping.column_id.distinct()))
#         if connection_id:
#             mapped_columns_query = mapped_columns_query.select_from(
#                 equivalence.ColumnMapping
#                 .join(metadata.ExternalColumn, equivalence.ColumnMapping.column_id == metadata.ExternalColumn.id)
#                 .join(metadata.ExternalTables, metadata.ExternalColumn.table_id == metadata.ExternalTables.id)
#             ).where(metadata.ExternalTables.connection_id == connection_id)
        
#         # Execute queries
#         total_result = await db.execute(total_columns_query)
#         total_columns = total_result.scalar() or 0
        
#         mapped_result = await db.execute(mapped_columns_query)
#         mapped_columns = mapped_result.scalar() or 0
        
#         # Calculate percentages
#         coverage_percentage = (mapped_columns / total_columns * 100) if total_columns > 0 else 0
        
#         return {
#             "total_columns": total_columns,
#             "mapped_columns": mapped_columns,
#             "unmapped_columns": total_columns - mapped_columns,
#             "coverage_percentage": round(coverage_percentage, 2)
#         }

#     @staticmethod
#     async def check_mapping_exists(
#         db: AsyncSession,
#         group_id: int,
#         column_id: int
#     ) -> bool:
#         """Check if a mapping already exists for a group-column combination."""
#         result = await db.execute(
#             select(equivalence.ColumnMapping)
#             .where(
#                 and_(
#                     equivalence.ColumnMapping.group_id == group_id,
#                     equivalence.ColumnMapping.column_id == column_id
#                 )
#             )
#         )
#         return result.scalars().first() is not None

#     @staticmethod
#     async def check_value_mapping_exists(
#         db: AsyncSession,
#         group_id: int,
#         source_column_id: int,
#         source_value: str
#     ) -> bool:
#         """Check if a value mapping already exists."""
#         result = await db.execute(
#             select(equivalence.ValueMapping)
#             .where(
#                 and_(
#                     equivalence.ValueMapping.group_id == group_id,
#                     equivalence.ValueMapping.source_column_id == source_column_id,
#                     equivalence.ValueMapping.source_value == source_value
#                 )
#             )
#         )
#         return result.scalars().first() is not None

#     @staticmethod
#     async def bulk_create_column_mappings(
#         db: AsyncSession,
#         mappings_data: List[Dict[str, Any]]
#     ) -> List[equivalence.ColumnMapping]:
#         """Create multiple column mappings in bulk."""
#         mappings = []
#         for mapping_data in mappings_data:
#             mapping = equivalence.ColumnMapping(**mapping_data)
#             db.add(mapping)
#             mappings.append(mapping)
        
#         await db.commit()
        
#         # Refresh all mappings
#         for mapping in mappings:
#             await db.refresh(mapping)
        
#         return mappings

#     @staticmethod
#     async def bulk_create_value_mappings(
#         db: AsyncSession,
#         mappings_data: List[Dict[str, Any]]
#     ) -> List[equivalence.ValueMapping]:
#         """Create multiple value mappings in bulk."""
#         mappings = []
#         for mapping_data in mappings_data:
#             mapping = equivalence.ValueMapping(**mapping_data)
#             db.add(mapping)
#             mappings.append(mapping)
        
#         await db.commit()
        
#         # Refresh all mappings
#         for mapping in mappings:
#             await db.refresh(mapping)
        
#         return mappings
