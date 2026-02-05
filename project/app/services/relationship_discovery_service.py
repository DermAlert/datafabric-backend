"""
Relationship Discovery Service

This service handles:
1. Auto-discovery of relationships from FK constraints
2. Discovery by column name patterns (e.g., user_id -> users.id)
3. Discovery by data analysis (value matching)
4. Management of relationship suggestions
5. CRUD operations for permanent relationships
"""

import re
from typing import Dict, Any, List, Optional, Tuple, Set
from collections import defaultdict
from datetime import datetime
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import aliased
from sqlalchemy import and_, or_, func, text
from fastapi import HTTPException, status

from ..database.core.core import DataConnection
from ..database.metadata.metadata import ExternalTables, ExternalColumn, ExternalSchema
from ..database.metadata.relationships import (
    TableRelationship,
    RelationshipSuggestion,
    SemanticColumnLink,
    RelationshipScope,
    RelationshipSource,
    RelationshipCardinality,
    JoinType,
)
from ..api.schemas.relationship_schemas import (
    TableRelationshipCreate,
    TableRelationshipUpdate,
    TableRelationshipResponse,
    RelationshipSuggestionResponse,
    DiscoverRelationshipsRequest,
    DiscoverRelationshipsResponse,
    ColumnInfo,
    RelationshipScopeEnum,
    RelationshipSourceEnum,
    RelationshipCardinalityEnum,
    JoinTypeEnum,
    SuggestionStatusEnum,
    RelationshipGraph,
    RelationshipGraphNode,
    RelationshipGraphEdge,
)

logger = logging.getLogger(__name__)


class RelationshipDiscoveryService:
    """
    Service for discovering and managing table relationships.
    """
    
    # Common patterns for FK column names
    FK_PATTERNS = [
        r'^(.+)_id$',           # user_id -> users
        r'^id_(.+)$',           # id_user -> users
        r'^fk_(.+)$',           # fk_user -> users
        r'^(.+)_fk$',           # user_fk -> users
        r'^(.+)_key$',          # user_key -> users
        r'^ref_(.+)$',          # ref_user -> users
    ]
    
    # Common PK column names
    PK_NAMES = ['id', 'pk', 'key', 'code', 'numero', 'codigo']
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    # ==================== DISCOVERY METHODS ====================
    
    async def discover_relationships(
        self, 
        request: DiscoverRelationshipsRequest,
        trino_client=None  # Trino client for constraint extraction
    ) -> DiscoverRelationshipsResponse:
        """
        Main discovery method that orchestrates all discovery strategies.
        
        When discover_fk=True, automatically extracts PK/FK constraints from
        source databases via Trino before running discovery.
        """
        warnings = []
        new_relationships = []
        new_suggestions = []
        
        # Get tables in scope
        tables = await self._get_tables_in_scope(
            request.connection_ids, 
            request.table_ids
        )
        
        if not tables:
            return DiscoverRelationshipsResponse(
                tables_analyzed=0,
                columns_analyzed=0,
                relationships_found=0,
                relationships_created=0,
                suggestions_created=0,
                new_relationships=[],
                new_suggestions=[],
                warnings=["No tables found in the specified scope"]
            )
        
        # ==== Extract PK/FK constraints from databases via Trino ====
        if trino_client is not None:
                from .constraint_extraction_service import ConstraintExtractionService
                constraint_service = ConstraintExtractionService(trino_client, self.db)
                
                # Get unique connection IDs
                connection_ids = set(t['connection_id'] for t in tables)
                
                for conn_id in connection_ids:
                    try:
                        result = await constraint_service.extract_constraints_for_connection(conn_id)
                        if result.get('status') == 'unsupported':
                            warnings.append(
                                f"Connection {conn_id}: Database type '{result.get('db_type')}' not supported for FK extraction yet. "
                                f"Supported: MySQL, PostgreSQL."
                            )
                        else:
                            pk_count = result.get('primary_keys_found', 0)
                            fk_count = result.get('foreign_keys_found', 0)
                            if pk_count > 0 or fk_count > 0:
                                logger.info(
                                    f"Extracted constraints for connection {conn_id}: "
                                    f"{pk_count} PKs, {fk_count} FKs"
                                )
                    except Exception as e:
                        warnings.append(f"Failed to extract constraints for connection {conn_id}: {str(e)}")
                        logger.error(f"Constraint extraction failed for connection {conn_id}: {e}")
        
        # Get all columns for these tables
        columns = await self._get_columns_for_tables([t['table_id'] for t in tables])
        
        # Build lookup structures
        table_lookup = {t['table_id']: t for t in tables}
        columns_by_table = defaultdict(list)
        for col in columns:
            columns_by_table[col['table_id']].append(col)
        
        # Discover from FK constraints (extracted from database)
        discovered = await self._discover_from_fk_constraints(
            tables, columns, table_lookup
        )
        
        # Deduplicate
        discovered = self._deduplicate_discoveries(discovered)
        
        # Check for existing relationships/suggestions
        existing_pairs = await self._get_existing_relationship_pairs()
        
        # Create relationships or suggestions
        relationships_created = 0
        suggestions_created = 0
        
        for disc in discovered:
            pair = (disc['left_column_id'], disc['right_column_id'])
            reverse_pair = (disc['right_column_id'], disc['left_column_id'])
            
            if pair in existing_pairs or reverse_pair in existing_pairs:
                continue  # Already exists
            
            # Auto-accept if configured, otherwise create as suggestion
            if request.auto_accept:
                rel = await self._create_relationship_from_discovery(disc)
                new_relationships.append(await self._build_relationship_response(rel))
                relationships_created += 1
            else:
                # Create as suggestion for review
                suggestion = await self._create_suggestion_from_discovery(disc)
                new_suggestions.append(await self._build_suggestion_response(suggestion))
                suggestions_created += 1
        
        await self.db.commit()
        
        return DiscoverRelationshipsResponse(
            tables_analyzed=len(tables),
            columns_analyzed=len(columns),
            relationships_found=len(discovered),
            relationships_created=relationships_created,
            suggestions_created=suggestions_created,
            new_relationships=new_relationships,
            new_suggestions=new_suggestions,
            warnings=warnings
        )
    
    async def _discover_from_fk_constraints(
        self,
        tables: List[Dict],
        columns: List[Dict],
        table_lookup: Dict
    ) -> List[Dict]:
        """
        Discover relationships from foreign key constraints.
        
        Uses two sources:
        1. FK metadata stored in external_columns (is_foreign_key, fk_referenced_table_id, fk_referenced_column_id)
        2. Legacy FK info in column properties
        
        If FK metadata is not populated, call POST /api/metadata/connections/{id}/extract-constraints first.
        """
        discovered = []
        
        for col in columns:
            # ==== Strategy 1: Use new FK metadata fields (preferred) ====
            # Check if this column is marked as FK with referenced table/column
            col_result = await self.db.execute(
                select(ExternalColumn).where(ExternalColumn.id == col['column_id'])
            )
            col_obj = col_result.scalar_one_or_none()
            
            if col_obj and col_obj.is_foreign_key and col_obj.fk_referenced_table_id and col_obj.fk_referenced_column_id:
                # FK metadata is populated - use it directly
                discovered.append({
                    'left_table_id': col_obj.fk_referenced_table_id,
                    'left_column_id': col_obj.fk_referenced_column_id,
                    'right_table_id': col['table_id'],
                    'right_column_id': col['column_id'],
                    'source': RelationshipSource.AUTO_FK,
                    'confidence': 1.0,
                    'cardinality': RelationshipCardinality.ONE_TO_MANY,  # FK -> PK is typically many-to-one (reversed: one-to-many)
                    'details': {
                        'reason': 'foreign_key_constraint',
                        'constraint_name': col_obj.fk_constraint_name,
                        'source': 'database_metadata'
                    }
                })
                continue  # Skip legacy check
            
            # ==== Strategy 2: Legacy - Check properties for FK info ====
            props = col.get('properties', {})
            fk_info = props.get('foreign_key') or props.get('fk') or props.get('references')
            
            if fk_info:
                # FK info might be stored as: {"table": "users", "column": "id"}
                ref_table = fk_info.get('table') or fk_info.get('referenced_table')
                ref_column = fk_info.get('column') or fk_info.get('referenced_column', 'id')
                
                if ref_table:
                    # Find the referenced table and column
                    ref_table_info = None
                    for t in tables:
                        if t['table_name'].lower() == ref_table.lower():
                            if t['connection_id'] == col['connection_id']:
                                ref_table_info = t
                                break
                    
                    if ref_table_info:
                        # Find the referenced column
                        ref_col_query = select(ExternalColumn).where(
                            and_(
                                ExternalColumn.table_id == ref_table_info['table_id'],
                                func.lower(ExternalColumn.column_name) == ref_column.lower()
                            )
                        )
                        result = await self.db.execute(ref_col_query)
                        ref_col = result.scalar_one_or_none()
                        
                        if ref_col:
                            discovered.append({
                                'left_table_id': ref_table_info['table_id'],
                                'left_column_id': ref_col.id,
                                'right_table_id': col['table_id'],
                                'right_column_id': col['column_id'],
                                'source': RelationshipSource.AUTO_FK,
                                'confidence': 1.0,
                                'cardinality': RelationshipCardinality.ONE_TO_MANY,
                                'details': {
                                    'reason': 'foreign_key_constraint',
                                    'fk_info': fk_info,
                                    'source': 'column_properties'
                                }
                            })
        
        return discovered
    
    async def _discover_by_name_patterns(
        self,
        tables: List[Dict],
        columns: List[Dict],
        columns_by_table: Dict,
        table_lookup: Dict,
        include_cross_connection: bool
    ) -> List[Dict]:
        """
        Discover relationships by analyzing column name patterns.
        
        Looks for patterns like:
        - user_id in table orders -> id in table users
        - id_paciente in table diagnostico -> id in table paciente
        """
        discovered = []
        
        # Build table name lookup (lowercase for matching)
        table_name_lookup = {}
        for t in tables:
            # Store both singular and plural forms
            name_lower = t['table_name'].lower()
            table_name_lookup[name_lower] = t
            
            # Try to get singular form
            if name_lower.endswith('s'):
                table_name_lookup[name_lower[:-1]] = t
            if name_lower.endswith('es'):
                table_name_lookup[name_lower[:-2]] = t
            if name_lower.endswith('ies'):
                table_name_lookup[name_lower[:-3] + 'y'] = t
        
        for col in columns:
            col_name = col['column_name'].lower()
            col_table = table_lookup.get(col['table_id'])
            
            if not col_table:
                continue
            
            # Skip if this is a PK column
            if col.get('is_primary_key') or col_name in self.PK_NAMES:
                continue
            
            # Try each FK pattern
            for pattern in self.FK_PATTERNS:
                match = re.match(pattern, col_name)
                if match:
                    ref_name = match.group(1).lower()
                    
                    # Look for a table with this name
                    ref_table = table_name_lookup.get(ref_name)
                    
                    if ref_table:
                        # Check connection scope
                        same_connection = ref_table['connection_id'] == col['connection_id']
                        
                        if not same_connection and not include_cross_connection:
                            continue
                        
                        # Skip self-references to same table
                        if ref_table['table_id'] == col['table_id']:
                            continue
                        
                        # Find the PK column in the referenced table
                        ref_columns = columns_by_table.get(ref_table['table_id'], [])
                        pk_col = None
                        
                        # First try to find explicit PK
                        for rc in ref_columns:
                            if rc.get('is_primary_key'):
                                pk_col = rc
                                break
                        
                        # If no explicit PK, look for 'id' column
                        if not pk_col:
                            for rc in ref_columns:
                                if rc['column_name'].lower() in self.PK_NAMES:
                                    pk_col = rc
                                    break
                        
                        if pk_col:
                            # Check data type compatibility
                            type_compatible = self._are_types_compatible(
                                col['data_type'], 
                                pk_col['data_type']
                            )
                            
                            confidence = 0.85 if type_compatible else 0.6
                            if same_connection:
                                confidence += 0.1  # Boost for same connection
                            
                            discovered.append({
                                'left_table_id': ref_table['table_id'],
                                'left_column_id': pk_col['column_id'],
                                'right_table_id': col['table_id'],
                                'right_column_id': col['column_id'],
                                'source': RelationshipSource.AUTO_NAME,
                                'confidence': min(confidence, 1.0),
                                'cardinality': RelationshipCardinality.ONE_TO_MANY,
                                'details': {
                                    'reason': 'name_pattern_match',
                                    'pattern': pattern,
                                    'matched_name': ref_name,
                                    'type_compatible': type_compatible
                                }
                            })
                    break  # Only match first pattern
        
        return discovered
    
    async def _discover_by_data_analysis(
        self,
        tables: List[Dict],
        columns: List[Dict],
        columns_by_table: Dict,
        table_lookup: Dict,
        include_cross_connection: bool
    ) -> List[Dict]:
        """
        Discover relationships by analyzing actual data values.
        
        This is more expensive but can find relationships that naming
        conventions miss. It compares sample values between columns.
        
        Note: This requires sample_values to be stored in column metadata.
        """
        discovered = []
        
        # Get columns that have sample values and look like IDs
        id_columns = []
        for col in columns:
            if col.get('is_primary_key') or col['column_name'].lower() in self.PK_NAMES:
                samples = col.get('sample_values', [])
                if samples:
                    id_columns.append(col)
        
        # Get columns that look like FKs
        fk_columns = []
        for col in columns:
            col_name = col['column_name'].lower()
            is_fk_like = any(re.match(p, col_name) for p in self.FK_PATTERNS)
            if is_fk_like:
                samples = col.get('sample_values', [])
                if samples:
                    fk_columns.append(col)
        
        # Compare sample values
        for fk_col in fk_columns:
            fk_samples = set(str(v) for v in fk_col.get('sample_values', []) if v is not None)
            if not fk_samples:
                continue
            
            for id_col in id_columns:
                # Skip same table
                if fk_col['table_id'] == id_col['table_id']:
                    continue
                
                # Check connection scope
                same_connection = fk_col['connection_id'] == id_col['connection_id']
                if not same_connection and not include_cross_connection:
                    continue
                
                id_samples = set(str(v) for v in id_col.get('sample_values', []) if v is not None)
                if not id_samples:
                    continue
                
                # Calculate overlap
                overlap = fk_samples & id_samples
                if overlap:
                    overlap_ratio = len(overlap) / len(fk_samples)
                    
                    if overlap_ratio >= 0.3:  # At least 30% overlap
                        confidence = 0.5 + (overlap_ratio * 0.4)  # 0.5 to 0.9
                        
                        discovered.append({
                            'left_table_id': id_col['table_id'],
                            'left_column_id': id_col['column_id'],
                            'right_table_id': fk_col['table_id'],
                            'right_column_id': fk_col['column_id'],
                            'source': RelationshipSource.AUTO_DATA,
                            'confidence': confidence,
                            'cardinality': RelationshipCardinality.ONE_TO_MANY,
                            'details': {
                                'reason': 'data_value_match',
                                'overlap_count': len(overlap),
                                'overlap_ratio': overlap_ratio,
                                'sample_matches': list(overlap)[:5]
                            }
                        })
        
        return discovered
    
    # ==================== CRUD METHODS ====================
    
    async def create_relationship(
        self, 
        data: TableRelationshipCreate
    ) -> TableRelationshipResponse:
        """Create a new table relationship manually."""
        
        # Validate columns exist and get their info
        left_col = await self._get_column_with_info(data.left_column_id)
        right_col = await self._get_column_with_info(data.right_column_id)
        
        if not left_col or not right_col:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="One or both columns not found"
            )
        
        # Determine scope
        scope = (
            RelationshipScope.INTRA_CONNECTION 
            if left_col['connection_id'] == right_col['connection_id']
            else RelationshipScope.INTER_CONNECTION
        )
        
        # Check for existing relationship
        existing = await self._get_existing_relationship(
            data.left_column_id, 
            data.right_column_id
        )
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Relationship already exists between these columns"
            )
        
        # Create relationship
        relationship = TableRelationship(
            left_table_id=data.left_table_id,
            left_column_id=data.left_column_id,
            right_table_id=data.right_table_id,
            right_column_id=data.right_column_id,
            name=data.name,
            description=data.description,
            scope=scope,
            source=RelationshipSource.MANUAL,
            cardinality=RelationshipCardinality(data.cardinality.value) if data.cardinality else None,
            default_join_type=JoinType(data.default_join_type.value),
            is_verified=True,  # Manual relationships are verified by default
            is_active=True,
            properties=data.properties
        )
        
        self.db.add(relationship)
        await self.db.commit()
        await self.db.refresh(relationship)
        
        return await self._build_relationship_response(relationship)
    
    async def get_relationship(self, relationship_id: int) -> TableRelationshipResponse:
        """Get a relationship by ID."""
        query = select(TableRelationship).where(TableRelationship.id == relationship_id)
        result = await self.db.execute(query)
        relationship = result.scalar_one_or_none()
        
        if not relationship:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Relationship {relationship_id} not found"
            )
        
        return await self._build_relationship_response(relationship)
    
    async def update_relationship(
        self, 
        relationship_id: int, 
        data: TableRelationshipUpdate
    ) -> TableRelationshipResponse:
        """Update a relationship."""
        query = select(TableRelationship).where(TableRelationship.id == relationship_id)
        result = await self.db.execute(query)
        relationship = result.scalar_one_or_none()
        
        if not relationship:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Relationship {relationship_id} not found"
            )
        
        # Update fields
        if data.name is not None:
            relationship.name = data.name
        if data.description is not None:
            relationship.description = data.description
        if data.cardinality is not None:
            relationship.cardinality = RelationshipCardinality(data.cardinality.value)
        if data.default_join_type is not None:
            relationship.default_join_type = JoinType(data.default_join_type.value)
        if data.is_verified is not None:
            relationship.is_verified = data.is_verified
        if data.is_active is not None:
            relationship.is_active = data.is_active
        if data.properties is not None:
            relationship.properties = data.properties
        
        await self.db.commit()
        await self.db.refresh(relationship)
        
        return await self._build_relationship_response(relationship)
    
    async def delete_relationship(self, relationship_id: int) -> None:
        """Delete a relationship and reset associated suggestion to pending (if exists)."""
        query = select(TableRelationship).where(TableRelationship.id == relationship_id)
        result = await self.db.execute(query)
        relationship = result.scalar_one_or_none()
        
        if not relationship:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Relationship {relationship_id} not found"
            )
        
        # Reset associated suggestion to 'pending' so it can be re-discovered/re-accepted
        sug_query = select(RelationshipSuggestion).where(
            or_(
                and_(
                    RelationshipSuggestion.left_column_id == relationship.left_column_id,
                    RelationshipSuggestion.right_column_id == relationship.right_column_id
                ),
                and_(
                    RelationshipSuggestion.left_column_id == relationship.right_column_id,
                    RelationshipSuggestion.right_column_id == relationship.left_column_id
                )
            )
        )
        sug_result = await self.db.execute(sug_query)
        suggestion = sug_result.scalar_one_or_none()
        
        if suggestion and suggestion.status == 'accepted':
            suggestion.status = 'pending'
            logger.info(f"Reset suggestion {suggestion.id} to 'pending' after relationship deletion")
        
        await self.db.delete(relationship)
        await self.db.commit()
    
    async def list_relationships(
        self,
        connection_id: Optional[int] = None,
        table_id: Optional[int] = None,
        scope: Optional[RelationshipScopeEnum] = None,
        is_active: bool = True,
        page: int = 1,
        size: int = 20
    ) -> Tuple[List[TableRelationshipResponse], int]:
        """List relationships with filters."""
        
        query = select(TableRelationship)
        count_query = select(func.count(TableRelationship.id))
        
        # Apply filters
        filters = []
        
        if is_active is not None:
            filters.append(TableRelationship.is_active == is_active)
        
        if scope:
            filters.append(TableRelationship.scope == RelationshipScope(scope.value))
        
        if table_id:
            filters.append(
                or_(
                    TableRelationship.left_table_id == table_id,
                    TableRelationship.right_table_id == table_id
                )
            )
        
        if connection_id:
            LeftTable = aliased(ExternalTables)
            RightTable = aliased(ExternalTables)
            
            query = query.join(LeftTable, TableRelationship.left_table_id == LeftTable.id)
            query = query.join(RightTable, TableRelationship.right_table_id == RightTable.id)
            count_query = count_query.join(LeftTable, TableRelationship.left_table_id == LeftTable.id)
            count_query = count_query.join(RightTable, TableRelationship.right_table_id == RightTable.id)
            
            filters.append(
                or_(
                    LeftTable.connection_id == connection_id,
                    RightTable.connection_id == connection_id
                )
            )
        
        if filters:
            query = query.where(and_(*filters))
            count_query = count_query.where(and_(*filters))
        
        # Get total count
        count_result = await self.db.execute(count_query)
        total = count_result.scalar()
        
        # Apply pagination
        query = query.offset((page - 1) * size).limit(size)
        
        result = await self.db.execute(query)
        relationships = result.scalars().all()
        
        # Build responses
        responses = []
        for rel in relationships:
            responses.append(await self._build_relationship_response(rel))
        
        return responses, total
    
    # ==================== SUGGESTION METHODS ====================
    
    async def accept_suggestion(
        self, 
        suggestion_id: int,
        cardinality: Optional[RelationshipCardinalityEnum] = None,
        default_join_type: Optional[JoinTypeEnum] = None,
        name: Optional[str] = None,
        description: Optional[str] = None
    ) -> TableRelationshipResponse:
        """Accept a suggestion and convert it to a relationship."""
        
        query = select(RelationshipSuggestion).where(
            RelationshipSuggestion.id == suggestion_id
        )
        result = await self.db.execute(query)
        suggestion = result.scalar_one_or_none()
        
        if not suggestion:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Suggestion {suggestion_id} not found"
            )
        
        if suggestion.status != 'pending':
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Suggestion is already {suggestion.status}"
            )
        
        # Get column info to determine scope
        left_col = await self._get_column_with_info(suggestion.left_column_id)
        right_col = await self._get_column_with_info(suggestion.right_column_id)
        
        scope = (
            RelationshipScope.INTRA_CONNECTION 
            if left_col['connection_id'] == right_col['connection_id']
            else RelationshipScope.INTER_CONNECTION
        )
        
        # Determine cardinality: use provided value, fallback to details, or default to ONE_TO_MANY for FK
        final_cardinality = None
        if cardinality:
            final_cardinality = RelationshipCardinality(cardinality.value)
        elif suggestion.details and suggestion.details.get('cardinality'):
            final_cardinality = RelationshipCardinality(suggestion.details['cardinality'])
        elif suggestion.suggestion_reason in ('foreign_key_constraint', 'fk_constraint', 'auto_fk'):
            # Default for FK relationships
            final_cardinality = RelationshipCardinality.ONE_TO_MANY
        
        # Create relationship
        relationship = TableRelationship(
            left_table_id=suggestion.left_table_id,
            left_column_id=suggestion.left_column_id,
            right_table_id=suggestion.right_table_id,
            right_column_id=suggestion.right_column_id,
            name=name,
            description=description,
            scope=scope,
            source=self._map_suggestion_reason_to_source(suggestion.suggestion_reason),
            cardinality=final_cardinality,
            default_join_type=JoinType(default_join_type.value) if default_join_type else JoinType.FULL,
            confidence_score=suggestion.confidence_score,
            is_verified=True,
            is_active=True,
            discovery_details=suggestion.details
        )
        
        self.db.add(relationship)
        
        # Update suggestion status
        suggestion.status = 'accepted'
        
        await self.db.commit()
        await self.db.refresh(relationship)
        
        return await self._build_relationship_response(relationship)
    
    def _map_suggestion_reason_to_source(self, reason: str) -> RelationshipSource:
        """Map suggestion reason string to RelationshipSource enum."""
        reason_mapping = {
            'foreign_key_constraint': RelationshipSource.AUTO_FK,
            'fk_constraint': RelationshipSource.AUTO_FK,
            'auto_fk': RelationshipSource.AUTO_FK,
            'name_match': RelationshipSource.AUTO_NAME,
            'name_pattern': RelationshipSource.AUTO_NAME,
            'auto_name': RelationshipSource.AUTO_NAME,
            'data_match': RelationshipSource.AUTO_DATA,
            'data_analysis': RelationshipSource.AUTO_DATA,
            'auto_data': RelationshipSource.AUTO_DATA,
            'manual': RelationshipSource.MANUAL,
            'suggested': RelationshipSource.SUGGESTED,
        }
        
        # Try direct mapping
        if reason in reason_mapping:
            return reason_mapping[reason]
        
        # Try lowercase
        reason_lower = reason.lower()
        if reason_lower in reason_mapping:
            return reason_mapping[reason_lower]
        
        # Default to SUGGESTED if unknown
        logger.warning(f"Unknown suggestion reason '{reason}', defaulting to SUGGESTED")
        return RelationshipSource.SUGGESTED
    
    async def reject_suggestion(self, suggestion_id: int) -> None:
        """Reject a suggestion."""
        query = select(RelationshipSuggestion).where(
            RelationshipSuggestion.id == suggestion_id
        )
        result = await self.db.execute(query)
        suggestion = result.scalar_one_or_none()
        
        if not suggestion:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Suggestion {suggestion_id} not found"
            )
        
        suggestion.status = 'rejected'
        await self.db.commit()
    
    async def reset_suggestion(self, suggestion_id: int) -> RelationshipSuggestionResponse:
        """
        Reset a rejected suggestion back to pending.
        
        This allows users to reconsider a previously rejected suggestion.
        """
        query = select(RelationshipSuggestion).where(
            RelationshipSuggestion.id == suggestion_id
        )
        result = await self.db.execute(query)
        suggestion = result.scalar_one_or_none()
        
        if not suggestion:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Suggestion {suggestion_id} not found"
            )
        
        if suggestion.status == 'pending':
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Suggestion is already pending"
            )
        
        if suggestion.status == 'accepted':
            # Check if the relationship still exists
            existing_rel = await self._get_existing_relationship(
                suggestion.left_column_id,
                suggestion.right_column_id
            )
            if existing_rel:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Cannot reset: relationship already exists. Delete the relationship first."
                )
        
        suggestion.status = 'pending'
        await self.db.commit()
        await self.db.refresh(suggestion)
        
        return await self._build_suggestion_response(suggestion)
    
    async def list_suggestions(
        self,
        connection_id: Optional[int] = None,
        table_id: Optional[int] = None,
        status_filter: Optional[SuggestionStatusEnum] = None,
        min_confidence: Optional[float] = None,
        page: int = 1,
        size: int = 20
    ) -> Tuple[List[RelationshipSuggestionResponse], int]:
        """List suggestions with filters."""
        
        query = select(RelationshipSuggestion)
        count_query = select(func.count(RelationshipSuggestion.id))
        
        filters = []
        
        if status_filter:
            filters.append(RelationshipSuggestion.status == status_filter.value)
        
        if min_confidence is not None:
            filters.append(RelationshipSuggestion.confidence_score >= min_confidence)
        
        if table_id:
            filters.append(
                or_(
                    RelationshipSuggestion.left_table_id == table_id,
                    RelationshipSuggestion.right_table_id == table_id
                )
            )
            
        if connection_id:
            LeftTable = aliased(ExternalTables)
            RightTable = aliased(ExternalTables)
            
            query = query.join(LeftTable, RelationshipSuggestion.left_table_id == LeftTable.id)
            query = query.join(RightTable, RelationshipSuggestion.right_table_id == RightTable.id)
            count_query = count_query.join(LeftTable, RelationshipSuggestion.left_table_id == LeftTable.id)
            count_query = count_query.join(RightTable, RelationshipSuggestion.right_table_id == RightTable.id)
            
            filters.append(
                or_(
                    LeftTable.connection_id == connection_id,
                    RightTable.connection_id == connection_id
                )
            )
        
        if filters:
            query = query.where(and_(*filters))
            count_query = count_query.where(and_(*filters))
        
        # Order by confidence descending
        query = query.order_by(RelationshipSuggestion.confidence_score.desc())
        
        # Get total count
        count_result = await self.db.execute(count_query)
        total = count_result.scalar()
        
        # Apply pagination
        query = query.offset((page - 1) * size).limit(size)
        
        result = await self.db.execute(query)
        suggestions = result.scalars().all()
        
        responses = []
        for sug in suggestions:
            responses.append(await self._build_suggestion_response(sug))
        
        return responses, total
    
    # ==================== GRAPH METHODS ====================
    
    async def get_relationship_graph(
        self,
        connection_ids: Optional[List[int]] = None,
        table_ids: Optional[List[int]] = None,
        include_inactive: bool = False
    ) -> RelationshipGraph:
        """
        Get a graph representation of relationships for visualization.
        """
        # Get relationships
        query = select(TableRelationship)
        
        filters = []
        if not include_inactive:
            filters.append(TableRelationship.is_active == True)
        
        if filters:
            query = query.where(and_(*filters))
        
        result = await self.db.execute(query)
        relationships = result.scalars().all()
        
        # Build nodes and edges
        table_ids_in_graph = set()
        edges = []
        
        for rel in relationships:
            table_ids_in_graph.add(rel.left_table_id)
            table_ids_in_graph.add(rel.right_table_id)
            
            # Get column names
            left_col = await self._get_column_with_info(rel.left_column_id)
            right_col = await self._get_column_with_info(rel.right_column_id)
            
            edges.append(RelationshipGraphEdge(
                id=f"rel_{rel.id}",
                source=f"table_{rel.left_table_id}",
                target=f"table_{rel.right_table_id}",
                relationship_id=rel.id,
                left_column=left_col['column_name'] if left_col else 'unknown',
                right_column=right_col['column_name'] if right_col else 'unknown',
                scope=RelationshipScopeEnum(rel.scope.value),
                cardinality=RelationshipCardinalityEnum(rel.cardinality.value) if rel.cardinality else None,
                is_verified=rel.is_verified
            ))
        
        # Get table info for nodes
        nodes = []
        if table_ids_in_graph:
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
                ExternalTables.id.in_(table_ids_in_graph)
            )
            
            tables_result = await self.db.execute(tables_query)
            tables = tables_result.fetchall()
            
            for t in tables:
                # Count columns
                col_count_query = select(func.count(ExternalColumn.id)).where(
                    ExternalColumn.table_id == t.id
                )
                col_count_result = await self.db.execute(col_count_query)
                col_count = col_count_result.scalar()
                
                nodes.append(RelationshipGraphNode(
                    id=f"table_{t.id}",
                    table_id=t.id,
                    table_name=t.table_name,
                    schema_name=t.schema_name,
                    connection_id=t.connection_id,
                    connection_name=t.connection_name,
                    column_count=col_count
                ))
        
        # Get unique connections
        connections = {}
        for node in nodes:
            if node.connection_id not in connections:
                connections[node.connection_id] = {
                    'id': node.connection_id,
                    'name': node.connection_name
                }
        
        return RelationshipGraph(
            nodes=nodes,
            edges=edges,
            total_tables=len(nodes),
            total_relationships=len(edges),
            connections_involved=list(connections.values())
        )
    
    # ==================== HELPER METHODS ====================
    
    async def _get_tables_in_scope(
        self,
        connection_ids: Optional[List[int]],
        table_ids: Optional[List[int]]
    ) -> List[Dict]:
        """Get tables within the specified scope."""
        query = select(
            ExternalTables.id.label('table_id'),
            ExternalTables.table_name,
            ExternalTables.connection_id,
            ExternalSchema.schema_name,
            DataConnection.name.label('connection_name')
        ).join(
            ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
        ).join(
            DataConnection, ExternalTables.connection_id == DataConnection.id
        )
        
        filters = []
        if connection_ids:
            filters.append(ExternalTables.connection_id.in_(connection_ids))
        if table_ids:
            filters.append(ExternalTables.id.in_(table_ids))
        
        if filters:
            query = query.where(and_(*filters))
        
        result = await self.db.execute(query)
        return [dict(row._mapping) for row in result.fetchall()]
    
    async def _get_columns_for_tables(self, table_ids: List[int]) -> List[Dict]:
        """Get all columns for the specified tables."""
        query = select(
            ExternalColumn.id.label('column_id'),
            ExternalColumn.table_id,
            ExternalColumn.column_name,
            ExternalColumn.data_type,
            ExternalColumn.is_primary_key,
            ExternalColumn.is_foreign_key,
            ExternalColumn.fk_referenced_table_id,
            ExternalColumn.fk_referenced_column_id,
            ExternalColumn.fk_constraint_name,
            ExternalColumn.properties,
            ExternalColumn.sample_values,
            ExternalTables.connection_id
        ).join(
            ExternalTables, ExternalColumn.table_id == ExternalTables.id
        ).where(
            ExternalColumn.table_id.in_(table_ids)
        )
        
        result = await self.db.execute(query)
        return [dict(row._mapping) for row in result.fetchall()]
    
    async def _get_column_with_info(self, column_id: int) -> Optional[Dict]:
        """Get column with full info including table and connection."""
        query = select(
            ExternalColumn.id.label('column_id'),
            ExternalColumn.column_name,
            ExternalColumn.data_type,
            ExternalColumn.table_id,
            ExternalTables.table_name,
            ExternalTables.connection_id,
            ExternalSchema.schema_name,
            DataConnection.name.label('connection_name')
        ).join(
            ExternalTables, ExternalColumn.table_id == ExternalTables.id
        ).join(
            ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
        ).join(
            DataConnection, ExternalTables.connection_id == DataConnection.id
        ).where(
            ExternalColumn.id == column_id
        )
        
        result = await self.db.execute(query)
        row = result.first()
        return dict(row._mapping) if row else None
    
    async def _get_existing_relationship_pairs(self) -> Set[Tuple[int, int]]:
        """Get all existing relationship column pairs."""
        query = select(
            TableRelationship.left_column_id,
            TableRelationship.right_column_id
        )
        result = await self.db.execute(query)
        
        pairs = set()
        for row in result.fetchall():
            pairs.add((row[0], row[1]))
            pairs.add((row[1], row[0]))  # Add reverse
        
        # Also check ALL suggestions (not just pending) to avoid unique constraint violations
        # The constraint is on (left_column_id, right_column_id) regardless of status
        sug_query = select(
            RelationshipSuggestion.left_column_id,
            RelationshipSuggestion.right_column_id
        )
        
        sug_result = await self.db.execute(sug_query)
        for row in sug_result.fetchall():
            pairs.add((row[0], row[1]))
            pairs.add((row[1], row[0]))
        
        return pairs
    
    async def _get_existing_relationship(
        self, 
        left_column_id: int, 
        right_column_id: int
    ) -> Optional[TableRelationship]:
        """Check if a relationship already exists between two columns."""
        query = select(TableRelationship).where(
            or_(
                and_(
                    TableRelationship.left_column_id == left_column_id,
                    TableRelationship.right_column_id == right_column_id
                ),
                and_(
                    TableRelationship.left_column_id == right_column_id,
                    TableRelationship.right_column_id == left_column_id
                )
            )
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()
    
    def _are_types_compatible(self, type1: str, type2: str) -> bool:
        """Check if two data types are compatible for a join."""
        # Normalize types
        type1 = type1.lower()
        type2 = type2.lower()
        
        # Exact match
        if type1 == type2:
            return True
        
        # Integer types
        int_types = {'int', 'integer', 'bigint', 'smallint', 'tinyint', 'int4', 'int8', 'int2'}
        if type1 in int_types and type2 in int_types:
            return True
        
        # String types
        str_types = {'varchar', 'char', 'text', 'string', 'nvarchar', 'nchar'}
        if any(t in type1 for t in str_types) and any(t in type2 for t in str_types):
            return True
        
        # UUID/GUID
        uuid_types = {'uuid', 'guid', 'uniqueidentifier'}
        if type1 in uuid_types and type2 in uuid_types:
            return True
        
        return False
    
    def _deduplicate_discoveries(self, discoveries: List[Dict]) -> List[Dict]:
        """Remove duplicate discoveries, keeping the highest confidence."""
        seen = {}
        for disc in discoveries:
            key = (
                min(disc['left_column_id'], disc['right_column_id']),
                max(disc['left_column_id'], disc['right_column_id'])
            )
            if key not in seen or disc['confidence'] > seen[key]['confidence']:
                seen[key] = disc
        return list(seen.values())
    
    async def _create_relationship_from_discovery(self, discovery: Dict) -> TableRelationship:
        """Create a TableRelationship from a discovery dict."""
        # Get column info to determine scope
        left_col = await self._get_column_with_info(discovery['left_column_id'])
        right_col = await self._get_column_with_info(discovery['right_column_id'])
        
        scope = (
            RelationshipScope.INTRA_CONNECTION 
            if left_col['connection_id'] == right_col['connection_id']
            else RelationshipScope.INTER_CONNECTION
        )
        
        relationship = TableRelationship(
            left_table_id=discovery['left_table_id'],
            left_column_id=discovery['left_column_id'],
            right_table_id=discovery['right_table_id'],
            right_column_id=discovery['right_column_id'],
            scope=scope,
            source=discovery['source'],
            cardinality=discovery.get('cardinality'),
            default_join_type=JoinType.FULL,
            confidence_score=discovery['confidence'],
            is_verified=discovery['source'] == RelationshipSource.AUTO_FK,
            is_active=True,
            discovery_details=discovery.get('details')
        )
        
        self.db.add(relationship)
        await self.db.flush()
        return relationship
    
    async def _create_suggestion_from_discovery(self, discovery: Dict) -> RelationshipSuggestion:
        """Create a RelationshipSuggestion from a discovery dict."""
        # Merge cardinality into details so it can be used when accepting
        details = discovery.get('details', {}).copy()
        if discovery.get('cardinality'):
            details['cardinality'] = discovery['cardinality'].value
        
        suggestion = RelationshipSuggestion(
            left_table_id=discovery['left_table_id'],
            left_column_id=discovery['left_column_id'],
            right_table_id=discovery['right_table_id'],
            right_column_id=discovery['right_column_id'],
            suggestion_reason=discovery['details'].get('reason', 'unknown'),
            confidence_score=discovery['confidence'],
            status='pending',
            details=details
        )
        
        self.db.add(suggestion)
        await self.db.flush()
        return suggestion
    
    async def _build_relationship_response(
        self, 
        relationship: TableRelationship
    ) -> TableRelationshipResponse:
        """Build a response object for a relationship."""
        left_col = await self._get_column_with_info(relationship.left_column_id)
        right_col = await self._get_column_with_info(relationship.right_column_id)
        
        return TableRelationshipResponse(
            id=relationship.id,
            left_column=ColumnInfo(
                column_id=left_col['column_id'],
                column_name=left_col['column_name'],
                data_type=left_col['data_type'],
                table_id=left_col['table_id'],
                table_name=left_col['table_name'],
                schema_name=left_col['schema_name'],
                connection_id=left_col['connection_id'],
                connection_name=left_col['connection_name']
            ),
            right_column=ColumnInfo(
                column_id=right_col['column_id'],
                column_name=right_col['column_name'],
                data_type=right_col['data_type'],
                table_id=right_col['table_id'],
                table_name=right_col['table_name'],
                schema_name=right_col['schema_name'],
                connection_id=right_col['connection_id'],
                connection_name=right_col['connection_name']
            ),
            name=relationship.name,
            description=relationship.description,
            scope=RelationshipScopeEnum(relationship.scope.value),
            source=RelationshipSourceEnum(relationship.source.value),
            cardinality=RelationshipCardinalityEnum(relationship.cardinality.value) if relationship.cardinality else None,
            default_join_type=JoinTypeEnum(relationship.default_join_type.value),
            confidence_score=relationship.confidence_score,
            is_verified=relationship.is_verified,
            is_active=relationship.is_active,
            data_criacao=relationship.data_criacao,
            data_atualizacao=relationship.data_atualizacao
        )
    
    async def _build_suggestion_response(
        self, 
        suggestion: RelationshipSuggestion
    ) -> RelationshipSuggestionResponse:
        """Build a response object for a suggestion."""
        left_col = await self._get_column_with_info(suggestion.left_column_id)
        right_col = await self._get_column_with_info(suggestion.right_column_id)
        
        return RelationshipSuggestionResponse(
            id=suggestion.id,
            left_column=ColumnInfo(
                column_id=left_col['column_id'],
                column_name=left_col['column_name'],
                data_type=left_col['data_type'],
                table_id=left_col['table_id'],
                table_name=left_col['table_name'],
                schema_name=left_col['schema_name'],
                connection_id=left_col['connection_id'],
                connection_name=left_col['connection_name']
            ),
            right_column=ColumnInfo(
                column_id=right_col['column_id'],
                column_name=right_col['column_name'],
                data_type=right_col['data_type'],
                table_id=right_col['table_id'],
                table_name=right_col['table_name'],
                schema_name=right_col['schema_name'],
                connection_id=right_col['connection_id'],
                connection_name=right_col['connection_name']
            ),
            suggestion_reason=suggestion.suggestion_reason,
            confidence_score=suggestion.confidence_score,
            status=SuggestionStatusEnum(suggestion.status),
            details=suggestion.details,
            generated_at=suggestion.generated_at
        )

