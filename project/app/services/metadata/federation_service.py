"""
Federation Service

This service handles CRUD operations for Federations - logical groupings
of data connections and tables for organizing cross-database relationships.
"""

from typing import List, Optional, Tuple
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import and_, func
from sqlalchemy.orm import selectinload
from fastapi import HTTPException, status

from ...database.models.core import DataConnection, ConnectionType
from ...database.models.metadata import ExternalTables, ExternalSchema
from ...database.models.relationships import (
    Federation,
    FederationConnection,
    FederationTable,
    TableRelationship,
)
from ...api.schemas.federation_schemas import (
    FederationCreate,
    FederationUpdate,
    FederationConnectionInfo,
    FederationTableInfo,
    FederationListItem,
    FederationResponse,
    FederationConnectionsResponse,
    FederationTablesResponse,
)

logger = logging.getLogger(__name__)


class FederationService:
    """
    Service for managing Federations.
    """
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    # ==================== CRUD OPERATIONS ====================
    
    async def create_federation(self, data: FederationCreate) -> FederationResponse:
        """Create a new federation."""
        federation = Federation(
            name=data.name,
            description=data.description
        )
        
        self.db.add(federation)
        await self.db.commit()
        await self.db.refresh(federation)
        
        return await self._build_federation_response(federation)
    
    async def get_federation(self, federation_id: int) -> FederationResponse:
        """Get a federation by ID with full details."""
        federation = await self._get_federation_or_404(federation_id)
        return await self._build_federation_response(federation)
    
    async def update_federation(
        self, 
        federation_id: int, 
        data: FederationUpdate
    ) -> FederationResponse:
        """Update a federation."""
        federation = await self._get_federation_or_404(federation_id)
        
        if data.name is not None:
            federation.name = data.name
        if data.description is not None:
            federation.description = data.description
        
        await self.db.commit()
        await self.db.refresh(federation)
        
        return await self._build_federation_response(federation)
    
    async def delete_federation(self, federation_id: int) -> None:
        """Delete a federation."""
        federation = await self._get_federation_or_404(federation_id)
        await self.db.delete(federation)
        await self.db.commit()
    
    async def list_federations(
        self, 
        page: int = 1, 
        size: int = 20
    ) -> Tuple[List[FederationListItem], int]:
        """List all federations with pagination."""
        # Count total
        count_query = select(func.count(Federation.id))
        count_result = await self.db.execute(count_query)
        total = count_result.scalar()
        
        # Get federations
        query = select(Federation).offset((page - 1) * size).limit(size)
        result = await self.db.execute(query)
        federations = result.scalars().all()
        
        # Build responses
        items = []
        for fed in federations:
            items.append(await self._build_federation_list_item(fed))
        
        return items, total
    
    # ==================== CONNECTION MANAGEMENT ====================
    
    async def add_connections(
        self, 
        federation_id: int, 
        connection_ids: List[int]
    ) -> FederationConnectionsResponse:
        """Add connections to a federation."""
        federation = await self._get_federation_or_404(federation_id)
        
        # Verify all connections exist
        for conn_id in connection_ids:
            conn_query = select(DataConnection).where(DataConnection.id == conn_id)
            conn_result = await self.db.execute(conn_query)
            if not conn_result.scalar_one_or_none():
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Connection {conn_id} not found"
                )
        
        # Get existing connections for this federation
        existing_query = select(FederationConnection.connection_id).where(
            FederationConnection.federation_id == federation_id
        )
        existing_result = await self.db.execute(existing_query)
        existing_ids = {row[0] for row in existing_result.fetchall()}
        
        # Add new connections (skip duplicates)
        for conn_id in connection_ids:
            if conn_id not in existing_ids:
                fed_conn = FederationConnection(
                    federation_id=federation_id,
                    connection_id=conn_id
                )
                self.db.add(fed_conn)
        
        await self.db.commit()
        
        return await self._build_connections_response(federation)
    
    async def remove_connection(
        self, 
        federation_id: int, 
        connection_id: int
    ) -> FederationConnectionsResponse:
        """Remove a connection from a federation."""
        federation = await self._get_federation_or_404(federation_id)
        
        # Find and delete the association
        query = select(FederationConnection).where(
            and_(
                FederationConnection.federation_id == federation_id,
                FederationConnection.connection_id == connection_id
            )
        )
        result = await self.db.execute(query)
        fed_conn = result.scalar_one_or_none()
        
        if not fed_conn:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection {connection_id} is not part of federation {federation_id}"
            )
        
        await self.db.delete(fed_conn)
        await self.db.commit()
        
        return await self._build_connections_response(federation)
    
    # ==================== TABLE MANAGEMENT ====================
    
    async def add_tables(
        self, 
        federation_id: int, 
        table_ids: List[int]
    ) -> FederationTablesResponse:
        """Add specific tables to a federation."""
        federation = await self._get_federation_or_404(federation_id)
        
        # Verify all tables exist
        for table_id in table_ids:
            table_query = select(ExternalTables).where(ExternalTables.id == table_id)
            table_result = await self.db.execute(table_query)
            if not table_result.scalar_one_or_none():
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Table {table_id} not found"
                )
        
        # Get existing tables for this federation
        existing_query = select(FederationTable.table_id).where(
            FederationTable.federation_id == federation_id
        )
        existing_result = await self.db.execute(existing_query)
        existing_ids = {row[0] for row in existing_result.fetchall()}
        
        # Add new tables (skip duplicates)
        for table_id in table_ids:
            if table_id not in existing_ids:
                fed_table = FederationTable(
                    federation_id=federation_id,
                    table_id=table_id
                )
                self.db.add(fed_table)
        
        await self.db.commit()
        
        return await self._build_tables_response(federation)
    
    async def remove_table(
        self, 
        federation_id: int, 
        table_id: int
    ) -> FederationTablesResponse:
        """Remove a table from a federation."""
        federation = await self._get_federation_or_404(federation_id)
        
        # Find and delete the association
        query = select(FederationTable).where(
            and_(
                FederationTable.federation_id == federation_id,
                FederationTable.table_id == table_id
            )
        )
        result = await self.db.execute(query)
        fed_table = result.scalar_one_or_none()
        
        if not fed_table:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Table {table_id} is not explicitly part of federation {federation_id}"
            )
        
        await self.db.delete(fed_table)
        await self.db.commit()
        
        return await self._build_tables_response(federation)
    
    # ==================== HELPER METHODS ====================
    
    async def _get_federation_or_404(self, federation_id: int) -> Federation:
        """Get federation by ID or raise 404."""
        query = select(Federation).where(Federation.id == federation_id)
        result = await self.db.execute(query)
        federation = result.scalar_one_or_none()
        
        if not federation:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Federation {federation_id} not found"
            )
        
        return federation
    
    async def _get_federation_connections(self, federation_id: int) -> List[FederationConnectionInfo]:
        """Get all connections for a federation with type info."""
        query = select(
            DataConnection.id,
            DataConnection.name,
            ConnectionType.name.label('type'),
            ConnectionType.color_hex.label('color'),
            ConnectionType.icon
        ).join(
            FederationConnection,
            FederationConnection.connection_id == DataConnection.id
        ).join(
            ConnectionType,
            DataConnection.connection_type_id == ConnectionType.id
        ).where(
            FederationConnection.federation_id == federation_id
        )
        
        result = await self.db.execute(query)
        rows = result.fetchall()
        
        return [
            FederationConnectionInfo(
                id=row.id,
                name=row.name,
                type=row.type,
                color=row.color,
                icon=row.icon
            )
            for row in rows
        ]
    
    async def _get_federation_tables(self, federation_id: int) -> List[FederationTableInfo]:
        """Get tables for a federation."""
        # First check if there are explicit tables
        explicit_query = select(FederationTable.table_id).where(
            FederationTable.federation_id == federation_id
        )
        explicit_result = await self.db.execute(explicit_query)
        explicit_table_ids = [row[0] for row in explicit_result.fetchall()]
        
        if explicit_table_ids:
            # Return only explicit tables
            query = select(
                ExternalTables.id,
                ExternalTables.table_name,
                ExternalSchema.schema_name,
                ExternalTables.connection_id,
                DataConnection.name.label('connection_name')
            ).join(
                ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
            ).join(
                DataConnection, ExternalTables.connection_id == DataConnection.id
            ).where(
                ExternalTables.id.in_(explicit_table_ids)
            )
        else:
            # Return all tables from federation connections
            conn_query = select(FederationConnection.connection_id).where(
                FederationConnection.federation_id == federation_id
            )
            conn_result = await self.db.execute(conn_query)
            connection_ids = [row[0] for row in conn_result.fetchall()]
            
            if not connection_ids:
                return []
            
            query = select(
                ExternalTables.id,
                ExternalTables.table_name,
                ExternalSchema.schema_name,
                ExternalTables.connection_id,
                DataConnection.name.label('connection_name')
            ).join(
                ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
            ).join(
                DataConnection, ExternalTables.connection_id == DataConnection.id
            ).where(
                ExternalTables.connection_id.in_(connection_ids)
            )
        
        result = await self.db.execute(query)
        rows = result.fetchall()
        
        return [
            FederationTableInfo(
                id=row.id,
                table_name=row.table_name,
                schema_name=row.schema_name,
                connection_id=row.connection_id,
                connection_name=row.connection_name
            )
            for row in rows
        ]
    
    async def _get_tables_count(self, federation_id: int) -> int:
        """Get count of tables in federation."""
        tables = await self._get_federation_tables(federation_id)
        return len(tables)
    
    async def _get_relationships_count(self, federation_id: int) -> int:
        """
        Get count of cross-database (INTER_CONNECTION) relationships 
        where both tables belong to the federation.
        
        Only counts INTER_CONNECTION since that's the main purpose of federations.
        """
        from ...database.models.relationships import RelationshipScope
        
        tables = await self._get_federation_tables(federation_id)
        table_ids = {t.id for t in tables}
        
        if not table_ids:
            return 0
        
        # Count only INTER_CONNECTION relationships where both tables are in the federation
        query = select(func.count(TableRelationship.id)).where(
            and_(
                TableRelationship.left_table_id.in_(table_ids),
                TableRelationship.right_table_id.in_(table_ids),
                TableRelationship.scope == RelationshipScope.INTER_CONNECTION,
                TableRelationship.is_active == True
            )
        )
        
        result = await self.db.execute(query)
        return result.scalar() or 0
    
    async def _build_federation_response(self, federation: Federation) -> FederationResponse:
        """Build full federation response."""
        connections = await self._get_federation_connections(federation.id)
        tables = await self._get_federation_tables(federation.id)
        relationships_count = await self._get_relationships_count(federation.id)
        
        return FederationResponse(
            id=federation.id,
            name=federation.name,
            description=federation.description,
            connections=connections,
            tables=tables,
            tables_count=len(tables),
            relationships_count=relationships_count,
            data_criacao=federation.data_criacao,
            data_atualizacao=federation.data_atualizacao
        )
    
    async def _build_federation_list_item(self, federation: Federation) -> FederationListItem:
        """Build federation list item response."""
        connections = await self._get_federation_connections(federation.id)
        tables_count = await self._get_tables_count(federation.id)
        relationships_count = await self._get_relationships_count(federation.id)
        
        return FederationListItem(
            id=federation.id,
            name=federation.name,
            description=federation.description,
            connections=connections,
            tables_count=tables_count,
            relationships_count=relationships_count,
            data_criacao=federation.data_criacao,
            data_atualizacao=federation.data_atualizacao
        )
    
    async def _build_connections_response(self, federation: Federation) -> FederationConnectionsResponse:
        """Build connections response."""
        connections = await self._get_federation_connections(federation.id)
        tables_count = await self._get_tables_count(federation.id)
        
        return FederationConnectionsResponse(
            id=federation.id,
            name=federation.name,
            connections=connections,
            tables_count=tables_count
        )
    
    async def _build_tables_response(self, federation: Federation) -> FederationTablesResponse:
        """Build tables response."""
        tables = await self._get_federation_tables(federation.id)
        
        return FederationTablesResponse(
            id=federation.id,
            name=federation.name,
            tables=tables,
            tables_count=len(tables)
        )
