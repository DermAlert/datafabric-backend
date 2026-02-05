"""
Federation API Routes

These routes manage Federations - logical groupings of data connections
and tables for organizing cross-database relationships.

A Federation is purely organizational - it groups connections together
so users can easily navigate and manage relationships in large environments.

The actual relationship discovery and management is done through the
existing /api/relationships endpoints.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
import logging

from ...database.database import get_db
from ...crud.token import get_current_user
from ..schemas.federation_schemas import (
    FederationCreate,
    FederationUpdate,
    FederationAddConnections,
    FederationAddTables,
    FederationListItem,
    FederationResponse,
    FederationConnectionsResponse,
    FederationTablesResponse,
)
from ..schemas.search import SearchResult
from ...services.federation_service import FederationService

router = APIRouter()
logger = logging.getLogger(__name__)


# ==================== CRUD ====================

@router.get("", response_model=SearchResult[FederationListItem])
async def list_federations(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    List all federations.
    
    Returns federation groups with their connections, table counts,
    and relationship counts.
    """
    try:
        service = FederationService(db)
        federations, total = await service.list_federations(page=page, size=size)
        
        return SearchResult(
            items=federations,
            total=total,
            page=page,
            size=size,
            pages=(total + size - 1) // size if total > 0 else 0
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list federations: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list federations: {str(e)}"
        )


@router.post("", response_model=FederationResponse, status_code=status.HTTP_201_CREATED)
async def create_federation(
    data: FederationCreate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Create a new federation.
    
    A federation is a logical grouping for organizing data connections
    and their cross-database relationships.
    
    After creating, add connections using POST /federations/{id}/connections
    """
    try:
        service = FederationService(db)
        return await service.create_federation(data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create federation: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create federation: {str(e)}"
        )


@router.get("/{federation_id}", response_model=FederationResponse)
async def get_federation(
    federation_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Get federation details.
    
    Returns the federation with:
    - All connections (with color and icon)
    - All tables (explicit or from connections)
    - Table and relationship counts
    """
    try:
        service = FederationService(db)
        return await service.get_federation(federation_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get federation: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get federation: {str(e)}"
        )


@router.put("/{federation_id}", response_model=FederationResponse)
async def update_federation(
    federation_id: int,
    data: FederationUpdate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Update federation name or description.
    """
    try:
        service = FederationService(db)
        return await service.update_federation(federation_id, data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update federation: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update federation: {str(e)}"
        )


@router.delete("/{federation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_federation(
    federation_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Delete a federation.
    
    This only removes the federation grouping - it does NOT delete
    the connections, tables, or relationships within it.
    """
    try:
        service = FederationService(db)
        await service.delete_federation(federation_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete federation: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete federation: {str(e)}"
        )


# ==================== CONNECTION MANAGEMENT ====================

@router.post("/{federation_id}/connections", response_model=FederationConnectionsResponse)
async def add_connections(
    federation_id: int,
    data: FederationAddConnections,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Add connections to a federation.
    
    Multiple connections can be added at once. Duplicates are ignored.
    
    After adding connections, use POST /api/relationships/discover
    with the connection IDs to discover relationships between them.
    """
    try:
        service = FederationService(db)
        return await service.add_connections(federation_id, data.connection_ids)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to add connections: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to add connections: {str(e)}"
        )


@router.delete("/{federation_id}/connections/{connection_id}", response_model=FederationConnectionsResponse)
async def remove_connection(
    federation_id: int,
    connection_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Remove a connection from a federation.
    
    This only removes the connection from this federation - it does NOT
    delete the connection itself or any relationships.
    """
    try:
        service = FederationService(db)
        return await service.remove_connection(federation_id, connection_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to remove connection: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to remove connection: {str(e)}"
        )


# ==================== TABLE MANAGEMENT ====================

@router.post("/{federation_id}/tables", response_model=FederationTablesResponse)
async def add_tables(
    federation_id: int,
    data: FederationAddTables,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Add specific tables to a federation.
    
    By default, a federation includes ALL tables from its connections.
    Use this endpoint to restrict the federation to specific tables only.
    
    Once you add explicit tables, only those tables will be considered
    part of the federation (not all tables from connections).
    """
    try:
        service = FederationService(db)
        return await service.add_tables(federation_id, data.table_ids)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to add tables: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to add tables: {str(e)}"
        )


@router.delete("/{federation_id}/tables/{table_id}", response_model=FederationTablesResponse)
async def remove_table(
    federation_id: int,
    table_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Remove a specific table from a federation.
    
    Only works for tables that were explicitly added.
    If no explicit tables remain, the federation will include
    all tables from its connections again.
    """
    try:
        service = FederationService(db)
        return await service.remove_table(federation_id, table_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to remove table: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to remove table: {str(e)}"
        )
