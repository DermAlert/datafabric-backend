"""
Table Relationships API Routes

These routes manage permanent relationships between tables in the metadata layer.
Relationships can be:
- Auto-discovered (FK constraints, naming patterns, data analysis)
- Manually defined by users
- Suggested by the system and confirmed by users

These relationships are stored at the metadata level and reused across datasets.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import logging

from ...database.session import get_db
from ...core.auth import get_current_user
from ..schemas.relationship_schemas import (
    TableRelationshipCreate,
    TableRelationshipUpdate,
    TableRelationshipResponse,
    RelationshipSuggestionResponse,
    DiscoverRelationshipsRequest,
    DiscoverRelationshipsResponse,
    AcceptSuggestionRequest,
    BulkAcceptSuggestionsRequest,
    SearchRelationships,
    SearchSuggestions,
    RelationshipGraph,
    RelationshipScopeEnum,
    RelationshipSourceEnum,
    SuggestionStatusEnum,
    JoinTypeEnum,
    RelationshipCardinalityEnum,
)
from ..schemas.search import SearchResult
from ...services.metadata.relationship_discovery_service import RelationshipDiscoveryService
from ...services.infrastructure.trino_client import get_trino_client

router = APIRouter()
logger = logging.getLogger(__name__)


# ==================== DISCOVERY ====================

@router.post("/discover", response_model=DiscoverRelationshipsResponse)
async def discover_relationships(
    request: DiscoverRelationshipsRequest,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Discover relationships between tables by extracting FK constraints from databases.
    
    **How it works:**
    1. Connects to source databases via Trino query passthrough
    2. Queries `information_schema` to extract PK/FK constraints
    3. Creates relationships or suggestions based on real database FKs
    
    **Supported Databases:**
    - MySQL / MariaDB
    - PostgreSQL
    - Other databases will show a warning (not supported yet)
    
    **Scope Control:**
    - `connection_ids`: Limit to specific connections
    - `table_ids`: Limit to specific tables
    
    **Results:**
    - By default creates suggestions for review (`auto_accept=false`)
    - Set `auto_accept=true` to create relationships directly
    
    **Note:** FK constraints only exist within the same database, so relationships
    discovered here are always INTRA_CONNECTION (same database).
    """
    try:
        service = RelationshipDiscoveryService(db)
        
        # Get Trino client for FK constraint extraction
        trino_client = None
        try:
            trino_client = await get_trino_client()
        except Exception as e:
            logger.warning(f"Could not get Trino client for FK extraction: {e}")
        
        return await service.discover_relationships(request, trino_client=trino_client)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to discover relationships: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to discover relationships: {str(e)}"
        )


# ==================== SUGGESTIONS ====================
# NOTE: These routes MUST come before /{relationship_id} to avoid path conflicts

@router.get("/suggestions", response_model=SearchResult[RelationshipSuggestionResponse])
async def list_suggestions(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    status: Optional[SuggestionStatusEnum] = Query(None, description="Filter by status (pending, accepted, rejected). If not provided, returns all."),
    connection_id: Optional[int] = None,
    table_id: Optional[int] = None,
    min_confidence: Optional[float] = Query(None, ge=0.0, le=1.0),
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    List relationship suggestions with optional status filter.
    
    **Filters:**
    - `status`: Filter by status (pending, accepted, rejected). If not provided, returns all.
    - `connection_id`: Filter by connection
    - `table_id`: Filter suggestions involving this table
    - `min_confidence`: Minimum confidence score (0.0-1.0)
    """
    try:
        service = RelationshipDiscoveryService(db)
        suggestions, total = await service.list_suggestions(
            connection_id=connection_id,
            table_id=table_id,
            status_filter=status,
            min_confidence=min_confidence,
            page=page,
            size=size
        )
        
        return SearchResult(
            items=suggestions,
            total=total,
            page=page,
            size=size,
            pages=(total + size - 1) // size
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list suggestions: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list suggestions: {str(e)}"
        )


@router.get("/suggestions/pending", response_model=SearchResult[RelationshipSuggestionResponse])
async def list_pending_suggestions(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    connection_id: Optional[int] = None,
    table_id: Optional[int] = None,
    min_confidence: Optional[float] = Query(None, ge=0.0, le=1.0),
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    List pending relationship suggestions.
    
    These are relationships discovered by the system that need
    user confirmation before becoming permanent.
    
    **Filters:**
    - `connection_id`: Filter by connection
    - `table_id`: Filter suggestions involving this table
    - `min_confidence`: Minimum confidence score (0.0-1.0)
    """
    try:
        service = RelationshipDiscoveryService(db)
        suggestions, total = await service.list_suggestions(
            connection_id=connection_id,
            table_id=table_id,
            status_filter=SuggestionStatusEnum.PENDING,
            min_confidence=min_confidence,
            page=page,
            size=size
        )
        
        return SearchResult(
            items=suggestions,
            total=total,
            page=page,
            size=size,
            pages=(total + size - 1) // size
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list suggestions: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list suggestions: {str(e)}"
        )


@router.post("/suggestions/{suggestion_id}/accept", response_model=TableRelationshipResponse)
async def accept_suggestion(
    suggestion_id: int,
    cardinality: Optional[RelationshipCardinalityEnum] = None,
    default_join_type: Optional[JoinTypeEnum] = Query(
        None, 
        description="Join type for the relationship. **Defaults to FULL** (safest for Data Fabric)."
    ),
    name: Optional[str] = None,
    description: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Accept a relationship suggestion.
    
    This converts the suggestion into a permanent, verified relationship.
    
    **Default join type is FULL OUTER JOIN** - the safest option for a Data Fabric
    as it preserves all data from both tables and reveals data quality issues.
    
    You can optionally override:
    - `cardinality`: Relationship cardinality (ONE_TO_ONE, ONE_TO_MANY, etc.)
    - `default_join_type`: Join strategy (INNER, LEFT, RIGHT, FULL). **Default: FULL**
    - `name`, `description`: Metadata
    """
    try:
        service = RelationshipDiscoveryService(db)
        return await service.accept_suggestion(
            suggestion_id,
            cardinality=cardinality,
            default_join_type=default_join_type,
            name=name,
            description=description
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to accept suggestion: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to accept suggestion: {str(e)}"
        )


@router.post("/suggestions/{suggestion_id}/reject", status_code=status.HTTP_204_NO_CONTENT)
async def reject_suggestion(
    suggestion_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Reject a relationship suggestion.
    
    The suggestion will be marked as rejected and won't appear
    in pending lists anymore.
    """
    try:
        service = RelationshipDiscoveryService(db)
        await service.reject_suggestion(suggestion_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to reject suggestion: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to reject suggestion: {str(e)}"
        )


@router.post("/suggestions/{suggestion_id}/reset", response_model=RelationshipSuggestionResponse)
async def reset_suggestion(
    suggestion_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Reset a rejected or accepted suggestion back to pending.
    
    Use this when you want to reconsider a previously rejected suggestion,
    or when you deleted a relationship and want the suggestion to appear
    in pending again.
    
    **Note:** If the suggestion was accepted and the relationship still exists,
    you must delete the relationship first before resetting the suggestion.
    """
    try:
        service = RelationshipDiscoveryService(db)
        return await service.reset_suggestion(suggestion_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to reset suggestion: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to reset suggestion: {str(e)}"
        )


@router.post("/suggestions/bulk-accept", response_model=List[TableRelationshipResponse])
async def bulk_accept_suggestions(
    request: BulkAcceptSuggestionsRequest,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Accept multiple suggestions at once.
    
    Useful for quickly accepting high-confidence suggestions.
    
    **Default join type is FULL OUTER JOIN** - the safest option for a Data Fabric
    as it preserves all data from both tables and reveals data quality issues.
    
    You can override `default_join_type` to apply a different join type to all
    accepted relationships.
    """
    try:
        service = RelationshipDiscoveryService(db)
        results = []
        
        for suggestion_id in request.suggestion_ids:
            try:
                rel = await service.accept_suggestion(
                    suggestion_id,
                    default_join_type=request.default_join_type
                )
                results.append(rel)
            except HTTPException as e:
                if e.status_code != status.HTTP_404_NOT_FOUND:
                    raise
                # Skip not found suggestions
        
        return results
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to bulk accept suggestions: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to bulk accept suggestions: {str(e)}"
        )


# ==================== GRAPH/VISUALIZATION ====================

@router.get("/graph", response_model=RelationshipGraph)
async def get_relationship_graph(
    connection_ids: Optional[str] = Query(None, description="Comma-separated connection IDs"),
    table_ids: Optional[str] = Query(None, description="Comma-separated table IDs"),
    include_inactive: bool = False,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Get a graph representation of relationships for visualization.
    
    Returns nodes (tables) and edges (relationships) suitable for
    rendering with graph visualization libraries.
    
    **Parameters:**
    - `connection_ids`: Filter to specific connections (comma-separated)
    - `table_ids`: Filter to specific tables (comma-separated)
    - `include_inactive`: Include inactive relationships
    """
    try:
        # Parse comma-separated IDs
        conn_ids = None
        if connection_ids:
            conn_ids = [int(x.strip()) for x in connection_ids.split(',')]
        
        tbl_ids = None
        if table_ids:
            tbl_ids = [int(x.strip()) for x in table_ids.split(',')]
        
        service = RelationshipDiscoveryService(db)
        return await service.get_relationship_graph(
            connection_ids=conn_ids,
            table_ids=tbl_ids,
            include_inactive=include_inactive
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get relationship graph: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get relationship graph: {str(e)}"
        )


# ==================== TABLE-SPECIFIC ====================

@router.get("/table/{table_id}", response_model=List[TableRelationshipResponse])
async def get_relationships_for_table(
    table_id: int,
    include_inactive: bool = False,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Get all relationships involving a specific table.
    
    Returns relationships where the table is either on the
    left side (parent) or right side (child).
    """
    try:
        service = RelationshipDiscoveryService(db)
        relationships, _ = await service.list_relationships(
            table_id=table_id,
            is_active=None if include_inactive else True,
            page=1,
            size=1000  # Get all
        )
        return relationships
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get relationships for table: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get relationships for table: {str(e)}"
        )


@router.get("/connection/{connection_id}", response_model=List[TableRelationshipResponse])
async def get_relationships_for_connection(
    connection_id: int,
    scope: Optional[RelationshipScopeEnum] = None,
    include_inactive: bool = False,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Get all relationships involving tables from a specific connection.
    
    **Parameters:**
    - `scope`: Filter by INTRA_CONNECTION (within this connection) or 
               INTER_CONNECTION (linking to other connections)
    """
    try:
        service = RelationshipDiscoveryService(db)
        relationships, _ = await service.list_relationships(
            connection_id=connection_id,
            scope=scope,
            is_active=None if include_inactive else True,
            page=1,
            size=1000
        )
        return relationships
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get relationships for connection: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get relationships for connection: {str(e)}"
        )


# ==================== RELATIONSHIPS CRUD ====================
# NOTE: Routes with /{relationship_id} MUST come LAST to avoid capturing other paths

@router.post("/", response_model=TableRelationshipResponse, status_code=status.HTTP_201_CREATED)
async def create_relationship(
    data: TableRelationshipCreate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Create a new table relationship manually.
    
    Use this when you know two columns are related but the system
    didn't discover it automatically.
    
    The relationship is automatically marked as verified since it's
    manually created.
    
    **Parameters:**
    - `left_table_id`, `left_column_id`: The "parent" or "one" side
    - `right_table_id`, `right_column_id`: The "child" or "many" side
    - `cardinality`: Optional relationship cardinality
    - `default_join_type`: Default join type (INNER, LEFT, etc.)
    """
    try:
        service = RelationshipDiscoveryService(db)
        return await service.create_relationship(data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create relationship: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create relationship: {str(e)}"
        )


@router.post("/search", response_model=SearchResult[TableRelationshipResponse])
async def search_relationships(
    search: SearchRelationships,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Search and list relationships with filters.
    
    **Filters:**
    - `connection_id`: Filter by connection
    - `table_id`: Filter relationships involving this table
    - `scope`: INTRA_CONNECTION or INTER_CONNECTION
    - `source`: How it was discovered (AUTO_FK, AUTO_NAME, MANUAL, etc.)
    - `is_verified`: Only verified relationships
    - `is_active`: Only active relationships
    """
    try:
        service = RelationshipDiscoveryService(db)
        relationships, total = await service.list_relationships(
            connection_id=search.connection_id,
            table_id=search.table_id,
            scope=search.scope,
            is_active=search.is_active,
            page=search.page,
            size=search.size
        )
        
        return SearchResult(
            items=relationships,
            total=total,
            page=search.page,
            size=search.size,
            pages=(total + search.size - 1) // search.size
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to search relationships: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to search relationships: {str(e)}"
        )


@router.get("/{relationship_id}", response_model=TableRelationshipResponse)
async def get_relationship(
    relationship_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Get a relationship by ID."""
    try:
        service = RelationshipDiscoveryService(db)
        return await service.get_relationship(relationship_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get relationship: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get relationship: {str(e)}"
        )


@router.put("/{relationship_id}", response_model=TableRelationshipResponse)
async def update_relationship(
    relationship_id: int,
    data: TableRelationshipUpdate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Update a relationship.
    
    You can update:
    - `name`, `description`: Metadata
    - `cardinality`: Relationship type
    - `default_join_type`: Default join strategy
    - `is_verified`: Mark as verified/unverified
    - `is_active`: Enable/disable without deleting
    """
    try:
        service = RelationshipDiscoveryService(db)
        return await service.update_relationship(relationship_id, data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update relationship: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update relationship: {str(e)}"
        )


@router.delete("/{relationship_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_relationship(
    relationship_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Delete a relationship permanently."""
    try:
        service = RelationshipDiscoveryService(db)
        await service.delete_relationship(relationship_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete relationship: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete relationship: {str(e)}"
        )
