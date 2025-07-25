from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional

from ...database.database import get_db
from ...crud.token import get_current_user
from ...api.schemas.delta_sharing_schemas import (
    ShareCreate, ShareUpdate, ShareDetail,
    SchemaCreate, SchemaUpdate, SchemaDetail,
    ShareTableCreate, ShareTableUpdate, ShareTableDetail,
    RecipientCreate, RecipientUpdate, RecipientDetail,
    RecipientShareAssignment, ShareRecipientAssignment,
    SearchShares, SearchSchemas, SearchTables, SearchRecipients
)
from ...api.schemas.search import SearchResult
from ...services.delta_sharing.share_service import ShareService, SchemaService
from ...services.delta_sharing.table_service import TableService, RecipientService
from ...services.delta_sharing.dataset_integration_service import DatasetDeltaSharingService

router = APIRouter()

# ==================== SHARE MANAGEMENT ====================

@router.post("/shares", response_model=ShareDetail, status_code=status.HTTP_201_CREATED)
async def create_share(
    share_data: ShareCreate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Create a new Delta Sharing share"""
    try:
        service = ShareService(db)
        # In production, get organization_id from current_user
        organization_id = 1  # Hardcoded for now
        return await service.create_share(share_data, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating share: {str(e)}"
        )

@router.get("/shares/{share_id}", response_model=ShareDetail)
async def get_share(
    share_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Get share by ID"""
    try:
        service = ShareService(db)
        organization_id = 1  # Hardcoded for now
        return await service.get_share(share_id, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving share: {str(e)}"
        )

@router.put("/shares/{share_id}", response_model=ShareDetail)
async def update_share(
    share_id: int,
    share_data: ShareUpdate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Update share"""
    try:
        service = ShareService(db)
        organization_id = 1  # Hardcoded for now
        return await service.update_share(share_id, share_data, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating share: {str(e)}"
        )

@router.delete("/shares/{share_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_share(
    share_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Delete share"""
    try:
        service = ShareService(db)
        organization_id = 1  # Hardcoded for now
        await service.delete_share(share_id, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting share: {str(e)}"
        )

@router.post("/shares/search", response_model=SearchResult[ShareDetail])
async def search_shares(
    search: SearchShares,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Search shares with pagination and filters"""
    try:
        service = ShareService(db)
        organization_id = 1  # Hardcoded for now
        return await service.list_shares(search, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error searching shares: {str(e)}"
        )

# ==================== SCHEMA MANAGEMENT ====================

@router.post("/shares/{share_id}/schemas", response_model=SchemaDetail, status_code=status.HTTP_201_CREATED)
async def create_schema(
    share_id: int,
    schema_data: SchemaCreate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Create a new schema in a share"""
    try:
        service = SchemaService(db)
        organization_id = 1  # Hardcoded for now
        return await service.create_schema(share_id, schema_data, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating schema: {str(e)}"
        )

@router.get("/shares/{share_id}/schemas/{schema_id}", response_model=SchemaDetail)
async def get_schema(
    share_id: int,
    schema_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Get schema by ID"""
    try:
        service = SchemaService(db)
        organization_id = 1  # Hardcoded for now
        return await service.get_schema(share_id, schema_id, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving schema: {str(e)}"
        )

@router.put("/shares/{share_id}/schemas/{schema_id}", response_model=SchemaDetail)
async def update_schema(
    share_id: int,
    schema_id: int,
    schema_data: SchemaUpdate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Update schema"""
    try:
        service = SchemaService(db)
        organization_id = 1  # Hardcoded for now
        return await service.update_schema(share_id, schema_id, schema_data, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating schema: {str(e)}"
        )

@router.delete("/shares/{share_id}/schemas/{schema_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_schema(
    share_id: int,
    schema_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Delete schema"""
    try:
        service = SchemaService(db)
        organization_id = 1  # Hardcoded for now
        await service.delete_schema(share_id, schema_id, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting schema: {str(e)}"
        )

@router.post("/shares/{share_id}/schemas/search", response_model=SearchResult[SchemaDetail])
async def search_schemas(
    share_id: int,
    search: SearchSchemas,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Search schemas in a share with pagination and filters"""
    try:
        service = SchemaService(db)
        organization_id = 1  # Hardcoded for now
        return await service.list_schemas(share_id, search, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error searching schemas: {str(e)}"
        )

# ==================== TABLE MANAGEMENT ====================

@router.post("/shares/{share_id}/schemas/{schema_id}/tables", response_model=ShareTableDetail, status_code=status.HTTP_201_CREATED)
async def create_table(
    share_id: int,
    schema_id: int,
    table_data: ShareTableCreate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Create a new shared table in a schema"""
    try:
        service = TableService(db)
        organization_id = 1  # Hardcoded for now
        return await service.create_table(share_id, schema_id, table_data, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating table: {str(e)}"
        )

@router.get("/shares/{share_id}/schemas/{schema_id}/tables/{table_id}", response_model=ShareTableDetail)
async def get_table(
    share_id: int,
    schema_id: int,
    table_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Get table by ID"""
    try:
        service = TableService(db)
        organization_id = 1  # Hardcoded for now
        return await service.get_table(share_id, schema_id, table_id, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving table: {str(e)}"
        )

@router.put("/shares/{share_id}/schemas/{schema_id}/tables/{table_id}", response_model=ShareTableDetail)
async def update_table(
    share_id: int,
    schema_id: int,
    table_id: int,
    table_data: ShareTableUpdate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Update shared table"""
    try:
        service = TableService(db)
        organization_id = 1  # Hardcoded for now
        return await service.update_table(share_id, schema_id, table_id, table_data, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating table: {str(e)}"
        )

@router.delete("/shares/{share_id}/schemas/{schema_id}/tables/{table_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_table(
    share_id: int,
    schema_id: int,
    table_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Delete shared table"""
    try:
        service = TableService(db)
        organization_id = 1  # Hardcoded for now
        await service.delete_table(share_id, schema_id, table_id, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting table: {str(e)}"
        )

@router.post("/shares/{share_id}/schemas/{schema_id}/tables/search", response_model=SearchResult[ShareTableDetail])
async def search_tables(
    share_id: int,
    schema_id: int,
    search: SearchTables,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Search tables in a schema with pagination and filters"""
    try:
        service = TableService(db)
        organization_id = 1  # Hardcoded for now
        return await service.list_tables(share_id, schema_id, search, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error searching tables: {str(e)}"
        )

# ==================== RECIPIENT MANAGEMENT ====================

@router.post("/recipients", response_model=RecipientDetail, status_code=status.HTTP_201_CREATED)
async def create_recipient(
    recipient_data: RecipientCreate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Create a new recipient"""
    try:
        service = RecipientService(db)
        organization_id = 1  # Hardcoded for now
        return await service.create_recipient(recipient_data, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating recipient: {str(e)}"
        )

@router.get("/recipients/{recipient_id}", response_model=RecipientDetail)
async def get_recipient(
    recipient_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Get recipient by ID"""
    try:
        service = RecipientService(db)
        return await service.get_recipient(recipient_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving recipient: {str(e)}"
        )

@router.put("/recipients/{recipient_id}", response_model=RecipientDetail)
async def update_recipient(
    recipient_id: int,
    recipient_data: RecipientUpdate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Update recipient"""
    try:
        service = RecipientService(db)
        return await service.update_recipient(recipient_id, recipient_data)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating recipient: {str(e)}"
        )

@router.delete("/recipients/{recipient_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_recipient(
    recipient_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Delete recipient"""
    try:
        service = RecipientService(db)
        await service.delete_recipient(recipient_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting recipient: {str(e)}"
        )

@router.post("/recipients/search", response_model=SearchResult[RecipientDetail])
async def search_recipients(
    search: SearchRecipients,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Search recipients with pagination and filters"""
    try:
        service = RecipientService(db)
        return await service.list_recipients(search)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error searching recipients: {str(e)}"
        )

@router.post("/recipients/{recipient_id}/regenerate-token")
async def regenerate_recipient_token(
    recipient_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Regenerate bearer token for a recipient"""
    try:
        service = RecipientService(db)
        new_token = await service.regenerate_token(recipient_id)
        return {"message": "Token regenerated successfully", "token": new_token}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error regenerating token: {str(e)}"
        )

@router.post("/recipients/{recipient_id}/shares", status_code=status.HTTP_204_NO_CONTENT)
async def assign_shares_to_recipient(
    recipient_id: int,
    assignment: RecipientShareAssignment,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Assign shares to a recipient"""
    try:
        service = RecipientService(db)
        organization_id = 1  # Hardcoded for now
        await service.assign_shares(recipient_id, assignment.share_ids, organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error assigning shares: {str(e)}"
        )

@router.post("/shares/{share_id}/recipients", status_code=status.HTTP_204_NO_CONTENT)
async def assign_recipients_to_share(
    share_id: int,
    assignment: ShareRecipientAssignment,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Assign recipients to a share"""
    try:
        # This would be implemented to assign multiple recipients to a share
        # For now, we'll use the existing assign_shares method in reverse
        service = RecipientService(db)
        organization_id = 1  # Hardcoded for now
        
        for recipient_id in assignment.recipient_ids:
            await service.assign_shares(recipient_id, [share_id], organization_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error assigning recipients: {str(e)}"
        )

# # ==================== DATASET INTEGRATION ====================

# @router.post("/shares/{share_id}/schemas/{schema_id}/tables/{table_id}/sync")
# async def sync_dataset_to_delta(
#     share_id: int,
#     schema_id: int,
#     table_id: int,
#     db: AsyncSession = Depends(get_db),
#     # current_user = Depends(get_current_user),
# ):
#     """
#     Synchronize a dataset to Delta format for sharing.
    
#     This endpoint converts the associated dataset to Delta/Parquet format
#     and stores it in MinIO for Delta Sharing protocol compatibility.
#     """
#     try:
#         service = DatasetDeltaSharingService(db)
#         result = await service.sync_dataset_to_delta(table_id)
#         return result
#     except HTTPException:
#         raise
#     except Exception as e:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Error synchronizing dataset: {str(e)}"
#         )

# @router.get("/shares/{share_id}/schemas/{schema_id}/tables/{table_id}/sync-status")
# async def get_sync_status(
#     share_id: int,
#     schema_id: int,
#     table_id: int,
#     db: AsyncSession = Depends(get_db),
#     # current_user = Depends(get_current_user),
# ):
#     """
#     Get synchronization status for a Delta table.
    
#     Returns information about whether the dataset has been synchronized
#     to Delta format and the current state of the shared data.
#     """
#     try:
#         service = DatasetDeltaSharingService(db)
#         return await service.get_sync_status(table_id)
#     except HTTPException:
#         raise
#     except Exception as e:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Error getting sync status: {str(e)}"
#         )
