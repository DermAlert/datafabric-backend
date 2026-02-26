import os
import re
import unicodedata
import asyncio

import boto3
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, desc, func
from typing import List, Optional

from ...database.session import get_db, reraise_db_timeout, reraise_db_timeout
from ...core.auth import get_current_user
from ...api.schemas.delta_sharing_schemas import (
    ShareCreate, ShareUpdate, ShareDetail,
    SchemaCreate, SchemaUpdate, SchemaDetail,
    ShareTableCreate, ShareTableUpdate, ShareTableDetail,
    RecipientCreate, RecipientUpdate, RecipientDetail,
    RecipientShareAssignment, ShareRecipientAssignment,
    SearchShares, SearchSchemas, SearchTables, SearchRecipients,
    # Bronze/Silver Integration
    AvailableDataset, DatasetSourceType,
    CreateTableFromBronze, CreateTableFromSilver, IntegrationTableDetail,
    PinDeltaVersionRequest, UnpinDeltaVersionResponse,
    # Virtualized Integration
    CreateTableFromBronzeVirtualized, CreateTableFromSilverVirtualized,
    VirtualizedSourceType
)
from ...api.schemas.search import SearchResult
from ...services.delta_sharing.share_service import ShareService, SchemaService
from ...services.delta_sharing.table_service import TableService, RecipientService
from ...services.delta_sharing.dataset_integration_service import DatasetDeltaSharingService
# Bronze/Silver models
from ...database.models.bronze import BronzePersistentConfig, BronzeExecution, BronzeExecutionStatus, BronzeVirtualizedConfig
from ...database.models.silver import TransformConfig, TransformExecution, TransformStatus, VirtualizedConfig
from ...database.models.delta_sharing import ShareTable, ShareSchema, Share, TableShareStatus, ShareTableSourceType
from ...database.models.core import Dataset

router = APIRouter()


def _make_s3_client():
    """Return a boto3 S3 client pointed at the internal MinIO endpoint."""
    endpoint_url = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    if not endpoint_url.startswith(("http://", "https://")):
        endpoint_url = f"http://{endpoint_url}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minio"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
        region_name="us-east-1",
        use_ssl=False,
        verify=False,
    )


def _delta_log_version_exists(storage_location: str, version: int) -> bool:
    """
    Check whether a specific Delta version commit file exists in MinIO.

    Accepts storage_location in any of these forms:
        bucket/prefix   (bare path stored in share_tables)
        s3a://bucket/prefix
        s3://bucket/prefix
    """
    loc = storage_location
    for scheme in ("s3a://", "s3://", "minio://"):
        if loc.startswith(scheme):
            loc = loc[len(scheme):]
            break
    loc = loc.strip("/")

    parts = loc.split("/", 1)
    bucket = parts[0]
    prefix = parts[1].rstrip("/") if len(parts) > 1 else ""

    log_key = f"{prefix}/_delta_log/{version:020d}.json"

    try:
        client = _make_s3_client()
        client.head_object(Bucket=bucket, Key=log_key)
        return True
    except Exception:
        return False


async def _assert_delta_version_exists(storage_location: str, version: int) -> None:
    """
    Raise HTTP 400 if *version* does not exist in the Delta log.
    Runs the blocking boto3 call in a thread pool to avoid blocking the event loop.
    """
    loop = asyncio.get_event_loop()
    exists = await loop.run_in_executor(
        None, _delta_log_version_exists, storage_location, version
    )
    if not exists:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Delta version {version} does not exist for this table",
        )


def _sanitize_table_name(name: str) -> str:
    """Convert an arbitrary config name into a valid Delta Sharing table name.

    Strips accents, lowercases, replaces runs of non-alphanumeric chars with
    underscores, and trims leading/trailing underscores.
    """
    normalized = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
    sanitized = re.sub(r'[^a-zA-Z0-9]+', '_', normalized.lower())
    sanitized = re.sub(r'_+', '_', sanitized).strip('_')
    return sanitized[:255] or "table"


async def _unique_table_name(db: AsyncSession, schema_id: int, base_name: str, source_type: str) -> str:
    """Return *base_name* if available, otherwise append a source_type suffix.

    Tries: base_name  →  base_name_silver  →  base_name_silver_2  → …
    """
    candidate = base_name
    suffix = source_type.lower().replace("_", "_")
    attempt = 0
    while True:
        existing = await db.execute(
            select(ShareTable).where(
                and_(ShareTable.schema_id == schema_id, ShareTable.name == candidate)
            )
        )
        if not existing.scalar_one_or_none():
            return candidate
        attempt += 1
        if attempt == 1:
            candidate = f"{base_name}_{suffix}"
        else:
            candidate = f"{base_name}_{suffix}_{attempt}"

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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
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
        reraise_db_timeout(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error assigning recipients: {str(e)}"
        )


# ==================== BRONZE/SILVER INTEGRATION ====================

@router.get("/integration/datasets", response_model=List[AvailableDataset])
async def list_available_datasets(
    source_type: Optional[DatasetSourceType] = Query(None, description="Filter by source type (bronze/silver)"),
    db: AsyncSession = Depends(get_db),
):
    """
    List all Bronze and Silver datasets available for Delta Sharing.
    Only returns configs that have been executed successfully at least once.
    """
    try:
        datasets = []

        # ── Bronze configs ─────────────────────────────────────────────────────
        # Single query: configs + latest successful execution via LATERAL/subquery.
        # Uses a correlated subquery ranked by finished_at DESC so we avoid N+1.
        if source_type is None or source_type == DatasetSourceType.BRONZE:
            # Subquery: pick the row with max finished_at per config_id
            bronze_exec_sq = (
                select(
                    BronzeExecution.config_id,
                    func.max(BronzeExecution.finished_at).label('max_finished_at'),
                )
                .where(BronzeExecution.status == BronzeExecutionStatus.SUCCESS)
                .group_by(BronzeExecution.config_id)
                .subquery()
            )
            bronze_query = (
                select(BronzePersistentConfig, BronzeExecution)
                .where(BronzePersistentConfig.last_execution_status == BronzeExecutionStatus.SUCCESS)
                .outerjoin(
                    bronze_exec_sq,
                    bronze_exec_sq.c.config_id == BronzePersistentConfig.id,
                )
                .outerjoin(
                    BronzeExecution,
                    and_(
                        BronzeExecution.config_id == BronzePersistentConfig.id,
                        BronzeExecution.finished_at == bronze_exec_sq.c.max_finished_at,
                        BronzeExecution.status == BronzeExecutionStatus.SUCCESS,
                    ),
                )
            )
            bronze_result = await db.execute(bronze_query)
            for config, latest_exec in bronze_result.fetchall():
                output_path = None
                if latest_exec and latest_exec.output_paths:
                    output_path = latest_exec.output_paths[0]
                datasets.append(AvailableDataset(
                    config_id=config.id,
                    name=config.name,
                    description=config.description,
                    source_type=DatasetSourceType.BRONZE,
                    output_path=output_path,
                    current_version=latest_exec.delta_version if latest_exec else None,
                    last_execution_status=config.last_execution_status,
                    last_execution_at=latest_exec.finished_at if latest_exec else None,
                    total_rows=latest_exec.rows_ingested if latest_exec else None,
                ))

        # ── Silver configs ─────────────────────────────────────────────────────
        if source_type is None or source_type == DatasetSourceType.SILVER:
            silver_exec_sq = (
                select(
                    TransformExecution.config_id,
                    func.max(TransformExecution.finished_at).label('max_finished_at'),
                )
                .where(TransformExecution.status == TransformStatus.SUCCESS)
                .group_by(TransformExecution.config_id)
                .subquery()
            )
            silver_query = (
                select(TransformConfig, TransformExecution)
                .where(TransformConfig.last_execution_status == TransformStatus.SUCCESS)
                .outerjoin(
                    silver_exec_sq,
                    silver_exec_sq.c.config_id == TransformConfig.id,
                )
                .outerjoin(
                    TransformExecution,
                    and_(
                        TransformExecution.config_id == TransformConfig.id,
                        TransformExecution.finished_at == silver_exec_sq.c.max_finished_at,
                        TransformExecution.status == TransformStatus.SUCCESS,
                    ),
                )
            )
            silver_result = await db.execute(silver_query)
            for config, latest_exec in silver_result.fetchall():
                datasets.append(AvailableDataset(
                    config_id=config.id,
                    name=config.name,
                    description=config.description,
                    source_type=DatasetSourceType.SILVER,
                    output_path=latest_exec.output_path if latest_exec else None,
                    current_version=latest_exec.delta_version if latest_exec else None,
                    last_execution_status=config.last_execution_status,
                    last_execution_at=latest_exec.finished_at if latest_exec else None,
                    total_rows=latest_exec.rows_output if latest_exec else None,
                ))
        
        return datasets
    except Exception as e:
        reraise_db_timeout(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing available datasets: {str(e)}"
        )


@router.post("/shares/{share_id}/schemas/{schema_id}/tables/from-bronze", 
             response_model=List[IntegrationTableDetail], 
             status_code=status.HTTP_201_CREATED)
async def create_table_from_bronze(
    share_id: int,
    schema_id: int,
    data: CreateTableFromBronze,
    db: AsyncSession = Depends(get_db),
):
    """
    Create Delta Sharing table(s) from a Bronze Persistent Config.
    When path_index is omitted, creates one table per output path.
    When path_index is specified, creates a single table for that path.
    """
    try:
        organization_id = 1  # Hardcoded for now
        
        # Verify share and schema exist
        schema_query = select(ShareSchema, Share).join(Share).where(
            and_(
                ShareSchema.id == schema_id,
                ShareSchema.share_id == share_id,
                Share.organization_id == organization_id
            )
        )
        schema_result = await db.execute(schema_query)
        row = schema_result.first()
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schema or share not found"
            )
        db_schema, db_share = row
        
        # Get Bronze config
        bronze_query = select(BronzePersistentConfig).where(
            BronzePersistentConfig.id == data.bronze_config_id
        )
        bronze_result = await db.execute(bronze_query)
        bronze_config = bronze_result.scalar_one_or_none()
        
        if not bronze_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Bronze config {data.bronze_config_id} not found"
            )
        
        # Resolve output paths
        connection_names = []
        if bronze_config.known_output_paths:
            sorted_items = sorted(bronze_config.known_output_paths.items())
            connection_names = [k for k, _ in sorted_items]
            stable_paths = [v for _, v in sorted_items]
        else:
            exec_query = select(BronzeExecution).where(
                and_(
                    BronzeExecution.config_id == bronze_config.id,
                    BronzeExecution.status == BronzeExecutionStatus.SUCCESS
                )
            ).order_by(desc(BronzeExecution.finished_at)).limit(1)
            exec_result = await db.execute(exec_query)
            latest_exec = exec_result.scalar_one_or_none()
            
            if not latest_exec or not latest_exec.output_paths:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Bronze config has no successful executions with output paths"
                )
            stable_paths = latest_exec.output_paths
        
        # Determine which path indices to create tables for
        if data.path_index is not None:
            if data.path_index >= len(stable_paths):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"path_index {data.path_index} is out of range. Available paths: {len(stable_paths)}"
                )
            indices_to_create = [data.path_index]
        else:
            indices_to_create = list(range(len(stable_paths)))
        
        # Create or get legacy dataset entry for FK
        dataset_query = select(Dataset).where(Dataset.name == f"bronze_{bronze_config.id}_{bronze_config.name}")
        dataset_result = await db.execute(dataset_query)
        legacy_dataset = dataset_result.scalar_one_or_none()
        
        if not legacy_dataset:
            legacy_dataset = Dataset(
                name=f"bronze_{bronze_config.id}_{bronze_config.name}",
                description=bronze_config.description,
                storage_type="delta",
                refresh_type="manual",
                status="active"
            )
            db.add(legacy_dataset)
            await db.flush()
        
        # Validate pinned_delta_version against the actual Delta log
        if data.pinned_delta_version is not None and stable_paths:
            # Use the first path as representative (all paths share the same bucket layout)
            check_path = stable_paths[data.path_index] if data.path_index is not None and data.path_index < len(stable_paths) else stable_paths[0]
            await _assert_delta_version_exists(check_path, data.pinned_delta_version)

        base_name = _sanitize_table_name(bronze_config.name)
        table_description = data.description if data.description is not None else bronze_config.description
        created_tables = []
        
        for idx in indices_to_create:
            output_path = stable_paths[idx]
            
            storage_location = output_path
            if storage_location.startswith('s3a://'):
                storage_location = storage_location[6:]
            elif storage_location.startswith('s3://'):
                storage_location = storage_location[5:]
            storage_location = storage_location.rstrip('/')
            
            # Resolve table name
            if data.name and len(indices_to_create) == 1:
                table_name = data.name
            elif len(stable_paths) > 1 and connection_names:
                suffix = _sanitize_table_name(connection_names[idx])
                table_name = f"{base_name}_{suffix}"
            else:
                table_name = base_name
            
            # Ensure unique name within the schema (auto-suffix if needed)
            table_name = await _unique_table_name(db, schema_id, table_name, "bronze")
            
            db_table = ShareTable(
                schema_id=schema_id,
                name=table_name,
                description=table_description,
                dataset_id=legacy_dataset.id,
                source_type=ShareTableSourceType.BRONZE,
                bronze_persistent_config_id=bronze_config.id,
                storage_location=storage_location,
                status=TableShareStatus.ACTIVE,
                share_mode=data.share_mode,
                filter_condition=data.filter_condition,
                current_version=bronze_config.current_delta_version or 1,
                pinned_delta_version=data.pinned_delta_version,
                table_format="delta"
            )
            db.add(db_table)
            await db.flush()
            
            created_tables.append(IntegrationTableDetail(
                table_id=db_table.id,
                table_name=db_table.name,
                share_id=db_share.id,
                share_name=db_share.name,
                schema_id=db_schema.id,
                schema_name=db_schema.name,
                source_type=DatasetSourceType.BRONZE,
                source_config_id=bronze_config.id,
                source_config_name=bronze_config.name,
                storage_location=storage_location,
                current_version=db_table.current_version,
                share_mode=db_table.share_mode,
                status=db_table.status.value
            ))
        
        if not created_tables:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="All tables for this config already exist in this schema"
            )
        
        await db.commit()
        return created_tables
        
    except HTTPException:
        raise
    except Exception as e:
        reraise_db_timeout(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating table from Bronze: {str(e)}"
        )


@router.post("/shares/{share_id}/schemas/{schema_id}/tables/from-silver",
             response_model=IntegrationTableDetail,
             status_code=status.HTTP_201_CREATED)
async def create_table_from_silver(
    share_id: int,
    schema_id: int,
    data: CreateTableFromSilver,
    db: AsyncSession = Depends(get_db),
):
    """
    Create a Delta Sharing table from a Silver Transform Config.
    Automatically links to the Silver Delta Lake storage location.
    """
    try:
        organization_id = 1  # Hardcoded for now
        
        # Verify share and schema exist
        schema_query = select(ShareSchema, Share).join(Share).where(
            and_(
                ShareSchema.id == schema_id,
                ShareSchema.share_id == share_id,
                Share.organization_id == organization_id
            )
        )
        schema_result = await db.execute(schema_query)
        row = schema_result.first()
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schema or share not found"
            )
        db_schema, db_share = row
        
        # Get Silver config
        silver_query = select(TransformConfig).where(
            TransformConfig.id == data.silver_config_id
        )
        silver_result = await db.execute(silver_query)
        silver_config = silver_result.scalar_one_or_none()
        
        if not silver_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Silver config {data.silver_config_id} not found"
            )
        
        # Get latest successful execution for storage path
        exec_query = select(TransformExecution).where(
            and_(
                TransformExecution.config_id == silver_config.id,
                TransformExecution.status == TransformStatus.SUCCESS
            )
        ).order_by(desc(TransformExecution.finished_at)).limit(1)
        exec_result = await db.execute(exec_query)
        latest_exec = exec_result.scalar_one_or_none()
        
        if not latest_exec or not latest_exec.output_path:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Silver config has no successful executions with output path"
            )
        
        output_path = latest_exec.output_path
        
        # Convert s3a:// to internal path format
        storage_location = output_path
        if storage_location.startswith('s3a://'):
            storage_location = storage_location[6:]
        elif storage_location.startswith('s3://'):
            storage_location = storage_location[5:]
        
        # Remove trailing slash
        storage_location = storage_location.rstrip('/')
        
        # Prevent sharing the same silver config twice in the same schema
        dup_check = await db.execute(
            select(ShareTable).where(
                and_(
                    ShareTable.schema_id == schema_id,
                    ShareTable.silver_persistent_config_id == silver_config.id,
                )
            )
        )
        if dup_check.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Silver config '{silver_config.name}' is already shared in this schema"
            )

        # Resolve name and description from the Silver config when not provided
        base_name = data.name if data.name else _sanitize_table_name(silver_config.name)
        table_name = await _unique_table_name(db, schema_id, base_name, "silver")
        table_description = data.description if data.description is not None else silver_config.description
        
        # Create or get legacy dataset entry for FK
        dataset_query = select(Dataset).where(Dataset.name == f"silver_{silver_config.id}_{silver_config.name}")
        dataset_result = await db.execute(dataset_query)
        legacy_dataset = dataset_result.scalar_one_or_none()
        
        if not legacy_dataset:
            legacy_dataset = Dataset(
                name=f"silver_{silver_config.id}_{silver_config.name}",
                description=silver_config.description,
                storage_type="delta",
                refresh_type="manual",
                status="active"
            )
            db.add(legacy_dataset)
            await db.flush()
        
        # Validate pinned_delta_version against the actual Delta log
        if data.pinned_delta_version is not None:
            await _assert_delta_version_exists(output_path, data.pinned_delta_version)

        # Create the table
        db_table = ShareTable(
            schema_id=schema_id,
            name=table_name,
            description=table_description,
            dataset_id=legacy_dataset.id,
            source_type=ShareTableSourceType.SILVER,
            silver_persistent_config_id=silver_config.id,
            storage_location=storage_location,
            status=TableShareStatus.ACTIVE,
            share_mode=data.share_mode,
            filter_condition=data.filter_condition,
            current_version=latest_exec.delta_version or 1,
            pinned_delta_version=data.pinned_delta_version,
            table_format="delta"
        )
        
        db.add(db_table)
        await db.commit()
        
        return IntegrationTableDetail(
            table_id=db_table.id,
            table_name=db_table.name,
            share_id=db_share.id,
            share_name=db_share.name,
            schema_id=db_schema.id,
            schema_name=db_schema.name,
            source_type=DatasetSourceType.SILVER,
            source_config_id=silver_config.id,
            source_config_name=silver_config.name,
            storage_location=storage_location,
            current_version=db_table.current_version,
            share_mode=db_table.share_mode,
            status=db_table.status.value
        )
        
    except HTTPException:
        raise
    except Exception as e:
        reraise_db_timeout(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating table from Silver: {str(e)}"
        )


@router.get("/integration/datasets/bronze/{config_id}", response_model=AvailableDataset)
async def get_bronze_dataset_info(
    config_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get detailed info about a Bronze config for Delta Sharing"""
    try:
        # Get Bronze config
        bronze_query = select(BronzePersistentConfig).where(
            BronzePersistentConfig.id == config_id
        )
        bronze_result = await db.execute(bronze_query)
        bronze_config = bronze_result.scalar_one_or_none()
        
        if not bronze_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Bronze config {config_id} not found"
            )
        
        # Get latest successful execution
        exec_query = select(BronzeExecution).where(
            and_(
                BronzeExecution.config_id == config_id,
                BronzeExecution.status == BronzeExecutionStatus.SUCCESS
            )
        ).order_by(desc(BronzeExecution.finished_at)).limit(1)
        exec_result = await db.execute(exec_query)
        latest_exec = exec_result.scalar_one_or_none()
        
        output_path = None
        if latest_exec and latest_exec.output_paths:
            output_path = latest_exec.output_paths[0]
        
        return AvailableDataset(
            config_id=bronze_config.id,
            name=bronze_config.name,
            description=bronze_config.description,
            source_type=DatasetSourceType.BRONZE,
            output_path=output_path,
            current_version=latest_exec.delta_version if latest_exec else None,
            last_execution_status=bronze_config.last_execution_status,
            last_execution_at=latest_exec.finished_at if latest_exec else None,
            total_rows=latest_exec.rows_ingested if latest_exec else None
        )
    except HTTPException:
        raise
    except Exception as e:
        reraise_db_timeout(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting Bronze config info: {str(e)}"
        )


@router.get("/integration/datasets/silver/{config_id}", response_model=AvailableDataset)
async def get_silver_dataset_info(
    config_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get detailed info about a Silver config for Delta Sharing"""
    try:
        # Get Silver config
        silver_query = select(TransformConfig).where(
            TransformConfig.id == config_id
        )
        silver_result = await db.execute(silver_query)
        silver_config = silver_result.scalar_one_or_none()
        
        if not silver_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Silver config {config_id} not found"
            )
        
        # Get latest successful execution
        exec_query = select(TransformExecution).where(
            and_(
                TransformExecution.config_id == config_id,
                TransformExecution.status == TransformStatus.SUCCESS
            )
        ).order_by(desc(TransformExecution.finished_at)).limit(1)
        exec_result = await db.execute(exec_query)
        latest_exec = exec_result.scalar_one_or_none()
        
        return AvailableDataset(
            config_id=silver_config.id,
            name=silver_config.name,
            description=silver_config.description,
            source_type=DatasetSourceType.SILVER,
            output_path=latest_exec.output_path if latest_exec else None,
            current_version=latest_exec.delta_version if latest_exec else None,
            last_execution_status=silver_config.last_execution_status,
            last_execution_at=latest_exec.finished_at if latest_exec else None,
            total_rows=latest_exec.rows_output if latest_exec else None
        )
    except HTTPException:
        raise
    except Exception as e:
        reraise_db_timeout(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting Silver config info: {str(e)}"
        )


# ==================== VIRTUALIZED TABLE INTEGRATION ====================

@router.post("/shares/{share_id}/schemas/{schema_id}/tables/from-bronze-virtualized",
             response_model=IntegrationTableDetail,
             status_code=status.HTTP_201_CREATED)
async def create_table_from_bronze_virtualized(
    share_id: int,
    schema_id: int,
    data: CreateTableFromBronzeVirtualized,
    db: AsyncSession = Depends(get_db),
):
    """
    Create a Delta Sharing table from a Bronze Virtualized Config.
    
    The table will serve data on-demand via Trino (no materialization).
    Recipients can access the data through the Data API endpoint:
    POST /api/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/data
    """
    try:
        organization_id = 1  # Hardcoded for now

        # Verify share and schema exist
        schema_query = select(ShareSchema, Share).join(Share).where(
            and_(
                ShareSchema.id == schema_id,
                ShareSchema.share_id == share_id,
                Share.organization_id == organization_id
            )
        )
        schema_result = await db.execute(schema_query)
        row = schema_result.first()
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schema or share not found"
            )
        db_schema, db_share = row

        # Get Bronze Virtualized Config
        config_query = select(BronzeVirtualizedConfig).where(
            and_(
                BronzeVirtualizedConfig.id == data.bronze_virtualized_config_id,
                BronzeVirtualizedConfig.is_active == True
            )
        )
        config_result = await db.execute(config_query)
        config = config_result.scalar_one_or_none()

        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Bronze Virtualized Config {data.bronze_virtualized_config_id} not found or inactive"
            )

        # Prevent sharing the same bronze virtualized config twice in the same schema
        dup_check = await db.execute(
            select(ShareTable).where(
                and_(
                    ShareTable.schema_id == schema_id,
                    ShareTable.bronze_virtualized_config_id == config.id,
                )
            )
        )
        if dup_check.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Bronze Virtualized config '{config.name}' is already shared in this schema"
            )

        # Resolve name and description from the Bronze Virtualized config when not provided
        base_name = data.name if data.name else _sanitize_table_name(config.name)
        table_name = await _unique_table_name(db, schema_id, base_name, "bronze_virtualized")
        table_description = data.description if data.description is not None else getattr(config, 'description', None)

        # Create the virtualized share table (no storage_location, no dataset_id needed)
        db_table = ShareTable(
            schema_id=schema_id,
            name=table_name,
            description=table_description,
            dataset_id=None,
            source_type=ShareTableSourceType.BRONZE_VIRTUALIZED,
            bronze_virtualized_config_id=config.id,
            silver_virtualized_config_id=None,
            storage_location=None,
            status=TableShareStatus.ACTIVE,
            share_mode='full',
            current_version=1,
            table_format="virtualized"
        )

        db.add(db_table)
        await db.commit()

        return IntegrationTableDetail(
            table_id=db_table.id,
            table_name=db_table.name,
            share_id=db_share.id,
            share_name=db_share.name,
            schema_id=db_schema.id,
            schema_name=db_schema.name,
            source_type=DatasetSourceType.BRONZE,
            source_config_id=config.id,
            source_config_name=config.name,
            storage_location=None,
            current_version=db_table.current_version,
            share_mode=db_table.share_mode,
            status=db_table.status.value
        )

    except HTTPException:
        raise
    except Exception as e:
        reraise_db_timeout(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating table from Bronze Virtualized: {str(e)}"
        )


@router.post("/shares/{share_id}/schemas/{schema_id}/tables/from-silver-virtualized",
             response_model=IntegrationTableDetail,
             status_code=status.HTTP_201_CREATED)
async def create_table_from_silver_virtualized(
    share_id: int,
    schema_id: int,
    data: CreateTableFromSilverVirtualized,
    db: AsyncSession = Depends(get_db),
):
    """
    Create a Delta Sharing table from a Silver Virtualized Config.
    
    The table will serve data on-demand via Trino (no materialization).
    Recipients can access the data through the Data API endpoint:
    POST /api/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}/data
    """
    try:
        organization_id = 1  # Hardcoded for now

        # Verify share and schema exist
        schema_query = select(ShareSchema, Share).join(Share).where(
            and_(
                ShareSchema.id == schema_id,
                ShareSchema.share_id == share_id,
                Share.organization_id == organization_id
            )
        )
        schema_result = await db.execute(schema_query)
        row = schema_result.first()
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Schema or share not found"
            )
        db_schema, db_share = row

        # Get Silver Virtualized Config
        config_query = select(VirtualizedConfig).where(
            and_(
                VirtualizedConfig.id == data.silver_virtualized_config_id,
                VirtualizedConfig.is_active == True
            )
        )
        config_result = await db.execute(config_query)
        config = config_result.scalar_one_or_none()

        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Silver Virtualized Config {data.silver_virtualized_config_id} not found or inactive"
            )

        # Prevent sharing the same silver virtualized config twice in the same schema
        dup_check = await db.execute(
            select(ShareTable).where(
                and_(
                    ShareTable.schema_id == schema_id,
                    ShareTable.silver_virtualized_config_id == config.id,
                )
            )
        )
        if dup_check.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Silver Virtualized config '{config.name}' is already shared in this schema"
            )

        # Resolve name and description from the Silver Virtualized config when not provided
        base_name = data.name if data.name else _sanitize_table_name(config.name)
        table_name = await _unique_table_name(db, schema_id, base_name, "silver_virtualized")
        table_description = data.description if data.description is not None else getattr(config, 'description', None)

        # Create the virtualized share table
        db_table = ShareTable(
            schema_id=schema_id,
            name=table_name,
            description=table_description,
            dataset_id=None,
            source_type=ShareTableSourceType.SILVER_VIRTUALIZED,
            bronze_virtualized_config_id=None,
            silver_virtualized_config_id=config.id,
            storage_location=None,
            status=TableShareStatus.ACTIVE,
            share_mode='full',
            current_version=1,
            table_format="virtualized"
        )

        db.add(db_table)
        await db.commit()

        return IntegrationTableDetail(
            table_id=db_table.id,
            table_name=db_table.name,
            share_id=db_share.id,
            share_name=db_share.name,
            schema_id=db_schema.id,
            schema_name=db_schema.name,
            source_type=DatasetSourceType.SILVER,
            source_config_id=config.id,
            source_config_name=config.name,
            storage_location=None,
            current_version=db_table.current_version,
            share_mode=db_table.share_mode,
            status=db_table.status.value
        )

    except HTTPException:
        raise
    except Exception as e:
        reraise_db_timeout(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating table from Silver Virtualized: {str(e)}"
        )


# ==================== VERSION PINNING ====================

async def _get_table_for_org(
    share_id: int, schema_id: int, table_id: int, organization_id: int, db: AsyncSession
) -> ShareTable:
    """Shared helper: fetch a ShareTable verifying it belongs to the organization."""
    query = select(ShareTable).join(
        ShareSchema, ShareTable.schema_id == ShareSchema.id
    ).join(
        Share, ShareSchema.share_id == Share.id
    ).where(
        and_(
            ShareTable.id == table_id,
            ShareTable.schema_id == schema_id,
            ShareSchema.share_id == share_id,
            Share.organization_id == organization_id
        )
    )
    result = await db.execute(query)
    db_table = result.scalar_one_or_none()
    if not db_table:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")
    return db_table


@router.put(
    "/shares/{share_id}/schemas/{schema_id}/tables/{table_id}/pin-version",
    response_model=ShareTableDetail,
)
async def pin_delta_version(
    share_id: int,
    schema_id: int,
    table_id: int,
    body: PinDeltaVersionRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Pin a Bronze or Silver shared table to a specific Delta Lake version.

    Consumers will always receive data as it existed at that version (time-travel).
    Use DELETE on this endpoint to unpin and return to always-latest behaviour.
    """
    try:
        organization_id = 1  # Hardcoded for now
        db_table = await _get_table_for_org(share_id, schema_id, table_id, organization_id, db)

        if db_table.source_type not in (ShareTableSourceType.BRONZE, ShareTableSourceType.SILVER):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Version pinning is only supported for Bronze and Silver source tables"
            )

        # Validate requested version against the actual Delta log — execution records
        # are not a reliable source of truth (they may be NULL for older runs or skip
        # intermediate versions created by LLM extraction writes).
        await _assert_delta_version_exists(db_table.storage_location, body.delta_version)

        db_table.pinned_delta_version = body.delta_version
        await db.commit()

        # Build response using table_service helper
        schema_result = await db.execute(
            select(ShareSchema, Share).join(Share).where(ShareSchema.id == db_table.schema_id)
        )
        row = schema_result.first()
        db_schema, db_share = row

        from ...services.delta_sharing.table_service import TableService
        service = TableService(db)
        return await service._build_table_detail(db_table, db_schema, db_share, None)

    except HTTPException:
        raise
    except Exception as e:
        reraise_db_timeout(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error pinning delta version: {str(e)}"
        )


@router.delete(
    "/shares/{share_id}/schemas/{schema_id}/tables/{table_id}/pin-version",
    response_model=UnpinDeltaVersionResponse,
)
async def unpin_delta_version(
    share_id: int,
    schema_id: int,
    table_id: int,
    db: AsyncSession = Depends(get_db),
):
    """
    Unpin a shared table from its fixed Delta Lake version.

    After unpinning, consumers will always receive the latest version of the data.
    """
    try:
        organization_id = 1  # Hardcoded for now
        db_table = await _get_table_for_org(share_id, schema_id, table_id, organization_id, db)

        db_table.pinned_delta_version = None
        await db.commit()

        return UnpinDeltaVersionResponse(
            message="Table version unpinned. Consumers will now receive the latest data.",
            table_id=db_table.id,
            table_name=db_table.name
        )

    except HTTPException:
        raise
    except Exception as e:
        reraise_db_timeout(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error unpinning delta version: {str(e)}"
        )
