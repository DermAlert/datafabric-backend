from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, desc
from typing import List, Optional

from ...database.session import get_db
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
        
        # Get Bronze configs with successful executions
        if source_type is None or source_type == DatasetSourceType.BRONZE:
            bronze_query = select(BronzePersistentConfig).where(
                BronzePersistentConfig.last_execution_status == BronzeExecutionStatus.SUCCESS
            )
            bronze_result = await db.execute(bronze_query)
            bronze_configs = bronze_result.scalars().all()
            
            for config in bronze_configs:
                # Get latest successful execution for output_path
                exec_query = select(BronzeExecution).where(
                    and_(
                        BronzeExecution.config_id == config.id,
                        BronzeExecution.status == BronzeExecutionStatus.SUCCESS
                    )
                ).order_by(desc(BronzeExecution.finished_at)).limit(1)
                exec_result = await db.execute(exec_query)
                latest_exec = exec_result.scalar_one_or_none()
                
                output_path = None
                if latest_exec and latest_exec.output_paths:
                    output_path = latest_exec.output_paths[0] if latest_exec.output_paths else None
                
                datasets.append(AvailableDataset(
                    config_id=config.id,
                    name=config.name,
                    description=config.description,
                    source_type=DatasetSourceType.BRONZE,
                    output_path=output_path,
                    current_version=latest_exec.delta_version if latest_exec else None,
                    last_execution_status=config.last_execution_status,
                    last_execution_at=latest_exec.finished_at if latest_exec else None,
                    total_rows=latest_exec.rows_ingested if latest_exec else None
                ))
        
        # Get Silver configs with successful executions
        if source_type is None or source_type == DatasetSourceType.SILVER:
            silver_query = select(TransformConfig).where(
                TransformConfig.last_execution_status == TransformStatus.SUCCESS
            )
            silver_result = await db.execute(silver_query)
            silver_configs = silver_result.scalars().all()
            
            for config in silver_configs:
                # Get latest successful execution
                exec_query = select(TransformExecution).where(
                    and_(
                        TransformExecution.config_id == config.id,
                        TransformExecution.status == TransformStatus.SUCCESS
                    )
                ).order_by(desc(TransformExecution.finished_at)).limit(1)
                exec_result = await db.execute(exec_query)
                latest_exec = exec_result.scalar_one_or_none()
                
                datasets.append(AvailableDataset(
                    config_id=config.id,
                    name=config.name,
                    description=config.description,
                    source_type=DatasetSourceType.SILVER,
                    output_path=latest_exec.output_path if latest_exec else None,
                    current_version=latest_exec.delta_version if latest_exec else None,
                    last_execution_status=config.last_execution_status,
                    last_execution_at=latest_exec.finished_at if latest_exec else None,
                    total_rows=latest_exec.rows_output if latest_exec else None
                ))
        
        return datasets
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing available datasets: {str(e)}"
        )


@router.post("/shares/{share_id}/schemas/{schema_id}/tables/from-bronze", 
             response_model=IntegrationTableDetail, 
             status_code=status.HTTP_201_CREATED)
async def create_table_from_bronze(
    share_id: int,
    schema_id: int,
    data: CreateTableFromBronze,
    db: AsyncSession = Depends(get_db),
):
    """
    Create a Delta Sharing table from a Bronze Persistent Config.
    Automatically links to the Bronze Delta Lake storage location.
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
        
        # Get latest successful execution for storage path
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
        
        # Handle path_index for non-federated configs
        if data.path_index >= len(latest_exec.output_paths):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"path_index {data.path_index} is out of range. Available paths: {len(latest_exec.output_paths)}"
            )
        
        output_path = latest_exec.output_paths[data.path_index]
        
        # Convert s3a:// to internal path format
        storage_location = output_path
        if storage_location.startswith('s3a://'):
            storage_location = storage_location[6:]
        elif storage_location.startswith('s3://'):
            storage_location = storage_location[5:]
        
        # Remove trailing slash
        storage_location = storage_location.rstrip('/')
        
        # Check if table name already exists
        existing = await db.execute(
            select(ShareTable).where(
                and_(ShareTable.schema_id == schema_id, ShareTable.name == data.name)
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Table '{data.name}' already exists in this schema"
            )
        
        # Create or get legacy dataset entry for FK
        dataset_query = select(Dataset).where(Dataset.name == f"bronze_{bronze_config.id}_{bronze_config.name}")
        dataset_result = await db.execute(dataset_query)
        legacy_dataset = dataset_result.scalar_one_or_none()
        
        if not legacy_dataset:
            legacy_dataset = Dataset(
                name=f"bronze_{bronze_config.id}_{bronze_config.name}",
                description=f"Auto-created for Bronze Config {bronze_config.id}",
                storage_type="delta",
                refresh_type="manual",
                status="active"
            )
            db.add(legacy_dataset)
            await db.flush()
        
        # Create the table
        db_table = ShareTable(
            schema_id=schema_id,
            name=data.name,
            description=data.description or f"Bronze data from {bronze_config.name}",
            dataset_id=legacy_dataset.id,
            source_type=ShareTableSourceType.BRONZE,
            storage_location=storage_location,
            status=TableShareStatus.ACTIVE,
            share_mode=data.share_mode,
            filter_condition=data.filter_condition,
            current_version=latest_exec.delta_version or 1,
            table_format="delta"
        )
        
        db.add(db_table)
        await db.commit()
        await db.refresh(db_table)
        
        return IntegrationTableDetail(
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
        )
        
    except HTTPException:
        raise
    except Exception as e:
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
        
        # Check if table name already exists
        existing = await db.execute(
            select(ShareTable).where(
                and_(ShareTable.schema_id == schema_id, ShareTable.name == data.name)
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Table '{data.name}' already exists in this schema"
            )
        
        # Create or get legacy dataset entry for FK
        dataset_query = select(Dataset).where(Dataset.name == f"silver_{silver_config.id}_{silver_config.name}")
        dataset_result = await db.execute(dataset_query)
        legacy_dataset = dataset_result.scalar_one_or_none()
        
        if not legacy_dataset:
            legacy_dataset = Dataset(
                name=f"silver_{silver_config.id}_{silver_config.name}",
                description=f"Auto-created for Silver Config {silver_config.id}",
                storage_type="delta",
                refresh_type="manual",
                status="active"
            )
            db.add(legacy_dataset)
            await db.flush()
        
        # Create the table
        db_table = ShareTable(
            schema_id=schema_id,
            name=data.name,
            description=data.description or f"Silver data from {silver_config.name}",
            dataset_id=legacy_dataset.id,
            source_type=ShareTableSourceType.SILVER,
            storage_location=storage_location,
            status=TableShareStatus.ACTIVE,
            share_mode=data.share_mode,
            filter_condition=data.filter_condition,
            current_version=latest_exec.delta_version or 1,
            table_format="delta"
        )
        
        db.add(db_table)
        await db.commit()
        await db.refresh(db_table)
        
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

        # Check if table name already exists in this schema
        existing = await db.execute(
            select(ShareTable).where(
                and_(ShareTable.schema_id == schema_id, ShareTable.name == data.name)
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Table '{data.name}' already exists in this schema"
            )

        # Create the virtualized share table (no storage_location, no dataset_id needed)
        db_table = ShareTable(
            schema_id=schema_id,
            name=data.name,
            description=data.description or f"Virtualized Bronze data from {config.name}",
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
        await db.refresh(db_table)

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

        # Check if table name already exists in this schema
        existing = await db.execute(
            select(ShareTable).where(
                and_(ShareTable.schema_id == schema_id, ShareTable.name == data.name)
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Table '{data.name}' already exists in this schema"
            )

        # Create the virtualized share table
        db_table = ShareTable(
            schema_id=schema_id,
            name=data.name,
            description=data.description or f"Virtualized Silver data from {config.name}",
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
        await db.refresh(db_table)

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
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating table from Silver Virtualized: {str(e)}"
        )
