"""
Bronze Layer API Routes

These routes implement the Smart Bronze architecture for data ingestion.
The Bronze layer focuses on raw data ingestion without transformations.

**Important:** Relationships are NOT defined here. They must be defined
beforehand in the metadata layer using the `/relationships` endpoints.

Key endpoints:
- POST /bronze/preview: Preview the ingestion plan
- POST /bronze/ingest: Execute the Bronze ingestion
- POST /bronze/relationships-preview: See which relationships will be used
- GET /bronze/{dataset_id}: Get Bronze configuration for a dataset
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from typing import List, Optional
from datetime import datetime
import logging

from ...database.database import get_db
from ...database.core.core import Dataset, DataConnection
from ...database.datasets.bronze import (
    DatasetBronzeConfig,
    DatasetIngestionGroup,
    IngestionGroupTable,
    InterSourceLink,
    IngestionStatus,
)
from ...crud.token import get_current_user
from ..schemas.bronze_schemas import (
    DatasetBronzeCreateRequest,
    BronzeIngestionPreview,
    BronzeIngestionResult,
    DatasetBronzeConfigResponse,
    IngestionGroupResponse,
    IngestionStatusEnum,
    RelationshipUsagePreview,
    BronzeVirtualizedRequest,
    BronzeVirtualizedResponse,
)
from ...services.bronze_ingestion_service import BronzeIngestionService

router = APIRouter()
logger = logging.getLogger(__name__)


# ==================== BRONZE INGESTION ====================

@router.post("/preview", response_model=BronzeIngestionPreview)
async def preview_bronze_ingestion(
    dataset_data: DatasetBronzeCreateRequest,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Preview the Bronze ingestion plan before execution.
    
    **How it works:**
    1. You specify the tables and columns you want
    2. The system finds all relationships between those tables automatically
    3. Shows the SQL that will be generated for Trino
    
    **Relationships are discovered automatically:**
    - The system looks up existing relationships in metadata
    - `relationship_ids` is optional — if not provided, uses all applicable relationships
    
    **What you'll see:**
    - How tables are grouped by connection
    - The SQL for each group (with JOINs if applicable)
    - Inter-DB links that will be saved for Silver
    - Output paths in MinIO
    """
    try:
        service = BronzeIngestionService(db)
        return await service.generate_ingestion_plan_simplified(dataset_data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to generate Bronze ingestion preview: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate ingestion preview: {str(e)}"
        )


@router.post("/ingest", response_model=BronzeIngestionResult, status_code=status.HTTP_201_CREATED)
async def execute_bronze_ingestion(
    dataset_data: DatasetBronzeCreateRequest,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Execute the Bronze layer ingestion.
    
    **Workflow:**
    1. Call this endpoint with tables/columns
    2. System discovers relationships automatically and uses Trino to extract data
    
    **Note:** `relationship_ids` is optional — if not provided, uses all applicable relationships.
    
    **What happens:**
    1. System looks up relationships between your selected tables automatically
    2. Groups tables by connection (for optimization)
    3. For same-connection tables with relationships → Trino does pushdown JOIN
    4. For different-connection tables → Extracts separately, stores link for Silver
    5. Saves raw data to MinIO (Parquet/Delta format)
    
    **Example:**
    ```json
    {
      "name": "clinical_dataset",
      "tables": [
        {"table_id": 1, "select_all": true},
        {"table_id": 2, "column_ids": [10, 11, 12]},
        {"table_id": 3, "select_all": true}
      ],
      "output_format": "parquet"
    }
    ```
    
    If tables 1 and 2 are in the same DB with a relationship defined,
    Trino will JOIN them. Table 3 (different DB) will be extracted separately.
    """
    try:
        service = BronzeIngestionService(db)
        return await service.execute_ingestion_simplified(dataset_data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to execute Bronze ingestion: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute Bronze ingestion: {str(e)}"
        )


@router.post("/virtualized", response_model=BronzeVirtualizedResponse)
async def execute_virtualized_query(
    request: BronzeVirtualizedRequest,
    limit: int = Query(1000, ge=1, le=10000, description="Maximum rows per group"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Execute a virtualized Bronze query WITHOUT saving to Delta.
    
    This endpoint generates the same SQL as `/ingest` but instead of 
    materializing the data, it executes the query and returns the results
    directly as JSON.
    
    **Note:** `relationship_ids` is optional — if not provided, uses all applicable relationships automatically.
    
    **Use cases:**
    - Data exploration/preview with actual data
    - Light API consumption (small to medium datasets)
    - Testing queries before materializing
    - Quick data access without storage overhead
    
    **Important:**
    - Data is NOT saved anywhere (purely virtual)
    - Use `limit` to control response size (default 1000, max 10000)
    - For large datasets, use `/ingest` to materialize
    
    **Example:**
    ```json
    {
      "tables": [
        {"table_id": 1, "select_all": true},
        {"table_id": 2, "column_ids": [10, 11, 12]}
      ]
    }
    ```
    
    **Response includes:**
    - Data from each group as JSON
    - Column names
    - SQL that was executed
    - Execution time
    """
    try:
        service = BronzeIngestionService(db)
        return await service.execute_virtualized(request, limit=limit, offset=offset)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to execute virtualized query: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute virtualized query: {str(e)}"
        )


@router.post("/relationships-preview", response_model=List[RelationshipUsagePreview])
async def preview_relationship_usage(
    table_ids: List[int],
    relationship_ids: Optional[List[int]] = None,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Preview which relationships will be used for the selected tables.
    
    Call this BEFORE creating a dataset to understand:
    - Which relationships exist between your selected tables
    - Which will be used for Trino pushdown JOINs (same DB)
    - Which will be stored for Silver layer (different DBs)
    
    **Parameters:**
    - `table_ids`: List of table IDs you plan to include
    - `relationship_ids`: Optional - limit to specific relationships
    
    **Example Response:**
    ```json
    [
      {
        "relationship_id": 1,
        "left_table": "users",
        "left_column": "id",
        "right_table": "orders",
        "right_column": "user_id",
        "scope": "intra_connection",
        "usage": "pushdown_join",
        "join_type": "inner"
      },
      {
        "relationship_id": 2,
        "left_table": "users",
        "left_column": "cpf",
        "right_table": "external_patients",
        "right_column": "cpf_paciente",
        "scope": "inter_connection",
        "usage": "silver_layer_link",
        "join_type": "left"
      }
    ]
    ```
    """
    try:
        service = BronzeIngestionService(db)
        return await service.preview_relationship_usage(table_ids, relationship_ids)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to preview relationship usage: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to preview relationship usage: {str(e)}"
        )


# ==================== BRONZE CONFIGURATION ====================

@router.get("/{dataset_id}", response_model=DatasetBronzeConfigResponse)
async def get_bronze_config(
    dataset_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Get the Bronze layer configuration for a dataset.
    
    Returns:
    - Storage location in MinIO
    - Output format (Parquet/Delta)
    - Partition columns
    - Last ingestion status and time
    - Number of ingestion groups
    """
    try:
        query = select(DatasetBronzeConfig).where(
            DatasetBronzeConfig.dataset_id == dataset_id
        )
        result = await db.execute(query)
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Bronze configuration not found for dataset {dataset_id}"
            )
        
        # Count ingestion groups
        groups_query = select(DatasetIngestionGroup).where(
            DatasetIngestionGroup.bronze_config_id == config.id
        )
        groups_result = await db.execute(groups_query)
        groups = groups_result.scalars().all()
        
        return DatasetBronzeConfigResponse(
            id=config.id,
            dataset_id=config.dataset_id,
            name=config.name,
            description=config.description,
            bronze_bucket=config.bronze_bucket,
            bronze_path_prefix=config.bronze_path_prefix,
            output_format=config.output_format,
            partition_columns=config.partition_columns,
            last_ingestion_time=config.last_ingestion_time,
            last_ingestion_status=config.last_ingestion_status.value if config.last_ingestion_status else None,
            ingestion_groups_count=len(groups)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get Bronze config: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get Bronze configuration: {str(e)}"
        )


@router.get("/{dataset_id}/groups", response_model=List[IngestionGroupResponse])
async def get_ingestion_groups(
    dataset_id: int,
    include_sql: bool = False,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Get all ingestion groups for a dataset's Bronze layer.
    
    Each group represents data from one source connection.
    If tables in the same connection had relationships, they were JOINed.
    
    **Parameters:**
    - `include_sql`: If True, includes the SQL that was executed
    """
    try:
        # First get the bronze config
        config_query = select(DatasetBronzeConfig).where(
            DatasetBronzeConfig.dataset_id == dataset_id
        )
        config_result = await db.execute(config_query)
        config = config_result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Bronze configuration not found for dataset {dataset_id}"
            )
        
        # Get ingestion groups with connection info
        groups_query = select(
            DatasetIngestionGroup,
            DataConnection.name.label('connection_name')
        ).join(
            DataConnection, DatasetIngestionGroup.connection_id == DataConnection.id
        ).where(
            DatasetIngestionGroup.bronze_config_id == config.id
        ).order_by(
            DatasetIngestionGroup.group_order
        )
        
        groups_result = await db.execute(groups_query)
        groups = groups_result.fetchall()
        
        response = []
        for group_row in groups:
            group = group_row[0]
            conn_name = group_row[1]
            
            # Count tables in this group
            tables_query = select(IngestionGroupTable).where(
                IngestionGroupTable.ingestion_group_id == group.id
            )
            tables_result = await db.execute(tables_query)
            tables_count = len(tables_result.scalars().all())
            
            response.append(IngestionGroupResponse(
                id=group.id,
                group_name=group.group_name,
                connection_id=group.connection_id,
                connection_name=conn_name,
                output_path=group.output_path,
                status=group.status.value if group.status else 'pending',
                last_execution_time=group.last_execution_time,
                rows_ingested=group.rows_ingested,
                tables_count=tables_count,
                generated_sql=group.generated_sql if include_sql else None
            ))
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get ingestion groups: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get ingestion groups: {str(e)}"
        )


@router.get("/{dataset_id}/links")
async def get_inter_source_links(
    dataset_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Get the inter-source links for a dataset.
    
    These are relationships between tables from DIFFERENT connections.
    They were NOT joined in Bronze (because they're in different DBs).
    They will be joined in the Silver layer using Spark.
    
    **Returns:**
    - Which columns link different parts of the Bronze data
    - The join strategy to use in Silver
    """
    try:
        # First get the bronze config
        config_query = select(DatasetBronzeConfig).where(
            DatasetBronzeConfig.dataset_id == dataset_id
        )
        config_result = await db.execute(config_query)
        config = config_result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Bronze configuration not found for dataset {dataset_id}"
            )
        
        # Get inter-source links
        links_query = select(
            InterSourceLink,
            DatasetIngestionGroup.group_name.label('left_group_name'),
        ).join(
            DatasetIngestionGroup, InterSourceLink.left_group_id == DatasetIngestionGroup.id
        ).where(
            InterSourceLink.bronze_config_id == config.id
        )
        
        links_result = await db.execute(links_query)
        links = links_result.fetchall()
        
        response = []
        for link_row in links:
            link = link_row[0]
            
            # Get right group name
            right_group_query = select(DatasetIngestionGroup.group_name).where(
                DatasetIngestionGroup.id == link.right_group_id
            )
            right_result = await db.execute(right_group_query)
            right_group_name = right_result.scalar_one_or_none()
            
            response.append({
                'id': link.id,
                'left_group_id': link.left_group_id,
                'left_group_name': link_row[1],
                'left_column_name': link.left_column_name,
                'right_group_id': link.right_group_id,
                'right_group_name': right_group_name,
                'right_column_name': link.right_column_name,
                'join_strategy': link.join_strategy.value if link.join_strategy else 'inner',
                'description': link.description,
                'note': 'This join will be performed in Silver layer using Spark'
            })
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get inter-source links: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get inter-source links: {str(e)}"
        )


# ==================== RE-INGESTION ====================

@router.post("/{dataset_id}/reingest", response_model=BronzeIngestionResult)
async def reingest_bronze_layer(
    dataset_id: int,
    group_ids: Optional[List[int]] = None,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Re-execute the Bronze ingestion for an existing dataset.
    
    Use this when:
    - Source data has been updated
    - Previous ingestion partially failed
    - You need to refresh on a schedule
    
    **Parameters:**
    - `group_ids`: Optional - reingest only specific groups.
                   If not provided, all groups will be reingested.
    """
    try:
        # Get the bronze config
        config_query = select(DatasetBronzeConfig).where(
            DatasetBronzeConfig.dataset_id == dataset_id
        )
        config_result = await db.execute(config_query)
        config = config_result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Bronze configuration not found for dataset {dataset_id}"
            )
        
        # Reconstruct request from config_snapshot
        from ..schemas.bronze_schemas import DatasetBronzeCreateRequest
        
        dataset_data = DatasetBronzeCreateRequest(**config.config_snapshot)
        
        # Execute ingestion
        service = BronzeIngestionService(db)
        preview = await service.generate_ingestion_plan_simplified(dataset_data)
        
        # Filter groups if specific ones requested
        if group_ids:
            groups_query = select(DatasetIngestionGroup.group_name).where(
                DatasetIngestionGroup.id.in_(group_ids)
            )
            groups_result = await db.execute(groups_query)
            group_names = {row[0] for row in groups_result.fetchall()}
            
            preview.ingestion_groups = [
                g for g in preview.ingestion_groups 
                if g.group_name in group_names
            ]
        
        result = await service.execute_ingestion_simplified(dataset_data)
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to reingest Bronze layer: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to reingest Bronze layer: {str(e)}"
        )
