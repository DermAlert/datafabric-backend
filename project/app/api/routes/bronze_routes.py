"""
Bronze Layer API Routes

These routes implement the Smart Bronze architecture for data ingestion.
The Bronze layer focuses on raw data ingestion without transformations.

**Important:** Relationships are NOT defined here. They must be defined
beforehand in the metadata layer using the `/relationships` endpoints.

## Workflow:

1. **Create a config** → POST /configs/persistent or /configs/virtualized
2. **Preview** → POST /configs/persistent/{id}/preview
3. **Execute** → POST /configs/persistent/{id}/execute
4. **View history** → GET /configs/persistent/{id}/executions

Key endpoints:
- POST /bronze/configs/virtualized: Create virtualized config (query without saving)
- POST /bronze/configs/persistent: Create persistent config (save to Delta Lake)
- POST /bronze/configs/persistent/{id}/execute: Execute the ingestion
- GET /bronze/configs/persistent/{id}/executions: View execution history
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from typing import List, Optional
from datetime import datetime
import logging

from ...database.session import get_db
from ...database.models.core import Dataset, DataConnection
from ...database.models.bronze import (
    DatasetBronzeConfig,
    DatasetIngestionGroup,
    IngestionGroupTable,
    InterSourceLink,
    IngestionStatus,
    BronzeVirtualizedConfig,
    BronzePersistentConfig,
    BronzeExecution,
    BronzeExecutionStatus,
)
from ...core.auth import get_current_user
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
    TableColumnSelection,
    OutputFormatEnum,
    WriteModeEnum,
    # Config schemas
    BronzeVirtualizedConfigCreate,
    BronzeVirtualizedConfigUpdate,
    BronzeVirtualizedConfigResponse,
    BronzeVirtualizedQueryResponse,
    BronzePersistentConfigCreate,
    BronzePersistentConfigUpdate,
    BronzePersistentConfigResponse,
    BronzePersistentPreviewResponse,
    BronzePersistentExecuteResponse,
    BronzeExecutionResponse,
    # Versioning schemas
    BronzeVersionHistoryResponse,
    BronzeDataQueryResponse,
)
from ...services.bronze.ingestion_service import BronzeIngestionService
from ...services.bronze.versioning_service import BronzeVersioningService

router = APIRouter()
logger = logging.getLogger(__name__)


# ==================== UTILITIES ====================

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


# ==============================================================================
# BRONZE VIRTUALIZED CONFIGS
# ==============================================================================
# These endpoints manage saved virtualized query configurations.
# Virtualized = query original sources via Trino, no persistence.

@router.get(
    "/configs/virtualized",
    response_model=List[BronzeVirtualizedConfigResponse],
    summary="List virtualized configs",
    description="List all virtualized Bronze configs. These configs query original data sources via Trino."
)
async def list_virtualized_configs(
    include_inactive: bool = Query(False, description="Include inactive configs"),
    db: AsyncSession = Depends(get_db),
):
    """List all virtualized configs."""
    try:
        query = select(BronzeVirtualizedConfig)
        if not include_inactive:
            query = query.where(BronzeVirtualizedConfig.is_active == True)
        query = query.order_by(BronzeVirtualizedConfig.name)
        
        result = await db.execute(query)
        configs = result.scalars().all()
        
        return [
            BronzeVirtualizedConfigResponse(
                id=c.id,
                name=c.name,
                description=c.description,
                tables=c.tables,
                relationship_ids=c.relationship_ids,
                enable_federated_joins=c.enable_federated_joins,
                generated_sql=c.generated_sql,
                is_active=c.is_active,
                last_query_time=c.last_query_time,
                last_query_rows=c.last_query_rows,
                data_criacao=c.data_criacao,
                data_atualizacao=c.data_atualizacao,
            )
            for c in configs
        ]
    except Exception as e:
        logger.error(f"Failed to list virtualized configs: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list virtualized configs: {str(e)}"
        )


@router.post(
    "/configs/virtualized",
    response_model=BronzeVirtualizedConfigResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create virtualized config",
    description="""
Create a virtualized config for querying original data sources via Trino.
Data is NOT saved - returned as JSON (use for exploration, APIs, etc.).

---

## **Request Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | ✅ | Unique name for the config |
| `description` | string | ❌ | Optional description |
| `tables` | object[] | ✅ | Tables and columns to include |
| `relationship_ids` | int[] | ❌ | Relationships for JOINs. `null`=auto-discover |
| `enable_federated_joins` | bool | ❌ | Enable cross-database JOINs (default: false) |

---

## **tables Structure:**

```json
"tables": [
  {"table_id": 1, "select_all": true},
  {"table_id": 2, "column_ids": [10, 11, 12]}
]
```

---

## **Example 1: Minimal**
```json
{
  "name": "users_exploration",
  "tables": [{"table_id": 1, "select_all": true}]
}
```

## **Example 2: Multiple tables**
```json
{
  "name": "users_orders_view",
  "tables": [
    {"table_id": 1, "select_all": true},
    {"table_id": 2, "column_ids": [10, 11, 12]}
  ]
}
```
"""
)
async def create_virtualized_config(
    config_data: BronzeVirtualizedConfigCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a new virtualized config."""
    try:
        # Check if name already exists
        existing = await db.execute(
            select(BronzeVirtualizedConfig).where(
                BronzeVirtualizedConfig.name == config_data.name
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Config with name '{config_data.name}' already exists"
            )
        
        # Create config
        config = BronzeVirtualizedConfig(
            name=config_data.name,
            description=config_data.description,
            tables=[t.model_dump() for t in config_data.tables],
            relationship_ids=config_data.relationship_ids,
            enable_federated_joins=config_data.enable_federated_joins,
            is_active=True,
        )
        
        db.add(config)
        await db.commit()
        await db.refresh(config)
        
        return BronzeVirtualizedConfigResponse(
            id=config.id,
            name=config.name,
            description=config.description,
            tables=config.tables,
            relationship_ids=config.relationship_ids,
            enable_federated_joins=config.enable_federated_joins,
            generated_sql=config.generated_sql,
            is_active=config.is_active,
            last_query_time=config.last_query_time,
            last_query_rows=config.last_query_rows,
            data_criacao=config.data_criacao,
            data_atualizacao=config.data_atualizacao,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to create virtualized config: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create virtualized config: {str(e)}"
        )


@router.get(
    "/configs/virtualized/{config_id}",
    response_model=BronzeVirtualizedConfigResponse,
    summary="Get virtualized config",
    description="Get a specific virtualized config by ID."
)
async def get_virtualized_config(
    config_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get a virtualized config by ID."""
    try:
        result = await db.execute(
            select(BronzeVirtualizedConfig).where(
                BronzeVirtualizedConfig.id == config_id
            )
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Virtualized config {config_id} not found"
            )
        
        return BronzeVirtualizedConfigResponse(
            id=config.id,
            name=config.name,
            description=config.description,
            tables=config.tables,
            relationship_ids=config.relationship_ids,
            enable_federated_joins=config.enable_federated_joins,
            generated_sql=config.generated_sql,
            is_active=config.is_active,
            last_query_time=config.last_query_time,
            last_query_rows=config.last_query_rows,
            data_criacao=config.data_criacao,
            data_atualizacao=config.data_atualizacao,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get virtualized config: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get virtualized config: {str(e)}"
        )


@router.put(
    "/configs/virtualized/{config_id}",
    response_model=BronzeVirtualizedConfigResponse,
    summary="Update virtualized config",
    description="Update an existing virtualized config."
)
async def update_virtualized_config(
    config_id: int,
    config_data: BronzeVirtualizedConfigUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update a virtualized config."""
    try:
        result = await db.execute(
            select(BronzeVirtualizedConfig).where(
                BronzeVirtualizedConfig.id == config_id
            )
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Virtualized config {config_id} not found"
            )
        
        # Update fields
        update_data = config_data.model_dump(exclude_unset=True)
        
        if 'tables' in update_data and update_data['tables']:
            update_data['tables'] = [t.model_dump() if hasattr(t, 'model_dump') else t for t in update_data['tables']]
        
        for field, value in update_data.items():
            setattr(config, field, value)
        
        await db.commit()
        await db.refresh(config)
        
        return BronzeVirtualizedConfigResponse(
            id=config.id,
            name=config.name,
            description=config.description,
            tables=config.tables,
            relationship_ids=config.relationship_ids,
            enable_federated_joins=config.enable_federated_joins,
            generated_sql=config.generated_sql,
            is_active=config.is_active,
            last_query_time=config.last_query_time,
            last_query_rows=config.last_query_rows,
            data_criacao=config.data_criacao,
            data_atualizacao=config.data_atualizacao,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to update virtualized config: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update virtualized config: {str(e)}"
        )


@router.delete(
    "/configs/virtualized/{config_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete virtualized config",
    description="Delete a virtualized config."
)
async def delete_virtualized_config(
    config_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Delete a virtualized config."""
    try:
        result = await db.execute(
            select(BronzeVirtualizedConfig).where(
                BronzeVirtualizedConfig.id == config_id
            )
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Virtualized config {config_id} not found"
            )
        
        await db.delete(config)
        await db.commit()
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to delete virtualized config: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete virtualized config: {str(e)}"
        )


@router.post(
    "/configs/virtualized/{config_id}/query",
    response_model=BronzeVirtualizedQueryResponse,
    summary="Execute virtualized query",
    description="""
Execute a virtualized query using a saved config and return data as JSON.

**Note:** This does NOT save data. For large datasets, use pagination (limit/offset).
"""
)
async def query_virtualized_config(
    config_id: int,
    limit: int = Query(1000, ge=1, le=10000, description="Maximum rows to return"),
    offset: int = Query(0, ge=0, description="Rows to skip"),
    db: AsyncSession = Depends(get_db),
):
    """Execute a virtualized config query."""
    try:
        # Get config
        result = await db.execute(
            select(BronzeVirtualizedConfig).where(
                BronzeVirtualizedConfig.id == config_id
            )
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Virtualized config {config_id} not found"
            )
        
        # Convert config to request format and execute
        tables = [TableColumnSelection(**t) for t in config.tables]
        request = BronzeVirtualizedRequest(
            tables=tables,
            relationship_ids=config.relationship_ids,
            enable_federated_joins=config.enable_federated_joins,
        )
        
        service = BronzeIngestionService(db)
        response = await service.execute_virtualized(request, limit=limit, offset=offset)
        
        # Update last query stats
        config.last_query_time = datetime.utcnow()
        config.last_query_rows = response.total_rows
        await db.commit()
        
        return BronzeVirtualizedQueryResponse(
            config_id=config.id,
            config_name=config.name,
            total_tables=response.total_tables,
            total_columns=response.total_columns,
            groups=response.groups,
            total_rows=response.total_rows,
            execution_time_seconds=response.execution_time_seconds,
            warnings=response.warnings,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to execute virtualized query: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute virtualized query: {str(e)}"
        )


# ==============================================================================
# BRONZE PERSISTENT CONFIGS
# ==============================================================================
# These endpoints manage saved persistent (materialization) configurations.
# Persistent = extract from sources and save to Delta Lake.

@router.get(
    "/configs/persistent",
    response_model=List[BronzePersistentConfigResponse],
    summary="List persistent configs",
    description="List all persistent Bronze configs. These configs materialize data to Delta Lake."
)
async def list_persistent_configs(
    include_inactive: bool = Query(False, description="Include inactive configs"),
    db: AsyncSession = Depends(get_db),
):
    """List all persistent configs."""
    try:
        query = select(BronzePersistentConfig)
        if not include_inactive:
            query = query.where(BronzePersistentConfig.is_active == True)
        query = query.order_by(BronzePersistentConfig.name)
        
        result = await db.execute(query)
        configs = result.scalars().all()
        
        return [
            BronzePersistentConfigResponse(
                id=c.id,
                name=c.name,
                description=c.description,
                tables=c.tables,
                relationship_ids=c.relationship_ids,
                enable_federated_joins=c.enable_federated_joins,
                output_format=c.output_format,
                output_bucket=c.output_bucket,
                output_path_prefix=c.output_path_prefix,
                partition_columns=c.partition_columns,
                properties=c.properties,
                is_active=c.is_active,
                last_execution_time=c.last_execution_time,
                last_execution_status=c.last_execution_status.value if c.last_execution_status else None,
                last_execution_rows=c.last_execution_rows,
                dataset_id=c.dataset_id,
                data_criacao=c.data_criacao,
                data_atualizacao=c.data_atualizacao,
            )
            for c in configs
        ]
    except Exception as e:
        logger.error(f"Failed to list persistent configs: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list persistent configs: {str(e)}"
        )


@router.post(
    "/configs/persistent",
    response_model=BronzePersistentConfigResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create persistent config",
    description="""
Create a persistent config for materializing raw data to Bronze Delta Lake.

---

## **Request Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | ✅ | Unique name for the config |
| `description` | string | ❌ | Optional description |
| `tables` | object[] | ✅ | Tables and columns to include |
| `relationship_ids` | int[] | ❌ | Relationships for JOINs. `null`=auto-discover |
| `enable_federated_joins` | bool | ❌ | Enable cross-database JOINs (default: false) |
| `output_format` | string | ❌ | "parquet" or "delta" (default: "parquet") |
| `output_bucket` | string | ❌ | MinIO bucket (defaults to system bronze bucket) |
| `output_path_prefix` | string | ❌ | Custom path prefix |
| `partition_columns` | string[] | ❌ | Columns to partition by |

---

## **Example 1: Minimal**
```json
{
  "name": "users_bronze",
  "tables": [{"table_id": 1, "select_all": true}]
}
```

## **Example 2: With output configuration**
```json
{
  "name": "clinical_bronze",
  "tables": [
    {"table_id": 1, "select_all": true},
    {"table_id": 2, "column_ids": [10, 11, 12]}
  ],
  "output_format": "delta",
  "partition_columns": ["year", "month"]
}
```

---

## **Workflow:**
1. **Create config** → POST /api/bronze/configs/persistent
2. **Preview** → POST /api/bronze/configs/persistent/{id}/preview
3. **Execute** → POST /api/bronze/configs/persistent/{id}/execute
"""
)
async def create_persistent_config(
    config_data: BronzePersistentConfigCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create a new persistent config."""
    try:
        # Check if name already exists
        existing = await db.execute(
            select(BronzePersistentConfig).where(
                BronzePersistentConfig.name == config_data.name
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Config with name '{config_data.name}' already exists"
            )
        
        # Create config (always uses overwrite mode)
        tables_dict = [t.model_dump() for t in config_data.tables]
        
        from ...database.models.bronze import WriteMode
        
        config = BronzePersistentConfig(
            name=config_data.name,
            description=config_data.description,
            tables=tables_dict,
            relationship_ids=config_data.relationship_ids,
            enable_federated_joins=config_data.enable_federated_joins,
            output_format=config_data.output_format.value,
            # Always use overwrite for simplicity
            write_mode=WriteMode.OVERWRITE,
            merge_keys=None,
            merge_keys_source=None,
            # Other fields
            output_bucket=config_data.output_bucket,
            output_path_prefix=config_data.output_path_prefix,
            partition_columns=config_data.partition_columns,
            properties=config_data.properties,
            config_snapshot=config_data.model_dump(),
            is_active=True,
        )
        
        db.add(config)
        await db.commit()
        await db.refresh(config)
        
        return BronzePersistentConfigResponse(
            id=config.id,
            name=config.name,
            description=config.description,
            tables=config.tables,
            relationship_ids=config.relationship_ids,
            enable_federated_joins=config.enable_federated_joins,
            output_format=config.output_format,
            # Versioning fields
            write_mode=config.write_mode.value if config.write_mode else 'overwrite',
            merge_keys=config.merge_keys,
            merge_keys_source=config.merge_keys_source,
            current_delta_version=config.current_delta_version,
            # Other fields
            output_bucket=config.output_bucket,
            output_path_prefix=config.output_path_prefix,
            partition_columns=config.partition_columns,
            properties=config.properties,
            is_active=config.is_active,
            last_execution_time=config.last_execution_time,
            last_execution_status=config.last_execution_status.value if config.last_execution_status else None,
            last_execution_rows=config.last_execution_rows,
            dataset_id=config.dataset_id,
            data_criacao=config.data_criacao,
            data_atualizacao=config.data_atualizacao,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to create persistent config: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create persistent config: {str(e)}"
        )


@router.get(
    "/configs/persistent/{config_id}",
    response_model=BronzePersistentConfigResponse,
    summary="Get persistent config",
    description="Get a specific persistent config by ID."
)
async def get_persistent_config(
    config_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get a persistent config by ID."""
    try:
        result = await db.execute(
            select(BronzePersistentConfig).where(
                BronzePersistentConfig.id == config_id
            )
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Persistent config {config_id} not found"
            )
        
        return BronzePersistentConfigResponse(
            id=config.id,
            name=config.name,
            description=config.description,
            tables=config.tables,
            relationship_ids=config.relationship_ids,
            enable_federated_joins=config.enable_federated_joins,
            output_format=config.output_format,
            # Versioning fields
            write_mode=config.write_mode.value if config.write_mode else 'overwrite',
            merge_keys=config.merge_keys,
            merge_keys_source=config.merge_keys_source,
            current_delta_version=config.current_delta_version,
            # Other fields
            output_bucket=config.output_bucket,
            output_path_prefix=config.output_path_prefix,
            partition_columns=config.partition_columns,
            properties=config.properties,
            is_active=config.is_active,
            last_execution_time=config.last_execution_time,
            last_execution_status=config.last_execution_status.value if config.last_execution_status else None,
            last_execution_rows=config.last_execution_rows,
            dataset_id=config.dataset_id,
            data_criacao=config.data_criacao,
            data_atualizacao=config.data_atualizacao,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get persistent config: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get persistent config: {str(e)}"
        )


@router.put(
    "/configs/persistent/{config_id}",
    response_model=BronzePersistentConfigResponse,
    summary="Update persistent config",
    description="Update an existing persistent config."
)
async def update_persistent_config(
    config_id: int,
    config_data: BronzePersistentConfigUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update a persistent config."""
    try:
        result = await db.execute(
            select(BronzePersistentConfig).where(
                BronzePersistentConfig.id == config_id
            )
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Persistent config {config_id} not found"
            )
        
        # Update fields
        update_data = config_data.model_dump(exclude_unset=True)
        
        if 'tables' in update_data and update_data['tables']:
            update_data['tables'] = [t.model_dump() if hasattr(t, 'model_dump') else t for t in update_data['tables']]
        
        if 'output_format' in update_data and update_data['output_format']:
            update_data['output_format'] = update_data['output_format'].value
        
        # Always use overwrite mode (ignore write_mode/merge_keys from request)
        from ...database.models.bronze import WriteMode
        update_data['write_mode'] = WriteMode.OVERWRITE
        update_data['merge_keys'] = None
        update_data['merge_keys_source'] = None
        
        for field, value in update_data.items():
            setattr(config, field, value)
        
        # Update snapshot
        config.config_snapshot = {
            'name': config.name,
            'description': config.description,
            'tables': config.tables,
            'relationship_ids': config.relationship_ids,
            'enable_federated_joins': config.enable_federated_joins,
            'output_format': config.output_format,
            'write_mode': 'overwrite',
            'merge_keys': None,
            'output_bucket': config.output_bucket,
            'output_path_prefix': config.output_path_prefix,
            'partition_columns': config.partition_columns,
            'properties': config.properties,
        }
        
        await db.commit()
        await db.refresh(config)
        
        return BronzePersistentConfigResponse(
            id=config.id,
            name=config.name,
            description=config.description,
            tables=config.tables,
            relationship_ids=config.relationship_ids,
            enable_federated_joins=config.enable_federated_joins,
            output_format=config.output_format,
            # Versioning fields
            write_mode=config.write_mode.value if config.write_mode else 'overwrite',
            merge_keys=config.merge_keys,
            merge_keys_source=config.merge_keys_source,
            current_delta_version=config.current_delta_version,
            # Other fields
            output_bucket=config.output_bucket,
            output_path_prefix=config.output_path_prefix,
            partition_columns=config.partition_columns,
            properties=config.properties,
            is_active=config.is_active,
            last_execution_time=config.last_execution_time,
            last_execution_status=config.last_execution_status.value if config.last_execution_status else None,
            last_execution_rows=config.last_execution_rows,
            dataset_id=config.dataset_id,
            data_criacao=config.data_criacao,
            data_atualizacao=config.data_atualizacao,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to update persistent config: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update persistent config: {str(e)}"
        )


@router.delete(
    "/configs/persistent/{config_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete persistent config",
    description="Delete a persistent config."
)
async def delete_persistent_config(
    config_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Delete a persistent config."""
    try:
        result = await db.execute(
            select(BronzePersistentConfig).where(
                BronzePersistentConfig.id == config_id
            )
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Persistent config {config_id} not found"
            )
        
        await db.delete(config)
        await db.commit()
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to delete persistent config: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete persistent config: {str(e)}"
        )


@router.post(
    "/configs/persistent/{config_id}/preview",
    response_model=BronzePersistentPreviewResponse,
    summary="Preview persistent config execution",
    description="Preview what will happen when the config is executed (SQL, paths, etc.)."
)
async def preview_persistent_config(
    config_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Preview a persistent config execution."""
    try:
        # Get config
        result = await db.execute(
            select(BronzePersistentConfig).where(
                BronzePersistentConfig.id == config_id
            )
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Persistent config {config_id} not found"
            )
        
        # Convert config to request format
        tables = [TableColumnSelection(**t) for t in config.tables]
        
        request = DatasetBronzeCreateRequest(
            name=config.name,
            description=config.description,
            tables=tables,
            relationship_ids=config.relationship_ids,
            enable_federated_joins=config.enable_federated_joins,
            output_format=OutputFormatEnum(config.output_format),
            output_bucket=config.output_bucket,
            partition_columns=config.partition_columns,
            properties=config.properties or {},
        )
        
        # Generate preview
        service = BronzeIngestionService(db)
        preview = await service.generate_ingestion_plan_simplified(request)
        
        return BronzePersistentPreviewResponse(
            config_id=config.id,
            config_name=config.name,
            ingestion_groups=preview.ingestion_groups,
            inter_db_links=preview.inter_db_links,
            estimated_output_paths=preview.estimated_output_paths,
            total_tables=preview.total_tables,
            total_columns=preview.total_columns,
            warnings=preview.warnings,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to preview persistent config: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to preview persistent config: {str(e)}"
        )


@router.post(
    "/configs/persistent/{config_id}/execute",
    response_model=BronzePersistentExecuteResponse,
    summary="Execute persistent config",
    description="""
Execute the persistent config to materialize data to Delta Lake.

This will:
1. Extract data from source databases via Trino
2. Save raw data to Bronze Delta Lake
3. Create a dataset entry
4. Record execution history
"""
)
async def execute_persistent_config(
    config_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Execute a persistent config with proper Delta Lake versioning."""
    try:
        # Get config
        result = await db.execute(
            select(BronzePersistentConfig).where(
                BronzePersistentConfig.id == config_id
            )
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Persistent config {config_id} not found"
            )
        
        # Create config snapshot for reproducibility
        config_snapshot = {
            "name": config.name,
            "description": config.description,
            "tables": config.tables,
            "relationship_ids": config.relationship_ids,
            "enable_federated_joins": config.enable_federated_joins,
            "output_format": config.output_format,
            "output_bucket": config.output_bucket,
            "output_path_prefix": config.output_path_prefix,
            "partition_columns": config.partition_columns,
            "write_mode": config.write_mode.value if config.write_mode else None,
            "merge_keys": config.merge_keys,
            "merge_keys_source": config.merge_keys_source,
            "properties": config.properties,
            "snapshot_at": datetime.utcnow().isoformat(),
        }
        
        # Create execution record
        execution = BronzeExecution(
            config_id=config.id,
            status=BronzeExecutionStatus.RUNNING,
            started_at=datetime.utcnow(),
            config_snapshot=config_snapshot,
        )
        db.add(execution)
        await db.commit()
        await db.refresh(execution)
        
        try:
            # Convert config tables to TableColumnSelection
            tables = [TableColumnSelection(**t) for t in config.tables]
            
            # Execute with proper versioning using Spark (always overwrite)
            service = BronzeIngestionService(db)
            ingestion_result = await service.execute_persistent_config_with_versioning(
                config_id=config.id,
                config_name=config.name,
                tables=tables,
                relationship_ids=config.relationship_ids,
                enable_federated_joins=config.enable_federated_joins,
                output_format=config.output_format,
                output_bucket=config.output_bucket,
                output_path_prefix=config.output_path_prefix,
                partition_columns=config.partition_columns,
                properties=config.properties or {},
            )
            
            # Update execution record with versioning info
            execution.status = BronzeExecutionStatus(ingestion_result['status'].value)
            execution.finished_at = datetime.utcnow()
            execution.rows_ingested = ingestion_result['total_rows_ingested']
            execution.output_paths = ingestion_result['bronze_paths']
            execution.group_results = [g.model_dump() for g in ingestion_result['groups']]
            execution.delta_version = ingestion_result.get('delta_version')
            execution.write_mode_used = ingestion_result.get('write_mode_used')
            execution.merge_keys_used = ingestion_result.get('merge_keys_used')
            execution.rows_inserted = ingestion_result.get('rows_inserted')
            execution.rows_updated = ingestion_result.get('rows_updated')
            execution.rows_deleted = ingestion_result.get('rows_deleted')
            execution.execution_details = {
                'size_bytes': ingestion_result.get('size_bytes'),
                'num_files': ingestion_result.get('num_files'),
                'execution_time_seconds': ingestion_result.get('execution_time_seconds'),
            }
            
            # Update config
            config.last_execution_time = datetime.utcnow()
            config.last_execution_status = BronzeExecutionStatus(ingestion_result['status'].value)
            config.last_execution_rows = ingestion_result['total_rows_ingested']
            config.current_delta_version = ingestion_result.get('delta_version')
            
            await db.commit()
            
            return BronzePersistentExecuteResponse(
                config_id=config.id,
                config_name=config.name,
                execution_id=execution.id,
                status=ingestion_result['status'],
                groups=ingestion_result['groups'],
                total_rows_ingested=ingestion_result['total_rows_ingested'],
                bronze_paths=ingestion_result['bronze_paths'],
                execution_time_seconds=ingestion_result['execution_time_seconds'],
                message=ingestion_result['message'],
                delta_version=ingestion_result.get('delta_version'),
                write_mode_used=ingestion_result.get('write_mode_used'),
                merge_keys_used=ingestion_result.get('merge_keys_used'),
                rows_inserted=ingestion_result.get('rows_inserted'),
                rows_updated=ingestion_result.get('rows_updated'),
                config_snapshot=config_snapshot,
            )
            
        except Exception as exec_error:
            # Update execution record with error
            execution.status = BronzeExecutionStatus.FAILED
            execution.finished_at = datetime.utcnow()
            execution.error_message = str(exec_error)
            
            # Update config
            config.last_execution_time = datetime.utcnow()
            config.last_execution_status = BronzeExecutionStatus.FAILED
            config.last_execution_error = str(exec_error)
            
            await db.commit()
            raise
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to execute persistent config: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute persistent config: {str(e)}"
        )


@router.get(
    "/configs/persistent/{config_id}/executions",
    response_model=List[BronzeExecutionResponse],
    summary="List config executions",
    description="List execution history for a persistent config."
)
async def list_config_executions(
    config_id: int,
    limit: int = Query(20, ge=1, le=100, description="Maximum executions to return"),
    db: AsyncSession = Depends(get_db),
):
    """List executions for a persistent config."""
    try:
        # Check config exists
        config_result = await db.execute(
            select(BronzePersistentConfig).where(
                BronzePersistentConfig.id == config_id
            )
        )
        if not config_result.scalar_one_or_none():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Persistent config {config_id} not found"
            )
        
        # Get executions
        result = await db.execute(
            select(BronzeExecution)
            .where(BronzeExecution.config_id == config_id)
            .order_by(BronzeExecution.started_at.desc())
            .limit(limit)
        )
        executions = result.scalars().all()
        
        return [
            BronzeExecutionResponse(
                id=e.id,
                config_id=e.config_id,
                status=e.status.value if e.status else None,
                started_at=e.started_at,
                finished_at=e.finished_at,
                group_results=e.group_results,
                rows_ingested=e.rows_ingested,
                output_paths=e.output_paths,
                error_message=e.error_message,
                # Versioning fields
                delta_version=e.delta_version,
                write_mode_used=e.write_mode_used,
                merge_keys_used=e.merge_keys_used,
                rows_inserted=e.rows_inserted,
                rows_updated=e.rows_updated,
                rows_deleted=e.rows_deleted,
            )
            for e in executions
        ]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list config executions: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list config executions: {str(e)}"
        )


# ==================== VERSIONING & TIME TRAVEL ====================

@router.get(
    "/configs/persistent/{config_id}/versions",
    response_model=BronzeVersionHistoryResponse,
    summary="Get Delta Lake version history",
    description="""
Get the version history of a Bronze Delta Lake table.

Each execution creates a new version. You can use version numbers
for time travel queries.

**Metrics are aggregated across all output paths** - For non-federated configs with multiple sources,
the response shows unified metrics from all paths.

---

## **Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int | 100 | Maximum versions to return (1-1000) |

---

## **Response:**

```json
{
  "config_id": 1,
  "config_name": "patients_bronze",
  "current_version": 5,
  "output_paths": [
    "s3a://datafabric-bronze/1-patients_bronze/unified/"
  ],
  "versions": [
    {
      "version": 5,
      "timestamp": "2024-01-20T14:30:00Z",
      "operation": "OVERWRITE",
      "execution_id": 12,
      "rows_inserted": 150,
      "rows_updated": 23,
      "total_rows": 10523,
      "num_files": 4,
      "size_bytes": 1048576,
      "config_snapshot": {
        "name": "patients_bronze",
        "tables": [{"table_id": 1, "select_all": true}],
        "write_mode": "overwrite",
        "merge_keys": ["patient_id"],
        "snapshot_at": "2024-01-20T14:30:00"
      }
    },
    {
      "version": 4,
      "timestamp": "2024-01-15T10:00:00Z",
      "operation": "OVERWRITE",
      "execution_id": 11,
      "rows_inserted": 500,
      "rows_updated": 0,
      "total_rows": 10350,
      "num_files": 3,
      "size_bytes": 950000,
      "config_snapshot": {
        "name": "patients_bronze",
        "tables": [{"table_id": 1, "column_ids": [1,2,3]}],
        "write_mode": "overwrite",
        "merge_keys": ["patient_id"],
        "snapshot_at": "2024-01-15T10:00:00"
      }
    }
  ]
}
```

---

## **Use Cases:**
- See all versions available for time travel
- Track changes over time
- Audit data modifications
- **Compare config snapshots between versions (git-like diff)** - Each version includes the exact config used at execution time

## **Notes:**
- Metrics are aggregated from execution records (not individual Delta paths)
- `output_paths` lists all paths where data was written
- `config_snapshot` contains the exact configuration used for each execution, enabling diff comparison
- `size_bytes` and `num_files` show the total size and file count of the Delta table at each version
"""
)
async def get_version_history(
    config_id: int,
    limit: int = Query(100, ge=1, le=1000, description="Maximum versions to return"),
    db: AsyncSession = Depends(get_db),
):
    """Get Delta Lake version history for a config."""
    try:
        # Get config
        result = await db.execute(
            select(BronzePersistentConfig).where(
                BronzePersistentConfig.id == config_id
            )
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Persistent config {config_id} not found"
            )
        
        # Get version history from execution records (aggregated metrics)
        versioning_service = BronzeVersioningService(db)
        history = await versioning_service.get_version_history(
            config_id=config_id,
            config_name=config.name,
            limit=limit
        )
        
        return history
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get version history: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get version history: {str(e)}"
        )


@router.get(
    "/configs/persistent/{config_id}/data",
    response_model=BronzeDataQueryResponse,
    summary="Query Bronze data with time travel",
    description="""
Query the Bronze Delta Lake table with optional time travel.

You can query:
- **Latest version** (default): No parameters needed
- **Specific version**: Use `version` parameter
- **Point in time**: Use `as_of_timestamp` parameter

---

## **Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `version` | int | null | Query specific version (0, 1, 2, ...) |
| `as_of_timestamp` | string | null | Query as of timestamp (ISO format) |
| `limit` | int | 1000 | Maximum rows to return (1-100000) |
| `offset` | int | 0 | Rows to skip for pagination |
| `path_index` | int | 0 | Index of output path to query. For federated configs (single `/unified/` path), use 0. For non-federated configs with multiple connection paths, specify which path (0, 1, 2...) |

---

## **Examples:**

```bash
# Latest data
GET /api/bronze/configs/persistent/1/data

# Specific version
GET /api/bronze/configs/persistent/1/data?version=2

# Point in time
GET /api/bronze/configs/persistent/1/data?as_of_timestamp=2024-01-15T10:00:00Z

# With pagination
GET /api/bronze/configs/persistent/1/data?version=2&limit=100&offset=500

# Query specific path for non-federated config
GET /api/bronze/configs/persistent/1/data?path_index=1
```

---

## **Response:**

```json
{
  "config_id": 1,
  "config_name": "patients_bronze",
  "version": 2,
  "columns": ["patient_id", "name", "cpf", "_source_table", "_ingestion_timestamp"],
  "data": [
    {"patient_id": 1, "name": "João Silva", "cpf": "123.456.789-00", ...},
    ...
  ],
  "row_count": 100,
  "total_rows": 10350,
  "execution_time_seconds": 0.5
}
```

---

## **Notes:**
- Output path is retrieved from the actual execution record (not constructed from config)
- For federated configs (`enable_federated_joins=true`), there is a single `/unified/` path
- For non-federated configs, each connection group has its own path (use `path_index` to select)
"""
)
async def query_bronze_data(
    config_id: int,
    version: Optional[int] = Query(None, description="Query specific Delta version"),
    as_of_timestamp: Optional[str] = Query(None, description="Query as of timestamp (ISO format)"),
    limit: int = Query(1000, ge=1, le=100000, description="Maximum rows to return"),
    offset: int = Query(0, ge=0, description="Rows to skip"),
    path_index: int = Query(0, ge=0, description="Index of output path to query (for non-federated configs with multiple paths)"),
    db: AsyncSession = Depends(get_db),
):
    """Query Bronze data with optional time travel."""
    import time
    start_time = time.time()
    
    try:
        # Get config
        result = await db.execute(
            select(BronzePersistentConfig).where(
                BronzePersistentConfig.id == config_id
            )
        )
        config = result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Persistent config {config_id} not found"
            )
        
        # Get output path from the latest successful execution
        # Note: We check for successful executions directly, not config.last_execution_status
        # because the last execution might have failed but a previous one succeeded
        # This is the actual path where data was written
        execution_result = await db.execute(
            select(BronzeExecution)
            .where(
                BronzeExecution.config_id == config_id,
                BronzeExecution.status == BronzeExecutionStatus.SUCCESS
            )
            .order_by(BronzeExecution.started_at.desc())
            .limit(1)
        )
        latest_execution = execution_result.scalar_one_or_none()
        
        if not latest_execution or not latest_execution.output_paths:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No successful execution found with output paths. Run execute first."
            )
        
        # Get the output path from execution record
        output_paths = latest_execution.output_paths
        if path_index >= len(output_paths):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"path_index {path_index} is out of range. Available paths: {len(output_paths)} (0-{len(output_paths)-1})"
            )
        
        output_path = output_paths[path_index]
        
        # Query using Spark
        from ...services.infrastructure.spark_manager import SparkManager
        spark_manager = SparkManager()
        
        versioning_service = BronzeVersioningService(db)
        
        with spark_manager.session_scope() as spark:
            columns, data, total_rows = await versioning_service.query_at_version(
                spark=spark,
                output_path=output_path,
                version=version,
                timestamp=as_of_timestamp,
                limit=limit,
                offset=offset
            )
        
        execution_time = time.time() - start_time
        
        # Parse timestamp if provided
        parsed_timestamp = None
        if as_of_timestamp:
            try:
                from datetime import datetime
                parsed_timestamp = datetime.fromisoformat(as_of_timestamp.replace('Z', '+00:00'))
            except:
                pass
        
        return BronzeDataQueryResponse(
            config_id=config_id,
            config_name=config.name,
            version=version,
            as_of_timestamp=parsed_timestamp,
            columns=columns,
            data=data,
            row_count=len(data),
            total_rows=total_rows,
            execution_time_seconds=round(execution_time, 3)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to query Bronze data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to query Bronze data: {str(e)}"
        )
