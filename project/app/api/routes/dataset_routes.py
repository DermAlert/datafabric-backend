from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update
from typing import List, Optional
from datetime import datetime

from ...database.database import get_db
from ...database.core.core import Dataset
from ...crud.token import get_current_user
from ..schemas.dataset_schemas import (
    DatasetCreate,
    DatasetUpdate,
    DatasetResponse,
    DatasetUnifiedCreate,
    DatasetUnificationPreview,
    DatasetUnificationPreviewRequest,
    SearchDataset
)
from ..schemas.search import SearchResult
from ..service.dataset_service import DatasetService

router = APIRouter()

# ==================== UNIFIED DATASET CREATION ====================

@router.post("/unified", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def create_unified_dataset(
    dataset_data: DatasetUnifiedCreate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Create a unified dataset from multiple tables or specific columns with automatic column mapping.
    
    This endpoint supports two selection modes:
    
    **Table-based selection (selection_mode = "tables"):**
    - Specify selected_tables with table IDs
    - Automatically includes all columns from selected tables
    - If auto_include_mapped_columns=true, includes entire tables that have mapped columns
    
    **Column-based selection (selection_mode = "columns"):**
    - Specify selected_columns with specific column IDs
    - Only includes the specified columns in the dataset
    - If auto_include_mapped_columns=true, includes only other mapped columns from the same groups, not entire tables
    
    The endpoint automatically:
    - Identifies columns with existing mappings
    - Applies column group mappings for semantic equivalence
    - Applies value mappings for data standardization
    - Creates unified columns based on selected approach
    
    **Parameters:**
    - selection_mode: "tables" or "columns"
    - selected_tables: List of table IDs (required when selection_mode="tables")
    - selected_columns: List of column IDs (required when selection_mode="columns")
    - auto_include_mapped_columns: Whether to include related mapped columns/tables
    - apply_value_mappings: Whether to apply value standardization mappings
    """
    try:
        service = DatasetService(db)
        return await service.create_unified_dataset(dataset_data)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating unified dataset: {str(e)}"
        )

@router.post("/unified/preview", response_model=DatasetUnificationPreview)
async def preview_unified_dataset(
    preview_request: DatasetUnificationPreviewRequest,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Preview what will be created when unifying the selected tables or columns.
    
    Supports both selection modes:
    - **Tables mode**: Shows all columns from selected tables and any auto-included tables
    - **Columns mode**: Shows only specified columns and any auto-included mapped columns
    
    Shows:
    - Selected tables/columns and their information
    - Existing column mappings that will be applied
    - Value mappings that will be used
    - Estimated unified columns count
    - Auto-included tables/columns due to mappings
    """
    try:
        service = DatasetService(db)
        return await service.get_unification_preview_enhanced(preview_request)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating preview: {str(e)}"
        )

# ==================== STANDARD DATASET OPERATIONS ====================

@router.post("/", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def create_dataset(
    dataset_data: DatasetCreate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Create a standard dataset with manual configuration."""
    try:
        service = DatasetService(db)
        return await service.create(dataset_data)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating dataset: {str(e)}"
        )

@router.get("/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(
    dataset_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Get dataset by ID with full details."""
    try:
        service = DatasetService(db)
        return await service.get(dataset_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving dataset: {str(e)}"
        )

@router.put("/{dataset_id}", response_model=DatasetResponse)
async def update_dataset(
    dataset_id: int,
    dataset_data: DatasetUpdate,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Update dataset metadata."""
    try:
        service = DatasetService(db)
        return await service.update(dataset_id, dataset_data)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating dataset: {str(e)}"
        )

@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dataset(
    dataset_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Delete dataset."""
    try:
        service = DatasetService(db)
        await service.delete(dataset_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting dataset: {str(e)}"
        )

@router.post("/search", response_model=SearchResult[DatasetResponse])
async def search_datasets(
    search: SearchDataset,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Search datasets with pagination and filters."""
    try:
        service = DatasetService(db)
        return await service.list(search)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error searching datasets: {str(e)}"
        )

# ==================== DATASET COLUMNS ====================

@router.get("/{dataset_id}/columns", response_model=List[dict])
async def get_dataset_columns(
    dataset_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Get all columns for a dataset."""
    try:
        service = DatasetService(db)
        dataset = await service.get(dataset_id)
        return dataset.columns
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving dataset columns: {str(e)}"
        )

@router.get("/{dataset_id}/sources", response_model=List[dict])
async def get_dataset_sources(
    dataset_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Get all source tables for a dataset."""
    try:
        service = DatasetService(db)
        dataset = await service.get(dataset_id)
        return dataset.sources
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving dataset sources: {str(e)}"
        )

# ==================== UTILITY ENDPOINTS ====================

@router.get("/{dataset_id}/sql", response_model=dict)
async def get_dataset_sql(
    dataset_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Generate SQL query for the dataset."""
    try:
        # This would generate the SQL query based on the dataset configuration
        # For now, returning a placeholder
        return {
            "sql": f"-- Generated SQL for dataset {dataset_id}\n-- This would contain the actual query based on sources and mappings",
            "estimated_rows": 0,
            "complexity": "medium"
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating SQL: {str(e)}"
        )

@router.post("/{dataset_id}/validate", response_model=dict)
async def validate_dataset(
    dataset_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Validate dataset configuration."""
    try:
        service = DatasetService(db)
        dataset = await service.get(dataset_id)
        
        # Basic validation
        validation_results = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "source_tables_count": len(dataset.sources),
            "columns_count": len(dataset.columns)
        }
        
        # Check if dataset has sources
        if not dataset.sources:
            validation_results["valid"] = False
            validation_results["errors"].append("Dataset must have at least one source table")
        
        # Check if dataset has columns
        if not dataset.columns:
            validation_results["valid"] = False
            validation_results["errors"].append("Dataset must have at least one column")
        
        return validation_results
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error validating dataset: {str(e)}"
        )

# ==================== MINIO INTEGRATION ENDPOINTS ====================

@router.post("/{dataset_id}/export-to-minio", response_model=dict)
async def export_dataset_to_minio(
    dataset_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Export dataset metadata to MinIO with Delta Lake format."""
    try:
        from app.services.dataset_minio_service import DatasetMinioService
        import os
        
        service = DatasetService(db)
        dataset = await service.get(dataset_id)
        
        # Check if dataset already has MinIO export
        if dataset.properties.get('minio_bucket'):
            return {
                "message": "Dataset already exported to MinIO",
                "bucket_name": dataset.properties.get('minio_bucket'),
                "delta_path": dataset.properties.get('delta_path'),
                "export_timestamp": dataset.properties.get('export_timestamp')
            }
        
        # Initialize MinIO service
        minio_config = {
            'endpoint': os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            'access_key': os.getenv('MINIO_ACCESS_KEY', 'minio'),
            'secret_key': os.getenv('MINIO_SECRET_KEY', 'minio123'),
            'secure': os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        }
        
        minio_service = DatasetMinioService(minio_config)
        await minio_service.initialize()
        
        try:
            # Create bucket and export metadata
            bucket_name = await minio_service.create_dataset_bucket(dataset.id, dataset.name)
            
            # Prepare metadata
            dataset_metadata = {
                "name": dataset.name,
                "description": dataset.description,
                "storage_type": dataset.storage_type,
                "refresh_type": dataset.refresh_type,
                "status": dataset.status,
                "version": dataset.version,
                "properties": dataset.properties
            }
            
            delta_path = await minio_service.export_dataset_metadata_to_delta(
                dataset.id,
                bucket_name,
                dataset_metadata,
                dataset.columns,
                dataset.sources
            )
            
            # Update dataset properties
            from sqlalchemy import update
            stmt = update(Dataset).where(Dataset.id == dataset_id).values(
                properties=Dataset.properties.op('||')({
                    'minio_bucket': bucket_name,
                    'delta_path': delta_path,
                    'export_timestamp': datetime.now().isoformat(),
                    'export_status': 'completed'
                })
            )
            await db.execute(stmt)
            await db.commit()
            
            return {
                "message": "Dataset exported to MinIO successfully",
                "bucket_name": bucket_name,
                "delta_path": delta_path,
                "export_timestamp": datetime.now().isoformat()
            }
            
        finally:
            minio_service.close()
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error exporting dataset to MinIO: {str(e)}"
        )

@router.get("/{dataset_id}/minio-status", response_model=dict)
async def get_dataset_minio_status(
    dataset_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Get MinIO export status for a dataset."""
    try:
        service = DatasetService(db)
        dataset = await service.get(dataset_id)
        
        if not dataset.properties.get('minio_bucket'):
            return {
                "exported": False,
                "message": "Dataset not exported to MinIO"
            }
        
        return {
            "exported": True,
            "bucket_name": dataset.properties.get('minio_bucket'),
            "delta_path": dataset.properties.get('delta_path'),
            "export_timestamp": dataset.properties.get('export_timestamp'),
            "export_status": dataset.properties.get('export_status', 'unknown')
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting MinIO status: {str(e)}"
        )

@router.post("/{dataset_id}/validate-minio-export", response_model=dict)
async def validate_dataset_minio_export(
    dataset_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Validate MinIO export for a dataset."""
    try:
        from app.services.dataset_minio_service import DatasetMinioService
        import os
        
        service = DatasetService(db)
        dataset = await service.get(dataset_id)
        
        bucket_name = dataset.properties.get('minio_bucket')
        if not bucket_name:
            return {
                "valid": False,
                "error": "Dataset not exported to MinIO"
            }
        
        # Initialize MinIO service
        minio_config = {
            'endpoint': os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            'access_key': os.getenv('MINIO_ACCESS_KEY', 'minio'),
            'secret_key': os.getenv('MINIO_SECRET_KEY', 'minio123'),
            'secure': os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        }
        
        minio_service = DatasetMinioService(minio_config)
        await minio_service.initialize()
        
        try:
            validation_result = await minio_service.validate_export(bucket_name, dataset_id)
            return validation_result
            
        finally:
            minio_service.close()
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error validating MinIO export: {str(e)}"
        )

@router.get("/{dataset_id}/minio-files", response_model=List[dict])
async def list_dataset_minio_files(
    dataset_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """List all files in the dataset's MinIO bucket."""
    try:
        from app.services.dataset_minio_service import DatasetMinioService
        import os
        
        service = DatasetService(db)
        dataset = await service.get(dataset_id)
        
        bucket_name = dataset.properties.get('minio_bucket')
        if not bucket_name:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Dataset not exported to MinIO"
            )
        
        # Initialize MinIO service
        minio_config = {
            'endpoint': os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            'access_key': os.getenv('MINIO_ACCESS_KEY', 'minio'),
            'secret_key': os.getenv('MINIO_SECRET_KEY', 'minio123'),
            'secure': os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        }
        
        minio_service = DatasetMinioService(minio_config)
        await minio_service.initialize()
        
        try:
            files = await minio_service.list_dataset_files(bucket_name)
            return files
            
        finally:
            minio_service.close()
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing MinIO files: {str(e)}"
        )

@router.delete("/{dataset_id}/minio-bucket", response_model=dict)
async def delete_dataset_minio_bucket(
    dataset_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Delete the dataset's MinIO bucket and all its contents."""
    try:
        from app.services.dataset_minio_service import DatasetMinioService
        import os
        
        service = DatasetService(db)
        dataset = await service.get(dataset_id)
        
        bucket_name = dataset.properties.get('minio_bucket')
        if not bucket_name:
            return {
                "message": "Dataset not exported to MinIO, nothing to delete"
            }
        
        # Initialize MinIO service
        minio_config = {
            'endpoint': os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            'access_key': os.getenv('MINIO_ACCESS_KEY', 'minio'),
            'secret_key': os.getenv('MINIO_SECRET_KEY', 'minio123'),
            'secure': os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        }
        
        minio_service = DatasetMinioService(minio_config)
        await minio_service.initialize()
        
        try:
            success = await minio_service.delete_dataset_bucket(bucket_name)
            
            if success:
                # Remove MinIO info from dataset properties
                from sqlalchemy import update
                new_properties = dataset.properties.copy()
                new_properties.pop('minio_bucket', None)
                new_properties.pop('delta_path', None)
                new_properties.pop('export_timestamp', None)
                new_properties.pop('export_status', None)
                
                stmt = update(Dataset).where(Dataset.id == dataset_id).values(
                    properties=new_properties
                )
                await db.execute(stmt)
                await db.commit()
                
                return {
                    "message": f"Bucket {bucket_name} deleted successfully"
                }
            else:
                return {
                    "message": f"Failed to delete bucket {bucket_name}"
                }
            
        finally:
            minio_service.close()
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting MinIO bucket: {str(e)}"
        )
