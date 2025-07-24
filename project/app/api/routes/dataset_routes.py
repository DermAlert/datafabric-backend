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
    SearchDataset,
    DatasetImageResponse,
    SearchDatasetImages,
    DatasetImageStats
)
from ..schemas.search import SearchResult
from ..service.dataset_service import DatasetService
from ...services.dataset_image_service import DatasetImageService

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
## deprecated
# @router.post("/", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED) ## deprecated
# async def create_dataset(
#     dataset_data: DatasetCreate,
#     db: AsyncSession = Depends(get_db),
#     # current_user = Depends(get_current_user),
# ):
#     """Create a standard dataset with manual configuration."""
#     try:
#         service = DatasetService(db)
#         return await service.create(dataset_data)
#     except HTTPException:
#         raise
#     except Exception as e:
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Error creating dataset: {str(e)}"
#        )

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

# ==================== DATASET IMAGES OPERATIONS ====================

@router.post("/{dataset_id}/images", response_model=SearchResult[DatasetImageResponse])
async def get_dataset_images(
    dataset_id: int,
    search: SearchDatasetImages,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Get paginated list of dataset images with presigned URLs."""
    try:
        service = DatasetImageService(db)
        return await service.get_dataset_images_paginated(dataset_id, search)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving dataset images: {str(e)}"
        )

@router.get("/{dataset_id}/images/{image_path:path}", response_model=DatasetImageResponse)
async def get_single_dataset_image(
    dataset_id: int,
    image_path: str,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """Get presigned URL for a specific dataset image."""
    try:
        service = DatasetImageService(db)
        return await service.get_single_image_presigned_url(dataset_id, image_path)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving image: {str(e)}"
        )

# ==================== DATASET IMAGES OPERATIONS ====================

@router.post("/{dataset_id}/images", response_model=SearchResult[DatasetImageResponse])
async def get_dataset_images_paginated(
    dataset_id: int,
    search_params: SearchDatasetImages,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Get paginated list of dataset images with presigned URLs.
    
    This endpoint provides paginated access to images stored in a dataset's MinIO bucket.
    For each image, it returns:
    - Image metadata (name, size, creation date, etc.)
    - Presigned URL for direct access (valid for 1 hour)
    - Content type and other file information
    
    **Requirements:**
    - Dataset must be stored in MinIO (storage_type = 'copy_to_minio')
    - Images should be stored in the 'images/' folder within the dataset bucket
    
    **Search Parameters:**
    - page: Page number (default: 1)
    - size: Items per page (default: 20, max: 100)
    - image_name: Filter by image name (partial match)
    - content_type: Filter by content type
    - date_from: Filter images created after this date
    - date_to: Filter images created before this date
    """
    try:
        image_service = DatasetImageService(db)
        return await image_service.get_dataset_images_paginated(dataset_id, search_params)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving dataset images: {str(e)}"
        )

@router.get("/{dataset_id}/images/{image_path:path}", response_model=DatasetImageResponse)
async def get_dataset_image(
    dataset_id: int,
    image_path: str,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Get presigned URL for a specific dataset image.
    
    This endpoint provides direct access to a specific image within a dataset.
    Returns a presigned URL that allows direct download/viewing of the image.
    
    **Parameters:**
    - dataset_id: The ID of the dataset
    - image_path: The full path to the image within the bucket (e.g., 'images/photo.jpg')
    
    **Requirements:**
    - Dataset must be stored in MinIO (storage_type = 'copy_to_minio')
    - Image must exist in the dataset's bucket
    """
    try:
        image_service = DatasetImageService(db)
        return await image_service.get_single_image_presigned_url(dataset_id, image_path)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving dataset image: {str(e)}"
        )

@router.get("/{dataset_id}/images/stats", response_model=DatasetImageStats)
async def get_dataset_image_stats(
    dataset_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user = Depends(get_current_user),
):
    """
    Get statistics about dataset images.
    
    This endpoint provides comprehensive statistics about all images in a dataset:
    - Total number of images
    - Total size in bytes
    - Distribution by content type
    - Upload date range (oldest and latest)
    - Average file size
    
    **Requirements:**
    - Dataset must be stored in MinIO (storage_type = 'copy_to_minio')
    """
    try:
        image_service = DatasetImageService(db)
        return await image_service.get_dataset_image_stats(dataset_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving dataset image statistics: {str(e)}"
        )
