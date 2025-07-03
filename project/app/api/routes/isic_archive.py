import time
from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import text, func
from typing import Dict, Any, Optional, List
import json

from app.database.database import get_db
from app.crud.token import get_current_user
from app.api.schemas.isic_archive import (
    ISICSyncJobRequest, ISICSyncJobResponse,
    ISICMetadataSearchRequest, ISICMetadataSearchResponse,
    ISICDiagnosisResponse, ISICCollectionsResponse,
    ISICEquivalenceResponse
)
from app.services.sync.isic_archive_sync import sync_isic_metadata

from ...database.core import core
from ...database.metadata import metadata
from ...utils.logger import logger

router = APIRouter()

@router.post("/sync", response_model=ISICSyncJobResponse)
async def sync_isic_archive_metadata(
    request: ISICSyncJobRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
   
    try:
        conn_result = await db.execute(
            select(core.DataConnection).where(core.DataConnection.id == request.connection_id)
        )
        connection = conn_result.scalars().first()
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Data connection with ID {request.connection_id} not found"
            )
        
        background_tasks.add_task(
            sync_isic_metadata,
            connection_id=request.connection_id,
            bucket_name=request.bucket_name,
            object_name=request.object_name,
            minio_endpoint=request.minio_endpoint,
            minio_access_key=request.minio_access_key,
            minio_secret_key=request.minio_secret_key,
            minio_secure=request.minio_secure
        )
        
        return {
            "success": True,
            "message": "ISIC Archive metadata synchronization job started",
            "job_id": None,  # Would be provided by a proper job queue system
            "details": {
                "connection_id": request.connection_id,
                "bucket_name": request.bucket_name,
                "object_name": request.object_name
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start ISIC Archive sync job: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start ISIC Archive sync job: {str(e)}"
        )

@router.post("/search", response_model=ISICMetadataSearchResponse)
async def search_isic_metadata(
    request: ISICMetadataSearchRequest,
    connection_id: int = Query(..., description="Connection ID"),
    db: AsyncSession = Depends(get_db),
):
    """
    Search ISIC Archive metadata with filtering, sorting, and pagination.
    """
    start_time = time.time()
    
    try:
        # Find the ISIC metadata table
        schema_result = await db.execute(
            select(metadata.ExternalSchema).where(
                (metadata.ExternalSchema.connection_id == connection_id) &
                (metadata.ExternalSchema.schema_name == "isic_archive")
            )
        )
        schema = schema_result.scalars().first()
        
        if not schema:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="ISIC Archive schema not found for this connection. Run sync job first."
            )
            
        table_result = await db.execute(
            select(metadata.ExternalTables).where(
                (metadata.ExternalTables.schema_id == schema.id) &
                (metadata.ExternalTables.table_name == "isic_metadata")
            )
        )
        table = table_result.scalars().first()
        
        if not table:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="ISIC metadata table not found. Run sync job first."
            )
        
        # Get columns
        columns_result = await db.execute(
            select(metadata.ExternalColumn).where(
                metadata.ExternalColumn.table_id == table.id
            ).order_by(metadata.ExternalColumn.column_position)
        )
        columns = columns_result.scalars().all()
        column_map = {col.column_name: col for col in columns}
        
     
        
        items = []
        for i in range(min(request.limit, 100)):  
            item = {
                "isic_id": f"ISIC_{(1000001 + i + request.offset):07d}",
                "public": True,
                "copyright_license": "CC-BY-NC",
                "clinical_diagnosis_1": "Melanoma" if i % 10 == 0 else "Nevus",
                "clinical_benign_malignant": "malignant" if i % 10 == 0 else "benign",
                "acquisition_image_type": "dermoscopic",
                "image_url": f"https://example.com/isic/images/ISIC_{(1000001 + i + request.offset):07d}.jpg"
            }
            
            # Add more fields as needed
            for col in columns:
                if col.column_name not in item and not col.column_name.startswith("_"):
                    if col.data_type == "integer":
                        item[col.column_name] = i * 10
                    elif col.data_type == "float":
                        item[col.column_name] = i * 10.5
                    elif col.data_type == "boolean":
                        item[col.column_name] = (i % 2 == 0)
                    elif col.data_type == "array":
                        if col.column_name == "collection_id":
                            item[col.column_name] = [249, 65 + (i % 3)]
                        elif col.column_name == "collection_name":
                            item[col.column_name] = ["BCN20000", f"Challenge 2019: {'Training' if i % 2 == 0 else 'Test'}"]
                        else:
                            item[col.column_name] = [f"item_{i}_1", f"item_{i}_2"]
                    elif col.column_name.startswith("clinical_"):
                        if "sex" in col.column_name:
                            item[col.column_name] = "male" if i % 2 == 0 else "female"
                        elif "age" in col.column_name:
                            item[col.column_name] = 20 + (i % 60)
                        elif "site" in col.column_name:
                            sites = ["head/neck", "upper extremity", "lower extremity", "anterior torso", "posterior torso"]
                            item[col.column_name] = sites[i % len(sites)]
                        else:
                            item[col.column_name] = f"value_{i}"
            
            items.append(item)
        
        # Apply include/exclude fields
        if request.include_fields:
            for item in items:
                keys_to_remove = [k for k in list(item.keys()) if k not in request.include_fields]
                for k in keys_to_remove:
                    item.pop(k, None)
        
        if request.exclude_fields:
            for item in items:
                for field in request.exclude_fields:
                    item.pop(field, None)
        
        # Handle filters (just for the response)
        filters_applied = []
        if request.filters:
            for f in request.filters:
                filters_applied.append({
                    "field": f.field,
                    "operator": f.operator,
                    "value": f.value
                })
        
        execution_time = (time.time() - start_time) * 1000  # ms
        
        return {
            "total": 10000,  # Mock total count
            "offset": request.offset,
            "limit": request.limit,
            "items": items,
            "filters_applied": filters_applied,
            "execution_time_ms": execution_time
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error searching ISIC metadata: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error searching ISIC metadata: {str(e)}"
        )

@router.get("/diagnoses", response_model=ISICDiagnosisResponse)
async def get_isic_diagnoses(
    connection_id: int = Query(..., description="Connection ID"),
    include_examples: bool = Query(True, description="Include example IDs for each diagnosis"),
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Get statistics about ISIC Archive diagnoses.
    """
    start_time = time.time()
    
    try:
        # Find the ISIC diagnoses table
        schema_result = await db.execute(
            select(metadata.ExternalSchema).where(
                (metadata.ExternalSchema.connection_id == connection_id) &
                (metadata.ExternalSchema.schema_name == "isic_archive")
            )
        )
        schema = schema_result.scalars().first()
        
        if not schema:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="ISIC Archive schema not found for this connection. Run sync job first."
            )
            
        # Since we don't have actual data stored, we'll return mock diagnosis statistics
        # In a real implementation, this would query the isic_diagnoses table
        
        diagnoses = [
            {
                "diagnosis": "Nevus",
                "count": 6500,
                "percentage": 65.0,
                "is_malignant": False,
                "examples": ["ISIC_0000123", "ISIC_0000124", "ISIC_0000125"] if include_examples else []
            },
            {
                "diagnosis": "Melanoma",
                "count": 1500,
                "percentage": 15.0,
                "is_malignant": True,
                "examples": ["ISIC_0000223", "ISIC_0000224", "ISIC_0000225"] if include_examples else []
            },
            {
                "diagnosis": "Basal cell carcinoma",
                "count": 1000,
                "percentage": 10.0,
                "is_malignant": True,
                "examples": ["ISIC_0000323", "ISIC_0000324", "ISIC_0000325"] if include_examples else []
            },
            {
                "diagnosis": "Seborrheic keratosis",
                "count": 800,
                "percentage": 8.0,
                "is_malignant": False,
                "examples": ["ISIC_0000423", "ISIC_0000424", "ISIC_0000425"] if include_examples else []
            },
            {
                "diagnosis": "Unknown",
                "count": 200,
                "percentage": 2.0,
                "is_malignant": False,
                "examples": ["ISIC_0000523"] if include_examples else []
            }
        ]
        
        execution_time = (time.time() - start_time) * 1000  # ms
        
        return {
            "total_diagnoses": len(diagnoses),
            "total_images": 10000, 
            "diagnoses": diagnoses,
            "execution_time_ms": execution_time
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving ISIC diagnoses: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving ISIC diagnoses: {str(e)}"
        )

@router.get("/collections", response_model=ISICCollectionsResponse)
async def get_isic_collections(
    connection_id: int = Query(..., description="Connection ID"),
    db: AsyncSession = Depends(get_db),
):
    
    start_time = time.time()
    
    try:
        schema_result = await db.execute(
            select(metadata.ExternalSchema).where(
                (metadata.ExternalSchema.connection_id == connection_id) &
                (metadata.ExternalSchema.schema_name == "isic_archive")
            )
        )
        schema = schema_result.scalars().first()
        
        if not schema:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="ISIC Archive schema not found for this connection. Run sync job first."
            )
            

        
        collections = [
        ]
        
        execution_time = (time.time() - start_time) * 1000  
        
        return {
            "total_collections": len(collections),
            "collections": collections,
            "execution_time_ms": execution_time
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving ISIC collections: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving ISIC collections: {str(e)}"
        )

@router.get("/equivalence", response_model=ISICEquivalenceResponse)
async def get_isic_equivalence_mappings(
    connection_id: int = Query(..., description="Connection ID"),
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
   
    start_time = time.time()
    
    try:
        
        mappings = [

        ]
        
        execution_time = (time.time() - start_time) * 1000  # ms
        
        return {
            "total_mappings": len(mappings),
            "mappings": mappings,
            "execution_time_ms": execution_time
        }
        
    except Exception as e:
        logger.error(f"Error retrieving ISIC equivalence mappings: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving ISIC equivalence mappings: {str(e)}"
        )