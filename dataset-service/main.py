from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import asyncio
import logging
from datetime import datetime
import os
import sys

# Add the shared project to the path to import shared modules
sys.path.append('/shared/project')

from app.database.database import get_db
from app.api.service.dataset_service import DatasetService
from app.api.schemas.dataset_schemas import DatasetUnifiedCreate, DatasetResponse
from sqlalchemy.ext.asyncio import AsyncSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Dataset Unification Service", 
    description="Microserviço responsável por processar criação de datasets unificados",
    version="1.0.0"
)

class DatasetJobRequest(BaseModel):
    dataset_data: DatasetUnifiedCreate
    job_id: Optional[str] = None
    priority: Optional[int] = 1

class DatasetJobResponse(BaseModel):
    job_id: str
    dataset_id: Optional[int] = None
    status: str
    message: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    details: Optional[Dict[str, Any]] = None

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "service": "dataset-service",
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }

@app.post("/process-dataset-creation", response_model=DatasetJobResponse)
async def process_dataset_creation_job(dataset_request: DatasetJobRequest):
    """
    Processa um job de criação de dataset unificado.
    Este endpoint é chamado pelo Airflow via DAG.
    """
    job_id = dataset_request.job_id or f"dataset_{dataset_request.dataset_data.name}_{int(datetime.now().timestamp())}"
    started_at = datetime.now()
    
    logger.info(f"Starting dataset creation job {job_id} for dataset '{dataset_request.dataset_data.name}'")
    
    try:
        async for db in get_db():
            try:
                # Initialize dataset service
                service = DatasetService(db)
                
                # Log dataset creation details
                logger.info(f"Dataset creation details - Job: {job_id}")
                logger.info(f"  - Name: {dataset_request.dataset_data.name}")
                logger.info(f"  - Selection Mode: {dataset_request.dataset_data.selection_mode}")
                logger.info(f"  - Storage Type: {dataset_request.dataset_data.storage_type}")
                logger.info(f"  - Refresh Type: {dataset_request.dataset_data.refresh_type}")
                
                if dataset_request.dataset_data.selection_mode.value == 'tables':
                    logger.info(f"  - Selected Tables: {dataset_request.dataset_data.selected_tables}")
                else:
                    logger.info(f"  - Selected Columns: {dataset_request.dataset_data.selected_columns}")
                
                # Create unified dataset
                dataset_result = await service.create_unified_dataset(dataset_request.dataset_data)
                
                completed_at = datetime.now()
                processing_time = (completed_at - started_at).total_seconds()
                
                success_message = f"Dataset creation job {job_id} completed successfully in {processing_time:.2f}s"
                logger.info(success_message)
                
                return DatasetJobResponse(
                    job_id=job_id,
                    dataset_id=dataset_result.id,
                    status='completed',
                    message=success_message,
                    started_at=started_at,
                    completed_at=completed_at,
                    details={
                        "dataset_name": dataset_result.name,
                        "dataset_id": dataset_result.id,
                        "storage_type": dataset_result.storage_type,
                        "processing_time_seconds": processing_time,
                        "selection_mode": dataset_request.dataset_data.selection_mode.value,
                        "tables_count": len(dataset_request.dataset_data.selected_tables or []),
                        "columns_count": len(dataset_request.dataset_data.selected_columns or [])
                    }
                )
                
            except HTTPException as he:
                logger.error(f"HTTP error in dataset job {job_id}: {he.detail}")
                raise he
                
            except Exception as e:
                logger.error(f"Error in dataset creation job {job_id}: {str(e)}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Dataset creation job failed: {str(e)}"
                )
            
            finally:
                await db.close()
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in dataset job {job_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error: {str(e)}"
        )

@app.post("/preview-dataset-creation")
async def preview_dataset_creation(dataset_request: DatasetJobRequest):
    """
    Gera preview de um dataset unificado sem criar o dataset.
    Útil para validação antes de executar o job completo.
    """
    logger.info(f"Generating dataset preview for '{dataset_request.dataset_data.name}'")
    
    try:
        async for db in get_db():
            try:
                from app.api.schemas.dataset_schemas import DatasetUnificationPreviewRequest
                
                # Convert DatasetUnifiedCreate to preview request
                preview_request = DatasetUnificationPreviewRequest(
                    selection_mode=dataset_request.dataset_data.selection_mode,
                    selected_tables=dataset_request.dataset_data.selected_tables,
                    selected_columns=dataset_request.dataset_data.selected_columns,
                    auto_include_mapped_columns=dataset_request.dataset_data.auto_include_mapped_columns,
                    apply_value_mappings=dataset_request.dataset_data.apply_value_mappings
                )
                
                # Initialize dataset service
                service = DatasetService(db)
                
                # Generate preview
                preview = await service.get_unification_preview_enhanced(preview_request)
                
                return {
                    "preview": preview,
                    "estimated_processing_time": "2-10 minutes",  # Rough estimate
                    "recommendations": [
                        "Review column mappings before proceeding",
                        "Ensure sufficient storage space for dataset creation",
                        "Consider data volume for processing time estimation"
                    ]
                }
                
            except Exception as e:
                logger.error(f"Error generating dataset preview: {str(e)}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Preview generation failed: {str(e)}"
                )
            
            finally:
                await db.close()
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in dataset preview: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error: {str(e)}"
        )

@app.get("/status/{job_id}")
async def get_dataset_job_status(job_id: str):
    """
    Obtém o status de um job de criação de dataset.
    Pode ser usado pelo Airflow para monitoramento.
    """
    # For now, we'll return a simple status based on job_id pattern
    # In a production environment, you'd want to store job status in a database or cache
    try:
        if job_id.startswith('dataset_'):
            return {
                "job_id": job_id,
                "status": "unknown",
                "message": "Job status tracking not implemented yet",
                "timestamp": datetime.now().isoformat(),
                "note": "Check Airflow DAG status for current job state"
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid job_id format: {job_id}"
            )
            
    except Exception as e:
        logger.error(f"Error getting job status for {job_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting job status: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8006)
