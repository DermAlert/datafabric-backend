"""
Internal API endpoints for Airflow and other internal services.
These endpoints are not meant to be exposed to end users.
"""
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime
import logging

from app.services.metadata_extraction import extract_metadata

router = APIRouter()
logger = logging.getLogger(__name__)


class SyncRequest(BaseModel):
    """Request model for sync operations (compatible with sync-service)"""
    connection_id: int = Field(..., description="ID da conexão de dados para sincronizar")
    job_id: str = Field(..., description="ID único do job de sincronização")
    priority: int = Field(default=1, description="Prioridade do job (1-10)")


class SyncResponse(BaseModel):
    """Response model for sync operations"""
    job_id: str
    connection_id: int
    status: str
    message: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    details: Optional[Dict[str, Any]] = None


@router.get("/health")
async def health_check():
    """Health check endpoint for internal services"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "datafabric-backend-internal"
    }


@router.post("/process-sync", response_model=SyncResponse)
async def process_sync(request: SyncRequest):
    """
    Processa sincronização de metadados para uma conexão de dados.
    Este endpoint é chamado pelo DAG do Airflow.
    
    Compatível com o formato do sync-service original.
    """
    start_time = datetime.now()
    
    logger.info(f"🚀 [Internal] Starting sync job {request.job_id} for connection {request.connection_id}")
    
    # Validate priority
    if not 1 <= request.priority <= 10:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Priority must be between 1 and 10"
        )
    
    try:
        # Validate connection_id
        if request.connection_id <= 0:
            raise ValueError("connection_id must be a positive integer")
        
        logger.info(f"📊 [Internal] Extracting metadata for connection {request.connection_id}")
        
        # Call the metadata extraction function
        success = await extract_metadata(request.connection_id)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        if success:
            logger.info(f"✅ [Internal] Sync job {request.job_id} completed successfully in {duration:.2f}s")
            
            return SyncResponse(
                job_id=request.job_id,
                connection_id=request.connection_id,
                status="completed",
                message=f"Metadata synchronization completed successfully for connection {request.connection_id}",
                started_at=start_time,
                completed_at=end_time,
                duration_seconds=duration,
                details={
                    "priority": request.priority,
                    "extraction_success": True
                }
            )
        else:
            logger.error(f"❌ [Internal] Sync job {request.job_id} failed - metadata extraction returned False")
            
            return SyncResponse(
                job_id=request.job_id,
                connection_id=request.connection_id,
                status="failed",
                message=f"Metadata synchronization failed for connection {request.connection_id}",
                started_at=start_time,
                completed_at=end_time,
                duration_seconds=duration,
                details={
                    "priority": request.priority,
                    "extraction_success": False,
                    "error": "Metadata extraction returned False"
                }
            )
            
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        error_msg = str(e)
        logger.error(f"💥 [Internal] Sync job {request.job_id} failed with error: {error_msg}")
        
        return SyncResponse(
            job_id=request.job_id,
            connection_id=request.connection_id,
            status="failed",
            message=f"Metadata synchronization failed: {error_msg}",
            started_at=start_time,
            completed_at=end_time,
            duration_seconds=duration,
            details={
                "priority": request.priority,
                "extraction_success": False,
                "error": error_msg
            }
        )

