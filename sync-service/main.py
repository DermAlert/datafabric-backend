from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
import logging
import asyncio
import os
import sys
from datetime import datetime
import uuid

# Add the project directory to the Python path so we can import from it
project_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'project')
sys.path.insert(0, project_path)

from app.services.metadata_extraction import extract_metadata

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Data Fabric Sync Service",
    description="Microserviço para sincronização de metadados de conexões de dados",
    version="1.0.0"
)

class SyncRequest(BaseModel):
    """Request model for sync operations"""
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

class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    timestamp: datetime
    service: str
    version: str

# In-memory job tracking (in production, use Redis or database)
active_jobs: Dict[str, Dict[str, Any]] = {}

@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint with service information"""
    return {
        "service": "Data Fabric Sync Service",
        "status": "running",
        "version": "1.0.0",
        "description": "Microserviço para sincronização de metadados"
    }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(),
        service="sync-service",
        version="1.0.0"
    )

@app.post("/process-sync", response_model=SyncResponse, status_code=status.HTTP_200_OK)
async def process_sync(request: SyncRequest):
    """
    Processa sincronização de metadados para uma conexão de dados.
    Este endpoint é chamado pelo DAG do Airflow.
    """
    start_time = datetime.now()
    
    logger.info(f"🚀 Starting sync job {request.job_id} for connection {request.connection_id}")
    
    # Validate priority
    if not 1 <= request.priority <= 10:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Priority must be between 1 and 10"
        )
    
    # Check if job is already running
    if request.job_id in active_jobs:
        logger.warning(f"Job {request.job_id} is already running")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Job {request.job_id} is already running"
        )
    
    # Register job as active
    active_jobs[request.job_id] = {
        "connection_id": request.connection_id,
        "status": "running",
        "started_at": start_time,
        "priority": request.priority
    }
    
    try:
        # Validate connection_id
        if request.connection_id <= 0:
            raise ValueError("connection_id must be a positive integer")
        
        logger.info(f"📊 Extracting metadata for connection {request.connection_id}")
        
        # Call the metadata extraction function
        success = await extract_metadata(request.connection_id)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        if success:
            logger.info(f"✅ Sync job {request.job_id} completed successfully in {duration:.2f}s")
            
            # Remove from active jobs
            if request.job_id in active_jobs:
                del active_jobs[request.job_id]
            
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
            logger.error(f"❌ Sync job {request.job_id} failed - metadata extraction returned False")
            
            # Remove from active jobs
            if request.job_id in active_jobs:
                del active_jobs[request.job_id]
            
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
        logger.error(f"💥 Sync job {request.job_id} failed with error: {error_msg}")
        
        # Remove from active jobs
        if request.job_id in active_jobs:
            del active_jobs[request.job_id]
        
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

@app.get("/jobs", response_model=Dict[str, Dict[str, Any]])
async def get_active_jobs():
    """Retorna lista de jobs ativos"""
    return active_jobs

@app.get("/jobs/{job_id}", response_model=Dict[str, Any])
async def get_job_status(job_id: str):
    """Retorna status de um job específico"""
    if job_id not in active_jobs:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    
    return active_jobs[job_id]

@app.delete("/jobs/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def cancel_job(job_id: str):
    """Cancela um job ativo (remove da lista de jobs ativos)"""
    if job_id not in active_jobs:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    
    logger.info(f"🛑 Cancelling job {job_id}")
    del active_jobs[job_id]

if __name__ == "__main__":
    import uvicorn
    
    # Get port from environment or default to 8007
    port = int(os.getenv("PORT", 8007))
    host = os.getenv("HOST", "0.0.0.0")
    
    logger.info(f"🚀 Starting Sync Service on {host}:{port}")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=True,
        log_level="info"
    )