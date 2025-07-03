from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List, Union

class ISICSyncJobRequest(BaseModel):
    connection_id: int
    bucket_name: str = Field(default="isic-parquet", description="file")
    object_name: str = Field(default="metadata_latest.parquet", description="Nametadata")
    minio_endpoint: str = Field(default="minio:9000", description=" server endpoint")
    minio_access_key: str = Field(default="minio", description=" access key")
    minio_secret_key: str = Field(default="minio123", description="MinIO secret key")
    minio_secure: bool = Field(default=False, description=" MinIO connection")

class ISICSyncJobResponse(BaseModel):
    success: bool
    message: str
    job_id: Optional[int] = None
    details: Optional[Dict[str, Any]] = None

class ISICMetadataFilter(BaseModel):
    field: str
    operator: str = "eq" 
    value: Any

class ISICMetadataSort(BaseModel):
    field: str
    direction: str = "asc" 

class ISICMetadataSearchRequest(BaseModel):
    filters: Optional[List[ISICMetadataFilter]] = None
    sort: Optional[List[ISICMetadataSort]] = None
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)
    include_fields: Optional[List[str]] = None
    exclude_fields: Optional[List[str]] = None
    query: Optional[str] = None  # Free text search

class ISICMetadataSearchResponse(BaseModel):
    total: int
    offset: int
    limit: int
    items: List[Dict[str, Any]]
    filters_applied: List[Dict[str, Any]]
    execution_time_ms: float

class ISICDiagnosisStatistics(BaseModel):
    diagnosis: str
    count: int
    percentage: float
    is_malignant: bool
    examples: List[str]

class ISICDiagnosisResponse(BaseModel):
    total_diagnoses: int
    total_images: int
    diagnoses: List[ISICDiagnosisStatistics]
    execution_time_ms: float

class ISICCollectionResponse(BaseModel):
    id: str
    name: str
    image_count: int

class ISICCollectionsResponse(BaseModel):
    total_collections: int
    collections: List[ISICCollectionResponse]
    execution_time_ms: float

class ISICEquivalenceItem(BaseModel):
    field: str
    equivalent_fields: List[str]
    semantic_type: Optional[str] = None
    description: Optional[str] = None

class ISICEquivalenceResponse(BaseModel):
    total_mappings: int
    mappings: List[ISICEquivalenceItem]
    execution_time_ms: float