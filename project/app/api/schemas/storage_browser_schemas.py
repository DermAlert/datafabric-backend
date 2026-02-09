"""
Schemas para Storage Browser API
=================================

Modelos Pydantic para navegação de arquivos e pastas
em Object Storage (S3/MinIO, GCS, Azure Blob Storage).
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


class StorageItemType(str, Enum):
    """Tipo de item no storage"""
    FILE = "file"
    FOLDER = "folder"


class StorageItem(BaseModel):
    """Representa um arquivo ou pasta no object storage"""
    name: str = Field(..., description="Nome do arquivo ou pasta")
    path: str = Field(..., description="Caminho/key completo no storage")
    type: StorageItemType = Field(..., description="Tipo: file ou folder")
    size: Optional[int] = Field(None, description="Tamanho em bytes (apenas arquivos)")
    last_modified: Optional[datetime] = Field(None, description="Data da última modificação")
    content_type: Optional[str] = Field(None, description="MIME type (apenas arquivos)")
    etag: Optional[str] = Field(None, description="ETag hash (apenas arquivos)")


class BucketInfo(BaseModel):
    """Representa um bucket/container no object storage"""
    name: str = Field(..., description="Nome do bucket/container")
    creation_date: Optional[datetime] = Field(None, description="Data de criação")
    region: Optional[str] = Field(None, description="Região/localização")


class BucketsResponse(BaseModel):
    """Resposta para listagem de buckets"""
    connection_id: int
    connection_name: str
    storage_type: str
    buckets: List[BucketInfo]
    total: int


class StorageBrowseResponse(BaseModel):
    """Resposta para navegação de objetos em um bucket"""
    connection_id: int
    connection_name: str
    storage_type: str
    bucket: str
    prefix: str = ""
    items: List[StorageItem]
    total_files: int
    total_folders: int
    is_truncated: bool = False
    next_continuation_token: Optional[str] = None


class StorageObjectDetail(BaseModel):
    """Informações detalhadas sobre um objeto específico"""
    connection_id: int
    bucket: str
    key: str
    size: int
    last_modified: Optional[datetime] = None
    content_type: Optional[str] = None
    etag: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    storage_class: Optional[str] = None


class DownloadUrlResponse(BaseModel):
    """Resposta com URL pré-assinada para download"""
    connection_id: int
    bucket: str
    key: str
    url: str
    expires_in: int = Field(..., description="Tempo de expiração da URL em segundos")


class StorageTestResult(BaseModel):
    """Resultado do teste de conexão de storage (usado internamente pelo service)"""
    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None
