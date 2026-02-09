"""
Storage Browser API Routes
==========================

Endpoints para navegação de arquivos e pastas em conexões de Object Storage.

Suporta:
- Amazon S3 / S3-compatible (MinIO, DigitalOcean Spaces, Wasabi)
- Google Cloud Storage (GCS)
- Azure Blob Storage / Azure Data Lake Storage Gen2

Endpoints:
    GET /storage-browser/{connection_id}/buckets       - Listar buckets/containers
    GET /storage-browser/{connection_id}/objects        - Navegar arquivos e pastas
    GET /storage-browser/{connection_id}/object-info    - Detalhes de um arquivo
    GET /storage-browser/{connection_id}/download-url   - Gerar URL de download temporária
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
import logging

from app.database.session import get_db
from app.api.schemas.storage_browser_schemas import (
    BucketsResponse, StorageBrowseResponse, StorageObjectDetail, DownloadUrlResponse,
)
from app.services.storage.storage_browser_service import StorageBrowserService

router = APIRouter()

logger = logging.getLogger(__name__)


@router.get(
    "/{connection_id}/buckets",
    response_model=BucketsResponse,
    summary="Listar buckets/containers",
    description="Lista todos os buckets (S3/GCS) ou containers (Azure) disponíveis na conexão.",
)
async def list_buckets(
    connection_id: int,
    db: AsyncSession = Depends(get_db),
):
    """
    Lista todos os buckets/containers para uma conexão de object storage.
    
    Suporta: S3, MinIO, GCS, Azure Blob Storage, Delta Lake
    """
    return await StorageBrowserService(db).list_buckets(connection_id)


@router.get(
    "/{connection_id}/objects",
    response_model=StorageBrowseResponse,
    summary="Navegar arquivos e pastas",
    description="Navega pela hierarquia de pastas e arquivos em um bucket/container.",
)
async def list_objects(
    connection_id: int,
    bucket: Optional[str] = Query(
        None,
        description="Nome do bucket/container. Usa o padrão da conexão se não especificado."
    ),
    prefix: str = Query(
        "",
        description="Prefixo de caminho para filtrar (ex: 'pasta1/subpasta/')"
    ),
    delimiter: str = Query(
        "/",
        description="Delimitador para hierarquia de pastas"
    ),
    max_keys: int = Query(
        1000, ge=1, le=10000,
        description="Número máximo de itens a retornar"
    ),
    continuation_token: Optional[str] = Query(
        None,
        description="Token para paginação (retornado na resposta anterior)"
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Navega pelos arquivos e pastas em um bucket de object storage.
    
    Use o parâmetro `prefix` para navegar pela hierarquia:
    - prefix="" → raiz do bucket
    - prefix="images/" → conteúdo da pasta images
    - prefix="images/2024/" → conteúdo da subpasta 2024
    
    Os resultados são organizados com pastas primeiro, depois arquivos.
    
    Suporta: S3, MinIO, GCS, Azure Blob Storage, Delta Lake
    """
    return await StorageBrowserService(db).list_objects(
        connection_id=connection_id,
        bucket=bucket,
        prefix=prefix,
        delimiter=delimiter,
        max_keys=max_keys,
        continuation_token=continuation_token,
    )


@router.get(
    "/{connection_id}/object-info",
    response_model=StorageObjectDetail,
    summary="Detalhes de um arquivo",
    description="Retorna metadados detalhados de um arquivo específico.",
)
async def get_object_info(
    connection_id: int,
    bucket: str = Query(..., description="Nome do bucket/container"),
    key: str = Query(..., description="Caminho/key completo do objeto"),
    db: AsyncSession = Depends(get_db),
):
    """
    Retorna informações detalhadas de um objeto (arquivo).
    
    Inclui: tamanho, tipo de conteúdo, data de modificação, metadados customizados, etc.
    
    Suporta: S3, MinIO, GCS, Azure Blob Storage, Delta Lake
    """
    return await StorageBrowserService(db).get_object_info(connection_id, bucket, key)


@router.get(
    "/{connection_id}/download-url",
    response_model=DownloadUrlResponse,
    summary="Gerar URL de download",
    description="Gera uma URL pré-assinada temporária para download direto de um arquivo.",
)
async def get_download_url(
    connection_id: int,
    bucket: str = Query(..., description="Nome do bucket/container"),
    key: str = Query(..., description="Caminho/key completo do objeto"),
    expires: int = Query(
        3600, ge=60, le=86400,
        description="Tempo de expiração em segundos (60 a 86400)"
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Gera uma URL pré-assinada para download direto de um arquivo.
    
    A URL é temporária e expira após o tempo especificado.
    Ideal para permitir download pelo frontend sem expor credenciais.
    
    Suporta: S3, MinIO, GCS, Azure Blob Storage, Delta Lake
    """
    return await StorageBrowserService(db).generate_download_url(
        connection_id=connection_id,
        bucket=bucket,
        key=key,
        expires=expires,
    )
