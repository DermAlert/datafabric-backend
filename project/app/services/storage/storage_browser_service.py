"""
Storage Browser Service
=======================

Serviço para navegação de arquivos e pastas em conexões de Object Storage.
Suporta S3/MinIO, Google Cloud Storage e Azure Blob Storage.

Uso:
----
    service = StorageBrowserService(db)
    
    # Listar buckets
    buckets = await service.list_buckets(connection_id=1)
    
    # Navegar arquivos
    objects = await service.list_objects(connection_id=1, bucket="my-bucket", prefix="images/")
    
    # Info de um arquivo
    info = await service.get_object_info(connection_id=1, bucket="my-bucket", key="images/photo.jpg")
    
    # URL de download temporária
    url = await service.generate_download_url(connection_id=1, bucket="my-bucket", key="images/photo.jpg")
"""

import asyncio
import json
import base64
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, NoCredentialsError

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException, status

from app.database.utils import DatabaseService
from app.database.models import core
from app.services.infrastructure.credential_service import get_credential_service
from app.api.schemas.storage_browser_schemas import (
    StorageItem, StorageItemType, BucketInfo,
    BucketsResponse, StorageBrowseResponse, StorageObjectDetail, DownloadUrlResponse,
    StorageTestResult,
)

logger = logging.getLogger(__name__)

# Tipos de conexão que são object storage
OBJECT_STORAGE_TYPES = {"s3", "gcs", "azure_storage", "azure_blob", "deltalake"}


# =============================================================================
# PROVIDERS (Strategy Pattern)
# =============================================================================

class BaseStorageProvider(ABC):
    """Interface base para providers de object storage"""

    @abstractmethod
    async def list_buckets(self) -> List[BucketInfo]:
        """Lista todos os buckets/containers disponíveis"""
        ...

    @abstractmethod
    async def list_objects(
        self, bucket: str, prefix: str = "", delimiter: str = "/",
        max_keys: int = 1000, continuation_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """Lista objetos em um bucket com suporte a hierarquia de pastas"""
        ...

    @abstractmethod
    async def get_object_info(self, bucket: str, key: str) -> Dict[str, Any]:
        """Retorna metadados detalhados de um objeto"""
        ...

    @abstractmethod
    async def generate_download_url(self, bucket: str, key: str, expires: int = 3600) -> str:
        """Gera URL pré-assinada para download"""
        ...


class S3StorageProvider(BaseStorageProvider):
    """
    Provider para Amazon S3 e serviços S3-compatíveis (MinIO, DigitalOcean Spaces, Wasabi, etc.)
    
    Usa boto3 para todas as operações.
    """

    def __init__(self, connection_params: Dict[str, Any]):
        self.params = connection_params
        self.client = self._create_client()

    def _create_client(self):
        endpoint = self.params.get("endpoint")
        access_key = self.params.get("access_key")
        secret_key = self.params.get("secret_key")
        session_token = self.params.get("session_token")
        region = self.params.get("region", "us-east-1")
        secure = self.params.get("secure", True)
        path_style = self.params.get("path_style_access", False)

        # Construir endpoint URL
        endpoint_url = None
        if endpoint:
            if not endpoint.startswith("http"):
                protocol = "https" if secure else "http"
                endpoint_url = f"{protocol}://{endpoint}"
            else:
                endpoint_url = endpoint
                # Detectar secure pelo scheme
                parsed = urlparse(endpoint_url)
                if parsed.scheme == "http":
                    secure = False
                elif parsed.scheme == "https":
                    secure = True

        # Configuração boto3
        config_kwargs = {}
        if path_style:
            config_kwargs["s3"] = {"addressing_style": "path"}

        client_kwargs = {
            "service_name": "s3",
            "region_name": region,
        }

        if config_kwargs:
            client_kwargs["config"] = BotoConfig(**config_kwargs)

        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
            if not secure:
                client_kwargs["verify"] = False

        if access_key and secret_key:
            client_kwargs["aws_access_key_id"] = access_key
            client_kwargs["aws_secret_access_key"] = secret_key

        if session_token:
            client_kwargs["aws_session_token"] = session_token

        return boto3.client(**client_kwargs)

    async def list_buckets(self) -> List[BucketInfo]:
        response = await asyncio.to_thread(self.client.list_buckets)
        return [
            BucketInfo(
                name=b["Name"],
                creation_date=b.get("CreationDate"),
            )
            for b in response.get("Buckets", [])
        ]

    async def list_objects(
        self, bucket: str, prefix: str = "", delimiter: str = "/",
        max_keys: int = 1000, continuation_token: Optional[str] = None
    ) -> Dict[str, Any]:
        kwargs = {
            "Bucket": bucket,
            "Prefix": prefix,
            "Delimiter": delimiter,
            "MaxKeys": max_keys,
        }
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        response = await asyncio.to_thread(self.client.list_objects_v2, **kwargs)

        # Pastas (common prefixes)
        folders = []
        for cp in response.get("CommonPrefixes", []):
            folder_path = cp["Prefix"]
            folder_name = folder_path.rstrip("/").split("/")[-1]
            folders.append(StorageItem(
                name=folder_name,
                path=folder_path,
                type=StorageItemType.FOLDER,
            ))

        # Arquivos
        files = []
        for obj in response.get("Contents", []):
            key = obj["Key"]
            # Pular o próprio prefix se aparecer como objeto
            if key == prefix:
                continue
            name = key.split("/")[-1] if "/" in key else key
            if not name:  # Pular marcadores de diretório
                continue
            files.append(StorageItem(
                name=name,
                path=key,
                type=StorageItemType.FILE,
                size=obj.get("Size"),
                last_modified=obj.get("LastModified"),
                etag=obj.get("ETag", "").strip('"'),
            ))

        return {
            "folders": folders,
            "files": files,
            "is_truncated": response.get("IsTruncated", False),
            "next_continuation_token": response.get("NextContinuationToken"),
        }

    async def get_object_info(self, bucket: str, key: str) -> Dict[str, Any]:
        response = await asyncio.to_thread(
            self.client.head_object, Bucket=bucket, Key=key
        )
        return {
            "size": response.get("ContentLength", 0),
            "last_modified": response.get("LastModified"),
            "content_type": response.get("ContentType"),
            "etag": response.get("ETag", "").strip('"'),
            "metadata": response.get("Metadata", {}),
            "storage_class": response.get("StorageClass"),
        }

    async def generate_download_url(self, bucket: str, key: str, expires: int = 3600) -> str:
        url = await asyncio.to_thread(
            self.client.generate_presigned_url,
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires,
        )
        return url


class GCSStorageProvider(BaseStorageProvider):
    """
    Provider para Google Cloud Storage.
    
    Requer: pip install google-cloud-storage
    """

    def __init__(self, connection_params: Dict[str, Any]):
        self.params = connection_params
        self.client = self._create_client()

    def _create_client(self):
        try:
            from google.cloud import storage as gcs_storage
        except ImportError:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="Google Cloud Storage SDK não instalado. "
                       "Instale com: pip install google-cloud-storage"
            )

        auth_type = self.params.get("auth_type", "service_account")
        project_id = self.params.get("project_id")

        if auth_type == "service_account" and self.params.get("service_account_key"):
            try:
                from google.oauth2 import service_account as gcs_service_account
            except ImportError:
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail="Google Auth SDK não instalado. "
                           "Instale com: pip install google-auth"
                )

            key_data = self.params["service_account_key"]
            if isinstance(key_data, str):
                try:
                    key_data = json.loads(key_data)
                except json.JSONDecodeError:
                    # Tentar base64 decode
                    try:
                        key_data = json.loads(base64.b64decode(key_data))
                    except Exception:
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail="service_account_key inválido. Deve ser JSON ou JSON em base64."
                        )

            credentials = gcs_service_account.Credentials.from_service_account_info(key_data)
            return gcs_storage.Client(project=project_id, credentials=credentials)

        elif auth_type == "service_account" and self.params.get("service_account_key_path"):
            try:
                from google.oauth2 import service_account as gcs_service_account
            except ImportError:
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail="Google Auth SDK não instalado. "
                           "Instale com: pip install google-auth"
                )
            credentials = gcs_service_account.Credentials.from_service_account_file(
                self.params["service_account_key_path"]
            )
            return gcs_storage.Client(project=project_id, credentials=credentials)

        elif auth_type == "access_token" and self.params.get("access_token"):
            try:
                from google.oauth2.credentials import Credentials as OAuth2Credentials
            except ImportError:
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail="Google Auth SDK não instalado. "
                           "Instale com: pip install google-auth"
                )
            credentials = OAuth2Credentials(token=self.params["access_token"])
            return gcs_storage.Client(project=project_id, credentials=credentials)

        else:
            # Application Default Credentials
            return gcs_storage.Client(project=project_id)

    async def list_buckets(self) -> List[BucketInfo]:
        buckets_list = await asyncio.to_thread(lambda: list(self.client.list_buckets()))
        return [
            BucketInfo(
                name=b.name,
                creation_date=b.time_created,
                region=b.location,
            )
            for b in buckets_list
        ]

    async def list_objects(
        self, bucket: str, prefix: str = "", delimiter: str = "/",
        max_keys: int = 1000, continuation_token: Optional[str] = None
    ) -> Dict[str, Any]:
        bucket_obj = self.client.bucket(bucket)

        kwargs = {
            "prefix": prefix,
            "delimiter": delimiter,
            "max_results": max_keys,
        }
        if continuation_token:
            kwargs["page_token"] = continuation_token

        # list_blobs retorna um iterador; precisamos iterar para pegar os prefixes
        iterator = await asyncio.to_thread(
            lambda: bucket_obj.list_blobs(**kwargs)
        )

        # Coletar blobs (arquivos) - a iteração também popula iterator.prefixes
        blobs_list = await asyncio.to_thread(lambda: list(iterator))

        files = []
        for blob in blobs_list:
            key = blob.name
            if key == prefix:
                continue
            name = key.split("/")[-1] if "/" in key else key
            if not name:
                continue
            files.append(StorageItem(
                name=name,
                path=key,
                type=StorageItemType.FILE,
                size=blob.size,
                last_modified=blob.updated,
                content_type=blob.content_type,
                etag=blob.etag,
            ))

        # Coletar prefixes (pastas) - disponíveis após iteração
        folders = []
        for folder_prefix in iterator.prefixes:
            folder_name = folder_prefix.rstrip("/").split("/")[-1]
            folders.append(StorageItem(
                name=folder_name,
                path=folder_prefix,
                type=StorageItemType.FOLDER,
            ))

        next_token = getattr(iterator, 'next_page_token', None)

        return {
            "folders": folders,
            "files": files,
            "is_truncated": next_token is not None,
            "next_continuation_token": next_token,
        }

    async def get_object_info(self, bucket: str, key: str) -> Dict[str, Any]:
        bucket_obj = self.client.bucket(bucket)
        blob = bucket_obj.blob(key)
        await asyncio.to_thread(blob.reload)

        return {
            "size": blob.size or 0,
            "last_modified": blob.updated,
            "content_type": blob.content_type,
            "etag": blob.etag,
            "metadata": blob.metadata or {},
            "storage_class": blob.storage_class,
        }

    async def generate_download_url(self, bucket: str, key: str, expires: int = 3600) -> str:
        bucket_obj = self.client.bucket(bucket)
        blob = bucket_obj.blob(key)
        url = await asyncio.to_thread(
            blob.generate_signed_url,
            version="v4",
            expiration=timedelta(seconds=expires),
            method="GET",
        )
        return url


class AzureStorageProvider(BaseStorageProvider):
    """
    Provider para Azure Blob Storage / Azure Data Lake Storage Gen2.
    
    Requer: pip install azure-storage-blob
    Para service principal: pip install azure-identity
    """

    def __init__(self, connection_params: Dict[str, Any]):
        self.params = connection_params
        self.service_client = self._create_client()

    def _create_client(self):
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="Azure Storage SDK não instalado. "
                       "Instale com: pip install azure-storage-blob"
            )

        auth_type = self.params.get("auth_type", "access_key")
        storage_account = self.params.get("storage_account")
        endpoint_suffix = self.params.get("endpoint_suffix", "core.windows.net")
        account_url = f"https://{storage_account}.blob.{endpoint_suffix}"

        if auth_type == "access_key":
            return BlobServiceClient(
                account_url=account_url,
                credential=self.params.get("access_key"),
            )

        elif auth_type == "sas_token":
            sas_token = self.params.get("sas_token", "")
            if sas_token and not sas_token.startswith("?"):
                sas_token = f"?{sas_token}"
            return BlobServiceClient(account_url=f"{account_url}{sas_token}")

        elif auth_type == "service_principal":
            try:
                from azure.identity import ClientSecretCredential
            except ImportError:
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail="Azure Identity SDK não instalado. "
                           "Instale com: pip install azure-identity"
                )
            credential = ClientSecretCredential(
                tenant_id=self.params.get("tenant_id"),
                client_id=self.params.get("client_id"),
                client_secret=self.params.get("client_secret"),
            )
            return BlobServiceClient(account_url=account_url, credential=credential)

        else:
            # managed_identity ou default
            try:
                from azure.identity import DefaultAzureCredential
            except ImportError:
                raise HTTPException(
                    status_code=status.HTTP_501_NOT_IMPLEMENTED,
                    detail="Azure Identity SDK não instalado. "
                           "Instale com: pip install azure-identity"
                )
            credential = DefaultAzureCredential(
                managed_identity_client_id=self.params.get("managed_identity_client_id")
            )
            return BlobServiceClient(account_url=account_url, credential=credential)

    async def list_buckets(self) -> List[BucketInfo]:
        containers_list = await asyncio.to_thread(
            lambda: list(self.service_client.list_containers())
        )
        return [
            BucketInfo(
                name=c["name"],
                creation_date=c.get("last_modified"),
            )
            for c in containers_list
        ]

    async def list_objects(
        self, bucket: str, prefix: str = "", delimiter: str = "/",
        max_keys: int = 1000, continuation_token: Optional[str] = None
    ) -> Dict[str, Any]:
        container_client = self.service_client.get_container_client(bucket)

        items_list = await asyncio.to_thread(
            lambda: list(container_client.walk_blobs(
                name_starts_with=prefix,
                delimiter=delimiter,
            ))
        )

        folders = []
        files = []

        for item in items_list:
            # BlobPrefix (pasta) não tem atributo 'last_modified'
            # BlobProperties (arquivo) tem 'last_modified', 'size', etc.
            if not hasattr(item, 'last_modified') or item.last_modified is None:
                # É um BlobPrefix (pasta)
                item_name = item.name if hasattr(item, 'name') else str(item)
                folder_name = item_name.rstrip("/").split("/")[-1]
                if folder_name:
                    folders.append(StorageItem(
                        name=folder_name,
                        path=item_name,
                        type=StorageItemType.FOLDER,
                    ))
            else:
                # É um BlobProperties (arquivo)
                key = item.name
                if key == prefix:
                    continue
                name = key.split("/")[-1] if "/" in key else key
                if not name:
                    continue
                files.append(StorageItem(
                    name=name,
                    path=key,
                    type=StorageItemType.FILE,
                    size=item.size,
                    last_modified=item.last_modified,
                    content_type=item.content_settings.content_type if hasattr(item, 'content_settings') and item.content_settings else None,
                    etag=item.etag.strip('"') if item.etag else None,
                ))

        # Aplicar limite
        files = files[:max_keys]

        return {
            "folders": folders,
            "files": files,
            "is_truncated": False,
            "next_continuation_token": None,
        }

    async def get_object_info(self, bucket: str, key: str) -> Dict[str, Any]:
        container_client = self.service_client.get_container_client(bucket)
        blob_client = container_client.get_blob_client(key)
        props = await asyncio.to_thread(blob_client.get_blob_properties)

        content_type = None
        if props.content_settings:
            content_type = props.content_settings.content_type

        return {
            "size": props.size or 0,
            "last_modified": props.last_modified,
            "content_type": content_type,
            "etag": props.etag.strip('"') if props.etag else None,
            "metadata": props.metadata or {},
            "storage_class": props.blob_tier,
        }

    async def generate_download_url(self, bucket: str, key: str, expires: int = 3600) -> str:
        try:
            from azure.storage.blob import generate_blob_sas, BlobSasPermissions
        except ImportError:
            raise HTTPException(
                status_code=status.HTTP_501_NOT_IMPLEMENTED,
                detail="Azure Storage SDK não instalado."
            )

        storage_account = self.params.get("storage_account")
        access_key = self.params.get("access_key")
        endpoint_suffix = self.params.get("endpoint_suffix", "core.windows.net")

        if not access_key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Access key é necessária para gerar URLs de download no Azure Storage. "
                       "Conexões via SAS token ou service principal não suportam geração de URLs pré-assinadas."
            )

        sas_token = generate_blob_sas(
            account_name=storage_account,
            container_name=bucket,
            blob_name=key,
            account_key=access_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(seconds=expires),
        )

        return f"https://{storage_account}.blob.{endpoint_suffix}/{bucket}/{key}?{sas_token}"


# =============================================================================
# SERVIÇO PRINCIPAL
# =============================================================================

class StorageBrowserService:
    """
    Serviço para navegação de arquivos/pastas em conexões de Object Storage.
    
    Busca a conexão no banco, descriptografa as credenciais e usa o provider
    adequado (S3, GCS, Azure) para listar arquivos e pastas.
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self.db_service = DatabaseService(db)
        self.credential_service = get_credential_service()

    async def _get_connection_and_type(
        self, connection_id: int
    ) -> Tuple[core.DataConnection, core.ConnectionType]:
        """
        Busca a conexão e valida que é um tipo de object storage.
        
        Raises:
            HTTPException 404: Conexão não encontrada
            HTTPException 400: Tipo de conexão não é object storage
        """
        stmt = select(core.DataConnection).where(core.DataConnection.id == connection_id)
        connection = await self.db_service.scalars_first(stmt)
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Conexão com ID {connection_id} não encontrada"
            )

        stmt_ct = select(core.ConnectionType).where(
            core.ConnectionType.id == connection.connection_type_id
        )
        conn_type = await self.db_service.scalars_first(stmt_ct)
        if not conn_type:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Tipo de conexão não encontrado"
            )

        if conn_type.name.lower() not in OBJECT_STORAGE_TYPES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Tipo de conexão '{conn_type.name}' não é um object storage. "
                       f"Tipos suportados: {', '.join(sorted(OBJECT_STORAGE_TYPES))}"
            )

        return connection, conn_type

    def _normalize_params_for_provider(
        self, conn_type_name: str, params: Dict[str, Any]
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Normaliza os parâmetros de conexão para o provider adequado.
        
        Conexões Delta Lake usam parâmetros S3 com nomes diferentes,
        então fazemos o mapeamento aqui.
        """
        name = conn_type_name.lower()

        if name == "deltalake":
            # Mapear params Delta Lake → formato S3
            return "s3", {
                "endpoint": params.get("s3a_endpoint"),
                "access_key": params.get("s3a_access_key"),
                "secret_key": params.get("s3a_secret_key"),
                "bucket_name": params.get("bucket_name"),
                "secure": str(params.get("s3a_endpoint", "")).startswith("https"),
                "path_style_access": True,
                "region": params.get("region", "us-east-1"),
            }

        return name, params

    def _create_provider(
        self, conn_type_name: str, decrypted_params: Dict[str, Any]
    ) -> BaseStorageProvider:
        """Factory method para criar o provider adequado"""
        provider_name, normalized_params = self._normalize_params_for_provider(
            conn_type_name, decrypted_params
        )

        if provider_name in ("s3", "minio"):
            return S3StorageProvider(normalized_params)
        elif provider_name == "gcs":
            return GCSStorageProvider(normalized_params)
        elif provider_name in ("azure_storage", "azure_blob"):
            return AzureStorageProvider(normalized_params)
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Tipo de storage não suportado: {conn_type_name}"
            )

    async def _get_provider_for_connection(
        self, connection_id: int
    ) -> Tuple[BaseStorageProvider, core.DataConnection, core.ConnectionType, Dict[str, Any]]:
        """
        Busca a conexão, descriptografa credenciais e cria o provider.
        
        Returns:
            Tupla com (provider, connection, connection_type, decrypted_params)
        """
        connection, conn_type = await self._get_connection_and_type(connection_id)

        decrypted_params = self.credential_service.decrypt_for_use(
            connection.connection_params,
            connection_id=connection.id,
            purpose="storage_browse"
        )

        provider = self._create_provider(conn_type.name, decrypted_params)
        return provider, connection, conn_type, decrypted_params

    def _get_default_bucket(
        self, conn_type_name: str, params: Dict[str, Any]
    ) -> Optional[str]:
        """Retorna o bucket padrão configurado na conexão"""
        name = conn_type_name.lower()

        if name in ("s3", "minio", "deltalake"):
            return params.get("bucket_name")
        elif name == "gcs":
            return params.get("bucket")
        elif name in ("azure_storage", "azure_blob"):
            return params.get("container")

        return None

    def _validate_bucket_access(
        self, conn_type_name: str, decrypted_params: Dict[str, Any],
        requested_bucket: str
    ) -> None:
        """
        Valida se o bucket requisitado é permitido para esta conexão.
        
        Se a conexão tem um bucket específico configurado (bucket_name, bucket, container),
        só permite acesso a esse bucket. Isso evita que credenciais de uma conexão
        sejam usadas para acessar buckets de outras aplicações.
        
        Se a conexão NÃO tem bucket definido, permite acesso a qualquer bucket.
        
        Raises:
            HTTPException 403: Acesso negado ao bucket
        """
        configured_bucket = self._get_default_bucket(conn_type_name, decrypted_params)

        if configured_bucket and requested_bucket != configured_bucket:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Acesso negado ao bucket '{requested_bucket}'. "
                       f"Esta conexão está configurada para o bucket '{configured_bucket}'."
            )

    # =========================================================================
    # APIs PÚBLICAS
    # =========================================================================

    async def list_buckets(self, connection_id: int) -> BucketsResponse:
        """
        Lista buckets/containers disponíveis na conexão.
        
        Se a conexão tem um bucket específico configurado, retorna apenas esse bucket.
        Se não tem, lista todos os buckets acessíveis pelas credenciais.
        """
        provider, connection, conn_type, decrypted_params = await self._get_provider_for_connection(
            connection_id
        )

        # Se a conexão tem bucket definido, retornar apenas ele
        configured_bucket = self._get_default_bucket(conn_type.name, decrypted_params)

        if configured_bucket:
            return BucketsResponse(
                connection_id=connection.id,
                connection_name=connection.name,
                storage_type=conn_type.name,
                buckets=[BucketInfo(name=configured_bucket)],
                total=1,
            )

        # Sem bucket definido: listar todos os disponíveis
        try:
            buckets = await provider.list_buckets()
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"[StorageBrowser] Falha ao listar buckets para conexão {connection_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Falha ao listar buckets: {str(e)}"
            )

        return BucketsResponse(
            connection_id=connection.id,
            connection_name=connection.name,
            storage_type=conn_type.name,
            buckets=buckets,
            total=len(buckets),
        )

    async def list_objects(
        self,
        connection_id: int,
        bucket: Optional[str] = None,
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        continuation_token: Optional[str] = None,
    ) -> StorageBrowseResponse:
        """
        Navega pelos objetos em um bucket.
        
        Use o parâmetro `prefix` para navegar pela hierarquia de pastas.
        Se `bucket` não for especificado, usa o bucket padrão da conexão.
        Se a conexão tem bucket definido, só permite acesso a esse bucket.
        """
        provider, connection, conn_type, decrypted_params = (
            await self._get_provider_for_connection(connection_id)
        )

        # Se nenhum bucket especificado, usar o padrão da conexão
        if not bucket:
            bucket = self._get_default_bucket(conn_type.name, decrypted_params)
            if not bucket:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Nenhum bucket especificado e a conexão não tem bucket padrão configurado"
                )
        else:
            # Bucket foi especificado: validar se tem permissão
            self._validate_bucket_access(conn_type.name, decrypted_params, bucket)

        try:
            result = await provider.list_objects(
                bucket=bucket,
                prefix=prefix,
                delimiter=delimiter,
                max_keys=max_keys,
                continuation_token=continuation_token,
            )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                f"[StorageBrowser] Falha ao listar objetos para conexão {connection_id}, "
                f"bucket={bucket}, prefix={prefix}: {e}"
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Falha ao listar objetos: {str(e)}"
            )

        items = result["folders"] + result["files"]

        return StorageBrowseResponse(
            connection_id=connection.id,
            connection_name=connection.name,
            storage_type=conn_type.name,
            bucket=bucket,
            prefix=prefix,
            items=items,
            total_files=len(result["files"]),
            total_folders=len(result["folders"]),
            is_truncated=result.get("is_truncated", False),
            next_continuation_token=result.get("next_continuation_token"),
        )

    async def get_object_info(
        self, connection_id: int, bucket: str, key: str
    ) -> StorageObjectDetail:
        """Retorna metadados detalhados de um objeto específico"""
        provider, connection, conn_type, decrypted_params = await self._get_provider_for_connection(
            connection_id
        )

        self._validate_bucket_access(conn_type.name, decrypted_params, bucket)

        try:
            info = await provider.get_object_info(bucket, key)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                f"[StorageBrowser] Falha ao obter info do objeto para conexão {connection_id}: {e}"
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Falha ao obter informações do objeto: {str(e)}"
            )

        return StorageObjectDetail(
            connection_id=connection.id,
            bucket=bucket,
            key=key,
            **info,
        )

    async def generate_download_url(
        self, connection_id: int, bucket: str, key: str, expires: int = 3600
    ) -> DownloadUrlResponse:
        """Gera uma URL pré-assinada para download de um objeto"""
        provider, connection, conn_type, decrypted_params = await self._get_provider_for_connection(
            connection_id
        )

        self._validate_bucket_access(conn_type.name, decrypted_params, bucket)

        if expires < 60 or expires > 86400:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Expiração deve ser entre 60 e 86400 segundos (1 minuto a 24 horas)"
            )

        try:
            url = await provider.generate_download_url(bucket, key, expires)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                f"[StorageBrowser] Falha ao gerar URL de download para conexão {connection_id}: {e}"
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Falha ao gerar URL de download: {str(e)}"
            )

        return DownloadUrlResponse(
            connection_id=connection.id,
            bucket=bucket,
            key=key,
            url=url,
            expires_in=expires,
        )

    # =========================================================================
    # TESTE DE CONEXÃO
    # =========================================================================

    async def _do_storage_test(
        self, conn_type_name: str, connection_params: Dict[str, Any]
    ) -> StorageTestResult:
        """
        Lógica central de teste de conexão de storage.
        
        Verifica:
        1. Conectividade com o storage (list buckets)
        2. Se o bucket configurado existe (quando definido)
        3. Permissão de leitura no bucket (list objects)
        """
        try:
            provider = self._create_provider(conn_type_name, connection_params)
        except HTTPException as e:
            return StorageTestResult(
                success=False,
                message=f"Falha ao criar cliente: {e.detail}",
                details={"error": e.detail}
            )
        except Exception as e:
            return StorageTestResult(
                success=False,
                message=f"Falha ao criar cliente de storage: {str(e)}",
                details={"error": str(e)}
            )

        # 1) Testar conectividade listando buckets
        try:
            buckets = await provider.list_buckets()
            bucket_names = [b.name for b in buckets]
        except Exception as e:
            return StorageTestResult(
                success=False,
                message=f"Falha na conexão: {str(e)}",
                details={
                    "phase": "list_buckets",
                    "error": str(e),
                }
            )

        # 2) Verificar se o bucket configurado existe
        configured_bucket = self._get_default_bucket(conn_type_name, connection_params)

        if configured_bucket:
            if configured_bucket not in bucket_names:
                return StorageTestResult(
                    success=False,
                    message=f"Conexão OK, mas o bucket '{configured_bucket}' não foi encontrado.",
                    details={
                        "phase": "bucket_check",
                        "configured_bucket": configured_bucket,
                        "available_buckets": bucket_names,
                    }
                )

            # 3) Testar leitura no bucket
            try:
                result = await provider.list_objects(
                    bucket=configured_bucket, prefix="", delimiter="/", max_keys=5
                )
                total_items = len(result.get("folders", [])) + len(result.get("files", []))

                return StorageTestResult(
                    success=True,
                    message=f"Conexão OK. Bucket '{configured_bucket}' acessível com {total_items} item(s) na raiz.",
                    details={
                        "phase": "complete",
                        "bucket": configured_bucket,
                        "bucket_exists": True,
                        "items_in_root": total_items,
                        "total_buckets": len(bucket_names),
                    }
                )
            except Exception as e:
                return StorageTestResult(
                    success=False,
                    message=f"Conexão OK, bucket existe, mas falha ao listar objetos: {str(e)}",
                    details={
                        "phase": "list_objects",
                        "bucket": configured_bucket,
                        "error": str(e),
                    }
                )
        else:
            # Sem bucket configurado - apenas verifica conectividade
            return StorageTestResult(
                success=True,
                message=f"Conexão OK. {len(bucket_names)} bucket(s) encontrado(s).",
                details={
                    "phase": "complete",
                    "total_buckets": len(bucket_names),
                    "buckets": bucket_names[:20],  # Limitar a 20 nomes
                }
            )

