import os
import asyncio
from typing import Dict, Any, Optional
import aiotrino
from minio import Minio
from ...utils.logger import logger
from .credential_service import get_credential_service
from .tunnel_manager import (
    get_tunnel_manager,
    has_tunnel_config,
    extract_tunnel_config,
    get_remote_address_from_params,
    TunnelError,
)


class TrinoManager:
    """
    Manager for Trino catalog operations.
    Uses aiotrino for native async I/O.
    """
    
    def __init__(self):
        self.host = os.getenv("TRINO_HOST", "trino")
        self.port = int(os.getenv("TRINO_PORT", 8080))
        self.user = os.getenv("TRINO_USER", "admin")
        self.catalog = "system"  # Start with system catalog to manage others
        
        # MinIO/S3 config for storing metastore data
        self.internal_metastore_bucket = os.getenv("INTERNAL_METASTORE_BUCKET", "datafabric-metastore/")
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minio123")
        self.minio_secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
        
        # Ensure internal metastore bucket exists (sync operation at init)
        self._ensure_internal_metastore_bucket()
    
    def _ensure_internal_metastore_bucket(self):
        """Create the internal metastore bucket if it doesn't exist"""
        try:
            client = Minio(
                endpoint=self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=self.minio_secure
            )
            
            if not client.bucket_exists(self.internal_metastore_bucket):
                client.make_bucket(self.internal_metastore_bucket)
                logger.info(f"Created internal metastore bucket: {self.internal_metastore_bucket}")
            else:
                logger.debug(f"Internal metastore bucket already exists: {self.internal_metastore_bucket}")
        except Exception as e:
            logger.warning(f"Could not ensure internal metastore bucket exists: {e}")
    
    async def get_connection(self):
        """Get an async Trino connection using aiotrino."""
        return aiotrino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema="information_schema",
        )

    def create_catalog_query(self, name: str, connector: str, properties: Dict[str, str]) -> str:
        """
        Generates the SQL for CREATE CATALOG.
        """
        props_list = []
        for k, v in properties.items():
            if v is None:
                continue
            v_str = str(v).replace("'", "''")
            props_list.append(f"\"{k}\" = '{v_str}'")
            
        props_str = ", ".join(props_list)
        return f"CREATE CATALOG IF NOT EXISTS \"{name}\" USING {connector} WITH ({props_str})"

    def get_catalog_properties(
        self, connection_type: str, params: Dict[str, Any], connection_id: Optional[int] = None
    ) -> Dict[str, str]:
        """
        Maps connection parameters to Trino connector properties.
        
        Se ``params`` contiver configuração de túnel SSH (``tunnel.enabled = true``),
        o endereço efetivo será substituído pelo endereço local do túnel,
        acessível pelo Trino via o hostname Docker do backend.
        
        SECURITY: Este método recebe parâmetros descriptografados.
        As credenciais são descriptografadas sob demanda pelo CredentialService.
        """
        connection_type = connection_type.lower()
        credential_service = get_credential_service()
        
        # Descriptografa credenciais se estiverem criptografadas
        decrypted_params = credential_service.decrypt_for_use(
            params, 
            purpose="trino_catalog_creation"
        )
        params = decrypted_params
        
        logger.info(f"[TrinoManager] Getting catalog properties for type: {connection_type}")
        # SEGURANÇA: Nunca logar valores de parâmetros, apenas as chaves
        logger.info(f"[TrinoManager] Input params keys: {list(params.keys())}")
        
        # Resolver endereço efetivo (tunnel ou direto)
        effective_host, effective_port = self._resolve_effective_address(
            connection_type, params, connection_id
        )
        
        if connection_type in ["postgresql", "postgres"]:
            db = params.get("database", "postgres")
            user = params.get("username")
            password = params.get("password")
            
            return {
                "connection-url": f"jdbc:postgresql://{effective_host}:{effective_port}/{db}",
                "connection-user": user,
                "connection-password": password,
                "case-insensitive-name-matching": "true",
            }
            
        elif connection_type in ["deltalake", "delta"]:
            region = params.get("region", params.get("aws_region", "us-east-1"))
            
            access_key = params.get("s3a_access_key") or params.get("access_key") or params.get("aws_access_key_id")
            secret_key = params.get("s3a_secret_key") or params.get("secret_key") or params.get("aws_secret_access_key")
            endpoint = params.get("endpoint_url") or params.get("s3a_endpoint") or params.get("endpoint")
            
            catalog_name = self._sanitize_identifier(params.get("_connection_name", "default"))
            internal_metastore_path = f"s3a://{self.internal_metastore_bucket}/{catalog_name}/"
            
            logger.info(f"[TrinoManager] Using internal metastore at: {internal_metastore_path}")
            
            # Se túnel ativo, substituir endpoint pelo endereço tunelado
            if has_tunnel_config(params):
                tunnel_manager = get_tunnel_manager()
                # Reconstruir endpoint com host/porta do túnel (para Trino, outro container)
                trino_host = tunnel_manager.get_trino_tunnel_host()
                scheme = "https" if str(endpoint or "").startswith("https") else "http"
                endpoint = f"{scheme}://{trino_host}:{effective_port}"
                logger.info(f"[TrinoManager] S3 endpoint via tunnel: {endpoint}")
            
            props = {
                "hive.metastore": "file",
                "hive.metastore.catalog.dir": internal_metastore_path,
                "delta.register-table-procedure.enabled": "true",
                "fs.native-s3.enabled": "true",
                "s3.region": region,
                "delta.enable-non-concurrent-writes": "true",
                "s3.path-style-access": "true",
            }
            
            if access_key:
                props["s3.aws-access-key"] = access_key
                # SEGURANÇA: Não logar credenciais, nem parcialmente
                logger.info(f"[TrinoManager] S3 access_key configured: [REDACTED]")
            if secret_key:
                props["s3.aws-secret-key"] = secret_key
                logger.info(f"[TrinoManager] S3 secret_key configured: [REDACTED]")
            if endpoint:
                props["s3.endpoint"] = endpoint
                logger.info(f"[TrinoManager] S3 endpoint configured: {endpoint}")
            
            logger.info(f"[TrinoManager] Final Delta catalog properties keys: {list(props.keys())}")
            
            return props
            
        elif connection_type in ["mysql"]:
             db = params.get("database", "")
             user = params.get("username")
             password = params.get("password")
             
             return {
                "connection-url": f"jdbc:mysql://{effective_host}:{effective_port}",
                "connection-user": user,
                "connection-password": password,
                "case-insensitive-name-matching": "true",
            }

        return {}
    
    def _resolve_effective_address(
        self,
        connection_type: str,
        params: Dict[str, Any],
        connection_id: Optional[int] = None,
    ) -> tuple:
        """
        Resolve o endereço efetivo para o catálogo Trino.
        
        Se houver túnel SSH configurado e ``connection_id`` for fornecido,
        verifica se já existe túnel ativo e retorna o endereço tunelado
        (hostname Docker do backend + porta local do túnel).
        
        Caso contrário, retorna o host:port original dos params.
        
        Returns:
            Tupla (host, port) efetivos.
        """
        # Endereço original
        original_host, original_port = get_remote_address_from_params(connection_type, params)
        
        if not has_tunnel_config(params):
            return original_host, original_port
        
        if connection_id is None:
            logger.warning(
                "[TrinoManager] Tunnel config present but no connection_id provided. "
                "Cannot resolve tunnel address. Using original address."
            )
            return original_host, original_port
        
        tunnel_manager = get_tunnel_manager()
        tunnel_port = tunnel_manager.get_tunnel_port(connection_id)
        
        if tunnel_port:
            # Túnel já ativo: para Trino (outro container) usar hostname Docker
            trino_host = tunnel_manager.get_trino_tunnel_host()
            logger.info(
                f"[TrinoManager] Using tunnel for connection {connection_id}: "
                f"{trino_host}:{tunnel_port} (original: {original_host}:{original_port})"
            )
            return trino_host, tunnel_port
        
        # Tunnel configurado mas não ativo — retornar original
        # (o chamador deve iniciar o tunnel antes)
        logger.warning(
            f"[TrinoManager] Tunnel config present for connection {connection_id} "
            f"but no active tunnel found. Using original address."
        )
        return original_host, original_port

    async def ensure_catalog_exists_async(self, connection_name: str, connection_type: str, params: Dict[str, Any], connection_id: Optional[int] = None) -> bool:
        """
        Checks if catalog exists, if not creates it (async version).
        
        Se ``params`` contiver configuração de túnel SSH, o túnel será
        criado automaticamente antes da criação do catálogo.
        
        Args:
            connection_name: Name of the connection
            connection_type: Type of connection (postgresql, mysql, delta, etc.)
            params: Connection parameters (may include tunnel config)
            connection_id: Optional connection ID to ensure unique catalog names
            
        Returns:
            True if catalog was created/exists, False otherwise
        """
        if connection_id is not None:
            catalog_name = self.generate_catalog_name(connection_name, connection_id)
        else:
            catalog_name = self._sanitize_identifier(connection_name)
        
        connector_map = {
            "postgresql": "postgresql",
            "postgres": "postgresql",
            "deltalake": "delta_lake",
            "delta": "delta_lake",
            "mysql": "mysql",
            "minio": "delta_lake",
            "s3": "delta_lake"
        }
        
        connector = connector_map.get(connection_type.lower())
        if not connector:
            logger.error(f"Unsupported connection type for Trino: {connection_type}")
            return False

        # Iniciar túnel SSH se configurado (antes de gerar properties)
        if has_tunnel_config(params) and connection_id is not None:
            try:
                tunnel_config = extract_tunnel_config(params)
                remote_host, remote_port = get_remote_address_from_params(connection_type, params)
                tunnel_manager = get_tunnel_manager()
                await tunnel_manager.get_or_create_tunnel(
                    connection_id=connection_id,
                    tunnel_config=tunnel_config,
                    remote_host=remote_host,
                    remote_port=remote_port,
                )
            except TunnelError as e:
                logger.error(f"Failed to create SSH tunnel for connection {connection_id}: {e}")
                return False

        params_with_name = {**params, "_connection_name": connection_name}
        properties = self.get_catalog_properties(connection_type, params_with_name, connection_id)
        if not properties:
            logger.error(f"Could not generate properties for {connection_type}")
            return False

        try:
            conn = await self.get_connection()
            cur = await conn.cursor()
            
            # Check if catalog exists
            await cur.execute(f"SHOW CATALOGS LIKE '{catalog_name}'")
            result = await cur.fetchall()
            
            if result:
                # Se a conexão usa túnel SSH, o catálogo pode estar apontando para
                # uma porta de túnel antiga (ex: após restart do backend).
                # Nesse caso, dropar e recriar para usar a porta atual.
                if has_tunnel_config(params) and connection_id is not None:
                    logger.info(
                        f"Catalog '{catalog_name}' exists but connection uses SSH tunnel — "
                        f"dropping and recreating to ensure correct tunnel port"
                    )
                    try:
                        await cur.execute(f"DROP CATALOG IF EXISTS \"{catalog_name}\"")
                        await cur.fetchall()
                    except Exception as drop_err:
                        logger.warning(f"Failed to drop stale catalog '{catalog_name}': {drop_err}")
                else:
                    # Sem túnel: catálogo existente pode ser reutilizado
                    logger.info(f"Catalog '{catalog_name}' already exists, skipping creation")
                    return True
            
            # Create catalog (only if it doesn't exist)
            logger.info(f"Creating Trino catalog '{catalog_name}' using {connector}")
            create_query = self.create_catalog_query(catalog_name, connector, properties)
            # SEGURANÇA: Não logar a query pois contém credenciais em texto claro
            logger.debug(f"Executing CREATE CATALOG for '{catalog_name}' (credentials redacted)")
            try:
                await cur.execute(create_query)
                await cur.fetchall()  # Consume result
                
                # Verify creation
                await cur.execute(f"SHOW CATALOGS LIKE '{catalog_name}'")
                if not await cur.fetchall():
                     logger.error(f"Failed to create catalog {catalog_name} - Verification failed")
                     return False
            except Exception as create_error:
                logger.error(f"Failed to create catalog {catalog_name}: {create_error}")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error ensuring catalog {catalog_name} exists: {str(e)}")
            return False
        finally:
            if 'conn' in locals():
                await conn.close()

    def ensure_catalog_exists(self, connection_name: str, connection_type: str, params: Dict[str, Any], connection_id: Optional[int] = None) -> bool:
        """
        Synchronous wrapper for ensure_catalog_exists_async.
        This is kept for backwards compatibility with code that calls this synchronously.
        It runs the async version in a new event loop if needed.
        """
        try:
            # Try to get running loop - if we're already in async context
            loop = asyncio.get_running_loop()
            # We're in an async context, create a task
            # This shouldn't happen in normal flow, but handle it
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(
                    asyncio.run,
                    self.ensure_catalog_exists_async(connection_name, connection_type, params, connection_id)
                )
                return future.result()
        except RuntimeError:
            # No running loop, safe to use asyncio.run
            return asyncio.run(
                self.ensure_catalog_exists_async(connection_name, connection_type, params, connection_id)
            )

    async def flush_metadata_cache(
        self,
        catalog_name: str,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> bool:
        """
        Flush Trino's Delta Lake metadata cache for a catalog, schema, or table.

        After external writes (e.g. delta-rs / Spark / Airflow DAGs) modify a
        Delta table, Trino may still hold a stale view of the Parquet files.
        Calling this forces Trino to re-read the Delta transaction log so
        subsequent queries see the latest data.

        Args:
            catalog_name: The Trino catalog to flush.
            schema_name: Optional schema to narrow the flush scope.
            table_name:  Optional table to narrow the flush scope (requires schema_name).

        Returns:
            True if flush succeeded, False otherwise.
        """
        try:
            conn = await self.get_connection()
            cur = await conn.cursor()

            if schema_name and table_name:
                query = (
                    f'CALL "{catalog_name}".system.flush_metadata_cache('
                    f"schema_name => '{schema_name}', "
                    f"table_name => '{table_name}')"
                )
            elif schema_name:
                query = (
                    f'CALL "{catalog_name}".system.flush_metadata_cache('
                    f"schema_name => '{schema_name}')"
                )
            else:
                query = f'CALL "{catalog_name}".system.flush_metadata_cache()'

            logger.info(
                f"[TrinoManager] Flushing metadata cache for catalog='{catalog_name}' "
                f"schema='{schema_name}' table='{table_name}'"
            )
            await cur.execute(query)
            await cur.fetchall()
            logger.info("[TrinoManager] Metadata cache flushed successfully")
            return True
        except Exception as e:
            logger.warning(f"[TrinoManager] Failed to flush metadata cache: {e}")
            return False
        finally:
            if "conn" in locals():
                await conn.close()

    async def drop_catalog_async(self, connection_name: str) -> bool:
        """Drop a catalog (async version)."""
        catalog_name = self._sanitize_identifier(connection_name)
        try:
            conn = await self.get_connection()
            cur = await conn.cursor()
            await cur.execute(f"DROP CATALOG IF EXISTS \"{catalog_name}\"")
            await cur.fetchall()  # Consume result
            return True
        except Exception as e:
            logger.error(f"Error dropping catalog {catalog_name}: {str(e)}")
            return False
        finally:
            if 'conn' in locals():
                await conn.close()

    def drop_catalog(self, connection_name: str) -> bool:
        """Synchronous wrapper for drop_catalog_async."""
        try:
            loop = asyncio.get_running_loop()
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(
                    asyncio.run,
                    self.drop_catalog_async(connection_name)
                )
                return future.result()
        except RuntimeError:
            return asyncio.run(self.drop_catalog_async(connection_name))

    def _sanitize_identifier(self, name: str, connection_id: Optional[int] = None) -> str:
        """
        Sanitizes a string to be used as a Trino identifier.
        """
        import re
        clean = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        if not clean:
            clean = "conn"
        if not clean[0].isalpha() and clean[0] != '_':
            clean = '_' + clean
        
        if connection_id is not None:
            clean = f"{clean}_{connection_id}"
        
        return clean.lower()
    
    def generate_catalog_name(self, connection_name: str, connection_id: int) -> str:
        """
        Generates a unique catalog name for Trino.
        """
        return self._sanitize_identifier(connection_name, connection_id)
