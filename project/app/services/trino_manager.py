import os
import asyncio
from typing import Dict, Any, Optional
from urllib.parse import urlparse
import aiotrino
from minio import Minio
from ..utils.logger import logger


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
        
        # Internal MinIO/S3 config for storing metastore data
        self.internal_metastore_bucket = os.getenv("INTERNAL_METASTORE_BUCKET", "datafabric-metastore/")
        self.internal_s3_endpoint = os.getenv("INTERNAL_S3_ENDPOINT", "http://minio:9000")
        self.internal_s3_access_key = os.getenv("INTERNAL_S3_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minio"))
        self.internal_s3_secret_key = os.getenv("INTERNAL_S3_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123"))
        
        # Ensure internal metastore bucket exists (sync operation at init)
        self._ensure_internal_metastore_bucket()
    
    def _ensure_internal_metastore_bucket(self):
        """Create the internal metastore bucket if it doesn't exist"""
        try:
            parsed = urlparse(self.internal_s3_endpoint)
            endpoint = parsed.netloc if parsed.netloc else parsed.path
            secure = parsed.scheme == "https"
            
            client = Minio(
                endpoint=endpoint,
                access_key=self.internal_s3_access_key,
                secret_key=self.internal_s3_secret_key,
                secure=secure
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

    def get_catalog_properties(self, connection_type: str, params: Dict[str, Any]) -> Dict[str, str]:
        """
        Maps connection parameters to Trino connector properties.
        """
        connection_type = connection_type.lower()
        
        logger.info(f"[TrinoManager] Getting catalog properties for type: {connection_type}")
        logger.info(f"[TrinoManager] Input params keys: {list(params.keys())}")
        
        if connection_type in ["postgresql", "postgres"]:
            host = params.get("host", "localhost")
            port = params.get("port", 5432)
            db = params.get("database", "postgres")
            user = params.get("username")
            password = params.get("password")
            
            return {
                "connection-url": f"jdbc:postgresql://{host}:{port}/{db}",
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
                logger.info(f"[TrinoManager] Mapped access_key: {access_key[:4]}***")
            if secret_key:
                props["s3.aws-secret-key"] = secret_key
                logger.info(f"[TrinoManager] Mapped secret_key: ***")
            if endpoint:
                props["s3.endpoint"] = endpoint
                logger.info(f"[TrinoManager] Mapped endpoint: {endpoint}")
            
            logger.info(f"[TrinoManager] Final Delta catalog properties keys: {list(props.keys())}")
            
            return props
            
        elif connection_type in ["mysql"]:
             host = params.get("host", "localhost")
             port = params.get("port", 3306)
             db = params.get("database", "")
             user = params.get("username")
             password = params.get("password")
             
             return {
                "connection-url": f"jdbc:mysql://{host}:{port}",
                "connection-user": user,
                "connection-password": password,
                "case-insensitive-name-matching": "true",
            }

        return {}

    async def ensure_catalog_exists_async(self, connection_name: str, connection_type: str, params: Dict[str, Any], connection_id: Optional[int] = None) -> bool:
        """
        Checks if catalog exists, if not creates it (async version).
        
        Args:
            connection_name: Name of the connection
            connection_type: Type of connection (postgresql, mysql, delta, etc.)
            params: Connection parameters
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

        params_with_name = {**params, "_connection_name": connection_name}
        properties = self.get_catalog_properties(connection_type, params_with_name)
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
                # Catalog exists - drop and recreate to ensure fresh config
                logger.info(f"Dropping existing catalog '{catalog_name}' to recreate with new config")
                try:
                    await cur.execute(f"DROP CATALOG \"{catalog_name}\"")
                    await cur.fetchall()  # Consume result
                except Exception as drop_error:
                    logger.warning(f"Could not drop catalog {catalog_name}: {drop_error}")
            
            # Create catalog with current properties
            logger.info(f"Creating Trino catalog '{catalog_name}' using {connector}")
            create_query = self.create_catalog_query(catalog_name, connector, properties)
            logger.debug(f"Executing query: {create_query}")
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
