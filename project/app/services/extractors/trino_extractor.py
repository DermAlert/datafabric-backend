from typing import Dict, Any, List, Optional
from trino.dbapi import connect
from ..trino_manager import TrinoManager
from ...utils.logger import logger
import urllib3
from urllib.parse import urlparse
from minio import Minio

class TrinoExtractor:
    """
    Generic Extractor using Trino for metadata extraction.
    """
    
    def __init__(self, connection_params: Dict[str, Any], connection_type: str, connection_name: str = "default"):
        self.connection_params = connection_params
        self.connection_type = connection_type
        self.connection_name = connection_name
        self.trino_manager = TrinoManager()
        self.catalog = self.trino_manager._sanitize_identifier(connection_name)
        self._ensure_connection()

    def _ensure_connection(self):
        """
        Ensures the catalog exists in Trino.
        """
        self.trino_manager.ensure_catalog_exists(
            self.connection_name, 
            self.connection_type, 
            self.connection_params
        )

    def _get_connection(self):
        return connect(
            host=self.trino_manager.host,
            port=self.trino_manager.port,
            user=self.trino_manager.user,
            catalog=self.catalog,
            schema="information_schema",
        )

    async def _discover_delta_metadata(self):
        """
        For Delta/MinIO connections, scan the bucket for Delta tables and schemas.
        - If a folder contains _delta_log, it's a table (likely in default schema).
        - If a folder contains subfolders with _delta_log, it's a schema.
        """
        logger.info(f"Starting Delta metadata discovery for connection type: {self.connection_type}")
        
        if self.connection_type.lower() not in ["deltalake", "delta", "minio", "s3"]:
            logger.info(f"Skipping discovery: connection type {self.connection_type} not supported")
            return

        bucket_name = self.connection_params.get("bucket_name")
        logger.info(f"Discovery bucket: {bucket_name}")
        
        if not bucket_name:
            logger.warning("Skipping discovery: No bucket_name in connection params")
            return

        # Resolve MinIO/S3 credentials from various possible keys
        endpoint = self.connection_params.get("endpoint") or self.connection_params.get("endpoint_url") or self.connection_params.get("s3_endpoint") or self.connection_params.get("s3a_endpoint")
        access_key = self.connection_params.get("access_key") or self.connection_params.get("aws_access_key_id") or self.connection_params.get("minio_access_key") or self.connection_params.get("s3a_access_key")
        secret_key = self.connection_params.get("secret_key") or self.connection_params.get("aws_secret_access_key") or self.connection_params.get("minio_secret_key") or self.connection_params.get("s3a_secret_key")
        
        secure = self.connection_params.get("secure", False)
        if isinstance(secure, str):
            secure = secure.lower() == 'true'
            
        region = self.connection_params.get("region", "us-east-1")
        
        logger.info(f"MinIO params - Endpoint: {endpoint}, Secure: {secure}, Region: {region}")

        if not endpoint or not access_key or not secret_key:
            logger.warning("Skipping discovery: Missing MinIO credentials")
            return
            
        try:
            # Parse endpoint
            parsed_endpoint = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
            clean_endpoint = parsed_endpoint.netloc if parsed_endpoint.netloc else parsed_endpoint.path
            
            if parsed_endpoint.scheme == "https":
                secure = True
            elif parsed_endpoint.scheme == "http":
                secure = False
            
            if not secure:
                 urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

            import asyncio
            
            def scan_bucket():
                logger.info(f"Connecting to MinIO at {clean_endpoint} for bucket {bucket_name}")
                client = Minio(
                    endpoint=clean_endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=secure,
                    region=region
                )
                
                # Helper to check if _delta_log exists in a path
                def has_delta_log(prefix):
                    try:
                        path = f"{prefix.rstrip('/')}/_delta_log/"
                        # List one object to check existence
                        objs = list(client.list_objects(bucket_name, prefix=path, recursive=True))
                        found = len(objs) > 0
                        # logger.debug(f"Checking {path}: {'Found' if found else 'Not Found'}")
                        return found
                    except Exception as e:
                        logger.debug(f"Error checking delta log at {prefix}: {e}")
                        return False

                discovered = []
                
                try:
                    # List top level folders
                    top_level = list(client.list_objects(bucket_name, prefix="", recursive=False))
                    logger.info(f"Found {len(top_level)} top-level objects in bucket")
                    
                    for obj in top_level:
                        if obj.is_dir:
                            folder_name = obj.object_name.strip('/')
                            logger.info(f"Inspecting folder: {folder_name}")
                            
                            # Check if this folder is itself a table
                            if has_delta_log(folder_name):
                                logger.info(f"Found table at root: {folder_name}")
                                discovered.append({
                                    "type": "table",
                                    "schema": "default",
                                    "table": folder_name,
                                "location": f"s3://{bucket_name}/{folder_name}"
                            })
                        else:
                            # Treat as potential schema, check subfolders
                            logger.info(f"Checking if {folder_name} is a schema...")
                            sub_objs = client.list_objects(bucket_name, prefix=f"{folder_name}/", recursive=False)
                            is_schema = False
                            for sub in sub_objs:
                                if sub.is_dir:
                                    # sub.object_name is like "folder/sub/"
                                    parts = sub.object_name.strip('/').split('/')
                                    sub_name = parts[-1]
                                    full_path = sub.object_name
                                    
                                    # Check if subfolder is a table
                                    if has_delta_log(full_path.strip('/')):
                                        logger.info(f"Found table in schema {folder_name}: {sub_name}")
                                        is_schema = True
                                        discovered.append({
                                            "type": "table",
                                            "schema": folder_name,
                                            "table": sub_name,
                                            "location": f"s3://{bucket_name}/{folder_name}/{sub_name}"
                                        })
                            
                                if is_schema:
                                    logger.info(f"Identified schema: {folder_name}")
                                    discovered.append({
                                        "type": "schema",
                                        "schema": folder_name,
                                        "location": f"s3://{bucket_name}/{folder_name}"
                                    })
                except Exception as scan_err:
                    logger.error(f"Error during bucket scan: {scan_err}")
                    
                return discovered

            items = await asyncio.to_thread(scan_bucket)
            logger.info(f"Discovery found {len(items)} items")
            
            if not items:
                return

            conn = self._get_connection()
            cur = conn.cursor()
            
            for item in items:
                try:
                    if item["type"] == "schema":
                        schema_name = item["schema"]
                        location = item["location"]
                        # Register Schema
                        query = f"CREATE SCHEMA IF NOT EXISTS \"{self.catalog}\".\"{schema_name}\" WITH (location = '{location}')"
                        logger.info(f"Executing: {query}")
                        cur.execute(query)
                        
                    elif item["type"] == "table":
                        schema_name = item["schema"]
                        table_name = item["table"]
                        location = item["location"]
                        
                        # Ensure schema exists for default
                        if schema_name == 'default':
                             # default usually exists, but maybe not explicitly created with location in this catalog context?
                             # Usually Trino delta catalogs have a default schema.
                             pass
                        
                        # Register Table
                        # First check if table exists to avoid error noise
                        check_query = f"SHOW TABLES FROM \"{self.catalog}\".\"{schema_name}\" LIKE '{table_name}'"
                        cur.execute(check_query)
                        if not cur.fetchall():
                            logger.info(f"Registering table {schema_name}.{table_name} at {location}")
                            reg_query = f"CALL \"{self.catalog}\".system.register_table(schema_name => '{schema_name}', table_name => '{table_name}', table_location => '{location}')"
                            cur.execute(reg_query)
                        else:
                            logger.debug(f"Table {schema_name}.{table_name} already exists")

                except Exception as item_err:
                    logger.error(f"Error registering item {item}: {item_err}")
                    
        except Exception as e:
            logger.error(f"Error discovering Delta metadata: {e}")
        finally:
            if 'conn' in locals():
                conn.close()

    async def extract_catalogs(self) -> List[Dict[str, Any]]:
        """
        Return the catalog info.
        """
        return [{
            "catalog_name": self.catalog,
            "catalog_type": self.connection_type,
            "external_reference": self.catalog,
            "properties": {}
        }]

    async def extract_schemas(self) -> List[Dict[str, Any]]:
        """
        Extract schemas using Trino.
        """
        # Try to discover schemas from storage if this is a Delta/MinIO connection
        await self._discover_delta_metadata()

        try:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute(f"SHOW SCHEMAS FROM \"{self.catalog}\"")
            rows = cur.fetchall()
            
            schemas = []
            for row in rows:
                schema_name = row[0]
                if schema_name in ('information_schema', 'sys'): 
                    continue
                    
                schemas.append({
                    "schema_name": schema_name,
                    "catalog_name": self.catalog,
                    "external_reference": f"{self.catalog}.{schema_name}",
                    "properties": {}
                })
            return schemas
        except Exception as e:
            logger.error(f"Error extracting schemas via Trino: {str(e)}")
            return []
        finally:
            if 'conn' in locals():
                conn.close()

    async def extract_tables(self, schema_name: str) -> List[Dict[str, Any]]:
        """
        Extract tables from a schema.
        """
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            # Get tables
            cur.execute(f"SHOW TABLES FROM \"{self.catalog}\".\"{schema_name}\"")
            rows = cur.fetchall()
            
            tables = []
            for row in rows:
                table_name = row[0]
                
                table_type = "table" # default
                
                # Get row count
                row_count = 0
                try:
                    cur.execute(f"SELECT count(*) FROM \"{self.catalog}\".\"{schema_name}\".\"{table_name}\"")
                    count_res = cur.fetchone()
                    if count_res:
                        row_count = count_res[0]
                except Exception as e:
                    logger.warning(f"Could not get row count for {table_name}: {e}")

                tables.append({
                    "table_name": table_name,
                    "external_reference": f"{self.catalog}.{schema_name}.{table_name}",
                    "table_type": table_type,
                    "estimated_row_count": row_count,
                    "total_size_bytes": 0,
                    "description": None,
                    "properties": {}
                })
                
            return tables
        except Exception as e:
            logger.error(f"Error extracting tables via Trino for {schema_name}: {str(e)}")
            return []
        finally:
            if 'conn' in locals():
                conn.close()

    async def extract_columns(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """
        Extract columns for a specific table.
        """
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            # Use information_schema.columns for better detail
            query = f"""
                SELECT 
                    column_name, 
                    data_type, 
                    is_nullable, 
                    ordinal_position,
                    column_default
                FROM information_schema.columns 
                WHERE table_catalog = '{self.catalog}' 
                AND table_schema = '{schema_name}' 
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """
            cur.execute(query)
            rows = cur.fetchall()
            
            columns = []
            for row in rows:
                col_name = row[0]
                col_type = row[1]
                is_nullable = row[2] == 'YES'
                position = row[3]
                default_val = row[4]
                
                # Get statistics/samples
                sample_values = []
                try:
                    # Try to get samples
                    sample_query = f"SELECT DISTINCT \"{col_name}\" FROM \"{self.catalog}\".\"{schema_name}\".\"{table_name}\" WHERE \"{col_name}\" IS NOT NULL LIMIT 5"
                    cur.execute(sample_query)
                    samples = cur.fetchall()
                    sample_values = [str(s[0]) for s in samples if s[0] is not None]
                except Exception:
                    pass

                columns.append({
                    "column_name": col_name,
                    "external_reference": f"{table_name}.{col_name}",
                    "data_type": col_type,
                    "is_nullable": is_nullable,
                    "column_position": position,
                    "max_length": None, 
                    "numeric_precision": None,
                    "numeric_scale": None,
                    "is_primary_key": False, 
                    "is_unique": False,
                    "is_indexed": False,
                    "default_value": default_val,
                    "description": None,
                    "statistics": {},
                    "sample_values": sample_values,
                    "properties": {
                        "full_data_type": col_type
                    }
                })
            
            return columns
        except Exception as e:
            logger.error(f"Error extracting columns via Trino for {table_name}: {str(e)}")
            return []
        finally:
            if 'conn' in locals():
                conn.close()
                
    async def extract_table_data(self, schema_name: str, table_name: str, limit: Optional[int] = None, offset: int = 0) -> Dict[str, Any]:
        """
        Extract actual data from a table via Trino.
        """
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            query = f"SELECT * FROM \"{self.catalog}\".\"{schema_name}\".\"{table_name}\""
            if limit is not None:
                query += f" LIMIT {limit}"
            if offset > 0:
                # Trino supports OFFSET
                query += f" OFFSET {offset}" 
                
            cur.execute(query)
            rows = cur.fetchall()
            
            # Get columns from cursor description if available, else try to infer
            columns = []
            if cur.description:
                columns = [desc[0] for desc in cur.description]
            
            data = []
            for row in rows:
                if columns:
                    data.append(dict(zip(columns, row)))
                else:
                    data.append(list(row))
                
            return {
                "schema_name": schema_name,
                "table_name": table_name,
                "columns": [{"column_name": c} for c in columns],
                "data": data,
                "total_rows": len(data), # Approx
                "fetched_rows": len(data),
                "offset": offset
            }
        except Exception as e:
            logger.error(f"Error extracting data via Trino: {e}")
            return {
                "schema_name": schema_name,
                "table_name": table_name, 
                "error": str(e), 
                "data": []
            }
        finally:
            if 'conn' in locals():
                conn.close()

    async def close(self):
        # No persistent connection to close in this pattern, 
        # connections are created/closed per method or we could keep one.
        pass
