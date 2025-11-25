import os
from typing import Dict, Any, Optional
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from ..utils.logger import logger

class TrinoManager:
    def __init__(self):
        self.host = os.getenv("TRINO_HOST", "trino")
        self.port = int(os.getenv("TRINO_PORT", 8080))
        self.user = os.getenv("TRINO_USER", "admin")
        self.catalog = "system"  # Start with system catalog to manage others
        
    def get_connection(self):
        return connect(
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
        # Quote property names and values to avoid SQL injection and syntax errors
        # Trino property names in WITH clause should be identifiers or string literals
        # Values must be string literals
        props_list = []
        for k, v in properties.items():
            # Escape single quotes in value
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
                "connection-password": password
            }
            
        elif connection_type in ["deltalake", "delta"]:
            # Assuming generic Delta connector usage with Hive Metastore or S3
            # If using the docker-compose hive-metastore:
            metastore_uri = params.get("hive_metastore_uri", "thrift://hive-metastore:9083")
            
            props = {
                "hive.metastore.uri": metastore_uri,
                "delta.register-table-procedure.enabled": "true",
                "fs.native-s3.enabled": "true",
                "s3.region": params.get("region", params.get("aws_region", "us-east-1")),
                "hive.metastore": "thrift",
                "delta.enable-non-concurrent-writes": "true"
            }
            
            # Add S3/MinIO access keys if provided (common for Delta)
            # Trino Delta Lake connector uses s3.* properties for native S3 access
            if "s3a_access_key" in params:
                props["s3.aws-access-key"] = params["s3a_access_key"]
                logger.info(f"[TrinoManager] Mapped s3a_access_key -> s3.aws-access-key: {params['s3a_access_key'][:4]}***")
            if "s3a_secret_key" in params:
                props["s3.aws-secret-key"] = params["s3a_secret_key"]
                logger.info(f"[TrinoManager] Mapped s3a_secret_key -> s3.aws-secret-key: ***")
            if "endpoint_url" in params:
                 props["s3.endpoint"] = params["endpoint_url"]
                 logger.info(f"[TrinoManager] Mapped endpoint_url -> s3.endpoint: {params['endpoint_url']}")
            if "s3a_endpoint" in params: # Handling internal naming convention from old extractor
                 props["s3.endpoint"] = params["s3a_endpoint"]
                 logger.info(f"[TrinoManager] Mapped s3a_endpoint -> s3.endpoint: {params['s3a_endpoint']}")
                 
            # MinIO specific - often needs path style access
            props["s3.path-style-access"] = "true"
            
            logger.info(f"[TrinoManager] Final Delta catalog properties keys: {list(props.keys())}")
            
            return props
            
        elif connection_type in ["mysql"]:
             host = params.get("host", "localhost")
             port = params.get("port", 3306)
             db = params.get("database", "")
             user = params.get("username")
             password = params.get("password")
             
             # MySQL connector requires URL without database name
             # Database is accessed as a schema when querying
             return {
                "connection-url": f"jdbc:mysql://{host}:{port}",
                "connection-user": user,
                "connection-password": password
            }

        # Add other connectors as needed
        return {}

    def ensure_catalog_exists(self, connection_name: str, connection_type: str, params: Dict[str, Any]) -> bool:
        """
        Checks if catalog exists, if not creates it.
        """
        # Sanitize connection name for Trino identifier
        catalog_name = self._sanitize_identifier(connection_name)
        
        # Map connection type to Trino connector name
        connector_map = {
            "postgresql": "postgresql",
            "postgres": "postgresql",
            "deltalake": "delta_lake",
            "delta": "delta_lake",
            "mysql": "mysql",
            "minio": "delta_lake", # Assuming minio means delta tables on minio
            "s3": "delta_lake"
        }
        
        connector = connector_map.get(connection_type.lower())
        if not connector:
            logger.error(f"Unsupported connection type for Trino: {connection_type}")
            return False

        properties = self.get_catalog_properties(connection_type, params)
        if not properties:
            logger.error(f"Could not generate properties for {connection_type}")
            return False

        try:
            conn = self.get_connection()
            cur = conn.cursor()
            
            # Check if catalog exists
            cur.execute(f"SHOW CATALOGS LIKE '{catalog_name}'")
            result = cur.fetchall()
            
            if not result:
                logger.info(f"Creating Trino catalog '{catalog_name}' using {connector}")
                create_query = self.create_catalog_query(catalog_name, connector, properties)
                logger.debug(f"Executing query: {create_query}")
                try:
                    cur.execute(create_query)
                    # Verify creation
                    cur.execute(f"SHOW CATALOGS LIKE '{catalog_name}'")
                    if not cur.fetchall():
                         logger.error(f"Failed to create catalog {catalog_name} - Verification failed")
                         return False
                except Exception as create_error:
                    logger.error(f"Failed to create catalog {catalog_name}: {create_error}")
                    return False
            else:
                logger.debug(f"Catalog {catalog_name} already exists")
                # Optional: We could DROP and RECREATE if params changed, but for now assume persistence
                
            return True
            
        except Exception as e:
            logger.error(f"Error ensuring catalog {catalog_name} exists: {str(e)}")
            return False
        finally:
            if 'conn' in locals():
                conn.close()

    def drop_catalog(self, connection_name: str) -> bool:
        catalog_name = self._sanitize_identifier(connection_name)
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(f"DROP CATALOG IF EXISTS \"{catalog_name}\"")
            return True
        except Exception as e:
            logger.error(f"Error dropping catalog {catalog_name}: {str(e)}")
            return False
        finally:
            if 'conn' in locals():
                conn.close()

    def _sanitize_identifier(self, name: str) -> str:
        """
        Sanitizes a string to be used as a Trino identifier.
        """
        # Replace spaces and special chars with underscores, keep generic enough
        import re
        clean = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        if not clean: # Empty string or all special chars
            clean = "conn"
        if not clean[0].isalpha() and clean[0] != '_':
            clean = '_' + clean
        return clean.lower()
