from typing import Dict, Any, List, Optional
from minio import Minio
from minio.error import S3Error
from datetime import datetime
import io
import csv
import json
import pandas as pd

class MinioExtractor:
    """
    Extract metadata from MinIO/S3 buckets and objects.
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize the extractor with connection parameters.
        """
        self.endpoint = connection_params.get("endpoint", "localhost:9000")
        self.access_key = connection_params.get("access_key")
        self.secret_key = connection_params.get("secret_key")
        self.secure = connection_params.get("secure", False)
        self.region = connection_params.get("region")
        self.client = None
        
    def _get_client(self):
        """
        Get or create a connection to MinIO.
        """
        if not self.client:
            self.client = Minio(
                endpoint=self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
                region=self.region
            )
        return self.client
    
    def _close_client(self):
        """
        Close the MinIO connection.
        """
        # MinIO client doesn't require explicit closing
        self.client = None
    
    async def extract_catalogs(self) -> List[Dict[str, Any]]:
        """
        MinIO doesn't have catalogs in the same way as databases.
        We'll return just one catalog representing the MinIO server.
        """
        client = self._get_client()
        
        # Get server information if available
        server_info = {
            "endpoint": self.endpoint,
            "secure": self.secure,
            "region": self.region if self.region else "default"
        }
        
        return [{
            "catalog_name": self.endpoint,
            "catalog_type": "minio",
            "external_reference": self.endpoint,
            "properties": server_info
        }]
    
    async def extract_schemas(self) -> List[Dict[str, Any]]:
        """
        Extract buckets from MinIO as "schemas".
        """
        try:
            client = self._get_client()
            
            buckets = client.list_buckets()
            
            schemas = []
            for bucket in buckets:
                # Get bucket policy if available
                policy = {}
                try:
                    policy_str = client.get_bucket_policy(bucket.name)
                    if policy_str:
                        policy = {"policy": policy_str}
                except:
                    pass
                
                # Get bucket versioning status if available
                versioning = {}
                try:
                    versioning_config = client.get_bucket_versioning(bucket.name)
                    if versioning_config:
                        # Convert VersioningConfig object to serializable dictionary
                        versioning_dict = {}
                        if hasattr(versioning_config, 'status'):
                            versioning_dict['status'] = versioning_config.status
                        if hasattr(versioning_config, 'mfa_delete'):
                            versioning_dict['mfa_delete'] = versioning_config.mfa_delete
                        
                        versioning = {"versioning": versioning_dict}
                except:
                    pass
                
                schemas.append({
                    "schema_name": bucket.name,
                    "catalog_name": self.endpoint,
                    "external_reference": f"{self.endpoint}/{bucket.name}",
                    "properties": {
                        "creation_date": bucket.creation_date.isoformat() if hasattr(bucket, 'creation_date') else None,
                        **policy,
                        **versioning
                    }
                })
            
            return schemas
            
        except Exception as e:
            print(f"Error extracting schemas: {str(e)}")
            return []
    
    async def extract_tables(self, schema_name: str) -> List[Dict[str, Any]]:
        """
        Extract "tables" from a MinIO bucket.
        In MinIO, we'll treat top-level prefixes (folders) as tables.
        """
        try:
            client = self._get_client()
            
            # List objects with delimiter to get "folders"
            objects = client.list_objects(schema_name, recursive=False)
            
            # Track prefixes we've seen
            prefixes = set()
            tables = []
            
            # Process objects to extract prefixes
            for obj in objects:
                # If object name contains '/', it's in a prefix (folder)
                if '/' in obj.object_name:
                    prefix = obj.object_name.split('/')[0] + '/'
                    if prefix not in prefixes:
                        prefixes.add(prefix)
                        
                        # Count objects and total size in this prefix
                        prefix_objects = client.list_objects(schema_name, prefix=prefix, recursive=True)
                        object_count = 0
                        total_size = 0
                        
                        for prefix_obj in prefix_objects:
                            object_count += 1
                            total_size += prefix_obj.size
                        
                        tables.append({
                            "table_name": prefix.rstrip('/'),
                            "external_reference": f"{self.endpoint}/{schema_name}/{prefix}",
                            "table_type": "folder",
                            "estimated_row_count": object_count,
                            "total_size_bytes": total_size,
                            "last_analyzed": datetime.now(),  # Use datetime object directly, not string
                            "description": f"Folder prefix: {prefix}",
                            "properties": {
                                "object_count": object_count
                            }
                        })
                else:
                    # Top-level objects are treated as individual "tables"
                    tables.append({
                        "table_name": obj.object_name,
                        "external_reference": f"{self.endpoint}/{schema_name}/{obj.object_name}",
                        "table_type": "object",
                        "estimated_row_count": 1,
                        "total_size_bytes": obj.size,
                        "last_analyzed": obj.last_modified if hasattr(obj, 'last_modified') else None,  # Use datetime object directly
                        "description": f"Object: {obj.object_name}",
                        "properties": {
                            "content_type": obj.content_type if hasattr(obj, 'content_type') else None,
                            "etag": obj.etag if hasattr(obj, 'etag') else None
                        }
                    })
            
            return tables
            
        except Exception as e:
            print(f"Error extracting tables for bucket {schema_name}: {str(e)}")
            return []
    
    async def extract_columns(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """
        Extract "columns" for a specific table (prefix or object).
        For MinIO, columns can be:
        1. For objects: metadata and content structure
        2. For prefixes: common attributes of contained objects
        """
        try:
            client = self._get_client()
            
            # Check if table_name is a prefix or an object
            is_prefix = False
            try:
                # Try to get object stats
                client.stat_object(schema_name, table_name)
                is_prefix = False
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    # It might be a prefix
                    is_prefix = True
                else:
                    raise
            
            columns = []
            
            if is_prefix:
                # For prefixes, analyze contained objects
                prefix = table_name + '/'
                objects = client.list_objects(schema_name, prefix=prefix, recursive=False)
                
                # Sample a few objects to determine common structure
                sample_objects = []
                for obj in objects:
                    sample_objects.append(obj)
                    if len(sample_objects) >= 5:
                        break
                
                # Extract common metadata
                if sample_objects:
                    # Add basic metadata columns
                    columns.append({
                        "column_name": "object_name",
                        "external_reference": f"{self.endpoint}/{schema_name}/{table_name}/object_name",
                        "data_type": "string",
                        "is_nullable": False,
                        "column_position": 1,
                        "is_primary_key": True,
                        "is_unique": True,
                        "is_indexed": True,
                        "description": "Object name/key",
                        "sample_values": [obj.object_name.replace(prefix, '') for obj in sample_objects[:5]],
                        "properties": {}
                    })
                    
                    columns.append({
                        "column_name": "size",
                        "external_reference": f"{self.endpoint}/{schema_name}/{table_name}/size",
                        "data_type": "integer",
                        "is_nullable": False,
                        "column_position": 2,
                        "is_primary_key": False,
                        "is_unique": False,
                        "is_indexed": False,
                        "description": "Object size in bytes",
                        "sample_values": [obj.size for obj in sample_objects[:5]],
                        "properties": {}
                    })
                    
                    columns.append({
                        "column_name": "last_modified",
                        "external_reference": f"{self.endpoint}/{schema_name}/{table_name}/last_modified",
                        "data_type": "timestamp",
                        "is_nullable": False,
                        "column_position": 3,
                        "is_primary_key": False,
                        "is_unique": False,
                        "is_indexed": True,
                        "description": "Last modified timestamp",
                        "sample_values": [obj.last_modified.isoformat() if hasattr(obj, 'last_modified') else None for obj in sample_objects[:5]],
                        "properties": {}
                    })
                    
                    columns.append({
                        "column_name": "content_type",
                        "external_reference": f"{self.endpoint}/{schema_name}/{table_name}/content_type",
                        "data_type": "string",
                        "is_nullable": True,
                        "column_position": 4,
                        "is_primary_key": False,
                        "is_unique": False,
                        "is_indexed": False,
                        "description": "Content MIME type",
                        "sample_values": [obj.content_type if hasattr(obj, 'content_type') else None for obj in sample_objects[:5]],
                        "properties": {}
                    })
                    
                    columns.append({
                        "column_name": "etag",
                        "external_reference": f"{self.endpoint}/{schema_name}/{table_name}/etag",
                        "data_type": "string",
                        "is_nullable": True,
                        "column_position": 5,
                        "is_primary_key": False,
                        "is_unique": True,
                        "is_indexed": False,
                        "description": "Entity tag/checksum",
                        "sample_values": [obj.etag if hasattr(obj, 'etag') else None for obj in sample_objects[:5]],
                        "properties": {}
                    })
                    
                    # Try to analyze content structure for the first object if it's a known format
                    if sample_objects and hasattr(sample_objects[0], 'content_type'):
                        first_obj = sample_objects[0]
                        content_type = first_obj.content_type if hasattr(first_obj, 'content_type') else ''
                        
                        if content_type and ('csv' in content_type or 'text/plain' in content_type):
                            try:
                                # Get object data
                                response = client.get_object(schema_name, first_obj.object_name)
                                data = response.read(8192)  # Read first 8KB
                                response.close()
                                
                                # Try to parse as CSV
                                csv_data = csv.reader(io.StringIO(data.decode('utf-8')))
                                header = next(csv_data, None)
                                
                                if header:
                                    for i, col_name in enumerate(header):
                                        columns.append({
                                            "column_name": f"content.{col_name}",
                                            "external_reference": f"{self.endpoint}/{schema_name}/{table_name}/content/{col_name}",
                                            "data_type": "string",
                                            "is_nullable": True,
                                            "column_position": 6 + i,
                                            "is_primary_key": False,
                                            "is_unique": False,
                                            "is_indexed": False,
                                            "description": f"CSV column: {col_name}",
                                            "sample_values": [],
                                            "properties": {
                                                "content_column": True
                                            }
                                        })
                            except:
                                pass
                                
                        elif content_type and 'json' in content_type:
                            try:
                                # Get object data
                                response = client.get_object(schema_name, first_obj.object_name)
                                data = response.read(8192)  # Read first 8KB
                                response.close()
                                
                                # Try to parse as JSON
                                json_data = json.loads(data.decode('utf-8'))
                                
                                if isinstance(json_data, dict):
                                    for i, (key, value) in enumerate(json_data.items()):
                                        data_type = type(value).__name__
                                        columns.append({
                                            "column_name": f"content.{key}",
                                            "external_reference": f"{self.endpoint}/{schema_name}/{table_name}/content/{key}",
                                            "data_type": data_type,
                                            "is_nullable": True,
                                            "column_position": 6 + i,
                                            "is_primary_key": False,
                                            "is_unique": False,
                                            "is_indexed": False,
                                            "description": f"JSON field: {key}",
                                            "sample_values": [str(value)],
                                            "properties": {
                                                "content_column": True
                                            }
                                        })
                            except:
                                pass
            else:
                # For individual objects
                obj_stat = client.stat_object(schema_name, table_name)
                
                # Add basic metadata columns
                columns.append({
                    "column_name": "object_name",
                    "external_reference": f"{self.endpoint}/{schema_name}/{table_name}/object_name",
                    "data_type": "string",
                    "is_nullable": False,
                    "column_position": 1,
                    "is_primary_key": True,
                    "is_unique": True,
                    "is_indexed": True,
                    "description": "Object name/key",
                    "sample_values": [table_name],
                    "properties": {}
                })
                
                columns.append({
                    "column_name": "size",
                    "external_reference": f"{self.endpoint}/{schema_name}/{table_name}/size",
                    "data_type": "integer",
                    "is_nullable": False,
                    "column_position": 2,
                    "is_primary_key": False,
                    "is_unique": False,
                    "is_indexed": False,
                    "description": "Object size in bytes",
                    "sample_values": [obj_stat.size],
                    "properties": {}
                })
                
                columns.append({
                    "column_name": "last_modified",
                    "external_reference": f"{self.endpoint}/{schema_name}/{table_name}/last_modified",
                    "data_type": "timestamp",
                    "is_nullable": False,
                    "column_position": 3,
                    "is_primary_key": False,
                    "is_unique": False,
                    "is_indexed": True,
                    "description": "Last modified timestamp",
                    "sample_values": [obj_stat.last_modified.isoformat() if hasattr(obj_stat, 'last_modified') else None],
                    "properties": {}
                })
                
                columns.append({
                    "column_name": "content_type",
                    "external_reference": f"{self.endpoint}/{schema_name}/{table_name}/content_type",
                    "data_type": "string",
                    "is_nullable": True,
                    "column_position": 4,
                    "is_primary_key": False,
                    "is_unique": False,
                    "is_indexed": False,
                    "description": "Content MIME type",
                    "sample_values": [obj_stat.content_type if hasattr(obj_stat, 'content_type') else None],
                    "properties": {}
                })
                
                columns.append({
                    "column_name": "etag",
                    "external_reference": f"{self.endpoint}/{schema_name}/{table_name}/etag",
                    "data_type": "string",
                    "is_nullable": True,
                    "column_position": 5,
                    "is_primary_key": False,
                    "is_unique": True,
                    "is_indexed": False,
                    "description": "Entity tag/checksum",
                    "sample_values": [obj_stat.etag if hasattr(obj_stat, 'etag') else None],
                    "properties": {}
                })
                
                # Add user-defined metadata
                if hasattr(obj_stat, 'metadata') and obj_stat.metadata:
                    position = 6
                    for key, value in obj_stat.metadata.items():
                        if key.startswith('x-amz-meta-'):
                            meta_key = key[11:]  # Remove 'x-amz-meta-' prefix
                        else:
                            meta_key = key
                            
                        columns.append({
                            "column_name": f"metadata.{meta_key}",
                            "external_reference": f"{self.endpoint}/{schema_name}/{table_name}/metadata/{meta_key}",
                            "data_type": "string",
                            "is_nullable": True,
                            "column_position": position,
                            "is_primary_key": False,
                            "is_unique": False,
                            "is_indexed": False,
                            "description": f"User metadata: {meta_key}",
                            "sample_values": [value],
                            "properties": {
                                "metadata": True
                            }
                        })
                        position += 1
                
                # Try to analyze content structure if it's a known format
                content_type = obj_stat.content_type if hasattr(obj_stat, 'content_type') else ''
                
                if content_type and ('csv' in content_type or 'text/plain' in content_type):
                    try:
                        # Get object data
                        response = client.get_object(schema_name, table_name)
                        data = response.read(8192)  # Read first 8KB
                        response.close()
                        
                        # Try to parse as CSV
                        csv_data = csv.reader(io.StringIO(data.decode('utf-8')))
                        header = next(csv_data, None)
                        
                        if header:
                            position = len(columns) + 1
                            for i, col_name in enumerate(header):
                                columns.append({
                                    "column_name": f"content.{col_name}",
                                    "external_reference": f"{self.endpoint}/{schema_name}/{table_name}/content/{col_name}",
                                    "data_type": "string",
                                    "is_nullable": True,
                                    "column_position": position + i,
                                    "is_primary_key": False,
                                    "is_unique": False,
                                    "is_indexed": False,
                                    "description": f"CSV column: {col_name}",
                                    "sample_values": [],
                                    "properties": {
                                        "content_column": True
                                    }
                                })
                    except:
                        pass
                        
                elif content_type and 'json' in content_type:
                    try:
                        # Get object data
                        response = client.get_object(schema_name, table_name)
                        data = response.read(8192)  # Read first 8KB
                        response.close()
                        
                        # Try to parse as JSON
                        json_data = json.loads(data.decode('utf-8'))
                        
                        if isinstance(json_data, dict):
                            position = len(columns) + 1
                            for i, (key, value) in enumerate(json_data.items()):
                                data_type = type(value).__name__
                                columns.append({
                                    "column_name": f"content.{key}",
                                    "external_reference": f"{self.endpoint}/{schema_name}/{table_name}/content/{key}",
                                    "data_type": data_type,
                                    "is_nullable": True,
                                    "column_position": position + i,
                                    "is_primary_key": False,
                                    "is_unique": False,
                                    "is_indexed": False,
                                    "description": f"JSON field: {key}",
                                    "sample_values": [str(value)],
                                    "properties": {
                                        "content_column": True
                                    }
                                })
                    except:
                        pass
            
            return columns
            
        except Exception as e:
            print(f"Error extracting columns for {schema_name}/{table_name}: {str(e)}")
            return []
    
    async def close(self):
        """
        Close any open connections.
        """
        self._close_client()
