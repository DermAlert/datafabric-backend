from typing import Dict, Any, List, Tuple
import io
import csv
import json
import pandas as pd
from minio import Minio
from minio.error import S3Error

async def preview_minio_data(
    connection_params: Dict[str, Any],
    schema_name: str,
    table_name: str,
    limit: int = 100
) -> Tuple[List[List[Any]], List[str]]:
    """
    Get a preview of data from a MinIO/S3 bucket and object/prefix.
    
    Args:
        connection_params: Connection parameters
        schema_name: Bucket name
        table_name: Object key or prefix
        limit: Maximum number of rows to return
        
    Returns:
        Tuple of (rows, column_names)
    """
    try:
        # Extract connection parameters with defaults
        endpoint = connection_params.get("endpoint", "localhost:9000")
        access_key = connection_params.get("access_key")
        secret_key = connection_params.get("secret_key")
        secure = connection_params.get("secure", False)
        region = connection_params.get("region")
        
        # Create MinIO client
        client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region
        )
        
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
        
        if is_prefix:
            # For prefixes, list objects and show as a table
            prefix = table_name + '/' if not table_name.endswith('/') else table_name
            objects = client.list_objects(schema_name, prefix=prefix, recursive=False)
            
            # Convert to list for preview
            object_list = []
            column_names = ["Name", "Size", "Last Modified", "Content Type", "ETag"]
            
            for obj in objects:
                # Extract the name relative to the prefix
                relative_name = obj.object_name[len(prefix):] if obj.object_name.startswith(prefix) else obj.object_name
                
                # Skip empty names (happens with the prefix itself)
                if not relative_name:
                    continue
                
                object_list.append([
                    relative_name,
                    obj.size,
                    obj.last_modified.isoformat() if hasattr(obj, 'last_modified') else None,
                    obj.content_type if hasattr(obj, 'content_type') else None,
                    obj.etag if hasattr(obj, 'etag') else None
                ])
                
                if len(object_list) >= limit:
                    break
            
            return object_list, column_names
        else:
            # For individual objects, try to preview content based on type
            obj_stat = client.stat_object(schema_name, table_name)
            content_type = obj_stat.content_type if hasattr(obj_stat, 'content_type') else ''
            
            # Get object data
            response = client.get_object(schema_name, table_name)
            
            # Handle different content types
            if 'csv' in content_type or table_name.endswith('.csv') or 'text/plain' in content_type:
                # CSV file
                try:
                    # Use pandas to read CSV
                    df = pd.read_csv(response, nrows=limit)
                    column_names = df.columns.tolist()
                    rows = df.values.tolist()
                    response.close()
                    return rows, column_names
                except Exception as e:
                    # Fallback to manual CSV parsing if pandas fails
                    response.seek(0)
                    csv_data = csv.reader(io.TextIOWrapper(response, encoding='utf-8'))
                    header = next(csv_data, None)
                    rows = []
                    
                    if header:
                        for i, row in enumerate(csv_data):
                            rows.append(row)
                            if i >= limit - 1:
                                break
                    
                    response.close()
                    return rows, header or []
            
            elif 'json' in content_type or table_name.endswith('.json'):
                # JSON file
                try:
                    data = json.loads(response.read().decode('utf-8'))
                    response.close()
                    
                    if isinstance(data, list):
                        # List of objects/records
                        if data and isinstance(data[0], dict):
                            # Extract column names from first object
                            column_names = list(data[0].keys())
                            
                            # Convert to rows
                            rows = []
                            for i, item in enumerate(data):
                                if i >= limit:
                                    break
                                row = [item.get(col, None) for col in column_names]
                                rows.append(row)
                            
                            return rows, column_names
                        else:
                            # Simple list
                            return [[item] for item in data[:limit]], ["Value"]
                    
                    elif isinstance(data, dict):
                        # Single object
                        column_names = ["Key", "Value"]
                        rows = [[k, str(v)] for k, v in list(data.items())[:limit]]
                        return rows, column_names
                    
                    else:
                        # Scalar value
                        return [[data]], ["Value"]
                
                except Exception as e:
                    # If JSON parsing fails
                    return [[f"Error parsing JSON: {str(e)}"]], ["Error"]
            
            elif 'parquet' in content_type or table_name.endswith('.parquet'):
                # Parquet file
                try:
                    # Save to temporary file first
                    temp_file = io.BytesIO(response.read())
                    response.close()
                    
                    # Use pandas to read parquet
                    df = pd.read_parquet(temp_file)
                    df = df.head(limit)
                    
                    column_names = df.columns.tolist()
                    rows = df.values.tolist()
                    
                    return rows, column_names
                except Exception as e:
                    # If parquet parsing fails
                    return [[f"Error parsing Parquet: {str(e)}"]], ["Error"]
            
            elif any(ext in table_name.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp']):
                # Image file - return metadata
                response.close()
                
                column_names = ["Property", "Value"]
                rows = [
                    ["File Name", table_name],
                    ["Size", f"{obj_stat.size} bytes"],
                    ["Content Type", content_type],
                    ["Last Modified", obj_stat.last_modified.isoformat() if hasattr(obj_stat, 'last_modified') else "Unknown"],
                    ["ETag", obj_stat.etag if hasattr(obj_stat, 'etag') else "Unknown"]
                ]
                
                # Add user metadata if available
                if hasattr(obj_stat, 'metadata') and obj_stat.metadata:
                    for key, value in obj_stat.metadata.items():
                        if key.startswith('x-amz-meta-'):
                            meta_key = key[11:]  # Remove 'x-amz-meta-' prefix
                        else:
                            meta_key = key
                        rows.append([f"Metadata: {meta_key}", value])
                
                return rows, column_names
            
            else:
                # Binary or unknown format - return metadata
                response.close()
                
                column_names = ["Property", "Value"]
                rows = [
                    ["File Name", table_name],
                    ["Size", f"{obj_stat.size} bytes"],
                    ["Content Type", content_type],
                    ["Last Modified", obj_stat.last_modified.isoformat() if hasattr(obj_stat, 'last_modified') else "Unknown"],
                    ["ETag", obj_stat.etag if hasattr(obj_stat, 'etag') else "Unknown"]
                ]
                
                # Add user metadata if available
                if hasattr(obj_stat, 'metadata') and obj_stat.metadata:
                    for key, value in obj_stat.metadata.items():
                        if key.startswith('x-amz-meta-'):
                            meta_key = key[11:]  # Remove 'x-amz-meta-' prefix
                        else:
                            meta_key = key
                        rows.append([f"Metadata: {meta_key}", value])
                
                return rows, column_names
        
    except Exception as e:
        print(f"Error previewing MinIO data: {str(e)}")
        return [[f"Error: {str(e)}"]], ["Error"]
