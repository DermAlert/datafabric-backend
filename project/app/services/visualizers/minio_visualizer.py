from typing import Dict, Any, List, Tuple, Optional
from minio import Minio
from minio.error import S3Error
import io
import csv
import json
import pandas as pd
from datetime import datetime, date

from ...api.schemas.data_visualization_schemas import Filter, Sort, FilterOperator, SortDirection

async def visualize_minio_data(
    connection_params: Dict[str, Any],
    schema_name: str,
    table_name: str,
    page: int = 1,
    page_size: int = 50,
    filters: Optional[List[Filter]] = None,
    sort_by: Optional[List[Sort]] = None,
    selected_columns: Optional[List[str]] = None,
    column_info: Optional[Dict[str, Any]] = None
) -> Tuple[int, List[List[Any]], List[str]]:
    """
    Get virtualized data from a MinIO/S3 bucket and object/prefix with pagination, filtering, and sorting.
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
            
            # Convert to list for visualization
            object_list = []
            all_objects = []
            
            # Define default columns
            default_columns = ["Name", "Size", "Last Modified", "Content Type", "ETag"]
            column_names = selected_columns if selected_columns else default_columns
            
            # Collect all objects
            for obj in objects:
                # Extract the name relative to the prefix
                relative_name = obj.object_name[len(prefix):] if obj.object_name.startswith(prefix) else obj.object_name
                
                # Skip empty names (happens with the prefix itself)
                if not relative_name:
                    continue
                
                # Create object data dictionary
                obj_data = {
                    "Name": relative_name,
                    "Size": obj.size,
                    "Last Modified": obj.last_modified.isoformat() if hasattr(obj, 'last_modified') else None,
                    "Content Type": obj.content_type if hasattr(obj, 'content_type') else None,
                    "ETag": obj.etag if hasattr(obj, 'etag') else None
                }
                
                all_objects.append(obj_data)
            
            # Apply filters if provided
            filtered_objects = all_objects
            if filters:
                filtered_objects = []
                for obj in all_objects:
                    include = True
                    
                    for filter_item in filters:
                        column = filter_item.column
                        if column not in obj:
                            continue
                            
                        value = obj[column]
                        
                        if filter_item.operator == FilterOperator.eq and value != filter_item.value:
                            include = False
                            break
                        elif filter_item.operator == FilterOperator.neq and value == filter_item.value:
                            include = False
                            break
                        elif filter_item.operator == FilterOperator.gt and not (value > filter_item.value):
                            include = False
                            break
                        elif filter_item.operator == FilterOperator.gte and not (value >= filter_item.value):
                            include = False
                            break
                        elif filter_item.operator == FilterOperator.lt and not (value < filter_item.value):
                            include = False
                            break
                        elif filter_item.operator == FilterOperator.lte and not (value <= filter_item.value):
                            include = False
                            break
                        elif filter_item.operator == FilterOperator.like and not (isinstance(value, str) and filter_item.value in value):
                            include = False
                            break
                        elif filter_item.operator == FilterOperator.ilike and not (isinstance(value, str) and filter_item.value.lower() in value.lower()):
                            include = False
                            break
                        elif filter_item.operator == FilterOperator.in_list and value not in filter_item.values:
                            include = False
                            break
                        elif filter_item.operator == FilterOperator.not_in and value in filter_item.values:
                            include = False
                            break
                        elif filter_item.operator == FilterOperator.is_null and value is not None:
                            include = False
                            break
                        elif filter_item.operator == FilterOperator.is_not_null and value is None:
                            include = False
                            break
                        elif filter_item.operator == FilterOperator.between and (
                            not filter_item.values or 
                            len(filter_item.values) < 2 or 
                            not (filter_item.values[0] <= value <= filter_item.values[1])
                        ):
                            include = False
                            break
                    
                    if include:
                        filtered_objects.append(obj)
            
            # Apply sorting if provided
            if sort_by:
                for sort_item in reversed(sort_by):  # Apply in reverse order for stable sort
                    column = sort_item.column
                    reverse = sort_item.direction == SortDirection.desc
                    
                    # Sort with None values at the end
                    filtered_objects.sort(
                        key=lambda x: (x.get(column) is None, x.get(column)),
                        reverse=reverse
                    )
            
            # Calculate total count
            total_count = len(filtered_objects)
            
            # Apply pagination
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            paginated_objects = filtered_objects[start_idx:end_idx]
            
            # Convert to rows
            rows = []
            for obj in paginated_objects:
                row = [obj.get(col) for col in column_names]
                rows.append(row)
            
            return total_count, rows, column_names
            
        else:
            # For individual objects, try to visualize content based on type
            obj_stat = client.stat_object(schema_name, table_name)
            content_type = obj_stat.content_type if hasattr(obj_stat, 'content_type') else ''
            
            # Get object data
            response = client.get_object(schema_name, table_name)
            
            # Handle different content types
            if 'csv' in content_type or table_name.endswith('.csv') or 'text/plain' in content_type:
                # CSV file
                try:
                    # Use pandas to read CSV
                    df = pd.read_csv(response)
                    
                    # Apply column selection if provided
                    if selected_columns:
                        available_columns = [col for col in selected_columns if col in df.columns]
                        if available_columns:
                            df = df[available_columns]
                    
                    # Apply filters if provided
                    if filters:
                        for filter_item in filters:
                            column = filter_item.column
                            if column not in df.columns:
                                continue
                                
                            if filter_item.operator == FilterOperator.eq:
                                df = df[df[column] == filter_item.value]
                            elif filter_item.operator == FilterOperator.neq:
                                df = df[df[column] != filter_item.value]
                            elif filter_item.operator == FilterOperator.gt:
                                df = df[df[column] > filter_item.value]
                            elif filter_item.operator == FilterOperator.gte:
                                df = df[df[column] >= filter_item.value]
                            elif filter_item.operator == FilterOperator.lt:
                                df = df[df[column] < filter_item.value]
                            elif filter_item.operator == FilterOperator.lte:
                                df = df[df[column] <= filter_item.value]
                            elif filter_item.operator == FilterOperator.like:
                                df = df[df[column].astype(str).str.contains(filter_item.value, na=False)]
                            elif filter_item.operator == FilterOperator.ilike:
                                df = df[df[column].astype(str).str.contains(filter_item.value, case=False, na=False)]
                            elif filter_item.operator == FilterOperator.in_list:
                                df = df[df[column].isin(filter_item.values)]
                            elif filter_item.operator == FilterOperator.not_in:
                                df = df[~df[column].isin(filter_item.values)]
                            elif filter_item.operator == FilterOperator.is_null:
                                df = df[df[column].isna()]
                            elif filter_item.operator == FilterOperator.is_not_null:
                                df = df[~df[column].isna()]
                            elif filter_item.operator == FilterOperator.between:
                                if filter_item.values and len(filter_item.values) >= 2:
                                    df = df[(df[column] >= filter_item.values[0]) & (df[column] <= filter_item.values[1])]
                    
                    # Apply sorting if provided
                    if sort_by:
                        sort_columns = []
                        sort_ascending = []
                        
                        for sort_item in sort_by:
                            if sort_item.column in df.columns:
                                sort_columns.append(sort_item.column)
                                sort_ascending.append(sort_item.direction == SortDirection.asc)
                        
                        if sort_columns:
                            df = df.sort_values(by=sort_columns, ascending=sort_ascending)
                    
                    # Get total count
                    total_count = len(df)
                    
                    # Apply pagination
                    start_idx = (page - 1) * page_size
                    df = df.iloc[start_idx:start_idx + page_size]
                    
                    # Convert to rows and columns
                    column_names = df.columns.tolist()
                    rows = df.values.tolist()
                    
                    response.close()
                    return total_count, rows, column_names
                    
                except Exception as e:
                    # Fallback to manual CSV parsing if pandas fails
                    response.seek(0)
                    csv_data = list(csv.reader(io.TextIOWrapper(response, encoding='utf-8')))
                    response.close()
                    
                    if not csv_data:
                        return 0, [], []
                    
                    header = csv_data[0]
                    data = csv_data[1:]
                    
                    # Apply column selection if provided
                    if selected_columns:
                        col_indices = [i for i, col in enumerate(header) if col in selected_columns]
                        if col_indices:
                            header = [header[i] for i in col_indices]
                            data = [[row[i] for i in col_indices] for row in data]
                    
                    # Apply filters (simplified)
                    if filters:
                        filtered_data = []
                        for row in data:
                            include = True
                            
                            for filter_item in filters:
                                if filter_item.column in header:
                                    col_idx = header.index(filter_item.column)
                                    value = row[col_idx]
                                    
                                    # Simple string comparison for basic filtering
                                    if filter_item.operator == FilterOperator.eq and str(value) != str(filter_item.value):
                                        include = False
                                        break
                                    elif filter_item.operator == FilterOperator.like and filter_item.value not in str(value):
                                        include = False
                                        break
                            
                            if include:
                                filtered_data.append(row)
                        
                        data = filtered_data
                    
                    # Get total count
                    total_count = len(data)
                    
                    # Apply pagination
                    start_idx = (page - 1) * page_size
                    end_idx = start_idx + page_size
                    data = data[start_idx:end_idx]
                    
                    return total_count, data, header
            
            elif 'json' in content_type or table_name.endswith('.json'):
                # JSON file
                try:
                    data = json.loads(response.read().decode('utf-8'))
                    response.close()
                    
                    if isinstance(data, list):
                        # List of objects/records
                        if data and isinstance(data[0], dict):
                            # Convert to DataFrame for easier processing
                            df = pd.DataFrame(data)
                            
                            # Apply column selection if provided
                            if selected_columns:
                                available_columns = [col for col in selected_columns if col in df.columns]
                                if available_columns:
                                    df = df[available_columns]
                            
                            # Apply filters if provided
                            if filters:
                                for filter_item in filters:
                                    column = filter_item.column
                                    if column not in df.columns:
                                        continue
                                        
                                    if filter_item.operator == FilterOperator.eq:
                                        df = df[df[column] == filter_item.value]
                                    elif filter_item.operator == FilterOperator.neq:
                                        df = df[df[column] != filter_item.value]
                                    elif filter_item.operator == FilterOperator.gt:
                                        df = df[df[column] > filter_item.value]
                                    elif filter_item.operator == FilterOperator.gte:
                                        df = df[df[column] >= filter_item.value]
                                    elif filter_item.operator == FilterOperator.lt:
                                        df = df[df[column] < filter_item.value]
                                    elif filter_item.operator == FilterOperator.lte:
                                        df = df[df[column] <= filter_item.value]
                                    elif filter_item.operator == FilterOperator.like:
                                        df = df[df[column].astype(str).str.contains(filter_item.value, na=False)]
                                    elif filter_item.operator == FilterOperator.ilike:
                                        df = df[df[column].astype(str).str.contains(filter_item.value, case=False, na=False)]
                                    elif filter_item.operator == FilterOperator.in_list:
                                        df = df[df[column].isin(filter_item.values)]
                                    elif filter_item.operator == FilterOperator.not_in:
                                        df = df[~df[column].isin(filter_item.values)]
                                    elif filter_item.operator == FilterOperator.is_null:
                                        df = df[df[column].isna()]
                                    elif filter_item.operator == FilterOperator.is_not_null:
                                        df = df[~df[column].isna()]
                                    elif filter_item.operator == FilterOperator.between:
                                        if filter_item.values and len(filter_item.values) >= 2:
                                            df = df[(df[column] >= filter_item.values[0]) & (df[column] <= filter_item.values[1])]
                            
                            # Apply sorting if provided
                            if sort_by:
                                sort_columns = []
                                sort_ascending = []
                                
                                for sort_item in sort_by:
                                    if sort_item.column in df.columns:
                                        sort_columns.append(sort_item.column)
                                        sort_ascending.append(sort_item.direction == SortDirection.asc)
                                
                                if sort_columns:
                                    df = df.sort_values(by=sort_columns, ascending=sort_ascending)
                            
                            # Get total count
                            total_count = len(df)
                            
                            # Apply pagination
                            start_idx = (page - 1) * page_size
                            df = df.iloc[start_idx:start_idx + page_size]
                            
                            # Convert to rows and columns
                            column_names = df.columns.tolist()
                            rows = df.values.tolist()
                            
                            return total_count, rows, column_names
                            
                        else:
                            # Simple list
                            total_count = len(data)
                            
                            # Apply pagination
                            start_idx = (page - 1) * page_size
                            end_idx = start_idx + page_size
                            paginated_data = data[start_idx:end_idx]
                            
                            return total_count, [[item] for item in paginated_data], ["Value"]
                    
                    elif isinstance(data, dict):
                        # Single object
                        items = list(data.items())
                        total_count = len(items)
                        
                        # Apply pagination
                        start_idx = (page - 1) * page_size
                        end_idx = start_idx + page_size
                        paginated_items = items[start_idx:end_idx]
                        
                        return total_count, [[k, str(v)] for k, v in paginated_items], ["Key", "Value"]
                    
                    else:
                        # Scalar value
                        return 1, [[data]], ["Value"]
                
                except Exception as e:
                    # If JSON parsing fails
                    return 0, [[f"Error parsing JSON: {str(e)}"]], ["Error"]
            
            elif 'parquet' in content_type or table_name.endswith('.parquet'):
                # Parquet file
                try:
                    # Save to temporary file first
                    temp_file = io.BytesIO(response.read())
                    response.close()
                    
                    # Use pandas to read parquet
                    df = pd.read_parquet(temp_file)
                    
                    # Apply column selection if provided
                    if selected_columns:
                        available_columns = [col for col in selected_columns if col in df.columns]
                        if available_columns:
                            df = df[available_columns]
                    
                    # Apply filters if provided
                    if filters:
                        for filter_item in filters:
                            column = filter_item.column
                            if column not in df.columns:
                                continue
                                
                            if filter_item.operator == FilterOperator.eq:
                                df = df[df[column] == filter_item.value]
                            elif filter_item.operator == FilterOperator.neq:
                                df = df[df[column] != filter_item.value]
                            elif filter_item.operator == FilterOperator.gt:
                                df = df[df[column] > filter_item.value]
                            elif filter_item.operator == FilterOperator.gte:
                                df = df[df[column] >= filter_item.value]
                            elif filter_item.operator == FilterOperator.lt:
                                df = df[df[column] < filter_item.value]
                            elif filter_item.operator == FilterOperator.lte:
                                df = df[df[column] <= filter_item.value]
                            elif filter_item.operator == FilterOperator.like:
                                df = df[df[column].astype(str).str.contains(filter_item.value, na=False)]
                            elif filter_item.operator == FilterOperator.ilike:
                                df = df[df[column].astype(str).str.contains(filter_item.value, case=False, na=False)]
                            elif filter_item.operator == FilterOperator.in_list:
                                df = df[df[column].isin(filter_item.values)]
                            elif filter_item.operator == FilterOperator.not_in:
                                df = df[~df[column].isin(filter_item.values)]
                            elif filter_item.operator == FilterOperator.is_null:
                                df = df[df[column].isna()]
                            elif filter_item.operator == FilterOperator.is_not_null:
                                df = df[~df[column].isna()]
                            elif filter_item.operator == FilterOperator.between:
                                if filter_item.values and len(filter_item.values) >= 2:
                                    df = df[(df[column] >= filter_item.values[0]) & (df[column] <= filter_item.values[1])]
                    
                    # Apply sorting if provided
                    if sort_by:
                        sort_columns = []
                        sort_ascending = []
                        
                        for sort_item in sort_by:
                            if sort_item.column in df.columns:
                                sort_columns.append(sort_item.column)
                                sort_ascending.append(sort_item.direction == SortDirection.asc)
                        
                        if sort_columns:
                            df = df.sort_values(by=sort_columns, ascending=sort_ascending)
                    
                    # Get total count
                    total_count = len(df)
                    
                    # Apply pagination
                    start_idx = (page - 1) * page_size
                    df = df.iloc[start_idx:start_idx + page_size]
                    
                    # Convert to rows and columns
                    column_names = df.columns.tolist()
                    rows = df.values.tolist()
                    
                    return total_count, rows, column_names
                    
                except Exception as e:
                    # If parquet parsing fails
                    return 0, [[f"Error parsing Parquet: {str(e)}"]], ["Error"]
            
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
                
                # Apply pagination
                total_count = len(rows)
                start_idx = (page - 1) * page_size
                end_idx = start_idx + page_size
                rows = rows[start_idx:end_idx]
                
                return total_count, rows, column_names
        
    except Exception as e:
        print(f"Error visualizing MinIO data: {str(e)}")
        return 0, [], []

async def execute_minio_query(
    connection_params: Dict[str, Any],
    query: str,
    max_rows: int = 1000
) -> Tuple[List[List[Any]], List[str]]:
    """
    Execute a custom query on MinIO/S3 using S3 Select if available.
    
    Note: This is a simplified implementation. S3 Select requires specific formatting
    and is only available for certain file types (CSV, JSON, Parquet).
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
        
        # Parse the query to extract bucket, object, and SQL
        # This is a simplified parser for demonstration
        # Format expected: SELECT ... FROM s3object[.bucket.object] WHERE ...
        
        query_lower = query.lower()
        if not query_lower.startswith("select"):
            raise ValueError("Only SELECT queries are supported")
        
        # Check if query contains bucket and object information
        if "s3object.bucket." in query_lower:
            # Extract bucket and object from query
            parts = query_lower.split("s3object.", 1)[1].split(" ", 1)[0].split(".")
            if len(parts) >= 2:
                bucket = parts[0]
                object_key = ".".join(parts[1:])
            else:
                raise ValueError("Invalid query format. Expected: SELECT ... FROM s3object.bucket.object WHERE ...")
        else:
            # Bucket and object must be provided in connection_params
            bucket = connection_params.get("bucket")
            object_key = connection_params.get("object_key")
            
            if not bucket or not object_key:
                raise ValueError("Bucket and object_key must be provided in connection_params or in the query")
        
        # Check if the object exists and get its content type
        try:
            obj_stat = client.stat_object(bucket, object_key)
            content_type = obj_stat.content_type if hasattr(obj_stat, 'content_type') else ''
        except:
            raise ValueError(f"Object {bucket}/{object_key} not found")
        
        # For this implementation, we'll download the object and process it locally
        # In a production environment, you would use S3 Select if available
        
        response = client.get_object(bucket, object_key)
        
        # Process based on content type
        if 'csv' in content_type or object_key.endswith('.csv') or 'text/plain' in content_type:
            # CSV file
            df = pd.read_csv(response)
            response.close()
            
            # Execute query using pandas query if possible
            if "where" in query_lower:
                where_clause = query_lower.split("where", 1)[1].strip()
                try:
                    df = df.query(where_clause)
                except:
                    # If pandas query fails, just return all data
                    pass
            
            # Apply limit
            df = df.head(max_rows)
            
            # Convert to rows and columns
            column_names = df.columns.tolist()
            rows = df.values.tolist()
            
            return rows, column_names
            
        elif 'json' in content_type or object_key.endswith('.json'):
            # JSON file
            data = json.loads(response.read().decode('utf-8'))
            response.close()
            
            if isinstance(data, list) and data and isinstance(data[0], dict):
                # List of objects/records
                df = pd.DataFrame(data)
                
                # Apply limit
                df = df.head(max_rows)
                
                # Convert to rows and columns
                column_names = df.columns.tolist()
                rows = df.values.tolist()
                
                return rows, column_names
            else:
                # Not a tabular format
                return [[str(data)]], ["Data"]
                
        elif 'parquet' in content_type or object_key.endswith('.parquet'):
            # Parquet file
            temp_file = io.BytesIO(response.read())
            response.close()
            
            df = pd.read_parquet(temp_file)
            
            # Apply limit
            df = df.head(max_rows)
            
            # Convert to rows and columns
            column_names = df.columns.tolist()
            rows = df.values.tolist()
            
            return rows, column_names
            
        else:
            # Unsupported format
            response.close()
            return [[f"Unsupported file format for query: {content_type}"]], ["Error"]
        
    except Exception as e:
        print(f"Error executing MinIO query: {str(e)}")
        return [[f"Error: {str(e)}"]], ["Error"]
