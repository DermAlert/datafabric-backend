import io
import pandas as pd
import numpy as np
import json
import time
import threading
from typing import Dict, Any, List, Tuple, Optional, Union
from minio import Minio
from datetime import datetime

from ..utils.logger import logger

class ISICArchiveService:
    """Service class for handling ISIC Archive data from MinIO parquet files."""
    
    def __init__(
        self,
        minio_endpoint: str = "minio:9000",
        minio_access_key: str = "minio",
        minio_secret_key: str = "minio123",
        minio_secure: bool = False,
        bucket_name: str = "isic-parquet",
        metadata_object_name: str = "metadata_latest.parquet"
    ):
        """Initialize the ISIC Archive service."""
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_secure = minio_secure
        self.bucket_name = bucket_name
        self.metadata_object_name = metadata_object_name
        self._df_cache = None
        self._cache_timestamp = None
        self._cache_lock = threading.Lock()
        self._cache_ttl = 300  # Cache TTL in seconds (5 minutes)

    def get_minio_client(self) -> Minio:
        """Get a MinIO client."""
        return Minio(
            endpoint=self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=self.minio_secure
        )

    def test_connection(self) -> Dict[str, Any]:
        """
        Test the connection to MinIO and verify the parquet file exists.
        
        Returns:
            Dict containing test results
        """
        start_time = time.time()
        result = {
            "success": False,
            "connection_test": False,
            "bucket_exists": False,
            "file_exists": False,
            "file_readable": False,
            "row_count": 0,
            "column_count": 0,
            "columns": [],
            "execution_time_ms": 0
        }
        
        try:
            # Test MinIO connection
            client = self.get_minio_client()
            buckets = client.list_buckets()
            result["connection_test"] = True
            
            # Test bucket existence
            bucket_exists = client.bucket_exists(self.bucket_name)
            result["bucket_exists"] = bucket_exists
            
            if not bucket_exists:
                result["message"] = f"Bucket '{self.bucket_name}' does not exist"
                return self._finalize_result(result, start_time)
            
            # Test file existence
            try:
                client.stat_object(self.bucket_name, self.metadata_object_name)
                result["file_exists"] = True
            except Exception as e:
                result["message"] = f"File '{self.metadata_object_name}' not found: {str(e)}"
                return self._finalize_result(result, start_time)
            
            # Test file readability by loading a small sample
            try:
                # Read just the file header to verify it's a valid parquet file
                response = client.get_object(self.bucket_name, self.metadata_object_name)
                data = response.read(8192)  # Read just enough bytes to parse header
                bio = io.BytesIO(data)
                
                # Try to read the parquet metadata without loading the full file
                parquet_metadata = pd.read_parquet(bio, engine='pyarrow').head(0)
                
                # Get actual row count
                response.close()
                
                # Read full file for row count
                response = client.get_object(self.bucket_name, self.metadata_object_name)
                full_data = response.read()
                bio = io.BytesIO(full_data)
                df = pd.read_parquet(bio)
                
                result["file_readable"] = True
                result["row_count"] = len(df)
                result["column_count"] = len(df.columns)
                result["columns"] = df.columns.tolist()
                result["success"] = True
                result["message"] = "Connection test successful"
                
            except Exception as e:
                result["message"] = f"Error reading parquet file: {str(e)}"
            finally:
                if response:
                    response.close()
                    response.release_conn()
                    
        except Exception as e:
            result["message"] = f"Connection error: {str(e)}"
            
        return self._finalize_result(result, start_time)
    
    def _finalize_result(self, result: Dict[str, Any], start_time: float) -> Dict[str, Any]:
        """Add execution time to result."""
        result["execution_time_ms"] = int((time.time() - start_time) * 1000)
        return result
    
    def get_metadata_df(self, force_refresh: bool = False) -> pd.DataFrame:
        """
        Get the ISIC metadata DataFrame from MinIO parquet file.
        Uses caching to avoid repeated reads from MinIO.
        
        Args:
            force_refresh: Force a refresh of the cached data
            
        Returns:
            DataFrame containing ISIC metadata
        """
        with self._cache_lock:
            current_time = time.time()
            
            # Return cached data if available and not expired
            if not force_refresh and self._df_cache is not None and self._cache_timestamp is not None:
                if current_time - self._cache_timestamp < self._cache_ttl:
                    return self._df_cache
            
            try:
                # Get data from MinIO
                client = self.get_minio_client()
                response = client.get_object(self.bucket_name, self.metadata_object_name)
                data = response.read()
                response.close()
                
                # Parse parquet data
                bio = io.BytesIO(data)
                df = pd.read_parquet(bio)
                
                # Update cache
                self._df_cache = df
                self._cache_timestamp = current_time
                
                return df
                
            except Exception as e:
                logger.error(f"Error reading ISIC metadata from MinIO: {str(e)}")
                if self._df_cache is not None:
                    # Return stale cache if available
                    logger.warning("Returning stale cache due to MinIO read error")
                    return self._df_cache
                raise
    
    def search_metadata(self, 
                      filters: Optional[List[Dict[str, Any]]] = None,
                      sort_fields: Optional[List[Dict[str, str]]] = None,
                      limit: int = 100,
                      offset: int = 0,
                      include_fields: Optional[List[str]] = None,
                      exclude_fields: Optional[List[str]] = None,
                      query: Optional[str] = None) -> Dict[str, Any]:
        """
        Search ISIC metadata with filtering, sorting, and pagination.
        
        Args:
            filters: List of filter conditions
            sort_fields: List of sort specifications
            limit: Maximum number of results
            offset: Result offset for pagination
            include_fields: Fields to include in results
            exclude_fields: Fields to exclude from results
            query: Free text search query
            
        Returns:
            Dict containing search results and metadata
        """
        start_time = time.time()
        
        try:
            # Get the DataFrame
            df = self.get_metadata_df()
            total_count = len(df)
            
            # Apply filters
            if filters:
                for filter_spec in filters:
                    field = filter_spec.get("field")
                    operator = filter_spec.get("operator", "eq")
                    value = filter_spec.get("value")
                    
                    if field not in df.columns:
                        continue
                    
                    # Handle array fields specially
                    is_array_field = False
                    if len(df) > 0:
                        sample_val = df[field].iloc[0]
                        if isinstance(sample_val, str) and sample_val.startswith('[') and sample_val.endswith(']'):
                            is_array_field = True
                    
                    if is_array_field:
                        # For array fields, we need special handling
                        if operator == "contains":
                            # Filter records where the array contains the specified value
                            df = df[df[field].apply(lambda x: 
                                isinstance(x, str) and str(value) in x)]
                        elif operator == "eq":
                            # Exact match for arrays as strings
                            df = df[df[field] == value]
                        else:
                            # Other operators not supported for arrays
                            continue
                    else:
                        # Normal field filtering
                        if operator == "eq":
                            df = df[df[field] == value]
                        elif operator == "neq":
                            df = df[df[field] != value]
                        elif operator == "gt":
                            df = df[df[field] > value]
                        elif operator == "lt":
                            df = df[df[field] < value]
                        elif operator == "gte":
                            df = df[df[field] >= value]
                        elif operator == "lte":
                            df = df[df[field] <= value]
                        elif operator == "like":
                            if isinstance(value, str):
                                df = df[df[field].astype(str).str.contains(value, case=True, na=False)]
                        elif operator == "ilike":
                            if isinstance(value, str):
                                df = df[df[field].astype(str).str.contains(value, case=False, na=False)]
                        elif operator == "in":
                            if isinstance(value, list):
                                df = df[df[field].isin(value)]
                        elif operator == "not_in":
                            if isinstance(value, list):
                                df = df[~df[field].isin(value)]
            
            # Apply free text search
            if query:
                # Search across all string columns
                string_cols = [col for col in df.columns if df[col].dtype == 'object']
                if string_cols:
                    # Create a combined search condition across all string columns
                    mask = False
                    for col in string_cols:
                        mask |= df[col].astype(str).str.contains(query, case=False, na=False)
                    df = df[mask]
            
            filtered_count = len(df)
            
            # Apply sorting
            if sort_fields:
                sort_cols = []
                sort_ascending = []
                
                for sort_spec in sort_fields:
                    field = sort_spec.get("field")
                    direction = sort_spec.get("direction", "asc")
                    
                    if field in df.columns:
                        sort_cols.append(field)
                        sort_ascending.append(direction.lower() == "asc")
                
                if sort_cols:
                    df = df.sort_values(by=sort_cols, ascending=sort_ascending)
            
            # Apply pagination
            if offset > 0:
                df = df.iloc[offset:]
            
            if limit > 0:
                df = df.iloc[:limit]
            
            # Select fields
            if include_fields:
                # Ensure we only include columns that exist
                cols_to_include = [col for col in include_fields if col in df.columns]
                df = df[cols_to_include]
            
            if exclude_fields:
                # Exclude specified fields
                cols_to_exclude = [col for col in exclude_fields if col in df.columns]
                df = df.drop(columns=cols_to_exclude, errors='ignore')
            
            # Convert to list of dictionaries for JSON response
            # Handle special data types like NaN, timestamps
            def clean_value(value):
                if pd.isna(value):
                    return None
                elif isinstance(value, pd.Timestamp):
                    return value.isoformat()
                elif isinstance(value, np.integer):
                    return int(value)
                elif isinstance(value, np.floating):
                    return float(value)
                elif isinstance(value, (np.bool_, bool)):
                    return bool(value)
                elif isinstance(value, str) and value.startswith('[') and value.endswith(']'):
                    # Try to parse JSON arrays
                    try:
                        return json.loads(value)
                    except:
                        return value
                return value
            
            # Convert DataFrame to list of dicts with clean values
            results = []
            for _, row in df.iterrows():
                item = {}
                for col in df.columns:
                    item[col] = clean_value(row[col])
                results.append(item)
            
            # Calculate execution time
            execution_time_ms = int((time.time() - start_time) * 1000)
            
            # Return response
            return {
                "total": filtered_count,
                "total_unfiltered": total_count,
                "offset": offset,
                "limit": limit,
                "items": results,
                "filters_applied": filters or [],
                "execution_time_ms": execution_time_ms
            }
            
        except Exception as e:
            logger.error(f"Error searching ISIC metadata: {str(e)}")
            raise
    
    def get_diagnoses_stats(self, include_examples: bool = True) -> Dict[str, Any]:
        """
        Get statistics about ISIC Archive diagnoses.
        
        Args:
            include_examples: Whether to include example image IDs
            
        Returns:
            Dict containing diagnosis statistics
        """
        start_time = time.time()
        
        try:
            # Get the DataFrame
            df = self.get_metadata_df()
            diagnosis_column = 'clinical_diagnosis_1'
            
            if diagnosis_column not in df.columns:
                return {
                    "error": f"Diagnosis column '{diagnosis_column}' not found",
                    "total_diagnoses": 0,
                    "total_images": len(df),
                    "diagnoses": [],
                    "execution_time_ms": int((time.time() - start_time) * 1000)
                }
            
            # Get diagnoses and counts
            diagnoses = df[diagnosis_column].value_counts().reset_index()
            diagnoses.columns = ['diagnosis', 'count']
            total_images = len(df)
            
            # Calculate percentage
            diagnoses['percentage'] = (diagnoses['count'] / total_images * 100).round(2)
            
            # Determine malignancy
            diagnoses['is_malignant'] = diagnoses['diagnosis'].str.lower().apply(
                lambda x: bool(x) and ('melanoma' in x.lower() or 'carcinoma' in x.lower() or 'malignant' in x.lower())
            )
            
            # Get examples if requested
            if include_examples:
                examples_dict = {}
                for diagnosis in diagnoses['diagnosis']:
                    # Get up to 5 examples of each diagnosis
                    examples = df[df[diagnosis_column] == diagnosis]['isic_id'].head(5).tolist()
                    examples_dict[diagnosis] = examples
                
                diagnoses['examples'] = diagnoses['diagnosis'].map(examples_dict)
            else:
                diagnoses['examples'] = [[] for _ in range(len(diagnoses))]
            
            # Convert to list of dicts
            result_list = diagnoses.to_dict(orient='records')
            
            return {
                "total_diagnoses": len(result_list),
                "total_images": total_images,
                "diagnoses": result_list,
                "execution_time_ms": int((time.time() - start_time) * 1000)
            }
            
        except Exception as e:
            logger.error(f"Error getting ISIC diagnosis stats: {str(e)}")
            return {
                "error": str(e),
                "total_diagnoses": 0,
                "total_images": 0,
                "diagnoses": [],
                "execution_time_ms": int((time.time() - start_time) * 1000)
            }
    
    def get_collections(self) -> Dict[str, Any]:
        """
        Get ISIC Archive collections information.
        
        Returns:
            Dict containing collection information
        """
        start_time = time.time()
        
        try:
            # Get the DataFrame
            df = self.get_metadata_df()
            
            if 'collection_id' not in df.columns or 'collection_name' not in df.columns:
                return {
                    "error": "Collection columns not found",
                    "total_collections": 0,
                    "collections": [],
                    "execution_time_ms": int((time.time() - start_time) * 1000)
                }
            
            # Extract unique collection IDs and names
            collections = []
            collection_ids = set()
            
            # Handle both string arrays and actual list objects
            for idx, row in df.iterrows():
                col_ids = row['collection_id']
                col_names = row['collection_name']
                
                # Parse string arrays if needed
                if isinstance(col_ids, str) and col_ids.startswith('[') and col_ids.endswith(']'):
                    try:
                        col_ids = json.loads(col_ids)
                    except:
                        col_ids = [col_ids]
                        
                if isinstance(col_names, str) and col_names.startswith('[') and col_names.endswith(']'):
                    try:
                        col_names = json.loads(col_names)
                    except:
                        col_names = [col_names]
                
                # Handle non-list values
                if not isinstance(col_ids, list):
                    col_ids = [col_ids]
                    
                if not isinstance(col_names, list):
                    col_names = [col_names]
                    
                # Match IDs with names
                for i, col_id in enumerate(col_ids):
                    if col_id and str(col_id) not in collection_ids:
                        collection_ids.add(str(col_id))
                        name = col_names[i] if i < len(col_names) else f"Collection {col_id}"
                        collections.append({
                            "id": str(col_id),
                            "name": name
                        })
            
            # Count images per collection
            for collection in collections:
                # Count images in each collection
                cid = collection["id"]
                
                # Function to check if collection ID is in the array
                def has_collection_id(value):
                    if pd.isna(value):
                        return False
                    
                    if isinstance(value, str) and value.startswith('['):
                        try:
                            ids = json.loads(value)
                            return cid in [str(id) for id in ids]
                        except:
                            return cid == value
                    elif isinstance(value, list):
                        return cid in [str(id) for id in value]
                    else:
                        return cid == str(value)
                
                count = df['collection_id'].apply(has_collection_id).sum()
                collection["image_count"] = count
            
            # Sort by image count descending
            collections.sort(key=lambda x: x["image_count"], reverse=True)
            
            return {
                "total_collections": len(collections),
                "collections": collections,
                "execution_time_ms": int((time.time() - start_time) * 1000)
            }
            
        except Exception as e:
            logger.error(f"Error getting ISIC collections: {str(e)}")
            return {
                "error": str(e),
                "total_collections": 0,
                "collections": [],
                "execution_time_ms": int((time.time() - start_time) * 1000)
            }