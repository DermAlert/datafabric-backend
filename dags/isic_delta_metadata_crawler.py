from datetime import datetime, timedelta
import requests
import io
import logging
import time
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(hours=2),
}

# API and storage configuration
ISIC_API_BASE = "https://api.isic-archive.com/api/v2"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_SECURE = False

# Bucket definitions for Delta Lake structure
DELTA_BUCKET = "isic-delta"
IMAGES_BUCKET = "isic-images"

# Delta Lake paths
COLLECTIONS_DELTA_PATH = "collections/collections.delta"
IMAGES_METADATA_DELTA_PATH = "images_metadata"

# S3-compatible storage configuration for Delta Lake
STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": f"http://{MINIO_ENDPOINT}",
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_REGION": "us-east-1",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true"
}

# API request parameters
REQUEST_TIMEOUT = 120
RETRY_COUNT = 3
RETRY_DELAY = 10
MAX_LIMIT = 10000

# Download limits for testing
MAX_IMAGES_PER_COLLECTION = 101  # Set to None for unlimited, or specify a number (e.g., 100) for testing

def get_minio_client():
    """Get MinIO client instance"""
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

def configure_delta_environment():
    """Configure environment variables for Delta Lake with MinIO"""
    env_vars = {
        "AWS_ENDPOINT_URL": f"http://{MINIO_ENDPOINT}",
        "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_ALLOW_HTTP": "true",
        # Additional Delta Lake specific settings
        "DELTA_RS_LOG": "warn",  # Reduce log verbosity
        "AWS_S3_ADDRESSING_STYLE": "path"  # Use path-style addressing for MinIO
    }
    
    for key, value in env_vars.items():
        os.environ[key] = value

def get_delta_table_path(bucket, path):
    """Generate S3 path for Delta table"""
    return f"s3://{bucket}/{path}"

def ensure_bucket_exists(bucket_name):
    """Ensure MinIO bucket exists"""
    client = get_minio_client()
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Created bucket {bucket_name}")
    except Exception as e:
        logger.error(f"Error creating bucket {bucket_name}: {str(e)}")
        raise

def flatten_metadata(metadata_dict, prefix=""):
    """Flatten nested metadata dictionary"""
    flattened = {}
    for key, value in metadata_dict.items():
        new_key = f"{prefix}_{key}" if prefix else key
        if isinstance(value, dict):
            flattened.update(flatten_metadata(value, new_key))
        else:
            flattened[new_key] = value
    return flattened

def api_request(url, params=None):
    logger.info(f"Making API request to: {url} with params: {params}")
    
    for attempt in range(RETRY_COUNT):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt < RETRY_COUNT - 1:
                sleep_time = RETRY_DELAY * (attempt + 1)
                logger.warning(f"Request failed: {str(e)}. Retrying in {sleep_time}s...")
                time.sleep(sleep_time)
            else:
                logger.error(f"Request failed after {RETRY_COUNT} attempts: {str(e)}")
                raise

def fetch_collections(**context):
    """Fetch all collections and save to Delta table with upsert logic"""
    logger.info("Fetching all collections")
    
    # Configure Delta Lake environment
    configure_delta_environment()
    
    # Get collections from API
    url = f"{ISIC_API_BASE}/collections"
    params = {"limit": MAX_LIMIT}
    response_data = api_request(url, params)
    
    if 'results' not in response_data:
        raise ValueError("Unexpected API response format")
    
    collections = response_data.get('results', [])
    logger.info(f"Retrieved {len(collections)} collections from API")
    
    # Prepare data for Delta table
    processed_collections = []
    current_timestamp = datetime.now().isoformat()
    
    for collection in collections:
        processed_collection = {
            'id': collection.get('id'),
            'name': collection.get('name', ''),
            'description': collection.get('description', ''),
            'created': collection.get('created', ''),
            'updated': collection.get('updated', ''),
            'public': collection.get('public', False),
            'creator': collection.get('creator', ''),
            'doi': collection.get('doi', ''),
            'license': collection.get('license', ''),
            'image_count': collection.get('image_count', 0),
            'updated_timestamp': current_timestamp
        }
        processed_collections.append(processed_collection)
    
    # Convert to DataFrame
    new_df = pd.DataFrame(processed_collections)
    
    # Ensure consistent data types
    type_mapping = {
        'id': 'int64',
        'name': 'string',
        'description': 'string',
        'created': 'string',
        'updated': 'string',
        'public': 'bool',
        'creator': 'string',
        'doi': 'string',
        'license': 'string',
        'image_count': 'int64',
        'updated_timestamp': 'string'
    }
    
    for col, dtype in type_mapping.items():
        if col in new_df.columns:
            try:
                if dtype == 'int64':
                    new_df[col] = pd.to_numeric(new_df[col], errors='coerce').fillna(0).astype('int64')
                elif dtype == 'bool':
                    new_df[col] = new_df[col].fillna(False).astype('bool')
                else:  # string
                    new_df[col] = new_df[col].fillna('').astype('string')
            except Exception as e:
                logger.warning(f"Could not convert column {col} to {dtype}: {str(e)}")
                # Fallback to string for problematic columns
                new_df[col] = new_df[col].fillna('').astype('string')
    
    # Ensure buckets exist
    ensure_bucket_exists(DELTA_BUCKET)
    
    # Write to Delta table
    delta_path = get_delta_table_path(DELTA_BUCKET, COLLECTIONS_DELTA_PATH)
    
    try:
        # Initialize structure if needed
        initialize_delta_structure()
        
        # Check if Delta table exists
        if delta_table_exists(delta_path):
            logger.info("Delta table exists, reading existing data")
            existing_df = safe_read_delta_table(delta_path)
            logger.info(f"Found existing Delta table with {len(existing_df)} collections")
            
            # Perform upsert operation
            new_collections, updated_collections = upsert_collections(existing_df, new_df, delta_path)
            
            logger.info(f"Collections processing complete:")
            logger.info(f"  New collections added: {new_collections}")
            logger.info(f"  Existing collections updated: {updated_collections}")
            logger.info(f"  Total collections in table: {len(new_df)}")
            
            return {
                "collections_count": len(new_df),
                "new_collections": new_collections,
                "updated_collections": updated_collections,
                "timestamp": current_timestamp
            }
        else:
            # Delta table doesn't exist, create it with all data
            logger.info("Delta table doesn't exist, creating new table")
            safe_write_delta_table(new_df, delta_path, mode="overwrite")
            logger.info(f"Successfully created new Delta table with {len(new_df)} collections")
            
            return {
                "collections_count": len(new_df),
                "new_collections": len(new_df),
                "updated_collections": 0,
                "timestamp": current_timestamp
            }
        
    except Exception as e:
        logger.error(f"Error writing to Delta table: {str(e)}")
        raise

def fetch_images_metadata(**context):
    """Fetch image metadata and save to partitioned Delta tables"""
    logger.info("Fetching image metadata")
    
    # Configure Delta Lake environment
    configure_delta_environment()
    
    # Read collections from Delta table
    collections_delta_path = get_delta_table_path(DELTA_BUCKET, COLLECTIONS_DELTA_PATH)
    
    try:
        collections_df = safe_read_delta_table(collections_delta_path)
        logger.info(f"Retrieved {len(collections_df)} collections from Delta table")
        
        # Filter collections that need processing
        collections_to_process = get_collections_to_process(collections_df, **context)
        logger.info(f"Will process {len(collections_to_process)} collections")
        
    except Exception as e:
        logger.error(f"Error reading collections Delta table: {str(e)}")
        raise
    
    ensure_bucket_exists(DELTA_BUCKET)
    ensure_bucket_exists(IMAGES_BUCKET)
    
    timestamp = datetime.now().isoformat()
    total_images_processed = 0
    
    for idx, collection_row in collections_to_process.iterrows():
        collection_id = collection_row['id']
        collection_name = collection_row['name']
        
        if pd.isna(collection_id) or collection_id == '':
            continue
            
        try:
            logger.info(f"Processing collection {idx+1}/{len(collections_to_process)}: {collection_name} (ID: {collection_id})")
            
            url = f"{ISIC_API_BASE}/images/search"
            params = {
                "collections": str(collection_id),
                "limit": MAX_LIMIT
            }
            
            collection_images = []
            page_count = 1
            images_processed_for_collection = 0
            
            while url:
                logger.info(f"Fetching page {page_count} for collection {collection_id}")
                
                if page_count == 1:
                    response_data = api_request(url, params)
                else:
                    response_data = api_request(url)
                
                if 'results' not in response_data:
                    logger.warning(f"Unexpected response structure for collection {collection_id}")
                    break
                    
                images = response_data.get('results', [])
                
                for image in images:
                    # Check if we've reached the limit for this collection
                    if MAX_IMAGES_PER_COLLECTION is not None and images_processed_for_collection >= MAX_IMAGES_PER_COLLECTION:
                        logger.info(f"Reached limit of {MAX_IMAGES_PER_COLLECTION} images for collection {collection_id}")
                        break
                    # Process each image
                    processed_image = {
                        'isic_id': image.get('isic_id', ''),
                        'collection_id': collection_id,
                        'collection_name': collection_name,
                        'copyright_license': image.get('copyright_license', ''),
                        'public': image.get('public', False),
                        'created': image.get('created', ''),
                        'updated': image.get('updated', ''),
                    }
                    
                    # Handle image files
                    if 'files' in image:
                        files = image['files']
                        if 'full' in files:
                            full_file = files['full']
                            processed_image['image_url'] = full_file.get('url', '')
                            processed_image['image_size'] = full_file.get('size', 0)
                            
                            # Generate image path in our bucket
                            isic_id = processed_image['isic_id']
                            if isic_id:
                                processed_image['image_path'] = f"s3://{IMAGES_BUCKET}/{isic_id}.jpg"
                        
                        if 'thumbnail_256' in files:
                            thumb_file = files['thumbnail_256']
                            processed_image['thumbnail_url'] = thumb_file.get('url', '')
                            processed_image['thumbnail_size'] = thumb_file.get('size', 0)
                    
                    # Flatten metadata
                    if 'metadata' in image:
                        flattened_metadata = flatten_metadata(image['metadata'])
                        processed_image.update(flattened_metadata)
                    
                    processed_image['updated_timestamp'] = timestamp
                    collection_images.append(processed_image)
                    images_processed_for_collection += 1
                
                # Check if we've reached the limit for this collection
                if MAX_IMAGES_PER_COLLECTION is not None and images_processed_for_collection >= MAX_IMAGES_PER_COLLECTION:
                    logger.info(f"Reached limit of {MAX_IMAGES_PER_COLLECTION} images for collection {collection_id}")
                    break
                
                logger.info(f"Retrieved {len(images)} images from collection {collection_id} page {page_count}")
                
                url = response_data.get('next')
                if url:
                    params = None 
                
                page_count += 1
            
            # Save collection images to Delta table (partitioned by collection)
            if collection_images:
                df = pd.DataFrame(collection_images)
                
                # Write to partitioned Delta table
                partition_path = f"{IMAGES_METADATA_DELTA_PATH}/collection={collection_id}"
                delta_path = get_delta_table_path(DELTA_BUCKET, partition_path)
                
                try:
                    # Check if collection partition already exists and compare
                    should_update = True
                    existing_df = None
                    
                    if delta_table_exists(delta_path):
                        logger.info(f"Collection {collection_id} partition exists, checking for changes")
                        existing_df = safe_read_delta_table(delta_path)
                        
                        # Compare image counts and schemas to decide if update is needed
                        schema_changed = set(existing_df.columns) != set(df.columns)
                        
                        if len(existing_df) == len(df) and not schema_changed:
                            # Check if we have the same images (by isic_id)
                            existing_ids = set(existing_df['isic_id'].tolist()) if 'isic_id' in existing_df.columns else set()
                            new_ids = set(df['isic_id'].tolist()) if 'isic_id' in df.columns else set()
                            
                            if existing_ids == new_ids:
                                should_update = False
                                logger.info(f"Collection {collection_id} unchanged, skipping update")
                        elif schema_changed:
                            logger.info(f"Collection {collection_id} schema changed, will update with schema evolution")
                            log_dataframe_schema(existing_df, f"Existing data for collection {collection_id}")
                            log_dataframe_schema(df, f"New data for collection {collection_id}")
                            
                            # Log specific column differences
                            existing_cols = set(existing_df.columns)
                            new_cols = set(df.columns)
                            added_cols = new_cols - existing_cols
                            removed_cols = existing_cols - new_cols
                            
                            if added_cols:
                                logger.info(f"New columns added: {list(added_cols)}")
                            if removed_cols:
                                logger.info(f"Columns removed: {list(removed_cols)}")
                            
                            # Log type changes for common columns
                            common_cols = existing_cols & new_cols
                            for col in common_cols:
                                existing_dtype = existing_df[col].dtype
                                new_dtype = df[col].dtype
                                if existing_dtype != new_dtype:
                                    logger.info(f"Column {col} type changed: {existing_dtype} -> {new_dtype}")
                            
                    else:
                        # Partition doesn't exist, we need to create it
                        logger.info(f"Collection {collection_id} partition doesn't exist, creating new")
                    
                    if should_update:
                        # Use the safer schema-aware write function
                        success = safe_write_with_schema_evolution(df, delta_path, mode="overwrite")
                        
                        if success:
                            logger.info(f"Successfully wrote {len(df)} images for collection {collection_id}")
                            total_images_processed += len(df)
                        else:
                            logger.error(f"Failed to write images for collection {collection_id}")
                    else:
                        # Still count existing images in our total
                        total_images_processed += len(df)
                    
                except Exception as e:
                    logger.error(f"Error writing Delta table for collection {collection_id}: {str(e)}")
            
            logger.info(f"Completed processing collection {collection_id}")
            
        except Exception as e:
            logger.error(f"Error processing collection {collection_id}: {e}")
    
    logger.info(f"Total images processed: {total_images_processed}")
    
    return {
        "total_images": total_images_processed,
        "collections_processed": len(collections_to_process),
        "timestamp": timestamp
    }

def download_images_batch(**context):
    """Download a batch of images from Delta table metadata"""
    logger.info("Starting image download process")
    
    # Configure Delta Lake environment
    configure_delta_environment()
    
    # Read all images metadata from Delta tables
    base_path = get_delta_table_path(DELTA_BUCKET, IMAGES_METADATA_DELTA_PATH)
    
    try:
        # List all collection partitions
        client = get_minio_client()
        objects = client.list_objects(DELTA_BUCKET, prefix=f"{IMAGES_METADATA_DELTA_PATH}/", recursive=True)
        
        collection_paths = set()
        for obj in objects:
            if obj.object_name.endswith('.parquet'):
                # Extract collection partition path
                parts = obj.object_name.split('/')
                if len(parts) >= 3 and parts[1].startswith('collection='):
                    collection_path = '/'.join(parts[:2])
                    collection_paths.add(collection_path)
        
        downloaded_count = 0
        error_count = 0
        total_collections_processed = 0
        total_images_available = 0
        total_images_skipped = 0
        
        for collection_path in collection_paths:
            try:
                full_delta_path = get_delta_table_path(DELTA_BUCKET, collection_path)
                
                # Check if the partition exists before trying to read it
                if not delta_table_exists(full_delta_path):
                    logger.debug(f"Skipping non-existent partition for download: {collection_path}")
                    continue
                
                dt = DeltaTable(full_delta_path, storage_options=STORAGE_OPTIONS)
                df = dt.to_pandas()
                
                collection_id = collection_path.split('=')[1]
                logger.info(f"Processing {len(df)} images from collection {collection_id}")
                
                total_collections_processed += 1
                total_images_available += len(df)
                images_downloaded_for_collection = 0
                images_skipped_for_collection = 0
                
                for _, row in df.iterrows():
                    # Check if we've reached the download limit for this collection
                    if MAX_IMAGES_PER_COLLECTION is not None and images_downloaded_for_collection >= MAX_IMAGES_PER_COLLECTION:
                        logger.info(f"Reached download limit of {MAX_IMAGES_PER_COLLECTION} images for collection {collection_id}")
                        break
                    
                    isic_id = row.get('isic_id')
                    image_url = row.get('image_url')
                    
                    if not isic_id or not image_url:
                        continue
                    
                    try:
                        # Check if image already exists
                        image_path = f"{isic_id}.jpg"
                        try:
                            client.stat_object(IMAGES_BUCKET, image_path)
                            logger.debug(f"Image {isic_id} already exists, skipping")
                            images_skipped_for_collection += 1
                            continue
                        except:
                            pass  # Image doesn't exist, proceed with download
                        
                        # Download image
                        response = requests.get(image_url, timeout=REQUEST_TIMEOUT)
                        response.raise_for_status()
                        
                        image_data = response.content
                        client.put_object(
                            IMAGES_BUCKET,
                            image_path,
                            io.BytesIO(image_data),
                            length=len(image_data),
                            content_type='image/jpeg'
                        )
                        
                        downloaded_count += 1
                        images_downloaded_for_collection += 1
                        
                        if downloaded_count % 100 == 0:
                            logger.info(f"Downloaded {downloaded_count} images so far")
                            
                    except Exception as e:
                        error_count += 1
                        logger.warning(f"Error downloading image {isic_id}: {str(e)}")
                        
                logger.info(f"Completed collection {collection_id}: Downloaded={images_downloaded_for_collection}, Skipped={images_skipped_for_collection}, Available={len(df)}")
                total_images_skipped += images_skipped_for_collection
                
            except Exception as e:
                logger.error(f"Error processing collection partition {collection_path}: {str(e)}")
                continue
        
        expected_downloads = total_collections_processed * MAX_IMAGES_PER_COLLECTION if MAX_IMAGES_PER_COLLECTION else total_images_available
        
        logger.info(f"Download complete:")
        logger.info(f"  Collections processed: {total_collections_processed}")
        logger.info(f"  Total images available: {total_images_available}")
        logger.info(f"  Images downloaded: {downloaded_count}")
        logger.info(f"  Images skipped (already exist): {total_images_skipped}")
        logger.info(f"  Download errors: {error_count}")
        logger.info(f"  Expected downloads (limit={MAX_IMAGES_PER_COLLECTION}): {expected_downloads}")
        
        return {
            "downloaded_count": downloaded_count,
            "error_count": error_count,
            "collections_processed": total_collections_processed,
            "total_images_available": total_images_available,
            "images_skipped": total_images_skipped,
            "expected_downloads": expected_downloads,
            "max_images_per_collection": MAX_IMAGES_PER_COLLECTION,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in download process: {str(e)}")
        raise

def initialize_delta_structure():
    """Initialize Delta Lake directory structure in MinIO"""
    client = get_minio_client()
    
    # Ensure buckets exist
    ensure_bucket_exists(DELTA_BUCKET)
    ensure_bucket_exists(IMAGES_BUCKET)
    
    # Create empty marker files to ensure directories exist
    try:
        # Create collections directory structure
        client.put_object(
            DELTA_BUCKET,
            "collections/.keep",
            io.BytesIO(b""),
            length=0
        )
        
        # Create images_metadata directory structure
        client.put_object(
            DELTA_BUCKET,
            "images_metadata/.keep",
            io.BytesIO(b""),
            length=0
        )
        
        logger.info("Delta Lake directory structure initialized")
        
    except Exception as e:
        logger.warning(f"Could not create directory markers: {str(e)}")

def safe_write_delta_table(df, table_path, mode="overwrite", max_retries=3):
    """Safely write to Delta table with retry logic and schema evolution"""
    configure_delta_environment()
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1}: Writing to Delta table {table_path}")
            
            # Convert DataFrame to Arrow Table first
            arrow_table = convert_pandas_to_arrow(df)
            
            # Try with schema evolution first
            try:
                write_deltalake(
                    table_path,
                    arrow_table,
                    mode=mode,
                    storage_options=STORAGE_OPTIONS,
                    schema_mode="merge"
                )
                logger.info(f"Successfully wrote {len(df)} records to {table_path}")
                return True
            except Exception as schema_error:
                # If schema evolution fails, try without it
                logger.warning(f"Schema evolution failed, trying without: {str(schema_error)}")
                write_deltalake(
                    table_path,
                    arrow_table,
                    mode=mode,
                    storage_options=STORAGE_OPTIONS
                )
                logger.info(f"Successfully wrote {len(df)} records to {table_path} (no schema evolution)")
                return True
            
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
            
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.error(f"Failed to write Delta table after {max_retries} attempts")
                raise

def safe_read_delta_table(table_path, max_retries=3):
    """Safely read from Delta table with retry logic"""
    configure_delta_environment()
    
    # First check if table exists to avoid unnecessary retries
    if not delta_table_exists(table_path):
        raise FileNotFoundError(f"Delta table does not exist at {table_path}")
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1}: Reading from Delta table {table_path}")
            
            dt = DeltaTable(table_path, storage_options=STORAGE_OPTIONS)
            df = dt.to_pandas()
            
            logger.info(f"Successfully read {len(df)} records from {table_path}")
            return df
            
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
            
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.error(f"Failed to read Delta table after {max_retries} attempts")
                raise

def upsert_collections(existing_df, new_df, delta_path):
    """
    Perform upsert operation on collections Delta table
    Returns (new_collections_count, updated_collections_count)
    """
    logger.info("Performing upsert operation on collections")
    
    # Create a set of existing collection IDs for faster lookup
    existing_ids = set(existing_df['id'].tolist()) if not existing_df.empty else set()
    
    # Identify new collections
    new_collections = new_df[~new_df['id'].isin(existing_ids)]
    new_collections_count = len(new_collections)
    
    # Identify potentially updated collections
    existing_collections = new_df[new_df['id'].isin(existing_ids)]
    
    # Compare existing collections with new data to find actual updates
    updated_collections = []
    updated_count = 0
    
    if not existing_collections.empty and not existing_df.empty:
        # Merge to compare fields that might have changed
        comparison_df = existing_collections.merge(
            existing_df[['id', 'name', 'description', 'updated', 'public', 'creator', 'doi', 'license', 'image_count']],
            on='id',
            suffixes=('_new', '_old')
        )
        
        # Check for differences in key fields
        for _, row in comparison_df.iterrows():
            has_changes = False;
            changes = []
            
            # Compare fields that might change
            fields_to_compare = [
                ('name', 'name'),
                ('description', 'description'), 
                ('updated', 'updated'),
                ('public', 'public'),
                ('creator', 'creator'),
                ('doi', 'doi'),
                ('license', 'license'),
                ('image_count', 'image_count')
            ]
            
            for field_new, field_old in fields_to_compare:
                new_val = row.get(f'{field_new}_new')
                old_val = row.get(f'{field_old}_old')
                
                # Handle NaN values
                if pd.isna(new_val) and pd.isna(old_val):
                    continue
                if pd.isna(new_val):
                    new_val = None
                if pd.isna(old_val):
                    old_val = None
                
                if new_val != old_val:
                    has_changes = True
                    changes.append(f"{field_new}: '{old_val}' -> '{new_val}'")
            
            if has_changes:
                updated_collections.append(row['id'])
                updated_count += 1
                logger.info(f"Collection {row['id']} has changes: {'; '.join(changes)}")
    
    # Log the results
    if new_collections_count > 0:
        logger.info(f"Found {new_collections_count} new collections:")
        for _, collection in new_collections.iterrows():
            logger.info(f"  New: {collection['id']} - {collection['name']}")
    
    if updated_count > 0:
        logger.info(f"Found {updated_count} updated collections")
    
    # Write the complete new dataset (this replaces the old approach)
    # In a production environment, you might want to use Delta Lake's merge functionality
    # For now, we'll append with the new data since we've detected changes
    if new_collections_count > 0 or updated_count > 0:
        logger.info("Writing updated collections to Delta table")
        safe_write_delta_table(new_df, delta_path, mode="overwrite")
    else:
        logger.info("No changes detected, skipping write operation")
    
    return new_collections_count, updated_count

def get_collections_to_process(collections_df, **context):
    """
    Determine which collections need to be processed based on changes
    Returns a filtered DataFrame of collections that need processing
    """
    logger.info("Determining which collections need processing")
    
    # Get the previous run's results from XCom if available
    try:
        # Try to get the collections update info from the previous task
        task_instance = context.get('task_instance')
        if task_instance:
            collections_info = task_instance.xcom_pull(task_ids='fetch_collections')
            if collections_info:
                new_collections = collections_info.get('new_collections', 0)
                updated_collections = collections_info.get('updated_collections', 0)
                
                if new_collections == 0 and updated_collections == 0:
                    logger.info("No new or updated collections found, processing all collections for completeness")
                    return collections_df
                else:
                    logger.info(f"Found {new_collections} new and {updated_collections} updated collections")
    except Exception as e:
        logger.warning(f"Could not determine collection changes: {str(e)}")
    
    # For now, return all collections (in a production environment, you might want to implement
    # more sophisticated logic to track which collections have been processed)
    return collections_df

def log_processing_summary(**context):
    """Log a summary of what was processed in this run"""
    try:
        task_instance = context.get('task_instance')
        if not task_instance:
            return
        
        # Get results from previous tasks
        collections_info = task_instance.xcom_pull(task_ids='fetch_collections')
        images_info = task_instance.xcom_pull(task_ids='fetch_images_metadata') 
        download_info = task_instance.xcom_pull(task_ids='download_images_batch')
        
        logger.info("="*60)
        logger.info("PROCESSING SUMMARY")
        logger.info("="*60)
        
        if collections_info:
            logger.info(f"Collections processed: {collections_info.get('collections_count', 0)}")
            logger.info(f"New collections: {collections_info.get('new_collections', 0)}")
            logger.info(f"Updated collections: {collections_info.get('updated_collections', 0)}")
        
        if images_info:
            logger.info(f"Total images metadata processed: {images_info.get('total_images', 0)}")
            logger.info(f"Collections with images processed: {images_info.get('collections_processed', 0)}")
        
        if download_info:
            logger.info(f"Images downloaded: {download_info.get('downloaded_count', 0)}")
            logger.info(f"Images skipped (already exist): {download_info.get('images_skipped', 0)}")
            logger.info(f"Download errors: {download_info.get('error_count', 0)}")
        
        logger.info("="*60)
        
    except Exception as e:
        logger.warning(f"Could not generate processing summary: {str(e)}")

def delta_table_exists(table_path):
    """Check if a Delta table exists without trying to read it"""
    try:
        configure_delta_environment()
        
        # Parse the S3 path to get bucket and key
        from urllib.parse import urlparse
        parsed = urlparse(table_path)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip('/')
        
        client = get_minio_client()
        
        # Check if there are any objects with the Delta table prefix
        objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
        
        # Look for Delta log files or parquet files
        for obj in objects:
            if (obj.object_name.endswith('.parquet') or 
                '_delta_log' in obj.object_name or
                obj.object_name.endswith('.json')):
                return True
        
        return False
        
    except Exception as e:
        logger.debug(f"Error checking if Delta table exists at {table_path}: {str(e)}")
        return False

def safe_write_with_schema_evolution(df, delta_path, mode="overwrite", max_retries=3):
    """
    Safely write to Delta table with schema evolution support and retry logic
    """
    configure_delta_environment()
    
    # Try to read existing table schema to handle type conversions
    existing_df = None
    try:
        if delta_table_exists(delta_path):
            existing_df = safe_read_delta_table(delta_path)
    except Exception as e:
        logger.debug(f"Could not read existing table schema: {str(e)}")
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1}: Writing to Delta table {delta_path} with schema evolution")
            
            # Handle schema evolution by reconciling data types
            if existing_df is not None:
                df = handle_schema_evolution(df, existing_df, delta_path)
            
            # Convert DataFrame to Arrow Table first
            arrow_table = convert_pandas_to_arrow(df)
            
            # Try with schema evolution first
            write_deltalake(
                delta_path,
                arrow_table,
                mode=mode,
                storage_options=STORAGE_OPTIONS,
                schema_mode="merge"
            )
            
            logger.info(f"Successfully wrote {len(df)} records to {delta_path}")
            return True
            
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} with schema evolution failed: {str(e)}")
            
            # Try without schema evolution as fallback
            try:
                logger.info(f"Attempt {attempt + 1}: Fallback write without schema evolution")
                arrow_table = convert_pandas_to_arrow(df)
                write_deltalake(
                    delta_path,
                    arrow_table,
                    mode=mode,
                    storage_options=STORAGE_OPTIONS
                )
                logger.info(f"Successfully wrote {len(df)} records to {delta_path} (fallback)")
                return True
            except Exception as fallback_error:
                logger.warning(f"Fallback attempt {attempt + 1} also failed: {str(fallback_error)}")
            
            if attempt < max_retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                logger.error(f"Failed to write Delta table after {max_retries} attempts")
                raise

    return False

def log_dataframe_schema(df, name="DataFrame"):
    """Log DataFrame schema information for debugging"""
    logger.info(f"{name} Schema:")
    logger.info(f"  Columns: {list(df.columns)}")
    logger.info(f"  Shape: {df.shape}")
    logger.info(f"  Data types:")
    for col, dtype in df.dtypes.items():
        logger.info(f"    {col}: {dtype}")

def convert_pandas_to_arrow(df):
    """Convert pandas DataFrame to Arrow Table for Delta Lake compatibility"""
    try:
        # Clean the DataFrame first
        df_cleaned = clean_dataframe_for_delta(df)
        
        # Convert to Arrow Table
        table = pa.Table.from_pandas(df_cleaned)
        return table
        
    except Exception as e:
        logger.error(f"Error converting DataFrame to Arrow: {str(e)}")
        logger.error(f"DataFrame info: shape={df.shape}, columns={list(df.columns)}")
        logger.error(f"DataFrame dtypes: {dict(df.dtypes)}")
        
        # Last resort fallback: convert everything to string
        try:
            logger.warning("Attempting last resort conversion - all columns to string")
            df_string = df.copy()
            for col in df_string.columns:
                df_string[col] = df_string[col].fillna('').astype(str)
            
            table = pa.Table.from_pandas(df_string)
            return table
        except Exception as final_error:
            logger.error(f"Final conversion attempt failed: {str(final_error)}")
            raise

def clean_dataframe_for_delta(df):
    """Clean and prepare DataFrame for Delta Lake writing"""
    df = df.copy()
    
    # Handle problematic data types
    for col in df.columns:
        if df[col].dtype == 'object':
            # Check if this appears to be a boolean column with string values
            unique_values = df[col].astype(str).str.strip().str.lower().unique()
            boolean_like_values = {'true', 'false', '1', '0', 'yes', 'no', 'y', 'n', '', 'nan', 'none', 'null'}
            
            if len(set(unique_values) - boolean_like_values) == 0 and len(unique_values) > 0:
                # This looks like a boolean column, try to convert it
                logger.info(f"Converting boolean-like column {col} to proper boolean type")
                boolean_mapping = {
                    'true': True, '1': True, 'yes': True, 'y': True,
                    'false': False, '0': False, 'no': False, 'n': False,
                    '': False, 'nan': False, 'none': False, 'null': False
                }
                df[col] = df[col].astype(str).str.strip().str.lower().map(boolean_mapping).fillna(False)
            else:
                # Convert object columns to string, handling None/NaN values
                df[col] = df[col].fillna('').astype(str)
                # Replace 'nan' and 'None' strings with empty strings
                df[col] = df[col].replace(['nan', 'None', 'null'], '')
        elif pd.api.types.is_integer_dtype(df[col]):
            # Ensure integer columns don't have NaN (fill with 0)
            df[col] = df[col].fillna(0)
        elif pd.api.types.is_float_dtype(df[col]):
            # Keep NaN for float columns as they're supported
            pass
        elif pd.api.types.is_bool_dtype(df[col]):
            # Fill NaN in boolean columns with False
            df[col] = df[col].fillna(False)
    
    # Log the final schema
    logger.debug(f"Cleaned DataFrame schema: {dict(df.dtypes)}")
    
    return df

def handle_schema_evolution(new_df, existing_df, delta_path):
    """
    Handle schema evolution by reconciling data types between existing and new data
    """
    logger.info(f"Handling schema evolution for {delta_path}")
    
    # Create a copy to avoid modifying the original
    df = new_df.copy()
    
    # Get column information
    existing_cols = set(existing_df.columns)
    new_cols = set(df.columns)
    
    # Handle columns that exist in both but might have different types
    common_cols = existing_cols & new_cols
    
    for col in common_cols:
        existing_dtype = existing_df[col].dtype
        new_dtype = df[col].dtype
        
        # Handle boolean fields that became object type
        if pd.api.types.is_bool_dtype(existing_dtype) and pd.api.types.is_object_dtype(new_dtype):
            logger.info(f"Converting column {col} from {new_dtype} to boolean to match existing schema")
            # Log sample values for debugging
            sample_values = df[col].dropna().astype(str).head(5).tolist()
            logger.debug(f"Sample values in {col}: {sample_values}")
            
            # Convert string values to boolean
            df[col] = df[col].astype(str).str.strip().str.lower()
            # Map string values to boolean
            boolean_mapping = {
                'true': True, '1': True, 'yes': True, 'y': True,
                'false': False, '0': False, 'no': False, 'n': False,
                '': False, 'nan': False, 'none': False, 'null': False
            }
            df[col] = df[col].map(boolean_mapping).fillna(False)
            logger.info(f"Successfully converted {col} to boolean type")
            
        # Handle integer fields that became float
        elif pd.api.types.is_integer_dtype(existing_dtype) and pd.api.types.is_float_dtype(new_dtype):
            logger.info(f"Converting column {col} from {new_dtype} to integer to match existing schema")
            df[col] = df[col].fillna(0).astype('int64')
            
        # Handle float fields that became object
        elif pd.api.types.is_float_dtype(existing_dtype) and pd.api.types.is_object_dtype(new_dtype):
            logger.info(f"Converting column {col} from {new_dtype} to float to match existing schema")
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            
        # Handle integer fields that became object
        elif pd.api.types.is_integer_dtype(existing_dtype) and pd.api.types.is_object_dtype(new_dtype):
            logger.info(f"Converting column {col} from {new_dtype} to integer to match existing schema")
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
    
    # Handle new columns that don't exist in existing data
    new_only_cols = new_cols - existing_cols
    if new_only_cols:
        logger.info(f"New columns detected: {list(new_only_cols)}")
        # These will be handled by schema evolution in Delta Lake
    
    # Handle columns that exist in existing data but not in new data
    missing_cols = existing_cols - new_cols
    if missing_cols:
        logger.info(f"Adding missing columns with default values: {list(missing_cols)}")
        for col in missing_cols:
            existing_dtype = existing_df[col].dtype
            if pd.api.types.is_string_dtype(existing_dtype) or pd.api.types.is_object_dtype(existing_dtype):
                df[col] = ""
            elif pd.api.types.is_bool_dtype(existing_dtype):
                df[col] = False
            elif pd.api.types.is_integer_dtype(existing_dtype):
                df[col] = 0
            elif pd.api.types.is_float_dtype(existing_dtype):
                df[col] = 0.0
            else:
                df[col] = None
    
    logger.info(f"Schema evolution complete for {delta_path}")
    return df

# Create the DAG
dag = DAG(
    'isic_delta_metadata_crawler',
    default_args=default_args,
    description='Daily ISIC metadata crawler with Delta Lake storage',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['isic', 'metadata', 'crawler', 'delta-lake'],
    max_active_runs=1,
)

# Define tasks
fetch_collections_task = PythonOperator(
    task_id='fetch_collections',
    python_callable=fetch_collections,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=5),
)

fetch_images_metadata_task = PythonOperator(
    task_id='fetch_images_metadata',
    python_callable=fetch_images_metadata,
    dag=dag,
    retries=2,
    retry_delay=timedelta(minutes=5),
)

download_images_task = PythonOperator(
    task_id='download_images_batch',
    python_callable=download_images_batch,
    dag=dag,
    retries=1,
    retry_delay=timedelta(minutes=5),
)

log_summary_task = PythonOperator(
    task_id='log_processing_summary',
    python_callable=log_processing_summary,
    dag=dag,
    retries=1,
    retry_delay=timedelta(minutes=1),
)

# Define task dependencies
fetch_collections_task >> fetch_images_metadata_task >> download_images_task >> log_summary_task


