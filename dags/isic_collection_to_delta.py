from datetime import datetime, timedelta
import json
import io
import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import time
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
ISIC_API_BASE = "https://api.isic-archive.com/api/v2"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_SECURE = False

# Bucket definitions
COLLECTIONS_BUCKET = "isic-collections-p"
PARQUET_BUCKET = "isic-parquet-p"
RAW_METADATA_BUCKET = "isic-raw-metadata-p"

# Delta Lake paths
DELTA_PATH_PREFIX = "delta"
DELTA_COLLECTIONS = f"{DELTA_PATH_PREFIX}/collections"
DELTA_METADATA_PREFIX = f"{DELTA_PATH_PREFIX}/metadata"

# API request settings
REQUEST_TIMEOUT = 120
RETRY_COUNT = 3
RETRY_DELAY = 10
MAX_LIMIT = 10000  # Using the same value as your working code
MAX_WORKERS = 5
BATCH_SIZE = 5000  # Batch size for memory management

# MinIO client setup
def get_minio_client():
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

def ensure_bucket_exists(bucket_name):
    client = get_minio_client()
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Created bucket {bucket_name}")
    except Exception as e:
        logger.error(f"Error creating bucket {bucket_name}: {str(e)}")
        raise

def setup_delta_environment():
    os.environ["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
    os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
    os.environ["AWS_ENDPOINT_URL"] = f"http://{MINIO_ENDPOINT}"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
    
    try:
        client = get_minio_client()
        client.list_buckets()
    except Exception as e:
        logger.error(f"Failed to connect to MinIO: {str(e)}")
        raise

def api_request(url, params=None):
    """Make API request with retry logic"""
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
    """Fetch all collections from ISIC Archive using the same logic as the working code"""
    logger.info("Fetching all collections")
    
    # Get collections from API
    url = f"{ISIC_API_BASE}/collections"
    params = {"limit": MAX_LIMIT}
    response_data = api_request(url, params)
    
    if 'results' not in response_data:
        raise ValueError("Unexpected API response format")
    
    collections = response_data.get('results', [])
    logger.info(f"Retrieved {len(collections)} collections")
    
    # Handle pagination to get all collections
    next_url = response_data.get('next')
    while next_url:
        logger.info(f"Fetching next page of collections from: {next_url}")
        response_data = api_request(next_url)
        if 'results' in response_data:
            collections.extend(response_data.get('results', []))
        next_url = response_data.get('next')
        logger.info(f"Total collections now: {len(collections)}")
    
    # Save to MinIO
    ensure_bucket_exists(COLLECTIONS_BUCKET)
    ensure_bucket_exists(PARQUET_BUCKET)
    
    client = get_minio_client()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save as JSON
    json_data = json.dumps(collections)
    client.put_object(
        COLLECTIONS_BUCKET,
        f"collections_{timestamp}.json",
        io.BytesIO(json_data.encode()),
        length=len(json_data),
        content_type='application/json'
    )
    
    client.put_object(
        COLLECTIONS_BUCKET,
        "collections_latest.json",
        io.BytesIO(json_data.encode()),
        length=len(json_data),
        content_type='application/json'
    )
    
    # Process collections into DataFrame - using the same fields as your working code
    collection_records = []
    for coll in collections:
        try:
            record = {
                'id': coll.get('id'),
                'name': coll.get('name', ''),
                'description': coll.get('description', ''),
                'public': coll.get('public', False),
                'pinned': coll.get('pinned', False),
                'image_count': coll.get('image_count', 0),
                'updated': coll.get('updated', '')
            }
            collection_records.append(record)
        except Exception as e:
            logger.warning(f"Error processing collection record: {e}")
    
    # Convert to DataFrame and save as Parquet
    df = pd.DataFrame(collection_records)
    
    # Save to parquet file
    parquet_file = f"collections_{timestamp}.parquet"
    with tempfile.NamedTemporaryFile(suffix='.parquet') as temp:
        df.to_parquet(temp.name, index=False)
        client.fput_object(
            PARQUET_BUCKET,
            f"{DELTA_COLLECTIONS}/{parquet_file}",
            temp.name,
            content_type='application/octet-stream'
        )
    
    # Save latest parquet file
    with tempfile.NamedTemporaryFile(suffix='.parquet') as temp:
        df.to_parquet(temp.name, index=False)
        client.fput_object(
            PARQUET_BUCKET,
            f"{DELTA_COLLECTIONS}/collections_latest.parquet",
            temp.name,
            content_type='application/octet-stream'
        )
    
    logger.info(f"Saved collections as Parquet file in {PARQUET_BUCKET}/{DELTA_COLLECTIONS}")
    
    # Filter and save valid collections - USING EXACTLY THE SAME LOGIC AS YOUR WORKING CODE
    valid_collections = []
    for collection in collections:
        try:
            cid = collection.get('id')
            if isinstance(cid, int) or (isinstance(cid, str) and cid.isdigit()):
                valid_collections.append({
                    'id': int(cid),
                    'name': collection.get('name', f"Collection {cid}")
                })
        except Exception:
            pass
    
    logger.info(f"Found {len(valid_collections)} valid collections")
    
    # Save valid collections for next task - this works around XCom serialization issues
    valid_json = json.dumps(valid_collections)
    client.put_object(
        COLLECTIONS_BUCKET,
        f"valid_collections_{timestamp}.json",
        io.BytesIO(valid_json.encode()),
        length=len(valid_json),
        content_type='application/json'
    )
    
    client.put_object(
        COLLECTIONS_BUCKET,
        "valid_collections_latest.json",
        io.BytesIO(valid_json.encode()),
        length=len(valid_json),
        content_type='application/json'
    )
    
    return {"timestamp": timestamp, "valid_count": len(valid_collections)}

def fetch_collection_metadata(collection, **context):
    """Fetch metadata for a specific collection"""
    collection_id = collection["id"]
    collection_name = collection["name"]
    
    logger.info(f"Processing collection: {collection_name} (ID: {collection_id})")
    
    ensure_bucket_exists(RAW_METADATA_BUCKET)
    client = get_minio_client()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    url = f"{ISIC_API_BASE}/images/search"
    params = {
        "collections": str(collection_id),
        "limit": MAX_LIMIT
    }
    
    all_images = []
    total_images = 0
    page_count = 1
    batch_count = 0
    
    while url:
        logger.info(f"Fetching page {page_count} for collection {collection_id}")
        
        try:
            if page_count == 1:
                response_data = api_request(url, params)
            else:
                response_data = api_request(url)
            
            if 'results' not in response_data:
                logger.warning(f"Unexpected response structure for collection {collection_id}")
                break
                
            images = response_data.get('results', [])
            
            for image in images:
                image['collection_id'] = collection_id
                image['collection_name'] = collection_name
            
            all_images.extend(images)
            total_images += len(images)
            
            logger.info(f"Retrieved {len(images)} images from collection {collection_id} page {page_count}")
            
            # Save batch if we've accumulated enough images to manage memory
            if len(all_images) >= BATCH_SIZE:
                batch_count += 1
                batch_json = json.dumps(all_images)
                
                client.put_object(
                    RAW_METADATA_BUCKET,
                    f"collection_{collection_id}_batch_{batch_count}_{timestamp}.json",
                    io.BytesIO(batch_json.encode()),
                    length=len(batch_json),
                    content_type='application/json'
                )
                
                logger.info(f"Saved batch {batch_count} with {len(all_images)} images for collection {collection_id}")
                all_images = []  # Clear memory
            
            url = response_data.get('next')
            if url:
                logger.info(f"Next page URL for collection {collection_id}: {url}")
                params = None
            
            page_count += 1
            
        except Exception as e:
            logger.error(f"Error fetching data for collection {collection_id}: {e}")
            break
    
    # Save any remaining images
    if all_images:
        batch_count += 1
        batch_json = json.dumps(all_images)
        
        client.put_object(
            RAW_METADATA_BUCKET,
            f"collection_{collection_id}_batch_{batch_count}_{timestamp}.json",
            io.BytesIO(batch_json.encode()),
            length=len(batch_json),
            content_type='application/json'
        )
        
        logger.info(f"Saved final batch {batch_count} with {len(all_images)} images for collection {collection_id}")
    
    logger.info(f"Total images fetched for collection {collection_id}: {total_images}")
    
    # Create a manifest file for this collection
    manifest = {
        "collection_id": collection_id,
        "collection_name": collection_name,
        "timestamp": timestamp,
        "total_images": total_images,
        "batch_count": batch_count,
        "pages_processed": page_count - 1
    }
    
    manifest_json = json.dumps(manifest)
    client.put_object(
        RAW_METADATA_BUCKET,
        f"collection_{collection_id}_manifest_{timestamp}.json",
        io.BytesIO(manifest_json.encode()),
        length=len(manifest_json),
        content_type='application/json'
    )
    
    client.put_object(
        RAW_METADATA_BUCKET,
        f"collection_{collection_id}_manifest_latest.json",
        io.BytesIO(manifest_json.encode()),
        length=len(manifest_json),
        content_type='application/json'
    )
    
    # Now process all batches into a single parquet file
    processed_records = []
    
    # Get all batch files for this collection and timestamp
    objects = client.list_objects(RAW_METADATA_BUCKET, prefix=f"collection_{collection_id}_batch_")
    batch_files = [obj.object_name for obj in objects if timestamp in obj.object_name]
    batch_files.sort()
    
    for batch_file in batch_files:
        try:
            response = client.get_object(RAW_METADATA_BUCKET, batch_file)
            batch_data = json.loads(response.data.decode('utf-8'))
            response.close()
            
            for image in batch_data:
                isic_id = image.get('isic_id', '')
                
                if not isic_id:
                    continue
                    
                record = {
                    'isic_id': isic_id,
                    'copyright_license': image.get('copyright_license', ''),
                    'public': image.get('public', False),
                    'collection_id': collection_id,
                    'collection_name': collection_name
                }
                
                # Add image URLs
                if 'files' in image:
                    if 'full' in image['files']:
                        record['image_url'] = image['files']['full'].get('url', '')
                        record['image_size'] = image['files']['full'].get('size', 0)
                    
                    if 'thumbnail_256' in image['files']:
                        record['thumbnail_url'] = image['files']['thumbnail_256'].get('url', '')
                        record['thumbnail_size'] = image['files']['thumbnail_256'].get('size', 0)
                
                # Extract all metadata fields
                if 'metadata' in image:
                    for category, fields in image['metadata'].items():
                        for field, value in fields.items():
                            field_key = f"{category}_{field}"
                            record[field_key] = value
                
                processed_records.append(record)
                
            logger.info(f"Processed {len(batch_data)} records from {batch_file}")
            
        except Exception as e:
            logger.error(f"Error processing batch file {batch_file}: {e}")
    
    # Convert to DataFrame and save as Parquet
    if processed_records:
        df = pd.DataFrame(processed_records)
        
        # Create directory structure if needed
        collection_dir = f"{DELTA_METADATA_PREFIX}/collection_{collection_id}"
        
        # Save to parquet file
        parquet_file = f"collection_{collection_id}_{timestamp}.parquet"
        with tempfile.NamedTemporaryFile(suffix='.parquet') as temp:
            df.to_parquet(temp.name, index=False)
            client.fput_object(
                PARQUET_BUCKET,
                f"{collection_dir}/{parquet_file}",
                temp.name,
                content_type='application/octet-stream'
            )
        
        # Save latest parquet file
        with tempfile.NamedTemporaryFile(suffix='.parquet') as temp:
            df.to_parquet(temp.name, index=False)
            client.fput_object(
                PARQUET_BUCKET,
                f"{collection_dir}/latest.parquet",
                temp.name,
                content_type='application/octet-stream'
            )
        
        logger.info(f"Saved collection {collection_id} metadata as Parquet with {len(df)} records")
        return {"collection_id": collection_id, "images_processed": len(df), "status": "success"}
    else:
        logger.warning(f"No valid records found for collection {collection_id}")
        return {"collection_id": collection_id, "images_processed": 0, "status": "no_data"}

def process_collections(**context):
    """Process collections in parallel"""
    logger.info("Starting to process collections")
    
    client = get_minio_client()
    
    # Read collections directly from MinIO - just like your working code
    try:
        response = client.get_object(COLLECTIONS_BUCKET, "valid_collections_latest.json")
        valid_collections = json.loads(response.data.decode('utf-8'))
        response.close()
    except Exception as e:
        logger.error(f"Failed to read valid_collections_latest.json: {e}")
        return {"error": "Failed to read valid collections"}
    
    logger.info(f"Processing {len(valid_collections)} collections")
    
    # Early return if no collections are found
    if not valid_collections:
        logger.warning("No collections to process, skipping")
        return {
            "processed": 0,
            "failed": 0,
            "empty": 0,
            "total_images": 0,
            "results": []
        }
    
    results = []
    
    # Process collections in chunks to avoid too many concurrent tasks
    chunk_size = max(1, min(10, len(valid_collections)))
    for i in range(0, len(valid_collections), chunk_size):
        chunk = valid_collections[i:i+chunk_size]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_collection = {executor.submit(fetch_collection_metadata, collection): collection for collection in chunk}
            
            for future in as_completed(future_to_collection):
                collection = future_to_collection[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error processing collection {collection['id']}: {str(e)}")
                    results.append({"collection_id": collection['id'], "error": str(e), "status": "failed"})
    
    # Summary statistics
    processed = len([r for r in results if r.get('status') == 'success'])
    failed = len([r for r in results if r.get('status') == 'failed'])
    empty = len([r for r in results if r.get('status') == 'no_data'])
    total_images = sum([r.get('images_processed', 0) for r in results])
    
    logger.info(f"Collection processing complete. Processed: {processed}, Failed: {failed}, Empty: {empty}, Total Images: {total_images}")
    
    return {
        "processed": processed,
        "failed": failed,
        "empty": empty,
        "total_images": total_images,
        "results": results
    }

def create_collection_schema_registry(**context):
    """Create schema registry for all collections"""
    logger.info("Creating schema registry for all collections")
    
    ensure_bucket_exists(PARQUET_BUCKET)
    client = get_minio_client()
    
    # Get all collection metadata files
    objects = client.list_objects(PARQUET_BUCKET, prefix=f"{DELTA_METADATA_PREFIX}/")
    parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('latest.parquet')]
    
    logger.info(f"Found {len(parquet_files)} collection metadata files")
    
    if not parquet_files:
        return {"error": "No collection metadata files found"}
    
    # Collect schema information
    schemas = {}
    all_columns = set()
    
    for parquet_file in parquet_files:
        try:
            # Extract collection ID from path
            parts = parquet_file.split('/')
            if len(parts) >= 3:
                collection_id = parts[2]
            else:
                continue
            
            with tempfile.NamedTemporaryFile() as temp:
                client.fget_object(PARQUET_BUCKET, parquet_file, temp.name)
                df = pd.read_parquet(temp.name)
                
                columns = df.columns.tolist()
                all_columns.update(columns)
                
                value_counts = {}
                for col in columns:
                    if col not in ['isic_id', 'image_url', 'thumbnail_url', 'collection_id', 'collection_name']:
                        try:
                            vc = df[col].value_counts().head(10).to_dict()
                            value_counts[col] = vc
                        except:
                            pass
                
                schemas[collection_id] = {
                    "columns": columns,
                    "record_count": len(df),
                    "common_values": value_counts
                }
                
                logger.info(f"Processed schema for collection {collection_id} with {len(df)} records and {len(columns)} columns")
                
        except Exception as e:
            logger.error(f"Error processing schema for {parquet_file}: {str(e)}")
    
    # Create global schema registry
    schema_registry = {
        "timestamp": datetime.now().isoformat(),
        "all_columns": sorted(list(all_columns)),
        "collection_schemas": schemas
    }
    
    # Save schema registry
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    schema_json = json.dumps(schema_registry)
    
    client.put_object(
        PARQUET_BUCKET,
        f"{DELTA_PATH_PREFIX}/schema_registry_{timestamp}.json",
        io.BytesIO(schema_json.encode()),
        length=len(schema_json),
        content_type='application/json'
    )
    
    client.put_object(
        PARQUET_BUCKET,
        f"{DELTA_PATH_PREFIX}/schema_registry_latest.json",
        io.BytesIO(schema_json.encode()),
        length=len(schema_json),
        content_type='application/json'
    )
    
    logger.info(f"Created schema registry with {len(schemas)} collections and {len(all_columns)} unique columns")
    
    return {
        "collections_processed": len(schemas),
        "unique_columns": len(all_columns)
    }

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(hours=6),
}

with DAG(
    'isic_metadata_collector-p',
    default_args=default_args,
    description='Collect ISIC metadata by collection and save as Parquet files',
    schedule_interval='0 0 * * 0',  # Weekly on Sunday at midnight
    start_date=datetime(2023, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=['isic', 'delta', 'metadata'],
) as dag:

    fetch_collections_task = PythonOperator(
        task_id='fetch_collections',
        python_callable=fetch_collections,
        provide_context=True,
    )
    
    process_collections_task = PythonOperator(
        task_id='process_collections',
        python_callable=process_collections,
        provide_context=True,
    )
    
    create_schema_registry_task = PythonOperator(
        task_id='create_schema_registry',
        python_callable=create_collection_schema_registry,
        provide_context=True,
    )
    
    fetch_collections_task >> process_collections_task >> create_schema_registry_task