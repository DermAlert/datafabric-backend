from datetime import datetime, timedelta
import json
import requests
import io
import logging
import time
import pandas as pd
from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator

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

# Bucket definitions
COLLECTIONS_BUCKET = "isic-collections"
IMAGES_METADATA_BUCKET = "isic-images-metadata"
STATS_BUCKET = "isic-statistics"

# API request parameters
REQUEST_TIMEOUT = 120
RETRY_COUNT = 3
RETRY_DELAY = 10
MAX_LIMIT = 10000

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
    """Fetch all collections and relevant information"""
    logger.info("Fetching all collections")
    
    # Get collections from API
    url = f"{ISIC_API_BASE}/collections"
    params = {"limit": MAX_LIMIT}
    response_data = api_request(url, params)
    
    if 'results' not in response_data:
        raise ValueError("Unexpected API response format")
    
    collections = response_data.get('results', [])
    logger.info(f"Retrieved {len(collections)} collections")
    
    # Save to MinIO
    ensure_bucket_exists(COLLECTIONS_BUCKET)
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
    
    # Filter and save valid collections only
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

def fetch_images_metadata(**context):
    """Fetch metadata for all images in valid collections"""
    logger.info("Fetching image metadata")
    
    client = get_minio_client()
    response = client.get_object(COLLECTIONS_BUCKET, "valid_collections_latest.json")
    valid_collections = json.loads(response.data.decode('utf-8'))
    response.close()
    
    logger.info(f"Processing {len(valid_collections)} collections")
    
    all_images = []
    total_images = 0
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    ensure_bucket_exists(IMAGES_METADATA_BUCKET)
    
    for idx, collection in enumerate(valid_collections):
        collection_id = collection['id']
        collection_name = collection['name']
        
        try:
            logger.info(f"Processing collection {idx+1}/{len(valid_collections)}: {collection_name} (ID: {collection_id})")
            
            url = f"{ISIC_API_BASE}/images/search"
            params = {
                "collections": str(collection_id),
                "limit": MAX_LIMIT
            }
            
            response_data = api_request(url, params)
            
            if 'results' not in response_data:
                logger.warning(f"Unexpected response structure for collection {collection_id}")
                continue
                
            images = response_data.get('results', [])
            
            for image in images:
                image['collection_id'] = collection_id
                image['collection_name'] = collection_name
            
            all_images.extend(images)
            total_images += len(images)
            
            logger.info(f"Retrieved {len(images)} images from collection {collection_id}")
            
            if len(all_images) >= 5000 or (idx == len(valid_collections) - 1 and len(all_images) > 0):
                batch_num = (idx // 10) + 1  
                
                batch_json = json.dumps(all_images)
                client.put_object(
                    IMAGES_METADATA_BUCKET,
                    f"images_batch_{batch_num}_{timestamp}.json",
                    io.BytesIO(batch_json.encode()),
                    length=len(batch_json),
                    content_type='application/json'
                )
                
                logger.info(f"Saved batch {batch_num} with {len(all_images)} images")
                
                # Clear memory
                all_images = []
            
        except Exception as e:
            logger.error(f"Error processing collection {collection_id}: {e}")
    
    logger.info(f"Total images fetched: {total_images}")
    
    # Create a manifest file
    manifest = {
        "timestamp": timestamp,
        "total_images": total_images,
        "collections_processed": len(valid_collections),
        "last_updated": datetime.now().isoformat()
    }
    
    manifest_json = json.dumps(manifest)
    client.put_object(
        IMAGES_METADATA_BUCKET,
        f"manifest_{timestamp}.json",
        io.BytesIO(manifest_json.encode()),
        length=len(manifest_json),
        content_type='application/json'
    )
    
    client.put_object(
        IMAGES_METADATA_BUCKET,
        "manifest_latest.json",
        io.BytesIO(manifest_json.encode()),
        length=len(manifest_json),
        content_type='application/json'
    )
    
    return {"timestamp": timestamp, "total_images": total_images}

def process_metadata(**context):
    """Process raw metadata into structured format"""
    logger.info("Processing image metadata")
    
    client = get_minio_client()
    ensure_bucket_exists(IMAGES_METADATA_BUCKET)
    
    # Get the manifest
    response = client.get_object(IMAGES_METADATA_BUCKET, "manifest_latest.json")
    manifest = json.loads(response.data.decode('utf-8'))
    response.close()
    
    timestamp = manifest.get("timestamp")
    total_images = manifest.get("total_images", 0)
    
    logger.info(f"Processing {total_images} images from timestamp {timestamp}")
    
    batch_objects = client.list_objects(IMAGES_METADATA_BUCKET, prefix=f"images_batch_")
    batch_files = [obj.object_name for obj in batch_objects]
    batch_files.sort()  # Ensure proper order
    
    # Process each batch
    all_records = []
    current_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    for batch_file in batch_files:
        if timestamp not in batch_file:
            continue  # Skip files from other runs
            
        try:
            # Load batch
            response = client.get_object(IMAGES_METADATA_BUCKET, batch_file)
            batch_data = json.loads(response.data.decode('utf-8'))
            response.close()
            
            # Process batch
            batch_records = []
            for image in batch_data:
                record = {
                    'isic_id': image.get('isic_id', ''),
                    'copyright_license': image.get('copyright_license', ''),
                    'collection_id': image.get('collection_id', ''),
                    'collection_name': image.get('collection_name', ''),
                    'public': image.get('public', False)
                }
                
                # Handle files info
                if 'files' in image and 'full' in image['files']:
                    record['image_url'] = image['files']['full'].get('url', '')
                    record['image_size'] = image['files']['full'].get('size', 0)
                
                # Extract metadata fields
                if 'metadata' in image:
                    for category, fields in image['metadata'].items():
                        for field, value in fields.items():
                            record[f"{category}_{field}"] = value
                
                batch_records.append(record)
            
            all_records.extend(batch_records)
            logger.info(f"Processed {len(batch_records)} records from {batch_file}")
            
        except Exception as e:
            logger.error(f"Error processing batch {batch_file}: {e}")
    
    df = pd.DataFrame(all_records)
    
    # Save as CSV
    if len(df) > 0:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        client.put_object(
            IMAGES_METADATA_BUCKET,
            f"processed_{current_timestamp}.csv",
            io.BytesIO(csv_data.encode()),
            length=len(csv_data),
            content_type='text/csv'
        )
        
        client.put_object(
            IMAGES_METADATA_BUCKET,
            "processed_latest.csv",
            io.BytesIO(csv_data.encode()),
            length=len(csv_data),
            content_type='text/csv'
        )
        
        logger.info(f"Saved processed data for {len(df)} images")
    else:
        logger.warning("No images to process")
    
    return {"processed_records": len(df), "timestamp": current_timestamp}

def generate_stats(**context):
    """Generate statistics from processed data"""
    logger.info("Generating statistics")
    
    try:
        client = get_minio_client()
        ensure_bucket_exists(STATS_BUCKET)
        
        # Load processed data
        response = client.get_object(IMAGES_METADATA_BUCKET, "processed_latest.csv")
        df = pd.read_csv(io.BytesIO(response.data))
        response.close()
        
        stats = {
            "timestamp": datetime.now().isoformat(),
            "total_images": len(df),
            "collections_count": df['collection_id'].nunique(),
            "collections": df['collection_name'].value_counts().head(20).to_dict(),
            "copyright_licenses": df['copyright_license'].value_counts().to_dict(),
            "public_images_count": int(df['public'].sum())
        }
        
        metadata_fields = {}
        for column in df.columns:
            if column.startswith(('clinical_', 'acquisition_')):
                try:
                    metadata_fields[column] = {
                        "count": df[column].nunique(),
                        "top_values": df[column].value_counts().head(5).to_dict()
                    }
                except Exception as e:
                    logger.warning(f"Error analyzing field {column}: {e}")
        
        stats["metadata_fields"] = metadata_fields
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        stats_json = json.dumps(stats)
        
        client.put_object(
            STATS_BUCKET,
            f"stats_{timestamp}.json",
            io.BytesIO(stats_json.encode()),
            length=len(stats_json),
            content_type='application/json'
        )
        
        client.put_object(
            STATS_BUCKET,
            "stats_latest.json",
            io.BytesIO(stats_json.encode()),
            length=len(stats_json),
            content_type='application/json'
        )
        
        logger.info(f"Generated statistics for {stats['total_images']} images")
        return stats
    
    except Exception as e:
        logger.error(f"Error generating statistics: {e}")
        return {"error": str(e)}

# Create the DAG
with DAG(
    'isic_simple_crawler',
    default_args=default_args,
    description='Simple ISIC Archive metadata crawler that avoids XCom serialization issues',
    schedule_interval='0 1 * * *',  
    start_date=datetime(2023, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=['isic', 'crawler', 'metadata'],
) as dag:

    fetch_collections_task = PythonOperator(
        task_id='fetch_collections',
        python_callable=fetch_collections,
        provide_context=True,
    )
    
    fetch_images_task = PythonOperator(
        task_id='fetch_images_metadata',
        python_callable=fetch_images_metadata,
        provide_context=True,
    )
    
    process_metadata_task = PythonOperator(
        task_id='process_metadata',
        python_callable=process_metadata,
        provide_context=True,
    )
    
    generate_stats_task = PythonOperator(
        task_id='generate_stats',
        python_callable=generate_stats,
        provide_context=True,
    )
    
    # Define task dependencies
    fetch_collections_task >> fetch_images_task >> process_metadata_task >> generate_stats_task