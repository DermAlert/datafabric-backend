from datetime import datetime, timedelta
import json
import io
import os
import logging
import pandas as pd
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
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_SECURE = False

# Bucket definitions
PARQUET_BUCKET = "isic-parquet-p"
IMAGES_BUCKET = "isic-images-p"
THUMBNAILS_BUCKET = "isic-thumbnails-p"



# Delta Lake paths
DELTA_PATH_PREFIX = "delta"
DELTA_METADATA_PREFIX = f"{DELTA_PATH_PREFIX}/metadata"
DELTA_IMAGES_STATUS = f"{DELTA_PATH_PREFIX}/images_status"

# Download settings
MAX_WORKERS = 5
MAX_RETRIES = 3
DOWNLOAD_TIMEOUT = 60
DEFAULT_BATCH_SIZE = 100

def get_minio_client():
    """Get MinIO client"""
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

def ensure_bucket_exists(bucket_name):
    """Ensure bucket exists in MinIO"""
    client = get_minio_client()
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Created bucket {bucket_name}")
    except Exception as e:
        logger.error(f"Error creating bucket {bucket_name}: {str(e)}")
        raise

def setup_delta_environment():
    """Setup environment variables for Delta Lake"""
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

def object_exists(client, bucket_name, object_name):
    """Check if object exists in MinIO bucket"""
    try:
        client.stat_object(bucket_name, object_name)
        return True
    except:
        return False

def find_missing_images(**context):
    """Find images that haven't been downloaded yet"""
    logger.info("Finding images that need to be downloaded")
    
    client = get_minio_client()
    ensure_bucket_exists(PARQUET_BUCKET)
    ensure_bucket_exists(IMAGES_BUCKET)
    ensure_bucket_exists(THUMBNAILS_BUCKET)
    
    # Get batch size
    batch_size = int(context['dag_run'].conf.get('batch_size', DEFAULT_BATCH_SIZE))
    logger.info(f"Using batch size: {batch_size}")
    
    # Determine which collection to process
    collection_id = context['dag_run'].conf.get('collection_id', None)
    
    # Create a record of downloaded images
    downloaded_images = set()
    downloaded_thumbnails = set()
    
    try:
        # List all objects in images bucket
        objects = list(client.list_objects(IMAGES_BUCKET, recursive=True))
        for obj in objects:
            if obj.object_name.endswith('.jpg'):
                image_id = obj.object_name.split('.')[0]
                downloaded_images.add(image_id)
        
        # List all objects in thumbnails bucket
        objects = list(client.list_objects(THUMBNAILS_BUCKET, recursive=True))
        for obj in objects:
            if obj.object_name.endswith('.jpg'):
                image_id = obj.object_name.split('.')[0]
                downloaded_thumbnails.add(image_id)
                
        logger.info(f"Found {len(downloaded_images)} downloaded images and {len(downloaded_thumbnails)} thumbnails")
    except Exception as e:
        logger.error(f"Error listing downloaded images: {str(e)}")
        raise
    
    # Find all collection metadata files
    all_images = []
    
    try:
        if collection_id:
            # Process only specified collection
            if object_exists(client, PARQUET_BUCKET, f"{DELTA_METADATA_PREFIX}/collection_{collection_id}/latest.parquet"):
                logger.info(f"Processing collection {collection_id}")
                
                with tempfile.NamedTemporaryFile() as temp:
                    client.fget_object(
                        PARQUET_BUCKET, 
                        f"{DELTA_METADATA_PREFIX}/collection_{collection_id}/latest.parquet", 
                        temp.name
                    )
                    df = pd.read_parquet(temp.name)
                    
                    # Find missing images
                    df['image_downloaded'] = df['isic_id'].apply(lambda x: x in downloaded_images)
                    df['thumbnail_downloaded'] = df['isic_id'].apply(lambda x: x in downloaded_thumbnails)
                    
                    missing_df = df[~df['image_downloaded'] | ~df['thumbnail_downloaded']].copy()
                    all_images.extend(missing_df.to_dict('records'))
                    
                    logger.info(f"Found {len(missing_df)} missing images in collection {collection_id}")
            else:
                logger.warning(f"Collection {collection_id} metadata not found")
        else:
            # Process all collections
            objects = client.list_objects(PARQUET_BUCKET, prefix=f"{DELTA_METADATA_PREFIX}/")
            parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('latest.parquet')]
            
            logger.info(f"Found {len(parquet_files)} collection metadata files")
            
            for parquet_file in parquet_files:
                with tempfile.NamedTemporaryFile() as temp:
                    client.fget_object(PARQUET_BUCKET, parquet_file, temp.name)
                    df = pd.read_parquet(temp.name)
                    
                    # Find missing images
                    df['image_downloaded'] = df['isic_id'].apply(lambda x: x in downloaded_images)
                    df['thumbnail_downloaded'] = df['isic_id'].apply(lambda x: x in downloaded_thumbnails)
                    
                    missing_df = df[~df['image_downloaded'] | ~df['thumbnail_downloaded']].copy()
                    all_images.extend(missing_df.to_dict('records'))
                    
                    collection_id = parquet_file.split('/')[2]
                    logger.info(f"Found {len(missing_df)} missing images in collection {collection_id}")
    except Exception as e:
        logger.error(f"Error finding missing images: {str(e)}")
        raise
    
    # Limit to batch size
    if len(all_images) > batch_size:
        logger.info(f"Limiting to {batch_size} images")
        all_images = all_images[:batch_size]
    
    # Save list of missing images
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_data = json.dumps(all_images)
    
    client.put_object(
        PARQUET_BUCKET,
        f"{DELTA_IMAGES_STATUS}/missing_images_{timestamp}.json",
        io.BytesIO(json_data.encode()),
        length=len(json_data),
        content_type='application/json'
    )
    
    client.put_object(
        PARQUET_BUCKET,
        f"{DELTA_IMAGES_STATUS}/missing_images_latest.json",
        io.BytesIO(json_data.encode()),
        length=len(json_data),
        content_type='application/json'
    )
    
    logger.info(f"Found {len(all_images)} images to download")
    
    return {
        "timestamp": timestamp,
        "missing_images": len(all_images),
        "images_to_download": all_images
    }

def download_image(image_data):
    """Download a single image"""
    isic_id = image_data.get('isic_id')
    image_url = image_data.get('image_url')
    thumbnail_url = image_data.get('thumbnail_url')
    image_downloaded = image_data.get('image_downloaded', False)
    thumbnail_downloaded = image_data.get('thumbnail_downloaded', False)
    
    client = get_minio_client()
    result = {
        'isic_id': isic_id,
        'image_downloaded': image_downloaded,
        'thumbnail_downloaded': thumbnail_downloaded,
        'errors': []
    }
    
    # Download main image if needed
    if not image_downloaded and image_url:
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(image_url, timeout=DOWNLOAD_TIMEOUT)
                response.raise_for_status()
                
                client.put_object(
                    IMAGES_BUCKET,
                    f"{isic_id}.jpg",
                    io.BytesIO(response.content),
                    length=len(response.content),
                    content_type='image/jpeg'
                )
                
                result['image_downloaded'] = True
                logger.info(f"Downloaded image {isic_id}")
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    sleep_time = 2 * (attempt + 1)
                    logger.warning(f"Failed to download image {isic_id}, retry {attempt+1}: {str(e)}")
                    time.sleep(sleep_time)
                else:
                    error_msg = f"Failed to download image {isic_id} after {MAX_RETRIES} attempts: {str(e)}"
                    logger.error(error_msg)
                    result['errors'].append(error_msg)
    
    # Download thumbnail if needed
    if not thumbnail_downloaded and thumbnail_url:
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(thumbnail_url, timeout=DOWNLOAD_TIMEOUT)
                response.raise_for_status()
                
                client.put_object(
                    THUMBNAILS_BUCKET,
                    f"{isic_id}.jpg",
                    io.BytesIO(response.content),
                    length=len(response.content),
                    content_type='image/jpeg'
                )
                
                result['thumbnail_downloaded'] = True
                logger.info(f"Downloaded thumbnail {isic_id}")
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    sleep_time = 2 * (attempt + 1)
                    logger.warning(f"Failed to download thumbnail {isic_id}, retry {attempt+1}: {str(e)}")
                    time.sleep(sleep_time)
                else:
                    error_msg = f"Failed to download thumbnail {isic_id} after {MAX_RETRIES} attempts: {str(e)}"
                    logger.error(error_msg)
                    result['errors'].append(error_msg)
    
    return result

def download_missing_images(**context):
    """Download missing images in parallel"""
    missing_data = context['ti'].xcom_pull(task_ids='find_missing_images')
    
    if not missing_data or 'images_to_download' not in missing_data or not missing_data['images_to_download']:
        logger.info("No images to download")
        return {"downloaded": 0, "failed": 0, "errors": []}
    
    images_to_download = missing_data['images_to_download']
    logger.info(f"Downloading {len(images_to_download)} images")
    
    results = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_image = {executor.submit(download_image, image): image for image in images_to_download}
        
        for future in as_completed(future_to_image):
            image = future_to_image[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Error downloading image {image.get('isic_id', 'unknown')}: {str(e)}")
                results.append({
                    'isic_id': image.get('isic_id', 'unknown'),
                    'image_downloaded': False,
                    'thumbnail_downloaded': False,
                    'errors': [str(e)]
                })
    
    # Calculate statistics
    successful_images = len([r for r in results if r['image_downloaded'] and not r.get('image_downloaded', False)])
    successful_thumbnails = len([r for r in results if r['thumbnail_downloaded'] and not r.get('thumbnail_downloaded', False)])
    failed = len([r for r in results if len(r.get('errors', [])) > 0])
    
    logger.info(f"Downloaded {successful_images} images and {successful_thumbnails} thumbnails. Failed: {failed}")
    
    # Save download results
    client = get_minio_client()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    results_json = json.dumps(results)
    client.put_object(
        PARQUET_BUCKET,
        f"{DELTA_IMAGES_STATUS}/download_results_{timestamp}.json",
        io.BytesIO(results_json.encode()),
        length=len(results_json),
        content_type='application/json'
    )
    
    client.put_object(
        PARQUET_BUCKET,
        f"{DELTA_IMAGES_STATUS}/download_results_latest.json",
        io.BytesIO(results_json.encode()),
        length=len(results_json),
        content_type='application/json'
    )
    
    return {
        "timestamp": timestamp,
        "downloaded_images": successful_images,
        "downloaded_thumbnails": successful_thumbnails,
        "failed": failed,
        "total_processed": len(results)
    }

def update_image_status(**context):
    """Update image download status in collection metadata files"""
    download_results = context['ti'].xcom_pull(task_ids='download_missing_images')
    
    if not download_results:
        logger.warning("No download results available")
        return {"status": "no_results"}
    
    # If nothing was downloaded, skip update
    if download_results.get('downloaded_images', 0) == 0 and download_results.get('downloaded_thumbnails', 0) == 0:
        logger.info("No new images were downloaded, skipping status update")
        return {"status": "no_updates"}
    
    client = get_minio_client()
    
    # Get current status of images
    downloaded_images = set()
    downloaded_thumbnails = set()
    
    try:
        # List all objects in images bucket
        objects = list(client.list_objects(IMAGES_BUCKET, recursive=True))
        for obj in objects:
            if obj.object_name.endswith('.jpg'):
                image_id = obj.object_name.split('.')[0]
                downloaded_images.add(image_id)
        
        # List all objects in thumbnails bucket
        objects = list(client.list_objects(THUMBNAILS_BUCKET, recursive=True))
        for obj in objects:
            if obj.object_name.endswith('.jpg'):
                image_id = obj.object_name.split('.')[0]
                downloaded_thumbnails.add(image_id)
                
        logger.info(f"Found {len(downloaded_images)} downloaded images and {len(downloaded_thumbnails)} thumbnails")
    except Exception as e:
        logger.error(f"Error listing downloaded images: {str(e)}")
        raise
    
    # Update collection metadata files
    objects = client.list_objects(PARQUET_BUCKET, prefix=f"{DELTA_METADATA_PREFIX}/")
    parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('latest.parquet')]
    
    updated_collections = 0
    
    for parquet_file in parquet_files:
        try:
            # Read parquet file
            with tempfile.NamedTemporaryFile() as temp_in:
                client.fget_object(PARQUET_BUCKET, parquet_file, temp_in.name)
                df = pd.read_parquet(temp_in.name)
            
            # Update download status
            df['image_downloaded'] = df['isic_id'].apply(lambda x: x in downloaded_images)
            df['thumbnail_downloaded'] = df['isic_id'].apply(lambda x: x in downloaded_thumbnails)
            
            # Write updated parquet file
            collection_id = parquet_file.split('/')[2]
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Save timestamped version
            with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_out:
                df.to_parquet(temp_out.name, index=False)
                client.fput_object(
                    PARQUET_BUCKET,
                    f"{DELTA_METADATA_PREFIX}/collection_{collection_id}/collection_{collection_id}_{timestamp}.parquet",
                    temp_out.name,
                    content_type='application/octet-stream'
                )
            
            # Update latest version
            with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_out:
                df.to_parquet(temp_out.name, index=False)
                client.fput_object(
                    PARQUET_BUCKET,
                    parquet_file,
                    temp_out.name,
                    content_type='application/octet-stream'
                )
                
            logger.info(f"Updated image status for collection {collection_id}")
            updated_collections += 1
            
        except Exception as e:
            logger.error(f"Error updating image status for {parquet_file}: {str(e)}")
    
    logger.info(f"Updated image status for {updated_collections} collections")
    
    return {
        "status": "success",
        "updated_collections": updated_collections
    }

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    'isic_image_downloader-p',
    default_args=default_args,
    description='Download missing ISIC images',
    schedule_interval='0 */4 * * *',  # Every 4 hours
    start_date=datetime(2023, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=['isic', 'images', 'download'],
) as dag:

    find_missing_task = PythonOperator(
        task_id='find_missing_images',
        python_callable=find_missing_images,
        provide_context=True,
    )
    
    download_task = PythonOperator(
        task_id='download_missing_images',
        python_callable=download_missing_images,
        provide_context=True,
    )
    
    update_status_task = PythonOperator(
        task_id='update_image_status',
        python_callable=update_image_status,
        provide_context=True,
    )
    
    find_missing_task >> download_task >> update_status_task