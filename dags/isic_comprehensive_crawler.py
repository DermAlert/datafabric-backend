from datetime import datetime, timedelta
import json
import requests
import io
import os
import logging
import time
import pandas as pd
import numpy as np
from urllib.parse import quote
from minio import Minio
from minio.error import S3Error
import hashlib
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, TaskInstance
from airflow.utils.session import provide_session
from airflow.models import XCom
from typing import List, Dict, Any, Optional, Union, Tuple

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

ISIC_API_BASE = "https://api.isic-archive.com/api/v2"
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_SECURE = False

COLLECTIONS_BUCKET = "isic-collections"
IMAGES_METADATA_BUCKET = "isic-images-metadata"
IMAGES_BUCKET = "isic-images"
QUERIES_BUCKET = "isic-queries"
DELTA_BUCKET = "isic-delta"

REQUEST_TIMEOUT = 30
RETRY_COUNT = 3
RETRY_DELAY = 5
PAGE_SIZE = 100

def get_minio_client():
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

def ensure_bucket_exists(client, bucket_name):
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Created bucket {bucket_name}")
    except Exception as e:
        logger.error(f"Error creating bucket {bucket_name}: {str(e)}")
        raise

def api_request(url, params=None, retry_count=RETRY_COUNT):
    for attempt in range(retry_count + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt < retry_count:
                sleep_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Request to {url} failed: {str(e)}. Retrying in {sleep_time}s...")
                time.sleep(sleep_time)
            else:
                logger.error(f"Request to {url} failed after {retry_count} attempts: {str(e)}")
                raise

def get_all_collections(**context):
    collections = []
    offset = 0
    total_obtained = 0
    total_available = float('inf')
    
    while total_obtained < total_available:
        params = {"limit": PAGE_SIZE, "offset": offset}
        url = f"{ISIC_API_BASE}/collections"
        try:
            response_data = api_request(url, params)
            
            if 'results' not in response_data:
                logger.error(f"Unexpected response structure: {response_data}")
                break
                
            batch = response_data['results']
            collections.extend(batch)
            
            total_available = response_data.get('count', len(batch))
            total_obtained += len(batch)
            offset += len(batch)
            
            logger.info(f"Retrieved {len(batch)} collections ({total_obtained}/{total_available})")
            
            if len(batch) == 0 or len(batch) < PAGE_SIZE:
                break
        except Exception as e:
            logger.error(f"Error retrieving collections: {str(e)}")
            break
    
    collections_hash = hashlib.md5(json.dumps(collections, sort_keys=True).encode()).hexdigest()
    context['ti'].xcom_push(key='collections_hash', value=collections_hash)
    
    logger.info(f"Total collections retrieved: {len(collections)}")
    return collections

def save_collections_to_minio(**context):
    collections = context['ti'].xcom_pull(task_ids='get_all_collections')
    if not collections:
        logger.error("No collections data available")
        return 0
        
    client = get_minio_client()
    ensure_bucket_exists(client, COLLECTIONS_BUCKET)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    json_data = json.dumps(collections)
    client.put_object(
        COLLECTIONS_BUCKET,
        f"collections_raw_{timestamp}.json",
        io.BytesIO(json_data.encode()),
        length=len(json_data),
        content_type='application/json'
    )
    
    client.put_object(
        COLLECTIONS_BUCKET,
        "collections_raw_latest.json",
        io.BytesIO(json_data.encode()),
        length=len(json_data),
        content_type='application/json'
    )
    
    df = pd.DataFrame(collections)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    
    client.put_object(
        COLLECTIONS_BUCKET,
        f"collections_{timestamp}.csv",
        io.BytesIO(csv_data.encode()),
        length=len(csv_data),
        content_type='text/csv'
    )
    
    client.put_object(
        COLLECTIONS_BUCKET,
        "collections_latest.csv",
        io.BytesIO(csv_data.encode()),
        length=len(csv_data),
        content_type='text/csv'
    )
    
    logger.info(f"Saved {len(collections)} collections to MinIO")
    return len(collections)

def get_images_by_collection(**context):
    collections = context['ti'].xcom_pull(task_ids='get_all_collections')
    if not collections:
        logger.error("No collections data available")
        return []
    
    all_images = []
    total_images = 0
    
    for collection in collections:
        collection_id = collection['id']
        collection_name = collection['name']
        
        logger.info(f"Processing collection '{collection_name}' (ID: {collection_id})")
        
        offset = 0
        collection_images = []
        
        while True:
            url = f"{ISIC_API_BASE}/images/search/"
            params = {"collections": collection_id, "limit": PAGE_SIZE, "offset": offset}
            
            try:
                response_data = api_request(url, params)
                
                if 'results' not in response_data:
                    logger.error(f"Unexpected response structure for collection {collection_id}: {response_data}")
                    break
                    
                batch = response_data['results']
                
                for image in batch:
                    image['collection_id'] = collection_id
                    image['collection_name'] = collection_name
                
                collection_images.extend(batch)
                total_batch = len(batch)
                
                logger.info(f"Collection {collection_id}: Retrieved {total_batch} images (offset {offset})")
                
                offset += total_batch
                
                if total_batch == 0 or total_batch < PAGE_SIZE:
                    break
                
            except Exception as e:
                logger.error(f"Error retrieving images for collection {collection_id}: {str(e)}")
                break
        
        logger.info(f"Completed collection {collection_id} with {len(collection_images)} images")
        total_images += len(collection_images)
        all_images.extend(collection_images)
    
    logger.info(f"Retrieved metadata for {total_images} images across {len(collections)} collections")
    
    client = get_minio_client()
    ensure_bucket_exists(client, IMAGES_METADATA_BUCKET)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    json_data = json.dumps(all_images)
    client.put_object(
        IMAGES_METADATA_BUCKET,
        f"images_by_collection_{timestamp}.json",
        io.BytesIO(json_data.encode()),
        length=len(json_data),
        content_type='application/json'
    )
    
    client.put_object(
        IMAGES_METADATA_BUCKET,
        "images_by_collection_latest.json",
        io.BytesIO(json_data.encode()),
        length=len(json_data),
        content_type='application/json'
    )
    
    return {"total_images": total_images, "timestamp": timestamp}

def get_images_by_query(**context):
    query = context['dag_run'].conf.get('query')
    if not query:
        logger.error("No query provided")
        return {"error": "No query provided"}
    
    logger.info(f"Executing query: {query}")
    
    encoded_query = quote(query)
    url = f"{ISIC_API_BASE}/images/search/"
    
    all_images = []
    offset = 0
    
    while True:
        params = {"query": query, "limit": PAGE_SIZE, "offset": offset}
        
        try:
            response_data = api_request(url, params)
            
            if 'results' not in response_data:
                logger.error(f"Unexpected response structure: {response_data}")
                break
                
            batch = response_data['results']
            all_images.extend(batch)
            
            logger.info(f"Query '{query}': Retrieved {len(batch)} images (offset {offset})")
            
            offset += len(batch)
            
            if len(batch) == 0 or len(batch) < PAGE_SIZE:
                break
            
        except Exception as e:
            logger.error(f"Error executing query '{query}': {str(e)}")
            break
    
    logger.info(f"Query '{query}' returned {len(all_images)} images")
    
    client = get_minio_client()
    ensure_bucket_exists(client, QUERIES_BUCKET)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    sanitized_query = query.replace(':', '_').replace(' ', '_').replace('"', '').replace('(', '').replace(')', '')[:50]
    
    json_data = json.dumps(all_images)
    filename = f"query_{sanitized_query}_{timestamp}.json"
    
    client.put_object(
        QUERIES_BUCKET,
        filename,
        io.BytesIO(json_data.encode()),
        length=len(json_data),
        content_type='application/json'
    )
    
    try:
        flattened_data = []
        
        for image in all_images:
            record = {
                'isic_id': image['isic_id'],
                'copyright_license': image.get('copyright_license', ''),
                'attribution': image.get('attribution', ''),
                'public': image.get('public', False)
            }
            
            if 'files' in image and 'full' in image['files']:
                record['image_url'] = image['files']['full'].get('url', '')
                record['image_size'] = image['files']['full'].get('size', 0)
                
            if 'metadata' in image:
                for category, fields in image['metadata'].items():
                    for field, value in fields.items():
                        record[f"{category}_{field}"] = value
            
            flattened_data.append(record)
        
        df = pd.DataFrame(flattened_data)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        csv_filename = f"query_{sanitized_query}_{timestamp}.csv"
        
        client.put_object(
            QUERIES_BUCKET,
            csv_filename,
            io.BytesIO(csv_data.encode()),
            length=len(csv_data),
            content_type='text/csv'
        )
    except Exception as e:
        logger.error(f"Error creating CSV for query '{query}': {str(e)}")
    
    return {
        "query": query,
        "image_count": len(all_images),
        "json_filename": filename,
        "csv_filename": csv_filename if 'csv_filename' in locals() else None
    }

def process_and_save_image_metadata(**context):
    images_info = context['ti'].xcom_pull(task_ids='get_images_by_collection')
    if not images_info or not isinstance(images_info, dict):
        logger.error("No valid images information available")
        return False
        
    client = get_minio_client()
    bucket_name = IMAGES_METADATA_BUCKET
    
    try:
        response = client.get_object(bucket_name, "images_by_collection_latest.json")
        image_data = json.loads(response.data.decode('utf-8'))
        response.close()
    except Exception as e:
        logger.error(f"Error loading image data: {str(e)}")
        return False
    
    flattened_data = []
    
    for image in image_data:
        record = {
            'isic_id': image['isic_id'],
            'copyright_license': image.get('copyright_license', ''),
            'attribution': image.get('attribution', ''),
            'collection_id': image.get('collection_id', ''),
            'collection_name': image.get('collection_name', ''),
            'public': image.get('public', False)
        }
        
        if 'files' in image and 'full' in image['files']:
            record['image_url'] = image['files']['full'].get('url', '')
            record['image_size'] = image['files']['full'].get('size', 0)
            
        if 'metadata' in image:
            for category, fields in image['metadata'].items():
                for field, value in fields.items():
                    record[f"{category}_{field}"] = value
        
        flattened_data.append(record)
    
    df = pd.DataFrame(flattened_data)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    
    client.put_object(
        bucket_name,
        f"images_metadata_{timestamp}.csv",
        io.BytesIO(csv_data.encode()),
        length=len(csv_data),
        content_type='text/csv'
    )
    
    client.put_object(
        bucket_name,
        "images_metadata_latest.csv",
        io.BytesIO(csv_data.encode()),
        length=len(csv_data),
        content_type='text/csv'
    )
    
    metadata_summary = {
        "total_images": len(df),
        "collections": df['collection_name'].value_counts().to_dict(),
        "metadata_fields": {col: df[col].nunique() for col in df.columns if col.startswith(('acquisition_', 'clinical_'))},
        "timestamp": timestamp
    }
    
    summary_json = json.dumps(metadata_summary)
    
    client.put_object(
        bucket_name,
        f"metadata_summary_{timestamp}.json",
        io.BytesIO(summary_json.encode()),
        length=len(summary_json),
        content_type='application/json'
    )
    
    client.put_object(
        bucket_name,
        "metadata_summary_latest.json",
        io.BytesIO(summary_json.encode()),
        length=len(summary_json),
        content_type='application/json'
    )
    
    logger.info(f"Saved processed metadata for {len(df)} images")
    return True

def download_images(**context):
    image_ids = context['dag_run'].conf.get('image_ids', [])
    query = context['dag_run'].conf.get('query')
    collection_id = context['dag_run'].conf.get('collection_id')
    limit = min(int(context['dag_run'].conf.get('limit', 10)), 100)
    
    if not any([image_ids, query, collection_id]):
        logger.error("No image selection criteria provided (image_ids, query, or collection_id)")
        return {"error": "No image selection criteria provided"}
    
    client = get_minio_client()
    ensure_bucket_exists(client, IMAGES_BUCKET)
    
    images_to_download = []
    
    if image_ids:
        logger.info(f"Downloading {len(image_ids)} specific images")
        
        for image_id in image_ids[:limit]:
            try:
                url = f"{ISIC_API_BASE}/images/{image_id}"
                image_data = api_request(url)
                
                if 'files' in image_data and 'full' in image_data['files'] and 'url' in image_data['files']['full']:
                    images_to_download.append({
                        'id': image_id,
                        'url': image_data['files']['full']['url']
                    })
                else:
                    logger.warning(f"Image {image_id} does not have a download URL")
            except Exception as e:
                logger.error(f"Error retrieving data for image {image_id}: {str(e)}")
    
    elif query:
        url = f"{ISIC_API_BASE}/images/search/"
        params = {"query": query, "limit": limit}
        
        try:
            response_data = api_request(url, params)
            
            if 'results' in response_data:
                for image in response_data['results']:
                    if 'isic_id' in image and 'files' in image and 'full' in image['files']:
                        images_to_download.append({
                            'id': image['isic_id'],
                            'url': image['files']['full']['url']
                        })
            
            logger.info(f"Found {len(images_to_download)} images matching query '{query}'")
        except Exception as e:
            logger.error(f"Error executing query '{query}': {str(e)}")
    
    elif collection_id:
        url = f"{ISIC_API_BASE}/images/search/"
        params = {"collections": collection_id, "limit": limit}
        
        try:
            response_data = api_request(url, params)
            
            if 'results' in response_data:
                for image in response_data['results']:
                    if 'isic_id' in image and 'files' in image and 'full' in image['files']:
                        images_to_download.append({
                            'id': image['isic_id'],
                            'url': image['files']['full']['url']
                        })
            
            logger.info(f"Found {len(images_to_download)} images in collection {collection_id}")
        except Exception as e:
            logger.error(f"Error retrieving images for collection {collection_id}: {str(e)}")
    
    successful_downloads = 0
    failed_downloads = 0
    skipped_downloads = 0
    
    for image in images_to_download:
        image_id = image['id']
        url = image['url']
        
        try:
            client.stat_object(IMAGES_BUCKET, f"{image_id}.jpg")
            logger.info(f"Image {image_id} already exists, skipping")
            skipped_downloads += 1
            continue
        except:
            pass
        
        try:
            for attempt in range(RETRY_COUNT + 1):
                try:
                    response = requests.get(url, timeout=REQUEST_TIMEOUT)
                    response.raise_for_status()
                    
                    client.put_object(
                        IMAGES_BUCKET,
                        f"{image_id}.jpg",
                        io.BytesIO(response.content),
                        length=len(response.content),
                        content_type='image/jpeg'
                    )
                    
                    successful_downloads += 1
                    logger.info(f"Successfully downloaded image {image_id}")
                    break
                except Exception as e:
                    if attempt < RETRY_COUNT:
                        sleep_time = RETRY_DELAY * (2 ** attempt)
                        logger.warning(f"Download of image {image_id} failed: {str(e)}. Retrying in {sleep_time}s...")
                        time.sleep(sleep_time)
                    else:
                        logger.error(f"Failed to download image {image_id} after {RETRY_COUNT} attempts: {str(e)}")
                        failed_downloads += 1
        except Exception as e:
            logger.error(f"Error processing image {image_id}: {str(e)}")
            failed_downloads += 1
    
    logger.info(f"Download summary: {successful_downloads} successful, {failed_downloads} failed, {skipped_downloads} skipped")
    
    return {
        "successful_downloads": successful_downloads,
        "failed_downloads": failed_downloads,
        "skipped_downloads": skipped_downloads,
        "total_processed": len(images_to_download)
    }

def extract_metadata_stats(**context):
    client = get_minio_client()
    
    try:
        response = client.get_object(IMAGES_METADATA_BUCKET, "images_metadata_latest.csv")
        df = pd.read_csv(io.BytesIO(response.data))
        response.close()
    except Exception as e:
        logger.error(f"Error loading metadata: {str(e)}")
        return {"error": str(e)}
    
    stats = {}
    
    stats['collections'] = {
        'total': df['collection_id'].nunique(),
        'top_5': df['collection_name'].value_counts().head(5).to_dict()
    }
    
    if 'clinical_diagnosis' in df.columns:
        stats['diagnosis'] = {
            'total_types': df['clinical_diagnosis'].nunique(),
            'top_5': df['clinical_diagnosis'].value_counts().head(5).to_dict()
        }
    
    if 'clinical_anatom_site_general' in df.columns:
        stats['anatomical_site'] = {
            'total_types': df['clinical_anatom_site_general'].nunique(),
            'distribution': df['clinical_anatom_site_general'].value_counts().to_dict()
        }
    
    if 'acquisition_image_type' in df.columns:
        stats['image_type'] = {
            'types': df['acquisition_image_type'].value_counts().to_dict()
        }
    
    stats['total_images'] = len(df)
    stats['copyright_licenses'] = df['copyright_license'].value_counts().to_dict()
    stats['public_images'] = int(df['public'].sum())
    stats['private_images'] = int((~df['public']).sum())
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    stats_json = json.dumps(stats)
    
    client.put_object(
        IMAGES_METADATA_BUCKET,
        f"metadata_stats_{timestamp}.json",
        io.BytesIO(stats_json.encode()),
        length=len(stats_json),
        content_type='application/json'
    )
    
    client.put_object(
        IMAGES_METADATA_BUCKET,
        "metadata_stats_latest.json",
        io.BytesIO(stats_json.encode()),
        length=len(stats_json),
        content_type='application/json'
    )
    
    logger.info(f"Generated metadata statistics for {stats['total_images']} images")
    return stats

with DAG(
    'isic_comprehensive_crawler',
    default_args=default_args,
    description='Comprehensive ISIC Archive crawler for collections and image metadata',
    schedule_interval='@weekly',
    start_date=datetime(2023, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=['isic', 'crawler', 'metadata'],
) as metadata_dag:

    get_collections_task = PythonOperator(
        task_id='get_all_collections',
        python_callable=get_all_collections,
        provide_context=True,
    )
    
    save_collections_task = PythonOperator(
        task_id='save_collections_to_minio',
        python_callable=save_collections_to_minio,
        provide_context=True,
    )
    
    get_images_task = PythonOperator(
        task_id='get_images_by_collection',
        python_callable=get_images_by_collection,
        provide_context=True,
    )
    
    process_metadata_task = PythonOperator(
        task_id='process_and_save_image_metadata',
        python_callable=process_and_save_image_metadata,
        provide_context=True,
    )
    
    extract_stats_task = PythonOperator(
        task_id='extract_metadata_stats',
        python_callable=extract_metadata_stats,
        provide_context=True,
    )
    
    get_collections_task >> save_collections_task >> get_images_task >> process_metadata_task >> extract_stats_task

with DAG(
    'isic_custom_query',
    default_args=default_args,
    description='Run custom queries against the ISIC Archive API',
    schedule_interval=None,
    start_date=datetime(2023, 5, 1),
    catchup=False,
    tags=['isic', 'query', 'metadata'],
) as query_dag:

    query_task = PythonOperator(
        task_id='get_images_by_query',
        python_callable=get_images_by_query,
        provide_context=True,
    )

with DAG(
    'isic_image_downloader',
    default_args=default_args,
    description='Download ISIC Archive images on demand',
    schedule_interval=None,
    start_date=datetime(2023, 5, 1),
    catchup=False,
    tags=['isic', 'download', 'images'],
) as download_dag:

    download_images_task = PythonOperator(
        task_id='download_images',
        python_callable=download_images,
        provide_context=True,
    )