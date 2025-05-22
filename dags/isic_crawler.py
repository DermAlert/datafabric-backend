from datetime import datetime, timedelta
import json
import requests
import io
import os
import logging
import pandas as pd
import hashlib
from minio import Minio
from minio.error import S3Error

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

ISIC_API_BASE = "https://api.isic-archive.com/api/v2"

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio" 
MINIO_SECRET_KEY = "minio123"
MINIO_SECURE = False

COLLECTIONS_BUCKET = "isic-collections"
METADATA_BUCKET = "isic-metadata"
CATEGORIES_BUCKET = "isic-categories"
IMAGES_BUCKET = "isic-images"

API_LIMIT = 100
DOWNLOAD_LIMIT = 10

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
            logging.info(f"Created bucket {bucket_name}")
    except Exception as e:
        logging.error(f"Error creating bucket {bucket_name}: {str(e)}")

def get_collections(**context):
    api_url = f"{ISIC_API_BASE}/collections"
    
    params = {
        "limit": 100,
        "offset": 0,
    }
    
    response = requests.get(api_url, params=params)
    response.raise_for_status()
    data = response.json()
    
    collection_list = []
    for collection in data['results']:
        collection_list.append({
            'id': collection.get('id'),
            'name': collection.get('name'),
            'description': collection.get('description'),
            'public': collection.get('public'),
            'pinned': collection.get('pinned'),
            'doi': collection.get('doi'),
            'created': collection.get('created'),
            'updated': collection.get('updated'),
            'image_count': collection.get('image_count'),
        })
    
    collections_hash = hashlib.md5(json.dumps(collection_list, sort_keys=True).encode()).hexdigest()
    context['ti'].xcom_push(key='collections_hash', value=collections_hash)
    context['ti'].xcom_push(key='collections', value=collection_list)
    
    logging.info(f"Retrieved {len(collection_list)} collections from ISIC Archive")
    return collection_list

def get_image_metadata(**context):
    api_url = f"{ISIC_API_BASE}/images"
    
    limit = API_LIMIT
    
    params = {
        "limit": limit,
        "offset": 0,
        "sort": "name",
    }
    
    response = requests.get(api_url, params=params)
    response.raise_for_status()
    data = response.json()
    
    image_list = []
    for image in data['results']:
        image_list.append({
            'id': image['isic_id'],
            'url': image.get("files", {}).get("full", {}).get("url"),
            'metadata': image.get("metadata"),
            'created': image.get("created"),
            'updated': image.get("updated"),
            'collection_ids': image.get("collection_ids", []),
        })
    
    metadata_hash = hashlib.md5(json.dumps(image_list, sort_keys=True).encode()).hexdigest()
    context['ti'].xcom_push(key='image_metadata_hash', value=metadata_hash)
    context['ti'].xcom_push(key='image_metadata', value=image_list)
    
    logging.info(f"Retrieved metadata for {len(image_list)} images")
    return image_list

def extract_categories(**context):
    image_list = context['ti'].xcom_pull(key='image_metadata', task_ids='get_image_metadata')
    
    categories = set()
    category_details = {}
    
    for image in image_list:
        if not image.get('metadata'):
            continue
            
        for key in image['metadata'].keys():
            categories.add(key)
            if key not in category_details:
                category_details[key] = {
                    'example_values': [image['metadata'][key]],
                    'count': 1
                }
            else:
                if len(category_details[key]['example_values']) < 5:  
                    if image['metadata'][key] not in category_details[key]['example_values']:
                        category_details[key]['example_values'].append(image['metadata'][key])
                category_details[key]['count'] += 1
    
    category_list = []
    for cat in sorted(categories):
        category_list.append({
            'name': cat,
            'count': category_details[cat]['count'],
            'examples': category_details[cat]['example_values'],
            'occurrence_percentage': (category_details[cat]['count'] / len(image_list)) * 100
        })
    
    context['ti'].xcom_push(key='categories', value=category_list)
    logging.info(f"Extracted {len(category_list)} unique metadata categories")
    
    return category_list

def check_for_updates(**context):
    client = get_minio_client()
    bucket_name = METADATA_BUCKET
    
    current_metadata_hash = context['ti'].xcom_pull(key='image_metadata_hash')
    current_collections_hash = context['ti'].xcom_pull(key='collections_hash')
    
    updates = {
        'metadata_updated': True,
        'collections_updated': True,
        'first_run': True
    }
    
    logging.info(f"Current hashes - Metadata: {current_metadata_hash}, Collections: {current_collections_hash}")
    
    ensure_bucket_exists(client, bucket_name)
    
    try:
        metadata_hash_exists = False
        collections_hash_exists = False
        
        try:
            client.stat_object(bucket_name, 'hash/metadata_hash.txt')
            metadata_hash_exists = True
        except:
            pass
            
        try:
            client.stat_object(bucket_name, 'hash/collections_hash.txt')
            collections_hash_exists = True
        except:
            pass
        
        if metadata_hash_exists:
            updates['first_run'] = False
            response = client.get_object(bucket_name, 'hash/metadata_hash.txt')
            old_hash = response.read().decode('utf-8')
            updates['metadata_updated'] = (old_hash != current_metadata_hash)
            logging.info(f"Found old metadata hash: {old_hash}")
            response.close()
            
        if collections_hash_exists:
            response = client.get_object(bucket_name, 'hash/collections_hash.txt')
            old_hash = response.read().decode('utf-8')
            updates['collections_updated'] = (old_hash != current_collections_hash)
            logging.info(f"Found old collections hash: {old_hash}")
            response.close()
    except Exception as e:
        logging.warning(f"Error checking for updates: {str(e)}")
        updates = {
            'metadata_updated': True,
            'collections_updated': True,
            'first_run': True
        }
        
    try:
        client.put_object(
            bucket_name,
            'hash/metadata_hash.txt',
            io.BytesIO(current_metadata_hash.encode()),
            length=len(current_metadata_hash)
        )
        
        client.put_object(
            bucket_name,
            'hash/collections_hash.txt',
            io.BytesIO(current_collections_hash.encode()),
            length=len(current_collections_hash)
        )
    except Exception as e:
        logging.error(f"Error saving hashes: {str(e)}")
    
    context['ti'].xcom_push(key='updates', value=updates)
    
    status = ', '.join([f"{k}: {v}" for k, v in updates.items()])
    logging.info(f"Update check complete: {status}")
    
    return updates

def save_collections_to_minio(**context):
    collections = context['ti'].xcom_pull(key='collections')
    updates = context['ti'].xcom_pull(key='updates', task_ids='check_for_updates')
    
    logging.info(f"Collections update status: {updates}")
    logging.info(f"Collections count: {len(collections) if collections else 0}")
    
    if not updates.get('collections_updated') and not updates.get('first_run'):
        logging.info("No updates to collections. Skipping save.")
        return 0
    
    df = pd.DataFrame(collections)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    client = get_minio_client()
    bucket_name = COLLECTIONS_BUCKET
    
    ensure_bucket_exists(client, bucket_name)
    
    date_str = datetime.now().strftime('%Y%m%d')
    csv_data = csv_buffer.getvalue()
    
    try:
        logging.info(f"Saving collections to {bucket_name}/collections_{date_str}.csv")
        client.put_object(
            bucket_name,
            f"collections_{date_str}.csv",
            io.BytesIO(csv_data.encode()),
            length=len(csv_data),
            content_type='text/csv'
        )
        
        logging.info(f"Saving collections to {bucket_name}/collections_latest.csv")
        client.put_object(
            bucket_name,
            "collections_latest.csv",
            io.BytesIO(csv_data.encode()),
            length=len(csv_data),
            content_type='text/csv'
        )
        
        logging.info(f"Successfully saved {len(collections)} collections to MinIO")
    except Exception as e:
        logging.error(f"Error saving collections to MinIO: {str(e)}")
    
    return len(collections)

def save_categories_to_minio(**context):
    categories = context['ti'].xcom_pull(key='categories')
    updates = context['ti'].xcom_pull(key='updates', task_ids='check_for_updates')
    
    logging.info(f"Categories update status: {updates}")
    logging.info(f"Categories count: {len(categories) if categories else 0}")
    
    if not updates.get('metadata_updated') and not updates.get('first_run'):
        logging.info("No updates to categories. Skipping save.")
        return 0
    
    df = pd.DataFrame(categories)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    client = get_minio_client()
    bucket_name = CATEGORIES_BUCKET
    
    ensure_bucket_exists(client, bucket_name)
    
    date_str = datetime.now().strftime('%Y%m%d')
    csv_data = csv_buffer.getvalue()
    
    try:
        client.put_object(
            bucket_name,
            f"categories_{date_str}.csv",
            io.BytesIO(csv_data.encode()),
            length=len(csv_data),
            content_type='text/csv'
        )
        
        client.put_object(
            bucket_name,
            "categories_latest.csv",
            io.BytesIO(csv_data.encode()),
            length=len(csv_data),
            content_type='text/csv'
        )
        
        logging.info(f"Successfully saved {len(categories)} categories to MinIO")
    except Exception as e:
        logging.error(f"Error saving categories to MinIO: {str(e)}")
    
    return len(categories)

def save_metadata_to_minio(**context):
    image_list = context['ti'].xcom_pull(key='image_metadata')
    updates = context['ti'].xcom_pull(key='updates', task_ids='check_for_updates')
    
    logging.info(f"Metadata update status: {updates}")
    logging.info(f"Image metadata count: {len(image_list) if image_list else 0}")
    
    if not updates.get('metadata_updated') and not updates.get('first_run'):
        logging.info("No updates to metadata. Skipping save.")
        return 0
    
    flattened_data = []
    for image in image_list:
        img_data = {
            'id': image['id'],
            'url': image['url'],
            'created': image.get('created'),
            'updated': image.get('updated'),
            'collection_ids': ','.join(image.get('collection_ids', [])),
        }
        
        if image.get('metadata'):
            for key, value in image['metadata'].items():
                img_data[f'meta_{key}'] = str(value)
                
        flattened_data.append(img_data)
    
    df = pd.DataFrame(flattened_data)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    client = get_minio_client()
    bucket_name = METADATA_BUCKET
    
    ensure_bucket_exists(client, bucket_name)
    
    date_str = datetime.now().strftime('%Y%m%d')
    csv_data = csv_buffer.getvalue()
    json_data = json.dumps(image_list)
    
    try:
        client.put_object(
            bucket_name,
            f"metadata_{date_str}.csv",
            io.BytesIO(csv_data.encode()),
            length=len(csv_data),
            content_type='text/csv'
        )
        
        client.put_object(
            bucket_name,
            "metadata_latest.csv",
            io.BytesIO(csv_data.encode()),
            length=len(csv_data),
            content_type='text/csv'
        )
        
        client.put_object(
            bucket_name,
            f"metadata_raw_{date_str}.json",
            io.BytesIO(json_data.encode()),
            length=len(json_data),
            content_type='application/json'
        )
        
        logging.info(f"Successfully saved metadata for {len(image_list)} images to MinIO")
    except Exception as e:
        logging.error(f"Error saving metadata to MinIO: {str(e)}")
    
    return len(image_list)

def download_images(**context):
    image_list = context['ti'].xcom_pull(key='image_metadata')
    updates = context['ti'].xcom_pull(key='updates', task_ids='check_for_updates')
    
    logging.info(f"Image download update status: {updates}")
    
    if not updates.get('metadata_updated') and not updates.get('first_run'):
        logging.info("No updates to metadata. Skipping image downloads.")
        return 0
    
    client = get_minio_client()
    bucket_name = IMAGES_BUCKET
    
    ensure_bucket_exists(client, bucket_name)
    
    existing_images = set()
    try:
        objects = client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            if obj.object_name.endswith('.jpg'):
                existing_images.add(obj.object_name.split('.')[0])
    except Exception as e:
        logging.warning(f"Error listing existing images: {str(e)}")
    
    download_limit = DOWNLOAD_LIMIT
    downloaded_count = 0
    skipped_count = 0
    error_count = 0
    
    for image in image_list:
        if downloaded_count >= download_limit:
            break
            
        image_id = image['id']
        
        if image_id in existing_images:
            skipped_count += 1
            continue
            
        try:
            if not image.get('url'):
                continue
                
            response = requests.get(image['url'])
            response.raise_for_status()
            
            client.put_object(
                bucket_name,
                f"{image_id}.jpg",
                io.BytesIO(response.content),
                length=len(response.content),
                content_type='image/jpeg'
            )
            
            downloaded_count += 1
                
        except Exception as e:
            error_count += 1
            logging.error(f"Error downloading image {image_id}: {str(e)}")
    
    logging.info(f"Downloaded {downloaded_count} new images, skipped {skipped_count} existing images, {error_count} errors")
    return downloaded_count

with DAG(
    'isic_archive_crawler',
    default_args=default_args,
    description='DAG for extracting data from ISIC Archive including images, collections, and categories',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 5, 5),
    catchup=False,
    tags=['crawler', 'isic', 'dermatology', 'datafabric'],
) as dag:

    get_collections_task = PythonOperator(
        task_id='get_collections',
        python_callable=get_collections,
        provide_context=True,
    )
    
    get_metadata_task = PythonOperator(
        task_id='get_image_metadata',
        python_callable=get_image_metadata,
        provide_context=True,
    )
    
    extract_categories_task = PythonOperator(
        task_id='extract_categories',
        python_callable=extract_categories,
        provide_context=True,
    )
    
    check_updates_task = PythonOperator(
        task_id='check_for_updates',
        python_callable=check_for_updates,
        provide_context=True,
    )
    
    save_collections_task = PythonOperator(
        task_id='save_collections_to_minio',
        python_callable=save_collections_to_minio,
        provide_context=True,
    )
    
    save_categories_task = PythonOperator(
        task_id='save_categories_to_minio',
        python_callable=save_categories_to_minio,
        provide_context=True,
    )
    
    save_metadata_task = PythonOperator(
        task_id='save_metadata_to_minio',
        python_callable=save_metadata_to_minio,
        provide_context=True,
    )
    
    download_images_task = PythonOperator(
        task_id='download_images',
        python_callable=download_images,
        provide_context=True,
    )
    
    [get_collections_task, get_metadata_task] >> check_updates_task
    get_metadata_task >> extract_categories_task
    check_updates_task >> [save_collections_task, save_metadata_task, download_images_task]
    extract_categories_task >> save_categories_task