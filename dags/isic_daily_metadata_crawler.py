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
            
            page_count = 1
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
                    image['collection_id'] = collection_id
                    image['collection_name'] = collection_name
                
                all_images.extend(images)
                total_images += len(images)
                
                logger.info(f"Retrieved {len(images)} images from collection {collection_id} page {page_count}")
                
                if len(all_images) >= 5000:
                    batch_num = (total_images // 5000)
                    
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
                
                url = response_data.get('next')
                if url:
                    logger.info(f"Next page URL: {url}")
                    params = None 
                
                page_count += 1
            
            logger.info(f"Completed fetching all pages for collection {collection_id}")
            
        except Exception as e:
            logger.error(f"Error processing collection {collection_id}: {e}")
    
    if all_images:
        batch_num = (total_images // 5000) + 1
        batch_json = json.dumps(all_images)
        client.put_object(
            IMAGES_METADATA_BUCKET,
            f"images_batch_{batch_num}_{timestamp}.json",
            io.BytesIO(batch_json.encode()),
            length=len(batch_json),
            content_type='application/json'
        )
        logger.info(f"Saved final batch {batch_num} with {len(all_images)} images")
    
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
    logger.info("Processing image metadata")
    
    client = get_minio_client()
    ensure_bucket_exists(IMAGES_METADATA_BUCKET)
    
    response = client.get_object(IMAGES_METADATA_BUCKET, "manifest_latest.json")
    manifest = json.loads(response.data.decode('utf-8'))
    response.close()
    
    timestamp = manifest.get("timestamp")
    total_images = manifest.get("total_images", 0)
    
    logger.info(f"Processing {total_images} images from timestamp {timestamp}")
    
    batch_objects = client.list_objects(IMAGES_METADATA_BUCKET, prefix=f"images_batch_")
    batch_files = [obj.object_name for obj in batch_objects]
    batch_files.sort()  
    
    combined_records = {}
    current_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    for batch_file in batch_files:
        if timestamp not in batch_file:
            continue  
            
        try:
            response = client.get_object(IMAGES_METADATA_BUCKET, batch_file)
            batch_data = json.loads(response.data.decode('utf-8'))
            response.close()
            
            for image in batch_data:
                isic_id = image.get('isic_id', '')
                
                if not isic_id:
                    logger.warning(f"Skipping image with no ISIC ID: {image}")
                    continue
                
                if isic_id not in combined_records:
                    combined_records[isic_id] = {
                        'isic_id': isic_id,
                        'copyright_license': image.get('copyright_license', ''),
                        'public': image.get('public', False),
                        'collection_id': [],
                        'collection_name': []
                    }
                    
                    if 'files' in image and 'full' in image['files']:
                        combined_records[isic_id]['image_url'] = image['files']['full'].get('url', '')
                        combined_records[isic_id]['image_size'] = image['files']['full'].get('size', 0)
                        
                    if 'files' in image and 'thumbnail_256' in image['files']:
                        combined_records[isic_id]['thumbnail_url'] = image['files']['thumbnail_256'].get('url', '')
                        combined_records[isic_id]['thumbnail_size'] = image['files']['thumbnail_256'].get('size', 0)
                
                collection_id = image.get('collection_id')
                collection_name = image.get('collection_name')
                
                if collection_id and collection_id not in combined_records[isic_id]['collection_id']:
                    combined_records[isic_id]['collection_id'].append(collection_id)
                
                if collection_name and collection_name not in combined_records[isic_id]['collection_name']:
                    combined_records[isic_id]['collection_name'].append(collection_name)
                
                if 'metadata' in image:
                    for category, fields in image['metadata'].items():
                        for field, value in fields.items():
                            field_key = f"{category}_{field}"
                            
                            if field_key in combined_records[isic_id]:
                                current_value = combined_records[isic_id][field_key]
                                
                                if isinstance(current_value, list):
                                    if value not in current_value:
                                        current_value.append(value)
                                else:
                                    if current_value != value:
                                        combined_records[isic_id][field_key] = [current_value, value]
                            else:
                                combined_records[isic_id][field_key] = value
            
            logger.info(f"Processed records from {batch_file}")
            
        except Exception as e:
            logger.error(f"Error processing batch {batch_file}: {e}")
    
    all_records = list(combined_records.values())
    
    for record in all_records:
        for key, value in record.items():
            if isinstance(value, list):
                record[key] = json.dumps(value)
    
    df = pd.DataFrame(all_records)
    
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
        
        json_data = json.dumps(all_records)
        client.put_object(
            IMAGES_METADATA_BUCKET,
            f"processed_{current_timestamp}.json",
            io.BytesIO(json_data.encode()),
            length=len(json_data),
            content_type='application/json'
        )
        
        client.put_object(
            IMAGES_METADATA_BUCKET,
            "processed_latest.json",
            io.BytesIO(json_data.encode()),
            length=len(json_data),
            content_type='application/json'
        )
        
        logger.info(f"Saved processed data for {len(df)} unique images")
    else:
        logger.warning("No images to process")
    
    return {"processed_records": len(df), "timestamp": current_timestamp}

def generate_stats(**context):
    logger.info("Generating statistics")
    
    try:
        client = get_minio_client()
        ensure_bucket_exists(STATS_BUCKET)
        
        response = client.get_object(IMAGES_METADATA_BUCKET, "processed_latest.json")
        records = json.loads(response.data.decode('utf-8'))
        response.close()
        
        df = pd.DataFrame(records)
        
        def count_values(series):
            counter = {}
            for item in series:
                if isinstance(item, str) and item.startswith('['):
                    try:
                        values = json.loads(item)
                        for val in values:
                            counter[val] = counter.get(val, 0) + 1
                    except:
                        counter[item] = counter.get(item, 0) + 1
                else:
                    counter[item] = counter.get(item, 0) + 1
            return counter
        
        collection_counts = {}
        for idx, row in df.iterrows():
            collections = row['collection_id']
            if isinstance(collections, str) and collections.startswith('['):
                try:
                    col_list = json.loads(collections)
                    for col in col_list:
                        collection_counts[col] = collection_counts.get(col, 0) + 1
                except:
                    pass
            elif collections:
                collection_counts[collections] = collection_counts.get(collections, 0) + 1
        
        # Calculate collection name statistics
        collection_name_counts = {}
        for idx, row in df.iterrows():
            names = row['collection_name']
            if isinstance(names, str) and names.startswith('['):
                try:
                    name_list = json.loads(names)
                    for name in name_list:
                        collection_name_counts[name] = collection_name_counts.get(name, 0) + 1
                except:
                    pass
            elif names:
                collection_name_counts[names] = collection_name_counts.get(names, 0) + 1
        
        stats = {
            "timestamp": datetime.now().isoformat(),
            "total_images": len(df),
            "collections_count": len(collection_counts),
            "collections": dict(sorted(collection_name_counts.items(), key=lambda x: x[1], reverse=True)[:20]),
            "copyright_licenses": count_values(df['copyright_license']),
            "public_images_count": int(df['public'].sum() if 'public' in df.columns and pd.api.types.is_bool_dtype(df['public']) else 0)
        }
        
        metadata_fields = {}
        for column in df.columns:
            if column.startswith(('clinical_', 'acquisition_')):
                try:
                    unique_values = set()
                    value_counts = {}
                    
                    for val in df[column]:
                        if isinstance(val, str) and val.startswith('['):
                            try:
                                values = json.loads(val)
                                for v in values:
                                    unique_values.add(v)
                                    value_counts[v] = value_counts.get(v, 0) + 1
                            except:
                                unique_values.add(val)
                                value_counts[val] = value_counts.get(val, 0) + 1
                        elif val is not None and val != '':
                            unique_values.add(val)
                            value_counts[val] = value_counts.get(val, 0) + 1
                    
                    top_values = dict(sorted(value_counts.items(), key=lambda x: x[1], reverse=True)[:5])
                    
                    metadata_fields[column] = {
                        "count": len(unique_values),
                        "top_values": top_values
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
    
    fetch_collections_task >> fetch_images_task >> process_metadata_task >> generate_stats_task