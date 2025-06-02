from datetime import datetime, timedelta
import json
import io
import os
import logging
import pandas as pd
import numpy as np
import hashlib
import pyarrow.parquet as pq
import pyarrow as pa
from minio import Minio
from minio.error import S3Error
import tempfile
import shutil
import requests
import traceback

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from deltalake import DeltaTable, write_deltalake
    DELTA_LAKE_AVAILABLE = True
except ImportError as e:
    logger.error(f"Failed to import Delta Lake: {str(e)}")
    DELTA_LAKE_AVAILABLE = False

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_SECURE = False

RAW_BUCKET = "isic-raw"
DELTA_BUCKET = "isic-delta"
PROCESSED_BUCKET = "isic-processed"
PARQUET_BUCKET = "isic-parquet"
COLLECTIONS_BUCKET = "isic-collections"
IMAGES_METADATA_BUCKET = "isic-images-metadata"
IMAGES_BUCKET = "isic-images"

DELTA_PATH_PREFIX = "delta"
DELTA_COLLECTIONS = f"{DELTA_PATH_PREFIX}/collections"
DELTA_METADATA = f"{DELTA_PATH_PREFIX}/metadata"
DELTA_CATEGORIES = f"{DELTA_PATH_PREFIX}/categories"
DELTA_UNIFIED = f"{DELTA_PATH_PREFIX}/unified"

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
        else:
            logger.info(f"Bucket {bucket_name} already exists")
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

def get_storage_options():
    return {
        "endpoint_url": f"http://{MINIO_ENDPOINT}",
        "access_key_id": MINIO_ACCESS_KEY,
        "secret_access_key": MINIO_SECRET_KEY,
        "region": "us-east-1",
        "allow_unsafe_rename": "true"
    }

def read_csv_with_error_handling(data):
    try:
        return pd.read_csv(io.BytesIO(data), on_bad_lines='skip')
    except Exception as e:
        logger.warning(f"First CSV read attempt failed: {str(e)}")
        
    try:
        text_data = data.decode('utf-8', errors='replace')
        lines = text_data.splitlines()
        fixed_lines = []
        
        for line in lines:
            if line.count('"') % 2 == 1:
                line = line + '"'
            fixed_lines.append(line)
        
        fixed_data = '\n'.join(fixed_lines)
        return pd.read_csv(io.StringIO(fixed_data))
    except Exception as e:
        logger.warning(f"Second CSV read attempt failed: {str(e)}")
    
    try:
        import csv
        return pd.read_csv(
            io.BytesIO(data),
            engine='python',
            sep=None,
            quotechar='"',    
            escapechar='\\',
            on_bad_lines='skip',
            quoting=csv.QUOTE_MINIMAL
        )
    except Exception as e:
        logger.error(f"All CSV read attempts failed: {str(e)}")
        raise

def read_json_with_error_handling(data_bytes):
    try:
        return json.loads(data_bytes.decode('utf-8'))
    except Exception as e:
        logger.warning(f"Standard JSON parsing failed: {str(e)}")
    
    try:
        import ujson
        return ujson.loads(data_bytes.decode('utf-8'))
    except Exception as e:
        logger.warning(f"ujson parsing failed: {str(e)}")
        
    try:
        text_data = data_bytes.decode('utf-8', errors='replace')
        return json.loads(text_data)
    except Exception as e:
        logger.error(f"All JSON parsing attempts failed: {str(e)}")
        raise

def save_as_parquet_in_minio(df, bucket_name, object_name):
    client = get_minio_client()
    ensure_bucket_exists(client, bucket_name)
    
    temp_path = f"/tmp/{object_name}.parquet"
    
    try:
        df.to_parquet(temp_path, index=False)
        client.fput_object(
            bucket_name, 
            f"{object_name}.parquet", 
            temp_path
        )
        logger.info(f"Successfully saved DataFrame as parquet to {bucket_name}/{object_name}.parquet")
        return True
    except Exception as e:
        logger.error(f"Error saving DataFrame as parquet: {str(e)}")
        return False
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

def write_delta_table(df, delta_dir_path):
    try:
        write_deltalake(
            delta_dir_path,
            df
        )
        logger.info(f"Created Delta table locally at {delta_dir_path}")
        return True
    except Exception as e:
        logger.error(f"Error creating Delta table: {str(e)}")
        raise

def upload_delta_to_minio(local_dir, minio_bucket, minio_prefix):
    client = get_minio_client()
    
    for root, _, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            rel_path = os.path.relpath(local_path, os.path.dirname(local_dir))
            minio_path = f"{minio_prefix}/{rel_path}" if minio_prefix else rel_path
            
            try:
                client.fput_object(
                    minio_bucket,
                    minio_path,
                    local_path
                )
                logger.info(f"Uploaded {rel_path} to {minio_bucket}/{minio_path}")
            except Exception as e:
                logger.error(f"Error uploading {rel_path}: {str(e)}")
                raise

def object_exists(client, bucket_name, object_name):
    try:
        client.stat_object(bucket_name, object_name)
        return True
    except:
        return False

def get_dataframe_from_parquet(client, bucket_name, object_name):
    temp_path = f"/tmp/{object_name}"
    
    try:
        client.fget_object(bucket_name, object_name, temp_path)
        df = pd.read_parquet(temp_path)
        return df
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

def collections_to_delta(**context):
    if not DELTA_LAKE_AVAILABLE:
        logger.error("Delta Lake library not available. Skipping delta conversion.")
        return False
        
    client = get_minio_client()
    ensure_bucket_exists(client, DELTA_BUCKET)
    ensure_bucket_exists(client, PARQUET_BUCKET)
    setup_delta_environment()
    
    try:
        if not client.bucket_exists(COLLECTIONS_BUCKET):
            logger.error(f"Source bucket '{COLLECTIONS_BUCKET}' does not exist!")
            return False
            
        try:
            client.stat_object(COLLECTIONS_BUCKET, "collections_latest.csv")
            logger.info("Found collections_latest.csv file")
        except Exception as e:
            logger.error(f"collections_latest.csv file not found: {str(e)}")
            return False
            
        response = client.get_object(COLLECTIONS_BUCKET, "collections_latest.csv")
        
        try:
            df = read_csv_with_error_handling(response.data)
            logger.info(f"Successfully read collections data with {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to read collections CSV: {str(e)}")
            df = pd.DataFrame({
                'id': ['placeholder'],
                'name': ['Error reading collections'],
                'description': ['Data could not be parsed'],
                'public': [False],
                'image_count': [0]
            })
        finally:
            response.close()
        
        save_as_parquet_in_minio(df, PARQUET_BUCKET, f"{os.path.basename(DELTA_COLLECTIONS)}_latest")
        
        temp_dir = tempfile.mkdtemp()
        delta_dir = os.path.join(temp_dir, os.path.basename(DELTA_COLLECTIONS))
        os.makedirs(delta_dir, exist_ok=True)
        
        try:
            write_delta_table(df, delta_dir)
            upload_delta_to_minio(delta_dir, DELTA_BUCKET, os.path.dirname(DELTA_COLLECTIONS))
            
            logger.info(f"Successfully created Delta table in MinIO at {DELTA_BUCKET}/{DELTA_COLLECTIONS}")
            return True
        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                
    except Exception as e:
        logger.error(f"Error in collections_to_delta: {str(e)}", exc_info=True)
        raise

def metadata_to_delta(**context):
    if not DELTA_LAKE_AVAILABLE:
        logger.error("Delta Lake library not available. Skipping delta conversion.")
        return False
        
    client = get_minio_client()
    ensure_bucket_exists(client, DELTA_BUCKET)
    ensure_bucket_exists(client, PARQUET_BUCKET)
    setup_delta_environment()
    
    try:
        if not client.bucket_exists(IMAGES_METADATA_BUCKET):
            logger.error(f"Source bucket '{IMAGES_METADATA_BUCKET}' does not exist!")
            return False
            
        try:
            client.stat_object(IMAGES_METADATA_BUCKET, "processed_latest.csv")
            logger.info("Found images_processed_latest.csv file")
        except Exception as e:
            logger.error(f"images_processed_latest.csv file not found: {str(e)}")
            return False
            
        response = client.get_object(IMAGES_METADATA_BUCKET, "processed_latest.csv")
        
        try:
            df = read_csv_with_error_handling(response.data)
            logger.info(f"Successfully read metadata with {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to read metadata CSV: {str(e)}")
            df = pd.DataFrame({
                'isic_id': ['placeholder'],
                'image_url': [''],
                'collection_id': [''],
                'collection_name': ['Error reading metadata'],
                'public': [False]
            })
        finally:
            response.close()
        
        save_as_parquet_in_minio(df, PARQUET_BUCKET, f"{os.path.basename(DELTA_METADATA)}_latest")
        
        temp_dir = tempfile.mkdtemp()
        delta_dir = os.path.join(temp_dir, os.path.basename(DELTA_METADATA))
        os.makedirs(delta_dir, exist_ok=True)
        
        try:
            write_delta_table(df, delta_dir)
            upload_delta_to_minio(delta_dir, DELTA_BUCKET, os.path.dirname(DELTA_METADATA))
            
            logger.info(f"Successfully created Delta table in MinIO at {DELTA_BUCKET}/{DELTA_METADATA}")
            return True
        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                
    except Exception as e:
        logger.error(f"Error in metadata_to_delta: {str(e)}", exc_info=True)
        raise

def extract_categories(**context):
    client = get_minio_client()
    ensure_bucket_exists(client, PROCESSED_BUCKET)
    
    try:
        if not client.bucket_exists(IMAGES_METADATA_BUCKET):
            logger.error(f"Source bucket '{IMAGES_METADATA_BUCKET}' does not exist!")
            return False
            
        try:
            response = client.get_object(IMAGES_METADATA_BUCKET, "processed_latest.csv")
            metadata_df = read_csv_with_error_handling(response.data)
            response.close()
        except Exception as e:
            logger.error(f"Error reading metadata: {str(e)}")
            return False
        
        categories = {}
        diagnosis_column = 'clinical_diagnosis'
        
        if diagnosis_column in metadata_df.columns:
            diagnoses = metadata_df[diagnosis_column].dropna().value_counts()
            total_images = len(metadata_df)
            
            categories_list = []
            
            for diagnosis, count in diagnoses.items():
                if count >= 5:  # Only include categories with at least 5 images
                    examples = metadata_df[metadata_df[diagnosis_column] == diagnosis]['isic_id'].sample(min(5, count)).tolist()
                    
                    categories_list.append({
                        'name': diagnosis,
                        'count': int(count),
                        'examples': json.dumps(examples),
                        'occurrence_percentage': round((count / total_images) * 100, 2)
                    })
            
            categories_df = pd.DataFrame(categories_list)
            categories_df = categories_df.sort_values('count', ascending=False)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            csv_buffer = io.StringIO()
            categories_df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            
            client.put_object(
                PROCESSED_BUCKET,
                f"categories_{timestamp}.csv",
                io.BytesIO(csv_data.encode()),
                length=len(csv_data),
                content_type='text/csv'
            )
            
            client.put_object(
                PROCESSED_BUCKET,
                "categories_latest.csv",
                io.BytesIO(csv_data.encode()),
                length=len(csv_data),
                content_type='text/csv'
            )
            
            logger.info(f"Successfully extracted {len(categories_df)} categories")
            return {"extracted_categories": len(categories_df)}
        else:
            logger.warning(f"Column '{diagnosis_column}' not found in metadata")
            return {"error": f"Column '{diagnosis_column}' not found"}
    except Exception as e:
        logger.error(f"Error extracting categories: {str(e)}")
        return {"error": str(e)}

def categories_to_delta(**context):
    if not DELTA_LAKE_AVAILABLE:
        logger.error("Delta Lake library not available. Skipping delta conversion.")
        return False
        
    client = get_minio_client()
    ensure_bucket_exists(client, DELTA_BUCKET)
    ensure_bucket_exists(client, PARQUET_BUCKET)
    setup_delta_environment()
    
    try:
        extract_result = context['ti'].xcom_pull(task_ids='extract_categories')
        if not extract_result or "error" in extract_result:
            logger.error("Categories extraction did not complete successfully")
            return False
            
        if not client.bucket_exists(PROCESSED_BUCKET):
            logger.error(f"Source bucket '{PROCESSED_BUCKET}' does not exist!")
            return False
            
        try:
            client.stat_object(PROCESSED_BUCKET, "categories_latest.csv")
            logger.info("Found categories_latest.csv file")
        except Exception as e:
            logger.error(f"categories_latest.csv file not found: {str(e)}")
            return False
            
        response = client.get_object(PROCESSED_BUCKET, "categories_latest.csv")
        
        try:
            df = read_csv_with_error_handling(response.data)
            logger.info(f"Successfully read categories with {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to read categories CSV: {str(e)}")
            df = pd.DataFrame({
                'name': ['placeholder'],
                'count': [0],
                'examples': ['[]'],
                'occurrence_percentage': [0.0]
            })
        finally:
            response.close()
        
        save_as_parquet_in_minio(df, PARQUET_BUCKET, f"{os.path.basename(DELTA_CATEGORIES)}_latest")
        
        temp_dir = tempfile.mkdtemp()
        delta_dir = os.path.join(temp_dir, os.path.basename(DELTA_CATEGORIES))
        os.makedirs(delta_dir, exist_ok=True)
        
        try:
            write_delta_table(df, delta_dir)
            upload_delta_to_minio(delta_dir, DELTA_BUCKET, os.path.dirname(DELTA_CATEGORIES))
            
            logger.info(f"Successfully created Delta table in MinIO at {DELTA_BUCKET}/{DELTA_CATEGORIES}")
            return True
        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                
    except Exception as e:
        logger.error(f"Error in categories_to_delta: {str(e)}", exc_info=True)
        raise

def create_unified_delta_table(**context):
    if not DELTA_LAKE_AVAILABLE:
        logger.error("Delta Lake library not available. Skipping delta conversion.")
        return False
        
    client = get_minio_client()
    ensure_bucket_exists(client, DELTA_BUCKET)
    ensure_bucket_exists(client, PARQUET_BUCKET)
    setup_delta_environment()
    
    try:
        metadata_df = None
        collections_df = None
        
        try:
            if object_exists(client, PARQUET_BUCKET, f"{os.path.basename(DELTA_METADATA)}_latest.parquet"):
                metadata_df = get_dataframe_from_parquet(client, PARQUET_BUCKET, f"{os.path.basename(DELTA_METADATA)}_latest.parquet")
                logger.info(f"Loaded metadata from parquet with {len(metadata_df)} records")
            else:
                response = client.get_object(IMAGES_METADATA_BUCKET, "processed_latest.csv")
                metadata_df = read_csv_with_error_handling(response.data)
                response.close()
                logger.info(f"Loaded metadata from CSV with {len(metadata_df)} records")
        except Exception as e:
            logger.error(f"Error reading metadata: {str(e)}")
            metadata_df = pd.DataFrame({
                'isic_id': ['placeholder'],
                'image_url': [''],
                'collection_id': ['']
            })
        
        try:
            if object_exists(client, PARQUET_BUCKET, f"{os.path.basename(DELTA_COLLECTIONS)}_latest.parquet"):
                collections_df = get_dataframe_from_parquet(client, PARQUET_BUCKET, f"{os.path.basename(DELTA_COLLECTIONS)}_latest.parquet")
                logger.info(f"Loaded collections from parquet with {len(collections_df)} records")
            else:
                response = client.get_object(COLLECTIONS_BUCKET, "collections_latest.csv")
                collections_df = read_csv_with_error_handling(response.data)
                response.close()
                logger.info(f"Loaded collections from CSV with {len(collections_df)} records")
        except Exception as e:
            logger.error(f"Error reading collections: {str(e)}")
            collections_df = pd.DataFrame({
                'id': ['placeholder'],
                'name': ['Error reading collections']
            })
        
        unified_df = metadata_df.copy()
        
        if 'isic_id' in unified_df.columns:
            unified_df.rename(columns={'isic_id': 'id'}, inplace=True)
        
        collection_dict = {str(row['id']): row for _, row in collections_df.iterrows()}
        
        try:
            def get_collection_name(row):
                try:
                    if pd.isna(row.get('collection_id')) or not row.get('collection_id', ''):
                        return ''
                    
                    collection_id = str(row.get('collection_id', ''))
                    return collection_dict.get(collection_id, {}).get('name', '')
                except Exception as e:
                    logger.debug(f"Error getting collection name: {str(e)}")
                    return ''
            
            if 'collection_id' in unified_df.columns and 'collection_name' not in unified_df.columns:
                unified_df['collection_name'] = unified_df.apply(get_collection_name, axis=1)
        except Exception as e:
            logger.warning(f"Error adding collection names: {str(e)}")
            if 'collection_name' not in unified_df.columns:
                unified_df['collection_name'] = ''
        
        try:
            if client.bucket_exists(IMAGES_BUCKET):
                objects = list(client.list_objects(IMAGES_BUCKET, recursive=True))
                downloaded_images = {obj.object_name.split('.')[0] for obj in objects if obj.object_name.endswith('.jpg')}
                unified_df['image_downloaded'] = unified_df['id'].apply(lambda x: x in downloaded_images)
                logger.info(f"Found {len(downloaded_images)} downloaded images")
            else:
                logger.warning("isic-images bucket does not exist, marking all images as not downloaded")
                unified_df['image_downloaded'] = False
        except Exception as e:
            logger.warning(f"Error checking downloaded images: {str(e)}")
            unified_df['image_downloaded'] = False
        
        save_as_parquet_in_minio(unified_df, PARQUET_BUCKET, f"{os.path.basename(DELTA_UNIFIED)}_latest")
        
        temp_dir = tempfile.mkdtemp()
        delta_dir = os.path.join(temp_dir, os.path.basename(DELTA_UNIFIED))
        os.makedirs(delta_dir, exist_ok=True)
        
        try:
            write_delta_table(unified_df, delta_dir)
            upload_delta_to_minio(delta_dir, DELTA_BUCKET, os.path.dirname(DELTA_UNIFIED))
            
            logger.info(f"Successfully created unified Delta table in MinIO at {DELTA_BUCKET}/{DELTA_UNIFIED}")
            return True
        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            
        return True
    except Exception as e:
        logger.error(f"Error creating unified Delta table: {str(e)}", exc_info=True)
        raise

def download_missing_images(**context):
    if not DELTA_LAKE_AVAILABLE:
        logger.error("Delta Lake library not available. Skipping image download.")
        return False
        
    client = get_minio_client()
    ensure_bucket_exists(client, IMAGES_BUCKET)
    setup_delta_environment()
    
    limit = int(context['dag_run'].conf.get('limit', 50))
    
    try:
        s3_path = f"s3://{DELTA_BUCKET}/{DELTA_UNIFIED}"
        dt = DeltaTable(s3_path)
        df = dt.to_pandas()
        
        missing_df = df[~df['image_downloaded'] & df['image_url'].notna()]
        missing_df = missing_df.head(limit)
        
        if len(missing_df) == 0:
            logger.info("No missing images to download")
            return {"downloaded": 0, "message": "No missing images"}
        
        logger.info(f"Found {len(missing_df)} images to download")
        
        downloaded = 0
        failed = 0
        
        for _, row in missing_df.iterrows():
            image_id = row['id']
            url = row['image_url']
            
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                
                client.put_object(
                    IMAGES_BUCKET,
                    f"{image_id}.jpg",
                    io.BytesIO(response.content),
                    length=len(response.content),
                    content_type='image/jpeg'
                )
                
                downloaded += 1
                logger.info(f"Downloaded image {image_id}")
            except Exception as e:
                failed += 1
                logger.error(f"Failed to download image {image_id}: {str(e)}")
        
        if downloaded > 0:
            dt = DeltaTable(s3_path)
            df = dt.to_pandas()
            
            objects = list(client.list_objects(IMAGES_BUCKET, recursive=True))
            downloaded_images = {obj.object_name.split('.')[0] for obj in objects if obj.object_name.endswith('.jpg')}
            df['image_downloaded'] = df['id'].apply(lambda x: x in downloaded_images)
            
            temp_dir = tempfile.mkdtemp()
            try:
                temp_path = os.path.join(temp_dir, "unified.parquet")
                df.to_parquet(temp_path)
                table = pq.read_table(temp_path)
                
                write_deltalake(
                    s3_path,
                    table,
                    mode="overwrite",
                    storage_options=get_storage_options()
                )
                logger.info(f"Updated Delta table with new download status")
            finally:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir)
        
        return {"downloaded": downloaded, "failed": failed, "total_processed": len(missing_df)}
    except Exception as e:
        logger.error(f"Error downloading missing images: {str(e)}")
        return {"error": str(e)}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'isic_delta_ingestion',
    default_args=default_args,
    description='Convert ISIC data to Delta Lake format',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 19),
    catchup=False,
    tags=['delta', 'isic', 'dermatology', 'datafabric'],
) as dag:

    collections_delta_task = PythonOperator(
        task_id='collections_to_delta',
        python_callable=collections_to_delta,
        provide_context=True,
    )
    
    metadata_delta_task = PythonOperator(
        task_id='metadata_to_delta',
        python_callable=metadata_to_delta,
        provide_context=True,
    )
    
    extract_categories_task = PythonOperator(
        task_id='extract_categories',
        python_callable=extract_categories,
        provide_context=True,
    )
    
    categories_delta_task = PythonOperator(
        task_id='categories_to_delta',
        python_callable=categories_to_delta,
        provide_context=True,
    )
    
    unified_delta_task = PythonOperator(
        task_id='create_unified_delta_table',
        python_callable=create_unified_delta_table,
        provide_context=True,
    )
    
    download_images_task = PythonOperator(
        task_id='download_missing_images',
        python_callable=download_missing_images,
        provide_context=True,
    )
    
    [collections_delta_task, metadata_delta_task] >> extract_categories_task >> categories_delta_task >> unified_delta_task >> download_images_task