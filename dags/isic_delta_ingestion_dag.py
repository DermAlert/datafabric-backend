from datetime import datetime, timedelta
import json
import io
import os
import logging
import pandas as pd
import hashlib
import pyarrow.parquet as pq
import pyarrow as pa
from minio import Minio
from minio.error import S3Error

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

try:
    import deltalake
    from deltalake import DeltaTable, write_deltalake
except ImportError:
    logging.error("Delta Lake error.")

# Configuration constants
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_SECURE = False

# Bucket names
RAW_BUCKET = "isic-raw"
DELTA_BUCKET = "isic-delta"
PROCESSED_BUCKET = "isic-processed"

# Delta table paths
DELTA_COLLECTIONS = "delta/collections"
DELTA_METADATA = "delta/metadata"
DELTA_CATEGORIES = "delta/categories"
DELTA_UNIFIED = "delta/unified"

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

def setup_delta_environment():
    """Configure environment for Delta Lake with MinIO"""
    os.environ["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
    os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
    os.environ["AWS_ENDPOINT_URL"] = f"http://{MINIO_ENDPOINT}"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"

def collections_to_delta(**context):
    """Convert collections data to Delta format"""
    client = get_minio_client()
    ensure_bucket_exists(client, DELTA_BUCKET)
    setup_delta_environment()
    
    try:
        response = client.get_object("isic-collections", "collections_latest.csv")
        df = pd.read_csv(io.BytesIO(response.data))
        response.close()
    except Exception as e:
        logging.error(f"Error reading collections data: {str(e)}")
        return False

    try:
        s3_path = f"s3://{DELTA_BUCKET}/{DELTA_COLLECTIONS}"
        
        table_exists = False
        try:
            dt = DeltaTable(s3_path)
            table_exists = True
        except:
            table_exists = False
            
        if table_exists:
            temp_path = "/tmp/collections_temp.parquet"
            df.to_parquet(temp_path)
            
            arrow_table = pq.read_table(temp_path)
            
            dt = DeltaTable(s3_path)
            write_deltalake(
                dt,
                arrow_table,
                mode="merge",
                merge_key=["id"],
                storage_options={
                    "AWS_ENDPOINT_URL": f"http://{MINIO_ENDPOINT}",
                    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
                    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY
                }
            )
            os.remove(temp_path)
        else:
            write_deltalake(
                s3_path,
                df,
                storage_options={
                    "AWS_ENDPOINT_URL": f"http://{MINIO_ENDPOINT}",
                    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
                    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY
                }
            )
            
        logging.info(f"Successfully wrote collections to Delta table: {s3_path}")
        return True
    except Exception as e:
        logging.error(f"Error writing collections to Delta: {str(e)}")
        return False

def metadata_to_delta(**context):
    """Convert image metadata to Delta format"""
    client = get_minio_client()
    ensure_bucket_exists(client, DELTA_BUCKET)
    setup_delta_environment()
    
    try:
        response = client.get_object("isic-metadata", "metadata_latest.csv")
        df = pd.read_csv(io.BytesIO(response.data))
        response.close()
    except Exception as e:
        logging.error(f"Error reading metadata: {str(e)}")
        return False

    try:
        s3_path = f"s3://{DELTA_BUCKET}/{DELTA_METADATA}"
        
        table_exists = False
        try:
            dt = DeltaTable(s3_path)
            table_exists = True
        except:
            table_exists = False
            
        if table_exists:
            temp_path = "/tmp/metadata_temp.parquet"
            df.to_parquet(temp_path)
            arrow_table = pq.read_table(temp_path)
            
            dt = DeltaTable(s3_path)
            write_deltalake(
                dt,
                arrow_table,
                mode="merge",
                merge_key=["id"],
                storage_options={
                    "AWS_ENDPOINT_URL": f"http://{MINIO_ENDPOINT}",
                    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
                    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY
                }
            )
            os.remove(temp_path)
        else:
            write_deltalake(
                s3_path,
                df,
                storage_options={
                    "AWS_ENDPOINT_URL": f"http://{MINIO_ENDPOINT}",
                    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
                    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY
                }
            )
            
        logging.info(f"Successfully wrote metadata to Delta table: {s3_path}")
        return True
    except Exception as e:
        logging.error(f"Error writing metadata to Delta: {str(e)}")
        return False

def categories_to_delta(**context):
    """Convert categories data to Delta format"""
    client = get_minio_client()
    ensure_bucket_exists(client, DELTA_BUCKET)
    setup_delta_environment()
    
    # Get categories from MinIO
    try:
        response = client.get_object("isic-categories", "categories_latest.csv")
        df = pd.read_csv(io.BytesIO(response.data))
        response.close()
    except Exception as e:
        logging.error(f"Error reading categories: {str(e)}")
        return False

    try:
        s3_path = f"s3://{DELTA_BUCKET}/{DELTA_CATEGORIES}"
        
        table_exists = False
        try:
            dt = DeltaTable(s3_path)
            table_exists = True
        except:
            table_exists = False
            
        if table_exists:
            temp_path = "/tmp/categories_temp.parquet"
            df.to_parquet(temp_path)
            arrow_table = pq.read_table(temp_path)
            
            dt = DeltaTable(s3_path)
            write_deltalake(
                dt,
                arrow_table,
                mode="merge",
                merge_key=["name"],
                storage_options={
                    "AWS_ENDPOINT_URL": f"http://{MINIO_ENDPOINT}",
                    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
                    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY
                }
            )
            os.remove(temp_path)
        else:
            write_deltalake(
                s3_path,
                df,
                storage_options={
                    "AWS_ENDPOINT_URL": f"http://{MINIO_ENDPOINT}",
                    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
                    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY
                }
            )
            
        logging.info(f"Successfully wrote categories to Delta table: {s3_path}")
        return True
    except Exception as e:
        logging.error(f"Error writing categories to Delta: {str(e)}")
        return False

def create_unified_delta_table(**context):
    """Create a unified Delta table with all data sources"""
    client = get_minio_client()
    setup_delta_environment()
    
    try:
        # Get metadata
        response = client.get_object("isic-metadata", "metadata_latest.csv")
        metadata_df = pd.read_csv(io.BytesIO(response.data))
        response.close()
        
        # Get collections
        response = client.get_object("isic-collections", "collections_latest.csv")
        collections_df = pd.read_csv(io.BytesIO(response.data))
        response.close()
        
        unified_df = metadata_df.copy()
        
        collection_dict = {row['id']: row for _, row in collections_df.iterrows()}
        
        # Add collection details
        unified_df['collection_name'] = unified_df.apply(
            lambda row: collection_dict.get(row['collection_ids'].split(',')[0], {}).get('name', '') 
            if not pd.isna(row['collection_ids']) and row['collection_ids'] 
            else '',
            axis=1
        )
        
        try:
            objects = client.list_objects("isic-images", recursive=True)
            downloaded_images = {obj.object_name.split('.')[0] for obj in objects if obj.object_name.endswith('.jpg')}
            unified_df['image_downloaded'] = unified_df['id'].apply(lambda x: x in downloaded_images)
        except Exception as e:
            logging.warning(f"Error checking downloaded images: {str(e)}")
            unified_df['image_downloaded'] = False
            
        # Write to Delta format
        s3_path = f"s3://{DELTA_BUCKET}/{DELTA_UNIFIED}"
        
        # Check if table exists
        table_exists = False
        try:
            dt = DeltaTable(s3_path)
            table_exists = True
        except:
            table_exists = False
            
        if table_exists:
            temp_path = "/tmp/unified_temp.parquet"
            unified_df.to_parquet(temp_path)
            arrow_table = pq.read_table(temp_path)
            
            dt = DeltaTable(s3_path)
            write_deltalake(
                dt,
                arrow_table,
                mode="merge",
                merge_key=["id"],
                storage_options={
                    "AWS_ENDPOINT_URL": f"http://{MINIO_ENDPOINT}",
                    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
                    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY
                }
            )
            os.remove(temp_path)
        else:
            write_deltalake(
                s3_path,
                unified_df,
                storage_options={
                    "AWS_ENDPOINT_URL": f"http://{MINIO_ENDPOINT}",
                    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
                    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY
                }
            )
            
        logging.info(f"Successfully created unified Delta table with {len(unified_df)} records")
        return True
    except Exception as e:
        logging.error(f"Error creating unified Delta table: {str(e)}")
        return False

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
    
    [collections_delta_task, metadata_delta_task, categories_delta_task] >> unified_delta_task