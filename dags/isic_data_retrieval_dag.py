from datetime import datetime, timedelta
import os
import logging
import io
import json
import pandas as pd
import pyarrow.parquet as pq
from minio import Minio

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, XCom
from airflow.utils.db import provide_session
from airflow.models.xcom import XCom as XComModel

try:
    from deltalake import DeltaTable, write_deltalake
except ImportError:
    logging.error("Delta Lake error.")

# Configuration constants
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_SECURE = False

# Bucket names
DELTA_BUCKET = "isic-delta"
PROCESSED_BUCKET = "isic-processed"
IMAGES_BUCKET = "isic-images"

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

@provide_session
def clear_previous_output(task_id, dag_id, session=None):
    """Clear previous XCom outputs for a particular task"""
    session.query(XComModel).filter(
        XComModel.dag_id == dag_id,
        XComModel.task_id == task_id
    ).delete()

def query_delta_table(table_name, columns=None, filters=None, limit=1000, **context):
    """
    Query a Delta table with specific parameters
    
    Args:
        table_name: Name of the Delta table (collections, metadata, categories, unified)
        columns: List of columns to retrieve (None = all columns)
        filters: Dictionary of filters {column: value}
        limit: Maximum number of rows to return
    """
    setup_delta_environment()
    
    table_path_map = {
        "collections": DELTA_COLLECTIONS,
        "metadata": DELTA_METADATA,
        "categories": DELTA_CATEGORIES,
        "unified": DELTA_UNIFIED
    }
    
    if table_name not in table_path_map:
        error_msg = f"Invalid table name: {table_name}. Valid options: {', '.join(table_path_map.keys())}"
        logging.error(error_msg)
        return {"error": error_msg}
    
    s3_path = f"s3://{DELTA_BUCKET}/{table_path_map[table_name]}"
    
    try:
        dt = DeltaTable(s3_path)
        
        df = dt.to_pandas()
        
        if filters:
            for col, value in filters.items():
                if col in df.columns:
                    df = df[df[col] == value]
                else:
                    logging.warning(f"Column {col} not found in table, skipping filter")
        
        if columns and all(col in df.columns for col in columns):
            df = df[columns]
        
        # Apply limit
        if limit:
            df = df.head(limit)
            
        result = df.to_json(orient="records")
        
        client = get_minio_client()
        ensure_bucket_exists(client, PROCESSED_BUCKET)
        
        query_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_name = f"query_{table_name}_{query_time}.json"
        
        client.put_object(
            PROCESSED_BUCKET,
            result_name,
            io.BytesIO(result.encode()),
            length=len(result),
            content_type='application/json'
        )
        
        # Clear previous output to avoid XCom size issues
        clear_previous_output('query_delta_table', context['dag_run'].dag_id)
        
        # Store query details in XCom
        query_info = {
            "table": table_name,
            "filters": filters,
            "columns": columns,
            "result_rows": len(df),
            "result_file": result_name
        }
        context['ti'].xcom_push(key='query_info', value=query_info)
        
        if len(result) < 1048576:  # 1MB XCom limit
            context['ti'].xcom_push(key='query_result', value=json.loads(result))
            
        return query_info
    
    except Exception as e:
        error_msg = f"Error querying Delta table: {str(e)}"
        logging.error(error_msg)
        return {"error": error_msg}

def download_isic_images(image_ids, **context):
    """
    Download specific ISIC images if not already present
    
    Args:
        image_ids: List of ISIC image IDs to download
    """
    if not image_ids:
        return {"message": "No image IDs provided", "downloaded": 0}
        
    setup_delta_environment()
    client = get_minio_client()
    ensure_bucket_exists(client, IMAGES_BUCKET)
    
    try:
        objects = client.list_objects(IMAGES_BUCKET, recursive=True)
        existing_images = {obj.object_name.split('.')[0] for obj in objects if obj.object_name.endswith('.jpg')}
    except Exception as e:
        logging.warning(f"Error listing existing images: {str(e)}")
        existing_images = set()
        
    missing_ids = [img_id for img_id in image_ids if img_id not in existing_images]
    
    if not missing_ids:
        return {"message": "All requested images already downloaded", "downloaded": 0}
    
    try:
        s3_path = f"s3://{DELTA_BUCKET}/{DELTA_UNIFIED}"
        dt = DeltaTable(s3_path)
        df = dt.to_pandas()
        
        image_data = df[df['id'].isin(missing_ids)]
        
        downloaded = 0
        errors = []
        
        import requests
        
        for _, row in image_data.iterrows():
            if pd.isna(row.get('url')):
                errors.append(f"No URL found for image {row['id']}")
                continue
                
            try:
                response = requests.get(row['url'])
                response.raise_for_status()
                
                client.put_object(
                    IMAGES_BUCKET,
                    f"{row['id']}.jpg",
                    io.BytesIO(response.content),
                    length=len(response.content),
                    content_type='image/jpeg'
                )
                
                downloaded += 1
                
            except Exception as e:
                errors.append(f"Error downloading {row['id']}: {str(e)}")
                
        if downloaded > 0:
            df.loc[df['id'].isin(missing_ids), 'image_downloaded'] = True
            
            temp_path = "/tmp/unified_update_temp.parquet"
            df.to_parquet(temp_path)
            arrow_table = pq.read_table(temp_path)
            
            write_deltalake(
                s3_path,
                arrow_table,
                mode="overwrite",
                storage_options={
                    "AWS_ENDPOINT_URL": f"http://{MINIO_ENDPOINT}",
                    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
                    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY
                }
            )
            
        result = {
            "message": f"Downloaded {downloaded} images, {len(errors)} errors",
            "downloaded": downloaded,
            "errors": errors if errors else None
        }
        
        return result
        
    except Exception as e:
        error_msg = f"Error retrieving image data: {str(e)}"
        logging.error(error_msg)
        return {"error": error_msg}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'isic_data_retrieval',
    default_args=default_args,
    description='DAG for retrieving specific data from Delta tables',
    schedule_interval=None,  # Triggered manually with parameters
    start_date=datetime(2023, 5, 19),
    catchup=False,
    tags=['delta', 'isic', 'dermatology', 'retrieval'],
) as dag:

    query_task = PythonOperator(
        task_id='query_delta_table',
        python_callable=query_delta_table,
        provide_context=True,
        op_kwargs={
            'table_name': "{{ dag_run.conf.get('table_name', 'unified') }}",
            'columns': "{{ dag_run.conf.get('columns', None) }}",
            'filters': "{{ dag_run.conf.get('filters', None) }}",
            'limit': "{{ dag_run.conf.get('limit', 1000) }}"
        }
    )
    
    download_images_task = PythonOperator(
        task_id='download_isic_images',
        python_callable=download_isic_images,
        provide_context=True,
        op_kwargs={
            'image_ids': "{{ dag_run.conf.get('image_ids', []) }}"
        }
    )
    
    query_task >> download_images_task