from datetime import datetime, timedelta
import os
import logging
import io
import json
import pandas as pd
import numpy as np
from minio import Minio
import requests
import tempfile
import traceback
import hashlib
from typing import List, Dict, Any, Optional, Union

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, XCom
from airflow.utils.db import provide_session
from airflow.models.xcom import XCom as XComModel

try:
    from deltalake import DeltaTable, write_deltalake
except ImportError:
    logging.error("Delta Lake error.")

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_SECURE = False

DELTA_BUCKET = "isic-delta"
PROCESSED_BUCKET = "isic-processed"
IMAGES_BUCKET = "isic-images"
RESULTS_BUCKET = "isic-results"

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
            logging.info(f"Created bucket {bucket_name}")
    except Exception as e:
        logging.error(f"Error creating bucket {bucket_name}: {str(e)}")

def setup_delta_environment():
    os.environ["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
    os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
    os.environ["AWS_ENDPOINT_URL"] = f"http://{MINIO_ENDPOINT}"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"

@provide_session
def clear_previous_output(task_id, dag_id, session=None):
    session.query(XComModel).filter(
        XComModel.dag_id == dag_id,
        XComModel.task_id == task_id
    ).delete()

def get_storage_options():
    return {
        "endpoint_url": f"http://{MINIO_ENDPOINT}",
        "access_key_id": MINIO_ACCESS_KEY,
        "secret_access_key": MINIO_SECRET_KEY,
        "region": "us-east-1"
    }

def query_delta_table(table_name, columns=None, filters=None, limit=1000, offset=0, **context):
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
        
        original_columns = list(df.columns)
        
        if filters:
            for col, value in filters.items():
                if col in df.columns:
                    if isinstance(value, list):
                        df = df[df[col].isin(value)]
                    else:
                        df = df[df[col] == value]
                else:
                    logging.warning(f"Column {col} not found in table, skipping filter")
        
        if columns and all(col in df.columns for col in columns):
            df = df[columns]
        
        total_count = len(df)
        
        df = df.iloc[offset:offset+limit]
        
        client = get_minio_client()
        ensure_bucket_exists(client, RESULTS_BUCKET)
        
        query_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        query_hash = hashlib.md5(f"{table_name}_{str(columns)}_{str(filters)}_{limit}_{offset}".encode()).hexdigest()[:8]
        
        result_name = f"query_{table_name}_{query_hash}_{query_time}"
        
        result_json = df.to_json(orient="records")
        client.put_object(
            RESULTS_BUCKET,
            f"{result_name}.json",
            io.BytesIO(result_json.encode()),
            length=len(result_json),
            content_type='application/json'
        )
        
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        client.put_object(
            RESULTS_BUCKET,
            f"{result_name}.csv",
            io.BytesIO(csv_data.encode()),
            length=len(csv_data),
            content_type='text/csv'
        )
        
        clear_previous_output('query_delta_table', context['dag_run'].dag_id)
        
        query_info = {
            "table": table_name,
            "filters": filters,
            "columns": columns,
            "result_rows": len(df),
            "total_rows": total_count,
            "result_file_json": f"{result_name}.json",
            "result_file_csv": f"{result_name}.csv"
        }
        
        context['ti'].xcom_push(key='query_info', value=query_info)
        
        if len(result_json) < 1048576:  # 1MB XCom limit
            context['ti'].xcom_push(key='query_result', value=json.loads(result_json))
            
        return query_info
    
    except Exception as e:
        error_msg = f"Error querying Delta table: {str(e)}"
        logging.error(error_msg)
        return {"error": error_msg}

def download_isic_images(image_ids, **context):
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
        
        for _, row in image_data.iterrows():
            if pd.isna(row.get('image_url')):
                errors.append(f"No URL found for image {row['id']}")
                continue
                
            try:
                response = requests.get(row['image_url'], timeout=30)
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
            
            temp_dir = tempfile.mkdtemp()
            try:
                temp_path = os.path.join(temp_dir, "unified_update.parquet")
                df.to_parquet(temp_path)
                table = pd.read_parquet(temp_path)
                
                write_deltalake(
                    s3_path,
                    table,
                    mode="overwrite",
                    storage_options=get_storage_options()
                )
            finally:
                if os.path.exists(temp_dir):
                    import shutil
                    shutil.rmtree(temp_dir)
            
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

def run_advanced_query(query_params, **context):
    """
    Run an advanced query with joins across tables and custom filtering
    
    query_params: {
        "select": ["unified.id", "unified.collection_name", "categories.name"],
        "from": "unified",
        "joins": [{"table": "categories", "on": "unified.clinical_diagnosis = categories.name"}],
        "where": "unified.public = True",
        "limit": 100
    }
    """
    setup_delta_environment()
    
    required_params = ["select", "from"]
    for param in required_params:
        if param not in query_params:
            return {"error": f"Missing required parameter: {param}"}
    
    table_path_map = {
        "collections": DELTA_COLLECTIONS,
        "metadata": DELTA_METADATA,
        "categories": DELTA_CATEGORIES,
        "unified": DELTA_UNIFIED
    }
    
    main_table = query_params["from"]
    if main_table not in table_path_map:
        return {"error": f"Invalid table name: {main_table}"}
    
    try:
        # Load main table
        main_path = f"s3://{DELTA_BUCKET}/{table_path_map[main_table]}"
        main_dt = DeltaTable(main_path)
        main_df = main_dt.to_pandas()
        
        # Load and join additional tables
        if "joins" in query_params:
            for join in query_params["joins"]:
                join_table = join["table"]
                join_condition = join["on"]
                
                if join_table not in table_path_map:
                    return {"error": f"Invalid join table: {join_table}"}
                
                join_path = f"s3://{DELTA_BUCKET}/{table_path_map[join_table]}"
                join_dt = DeltaTable(join_path)
                join_df = join_dt.to_pandas()
                
                # Parse join condition (basic implementation)
                # Format: "table1.col1 = table2.col2"
                parts = join_condition.split("=")
                if len(parts) != 2:
                    return {"error": f"Invalid join condition: {join_condition}"}
                
                left_col = parts[0].strip().split(".")[-1]
                right_col = parts[1].strip().split(".")[-1]
                
                # Perform join (improve with proper merge conditions)
                main_df = main_df.merge(join_df, left_on=left_col, right_on=right_col, how="left", suffixes=("", f"_{join_table}"))
        
        # Apply where condition if specified
        if "where" in query_params:
            try:
                filtered_df = main_df.query(query_params["where"])
                main_df = filtered_df
            except Exception as e:
                return {"error": f"Error applying where condition: {str(e)}"}
        
        # Apply select (columns)
        if query_params["select"] != ["*"]:
            columns_to_select = []
            for col in query_params["select"]:
                if "." in col:
                    # Handle table.column format
                    table_name, col_name = col.split(".")
                    if table_name != main_table:
                        col_name = f"{col_name}_{table_name}"
                    columns_to_select.append(col_name)
                else:
                    columns_to_select.append(col)
                    
            # Only select columns that exist
            valid_columns = [col for col in columns_to_select if col in main_df.columns]
            main_df = main_df[valid_columns]
        
        # Apply limit
        limit = query_params.get("limit", 1000)
        main_df = main_df.head(limit)
        
        # Save results
        client = get_minio_client()
        ensure_bucket_exists(client, RESULTS_BUCKET)
        
        query_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_name = f"advanced_query_{query_time}"
        
        # Save as JSON
        result_json = main_df.to_json(orient="records")
        client.put_object(
            RESULTS_BUCKET,
            f"{result_name}.json",
            io.BytesIO(result_json.encode()),
            length=len(result_json),
            content_type='application/json'
        )
        
        # Save as CSV
        csv_buffer = io.StringIO()
        main_df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        client.put_object(
            RESULTS_BUCKET,
            f"{result_name}.csv",
            io.BytesIO(csv_data.encode()),
            length=len(csv_data),
            content_type='text/csv'
        )
        
        # Return results
        result = {
            "row_count": len(main_df),
            "columns": list(main_df.columns),
            "result_file_json": f"{result_name}.json",
            "result_file_csv": f"{result_name}.csv"
        }
        
        # Push to XCom if small enough
        if len(result_json) < 1048576:  # 1MB XCom limit
            context['ti'].xcom_push(key='query_result', value=json.loads(result_json))
            
        return result
    
    except Exception as e:
        logging.error(f"Advanced query error: {str(e)}")
        return {"error": f"Error executing query: {str(e)}"}

def prepare_image_dataset(**context):
    """
    Prepare a dataset of images with metadata for analysis
    
    Parameters expected in context['dag_run'].conf:
    - filters: dict of filters to apply
    - limit: max number of images
    - include_clinical: whether to include clinical fields
    - download_missing: whether to download missing images
    """
    setup_delta_environment()
    
    conf = context['dag_run'].conf
    filters = conf.get('filters', {})
    limit = min(conf.get('limit', 100), 500)  # Cap at 500 for safety
    include_clinical = conf.get('include_clinical', True)
    download_missing = conf.get('download_missing', True)
    
    try:
        # Query unified table
        s3_path = f"s3://{DELTA_BUCKET}/{DELTA_UNIFIED}"
        dt = DeltaTable(s3_path)
        df = dt.to_pandas()
        
        # Apply filters
        for col, value in filters.items():
            if col in df.columns:
                if isinstance(value, list):
                    df = df[df[col].isin(value)]
                else:
                    df = df[df[col] == value]
            else:
                logging.warning(f"Column {col} not found in table, skipping filter")
        
        # Apply limit
        df = df.head(limit)
        
        if len(df) == 0:
            return {"error": "No images matched the criteria"}
        
        # Filter columns if needed
        if not include_clinical:
            clinical_cols = [col for col in df.columns if col.startswith('clinical_')]
            df = df.drop(columns=clinical_cols)
        
        # Check which images need to be downloaded
        client = get_minio_client()
        ensure_bucket_exists(client, IMAGES_BUCKET)
        
        missing_images = []
        if download_missing:
            for image_id in df['id'].tolist():
                try:
                    client.stat_object(IMAGES_BUCKET, f"{image_id}.jpg")
                except:
                    missing_images.append(image_id)
            
            if missing_images:
                logging.info(f"Downloading {len(missing_images)} missing images")
                download_isic_images(missing_images, **context)
        
        # Create a dataset manifest
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        manifest = {
            "timestamp": timestamp,
            "total_images": len(df),
            "filters_applied": filters,
            "clinical_data_included": include_clinical,
            "image_ids": df['id'].tolist()
        }
        
        ensure_bucket_exists(client, RESULTS_BUCKET)
        
        manifest_json = json.dumps(manifest)
        dataset_prefix = f"dataset_{timestamp}"
        
        client.put_object(
            RESULTS_BUCKET,
            f"{dataset_prefix}_manifest.json",
            io.BytesIO(manifest_json.encode()),
            length=len(manifest_json),
            content_type='application/json'
        )
        
        # Save dataset metadata
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        client.put_object(
            RESULTS_BUCKET,
            f"{dataset_prefix}_metadata.csv",
            io.BytesIO(csv_data.encode()),
            length=len(csv_data),
            content_type='text/csv'
        )
        
        return {
            "dataset_id": timestamp,
            "image_count": len(df),
            "manifest_file": f"{dataset_prefix}_manifest.json",
            "metadata_file": f"{dataset_prefix}_metadata.csv",
            "downloaded_missing": len(missing_images)
        }
    
    except Exception as e:
        logging.error(f"Error preparing dataset: {str(e)}")
        return {"error": f"Dataset preparation failed: {str(e)}"}

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
            'limit': "{{ dag_run.conf.get('limit', 1000) }}",
            'offset': "{{ dag_run.conf.get('offset', 0) }}"
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
    
    advanced_query_task = PythonOperator(
        task_id='run_advanced_query',
        python_callable=run_advanced_query,
        provide_context=True,
        op_kwargs={
            'query_params': "{{ dag_run.conf.get('query_params', {}) }}"
        }
    )
    
    prepare_dataset_task = PythonOperator(
        task_id='prepare_image_dataset',
        python_callable=prepare_image_dataset,
        provide_context=True
    )
    
    query_task >> download_images_task