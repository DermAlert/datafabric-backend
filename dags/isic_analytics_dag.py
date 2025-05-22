from datetime import datetime, timedelta
import os
import logging
import io
import json
import pandas as pd
import numpy as np
from minio import Minio

from airflow import DAG
from airflow.operators.python import PythonOperator

try:
    from deltalake import DeltaTable
except ImportError:
    logging.error("Delta Lake error.")

# Configuration constants
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_SECURE = False

# Bucket names
DELTA_BUCKET = "isic-delta"
REPORTS_BUCKET = "isic-reports"
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

def generate_dashboard_data(**context):
    """Generate summary statistics for dashboard"""
    setup_delta_environment()
    client = get_minio_client()
    ensure_bucket_exists(client, REPORTS_BUCKET)
    
    dashboard = {}
    
    try:
        # Collection statistics
        collections_path = f"s3://{DELTA_BUCKET}/{DELTA_COLLECTIONS}"
        collections_dt = DeltaTable(collections_path)
        collections_df = collections_dt.to_pandas()
        
        dashboard["collections"] = {
            "total_count": len(collections_df),
            "public_count": collections_df['public'].sum(),
            "total_images": collections_df['image_count'].sum(),
            "top_collections": collections_df.sort_values('image_count', ascending=False)[['name', 'image_count']].head(5).to_dict('records')
        }
        
        # Metadata statistics
        unified_path = f"s3://{DELTA_BUCKET}/{DELTA_UNIFIED}"
        unified_dt = DeltaTable(unified_path)
        unified_df = unified_dt.to_pandas()
        
        # Count downloaded images
        downloaded_count = unified_df['image_downloaded'].sum() if 'image_downloaded' in unified_df.columns else 0
        
        dashboard["images"] = {
            "total_count": len(unified_df),
            "downloaded_count": int(downloaded_count),
            "download_percentage": round((downloaded_count / len(unified_df)) * 100, 2) if len(unified_df) > 0 else 0
        }
        
        # Category statistics
        categories_path = f"s3://{DELTA_BUCKET}/{DELTA_CATEGORIES}"
        categories_dt = DeltaTable(categories_path)
        categories_df = categories_dt.to_pandas()
        
        dashboard["categories"] = {
            "total_count": len(categories_df),
            "most_common": categories_df.sort_values('occurrence_percentage', ascending=False)[['name', 'count', 'occurrence_percentage']].head(10).to_dict('records')
        }
        
        # Storage statistics
        storage_stats = {
            "buckets": {}
        }
        
        for bucket in [DELTA_BUCKET, REPORTS_BUCKET, IMAGES_BUCKET]:
            try:
                objects = client.list_objects(bucket, recursive=True)
                total_size = sum(obj.size for obj in objects)
                object_count = sum(1 for _ in client.list_objects(bucket, recursive=True))
                
                storage_stats["buckets"][bucket] = {
                    "size_bytes": total_size,
                    "size_mb": round(total_size / (1024 * 1024), 2),
                    "object_count": object_count
                }
            except Exception as e:
                logging.error(f"Error getting storage stats for {bucket}: {str(e)}")
        
        dashboard["storage"] = storage_stats
        
        # Save dashboard data
        report_date = datetime.now().strftime("%Y%m%d")
        dashboard_json = json.dumps(dashboard, indent=2)
        
        client.put_object(
            REPORTS_BUCKET,
            f"dashboard_{report_date}.json",
            io.BytesIO(dashboard_json.encode()),
            length=len(dashboard_json),
            content_type='application/json'
        )
        
        client.put_object(
            REPORTS_BUCKET,
            "dashboard_latest.json",
            io.BytesIO(dashboard_json.encode()),
            length=len(dashboard_json),
            content_type='application/json'
        )
        
        context['ti'].xcom_push(key='dashboard_summary', value={
            "collections_count": dashboard["collections"]["total_count"],
            "total_images": dashboard["images"]["total_count"],
            "downloaded_images": dashboard["images"]["downloaded_count"],
            "categories_count": dashboard["categories"]["total_count"]
        })
        
        return dashboard
        
    except Exception as e:
        error_msg = f"Error generating dashboard data: {str(e)}"
        logging.error(error_msg)
        return {"error": error_msg}

def generate_quality_report(**context):
    """Generate data quality report"""
    setup_delta_environment()
    client = get_minio_client()
    
    quality_report = {
        "timestamp": datetime.now().isoformat(),
        "tables": {}
    }
    
    try:
        unified_path = f"s3://{DELTA_BUCKET}/{DELTA_UNIFIED}"
        unified_dt = DeltaTable(unified_path)
        unified_df = unified_dt.to_pandas()
        
        completeness = {}
        for column in unified_df.columns:
            non_null = unified_df[column].count()
            total = len(unified_df)
            completeness[column] = {
                "non_null_count": int(non_null),
                "completeness_pct": round((non_null / total) * 100, 2) if total > 0 else 0
            }
        
        # Find duplicate records
        id_count = unified_df['id'].value_counts()
        duplicate_ids = id_count[id_count > 1].to_dict()
        
        quality_report["tables"]["unified"] = {
            "row_count": len(unified_df),
            "column_count": len(unified_df.columns),
            "completeness": completeness,
            "duplicate_count": sum(value - 1 for value in duplicate_ids.values()) if duplicate_ids else 0,
            "duplicate_ids": duplicate_ids if duplicate_ids else None
        }
        
        report_date = datetime.now().strftime("%Y%m%d")
        quality_json = json.dumps(quality_report, indent=2)
        
        client.put_object(
            REPORTS_BUCKET,
            f"quality_{report_date}.json",
            io.BytesIO(quality_json.encode()),
            length=len(quality_json),
            content_type='application/json'
        )
        
        client.put_object(
            REPORTS_BUCKET,
            "quality_latest.json",
            io.BytesIO(quality_json.encode()),
            length=len(quality_json),
            content_type='application/json'
        )
        
        context['ti'].xcom_push(key='quality_summary', value={
            "row_count": quality_report["tables"]["unified"]["row_count"],
            "duplicate_count": quality_report["tables"]["unified"]["duplicate_count"],
            "columns_below_50pct_complete": sum(1 for col, stats in completeness.items() 
                                              if stats["completeness_pct"] < 50)
        })
        
        return quality_report
        
    except Exception as e:
        error_msg = f"Error generating quality report: {str(e)}"
        logging.error(error_msg)
        return {"error": error_msg}

def generate_data_catalog(**context):
    """Generate data catalog with schema information"""
    setup_delta_environment()
    client = get_minio_client()
    
    catalog = {
        "timestamp": datetime.now().isoformat(),
        "tables": {}
    }
    
    try:
        # Process each table
        for table_name, table_path in {
            "collections": DELTA_COLLECTIONS,
            "metadata": DELTA_METADATA,
            "categories": DELTA_CATEGORIES,
            "unified": DELTA_UNIFIED
        }.items():
            try:
                s3_path = f"s3://{DELTA_BUCKET}/{table_path}"
                dt = DeltaTable(s3_path)
                df = dt.to_pandas()
                
                # Get schema information
                schema_info = []
                for column in df.columns:
                    col_info = {
                        "name": column,
                        "type": str(df[column].dtype),
                        "non_null_count": int(df[column].count()),
                        "non_null_percentage": round((df[column].count() / len(df)) * 100, 2) if len(df) > 0 else 0,
                        "unique_count": int(df[column].nunique())
                    }
                    
                    if df[column].dtype == 'object':
                        sample_values = df[column].dropna().sample(min(5, df[column].nunique())).tolist()
                        col_info["sample_values"] = sample_values
                    
                    if np.issubdtype(df[column].dtype, np.number):
                        col_info["min"] = float(df[column].min()) if not df[column].empty else None
                        col_info["max"] = float(df[column].max()) if not df[column].empty else None
                        col_info["mean"] = float(df[column].mean()) if not df[column].empty else None
                        col_info["median"] = float(df[column].median()) if not df[column].empty else None
                    
                    schema_info.append(col_info)
                
                catalog["tables"][table_name] = {
                    "row_count": len(df),
                    "column_count": len(df.columns),
                    "schema": schema_info,
                    "location": s3_path
                }
                
            except Exception as e:
                logging.error(f"Error processing table {table_name}: {str(e)}")
                catalog["tables"][table_name] = {"error": str(e)}
        
        # Save catalog
        report_date = datetime.now().strftime("%Y%m%d")
        catalog_json = json.dumps(catalog, indent=2)
        
        client.put_object(
            REPORTS_BUCKET,
            f"catalog_{report_date}.json",
            io.BytesIO(catalog_json.encode()),
            length=len(catalog_json),
            content_type='application/json'
        )
        
        client.put_object(
            REPORTS_BUCKET,
            "catalog_latest.json",
            io.BytesIO(catalog_json.encode()),
            length=len(catalog_json),
            content_type='application/json'
        )
        
        # Generate catalog summary
        table_summaries = {}
        for table_name, table_info in catalog["tables"].items():
            if "row_count" in table_info:
                table_summaries[table_name] = {
                    "rows": table_info["row_count"],
                    "columns": table_info["column_count"]
                }
                
        context['ti'].xcom_push(key='catalog_summary', value=table_summaries)
        
        return catalog
        
    except Exception as e:
        error_msg = f"Error generating data catalog: {str(e)}"
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
    'isic_analytics',
    default_args=default_args,
    description='Generate analytics and reports for ISIC data lake',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 5, 5),
    catchup=False,
    tags=['delta', 'isic', 'dermatology', 'analytics'],
) as dag:

    dashboard_task = PythonOperator(
        task_id='generate_dashboard_data',
        python_callable=generate_dashboard_data,
        provide_context=True,
    )
    
    quality_task = PythonOperator(
        task_id='generate_quality_report',
        python_callable=generate_quality_report,
        provide_context=True,
    )
    
    catalog_task = PythonOperator(
        task_id='generate_data_catalog',
        python_callable=generate_data_catalog,
        provide_context=True,
    )
    
    dashboard_task >> quality_task >> catalog_task