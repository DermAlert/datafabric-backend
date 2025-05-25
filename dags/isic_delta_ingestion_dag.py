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
import csv
import tempfile
import shutil
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import Delta Lake with better error handling
try:
    from deltalake import DeltaTable, write_deltalake
    DELTA_LAKE_AVAILABLE = True
    logger.info("Delta Lake library loaded successfully!")
except ImportError as e:
    logger.error(f"Failed to import Delta Lake: {str(e)}")
    DELTA_LAKE_AVAILABLE = False

# Configuration constants
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_SECURE = False

# Bucket names
RAW_BUCKET = "isic-raw"
DELTA_BUCKET = "isic-delta"
PROCESSED_BUCKET = "isic-processed"
PARQUET_BUCKET = "isic-parquet"  # For temporary parquet storage

DELTA_COLLECTIONS = "collections"
DELTA_METADATA = "metadata"
DELTA_CATEGORIES = "categories"
DELTA_UNIFIED = "unified"

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
    """Configure environment for Delta Lake with MinIO"""
    # Set environment variables
    os.environ["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
    os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
    os.environ["AWS_ENDPOINT_URL"] = f"http://{MINIO_ENDPOINT}"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"
    
    # Log the configuration
    logger.info("Delta Lake environment configured with the following parameters:")
    logger.info(f"AWS_ENDPOINT_URL: {os.environ.get('AWS_ENDPOINT_URL')}")
    logger.info(f"AWS_REGION: {os.environ.get('AWS_REGION')}")
    
    # Verify that we can access MinIO
    try:
        client = get_minio_client()
        buckets = client.list_buckets()
        logger.info(f"Successfully connected to MinIO. Available buckets: {[b.name for b in buckets]}")
    except Exception as e:
        logger.error(f"Failed to connect to MinIO: {str(e)}")
        raise

def get_storage_options():
    """Return storage options for Delta Lake operations"""
    return {
        "endpoint_url": f"http://{MINIO_ENDPOINT}",  # Changed format for Python deltalake
        "access_key_id": MINIO_ACCESS_KEY,           # Changed keys
        "secret_access_key": MINIO_SECRET_KEY,       # Changed keys
        "region": "us-east-1",
        "allow_unsafe_rename": "true"
    }

def read_csv_with_error_handling(data):
    """Read CSV with multiple fallback methods to handle malformed data"""
    # First try: standard read with on_bad_lines='skip'
    try:
        return pd.read_csv(io.BytesIO(data), on_bad_lines='skip')
    except Exception as e:
        logger.warning(f"First CSV read attempt failed: {str(e)}")
        
    # Second try: use Python's CSV module to preprocess
    try:
        # Create a string from the data
        text_data = data.decode('utf-8', errors='replace')
        
        # Fix common CSV issues
        lines = text_data.splitlines()
        fixed_lines = []
        
        for line in lines:
            # Count quotes - if odd number, add one at the end
            if line.count('"') % 2 == 1:
                line = line + '"'
            fixed_lines.append(line)
        
        fixed_data = '\n'.join(fixed_lines)
        return pd.read_csv(io.StringIO(fixed_data), quoting=csv.QUOTE_MINIMAL)
    except Exception as e:
        logger.warning(f"Second CSV read attempt failed: {str(e)}")
    
    # Final try: read with most permissive settings
    try:
        return pd.read_csv(
            io.BytesIO(data),
            engine='python',  # Switch to Python parser which is more forgiving
            sep=None,         # Auto-detect separator
            quotechar='"',    
            escapechar='\\',
            on_bad_lines='skip',
            quoting=csv.QUOTE_MINIMAL
        )
    except Exception as e:
        logger.error(f"All CSV read attempts failed: {str(e)}")
        raise

def save_as_parquet_in_minio(df, bucket_name, object_name):
    """Save DataFrame as parquet in MinIO"""
    client = get_minio_client()
    ensure_bucket_exists(client, bucket_name)
    
    temp_path = f"/tmp/{object_name}.parquet"
    
    try:
        # Save as parquet
        df.to_parquet(temp_path, index=False)
        
        # Upload to MinIO
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

def collections_to_delta(**context):
    """Convert collections data to Delta format"""
    if not DELTA_LAKE_AVAILABLE:
        logger.error("Delta Lake library not available. Skipping delta conversion.")
        return False
        
    client = get_minio_client()
    ensure_bucket_exists(client, DELTA_BUCKET)
    ensure_bucket_exists(client, PARQUET_BUCKET)
    setup_delta_environment()
    
    try:
        # Check if collections bucket exists
        if not client.bucket_exists("isic-collections"):
            logger.error("Source bucket 'isic-collections' does not exist!")
            return False
            
        # Verify file exists
        try:
            client.stat_object("isic-collections", "collections_latest.csv")
            logger.info("Found collections_latest.csv file")
        except Exception as e:
            logger.error(f"collections_latest.csv file not found: {str(e)}")
            return False
            
        logger.info("Reading collections data from MinIO")
        response = client.get_object("isic-collections", "collections_latest.csv")
        
        # Use our enhanced CSV reader
        try:
            df = read_csv_with_error_handling(response.data)
            logger.info(f"Successfully read collections data with {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to read collections CSV with error handling: {str(e)}")
            # Create a minimal dataframe to continue the workflow
            df = pd.DataFrame({
                'id': ['placeholder'],
                'name': ['Error reading collections'],
                'description': ['Data could not be parsed'],
                'public': [False],
                'image_count': [0]
            })
        finally:
            response.close()
        
        # Save as parquet in MinIO
        save_as_parquet_in_minio(df, PARQUET_BUCKET, f"{DELTA_COLLECTIONS}_latest")
        
        # Create a temporary directory for Delta table
        temp_dir = tempfile.mkdtemp()
        delta_dir = os.path.join(temp_dir, DELTA_COLLECTIONS)
        os.makedirs(delta_dir, exist_ok=True)
        
        try:
            # Write directly to local path
            write_deltalake(
                delta_dir,
                df
            )
            logger.info(f"Created Delta table locally at {delta_dir}")
            
            # Now upload all files from the delta directory to MinIO
            for root, _, files in os.walk(delta_dir):
                for file in files:
                    local_path = os.path.join(root, file)
                    rel_path = os.path.relpath(local_path, temp_dir)
                    
                    client.fput_object(
                        DELTA_BUCKET,
                        rel_path,
                        local_path
                    )
                    logger.info(f"Uploaded {rel_path} to {DELTA_BUCKET}")
                    
            logger.info(f"Successfully created Delta table in MinIO at {DELTA_BUCKET}/{DELTA_COLLECTIONS}")
            return True
        except Exception as e:
            logger.error(f"Error creating Delta table: {str(e)}")
            raise
        finally:
            # Clean up
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                
    except Exception as e:
        logger.error(f"Error in collections_to_delta: {str(e)}", exc_info=True)
        raise

def metadata_to_delta(**context):
    """Convert image metadata to Delta format"""
    if not DELTA_LAKE_AVAILABLE:
        logger.error("Delta Lake library not available. Skipping delta conversion.")
        return False
        
    client = get_minio_client()
    ensure_bucket_exists(client, DELTA_BUCKET)
    ensure_bucket_exists(client, PARQUET_BUCKET)
    setup_delta_environment()
    
    try:
        # Check if metadata bucket exists
        if not client.bucket_exists("isic-metadata"):
            logger.error("Source bucket 'isic-metadata' does not exist!")
            return False
            
        # Verify file exists
        try:
            client.stat_object("isic-metadata", "metadata_latest.csv")
            logger.info("Found metadata_latest.csv file")
        except Exception as e:
            logger.error(f"metadata_latest.csv file not found: {str(e)}")
            return False
            
        logger.info("Reading metadata from MinIO")
        response = client.get_object("isic-metadata", "metadata_latest.csv")
        
        # Use our enhanced CSV reader
        try:
            df = read_csv_with_error_handling(response.data)
            logger.info(f"Successfully read metadata with {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to read metadata CSV: {str(e)}")
            # Create a minimal dataframe to continue the workflow
            df = pd.DataFrame({
                'id': ['placeholder'],
                'url': [''],
                'created': [datetime.now().isoformat()],
                'updated': [datetime.now().isoformat()],
                'collection_ids': ['']
            })
        finally:
            response.close()
        
        # Save as parquet in MinIO
        save_as_parquet_in_minio(df, PARQUET_BUCKET, f"{DELTA_METADATA}_latest")
        
        # Create a temporary directory for Delta table
        temp_dir = tempfile.mkdtemp()
        delta_dir = os.path.join(temp_dir, DELTA_METADATA)
        os.makedirs(delta_dir, exist_ok=True)
        
        try:
            # Write directly to local path
            write_deltalake(
                delta_dir,
                df
            )
            logger.info(f"Created Delta table locally at {delta_dir}")
            
            # Now upload all files from the delta directory to MinIO
            for root, _, files in os.walk(delta_dir):
                for file in files:
                    local_path = os.path.join(root, file)
                    rel_path = os.path.relpath(local_path, temp_dir)
                    
                    client.fput_object(
                        DELTA_BUCKET,
                        rel_path,
                        local_path
                    )
                    logger.info(f"Uploaded {rel_path} to {DELTA_BUCKET}")
                    
            logger.info(f"Successfully created Delta table in MinIO at {DELTA_BUCKET}/{DELTA_METADATA}")
            return True
        except Exception as e:
            logger.error(f"Error creating Delta table: {str(e)}")
            raise
        finally:
            # Clean up
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                
    except Exception as e:
        logger.error(f"Error in metadata_to_delta: {str(e)}", exc_info=True)
        raise

def categories_to_delta(**context):
    """Convert categories data to Delta format"""
    if not DELTA_LAKE_AVAILABLE:
        logger.error("Delta Lake library not available. Skipping delta conversion.")
        return False
        
    client = get_minio_client()
    ensure_bucket_exists(client, DELTA_BUCKET)
    ensure_bucket_exists(client, PARQUET_BUCKET)
    setup_delta_environment()
    
    try:
        # Check if categories bucket exists
        if not client.bucket_exists("isic-categories"):
            logger.error("Source bucket 'isic-categories' does not exist!")
            return False
            
        # Verify file exists
        try:
            client.stat_object("isic-categories", "categories_latest.csv")
            logger.info("Found categories_latest.csv file")
        except Exception as e:
            logger.error(f"categories_latest.csv file not found: {str(e)}")
            return False
            
        logger.info("Reading categories from MinIO")
        response = client.get_object("isic-categories", "categories_latest.csv")
        
        # Use our enhanced CSV reader
        try:
            df = read_csv_with_error_handling(response.data)
            logger.info(f"Successfully read categories with {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to read categories CSV: {str(e)}")
            # Create a minimal dataframe to continue the workflow
            df = pd.DataFrame({
                'name': ['placeholder'],
                'count': [0],
                'examples': ['[]'],
                'occurrence_percentage': [0.0]
            })
        finally:
            response.close()
        
        # Save as parquet in MinIO
        save_as_parquet_in_minio(df, PARQUET_BUCKET, f"{DELTA_CATEGORIES}_latest")
        
        # Create a temporary directory for Delta table
        temp_dir = tempfile.mkdtemp()
        delta_dir = os.path.join(temp_dir, DELTA_CATEGORIES)
        os.makedirs(delta_dir, exist_ok=True)
        
        try:
            # Write directly to local path
            write_deltalake(
                delta_dir,
                df
            )
            logger.info(f"Created Delta table locally at {delta_dir}")
            
            # Now upload all files from the delta directory to MinIO
            for root, _, files in os.walk(delta_dir):
                for file in files:
                    local_path = os.path.join(root, file)
                    rel_path = os.path.relpath(local_path, temp_dir)
                    
                    client.fput_object(
                        DELTA_BUCKET,
                        rel_path,
                        local_path
                    )
                    logger.info(f"Uploaded {rel_path} to {DELTA_BUCKET}")
                    
            logger.info(f"Successfully created Delta table in MinIO at {DELTA_BUCKET}/{DELTA_CATEGORIES}")
            return True
        except Exception as e:
            logger.error(f"Error creating Delta table: {str(e)}")
            raise
        finally:
            # Clean up
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                
    except Exception as e:
        logger.error(f"Error in categories_to_delta: {str(e)}", exc_info=True)
        raise

def create_unified_delta_table(**context):
    """Create a unified Delta table with all data sources"""
    if not DELTA_LAKE_AVAILABLE:
        logger.error("Delta Lake library not available. Skipping delta conversion.")
        return False
        
    client = get_minio_client()
    ensure_bucket_exists(client, DELTA_BUCKET)
    ensure_bucket_exists(client, PARQUET_BUCKET)
    setup_delta_environment()
    
    try:
        # First we'll load from parquet since that's more reliable
        for required_file in [f"{DELTA_METADATA}_latest.parquet", f"{DELTA_COLLECTIONS}_latest.parquet"]:
            if not object_exists(client, PARQUET_BUCKET, required_file):
                logger.error(f"Required file {required_file} not found in {PARQUET_BUCKET}")
                # If parquet not found, try from csv
                continue
        
        # Get metadata from parquet or CSV
        try:
            if object_exists(client, PARQUET_BUCKET, f"{DELTA_METADATA}_latest.parquet"):
                metadata_df = get_dataframe_from_parquet(client, PARQUET_BUCKET, f"{DELTA_METADATA}_latest.parquet")
                logger.info(f"Loaded metadata from parquet with {len(metadata_df)} records")
            else:
                response = client.get_object("isic-metadata", "metadata_latest.csv")
                metadata_df = read_csv_with_error_handling(response.data)
                response.close()
                logger.info(f"Loaded metadata from CSV with {len(metadata_df)} records")
        except Exception as e:
            logger.error(f"Error reading metadata: {str(e)}")
            # Create minimal dataframe
            metadata_df = pd.DataFrame({
                'id': ['placeholder'],
                'url': [''],
                'collection_ids': ['']
            })
        
        # Get collections from parquet or CSV
        try:
            if object_exists(client, PARQUET_BUCKET, f"{DELTA_COLLECTIONS}_latest.parquet"):
                collections_df = get_dataframe_from_parquet(client, PARQUET_BUCKET, f"{DELTA_COLLECTIONS}_latest.parquet")
                logger.info(f"Loaded collections from parquet with {len(collections_df)} records")
            else:
                response = client.get_object("isic-collections", "collections_latest.csv")
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
        
        # Convert collections to a lookup dictionary
        collection_dict = {row['id']: row for _, row in collections_df.iterrows()}
        
        # Add collection details - with error handling
        try:
            def get_collection_name(row):
                try:
                    if pd.isna(row.get('collection_ids')) or not row.get('collection_ids', ''):
                        return ''
                    
                    collection_id = row.get('collection_ids', '').split(',')[0]
                    return collection_dict.get(collection_id, {}).get('name', '')
                except Exception:
                    return ''
            
            unified_df['collection_name'] = unified_df.apply(get_collection_name, axis=1)
        except Exception as e:
            logger.warning(f"Error adding collection names: {str(e)}")
            unified_df['collection_name'] = ''
        
        # Check for downloaded images
        try:
            if client.bucket_exists("isic-images"):
                objects = list(client.list_objects("isic-images", recursive=True))
                downloaded_images = {obj.object_name.split('.')[0] for obj in objects if obj.object_name.endswith('.jpg')}
                unified_df['image_downloaded'] = unified_df['id'].apply(lambda x: x in downloaded_images)
                logger.info(f"Found {len(downloaded_images)} downloaded images")
            else:
                logger.warning("isic-images bucket does not exist, marking all images as not downloaded")
                unified_df['image_downloaded'] = False
        except Exception as e:
            logger.warning(f"Error checking downloaded images: {str(e)}")
            unified_df['image_downloaded'] = False
        
        # Save as parquet in MinIO for downstream processing
        save_as_parquet_in_minio(unified_df, PARQUET_BUCKET, f"{DELTA_UNIFIED}_latest")
        
        # Create a temporary directory for Delta table
        temp_dir = tempfile.mkdtemp()
        delta_dir = os.path.join(temp_dir, DELTA_UNIFIED)
        os.makedirs(delta_dir, exist_ok=True)
        
        try:
            # Write directly to local path
            write_deltalake(
                delta_dir,
                unified_df
            )
            logger.info(f"Created Delta table locally at {delta_dir}")
            
            # Now upload all files from the delta directory to MinIO
            for root, _, files in os.walk(delta_dir):
                for file in files:
                    local_path = os.path.join(root, file)
                    rel_path = os.path.relpath(local_path, temp_dir)
                    
                    client.fput_object(
                        DELTA_BUCKET,
                        rel_path,
                        local_path
                    )
                    logger.info(f"Uploaded {rel_path} to {DELTA_BUCKET}")
                    
            logger.info(f"Successfully created Delta table in MinIO at {DELTA_BUCKET}/{DELTA_UNIFIED}")
            return True
        except Exception as e:
            logger.error(f"Error creating Delta table: {str(e)}")
            raise
        finally:
            # Clean up
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            
        return True
    except Exception as e:
        logger.error(f"Error creating unified Delta table: {str(e)}", exc_info=True)
        raise

def object_exists(client, bucket_name, object_name):
    """Check if an object exists in MinIO"""
    try:
        client.stat_object(bucket_name, object_name)
        return True
    except Exception:
        return False

def get_dataframe_from_parquet(client, bucket_name, object_name):
    """Get a DataFrame from a parquet file in MinIO"""
    temp_path = f"/tmp/{object_name}"
    
    try:
        client.fget_object(bucket_name, object_name, temp_path)
        df = pd.read_parquet(temp_path)
        return df
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

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