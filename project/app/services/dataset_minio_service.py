import asyncio
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from io import BytesIO
import pyspark
from delta import configure_spark_with_delta_pip
from minio import Minio
from minio.error import S3Error
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType
import os
import logging

# Set up logger
logger = logging.getLogger(__name__)

from .connectors.minio_connector import test_minio_connection
class DatasetMinioService:
    """Service for managing dataset exports to MinIO with Delta Lake metadata"""
    
    def __init__(self, minio_config: Dict[str, Any]):
        self.minio_config = minio_config
        self.minio_client = None
        self.spark = None
        
    async def initialize(self):
        """Initialize MinIO client and Spark session"""
        try:
            # Initialize MinIO client
            endpoint = self.minio_config.get('endpoint', 'localhost:9000')
            access_key = self.minio_config.get('access_key')
            secret_key = self.minio_config.get('secret_key')
            secure = self.minio_config.get('secure', False)
            
            # Remove http:// or https:// prefix if present
            if endpoint.startswith('http://'):
                endpoint = endpoint[7:]
                secure = False
            elif endpoint.startswith('https://'):
                endpoint = endpoint[8:]
                secure = True
            
            self.minio_client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
            
            # Initialize Spark session for Delta Lake
            self.spark = self._create_spark_session()
            
            logger.info("DatasetMinioService initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize DatasetMinioService: {e}")
            raise
    
    def _create_spark_session(self):
        """Create Spark session configured for Delta Lake and MinIO"""
        try:
            endpoint_with_protocol = f"http://{self.minio_config.get('endpoint', 'localhost:9000')}"
            if self.minio_config.get('secure', False):
                endpoint_with_protocol = f"https://{self.minio_config.get('endpoint', 'localhost:9000')}"

            print("--------------  minio_config  ---------------")
            print(endpoint_with_protocol)
            print(self.minio_config.get('access_key'))
            print(self.minio_config.get('secret_key'))
            print(self.minio_config.get('secure'))
            
            builder = pyspark.sql.SparkSession.builder \
                .appName("DatasetMinioService") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.hadoop.fs.s3a.endpoint", endpoint_with_protocol) \
                .config("spark.hadoop.fs.s3a.access.key", self.minio_config.get('access_key')) \
                .config("spark.hadoop.fs.s3a.secret.key", self.minio_config.get('secret_key')) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(self.minio_config.get('secure', False)).lower()) \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            
            packages = [
                "org.apache.hadoop:hadoop-aws:3.4.0",
                "io.delta:delta-spark_2.13:4.0.0",
            ]
            spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
            
            # Set log level to reduce noise
            spark.sparkContext.setLogLevel("WARN")
            
            return spark
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            raise
    
    async def create_dataset_bucket(self, dataset_id: int, dataset_name: str) -> str:
        """Create a new bucket for the dataset"""
        try:
            # Generate bucket name (lowercase, no spaces, no special chars)
            bucket_name = f"dataset-{dataset_id}-{dataset_name.lower().replace(' ', '-').replace('_', '-')}"
            # Remove any non-alphanumeric characters except hyphens
            bucket_name = ''.join(c for c in bucket_name if c.isalnum() or c == '-')
            # Ensure it doesn't start or end with a hyphen
            bucket_name = bucket_name.strip('-')
            
            # Check if bucket already exists
            if self.minio_client.bucket_exists(bucket_name):
                logger.info(f"Bucket {bucket_name} already exists")
                return bucket_name
            
            # Create bucket
            self.minio_client.make_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")
            
            # Create folder structure
            await self._create_bucket_structure(bucket_name)
            
            return bucket_name
            
        except S3Error as e:
            logger.error(f"Failed to create bucket for dataset {dataset_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating bucket for dataset {dataset_id}: {e}")
            raise
    
    async def _create_bucket_structure(self, bucket_name: str):
        """Create folder structure in the bucket"""
        try:
            folders = [
                "metadata/",
                "data/",
                "logs/",
                "schemas/",
                "manifests/"
            ]
            
            for folder in folders:
                # Create an empty object to represent the folder
                empty_stream = BytesIO(b"")
                try:
                    self.minio_client.put_object(
                        bucket_name,
                        folder + ".keep",
                        data=empty_stream,
                        length=0
                    )
                    logger.debug(f"Created folder: {folder}")
                except Exception as e:
                    logger.error(f"Failed to create folder {folder}: {e}")
                    raise
            
            logger.info(f"Created folder structure in bucket {bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to create folder structure in bucket {bucket_name}: {e}")
            raise
    
    async def export_dataset_with_real_data_to_delta(
        self, 
        dataset_id: int, 
        bucket_name: str, 
        dataset_metadata: Dict[str, Any],
        columns_metadata: List[Dict[str, Any]],
        sources_metadata: List[Dict[str, Any]],
        unified_data_df: pd.DataFrame  # Changed to accept pandas DataFrame
    ) -> str:
        """Export dataset with real data to Delta Lake format in MinIO"""
        try:
            # Export metadata first
            await self._export_metadata_to_delta(dataset_id, bucket_name, dataset_metadata, columns_metadata, sources_metadata)
            
            # Export unified real data
            data_path = await self._export_unified_dataframe_to_delta(dataset_id, bucket_name, unified_data_df)
            
            logger.info(f"Exported dataset {dataset_id} with unified real data to Delta Lake")
            return data_path
            
        except Exception as e:
            logger.error(f"Failed to export dataset {dataset_id} with real data to Delta Lake: {e}")
            raise

    async def export_dataset_metadata_to_delta(
        self, 
        dataset_id: int, 
        bucket_name: str, 
        dataset_metadata: Dict[str, Any],
        columns_metadata: List[Dict[str, Any]],
        sources_metadata: List[Dict[str, Any]]
    ) -> str:
        """Export only dataset metadata to Delta Lake (for use with incremental data save)"""
        try:
            # Export metadata only (data was already saved incrementally)
            await self._export_metadata_to_delta(dataset_id, bucket_name, dataset_metadata, columns_metadata, sources_metadata)
            
            # Return the data path (where incremental data was saved)
            data_path = f"s3a://{bucket_name}/data/unified_data"
            
            logger.info(f"Exported metadata for dataset {dataset_id} to Delta Lake (data was saved incrementally)")
            return data_path
            
        except Exception as e:
            logger.error(f"Failed to export metadata for dataset {dataset_id} to Delta Lake: {e}")
            raise
    
    async def _export_metadata_to_delta(
        self, 
        dataset_id: int, 
        bucket_name: str, 
        dataset_metadata: Dict[str, Any],
        columns_metadata: List[Dict[str, Any]],
        sources_metadata: List[Dict[str, Any]]
    ) -> str:
        """Export only metadata to Delta Lake (original functionality)"""
        try:
            # Prepare metadata for Delta Lake
            metadata_records = []
            
            # Add dataset record with all required fields
            dataset_record = {
                "record_type": "dataset",
                "dataset_id": int(dataset_id),
                "name": str(dataset_metadata.get("name", "")),
                "description": str(dataset_metadata.get("description", "")),
                "data_type": None,
                "storage_type": str(dataset_metadata.get("storage_type", "")),
                "refresh_type": str(dataset_metadata.get("refresh_type", "")),
                "status": str(dataset_metadata.get("status", "")),
                "version": int(float(str(dataset_metadata.get("version", 1)))),  # Convert to int safely
                "properties": json.dumps(dataset_metadata.get("properties", {})),
                "is_nullable": None,
                "column_position": None,
                "is_visible": None,
                "table_id": None,
                "join_type": None,
                "join_condition": None,
                "filter_condition": None,
                "data_criacao": datetime.now(),
                "data_atualizacao": datetime.now()
            }
            metadata_records.append(dataset_record)
            
            # Add column records with all required fields
            for col in columns_metadata:
                col_record = {
                    "record_type": "column",
                    "dataset_id": int(dataset_id),
                    "name": str(col.get("name", "")),
                    "description": str(col.get("description", "") if col.get("description") else ""),
                    "data_type": str(col.get("data_type", "")),
                    "storage_type": None,
                    "refresh_type": None,
                    "status": None,
                    "version": None,
                    "properties": json.dumps(col.get("properties", {})),
                    "is_nullable": bool(col.get("is_nullable", True)),
                    "column_position": int(col.get("column_position", 0)) if col.get("column_position") is not None else None,
                    "is_visible": bool(col.get("is_visible", True)),
                    "table_id": None,
                    "join_type": None,
                    "join_condition": None,
                    "filter_condition": None,
                    "data_criacao": datetime.now(),
                    "data_atualizacao": datetime.now()
                }
                metadata_records.append(col_record)
            
            # Add source records with all required fields
            for source in sources_metadata:
                source_record = {
                    "record_type": "source",
                    "dataset_id": int(dataset_id),
                    "name": None,
                    "description": None,
                    "data_type": None,
                    "storage_type": None,
                    "refresh_type": None,
                    "status": None,
                    "version": None,
                    "properties": None,
                    "is_nullable": None,
                    "column_position": None,
                    "is_visible": None,
                    "table_id": int(source.get("table_id")) if source.get("table_id") is not None else None,
                    "join_type": str(source.get("join_type", "")),
                    "join_condition": str(source.get("join_condition", "")) if source.get("join_condition") else None,
                    "filter_condition": str(source.get("filter_condition", "")) if source.get("filter_condition") else None,
                    "data_criacao": datetime.now(),
                    "data_atualizacao": datetime.now()
                }
                metadata_records.append(source_record)
            
            # Define explicit schema for DataFrame
            schema = StructType([
                StructField("record_type", StringType(), False),
                StructField("dataset_id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("data_type", StringType(), True),
                StructField("storage_type", StringType(), True),
                StructField("refresh_type", StringType(), True),
                StructField("status", StringType(), True),
                StructField("version", IntegerType(), True),
                StructField("properties", StringType(), True),
                StructField("is_nullable", BooleanType(), True),
                StructField("column_position", IntegerType(), True),
                StructField("is_visible", BooleanType(), True),
                StructField("table_id", IntegerType(), True),
                StructField("join_type", StringType(), True),
                StructField("join_condition", StringType(), True),
                StructField("filter_condition", StringType(), True),
                StructField("data_criacao", TimestampType(), False),
                StructField("data_atualizacao", TimestampType(), False)
            ])
            
            # Convert to Spark DataFrame with explicit schema
            df = self.spark.createDataFrame(metadata_records, schema)
            
            # Define Delta table path
            delta_path = f"s3a://{bucket_name}/metadata/dataset_metadata"
            
            # Write to Delta Lake
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(delta_path)
            
            logger.info(f"Exported dataset {dataset_id} metadata to Delta Lake at {delta_path}")
            
            # Create manifest file
            await self._create_manifest_file(bucket_name, dataset_id, metadata_records)
            
            return delta_path
            
        except Exception as e:
            logger.error(f"Failed to export dataset {dataset_id} metadata to Delta Lake: {e}")
            raise
    
    async def _export_unified_dataframe_to_delta(self, dataset_id: int, bucket_name: str, unified_df: pd.DataFrame) -> str:
        """Export unified pandas DataFrame to Delta Lake"""
        try:
            if unified_df.empty:
                logger.warning(f"No unified data to export for dataset {dataset_id}")
                return f"s3a://{bucket_name}/data/unified_data"
            
            # Add dataset metadata columns
            unified_df = unified_df.copy()
            unified_df["_dataset_id"] = dataset_id
            unified_df["_extraction_timestamp"] = datetime.now()
            
            # Clean up the DataFrame: convert nan/NaT to proper NULL values
            # This ensures proper NULL handling in Delta Lake instead of string representations
            import numpy as np
            from pyspark.sql.types import StructType, StructField, StringType
            
            cleaned_df = unified_df.copy()
            for column in cleaned_df.columns:
                if column not in ["_dataset_id", "_extraction_timestamp"]:
                    # Replace various forms of missing values with None (which becomes NULL in Delta Lake)
                    cleaned_df[column] = cleaned_df[column].replace({
                        'nan': None,
                        'NaT': None,
                        np.nan: None,
                        pd.NaT: None,
                        'None': None,
                        '': None
                    })
                    
                    # Convert remaining values to string but keep None as None
                    cleaned_df[column] = cleaned_df[column].apply(
                        lambda x: None if pd.isna(x) or x in ['nan', 'NaT'] else str(x)
                    )
            
            # Define explicit schema to avoid type inference issues
            schema_fields = []
            for column in cleaned_df.columns:
                if column in ["_dataset_id"]:
                    schema_fields.append(StructField(column, IntegerType(), True))
                elif column in ["_extraction_timestamp"]:
                    schema_fields.append(StructField(column, TimestampType(), True))
                else:
                    # All data columns as string to avoid type conflicts
                    schema_fields.append(StructField(column, StringType(), True))
            
            explicit_schema = StructType(schema_fields)
            
            # Convert cleaned pandas DataFrame to Spark DataFrame with explicit schema
            spark_df = self.spark.createDataFrame(cleaned_df, schema=explicit_schema)
            
            # Define Delta table path for unified data
            delta_path = f"s3a://{bucket_name}/data/unified_data"
            
            # Write to Delta Lake
            spark_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(delta_path)
            
            logger.info(f"Exported {len(unified_df)} unified rows to Delta Lake at {delta_path}")
            return delta_path
            
        except Exception as e:
            logger.error(f"Failed to export unified DataFrame to Delta Lake: {e}")
            raise

    async def initialize_incremental_delta_table(
        self, 
        dataset_id: int, 
        bucket_name: str, 
        column_names: List[str]
    ) -> str:
        """Initialize an empty Delta table for incremental writes"""
        try:
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
            
            logger.info(f"Initializing incremental Delta table for dataset {dataset_id} with {len(column_names)} columns")
            
            # Validate inputs
            if not self.spark:
                raise ValueError("Spark session is not initialized")
            
            if not bucket_name:
                raise ValueError("Bucket name cannot be empty")
            
            if not column_names:
                logger.warning("No column names provided, creating table with metadata columns only")
            
            # Create schema for the Delta table
            schema_fields = []
            
            # Add data columns first
            for column in column_names:
                if column not in ["_dataset_id", "_extraction_timestamp", "_batch_id"]:
                    # All data columns as string to avoid type conflicts
                    schema_fields.append(StructField(column, StringType(), True))
            
            # Add metadata columns (these are added automatically during processing)
            schema_fields.extend([
                StructField("_dataset_id", IntegerType(), True),
                StructField("_extraction_timestamp", TimestampType(), True),
                StructField("_batch_id", IntegerType(), True)
            ])
            
            schema = StructType(schema_fields)
            logger.info(f"Created schema with {len(schema_fields)} fields")
            
            # Create empty DataFrame with the schema
            empty_df = self.spark.createDataFrame([], schema)
            logger.info(f"Created empty DataFrame with schema")
            
            # Define Delta table path
            delta_path = f"s3a://{bucket_name}/data/unified_data"
            logger.info(f"Writing to Delta path: {delta_path}")
            
            # Write empty Delta table to establish schema
            empty_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(delta_path)
            
            logger.info(f"Successfully initialized incremental Delta table for dataset {dataset_id} at {delta_path}")
            return delta_path
            
        except Exception as e:
            logger.error(f"Failed to initialize incremental Delta table for dataset {dataset_id}: {e}")
            logger.error(f"Error details - bucket: {bucket_name}, columns: {len(column_names) if column_names else 0}")
            raise

    async def append_batch_to_delta_table(
        self, 
        dataset_id: int, 
        bucket_name: str, 
        batch_df: pd.DataFrame, 
        batch_id: int
    ) -> bool:
        """Append a batch of data to the existing Delta table"""
        try:
            # Validate inputs
            if not self.spark:
                raise ValueError("Spark session is not initialized")
                
            if batch_df.empty:
                logger.warning(f"Empty batch {batch_id} for dataset {dataset_id} - skipping")
                return False
            
            if not bucket_name:
                raise ValueError("Bucket name cannot be empty")
            
            logger.info(f"Processing batch {batch_id} for dataset {dataset_id} with {len(batch_df)} rows")
            
            # Add metadata columns
            batch_df = batch_df.copy()
            batch_df["_dataset_id"] = dataset_id
            batch_df["_extraction_timestamp"] = datetime.now()
            batch_df["_batch_id"] = batch_id
            
            # Use helper method to prepare DataFrame for Spark
            try:
                cleaned_df = self._prepare_dataframe_for_spark(batch_df)
                logger.info(f"Successfully prepared DataFrame for batch {batch_id}")
            except Exception as prep_error:
                logger.error(f"Failed to prepare DataFrame for batch {batch_id}: {prep_error}")
                raise
            
            # Create explicit schema to avoid type inference issues
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
            
            schema_fields = []
            for column in cleaned_df.columns:
                if column == "_dataset_id":
                    schema_fields.append(StructField(column, IntegerType(), True))
                elif column in ["_extraction_timestamp"]:
                    schema_fields.append(StructField(column, TimestampType(), True))
                elif column == "_batch_id":
                    schema_fields.append(StructField(column, IntegerType(), True))
                else:
                    # All data columns as string to avoid type conflicts
                    schema_fields.append(StructField(column, StringType(), True))
            
            explicit_schema = StructType(schema_fields)
            
            # Debug: Log schema and sample data
            logger.info(f"Creating Spark DataFrame for batch {batch_id} with {len(cleaned_df)} rows and {len(cleaned_df.columns)} columns")
            
            try:
                # Convert to Spark DataFrame with explicit schema
                spark_df = self.spark.createDataFrame(cleaned_df, schema=explicit_schema)
                logger.info(f"Successfully created Spark DataFrame for batch {batch_id}")
            except Exception as spark_error:
                logger.error(f"Failed to create Spark DataFrame for batch {batch_id}: {spark_error}")
                logger.error(f"DataFrame sample: {cleaned_df.head()}")
                logger.error(f"DataFrame dtypes: {cleaned_df.dtypes}")
                logger.error(f"Expected schema fields: {[f.name for f in explicit_schema.fields]}")
                raise
            
            # Define Delta table path
            delta_path = f"s3a://{bucket_name}/data/unified_data"
            logger.info(f"Writing batch {batch_id} to Delta path: {delta_path}")
            
            try:
                # Append to existing Delta table
                spark_df.write \
                    .format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .save(delta_path)
                
                logger.info(f"Successfully appended batch {batch_id} ({len(batch_df)} rows) to Delta table for dataset {dataset_id}")
            except Exception as delta_error:
                logger.error(f"Failed to write batch {batch_id} to Delta table: {delta_error}")
                logger.error(f"Delta path: {delta_path}")
                logger.error(f"Spark DataFrame schema: {spark_df.schema}")
                raise
            
            # Clear DataFrame from memory
            del batch_df, cleaned_df, spark_df
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to append batch {batch_id} to Delta table: {e}")
            
            # Additional debugging information
            try:
                logger.error(f"Batch DataFrame info - Shape: {batch_df.shape}, Columns: {list(batch_df.columns)}")
                logger.error(f"DataFrame dtypes: {batch_df.dtypes.to_dict()}")
                
                # Check for completely empty columns
                empty_cols = [col for col in batch_df.columns if batch_df[col].isna().all()]
                if empty_cols:
                    logger.error(f"Completely empty columns detected: {empty_cols}")
                
                # Check for columns with mixed types
                for col in batch_df.columns:
                    unique_types = set(type(x).__name__ for x in batch_df[col].dropna().values[:5])
                    if len(unique_types) > 1:
                        logger.error(f"Column {col} has mixed types: {unique_types}")
                        
            except Exception as debug_error:
                logger.error(f"Could not gather debug info: {debug_error}")
            
            raise

    async def finalize_incremental_delta_table(
        self, 
        dataset_id: int, 
        bucket_name: str
    ) -> bool:
        """Finalize the incremental Delta table (optimize, vacuum, etc.)"""
        try:
            delta_path = f"s3a://{bucket_name}/data/unified_data"
            
            # Read the Delta table to get the total count
            df = self.spark.read.format("delta").load(delta_path)
            total_rows = df.count()
            
            logger.info(f"Finalizing Delta table for dataset {dataset_id} with {total_rows} total rows")
            
            # Optimize the Delta table (compaction)
            try:
                from delta.tables import DeltaTable
                delta_table = DeltaTable.forPath(self.spark, delta_path)
                delta_table.optimize().executeCompaction()
                logger.info(f"Optimized Delta table for dataset {dataset_id}")
            except Exception as optimize_error:
                logger.warning(f"Could not optimize Delta table: {optimize_error}")
            
            # Vacuum old files (keep 7 days of history)
            try:
                delta_table = DeltaTable.forPath(self.spark, delta_path)
                delta_table.vacuum(168)  # 7 days * 24 hours
                logger.info(f"Vacuumed Delta table for dataset {dataset_id}")
            except Exception as vacuum_error:
                logger.warning(f"Could not vacuum Delta table: {vacuum_error}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to finalize incremental Delta table: {e}")
            return False

    def _prepare_dataframe_for_spark(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare pandas DataFrame for Spark to avoid type inference issues"""
        try:
            import numpy as np
            
            cleaned_df = df.copy()
            
            # Fix each column individually
            for column in cleaned_df.columns:
                if column not in ["_dataset_id", "_extraction_timestamp", "_batch_id"]:
                    # Convert to object type first to handle mixed types
                    cleaned_df[column] = cleaned_df[column].astype('object')
                    
                    # Replace various forms of missing values
                    cleaned_df[column] = cleaned_df[column].replace({
                        'nan': None,
                        'NaT': None,
                        np.nan: None,
                        pd.NaT: None,
                        'None': None,
                        '': None,
                        'null': None,
                        'NULL': None
                    })
                    
                    # Convert to string, handling None values properly
                    cleaned_df[column] = cleaned_df[column].apply(
                        lambda x: None if (x is None or pd.isna(x) or str(x).lower() in ['nan', 'nat', 'none', 'null']) else str(x)
                    )
                    
                    # If column is still all None/NaN, fill with empty string
                    if cleaned_df[column].isna().all() or all(x is None for x in cleaned_df[column]):
                        cleaned_df[column] = ""
                        logger.warning(f"Column {column} was completely empty, filled with empty strings")
            
            # Ensure metadata columns have correct types
            if "_dataset_id" in cleaned_df.columns:
                cleaned_df["_dataset_id"] = cleaned_df["_dataset_id"].astype(int)
            if "_batch_id" in cleaned_df.columns:
                cleaned_df["_batch_id"] = cleaned_df["_batch_id"].astype(int)
            
            logger.debug(f"Prepared DataFrame with shape {cleaned_df.shape} and dtypes: {cleaned_df.dtypes.to_dict()}")
            return cleaned_df
            
        except Exception as e:
            logger.error(f"Error preparing DataFrame for Spark: {e}")
            raise
    
    async def _export_real_data_to_delta(self, dataset_id: int, bucket_name: str, real_data: Dict[str, Any]) -> str:
        """Export real extracted data to Delta Lake"""
        try:
            all_unified_data = []
            
            # Process data from each table
            for table_id, table_info in real_data.get("tables", {}).items():
                table_data = table_info.get("data", [])
                table_name = table_info.get("table_name")
                connection_type = table_info.get("connection_type")
                
                # Add metadata to each row
                for row in table_data:
                    # Add source information to each row
                    enhanced_row = row.copy()
                    enhanced_row["_dataset_id"] = dataset_id
                    enhanced_row["_source_table_id"] = int(table_id)
                    enhanced_row["_source_table_name"] = table_name
                    enhanced_row["_source_connection_type"] = connection_type
                    enhanced_row["_extraction_timestamp"] = datetime.now()
                    
                    all_unified_data.append(enhanced_row)
            
            if not all_unified_data:
                logger.warning(f"No real data to export for dataset {dataset_id}")
                return f"s3a://{bucket_name}/data/unified_data"
            
            # Convert to Spark DataFrame
            # Create a more flexible schema that can handle various data types
            sample_row = all_unified_data[0]
            schema_fields = []
            
            for column_name, value in sample_row.items():
                if isinstance(value, int):
                    field_type = IntegerType()
                elif isinstance(value, float):
                    field_type = StringType()  # Use string to handle float precision
                elif isinstance(value, bool):
                    field_type = BooleanType()
                elif isinstance(value, datetime):
                    field_type = TimestampType()
                else:
                    field_type = StringType()  # Default to string for safety
                
                schema_fields.append(StructField(column_name, field_type, True))
            
            data_schema = StructType(schema_fields)
            
            # Normalize data to match schema
            normalized_data = []
            for row in all_unified_data:
                normalized_row = {}
                for field in schema_fields:
                    field_name = field.name
                    value = row.get(field_name)
                    
                    # Convert values to string if they don't match expected type
                    if value is not None:
                        if field.dataType == StringType():
                            normalized_row[field_name] = str(value)
                        elif field.dataType == IntegerType():
                            try:
                                normalized_row[field_name] = int(value) if value is not None else None
                            except (ValueError, TypeError):
                                normalized_row[field_name] = None
                        elif field.dataType == BooleanType():
                            normalized_row[field_name] = bool(value) if value is not None else None
                        elif field.dataType == TimestampType():
                            normalized_row[field_name] = value if isinstance(value, datetime) else datetime.now()
                        else:
                            normalized_row[field_name] = value
                    else:
                        normalized_row[field_name] = None
                
                normalized_data.append(normalized_row)
            
            # Create DataFrame with real data
            df = self.spark.createDataFrame(normalized_data, data_schema)
            
            # Define Delta table path for real data
            data_path = f"s3a://{bucket_name}/data/unified_data"
            
            # Write real data to Delta Lake
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(data_path)
            
            logger.info(f"Exported {len(normalized_data)} rows of real data to Delta Lake at {data_path}")
            
            # Create data manifest
            await self._create_data_manifest_file(bucket_name, dataset_id, {
                "total_rows": len(normalized_data),
                "tables_count": len(real_data.get("tables", {})),
                "columns_count": len(schema_fields),
                "data_path": data_path
            })
            
            return data_path
            
        except Exception as e:
            logger.error(f"Failed to export real data for dataset {dataset_id}: {e}")
            raise
    
    async def _create_manifest_file(self, bucket_name: str, dataset_id: int, metadata_records: List[Dict]):
        """Create a manifest file with export information"""
        try:
            manifest = {
                "dataset_id": dataset_id,
                "export_timestamp": datetime.now().isoformat(),
                "total_records": len(metadata_records),
                "record_types": {
                    "dataset": len([r for r in metadata_records if r["record_type"] == "dataset"]),
                    "columns": len([r for r in metadata_records if r["record_type"] == "column"]),
                    "sources": len([r for r in metadata_records if r["record_type"] == "source"])
                },
                "delta_path": f"s3a://{bucket_name}/metadata/dataset_metadata",
                "bucket_name": bucket_name,
                "export_status": "completed"
            }
            
            manifest_json = json.dumps(manifest, indent=2, default=str)
            
            # Upload manifest file
            manifest_bytes = manifest_json.encode('utf-8')
            manifest_stream = BytesIO(manifest_bytes)
            
            try:
                self.minio_client.put_object(
                    bucket_name,
                    f"manifests/dataset_{dataset_id}_manifest.json",
                    data=manifest_stream,
                    length=len(manifest_bytes),
                    content_type='application/json'
                )
                logger.info(f"Created manifest file for dataset {dataset_id}")
            except Exception as e:
                logger.error(f"Failed to upload manifest file: {e}")
                logger.error(f"Manifest stream type: {type(manifest_stream)}")
                logger.error(f"Manifest bytes length: {len(manifest_bytes)}")
                raise
                
        except Exception as e:
            logger.error(f"Failed to create manifest file for dataset {dataset_id}: {e}")
            raise
    
    async def _create_data_manifest_file(self, bucket_name: str, dataset_id: int, data_info: Dict[str, Any]):
        """Create a manifest file for the real data export"""
        try:
            manifest = {
                "dataset_id": dataset_id,
                "export_timestamp": datetime.now().isoformat(),
                "export_type": "real_data",
                "total_rows": data_info.get("total_rows", 0),
                "tables_count": data_info.get("tables_count", 0),
                "columns_count": data_info.get("columns_count", 0),
                "data_path": data_info.get("data_path"),
                "bucket_name": bucket_name,
                "export_status": "completed"
            }
            
            manifest_json = json.dumps(manifest, indent=2, default=str)
            
            # Upload data manifest file
            manifest_bytes = manifest_json.encode('utf-8')
            manifest_stream = BytesIO(manifest_bytes)
            
            try:
                self.minio_client.put_object(
                    bucket_name,
                    f"manifests/dataset_{dataset_id}_data_manifest.json",
                    data=manifest_stream,
                    length=len(manifest_bytes),
                    content_type='application/json'
                )
                logger.info(f"Created data manifest file for dataset {dataset_id}")
            except Exception as e:
                logger.error(f"Failed to upload data manifest file: {e}")
                raise
                
        except Exception as e:
            logger.error(f"Failed to create data manifest file for dataset {dataset_id}: {e}")
            raise
    
    async def validate_export(self, bucket_name: str, dataset_id: int) -> Dict[str, Any]:
        """Validate the exported dataset"""
        try:
            validation_result = {
                "valid": True,
                "errors": [],
                "warnings": [],
                "details": {}
            }
            
            # Check if bucket exists
            if not self.minio_client.bucket_exists(bucket_name):
                validation_result["valid"] = False
                validation_result["errors"].append(f"Bucket {bucket_name} does not exist")
                return validation_result
            
            # Check Delta table
            delta_path = f"s3a://{bucket_name}/metadata/dataset_metadata"
            try:
                df = self.spark.read.format("delta").load(delta_path)
                record_count = df.count()
                validation_result["details"]["record_count"] = record_count
                
                # Check record types
                record_types = df.select("record_type").distinct().collect()
                validation_result["details"]["record_types"] = [row.record_type for row in record_types]
                
                if record_count == 0:
                    validation_result["warnings"].append("Delta table is empty")
                
            except Exception as e:
                validation_result["valid"] = False
                validation_result["errors"].append(f"Failed to read Delta table: {str(e)}")
            
            # Check manifest file
            try:
                manifest_object = self.minio_client.get_object(bucket_name, f"manifests/dataset_{dataset_id}_manifest.json")
                manifest_data = json.loads(manifest_object.read().decode('utf-8'))
                validation_result["details"]["manifest"] = manifest_data
            except Exception as e:
                validation_result["warnings"].append(f"Failed to read manifest file: {str(e)}")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Failed to validate export for dataset {dataset_id}: {e}")
            return {
                "valid": False,
                "errors": [f"Validation error: {str(e)}"],
                "warnings": [],
                "details": {}
            }
    
    async def list_dataset_files(self, bucket_name: str) -> List[Dict[str, Any]]:
        """List all files in the dataset bucket"""
        try:
            files = []
            objects = self.minio_client.list_objects(bucket_name, recursive=True)
            
            for obj in objects:
                files.append({
                    "name": obj.object_name,
                    "size": obj.size,
                    "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
                    "etag": obj.etag
                })
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files in bucket {bucket_name}: {e}")
            raise
    
    async def delete_dataset_bucket(self, bucket_name: str) -> bool:
        """Delete the entire dataset bucket"""
        try:
            # First, delete all objects in the bucket
            objects = self.minio_client.list_objects(bucket_name, recursive=True)
            for obj in objects:
                self.minio_client.remove_object(bucket_name, obj.object_name)
            
            # Then delete the bucket
            self.minio_client.remove_bucket(bucket_name)
            
            logger.info(f"Deleted bucket {bucket_name} and all its contents")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete bucket {bucket_name}: {e}")
            return False
    
    def close(self):
        """Close Spark session"""
        if self.spark:
            self.spark.stop()
            self.spark = None
