"""
Spark Manager for Delta Lake Operations

This service manages PySpark sessions for Silver layer transformations.
It handles:
1. SparkSession creation (local or cluster mode)
2. Delta Lake configuration
3. S3/MinIO connectivity
4. Session lifecycle management

Note: Spark is used for Silver layer because:
- More efficient for large-scale transformations
- Native Delta Lake support with ACID transactions
- Better parallelization than Trino for ETL workloads
- UDF support for Python transformations
"""

import os
import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class SparkManager:
    """
    Manager for PySpark sessions with Delta Lake support.
    
    This service provides:
    - SparkSession creation with Delta Lake extensions
    - S3/MinIO configuration for data access
    - Support for local (standalone) and cluster modes
    
    Usage:
        spark_manager = SparkManager()
        spark = spark_manager.get_or_create_session()
        # Use spark for Delta Lake operations
        spark_manager.stop_session()  # When done
    """
    
    _instance: Optional['SparkManager'] = None
    _spark_session = None
    
    def __new__(cls):
        """Singleton pattern to reuse SparkSession across requests."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize SparkManager with configuration from environment."""
        if hasattr(self, '_initialized') and self._initialized:
            return
        
        # Spark mode: 'local' or 'cluster'
        self.spark_mode = os.getenv("SPARK_MODE", "local")
        self.spark_master = os.getenv("SPARK_MASTER", "local[*]")
        self.app_name = os.getenv("SPARK_APP_NAME", "DataFabric-Silver")
        
        # S3/MinIO configuration
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.s3_endpoint = f"http://{minio_endpoint}" if not minio_endpoint.startswith(('http://', 'https://')) else minio_endpoint
        self.s3_access_key = os.getenv("MINIO_ACCESS_KEY", "minio")
        self.s3_secret_key = os.getenv("MINIO_SECRET_KEY", "minio123")
        self.s3_region = os.getenv("AWS_REGION", "us-east-1")
        
        # Buckets
        self.bronze_bucket = os.getenv("BRONZE_BUCKET", "datafabric-bronze")
        self.silver_bucket = os.getenv("SILVER_BUCKET", "datafabric-silver")
        
        # Delta Lake settings
        self.delta_log_retention = os.getenv("DELTA_LOG_RETENTION_DAYS", "30")
        
        self._initialized = True
        logger.info(f"SparkManager initialized with mode: {self.spark_mode}")
    
    def get_or_create_session(self):
        """
        Get or create a SparkSession configured for Delta Lake.
        
        Returns:
            SparkSession: A configured SparkSession instance
        """
        if self._spark_session is not None:
            try:
                # Check if session is still active by doing a simple operation
                # This catches both stopped sessions and lost py4j connections
                _ = self._spark_session.sparkContext.getConf().get("spark.app.name")
                if not self._spark_session.sparkContext._jsc.sc().isStopped():
                    return self._spark_session
            except Exception as e:
                # Session is dead or connection lost, create new one
                logger.warning(f"Existing SparkSession is dead ({type(e).__name__}), creating new one")
                try:
                    self._spark_session.stop()
                except:
                    pass
                self._spark_session = None
                SparkManager._spark_session = None  # Clear class-level reference too
        
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        
        # Force cleanup of any stale session
        try:
            existing = SparkSession.getActiveSession()
            if existing:
                logger.info("Stopping existing active SparkSession")
                existing.stop()
        except:
            pass
        
        logger.info(f"Creating SparkSession with master: {self.spark_master}")
        
        # Build SparkSession with Delta Lake extensions
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(self.spark_master) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # S3/MinIO configuration
        builder = builder \
            .config("spark.hadoop.fs.s3a.endpoint", self.s3_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", self.s3_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.s3_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        # Delta Lake optimizations
        builder = builder \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .config("spark.databricks.delta.properties.defaults.logRetentionDuration", 
                    f"{self.delta_log_retention} days") \
            .config("spark.databricks.delta.properties.defaults.deletedFileRetentionDuration", 
                    f"{self.delta_log_retention} days")
        
        # Local mode optimizations
        if self.spark_mode == "local":
            builder = builder \
                .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g")) \
                .config("spark.sql.shuffle.partitions", 
                        os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")) \
                .config("spark.default.parallelism", 
                        os.getenv("SPARK_DEFAULT_PARALLELISM", "8"))
        else:
            # Cluster mode settings
            builder = builder \
                .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "4g")) \
                .config("spark.executor.cores", os.getenv("SPARK_EXECUTOR_CORES", "2")) \
                .config("spark.dynamicAllocation.enabled", "true") \
                .config("spark.dynamicAllocation.minExecutors", "1") \
                .config("spark.dynamicAllocation.maxExecutors", 
                        os.getenv("SPARK_MAX_EXECUTORS", "10"))
        
        # Use configure_spark_with_delta_pip for proper Delta Lake setup
        self._spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
        
        # Set log level
        self._spark_session.sparkContext.setLogLevel(
            os.getenv("SPARK_LOG_LEVEL", "WARN")
        )
        
        logger.info("SparkSession created successfully")
        return self._spark_session
    
    def stop_session(self):
        """Stop the current SparkSession."""
        if self._spark_session is not None:
            try:
                self._spark_session.stop()
                logger.info("SparkSession stopped")
            except Exception as e:
                logger.warning(f"Error stopping SparkSession: {e}")
            finally:
                self._spark_session = None
    
    @contextmanager
    def session_scope(self):
        """
        Context manager for SparkSession.
        
        Usage:
            with spark_manager.session_scope() as spark:
                df = spark.read.format("delta").load(path)
        """
        spark = self.get_or_create_session()
        try:
            yield spark
        except Exception as e:
            logger.error(f"Error in Spark session: {e}")
            raise
    
    def get_bronze_path(self, dataset_folder: str, part_name: Optional[str] = None) -> str:
        """
        Get the S3 path for a Bronze dataset.
        
        Args:
            dataset_folder: The dataset folder name (e.g., "15-melanoma_study")
            part_name: Optional part name within the dataset
            
        Returns:
            S3A path to the Bronze data
        """
        base_path = f"s3a://{self.bronze_bucket}/{dataset_folder}"
        if part_name:
            return f"{base_path}/{part_name}"
        return base_path
    
    def get_silver_path(self, config_name: str, config_id: int) -> str:
        """
        Get the S3 path for a Silver dataset.
        
        Args:
            config_name: Name of the transform config
            config_id: ID of the transform config
            
        Returns:
            S3A path for the Silver output
        """
        # Use format: {id}-{name} for consistency with Bronze
        folder_name = f"{config_id}-{config_name}"
        return f"s3a://{self.silver_bucket}/{folder_name}"
    
    def read_bronze_delta(self, path: str):
        """
        Read a Bronze Delta table.
        
        Args:
            path: S3A path to the Bronze Delta table
            
        Returns:
            DataFrame: Spark DataFrame with the Bronze data
        """
        spark = self.get_or_create_session()
        logger.info(f"Reading Bronze Delta from: {path}")
        return spark.read.format("delta").load(path)
    
    def write_silver_delta(
        self, 
        df, 
        path: str, 
        mode: str = "overwrite",
        partition_by: Optional[list] = None
    ):
        """
        Write a DataFrame to Silver Delta Lake.
        
        Args:
            df: Spark DataFrame to write
            path: S3A path for the output
            mode: Write mode ('overwrite', 'append', 'merge')
            partition_by: Optional list of columns to partition by
            
        Returns:
            int: Number of rows written
        """
        logger.info(f"Writing Silver Delta to: {path}")
        
        writer = df.write.format("delta").mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        # Enable optimizations
        writer = writer \
            .option("overwriteSchema", "true") \
            .option("mergeSchema", "true")
        
        writer.save(path)
        
        # Get row count
        row_count = df.count()
        logger.info(f"Written {row_count} rows to Silver Delta")
        
        return row_count
    
    def list_bronze_tables(self, dataset_folder: str) -> list:
        """
        List all Delta tables in a Bronze dataset folder.
        
        Args:
            dataset_folder: The dataset folder name
            
        Returns:
            List of table paths within the dataset
        """
        spark = self.get_or_create_session()
        
        try:
            from py4j.java_gateway import java_import
            
            # Use Hadoop FileSystem to list directories
            hadoop_conf = spark._jsc.hadoopConfiguration()
            
            # Set S3A credentials
            hadoop_conf.set("fs.s3a.endpoint", self.s3_endpoint)
            hadoop_conf.set("fs.s3a.access.key", self.s3_access_key)
            hadoop_conf.set("fs.s3a.secret.key", self.s3_secret_key)
            hadoop_conf.set("fs.s3a.path.style.access", "true")
            
            base_path = f"s3a://{self.bronze_bucket}/{dataset_folder}"
            
            # Use the Java FileSystem API
            java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
            java_import(spark._jvm, "org.apache.hadoop.fs.Path")
            
            uri = spark._jvm.java.net.URI(base_path)
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
            
            path = spark._jvm.org.apache.hadoop.fs.Path(base_path)
            
            if not fs.exists(path):
                return []
            
            status_list = fs.listStatus(path)
            
            tables = []
            for status in status_list:
                if status.isDirectory():
                    subdir = status.getPath().getName()
                    # Check if it's a Delta table (has _delta_log)
                    delta_log_path = spark._jvm.org.apache.hadoop.fs.Path(
                        f"{base_path}/{subdir}/_delta_log"
                    )
                    if fs.exists(delta_log_path):
                        tables.append(f"{base_path}/{subdir}")
            
            return tables
            
        except Exception as e:
            logger.warning(f"Error listing Bronze tables: {e}")
            # Fallback: try to read directly
            return [f"s3a://{self.bronze_bucket}/{dataset_folder}"]
    
    def get_table_schema(self, path: str) -> Dict[str, str]:
        """
        Get the schema of a Delta table.
        
        Args:
            path: Path to the Delta table
            
        Returns:
            Dict mapping column names to data types
        """
        spark = self.get_or_create_session()
        
        try:
            df = spark.read.format("delta").load(path)
            return {field.name: str(field.dataType) for field in df.schema.fields}
        except Exception as e:
            logger.error(f"Error getting schema for {path}: {e}")
            return {}
    
    def optimize_table(self, path: str, z_order_columns: Optional[list] = None):
        """
        Optimize a Delta table using Delta Lake's optimize command.
        
        Args:
            path: Path to the Delta table
            z_order_columns: Optional columns for Z-ordering
        """
        from delta.tables import DeltaTable
        
        spark = self.get_or_create_session()
        
        try:
            delta_table = DeltaTable.forPath(spark, path)
            
            if z_order_columns:
                delta_table.optimize().executeZOrderBy(*z_order_columns)
            else:
                delta_table.optimize().executeCompaction()
            
            logger.info(f"Optimized Delta table at {path}")
        except Exception as e:
            logger.warning(f"Could not optimize table {path}: {e}")
    
    def vacuum_table(self, path: str, retention_hours: int = 168):
        """
        Vacuum a Delta table to remove old files.
        
        Args:
            path: Path to the Delta table
            retention_hours: Hours to retain old files (default: 7 days)
        """
        from delta.tables import DeltaTable
        
        spark = self.get_or_create_session()
        
        try:
            delta_table = DeltaTable.forPath(spark, path)
            delta_table.vacuum(retention_hours)
            
            logger.info(f"Vacuumed Delta table at {path}")
        except Exception as e:
            logger.warning(f"Could not vacuum table {path}: {e}")



