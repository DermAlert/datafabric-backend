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
import threading
from typing import Optional, Dict, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Global reference counter so that Bronze and Silver jobs can share the single
# SparkSession without one stopping the JVM while the other is still running.
_spark_ref_count: int = 0
_spark_ref_lock: threading.Lock = threading.Lock()

# PYSPARK_SUBMIT_ARGS must be set before pyspark is imported so the JVM starts
# with the correct heap size. Setting spark.driver.memory via SparkSession.builder
# has no effect on the already-running JVM heap — only this env var does.
_driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "2g")
_current_submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
if "--driver-memory" not in _current_submit_args:
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-memory {_driver_memory} {_current_submit_args}".strip()
        if _current_submit_args else
        f"--driver-memory {_driver_memory} pyspark-shell"
    )


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
    
    def _force_cleanup_spark_state(self):
        """
        Force cleanup of all PySpark/Py4j internal state.
        
        This is needed when the JVM process dies (becomes zombie) but PySpark
        still holds stale references. Without this cleanup, getOrCreate() will
        try to connect to the dead Py4j gateway and fail with ConnectionRefused.
        """
        from pyspark.sql import SparkSession
        from pyspark import SparkContext
        
        # Clear PySpark's internal active session references
        try:
            SparkSession._instantiatedSession = None
        except (AttributeError, Exception):
            pass
        
        try:
            SparkSession._activeSession = None
        except (AttributeError, Exception):
            pass
        
        # Clear SparkContext's active context reference
        try:
            if SparkContext._active_spark_context is not None:
                try:
                    SparkContext._active_spark_context.stop()
                except Exception:
                    pass
                SparkContext._active_spark_context = None
        except (AttributeError, Exception):
            pass
        
        # Clear the gateway connection so Py4j creates a fresh one
        try:
            if SparkContext._gateway is not None:
                try:
                    SparkContext._gateway.shutdown()
                except Exception:
                    pass
                SparkContext._gateway = None
        except (AttributeError, Exception):
            pass
        
        # Also clear the jvm reference
        try:
            SparkContext._jvm = None
        except (AttributeError, Exception):
            pass
        
        logger.info("Forced cleanup of all PySpark/Py4j internal state")
    
    def get_or_create_session(self):
        """
        Get or create a SparkSession configured for Delta Lake.
        
        Returns:
            SparkSession: A configured SparkSession instance
        """
        if self._spark_session is not None:
            try:
                sc = self._spark_session.sparkContext
                _ = sc.getConf().get("spark.app.name")
                if not sc._jsc.sc().isStopped():
                    # Verify the cached session uses the correct master.
                    # If it doesn't (e.g., a legacy local session left over from
                    # delta.py module-level init), stop it and recreate.
                    current_master = sc.getConf().get("spark.master", "")
                    if current_master != self.spark_master:
                        logger.warning(
                            f"Cached SparkSession uses master={current_master!r}, "
                            f"expected {self.spark_master!r}. Recreating session."
                        )
                        try:
                            self._spark_session.stop()
                        except Exception:
                            pass
                        self._spark_session = None
                        SparkManager._spark_session = None
                        self._force_cleanup_spark_state()
                    else:
                        return self._spark_session
            except Exception as e:
                logger.warning(f"Existing SparkSession is dead ({type(e).__name__}), creating new one")
                try:
                    self._spark_session.stop()
                except Exception:
                    pass
                self._spark_session = None
                SparkManager._spark_session = None
                self._force_cleanup_spark_state()
        
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        
        # Force cleanup of any stale session
        try:
            existing = SparkSession.getActiveSession()
            if existing:
                logger.info("Stopping existing active SparkSession")
                existing.stop()
        except Exception:
            # If even getActiveSession fails, the Py4j gateway is dead
            # Force cleanup everything
            logger.warning("Failed to get active session, forcing full cleanup of PySpark state")
            self._force_cleanup_spark_state()
        
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
        # optimizeWrite: Delta rewrites shuffle output into ~128 MB files automatically,
        #   eliminating the small-files problem without manual coalesce calls.
        # autoCompact: after each commit, Delta checks if there are too many small files
        #   in the affected partition and merges them (bin-packing).
        # retentionDurationCheck: MUST be true in production — prevents VACUUM from
        #   deleting files that could still be read by concurrent transactions.
        #   Disabling it (false) would allow accidental data loss on short VACUUM runs.
        builder = builder \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "true") \
            .config("spark.databricks.delta.properties.defaults.logRetentionDuration",
                    f"{self.delta_log_retention} days") \
            .config("spark.databricks.delta.properties.defaults.deletedFileRetentionDuration",
                    f"{self.delta_log_retention} days") \
            .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
            .config("spark.databricks.delta.autoCompact.enabled", "true") \
            .config("spark.databricks.delta.autoCompact.minNumFiles", "5") \
            .config("spark.databricks.delta.stats.collect", "true") \
            .config("spark.databricks.delta.checkpoint.partSize", "5000000")

        # FAIR scheduler — allows multiple concurrent Spark jobs to share resources
        # instead of queuing (FIFO default). Critical for concurrent Silver executions
        # where multiple API requests submit jobs to the same SparkContext.
        #
        # SPARK_TMP_DIR overrides /tmp for environments with read-only root filesystems
        # (hardened Docker containers, certain Kubernetes pod security policies).
        # Falls back to Python's tempfile.gettempdir() which is always writable.
        import tempfile as _tempfile
        _tmp_base = os.getenv("SPARK_TMP_DIR", _tempfile.gettempdir())
        _fairscheduler_path = os.path.join(_tmp_base, "fairscheduler.xml")

        try:
            with open(_fairscheduler_path, "w") as f:
                f.write("""<?xml version="1.0"?>
<allocations>
  <pool name="default">
    <schedulingMode>FAIR</schedulingMode>
    <weight>1</weight>
    <minShare>2</minShare>
  </pool>
</allocations>""")
            builder = builder \
                .config("spark.scheduler.mode", "FAIR") \
                .config("spark.scheduler.allocation.file", _fairscheduler_path)
        except Exception as e:
            logger.warning(
                f"Failed to write fairscheduler.xml to {_fairscheduler_path}: {e}. "
                "Spark will use default FIFO scheduling."
            )

        # Adaptive Query Execution — auto-tunes shuffle partitions, skew joins, etc.
        # advisoryPartitionSizeInBytes = 128 MB: AQE coalesces post-shuffle partitions
        #   to target this size, matching Delta's targetFileSize 1-to-1.
        # parallelismFirst=false: AQE coalesces purely by data size, not by available
        #   parallelism. This is critical so that small datasets (e.g. 500 rows) get
        #   coalesced all the way down to 1 partition instead of keeping 200 empty ones.
        #   Without this, SPARK_SHUFFLE_PARTITIONS=200 creates 200 near-empty shuffle
        #   files for small data, dominating execution time.
        builder = builder \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1b") \
            .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "134217728") \
            .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .config("spark.cleaner.referenceTracking.blocking", "false")

        # Network timeout configurations to prevent Py4J ConnectionRefusedError
        builder = builder \
            .config("spark.network.timeout", "600s") \
            .config("spark.executor.heartbeatInterval", "120s") \
            .config("spark.core.connection.ack.wait.timeout", "600s") \
            .config("spark.rpc.askTimeout", "600s")

        # Read partition sizing — 128 MB per read partition (ideal Parquet/Delta chunk)
        builder = builder \
            .config("spark.sql.files.maxPartitionBytes", "134217728") \
            .config("spark.sql.files.openCostInBytes", "4194304")

        # S3A / MinIO throughput tuning
        builder = builder \
            .config("spark.hadoop.fs.s3a.connection.maximum", "200") \
            .config("spark.hadoop.fs.s3a.threads.max", "64") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "200000") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array") \
            .config("spark.hadoop.fs.s3a.multipart.size", "134217728") \
            .config("spark.hadoop.fs.s3a.multipart.threshold", "134217728") \
            .config("spark.hadoop.fs.s3a.block.size", "134217728") \
            .config("spark.hadoop.fs.s3a.readahead.range", "131072")

        # Memory and Resource tuning
        # spark.memory.fraction=0.75: 75 % of JVM heap for Spark execution+storage.
        # spark.memory.storageFraction=0.3: only 30 % of that goes to the storage
        #   cache, giving 70 % to execution (shuffle, sort, aggregations) — reduces
        #   the chance of spill and makes room for the off-heap buffer.
        # Off-heap memory (512 MB) is used for internal Tungsten buffers, reducing GC
        #   pressure during heavy sorts/aggregations.
        # Spill-to-disk tuning:
        #   spark.shuffle.file.buffer: larger write-side buffer (1 MB vs default 32 KB)
        #     reduces the number of disk I/O ops when spilling.
        #   spark.reducer.maxSizeInFlight: more in-flight shuffle blocks (96 MB vs 48 MB),
        #     fewer network round trips.
        #   spark.shuffle.sort.bypassMergeThreshold: use the bypass path for low-cardinality
        #     shuffles (avoids an extra sort pass).
        #   spark.local.dir: dedicated temp directory for spill files.
        if self.spark_mode == "local":
            builder = builder \
                .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g")) \
                .config("spark.driver.maxResultSize", "2g") \
                .config("spark.sql.shuffle.partitions",
                        os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")) \
                .config("spark.default.parallelism",
                        os.getenv("SPARK_DEFAULT_PARALLELISM", "16")) \
                .config("spark.memory.fraction", "0.75") \
                .config("spark.memory.storageFraction", "0.3") \
                .config("spark.memory.offHeap.enabled", "true") \
                .config("spark.memory.offHeap.size", "512m") \
                .config("spark.shuffle.file.buffer", "1m") \
                .config("spark.reducer.maxSizeInFlight", "96m") \
                .config("spark.shuffle.sort.bypassMergeThreshold", "200") \
                .config("spark.local.dir", os.path.join(_tmp_base, "spark-spill")) \
                .config("spark.driver.bindAddress", "127.0.0.1") \
                .config("spark.driver.host", "127.0.0.1") \
                .config("spark.executor.extraJavaOptions",
                        "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35") \
                .config("spark.driver.extraJavaOptions",
                        "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")
        else:
            # Standalone / cluster mode settings.
            #
            # spark.driver.host MUST be the hostname reachable by Spark workers.
            # In Docker, this is the service name ("dermalert-backend").
            # Without this, executors try to connect to 127.0.0.1 and time out,
            # causing Spark to silently fall back to running everything locally.
            #
            # SPARK_DRIVER_HOST env var overrides — useful when running outside
            # Docker (e.g. on K8s with a different hostname convention).
            _driver_host = os.getenv("SPARK_DRIVER_HOST", "dermalert-backend")
            builder = builder \
                .config("spark.driver.host", _driver_host) \
                .config("spark.driver.bindAddress", "0.0.0.0") \
                .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g")) \
                .config("spark.driver.maxResultSize", "2g") \
                .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "4g")) \
                .config("spark.executor.cores", os.getenv("SPARK_EXECUTOR_CORES", "2")) \
                .config("spark.sql.shuffle.partitions",
                        os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")) \
                .config("spark.dynamicAllocation.enabled", "true") \
                .config("spark.dynamicAllocation.minExecutors", "1") \
                .config("spark.dynamicAllocation.maxExecutors",
                        os.getenv("SPARK_MAX_EXECUTORS", "10")) \
                .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
                .config("spark.executor.extraJavaOptions",
                        "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35") \
                .config("spark.driver.extraJavaOptions",
                        "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")
        
        # Delta Lake + hadoop-aws + Trino JDBC packages.
        # configure_spark_with_delta_pip is designed for local mode only.
        # For Spark Standalone / cluster, we set spark.jars.packages directly
        # so the driver downloads JARs and distributes them to executors.
        #
        # trino-jdbc: allows Spark to read from Trino directly via JDBC, bypassing
        # Python memory entirely. Data flows JVM→JVM: Trino JDBC driver → Spark DataFrame
        # → Delta write. This eliminates the Python fetchall() memory bottleneck.
        # Version must match the running Trino server (io.trino:trino-jdbc:{server_version}).
        _delta_version      = "4.0.0"
        _hadoop_version     = "3.4.0"
        _scala_version      = "2.13"
        _trino_jdbc_version = os.getenv("TRINO_JDBC_VERSION", "479")
        _packages = (
            f"io.delta:delta-spark_{_scala_version}:{_delta_version},"
            f"org.apache.hadoop:hadoop-aws:{_hadoop_version},"
            f"io.trino:trino-jdbc:{_trino_jdbc_version}"
        )

        if self.spark_mode == "local":
            extra_packages = [
                f"org.apache.hadoop:hadoop-aws:{_hadoop_version}",
                f"io.trino:trino-jdbc:{_trino_jdbc_version}",
            ]
            # configure_spark_with_delta_pip works only in local mode
            # (it calls builder.getOrCreate internally with Delta Lake configuration)
        else:
            # Cluster / standalone mode: inject packages directly into the builder
            # so they are distributed to executors automatically.
            builder = builder \
                .config("spark.jars.packages", _packages) \
                .config("spark.jars.ivy", "/tmp/.ivy2") \
                .config("spark.driver.port", os.getenv("SPARK_DRIVER_PORT", "7078")) \
                .config("spark.blockManager.port", os.getenv("SPARK_BLOCKMANAGER_PORT", "7079"))
            extra_packages = []

        # Ensure the spill directory exists before Spark starts writing to it.
        import pathlib
        _spill_dir = os.path.join(_tmp_base, "spark-spill")
        try:
            pathlib.Path(_spill_dir).mkdir(parents=True, exist_ok=True)
        except Exception as _mkdir_err:
            logger.warning(
                f"Could not create spill directory {_spill_dir}: {_mkdir_err}. "
                "Spark will use its default temp location."
            )

        try:
            if self.spark_mode == "local":
                self._spark_session = configure_spark_with_delta_pip(
                    builder, extra_packages=extra_packages
                ).getOrCreate()
            else:
                self._spark_session = builder.getOrCreate()

            # Set log level
            self._spark_session.sparkContext.setLogLevel(
                os.getenv("SPARK_LOG_LEVEL", "WARN")
            )

            logger.info("SparkSession created successfully")
            return self._spark_session
        except Exception as e:
            logger.error(f"Failed to create SparkSession: {e}")
            self._force_cleanup_spark_state()
            raise
    
    # ------------------------------------------------------------------
    # Reference counting — prevent JVM from being torn down while a
    # concurrent Bronze or Silver job is still using the session.
    # ------------------------------------------------------------------

    def acquire(self) -> None:
        """Increment the active-job reference count."""
        global _spark_ref_count
        with _spark_ref_lock:
            _spark_ref_count += 1
            logger.debug(f"SparkManager.acquire() → ref_count={_spark_ref_count}")

    def release(self) -> None:
        """
        Decrement the active-job reference count.

        The session is NOT stopped automatically on release — callers that
        want to stop the session after all jobs finish should call
        stop_session() explicitly.  Here we just log the transition so it
        is easy to detect leaks in tests/logs.
        """
        global _spark_ref_count
        with _spark_ref_lock:
            _spark_ref_count = max(0, _spark_ref_count - 1)
            logger.debug(f"SparkManager.release() → ref_count={_spark_ref_count}")

    @property
    def active_jobs(self) -> int:
        """Return the number of jobs currently holding a reference."""
        return _spark_ref_count

    def stop_session(self) -> bool:
        """
        Stop the current SparkSession and clean up JVM resources.

        Safe to call at any time: if active jobs are still holding references
        (ref_count > 0), the stop is refused and False is returned so callers
        can decide whether to retry or log a warning.

        Returns:
            True  — session was stopped successfully.
            False — refused because active jobs are still running.
        """
        with _spark_ref_lock:
            if _spark_ref_count > 0:
                logger.warning(
                    f"stop_session() refused: {_spark_ref_count} active job(s) "
                    "still hold a reference. Call release() for each job first."
                )
                return False

        if self._spark_session is not None:
            try:
                self._spark_session.stop()
                logger.info("SparkSession stopped")
            except Exception as e:
                logger.warning(f"Error stopping SparkSession: {e}")
            finally:
                self._spark_session = None
                SparkManager._spark_session = None
                self._force_cleanup_spark_state()
        return True

    @contextmanager
    def session_scope(self):
        """
        Context manager for SparkSession.

        Automatically increments the reference counter on entry and decrements
        it on exit (even on exception), keeping stop_session() safe to call
        from any concurrent thread without risk of killing an in-flight job.

        Usage:
            with spark_manager.session_scope() as spark:
                df = spark.read.format("delta").load(path)
        """
        spark = self.get_or_create_session()
        self.acquire()
        try:
            yield spark
        except Exception as e:
            logger.error(f"Error in Spark session: {e}")
            raise
        finally:
            self.release()
    
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
        partition_by: Optional[list] = None,
        precomputed_row_count: Optional[int] = None,
    ):
        """
        Write a DataFrame to Silver Delta Lake.

        Args:
            df: Spark DataFrame to write
            path: S3A path for the output
            mode: Write mode ('overwrite', 'append', 'merge')
            partition_by: Optional list of columns to partition by
            precomputed_row_count: If provided, skip the post-write count() scan.

        Returns:
            int: Number of rows written
        """
        logger.info(f"Writing Silver Delta to: {path}")

        writer = df.write.format("delta").mode(mode)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer = writer \
            .option("overwriteSchema", "true") \
            .option("mergeSchema", "true")

        writer.save(path)

        if precomputed_row_count is not None:
            row_count = precomputed_row_count
        else:
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
            # Uses the Hadoop FileSystem API via Py4J (Java bridge) to list S3A
            # paths directly — avoids a Python boto3 round trip and reuses the
            # already-configured S3A credentials from the SparkSession.
            # Trade-off: tightly coupled to Hadoop internal APIs; if Spark upgrades
            # change the package layout, this path can break. The except block below
            # provides a safe fallback so the system degrades gracefully.
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



