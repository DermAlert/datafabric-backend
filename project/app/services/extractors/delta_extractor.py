from typing import Dict, Any, List, Optional
import asyncio
from datetime import datetime
import os

from ..connectors.delta_connector import get_spark_session
from ...utils.logger import logger


class DeltaExtractor:
    """
    Extractor for Delta Lake metadata.
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        self.connection_params = connection_params
        self.spark = None
        self._table_paths_cache = {}  # Cache to store table name -> path mappings
    
    def _get_spark_session(self):
        """Get or create Spark session."""
        if self.spark is None:
            self.spark = get_spark_session(self.connection_params)
        return self.spark
    
    async def extract_catalogs(self) -> List[Dict[str, Any]]:
        """
        Extract catalog information from Delta Lake.
        For Delta Lake, we typically have a single catalog.
        """
        try:
            spark = self._get_spark_session()
            
            # For Delta Lake, we usually work with a single catalog
            # But we can check if there are configured catalogs
            catalogs = []
            
            # Default catalog
            catalogs.append({
                "catalog_name": "spark_catalog",
                "catalog_type": "delta",
                "external_reference": "spark_catalog",
                "properties": {
                    "spark_version": spark.version,
                    "delta_extensions": spark.conf.get("spark.sql.extensions", ""),
                    "s3a_endpoint": self.connection_params.get('s3a_endpoint', 'http://minio:9000')
                }
            })
            
            return catalogs
            
        except Exception as e:
            logger.error(f"Error extracting Delta Lake catalogs: {str(e)}")
            return []
    
    async def extract_schemas(self) -> List[Dict[str, Any]]:
        """
        Extract schema information from Delta Lake.
        For Delta Lake, we primarily work with S3 buckets as schemas.
        """
        try:
            spark = self._get_spark_session()
            schemas = []
            
            # Try to get databases/schemas, but make it optional
            try:
                databases_df = spark.sql("SHOW DATABASES")
                # Try different possible column names for database
                databases = []
                
                # Check what columns are available
                columns = databases_df.columns
                logger.debug(f"SHOW DATABASES columns: {columns}")
                
                if 'databaseName' in columns:
                    databases = [row.databaseName for row in databases_df.collect()]
                elif 'database' in columns:
                    databases = [row.database for row in databases_df.collect()]
                elif 'namespace' in columns:
                    databases = [row.namespace for row in databases_df.collect()]
                else:
                    # If we can't determine the column name, get the first column
                    rows = databases_df.collect()
                    if rows and len(rows[0]) > 0:
                        databases = [row[0] for row in rows]
                
                for db_name in databases:
                    if db_name and db_name != "default":  # Skip empty or default database
                        schemas.append({
                            "schema_name": db_name,
                            "catalog_name": "spark_catalog",
                            "external_reference": db_name,
                            "properties": {
                                "database_type": "delta_database"
                            }
                        })
                        
            except Exception as db_error:
                logger.debug(f"Could not extract databases (this is normal for S3-only setups): {str(db_error)}")
            
            # Add the S3 bucket as a schema (this is the primary schema for most Delta Lake setups)
            bucket_name = self.connection_params.get('bucket_name')
            if bucket_name:
                schemas.append({
                    "schema_name": bucket_name,
                    "catalog_name": "spark_catalog", 
                    "external_reference": f"s3a://{bucket_name}",
                    "properties": {
                        "schema_type": "s3_bucket",
                        "bucket_name": bucket_name,
                        "primary_schema": True  # Mark this as the primary schema
                    }
                })
            
            # If no schemas found, create a default one
            if not schemas and bucket_name:
                schemas.append({
                    "schema_name": "default",
                    "catalog_name": "spark_catalog",
                    "external_reference": "default",
                    "properties": {
                        "schema_type": "default_schema",
                        "note": "Default schema for Delta Lake"
                    }
                })
            
            return schemas
            
        except Exception as e:
            logger.error(f"Error extracting Delta Lake schemas: {str(e)}")
            # Return at least the bucket schema as fallback
            bucket_name = self.connection_params.get('bucket_name')
            if bucket_name:
                return [{
                    "schema_name": bucket_name,
                    "catalog_name": "spark_catalog", 
                    "external_reference": f"s3a://{bucket_name}",
                    "properties": {
                        "schema_type": "s3_bucket",
                        "bucket_name": bucket_name,
                        "fallback_schema": True
                    }
                }]
            return []
    
    async def extract_tables(self, schema_name: str) -> List[Dict[str, Any]]:
        """
        Extract table information from a Delta Lake schema.
        """
        try:
            spark = self._get_spark_session()
            tables = []
            
            # Check if this is the S3 bucket schema
            bucket_name = self.connection_params.get('bucket_name')
            if schema_name == bucket_name:
                # Extract tables from S3 bucket structure
                tables.extend(await self._extract_s3_tables(bucket_name))
            else:
                # Extract tables from database
                try:
                    spark.sql(f"USE {schema_name}")
                    tables_df = spark.sql("SHOW TABLES")
                    
                    for row in tables_df.collect():
                        table_name = row.tableName
                        table_info = await self._get_table_info(schema_name, table_name)
                        tables.append(table_info)
                        
                except Exception as e:
                    logger.warning(f"Could not extract tables from database {schema_name}: {str(e)}")
            
            return tables
            
        except Exception as e:
            logger.error(f"Error extracting tables from schema {schema_name}: {str(e)}")
            return []
    
    async def extract_columns(self, schema_name: str, table_name: str) -> List[Dict[str, Any]]:
        """
        Extract column information for a specific Delta Lake table.
        """
        try:
            spark = self._get_spark_session()
            
            # First, check if we have the table path in cache
            table_path = None
            bucket_name = self.connection_params.get('bucket_name')
            
            if schema_name == bucket_name:
                # Check cache first
                if table_name in self._table_paths_cache:
                    table_path = self._table_paths_cache[table_name]
                    logger.debug(f"Using cached table path for {table_name}: {table_path}")
                else:
                    # If not in cache, try to find it by re-extracting tables
                    logger.debug(f"Table {table_name} not in cache, re-extracting tables to find path...")
                    tables = await self.extract_tables(schema_name)
                    target_table = None
                    for table in tables:
                        if table.get('table_name') == table_name:
                            target_table = table
                            break
                    
                    if target_table and 'external_reference' in target_table:
                        table_path = target_table['external_reference']
                        # Update cache
                        self._table_paths_cache[table_name] = table_path
                        logger.debug(f"Found table path for {table_name}: {table_path}")
                    else:
                        # Final fallback to simple path construction
                        if table_name == bucket_name:
                            table_path = f"s3a://{bucket_name}/"
                        else:
                            table_path = f"s3a://{bucket_name}/{table_name}"
                        logger.warning(f"Could not find correct path for {table_name}, using fallback: {table_path}")
            else:
                # Database table
                table_path = f"{schema_name}.{table_name}"
            
            logger.debug(f"Attempting to read Delta table from path: {table_path}")
            
            # Read the Delta table
            df = spark.read.format("delta").load(table_path)
            
            # Extract column information
            columns = await self._get_columns_info(df, table_name)
            
            return columns
            
        except Exception as e:
            logger.error(f"Error extracting columns for {schema_name}.{table_name}: {str(e)}")
            return []
    
    async def _extract_s3_tables(self, bucket_name: str) -> List[Dict[str, Any]]:
        """Extract Delta tables from S3 bucket structure using generic discovery."""
        tables = []
        
        try:
            spark = self._get_spark_session()
            
            # Generic approach: try to discover Delta tables in the bucket
            # We'll use Hadoop FileSystem to list directories and check for Delta tables
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark.sparkContext._jvm.java.net.URI.create(f"s3a://{bucket_name}/"),
                hadoop_conf
            )
            
            try:
                # List top-level directories in the bucket
                bucket_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket_name}/")
                
                if fs.exists(bucket_path):
                    # First check if bucket root is a Delta table
                    logger.debug(f"Checking bucket root as Delta table: s3a://{bucket_name}/")
                    await self._check_delta_table(spark, f"s3a://{bucket_name}/", tables)
                    
                    # Then recursively search for Delta tables in subdirectories
                    await self._search_delta_tables_recursive(fs, spark, bucket_path, tables, bucket_name, max_depth=3)
                
            except Exception as e:
                logger.debug(f"Error listing bucket contents: {str(e)}")
                
                # Fallback: try bucket root only
                await self._check_delta_table(spark, f"s3a://{bucket_name}/", tables)
                    
        except Exception as e:
            logger.error(f"Error extracting S3 tables from bucket {bucket_name}: {str(e)}")
        
        return tables
    
    async def _check_delta_table(self, spark, path: str, tables: List[Dict[str, Any]]):
        """Check if a path contains a valid Delta table."""
        try:
            # First, check if the directory has a _delta_log subdirectory
            # This is a more efficient way to check if it's a Delta table
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark.sparkContext._jvm.java.net.URI.create(path),
                hadoop_conf
            )
            
            delta_log_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(f"{path.rstrip('/')}/_delta_log")
            if not fs.exists(delta_log_path):
                logger.debug(f"No _delta_log found at {path}, not a Delta table")
                return
            
            # Try to read as Delta table
            logger.debug(f"Found _delta_log, attempting to read Delta table: {path}")
            df = spark.read.format("delta").load(path)
            
            # Extract table name from path
            path_parts = path.rstrip('/').split('/')
            table_name = path_parts[-1] if path_parts[-1] else path_parts[-2]
            
            # For bucket root, use bucket name with suffix
            bucket_name_param = self.connection_params.get('bucket_name', '')
            if path.rstrip('/').endswith(f"s3a://{bucket_name_param}") or table_name == bucket_name_param:
                table_name = f"{bucket_name_param}_root"
            
            # Avoid duplicate table names (if bucket root and directory have same name)
            existing_table_names = [table.get("table_name") for table in tables]
            if table_name in existing_table_names:
                logger.debug(f"Table {table_name} already found, skipping duplicate")
                return
            
            # Get table statistics
            logger.debug(f"Getting row count for Delta table: {table_name}")
            row_count = df.count()
            columns = df.columns
            
            table_info = {
                "table_name": table_name,
                "external_reference": path,
                "table_type": "delta_table",
                "estimated_row_count": row_count,
                "properties": {
                    "delta_path": path,
                    "column_count": len(columns),
                    "storage_type": "s3a",
                    "has_delta_log": True
                },
                "columns": await self._get_columns_info(df, table_name)
            }
            
            tables.append(table_info)
            
            # Cache the table path for later use in extract_columns
            self._table_paths_cache[table_name] = path
            
            logger.info(f"Found Delta table: {table_name} at {path} with {row_count} rows")
            
        except Exception as e:
            logger.debug(f"Path {path} is not a valid Delta table: {str(e)}")
    
    async def _get_table_info(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """Get detailed information about a Delta table."""
        try:
            spark = self._get_spark_session()
            
            # Get table details
            table_df = spark.table(f"{schema_name}.{table_name}")
            row_count = table_df.count()
            columns = table_df.columns
            
            table_info = {
                "table_name": table_name,
                "external_reference": f"{schema_name}.{table_name}",
                "table_type": "delta_table",
                "estimated_row_count": row_count,
                "properties": {
                    "schema_name": schema_name,
                    "column_count": len(columns)
                },
                "columns": await self._get_columns_info(table_df, table_name)
            }
            
            return table_info
            
        except Exception as e:
            logger.error(f"Error getting table info for {schema_name}.{table_name}: {str(e)}")
            return {
                "table_name": table_name,
                "external_reference": f"{schema_name}.{table_name}",
                "table_type": "delta_table",
                "estimated_row_count": 0,
                "properties": {},
                "columns": []
            }
    
    async def _get_columns_info(self, df, table_name: str) -> List[Dict[str, Any]]:
        """Extract column information from a DataFrame."""
        columns = []
        
        try:
            for idx, (col_name, col_type) in enumerate(df.dtypes):
                column_info = {
                    "column_name": col_name,
                    "external_reference": f"{table_name}.{col_name}",
                    "data_type": col_type,
                    "is_nullable": True,  # Delta Lake allows nulls by default
                    "column_position": idx + 1,
                    "properties": {
                        "spark_data_type": col_type
                    }
                }
                
                # Try to get sample values
                try:
                    sample_values = df.select(col_name).distinct().limit(5).collect()
                    column_info["sample_values"] = [row[0] for row in sample_values if row[0] is not None]
                except Exception as e:
                    logger.debug(f"Could not get sample values for {col_name}: {str(e)}")
                    column_info["sample_values"] = []
                
                columns.append(column_info)
                
        except Exception as e:
            logger.error(f"Error extracting column info: {str(e)}")
        
        return columns
    
    async def _search_delta_tables_recursive(self, fs, spark, path, tables: List[Dict[str, Any]], bucket_name: str, max_depth: int = 3, current_depth: int = 0):
        """Recursively search for Delta tables in subdirectories."""
        if current_depth >= max_depth:
            logger.debug(f"Reached maximum depth {max_depth}, stopping search")
            return
        
        try:
            if fs.exists(path):
                file_statuses = fs.listStatus(path)
                logger.debug(f"Searching at depth {current_depth}, found {len(file_statuses)} items in {path}")
                
                # Separate directories by whether they look like Delta tables
                delta_dirs = []
                regular_dirs = []
                
                for file_status in file_statuses:
                    if file_status.isDirectory():
                        item_path = str(file_status.getPath())
                        item_name = item_path.split('/')[-1]
                        
                        # Prioritize directories that end with .delta or contain _delta_log
                        if item_name.endswith('.delta') or item_name == '_delta_log':
                            delta_dirs.append(file_status)
                        else:
                            regular_dirs.append(file_status)
                
                # First check potential Delta directories
                for file_status in delta_dirs:
                    item_path = str(file_status.getPath())
                    item_name = item_path.split('/')[-1]
                    logger.debug(f"Checking potential Delta directory: {item_name} at {item_path}")
                    
                    if item_name != '_delta_log':  # Skip the log directory itself
                        await self._check_delta_table(spark, item_path, tables)
                
                # Then check regular directories and recurse
                for file_status in regular_dirs:
                    item_path = str(file_status.getPath())
                    item_name = item_path.split('/')[-1]
                    logger.debug(f"Checking regular directory: {item_name} at {item_path}")
                    
                    # Check if this directory is a Delta table
                    await self._check_delta_table(spark, item_path, tables)
                    
                    # Continue recursive search in subdirectories
                    await self._search_delta_tables_recursive(
                        fs, spark, file_status.getPath(), tables, bucket_name, 
                        max_depth, current_depth + 1
                    )
                        
        except Exception as e:
            logger.debug(f"Error searching at path {path}, depth {current_depth}: {str(e)}")
    
    def __del__(self):
        """Cleanup Spark session when object is destroyed."""
        if self.spark:
            try:
                self.spark.stop()
            except Exception as e:
                logger.warning(f"Error stopping Spark session: {str(e)}")
