"""
Data Source Extraction Service
Serviço responsável por extrair dados reais das fontes de dados (PostgreSQL, MongoDB, etc.)
"""

import asyncio
import pandas as pd
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .extractors.postgres_extractor import PostgresExtractor
from ..database.metadata.metadata import ExternalTables, ExternalSchema
from ..database.core.core import DataConnection, ConnectionType

logger = logging.getLogger(__name__)

class DataSourceExtractionService:
    """Service for extracting real data from data sources"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def extract_data_from_tables(self, table_ids: List[int], limit_per_table: Optional[int] = None) -> Dict[str, Any]:
        """
        Extract real data from multiple tables across different data sources.
        
        Args:
            table_ids: List of table IDs to extract data from
            limit_per_table: Maximum rows per table (None for all data)
        
        Returns:
            Dict containing extracted data from all tables
        """
        try:
            extraction_results = {
                "tables": {},
                "total_tables": len(table_ids),
                "total_rows": 0,
                "extraction_timestamp": datetime.now().isoformat(),
                "errors": []
            }
            
            for table_id in table_ids:
                try:
                    # Get table metadata using direct query with schema join
                    table_query = select(ExternalTables, ExternalSchema.schema_name).join(
                        ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
                    ).where(ExternalTables.id == table_id)
                    table_result = await self.db.execute(table_query)
                    table_data = table_result.first()
                    
                    if not table_data:
                        error_msg = f"Table with ID {table_id} not found"
                        extraction_results["errors"].append(error_msg)
                        logger.error(error_msg)
                        continue
                    
                    table, schema_name = table_data
                    
                    # Get connection info with connection type
                    connection_query = select(DataConnection, ConnectionType).join(
                        ConnectionType, DataConnection.connection_type_id == ConnectionType.id
                    ).where(DataConnection.id == table.connection_id)
                    connection_result = await self.db.execute(connection_query)
                    connection_data = connection_result.first()
                    
                    if not connection_data:
                        error_msg = f"Connection for table {table_id} not found"
                        extraction_results["errors"].append(error_msg)
                        logger.error(error_msg)
                        continue
                    
                    connection, connection_type = connection_data
                    
                    # Extract data based on connection type (pass schema_name separately)
                    table_data = await self._extract_data_by_connection_type(
                        connection, connection_type, table, schema_name, limit_per_table
                    )
                    
                    if table_data:
                        extraction_results["tables"][table_id] = {
                            "table_name": table.table_name,
                            "schema_name": schema_name,
                            "connection_type": connection_type.name,
                            "connection_name": connection.name,
                            "data": table_data["data"],
                            "columns": table_data["columns"],
                            "total_rows": table_data["total_rows"],
                            "fetched_rows": table_data["fetched_rows"]
                        }
                        extraction_results["total_rows"] += table_data["fetched_rows"]
                        
                        logger.info(f"Extracted {table_data['fetched_rows']} rows from table {table.table_name}")
                    
                except Exception as e:
                    error_msg = f"Error extracting data from table {table_id}: {str(e)}"
                    extraction_results["errors"].append(error_msg)
                    logger.error(error_msg)
            
            logger.info(f"Total extraction completed: {extraction_results['total_rows']} rows from {len(extraction_results['tables'])} tables")
            return extraction_results
            
        except Exception as e:
            logger.error(f"Failed to extract data from tables: {e}")
            raise
    
    async def _extract_data_by_connection_type(self, connection, connection_type, table, schema_name: str, limit: Optional[int]) -> Optional[Dict[str, Any]]:
        """Extract data based on the connection type"""
        
        connection_type_name = connection_type.name.lower()
        
        if connection_type_name == 'postgresql':
            return await self._extract_postgresql_data(connection, table, schema_name, limit)
        elif connection_type_name == 'mysql':
            return await self._extract_mysql_data(connection, table, schema_name, limit)
        elif connection_type_name == 'mongodb':
            return await self._extract_mongodb_data(connection, table, schema_name, limit)
        elif connection_type_name == 'oracle':
            return await self._extract_oracle_data(connection, table, schema_name, limit)
        elif connection_type_name == 'deltalake':
            return await self._extract_deltalake_data(connection, table, limit)
        else:
            logger.warning(f"Unsupported connection type: {connection_type_name}")
            return None
    
    async def _extract_postgresql_data(self, connection, table, schema_name: str, limit: Optional[int]) -> Dict[str, Any]:
        """Extract data from PostgreSQL"""
        try:
            # Parse connection params from JSON
            params = connection.connection_params
            connection_params = {
                "host": params.get("host"),
                "port": params.get("port", 5432),
                "database": params.get("database"),
                "username": params.get("username"),
                "password": params.get("password"),
                "ssl": params.get("ssl", False)
            }
            
            # Create extractor
            extractor = PostgresExtractor(connection_params)
            
            # Extract data using the provided schema_name
            data_result = await extractor.extract_table_data(
                schema_name, 
                table.table_name, 
                limit
            )
            
            # Close connection
            await extractor.close()
            
            return data_result
            
        except Exception as e:
            logger.error(f"Error extracting PostgreSQL data: {e}")
            raise
    
    async def _extract_mysql_data(self, connection, table, schema_name: str, limit: Optional[int]) -> Dict[str, Any]:
        """Extract data from MySQL (placeholder for future implementation)"""
        logger.warning("MySQL extraction not yet implemented")
        return {
            "schema_name": schema_name,
            "table_name": table.table_name,
            "columns": [],
            "data": [],
            "total_rows": 0,
            "fetched_rows": 0,
            "error": "MySQL extraction not implemented"
        }
    
    async def _extract_mongodb_data(self, connection, table, schema_name: str, limit: Optional[int]) -> Dict[str, Any]:
        """Extract data from MongoDB (placeholder for future implementation)"""
        logger.warning("MongoDB extraction not yet implemented")
        return {
            "schema_name": schema_name,
            "table_name": table.table_name,
            "columns": [],
            "data": [],
            "total_rows": 0,
            "fetched_rows": 0,
            "error": "MongoDB extraction not implemented"
        }
    
    async def _extract_oracle_data(self, connection, table, schema_name: str, limit: Optional[int]) -> Dict[str, Any]:
        """Extract data from Oracle (placeholder for future implementation)"""
        logger.warning("Oracle extraction not yet implemented")
        return {
            "schema_name": schema_name,
            "table_name": table.table_name,
            "columns": [],
            "data": [],
            "total_rows": 0,
            "fetched_rows": 0,
            "error": "Oracle extraction not implemented"
        }
    
    async def extract_and_unify_data(
        self, 
        table_ids: List[int], 
        column_mappings: Dict[str, str] = None,
        value_mappings: Dict[str, Dict[str, str]] = None,
        limit_per_table: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Extract data from multiple sources and unify into a single DataFrame.
        
        Args:
            table_ids: List of table IDs
            column_mappings: Dictionary of column name mappings
            value_mappings: Dictionary of value mappings
            limit_per_table: Limit rows per table
        
        Returns:
            Unified pandas DataFrame
        """
        try:
            # Extract data from all tables
            extraction_results = await self.extract_data_from_tables(table_ids, limit_per_table)
            
            unified_data = []
            all_columns = set()
            
            # Process each table's data
            for table_id, table_info in extraction_results["tables"].items():
                table_data = table_info["data"]
                table_name = table_info["table_name"]
                
                # Apply column mappings
                if column_mappings:
                    for row in table_data:
                        mapped_row = {}
                        for col, value in row.items():
                            # Try table-specific mapping first (table_name.column_name)
                            table_col_key = f"{table_name}.{col}"
                            if table_col_key in column_mappings:
                                mapped_col = column_mappings[table_col_key]
                            else:
                                # Fallback to column name only
                                mapped_col = column_mappings.get(col, col)
                            mapped_row[mapped_col] = value
                        
                        # Apply value mappings
                        if value_mappings:
                            for col, value in mapped_row.items():
                                if col in value_mappings and str(value) in value_mappings[col]:
                                    mapped_row[col] = value_mappings[col][str(value)]
                        
                        # Add source table info
                        mapped_row['_source_table'] = table_name
                        mapped_row['_source_table_id'] = table_id
                        
                        unified_data.append(mapped_row)
                        all_columns.update(mapped_row.keys())
                else:
                    # No mappings, just add source info
                    for row in table_data:
                        row['_source_table'] = table_name
                        row['_source_table_id'] = table_id
                        unified_data.append(row)
                        all_columns.update(row.keys())
            
            # Create unified DataFrame
            if unified_data:
                unified_df = pd.DataFrame(unified_data)
                logger.info(f"Created unified DataFrame with {len(unified_df)} rows and {len(unified_df.columns)} columns")
                return unified_df
            else:
                logger.warning("No data extracted from any table")
                return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error in extract_and_unify_data: {e}")
            raise
    
    async def get_data_preview(self, table_ids: List[int], preview_rows: int = 100) -> Dict[str, Any]:
        """Get a preview of data from multiple tables"""
        try:
            extraction_results = await self.extract_data_from_tables(table_ids, preview_rows)
            
            preview = {
                "tables": {},
                "total_tables": extraction_results["total_tables"],
                "preview_rows_per_table": preview_rows,
                "extraction_timestamp": extraction_results["extraction_timestamp"]
            }
            
            for table_id, table_info in extraction_results["tables"].items():
                preview["tables"][table_id] = {
                    "table_name": table_info["table_name"],
                    "connection_type": table_info["connection_type"],
                    "columns": [col["column_name"] for col in table_info["columns"]],
                    "data_sample": table_info["data"][:10],  # First 10 rows
                    "total_rows": table_info["total_rows"],
                    "columns_count": len(table_info["columns"])
                }
            
            return preview
            
        except Exception as e:
            logger.error(f"Error getting data preview: {e}")
            raise

    async def _extract_deltalake_data(self, connection, table, limit: Optional[int]) -> Dict[str, Any]:
        """Extract data from Delta Lake using Spark"""
        try:
            from pyspark.sql import SparkSession
            
            # Get connection parameters
            params = connection.connection_params
            
            # Initialize Spark session with Delta Lake support
            spark = SparkSession.builder \
                .appName(params.get("app_name", "DeltaLakeExtraction")) \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.hadoop.fs.s3a.endpoint", params.get("s3a_endpoint", "localhost:9000")) \
                .config("spark.hadoop.fs.s3a.access.key", params.get("s3a_access_key", "minio")) \
                .config("spark.hadoop.fs.s3a.secret.key", params.get("s3a_secret_key", "minio123")) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .getOrCreate()
            
            # Build the Delta table path using bucket_name from params
            bucket_name = params.get("bucket_name", "isic-delta")
            delta_path = params.get("delta_path", f"s3a://{bucket_name}/{table.table_name}")
            
            logger.info(f"Reading Delta table from: {delta_path}")
            
            # Read Delta table
            df = spark.read.format("delta").load(delta_path)
            
            # Apply limit if specified
            if limit:
                df = df.limit(limit)
            
            # Get column information
            columns = []
            for field in df.schema.fields:
                columns.append({
                    "column_name": field.name,
                    "data_type": str(field.dataType),
                    "nullable": field.nullable
                })
            
            # Convert to list of dictionaries
            data = [row.asDict() for row in df.collect()]
            
            # Get total row count (without limit)
            total_df = spark.read.format("delta").load(delta_path)
            total_rows = total_df.count()
            
            logger.info(f"Extracted {len(data)} rows from Delta table {table.table_name}")
            
            return {
                "schema_name": "delta",
                "table_name": table.table_name,
                "columns": columns,
                "data": data,
                "total_rows": total_rows,
                "fetched_rows": len(data),
                "delta_path": delta_path
            }
            
        except Exception as e:
            logger.error(f"Error extracting Delta Lake data: {e}")
            return {
                "schema_name": "delta",
                "table_name": table.table_name,
                "columns": [],
                "data": [],
                "total_rows": 0,
                "fetched_rows": 0,
                "error": f"Delta Lake extraction failed: {str(e)}"
            }
