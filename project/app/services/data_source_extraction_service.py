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
from ..database.metadata.metadata import ExternalTables, ExternalSchema, ExternalColumn
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
        limit_per_table: Optional[int] = None,
        batch_size: int = 5000
    ) -> pd.DataFrame:
        """
        Extract data from multiple sources and unify into a single DataFrame using batch processing.
        
        Args:
            table_ids: List of table IDs
            column_mappings: Dictionary of column name mappings
            value_mappings: Dictionary of value mappings
            limit_per_table: Limit rows per table (None for all data)
            batch_size: Number of rows to process per batch
        
        Returns:
            Unified pandas DataFrame
        """
        try:
            logger.info(f"Starting batch extraction and unification for {len(table_ids)} tables with batch size {batch_size}")
            
            # Get all unified columns to ensure consistent DataFrame structure
            all_unified_columns = set()
            
            # First pass: determine all possible columns
            for table_id in table_ids:
                table_info = await self._get_table_info(table_id)
                if table_info:
                    table_name = table_info["table_name"]
                    columns = table_info["columns"]
                    
                    for col in columns:
                        col_name = col["column_name"]
                        
                        # Apply column mapping to determine unified column name
                        if column_mappings:
                            table_col_key = f"{table_name}.{col_name}"
                            if table_col_key in column_mappings:
                                unified_col = column_mappings[table_col_key]
                            else:
                                unified_col = column_mappings.get(col_name, col_name)
                        else:
                            unified_col = col_name
                        
                        all_unified_columns.add(unified_col)
            
            # Add source tracking columns
            all_unified_columns.update(['_source_table', '_source_table_id'])
            
            # Create empty DataFrame with all expected columns
            unified_df = pd.DataFrame(columns=list(all_unified_columns))
            
            # Process each table in batches
            total_processed_rows = 0
            for table_id in table_ids:
                logger.info(f"Processing table {table_id} in batches...")
                
                table_rows_processed = 0
                offset = 0
                
                while True:
                    # Extract batch data for current table
                    batch_data = await self._extract_table_batch(
                        table_id, 
                        offset, 
                        batch_size, 
                        limit_per_table
                    )
                    
                    if not batch_data or not batch_data.get("data"):
                        logger.info(f"No more data for table {table_id} at offset {offset}")
                        break
                    
                    # Process batch and apply mappings
                    batch_df = await self._process_batch_with_mappings(
                        batch_data,
                        column_mappings,
                        value_mappings,
                        all_unified_columns
                    )
                    
                    # Concatenate to main DataFrame
                    if not batch_df.empty:
                        unified_df = pd.concat([unified_df, batch_df], ignore_index=True)
                        batch_rows = len(batch_df)
                        table_rows_processed += batch_rows
                        total_processed_rows += batch_rows
                        
                        logger.info(f"Processed batch from table {table_id}: {batch_rows} rows (total from table: {table_rows_processed}, overall total: {total_processed_rows})")
                    
                    offset += batch_size
                    
                    # Check if we've reached the limit for this table
                    if limit_per_table and table_rows_processed >= limit_per_table:
                        logger.info(f"Reached limit of {limit_per_table} rows for table {table_id}")
                        break
                    
                    # Check if batch was smaller than batch_size (end of data)
                    if len(batch_data["data"]) < batch_size:
                        logger.info(f"Reached end of data for table {table_id}")
                        break
            
            logger.info(f"Batch extraction completed: {total_processed_rows} total rows from {len(table_ids)} tables")
            return unified_df
            
        except Exception as e:
            logger.error(f"Error in extract_and_unify_data with batching: {e}")
            raise

    async def extract_and_unify_data_legacy(
        self, 
        table_ids: List[int], 
        column_mappings: Dict[str, str] = None,
        value_mappings: Dict[str, Dict[str, str]] = None,
        limit_per_table: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Legacy method for extracting and unifying data (single-batch approach).
        Use extract_and_unify_data for better memory management.
        
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
            logger.error(f"Error in extract_and_unify_data_legacy: {e}")
            raise

    def _calculate_optimal_batch_size(self, table_count: int, estimated_columns: int = 50, connection_types: List[str] = None) -> int:
        """
        Calculate optimal batch size based on table count, estimated columns, and connection types
        
        Args:
            table_count: Number of tables being processed
            estimated_columns: Estimated number of columns per table
            connection_types: List of connection types being used
        
        Returns:
            Optimal batch size
        """
        # Base batch size - more conservative
        base_batch_size = 2000
        
        # Check if Delta Lake is involved (requires smaller batches due to Spark overhead)
        has_deltalake = connection_types and any('delta' in ct.lower() for ct in connection_types)
        if has_deltalake:
            base_batch_size = 500  # Much smaller for Delta Lake to avoid Spark memory issues
            logger.info("Delta Lake detected - using smaller batch size for memory optimization")
        
        # Adjust based on table count (more tables = smaller batches)
        if table_count > 10:
            base_batch_size = int(base_batch_size * 0.4)
        elif table_count > 5:
            base_batch_size = int(base_batch_size * 0.6)
        elif table_count > 3:
            base_batch_size = int(base_batch_size * 0.8)
        
        # Adjust based on estimated columns (more columns = smaller batches)
        if estimated_columns > 200:
            base_batch_size = int(base_batch_size * 0.3)
        elif estimated_columns > 100:
            base_batch_size = int(base_batch_size * 0.5)
        elif estimated_columns > 50:
            base_batch_size = int(base_batch_size * 0.7)
        
        # Ensure minimum batch size but keep it small for Delta Lake
        min_batch_size = 100 if has_deltalake else 500
        final_batch_size = max(min_batch_size, base_batch_size)
        
        logger.info(f"Calculated optimal batch size: {final_batch_size} (tables: {table_count}, columns: {estimated_columns}, has_delta: {has_deltalake})")
        return final_batch_size

    async def extract_and_unify_data_with_incremental_delta_save(
        self, 
        table_ids: List[int], 
        column_mappings: Dict[str, str] = None,
        value_mappings: Dict[str, Dict[str, str]] = None,
        limit_per_table: Optional[int] = None,
        batch_size: int = 1000,
        minio_service=None,
        dataset_id: int = None,
        bucket_name: str = None,
        specific_column_ids: Optional[List[int]] = None  # NEW: for column-mode selection
    ) -> Dict[str, Any]:
        """
        Extract data from multiple sources and save incrementally to Delta Lake.
        This avoids memory issues by saving each batch directly to Delta Lake.
        
        Args:
            table_ids: List of table IDs
            column_mappings: Dictionary of column name mappings
            value_mappings: Dictionary of value mappings
            limit_per_table: Limit rows per table (None for all data)
            batch_size: Number of rows to process per batch
            minio_service: MinIO service instance for Delta Lake operations
            dataset_id: Dataset ID for tracking
            bucket_name: MinIO bucket name
            specific_column_ids: Optional list of specific column IDs to extract (for column-mode)
        
        Returns:
            Dict with processing summary
        """
        try:
            logger.info(f"Starting incremental batch extraction and Delta Lake save for {len(table_ids)} tables with batch size {batch_size}")
            
            if not minio_service or not dataset_id or not bucket_name:
                raise ValueError("minio_service, dataset_id, and bucket_name are required for incremental save")
            
            # Get all unified columns to ensure consistent DataFrame structure
            all_unified_columns = set()
            
            if specific_column_ids:
                # Column-mode: only include specified columns
                logger.info(f"Using column-mode: extracting only {len(specific_column_ids)} specific columns")
                
                # Get information about specific columns
                for table_id in table_ids:
                    table_info = await self._get_table_info(table_id)
                    if table_info:
                        table_name = table_info["table_name"]
                        columns = table_info["columns"]
                        
                        for col in columns:
                            # Only include this column if it's in the specific_column_ids list
                            if col["id"] in specific_column_ids:
                                col_name = col["column_name"]
                                
                                # Apply column mapping to determine unified column name
                                if column_mappings:
                                    table_col_key = f"{table_name}.{col_name}"
                                    if table_col_key in column_mappings:
                                        unified_col = column_mappings[table_col_key]
                                    else:
                                        unified_col = column_mappings.get(col_name, col_name)
                                else:
                                    unified_col = col_name
                                
                                all_unified_columns.add(unified_col)
                                logger.debug(f"Including column {col_name} from table {table_name} as {unified_col}")
            else:
                # Table-mode: include all columns from selected tables
                logger.info(f"Using table-mode: extracting all columns from {len(table_ids)} tables")
                
                # First pass: determine all possible columns
                for table_id in table_ids:
                    table_info = await self._get_table_info(table_id)
                    if table_info:
                        table_name = table_info["table_name"]
                        columns = table_info["columns"]
                        
                        for col in columns:
                            col_name = col["column_name"]
                            
                            # Apply column mapping to determine unified column name
                            if column_mappings:
                                table_col_key = f"{table_name}.{col_name}"
                                if table_col_key in column_mappings:
                                    unified_col = column_mappings[table_col_key]
                                else:
                                    unified_col = column_mappings.get(col_name, col_name)
                            else:
                                unified_col = col_name
                            
                            all_unified_columns.add(unified_col)
            
            # Add source tracking columns
            all_unified_columns.update(['_source_table', '_source_table_id'])
            
            # Initialize Delta table (create empty table first)
            await minio_service.initialize_incremental_delta_table(
                dataset_id, 
                bucket_name, 
                list(all_unified_columns)
            )
            
            # Process each table in batches and save incrementally
            total_processed_rows = 0
            batch_count = 0
            
            for table_id in table_ids:
                logger.info(f"Processing table {table_id} in incremental batches...")
                
                # Get specific columns for this table if in column-mode
                table_specific_columns = None
                if specific_column_ids:
                    # Get columns that belong to this table
                    table_info = await self._get_table_info(table_id)
                    if table_info:
                        table_specific_columns = [
                            col["id"] for col in table_info["columns"] 
                            if col["id"] in specific_column_ids
                        ]
                        logger.info(f"Table {table_id}: filtering to {len(table_specific_columns)} specific columns")
                
                table_rows_processed = 0
                offset = 0
                
                while True:
                    # Extract batch data for current table
                    batch_data = await self._extract_table_batch(
                        table_id, 
                        offset, 
                        batch_size, 
                        limit_per_table,
                        specific_column_ids=table_specific_columns  # Pass specific columns
                    )
                    
                    if not batch_data or not batch_data.get("data"):
                        logger.info(f"No more data for table {table_id} at offset {offset}")
                        break
                    
                    # Process batch and apply mappings
                    batch_df = await self._process_batch_with_mappings(
                        batch_data,
                        column_mappings,
                        value_mappings,
                        all_unified_columns
                    )
                    
                    # Save batch directly to Delta Lake
                    if not batch_df.empty:
                        # Validate DataFrame before saving
                        logger.debug(f"Batch {batch_count} DataFrame info - Shape: {batch_df.shape}, Columns: {list(batch_df.columns)}")
                        
                        # Check for any issues that might cause Spark problems
                        validation_issues = []
                        
                        # Check for completely empty columns
                        for col in batch_df.columns:
                            if batch_df[col].isna().all():
                                validation_issues.append(f"Column {col} is completely empty")
                                # Fill with empty string to avoid Spark issues
                                batch_df[col] = ""
                        
                        if validation_issues:
                            logger.warning(f"Fixed validation issues in batch {batch_count}: {validation_issues}")
                        
                        await minio_service.append_batch_to_delta_table(
                            dataset_id,
                            bucket_name,
                            batch_df,
                            batch_count
                        )
                        
                        batch_rows = len(batch_df)
                        table_rows_processed += batch_rows
                        total_processed_rows += batch_rows
                        batch_count += 1
                        
                        logger.info(f"Saved batch {batch_count} to Delta Lake from table {table_id}: {batch_rows} rows (total from table: {table_rows_processed}, overall total: {total_processed_rows})")
                    
                    offset += batch_size
                    
                    # Check if we've reached the limit for this table
                    if limit_per_table and table_rows_processed >= limit_per_table:
                        logger.info(f"Reached limit of {limit_per_table} rows for table {table_id}")
                        break
                    
                    # Check if batch was smaller than batch_size (end of data)
                    if len(batch_data["data"]) < batch_size:
                        logger.info(f"Reached end of data for table {table_id}")
                        break
            
            # Finalize Delta table (optimize, vacuum, etc.)
            await minio_service.finalize_incremental_delta_table(dataset_id, bucket_name)
            
            logger.info(f"Incremental batch extraction completed: {total_processed_rows} total rows from {len(table_ids)} tables saved in {batch_count} batches")
            
            return {
                "total_rows_processed": total_processed_rows,
                "total_batches": batch_count,
                "total_tables": len(table_ids),
                "success": True
            }
            
        except Exception as e:
            logger.error(f"Error in incremental batch extraction and Delta save: {e}")
            raise

    async def extract_and_unify_data_with_auto_batch(
        self, 
        table_ids: List[int], 
        column_mappings: Dict[str, str] = None,
        value_mappings: Dict[str, Dict[str, str]] = None,
        limit_per_table: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Extract and unify data with automatic batch size calculation
        
        Args:
            table_ids: List of table IDs
            column_mappings: Dictionary of column name mappings
            value_mappings: Dictionary of value mappings
            limit_per_table: Limit rows per table (None for all data)
        
        Returns:
            Unified pandas DataFrame
        """
        # Get connection types to optimize batch size
        connection_types = []
        try:
            for table_id in table_ids:
                table_query = select(ExternalTables, ConnectionType.name).join(
                    ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
                ).join(
                    DataConnection, ExternalTables.connection_id == DataConnection.id
                ).join(
                    ConnectionType, DataConnection.connection_type_id == ConnectionType.id
                ).where(ExternalTables.id == table_id)
                
                table_result = await self.db.execute(table_query)
                table_data = table_result.first()
                if table_data:
                    connection_types.append(table_data[1])
        except Exception as e:
            logger.warning(f"Could not determine connection types: {e}")
        
        # Calculate optimal batch size
        estimated_columns = len(column_mappings) if column_mappings else 50
        optimal_batch_size = self._calculate_optimal_batch_size(
            len(table_ids), 
            estimated_columns, 
            connection_types
        )
        
        logger.info(f"Using automatic batch size: {optimal_batch_size} for {len(table_ids)} tables with connection types: {connection_types}")
        
        return await self.extract_and_unify_data(
            table_ids,
            column_mappings,
            value_mappings,
            limit_per_table,
            optimal_batch_size
        )

    async def _get_table_info(self, table_id: int) -> Optional[Dict[str, Any]]:
        """Get basic table information including columns"""
        try:
            # Get table metadata using direct query with schema join
            table_query = select(ExternalTables, ExternalSchema.schema_name).join(
                ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
            ).where(ExternalTables.id == table_id)
            table_result = await self.db.execute(table_query)
            table_data = table_result.first()
            
            if not table_data:
                return None
            
            table, schema_name = table_data
            
            # Get columns for this table
            columns_query = select(ExternalColumn).where(ExternalColumn.table_id == table_id)
            columns_result = await self.db.execute(columns_query)
            columns = columns_result.scalars().all()
            
            return {
                "table_name": table.table_name,
                "schema_name": schema_name,
                "columns": [{"id": col.id, "column_name": col.column_name, "data_type": col.data_type} for col in columns]
            }
            
        except Exception as e:
            logger.error(f"Error getting table info for table {table_id}: {e}")
            return None

    async def _extract_table_batch(
        self, 
        table_id: int, 
        offset: int, 
        batch_size: int, 
        limit_per_table: Optional[int] = None,
        specific_column_ids: Optional[List[int]] = None
    ) -> Optional[Dict[str, Any]]:
        """Extract a batch of data from a specific table"""
        try:
            # Get table and connection info
            table_query = select(ExternalTables, ExternalSchema.schema_name).join(
                ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
            ).where(ExternalTables.id == table_id)
            table_result = await self.db.execute(table_query)
            table_data = table_result.first()
            
            if not table_data:
                return None
            
            table, schema_name = table_data
            
            # Get connection info
            connection_query = select(DataConnection, ConnectionType).join(
                ConnectionType, DataConnection.connection_type_id == ConnectionType.id
            ).where(DataConnection.id == table.connection_id)
            connection_result = await self.db.execute(connection_query)
            connection_data = connection_result.first()
            
            if not connection_data:
                return None
            
            connection, connection_type = connection_data
            
            # Calculate actual batch limit
            actual_limit = batch_size
            if limit_per_table:
                remaining_limit = limit_per_table - offset
                if remaining_limit <= 0:
                    return None
                actual_limit = min(batch_size, remaining_limit)
            
            # Extract batch data
            batch_result = await self._extract_data_by_connection_type_with_offset(
                connection, connection_type, table, schema_name, actual_limit, offset
            )
            
            # Filter columns if specific_column_ids is provided
            if batch_result and specific_column_ids:
                batch_result = await self._filter_batch_by_columns(batch_result, table_id, specific_column_ids)
            
            return batch_result
            
        except Exception as e:
            logger.error(f"Error extracting batch from table {table_id}: {e}")
            return None

    async def _filter_batch_by_columns(
        self,
        batch_result: Dict[str, Any],
        table_id: int,
        specific_column_ids: List[int]
    ) -> Dict[str, Any]:
        """Filter batch result to include only specific columns"""
        try:
            if not batch_result or not batch_result.get("data"):
                return batch_result
            
            # Get table info to map column ids to column names
            table_info = await self._get_table_info(table_id)
            if not table_info:
                return batch_result
            
            # Create mapping from column_id to column_name for the specific columns
            allowed_columns = {}
            for col in table_info["columns"]:
                if col["id"] in specific_column_ids:
                    allowed_columns[col["column_name"]] = col["id"]
            
            if not allowed_columns:
                # No specific columns found for this table
                return {
                    "data": [],
                    "table_name": batch_result.get("table_name"),
                    "table_id": batch_result.get("table_id"),
                    "connection_type": batch_result.get("connection_type")
                }
            
            # Filter each row to include only allowed columns
            filtered_data = []
            for row in batch_result["data"]:
                filtered_row = {col_name: row.get(col_name) for col_name in allowed_columns.keys() if col_name in row}
                filtered_data.append(filtered_row)
            
            # Update the batch result
            filtered_result = batch_result.copy()
            filtered_result["data"] = filtered_data
            
            logger.debug(f"Filtered table {table_id} from {len(batch_result['data'][0].keys()) if batch_result['data'] else 0} to {len(allowed_columns)} columns")
            
            return filtered_result
            
        except Exception as e:
            logger.error(f"Error filtering batch by columns for table {table_id}: {e}")
            return batch_result

    async def _process_batch_with_mappings(
        self,
        batch_data: Dict[str, Any],
        column_mappings: Dict[str, str] = None,
        value_mappings: Dict[str, Dict[str, str]] = None,
        all_unified_columns: set = None
    ) -> pd.DataFrame:
        """Process a batch of data and apply column and value mappings"""
        try:
            table_data = batch_data["data"]
            table_name = batch_data["table_name"]
            table_id = batch_data.get("table_id")
            
            if not table_data:
                return pd.DataFrame()
            
            processed_rows = []
            
            for row in table_data:
                mapped_row = {}
                
                # Apply column mappings
                if column_mappings:
                    for col, value in row.items():
                        # Try table-specific mapping first
                        table_col_key = f"{table_name}.{col}"
                        if table_col_key in column_mappings:
                            mapped_col = column_mappings[table_col_key]
                        else:
                            # Fallback to column name only
                            mapped_col = column_mappings.get(col, col)
                        mapped_row[mapped_col] = value
                else:
                    mapped_row = row.copy()
                
                # Apply value mappings
                if value_mappings:
                    for col, value in mapped_row.items():
                        if col in value_mappings and str(value) in value_mappings[col]:
                            mapped_row[col] = value_mappings[col][str(value)]
                
                # Add source tracking info
                mapped_row['_source_table'] = table_name
                mapped_row['_source_table_id'] = table_id or batch_data.get("table_id")
                
                processed_rows.append(mapped_row)
            
            # Create DataFrame with all expected columns
            batch_df = pd.DataFrame(processed_rows)
            
            # Ensure all unified columns are present (fill missing with None)
            if all_unified_columns:
                for col in all_unified_columns:
                    if col not in batch_df.columns:
                        batch_df[col] = None
                
                # Reorder columns to match expected structure
                batch_df = batch_df.reindex(columns=list(all_unified_columns))
            
            return batch_df
            
        except Exception as e:
            logger.error(f"Error processing batch with mappings: {e}")
            return pd.DataFrame()

    async def _extract_data_by_connection_type_with_offset(
        self, 
        connection, 
        connection_type, 
        table, 
        schema_name: str, 
        limit: int, 
        offset: int
    ) -> Optional[Dict[str, Any]]:
        """Extract data with offset support for batch processing"""
        
        connection_type_name = connection_type.name.lower()
        
        if connection_type_name == 'postgresql':
            return await self._extract_postgresql_data_with_offset(connection, table, schema_name, limit, offset)
        elif connection_type_name == 'mysql':
            return await self._extract_mysql_data_with_offset(connection, table, schema_name, limit, offset)
        elif connection_type_name == 'mongodb':
            return await self._extract_mongodb_data_with_offset(connection, table, schema_name, limit, offset)
        elif connection_type_name == 'oracle':
            return await self._extract_oracle_data_with_offset(connection, table, schema_name, limit, offset)
        elif connection_type_name == 'deltalake':
            return await self._extract_deltalake_data_with_offset(connection, table, limit, offset)
        else:
            logger.warning(f"Unsupported connection type: {connection_type_name}")
            return None
    
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

    # Batch extraction methods with offset support
    
    async def _extract_postgresql_data_with_offset(
        self, 
        connection, 
        table, 
        schema_name: str, 
        limit: int, 
        offset: int
    ) -> Dict[str, Any]:
        """Extract PostgreSQL data with offset for batch processing"""
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
            
            # Extract data with offset using the provided schema_name
            data_result = await extractor.extract_table_data(
                schema_name, 
                table.table_name, 
                limit,
                offset
            )
            
            # Add table identification to the result
            data_result["table_id"] = table.id
            data_result["table_name"] = table.table_name
            
            # Close connection
            await extractor.close()
            
            return data_result
            
        except Exception as e:
            logger.error(f"Error extracting PostgreSQL data with offset: {e}")
            raise

    async def _extract_mysql_data_with_offset(
        self, 
        connection, 
        table, 
        schema_name: str, 
        limit: int, 
        offset: int
    ) -> Dict[str, Any]:
        """Extract MySQL data with offset (placeholder for future implementation)"""
        logger.warning("MySQL batch extraction not yet implemented")
        return {
            "table_id": table.id,
            "table_name": table.table_name,
            "schema_name": schema_name,
            "columns": [],
            "data": [],
            "total_rows": 0,
            "fetched_rows": 0,
            "error": "MySQL batch extraction not implemented"
        }

    async def _extract_mongodb_data_with_offset(
        self, 
        connection, 
        table, 
        schema_name: str, 
        limit: int, 
        offset: int
    ) -> Dict[str, Any]:
        """Extract MongoDB data with offset (placeholder for future implementation)"""
        logger.warning("MongoDB batch extraction not yet implemented")
        return {
            "table_id": table.id,
            "table_name": table.table_name,
            "schema_name": schema_name,
            "columns": [],
            "data": [],
            "total_rows": 0,
            "fetched_rows": 0,
            "error": "MongoDB batch extraction not implemented"
        }

    async def _extract_oracle_data_with_offset(
        self, 
        connection, 
        table, 
        schema_name: str, 
        limit: int, 
        offset: int
    ) -> Dict[str, Any]:
        """Extract Oracle data with offset (placeholder for future implementation)"""
        logger.warning("Oracle batch extraction not yet implemented")
        return {
            "table_id": table.id,
            "table_name": table.table_name,
            "schema_name": schema_name,
            "columns": [],
            "data": [],
            "total_rows": 0,
            "fetched_rows": 0,
            "error": "Oracle batch extraction not implemented"
        }

    async def _extract_deltalake_data_with_offset(
        self, 
        connection, 
        table, 
        limit: int, 
        offset: int
    ) -> Dict[str, Any]:
        """Extract Delta Lake data with offset for batch processing"""
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.functions import monotonically_increasing_id
            
            # Get connection parameters
            params = connection.connection_params
            
            # Configure Spark for low memory usage
            spark_config = {
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.hadoop.fs.s3a.endpoint": params.get("s3a_endpoint", "localhost:9000"),
                "spark.hadoop.fs.s3a.access.key": params.get("s3a_access_key", "minio"),
                "spark.hadoop.fs.s3a.secret.key": params.get("s3a_secret_key", "minio123"),
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                # Memory optimization settings
                # "spark.executor.memory": "512m",
                # "spark.driver.memory": "256m",
                # "spark.executor.memoryFraction": "0.6",
                # "spark.sql.adaptive.enabled": "true",
                # "spark.sql.adaptive.coalescePartitions.enabled": "true",
                # "spark.sql.adaptive.coalescePartitions.minPartitionSize": "1MB",
                # "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
                # "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                # "spark.kryo.unsafe": "false",
                # "spark.sql.execution.arrow.pyspark.enabled": "true",
                # "spark.sql.execution.arrow.maxRecordsPerBatch": str(min(limit, 1000))
            }
            
            # Initialize Spark session with optimized configuration
            builder = SparkSession.builder.appName(params.get("app_name", "DeltaLakeExtraction"))
            
            for key, value in spark_config.items():
                builder = builder.config(key, value)
            
            spark = builder.getOrCreate()
            
            # Build the Delta table path
            bucket_name = params.get("bucket_name", "isic-delta")
            delta_path = params.get("delta_path", f"s3a://{bucket_name}/{table.table_name}")
            
            logger.info(f"Reading Delta table batch from: {delta_path} (offset: {offset}, limit: {limit})")
            
            # Read Delta table
            df = spark.read.format("delta").load(delta_path)
            
            # Add row numbers for consistent ordering and offset
            df_with_row_number = df.withColumn("row_number", monotonically_increasing_id())
            
            # Apply offset and limit using row numbers for consistent pagination
            df_paginated = df_with_row_number.filter(
                (df_with_row_number.row_number >= offset) & 
                (df_with_row_number.row_number < offset + limit)
            ).drop("row_number")
            
            # Cache the result to avoid recomputation
            df_paginated.cache()
            
            # Get column information (exclude the temporary row_number column)
            columns = []
            for field in df.schema.fields:
                columns.append({
                    "column_name": field.name,
                    "data_type": str(field.dataType),
                    "nullable": field.nullable
                })
            
            # Convert to list of dictionaries in smaller chunks to avoid memory issues
            logger.info(f"Converting Delta data to Python objects (limit: {limit})")
            
            # Collect data with memory management
            try:
                # Use toPandas() for better memory efficiency if available
                pandas_df = df_paginated.toPandas()
                data = pandas_df.to_dict('records')
                pandas_df = None  # Free memory
            except Exception as pandas_error:
                logger.warning(f"Failed to use Pandas conversion, falling back to collect(): {pandas_error}")
                # Fallback to collect() but with smaller chunks
                data = [row.asDict() for row in df_paginated.collect()]
            
            # Unpersist the cached DataFrame
            df_paginated.unpersist()
            
            # Get total row count only if offset is 0 to avoid repeated expensive operations
            total_rows = 0
            if offset == 0:
                try:
                    total_rows = df.count()
                    logger.info(f"Total rows in Delta table {table.table_name}: {total_rows}")
                except Exception as count_error:
                    logger.warning(f"Could not get total row count: {count_error}")
                    total_rows = 0
            
            logger.info(f"Extracted {len(data)} rows from Delta table {table.table_name} (batch: offset={offset}, limit={limit})")
            
            return {
                "table_id": table.id,
                "table_name": table.table_name,
                "schema_name": "delta",
                "columns": columns,
                "data": data,
                "total_rows": total_rows,
                "fetched_rows": len(data),
                "delta_path": delta_path
            }
            
        except Exception as e:
            logger.error(f"Error extracting Delta Lake data with offset: {e}")
            return {
                "table_id": table.id,
                "table_name": table.table_name,
                "schema_name": "delta",
                "columns": [],
                "data": [],
                "total_rows": 0,
                "fetched_rows": 0,
                "error": f"Delta Lake batch extraction failed: {str(e)}"
            }
        finally:
            # Clean up Spark context to free memory
            try:
                if 'spark' in locals():
                    spark.catalog.clearCache()
            except Exception as cleanup_error:
                logger.warning(f"Error during Spark cleanup: {cleanup_error}")
