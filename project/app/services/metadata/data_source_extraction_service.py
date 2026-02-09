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

# Use generic Trino Extractor
from ..infrastructure.trino_extractor import TrinoExtractor
from ..infrastructure.credential_service import get_credential_service

from ...database.models.metadata import ExternalTables, ExternalSchema, ExternalColumn
from ...database.models.core import DataConnection, ConnectionType
from ..visualization.image_service import ImageService

logger = logging.getLogger(__name__)

class DataSourceExtractionService:
    """Service for extracting real data from data sources"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def extract_data_from_tables(self, table_ids: List[int], limit_per_table: Optional[int] = None) -> Dict[str, Any]:
        """
        Extract real data from multiple tables across different data sources.
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
                    
                    # Extract data using Trino (generic)
                    table_data_result = await self._extract_data_generic(
                        connection, connection_type, table, schema_name, limit_per_table
                    )
                    
                    if table_data_result and "error" not in table_data_result:
                        extraction_results["tables"][table_id] = {
                            "table_name": table.table_name,
                            "schema_name": schema_name,
                            "connection_type": connection_type.name,
                            "connection_name": connection.name,
                            "data": table_data_result["data"],
                            "columns": table_data_result["columns"],
                            "total_rows": table_data_result["total_rows"],
                            "fetched_rows": table_data_result["fetched_rows"]
                        }
                        extraction_results["total_rows"] += table_data_result["fetched_rows"]
                        
                        logger.info(f"Extracted {table_data_result['fetched_rows']} rows from table {table.table_name}")
                    elif table_data_result and "error" in table_data_result:
                         extraction_results["errors"].append(f"Error extracting {table.table_name}: {table_data_result['error']}")
                    
                except Exception as e:
                    error_msg = f"Error extracting data from table {table_id}: {str(e)}"
                    extraction_results["errors"].append(error_msg)
                    logger.error(error_msg)
            
            logger.info(f"Total extraction completed: {extraction_results['total_rows']} rows from {len(extraction_results['tables'])} tables")
            return extraction_results
            
        except Exception as e:
            logger.error(f"Failed to extract data from tables: {e}")
            raise
    
    async def _extract_data_generic(self, connection, connection_type, table, schema_name: str, limit: Optional[int]) -> Dict[str, Any]:
        """Generic data extraction using Trino"""
        try:
            # SEGURANÇA: Descriptografar credenciais antes de usar
            credential_service = get_credential_service()
            decrypted_params = credential_service.decrypt_for_use(
                connection.connection_params,
                connection_id=connection.id,
                purpose="data_extraction"
            )
            
            extractor = TrinoExtractor(
                connection_params=decrypted_params,
                connection_type=connection_type.name,
                connection_name=connection.name,
                connection_id=connection.id
            )
            
            result = await extractor.extract_table_data(
                schema_name=schema_name, 
                table_name=table.table_name, 
                limit=limit
            )
            return result
        except Exception as e:
            logger.error(f"Generic extraction failed: {e}")
            return {"error": str(e)}

    async def _extract_data_generic_with_offset(self, connection, connection_type, table, schema_name: str, limit: int, offset: int) -> Dict[str, Any]:
        """Generic batch data extraction using Trino"""
        try:
            # SEGURANÇA: Descriptografar credenciais antes de usar
            credential_service = get_credential_service()
            decrypted_params = credential_service.decrypt_for_use(
                connection.connection_params,
                connection_id=connection.id,
                purpose="data_extraction_batch"
            )
            
            extractor = TrinoExtractor(
                connection_params=decrypted_params,
                connection_type=connection_type.name,
                connection_name=connection.name,
                connection_id=connection.id
            )
            
            result = await extractor.extract_table_data(
                schema_name=schema_name, 
                table_name=table.table_name, 
                limit=limit,
                offset=offset
            )
            # Add table metadata for unification logic
            result["table_id"] = table.id
            result["table_name"] = table.table_name
            result["connection_type"] = connection_type.name
            
            return result
        except Exception as e:
            logger.error(f"Generic batch extraction failed: {e}")
            return {"error": str(e), "data": []}

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
            
            # Extract batch data using generic Trino extractor
            batch_result = await self._extract_data_generic_with_offset(
                connection, connection_type, table, schema_name, actual_limit, offset
            )
            
            # Filter columns if specific_column_ids is provided
            if batch_result and specific_column_ids:
                batch_result = await self._filter_batch_by_columns(batch_result, table_id, specific_column_ids)
            
            return batch_result
            
        except Exception as e:
            logger.error(f"Error extracting batch from table {table_id}: {e}")
            return None

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

    async def _filter_batch_by_columns(self, batch_result, table_id, specific_column_ids):
        """Filter batch result to include only specific columns"""
        try:
            if not batch_result or not batch_result.get("data"):
                return batch_result
            
            table_info = await self._get_table_info(table_id)
            if not table_info:
                return batch_result
            
            allowed_columns = {}
            for col in table_info["columns"]:
                if col["id"] in specific_column_ids:
                    allowed_columns[col["column_name"]] = col["id"]
            
            if not allowed_columns:
                return {
                    "data": [],
                    "table_name": batch_result.get("table_name"),
                    "table_id": batch_result.get("table_id"),
                    "connection_type": batch_result.get("connection_type")
                }
            
            filtered_data = []
            for row in batch_result["data"]:
                filtered_row = {col_name: row.get(col_name) for col_name in allowed_columns.keys() if col_name in row}
                filtered_data.append(filtered_row)
            
            filtered_result = batch_result.copy()
            filtered_result["data"] = filtered_data
            
            return filtered_result
        except Exception as e:
            logger.error(f"Error filtering batch: {e}")
            return batch_result

    async def _process_batch_with_mappings(self, batch_data, column_mappings, value_mappings, all_unified_columns):
        """Process a batch of data and apply column and value mappings"""
        try:
            table_data = batch_data["data"]
            table_name = batch_data.get("table_name", "unknown_table")
            table_id = batch_data.get("table_id")
            
            if not table_data:
                return pd.DataFrame()
            
            processed_rows = []
            for row in table_data:
                mapped_row = {}
                if column_mappings:
                    for col, value in row.items():
                        if col.endswith('_base64'):
                            base_col = col[:-7]
                            table_col_key = f"{table_name}.{base_col}"
                            mapped_col = f"{column_mappings.get(table_col_key, column_mappings.get(base_col, base_col))}_base64"
                            mapped_row[mapped_col] = value
                        else:
                            table_col_key = f"{table_name}.{col}"
                            mapped_col = column_mappings.get(table_col_key, column_mappings.get(col, col))
                            mapped_row[mapped_col] = value
                else:
                    mapped_row = row.copy()
                
                if value_mappings:
                    for col, value in mapped_row.items():
                        if not col.endswith('_base64') and col in value_mappings and str(value) in value_mappings[col]:
                            mapped_row[col] = value_mappings[col][str(value)]
                
                mapped_row['_source_table'] = table_name
                mapped_row['_source_table_id'] = table_id or batch_data.get("table_id")
                processed_rows.append(mapped_row)
            
            batch_df = pd.DataFrame(processed_rows)
            
            if all_unified_columns:
                for col in all_unified_columns:
                    if col not in batch_df.columns:
                        batch_df[col] = None
                batch_df = batch_df.reindex(columns=list(all_unified_columns))
            
            return batch_df
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            return pd.DataFrame()

    def _calculate_optimal_batch_size(self, table_count, estimated_columns, connection_types):
        """Calculate optimal batch size"""
        base_batch_size = 2000
        has_deltalake = connection_types and any('delta' in ct.lower() for ct in connection_types)
        if has_deltalake:
            base_batch_size = 1000
        
        if table_count > 10:
            base_batch_size = int(base_batch_size * 0.4)
        elif table_count > 5:
             base_batch_size = int(base_batch_size * 0.6)
        elif table_count > 3:
             base_batch_size = int(base_batch_size * 0.8)
             
        if estimated_columns > 200:
             base_batch_size = int(base_batch_size * 0.3)
        elif estimated_columns > 100:
             base_batch_size = int(base_batch_size * 0.5)
        elif estimated_columns > 50:
             base_batch_size = int(base_batch_size * 0.7)
             
        min_batch_size = 100 if has_deltalake else 500
        return max(min_batch_size, base_batch_size)

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
        specific_column_ids: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """Extract data and save incrementally to Delta Lake"""
        return await self.extract_and_unify_data_with_incremental_delta_save_with_images(
            table_ids, column_mappings, value_mappings, limit_per_table, batch_size, minio_service, dataset_id, bucket_name, specific_column_ids, process_images=False
        )

    async def extract_and_unify_data_with_incremental_delta_save_with_images(
        self, 
        table_ids: List[int], 
        column_mappings: Dict[str, str] = None,
        value_mappings: Dict[str, Dict[str, str]] = None,
        limit_per_table: Optional[int] = None,
        batch_size: int = 1000,
        minio_service=None,
        dataset_id: int = None,
        bucket_name: str = None,
        specific_column_ids: Optional[List[int]] = None,
        process_images: bool = True
    ) -> Dict[str, Any]:
        """
        Extract data from multiple sources with image processing and save incrementally to Delta Lake.
        """
        try:
            logger.info(f"Starting incremental batch extraction with image processing for {len(table_ids)} tables with batch size {batch_size}")
            
            if not minio_service or not dataset_id or not bucket_name:
                raise ValueError("minio_service, dataset_id, and bucket_name are required for incremental save")
            
            # Initialize image service
            image_service = ImageService(self.db) if process_images else None
            
            # Get image columns info for all relevant tables
            image_columns_info = {}
            total_image_columns = 0
            
            if process_images:
                logger.info("Identifying image columns...")
                for table_id in table_ids:
                    # Get all column IDs for this table
                    columns_query = select(ExternalColumn.id, ExternalColumn.column_name).where(
                        ExternalColumn.table_id == table_id
                    )
                    columns_result = await self.db.execute(columns_query)
                    columns_data = columns_result.fetchall()
                    
                    table_column_ids = [col.id for col in columns_data]
                    
                    # Filter by specific columns if in column mode
                    if specific_column_ids:
                        table_column_ids = [col_id for col_id in table_column_ids if col_id in specific_column_ids]
                    
                    # Get image columns info for this table
                    table_image_info = await image_service.get_image_columns_info(table_column_ids)
                    
                    if table_image_info:
                        image_columns_info[table_id] = table_image_info
                        total_image_columns += len(table_image_info)
                        logger.info(f"Table {table_id}: found {len(table_image_info)} image columns")
                
                logger.info(f"Total image columns found: {total_image_columns}")
            
            # Get all unified columns to ensure consistent DataFrame structure
            all_unified_columns = set()
            
            if specific_column_ids:
                # Column-mode
                for table_id in table_ids:
                    table_info = await self._get_table_info(table_id)
                    if table_info:
                        table_name = table_info["table_name"]
                        columns = table_info["columns"]
                        
                        for col in columns:
                            if col["id"] in specific_column_ids:
                                col_name = col["column_name"]
                                
                                if column_mappings:
                                    table_col_key = f"{table_name}.{col_name}"
                                    if table_col_key in column_mappings:
                                        unified_col = column_mappings[table_col_key]
                                    else:
                                        unified_col = column_mappings.get(col_name, col_name)
                                else:
                                    unified_col = col_name
                                
                                all_unified_columns.add(unified_col)
            else:
                # Table-mode
                for table_id in table_ids:
                    table_info = await self._get_table_info(table_id)
                    if table_info:
                        table_name = table_info["table_name"]
                        columns = table_info["columns"]
                        
                        for col in columns:
                            col_name = col["column_name"]
                            
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
            
            # Initialize Delta table
            await minio_service.initialize_incremental_delta_table(
                dataset_id, 
                bucket_name, 
                list(all_unified_columns)
            )
            
            # Process each table in batches and save incrementally
            total_processed_rows = 0
            batch_count = 0
            total_images_processed = 0
            
            for table_id in table_ids:
                logger.info(f"Processing table {table_id} in incremental batches...")
                
                table_specific_columns = None
                if specific_column_ids:
                    table_info = await self._get_table_info(table_id)
                    if table_info:
                        table_specific_columns = [
                            col["id"] for col in table_info["columns"] 
                            if col["id"] in specific_column_ids
                        ]
                
                table_rows_processed = 0
                offset = 0
                
                while True:
                    # Extract batch data
                    batch_data = await self._extract_table_batch(
                        table_id, 
                        offset, 
                        batch_size, 
                        limit_per_table,
                        specific_column_ids=table_specific_columns
                    )
                    
                    if not batch_data or not batch_data.get("data"):
                        logger.info(f"No more data for table {table_id} at offset {offset}")
                        break
                    
                    # Convert to DataFrame
                    batch_df = pd.DataFrame(batch_data["data"])
                    
                    # Process images
                    if process_images and table_id in image_columns_info and not batch_df.empty:
                        try:
                            destination_minio_client = minio_service.minio_client if minio_service else None
                            
                            batch_df = await image_service.process_image_columns_in_dataframe(
                                batch_df, 
                                image_columns_info[table_id],
                                destination_minio_client=destination_minio_client,
                                destination_bucket=bucket_name,
                                save_images_to_destination=True
                            )
                            
                            for img_info in image_columns_info[table_id]:
                                base64_col = f"{img_info['column_name']}_base64"
                                if base64_col in batch_df.columns:
                                    images_in_batch = len([img for img in batch_df[base64_col] if img])
                                    total_images_processed += images_in_batch
                        
                        except Exception as e:
                            logger.error(f"Error processing images for batch {batch_count}: {e}")
                    
                    # Convert back to dict for mapping
                    batch_data_for_mapping = {
                        "data": batch_df.to_dict('records'),
                        "columns": batch_data.get("columns", []),
                        "table_name": batch_data.get("table_name", f"table_{table_id}"),
                        "table_id": table_id
                    }
                    
                    # Process batch and apply mappings
                    batch_df = await self._process_batch_with_mappings(
                        batch_data_for_mapping,
                        column_mappings,
                        value_mappings,
                        all_unified_columns
                    )
                    
                    # Save batch directly to Delta Lake
                    if not batch_df.empty:
                        # Validation
                        validation_issues = []
                        for col in batch_df.columns:
                            if batch_df[col].isna().all():
                                validation_issues.append(f"Column {col} is completely empty")
                                batch_df[col] = ""
                        
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
                        
                        logger.info(f"Saved batch {batch_count} to Delta Lake from table {table_id}: {batch_rows} rows")
                    
                    offset += batch_size
                    
                    if limit_per_table and table_rows_processed >= limit_per_table:
                        break
                    
                    if len(batch_data["data"]) < batch_size:
                        break
            
            # Finalize Delta table
            await minio_service.finalize_incremental_delta_table(dataset_id, bucket_name)
            
            return {
                "total_rows_processed": total_processed_rows,
                "total_batches": batch_count,
                "total_tables": len(table_ids),
                "total_images_processed": total_images_processed,
                "image_columns_count": total_image_columns,
                "image_processing_enabled": process_images,
                "success": True
            }
            
        except Exception as e:
            logger.error(f"Error in incremental batch extraction: {e}")
            raise
            
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
