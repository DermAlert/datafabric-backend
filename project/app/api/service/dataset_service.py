from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload, joinedload
from sqlalchemy import and_, or_, func, desc
from typing import List, Optional, Dict, Any, Tuple
from fastapi import HTTPException, status
from datetime import datetime
import os
import logging

logger = logging.getLogger(__name__)

from ...database.core.core import Dataset, DatasetSource, DatasetColumn, DatasetColumnSource
from ...database.metadata.metadata import ExternalTables, ExternalColumn, ExternalSchema
from ...database.equivalence.equivalence import ColumnMapping, ValueMapping, ColumnGroup
from ...database.storage.storage import DatasetStorage
from ..schemas.dataset_schemas import (
    DatasetCreate, DatasetUpdate, DatasetResponse, DatasetUnifiedCreate,
    DatasetUnificationPreview, DatasetUnificationPreviewRequest, SearchDataset, SelectedTableInfo, MappingInfo, SelectionMode
)
from ..schemas.search import SearchResult
from .data_unification_service import DataUnificationService
from app.services.dataset_minio_service import DatasetMinioService
from app.services.data_source_extraction_service import DataSourceExtractionService

class DatasetService:
    def __init__(self, db: AsyncSession):
        self.db = db
        # Initialize MinIO configuration
        self.minio_config = {
            'endpoint': os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            'access_key': os.getenv('MINIO_ACCESS_KEY', 'minio'),
            'secret_key': os.getenv('MINIO_SECRET_KEY', 'minio123'),
            'secure': os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        }
        
        # Configuration for batch processing
        self.extraction_config = {
            'default_batch_size': int(os.getenv('EXTRACTION_BATCH_SIZE', '2000')),  # Reduced from 5000
            'delta_batch_size': int(os.getenv('DELTA_BATCH_SIZE', '1000')),  # Smaller batches for Delta Lake
            'max_memory_usage_mb': int(os.getenv('MAX_MEMORY_USAGE_MB', '256')),  # Reduced from 512
            'spark_executor_memory': os.getenv('SPARK_EXECUTOR_MEMORY', '1g'),
            'spark_driver_memory': os.getenv('SPARK_DRIVER_MEMORY', '512m')
        }

    async def create_unified_dataset(self, dataset_data: DatasetUnifiedCreate) -> DatasetResponse:
        """Create a dataset with automatic column mapping unification"""
        try:
            # Initialize unification service
            unification_service = DataUnificationService(self.db)
            
            # Determine what to validate based on selection mode
            if dataset_data.selection_mode.value == 'tables':
                if not dataset_data.selected_tables:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="selected_tables is required when selection_mode is 'tables'"
                    )
                
                # 1. Validate compatibility for table-based selection
                validation = await unification_service.validate_unification_compatibility(dataset_data.selected_tables)
                if not validation['valid']:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Unification validation failed: {validation['errors']}"
                    )
                
                # 2. Get unified tables with mappings (table-based approach)
                unified_tables_info = await unification_service.get_unified_tables_with_mappings(
                    dataset_data.selected_tables,
                    dataset_data.auto_include_mapped_columns
                )
                
                # 3. Generate unified columns from tables
                unified_columns = await unification_service.generate_unified_columns(unified_tables_info)
                
            else:  # selection_mode == 'columns'
                if not dataset_data.selected_columns:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="selected_columns is required when selection_mode is 'columns'"
                    )
                
                # 1. Get unified structure from selected columns (column-based approach)
                unified_columns_info = await unification_service.get_unified_columns_from_selected_columns(
                    dataset_data.selected_columns,
                    dataset_data.auto_include_mapped_columns
                )
                
                # 2. Generate unified columns from selected columns
                unified_columns = await unification_service.generate_unified_columns_from_selected_columns(unified_columns_info)
                
                # 3. Create tables info structure for compatibility with the rest of the code
                unified_tables_info = {
                    'table_ids': unified_columns_info['table_ids'],
                    'mapping_groups': unified_columns_info['mapping_groups'],
                    'additional_tables': []  # Not applicable for column-based selection
                }
            
            # 4. Get value mappings if requested
            value_mappings = {}
            if dataset_data.apply_value_mappings:
                group_ids = [group['group_id'] for group in unified_tables_info['mapping_groups']]
                value_mappings = await unification_service.get_value_mappings_for_groups(group_ids)
            
            # 5. Create the dataset
            dataset = Dataset(
                name=dataset_data.name,
                description=dataset_data.description,
                storage_type=dataset_data.storage_type.value,
                refresh_type=dataset_data.refresh_type.value,
                refresh_schedule=dataset_data.refresh_schedule,
                version=dataset_data.version,
                properties=dataset_data.properties,
                status='draft'
            )
            
            self.db.add(dataset)
            await self.db.flush()
            
            # 6. Create dataset sources
            primary_table = True
            for table_id in unified_tables_info['table_ids']:
                source = DatasetSource(
                    dataset_id=dataset.id,
                    table_id=table_id,
                    join_type='primary' if primary_table else 'left'
                )
                self.db.add(source)
                primary_table = False
            
            # 7. Create unified columns and their sources
            await self._create_unified_columns_enhanced(dataset.id, unified_columns, value_mappings)
            
            # 8. Create storage configuration and export to MinIO if needed
            storage_location = f"dataset_{dataset.id}"
            
            if dataset_data.storage_type.value == 'copy_to_minio':
                # Export to MinIO with Delta Lake using Trino Federation (CTAS)
                minio_service = DatasetMinioService(self.minio_config)
                await minio_service.initialize()
                
                from app.services.trino_manager import TrinoManager
                trino_manager = TrinoManager()
                
                try:
                    # Create bucket for the dataset
                    bucket_name = await minio_service.create_dataset_bucket(dataset.id, dataset.name)
                    storage_location = f"s3a://{bucket_name}"
                    
                    # Ensure we have a catalog for our MinIO output
                    # We use a generic 'minio_datalake' catalog for all datasets
                    minio_params = {
                        "endpoint_url": self.minio_config['endpoint'], # TrinoManager maps this to s3.endpoint
                        "s3a_access_key": self.minio_config['access_key'],
                        "s3a_secret_key": self.minio_config['secret_key'],
                        "secure": str(self.minio_config['secure']),
                        "region": "us-east-1",
                        "fs.native-s3.enabled": "true",
                        "s3.path-style-access": "true"
                    }
                    
                    # Log configuration for debugging (mask secrets)
                    logger.info(f"MinIO Config - Endpoint: {self.minio_config['endpoint']}")
                    logger.info(f"MinIO Config - Access Key: {self.minio_config['access_key']}")
                    logger.info(f"MinIO Config - Secret Key: {self.minio_config['secret_key']}")
                    logger.info(f"MinIO Config - Secure: {self.minio_config['secure']}")
                    logger.info(f"Catalog params being sent to TrinoManager: {list(minio_params.keys())}")
                    
                    # Force recreation of catalog with credentials if it might be missing them
                    # trino_manager.drop_catalog("minio_datalake") 
                    trino_manager.ensure_catalog_exists("minio_datalake", "delta", minio_params)

                    
                    # Generate Federated Query
                    logger.info("Generating federated SQL query for unification...")
                    select_query = await unification_service.generate_federated_query(
                        unified_columns, 
                        unified_tables_info, 
                        value_mappings
                    )
                    
                    # Execute CTAS in Trino
                    conn = trino_manager.get_connection()
                    cur = conn.cursor()
                    
                    try:
                        # Create schema mapping to the bucket
                        schema_name = f"dataset_{dataset.id}"
                        schema_location_uri = f"s3a://{bucket_name}/{schema_name}"
                        
                        logger.info(f"Creating Trino schema {schema_name} at {schema_location_uri}")
                        cur.execute(f"CREATE SCHEMA IF NOT EXISTS minio_datalake.\"{schema_name}\" WITH (location = '{schema_location_uri}')")
                        
                        # Create Table AS Select
                        table_name = "unified_data"
                        ctas_query = f"""
                            CREATE TABLE minio_datalake."{schema_name}"."{table_name}" 
                            AS 
                            {select_query}
                        """
                        
                        logger.info(f"Executing CTAS in Trino (this may take a while)...")
                        cur.execute(ctas_query)
                        
                        # Get stats
                        cur.execute(f"SELECT count(*) FROM minio_datalake.\"{schema_name}\".\"{table_name}\"")
                        total_rows_processed = cur.fetchone()[0]
                        logger.info(f"CTAS completed. Created {total_rows_processed} rows.")
                        
                        delta_path = f"{schema_location_uri}/{table_name}"
                        
                    finally:
                        conn.close()
                    
                    # Create dummy DataFrame for metadata export (actual data is already in Delta Lake)
                    dataset_metadata = {
                        "name": dataset.name,
                        "description": dataset.description,
                        "storage_type": dataset.storage_type,
                        "refresh_type": dataset.refresh_type,
                        "status": dataset.status,
                        "version": dataset.version,
                        "properties": dataset.properties
                    }
                    
                    # Get columns and sources metadata
                    columns_metadata = [{
                        "name": col["name"],
                        "description": col.get("description"),
                        "data_type": col["data_type"],
                        "is_nullable": True,
                        "column_position": i + 1,
                        "is_visible": True,
                        "properties": col.get("properties", {})
                    } for i, col in enumerate(unified_columns)]
                    
                    sources_metadata = [{
                        "table_id": table_id,
                        "join_type": "primary" if i == 0 else "left",
                        "join_condition": None,
                        "filter_condition": None
                    } for i, table_id in enumerate(unified_tables_info['table_ids'])]
                    
                    # Export metadata files (using minio_service for JSON/Parquet metadata only)
                    await minio_service.export_dataset_metadata_only(
                        dataset.id,
                        bucket_name,
                        dataset_metadata,
                        columns_metadata,
                        sources_metadata
                    )
                    
                    # Update dataset properties with export information
                    dataset.properties.update({
                        'minio_bucket': bucket_name,
                        'delta_path': delta_path,
                        'export_timestamp': datetime.now().isoformat(),
                        'export_status': 'completed',
                        'extraction_method': 'trino_ctas',
                        'total_rows_processed': total_rows_processed,
                        'total_batches': 1, # CTAS is one batch effectively
                        'batch_size': 0
                    })
                    
                    # Validate export (simple check)
                    validation_result = await minio_service.validate_export(bucket_name, dataset.id)
                    if not validation_result['valid']:
                        logger.warning(f"MinIO export validation warning: {validation_result['errors']}")
                    
                finally:
                    minio_service.close()
            
            # Create storage record
            if dataset_data.storage_properties or dataset_data.storage_type.value == 'copy_to_minio':
                storage_props = dataset_data.storage_properties.copy() if dataset_data.storage_properties else {}
                if dataset_data.storage_type.value == 'copy_to_minio':
                    storage_props.update({
                        'minio_endpoint': self.minio_config['endpoint'],
                        'bucket_name': dataset.properties.get('minio_bucket'),
                        'delta_path': dataset.properties.get('delta_path')
                    })
                
                storage = DatasetStorage(
                    dataset_id=dataset.id,
                    storage_type=dataset_data.storage_type.value,
                    storage_location=storage_location,
                    storage_properties=storage_props
                )
                self.db.add(storage)
            
            # 9. Generate SQL transformation if needed
            if dataset_data.storage_type.value in ['materialized', 'copy_to_minio']:
                sql_transformation = await unification_service.generate_transformation_sql(unified_columns, value_mappings)
                dataset.properties['sql_transformation'] = sql_transformation
            
            await self.db.commit()
            
            return await self.get(dataset.id)
            
        except HTTPException:
            await self.db.rollback()
            raise
        except Exception as e:
            await self.db.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error creating unified dataset: {str(e)}"
            )

    async def get_unification_preview(self, selected_table_ids: List[int]) -> DatasetUnificationPreview:
        """Get preview of what will be created when unifying the selected tables"""
        try:
            # Use the unification service for enhanced preview
            unification_service = DataUnificationService(self.db)
            
            # Get unified tables info (default to auto_include_mapped_columns=True for preview)
            unified_tables_info = await unification_service.get_unified_tables_with_mappings(
                selected_table_ids, 
                auto_include_mapped_columns=True
            )
            
            # Get unified columns
            unified_columns = await unification_service.generate_unified_columns(unified_tables_info)
            
            # Get value mappings count
            group_ids = [group['group_id'] for group in unified_tables_info['mapping_groups']]
            value_mappings = await unification_service.get_value_mappings_for_groups(group_ids)
            value_mappings_count = sum(len(mappings) for mappings in value_mappings.values())
            
            # Get table information
            tables_query = select(
                ExternalTables.id,
                ExternalTables.table_name,
                ExternalSchema.schema_name,
                ExternalTables.connection_id
            ).join(
                ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
            ).where(
                ExternalTables.id.in_(unified_tables_info['table_ids'])
            )
            
            tables_result = await self.db.execute(tables_query)
            tables_data = tables_result.fetchall()
            
            # Build selected tables info
            selected_tables = []
            for table_data in tables_data:
                # Get columns for this table
                columns_in_table = [
                    col_info['column_id'] for col in unified_columns 
                    for col_info in col['source_columns'] 
                    if col_info.get('table_id') == table_data.id
                ]
                
                selected_tables.append(SelectedTableInfo(
                    table_id=table_data.id,
                    table_name=table_data.table_name,
                    schema_name=table_data.schema_name,
                    connection_name=f"Connection_{table_data.connection_id}",
                    selected_columns=columns_in_table
                ))
            
            # Build mapping info
            mapping_groups = []
            for group in unified_tables_info['mapping_groups']:
                mapping_groups.append(MappingInfo(
                    group_id=group['group_id'],
                    group_name=f"Group_{group['group_id']}",
                    mapped_columns=[col['column_id'] for col in group['columns']],
                    standard_column_name=f"unified_column_{group['group_id']}",
                    data_type='string'
                ))
            
            # Convert unified columns to the expected format
            unified_columns_response = []
            for col in unified_columns:
                unified_columns_response.append({
                    'name': col['name'],
                    'type': col['source_type'],
                    'source_columns': [src['column_id'] for src in col['source_columns']],
                    'data_type': col['data_type'],
                    'group_id': col.get('group_id')
                })
            
            return DatasetUnificationPreview(
                selected_tables=selected_tables,
                unified_columns=unified_columns_response,
                mapping_groups=mapping_groups,
                value_mappings_count=value_mappings_count,
                estimated_columns_count=len(unified_columns)
            )
            
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error generating unification preview: {str(e)}"
            )

    async def get_unification_preview_enhanced(self, preview_request: DatasetUnificationPreviewRequest) -> DatasetUnificationPreview:
        """Get enhanced preview supporting both table and column selection modes"""
        try:
            unification_service = DataUnificationService(self.db)
            
            if preview_request.selection_mode == SelectionMode.TABLES:
                # Table-based preview
                unified_tables_info = await unification_service.get_unified_tables_with_mappings(
                    preview_request.selected_tables,
                    preview_request.auto_include_mapped_columns
                )
                unified_columns = await unification_service.generate_unified_columns(unified_tables_info)
                
                # Get table information
                tables_query = select(
                    ExternalTables.id,
                    ExternalTables.table_name,
                    ExternalSchema.schema_name,
                    ExternalTables.connection_id
                ).join(
                    ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
                ).where(
                    ExternalTables.id.in_(unified_tables_info['table_ids'])
                )
                
                tables_result = await self.db.execute(tables_query)
                tables_data = tables_result.fetchall()
                
                # Build selected tables info
                selected_tables = []
                for table_data in tables_data:
                    columns_in_table = [
                        col_info['column_id'] for col in unified_columns 
                        for col_info in col['source_columns'] 
                        if col_info.get('table_id') == table_data.id
                    ]
                    
                    selected_tables.append(SelectedTableInfo(
                        table_id=table_data.id,
                        table_name=table_data.table_name,
                        schema_name=table_data.schema_name,
                        connection_name=f"Connection_{table_data.connection_id}",
                        selected_columns=columns_in_table
                    ))
                
                additional_tables_included = unified_tables_info.get('additional_tables', [])
                additional_columns_included = []
                selected_columns_info = None
                
            else:  # Column-based preview
                unified_columns_info = await unification_service.get_unified_columns_from_selected_columns(
                    preview_request.selected_columns,
                    preview_request.auto_include_mapped_columns
                )
                unified_columns = await unification_service.generate_unified_columns_from_selected_columns(unified_columns_info)
                
                # Get column information for preview
                columns_query = select(
                    ExternalColumn.id,
                    ExternalColumn.column_name,
                    ExternalColumn.data_type,
                    ExternalColumn.table_id,
                    ExternalTables.table_name,
                    ExternalSchema.schema_name,
                    ExternalTables.connection_id
                ).join(
                    ExternalTables, ExternalColumn.table_id == ExternalTables.id
                ).join(
                    ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
                ).where(
                    ExternalColumn.id.in_(unified_columns_info['column_ids'])
                )
                
                columns_result = await self.db.execute(columns_query)
                columns_data = columns_result.fetchall()
                
                # Build selected tables info (group by table)
                tables_dict = {}
                for col_data in columns_data:
                    if col_data.table_id not in tables_dict:
                        tables_dict[col_data.table_id] = {
                            'table_id': col_data.table_id,
                            'table_name': col_data.table_name,
                            'schema_name': col_data.schema_name,
                            'connection_name': f"Connection_{col_data.connection_id}",
                            'selected_columns': []
                        }
                    tables_dict[col_data.table_id]['selected_columns'].append(col_data.id)
                
                selected_tables = [SelectedTableInfo(**table_info) for table_info in tables_dict.values()]
                
                # Build selected columns info
                selected_columns_info = [
                    {
                        'column_id': col_data.id,
                        'column_name': col_data.column_name,
                        'data_type': col_data.data_type,
                        'table_name': col_data.table_name,
                        'schema_name': col_data.schema_name,
                        'is_originally_selected': col_data.id in preview_request.selected_columns
                    }
                    for col_data in columns_data
                ]
                
                unified_tables_info = {
                    'mapping_groups': unified_columns_info['mapping_groups']
                }
                
                additional_tables_included = []
                additional_columns_included = unified_columns_info.get('additional_column_ids', [])
            
            # Get value mappings count
            group_ids = [group['group_id'] for group in unified_tables_info['mapping_groups']]
            value_mappings = await unification_service.get_value_mappings_for_groups(group_ids)
            value_mappings_count = sum(len(mappings) for mappings in value_mappings.values())
            
            # Build mapping info
            mapping_groups = []
            for group in unified_tables_info['mapping_groups']:
                mapping_groups.append(MappingInfo(
                    group_id=group['group_id'],
                    group_name=f"Group_{group['group_id']}",
                    mapped_columns=[col['column_id'] for col in group['columns']],
                    standard_column_name=f"unified_column_{group['group_id']}",
                    data_type='string'
                ))
            
            # Convert unified columns to the expected format
            unified_columns_response = []
            for col in unified_columns:
                unified_columns_response.append({
                    'name': col['name'],
                    'type': col['source_type'],
                    'source_columns': [src['column_id'] for src in col['source_columns']],
                    'data_type': col['data_type'],
                    'group_id': col.get('group_id')
                })
            
            return DatasetUnificationPreview(
                selection_mode=preview_request.selection_mode,
                selected_tables=selected_tables,
                selected_columns_info=selected_columns_info,
                unified_columns=unified_columns_response,
                mapping_groups=mapping_groups,
                value_mappings_count=value_mappings_count,
                estimated_columns_count=len(unified_columns),
                additional_tables_included=additional_tables_included,
                additional_columns_included=additional_columns_included
            )
            
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error generating enhanced unification preview: {str(e)}"
            )

    async def _create_unified_columns_enhanced(self, dataset_id: int, unified_columns: List[Dict[str, Any]], value_mappings: Dict[int, List[Dict[str, Any]]]):
        """Create unified columns with enhanced mapping information"""
        position = 1
        
        for unified_col in unified_columns:
            # Determine the best data type from source columns
            source_data_types = [src['data_type'] for src in unified_col['source_columns']]
            unified_data_type = self._determine_unified_data_type(source_data_types)
            
            # Create dataset column
            dataset_column = DatasetColumn(
                dataset_id=dataset_id,
                name=unified_col['name'],
                description=unified_col.get('description'),
                data_type=unified_data_type,
                is_nullable=True,
                column_position=position,
                is_visible=True,
                properties={
                    'source_type': unified_col['source_type'],
                    'group_id': unified_col.get('group_id'),
                    'has_value_mappings': unified_col.get('group_id') in value_mappings if value_mappings else False
                }
            )
            
            self.db.add(dataset_column)
            await self.db.flush()
            
            # Create column sources
            for i, source_col in enumerate(unified_col['source_columns']):
                transformation_expression = None
                
                # Add value mapping transformation if available
                if unified_col.get('group_id') and unified_col['group_id'] in value_mappings:
                    col_mappings = [
                        m for m in value_mappings[unified_col['group_id']] 
                        if m['source_column_id'] == source_col['column_id']
                    ]
                    if col_mappings:
                        # Create CASE statement for value mapping
                        case_conditions = []
                        for mapping in col_mappings:
                            case_conditions.append(f"WHEN '{mapping['source_value']}' THEN '{mapping['standard_value']}'")
                        
                        transformation_expression = f"CASE {source_col['column_name']} {' '.join(case_conditions)} ELSE {source_col['column_name']} END"
                
                column_source = DatasetColumnSource(
                    dataset_column_id=dataset_column.id,
                    source_column_id=source_col['column_id'],
                    transformation_type='calculated' if transformation_expression else 'direct',
                    transformation_expression=transformation_expression,
                    is_primary_source=i == 0
                )
                self.db.add(column_source)
            
            position += 1

    def _determine_unified_data_type(self, source_data_types: List[str]) -> str:
        """Determine the best unified data type from source column types"""
        # Remove duplicates and None values
        types = list(set(t for t in source_data_types if t))
        
        if not types:
            return 'string'
        
        if len(types) == 1:
            return types[0]
        
        # Priority order for type unification
        type_priority = {
            'string': 1,
            'text': 1,
            'varchar': 1,
            'integer': 2,
            'int': 2,
            'bigint': 2,
            'decimal': 3,
            'numeric': 3,
            'float': 3,
            'double': 3,
            'boolean': 4,
            'date': 5,
            'timestamp': 5,
            'datetime': 5
        }
        
        # If we have mixed types, default to string for compatibility
        if len(set(type_priority.get(t.lower(), 0) for t in types)) > 1:
            return 'string'
        
        # Return the most specific type
        return min(types, key=lambda t: type_priority.get(t.lower(), 0))

    async def create(self, dataset_data: DatasetCreate) -> DatasetResponse:
        """Create a standard dataset"""
        try:
            dataset = Dataset(
                name=dataset_data.name,
                description=dataset_data.description,
                storage_type=dataset_data.storage_type.value,
                refresh_type=dataset_data.refresh_type.value,
                refresh_schedule=dataset_data.refresh_schedule,
                version=dataset_data.version,
                properties=dataset_data.properties,
                status='draft'
            )
            
            self.db.add(dataset)
            await self.db.flush()
            
            # Create sources
            for source_data in dataset_data.sources:
                source = DatasetSource(
                    dataset_id=dataset.id,
                    table_id=source_data.table_id,
                    join_type=source_data.join_type.value,
                    join_condition=source_data.join_condition,
                    filter_condition=source_data.filter_condition
                )
                self.db.add(source)
            
            # Create columns
            for col_data in dataset_data.columns:
                column = DatasetColumn(
                    dataset_id=dataset.id,
                    name=col_data.name,
                    description=col_data.description,
                    data_type=col_data.data_type,
                    is_nullable=col_data.is_nullable,
                    column_position=col_data.column_position,
                    transformation_expression=col_data.transformation_expression,
                    is_visible=col_data.is_visible,
                    format_pattern=col_data.format_pattern,
                    properties=col_data.properties
                )
                self.db.add(column)
            
            await self.db.commit()
            return await self.get(dataset.id)
            
        except Exception as e:
            await self.db.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error creating dataset: {str(e)}"
            )

    async def get(self, dataset_id: int) -> DatasetResponse:
        """Get dataset by ID"""
        query = select(Dataset).where(Dataset.id == dataset_id)
        result = await self.db.execute(query)
        dataset = result.scalar_one_or_none()
        
        if not dataset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset with id {dataset_id} not found"
            )
        
        # Get sources
        sources_query = select(DatasetSource).where(DatasetSource.dataset_id == dataset_id)
        sources_result = await self.db.execute(sources_query)
        sources = sources_result.scalars().all()
        
        # Get columns
        columns_query = select(DatasetColumn).where(
            DatasetColumn.dataset_id == dataset_id
        ).order_by(DatasetColumn.column_position)
        columns_result = await self.db.execute(columns_query)
        columns = columns_result.scalars().all()
        
        return DatasetResponse(
            id=dataset.id,
            name=dataset.name,
            description=dataset.description,
            storage_type=dataset.storage_type,
            refresh_type=dataset.refresh_type,
            refresh_schedule=dataset.refresh_schedule,
            status=dataset.status,
            version=dataset.version,
            properties=dataset.properties,
            sources=[{
                'id': s.id,
                'table_id': s.table_id,
                'join_type': s.join_type,
                'join_condition': s.join_condition,
                'filter_condition': s.filter_condition
            } for s in sources],
            columns=[{
                'id': c.id,
                'name': c.name,
                'description': c.description,
                'data_type': c.data_type,
                'is_nullable': c.is_nullable,
                'column_position': c.column_position,
                'transformation_expression': c.transformation_expression,
                'is_visible': c.is_visible,
                'format_pattern': c.format_pattern,
                'properties': c.properties
            } for c in columns],
            data_criacao=dataset.data_criacao,
            data_atualizacao=dataset.data_atualizacao
        )

    async def update(self, dataset_id: int, dataset_data: DatasetUpdate) -> DatasetResponse:
        """Update dataset"""
        query = select(Dataset).where(Dataset.id == dataset_id)
        result = await self.db.execute(query)
        dataset = result.scalar_one_or_none()
        
        if not dataset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset with id {dataset_id} not found"
            )
        
        # Update fields
        for field, value in dataset_data.model_dump(exclude_unset=True).items():
            if hasattr(dataset, field):
                setattr(dataset, field, value.value if hasattr(value, 'value') else value)
        
        await self.db.commit()
        return await self.get(dataset_id)

    async def delete(self, dataset_id: int):
        """Delete dataset"""
        query = select(Dataset).where(Dataset.id == dataset_id)
        result = await self.db.execute(query)
        dataset = result.scalar_one_or_none()
        
        if not dataset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Dataset with id {dataset_id} not found"
            )
        
        await self.db.delete(dataset)
        await self.db.commit()

    async def list(self, search: SearchDataset) -> SearchResult[DatasetResponse]:
        """List datasets with search and pagination"""
        query = select(Dataset)
        
        # Apply filters
        if search.name:
            query = query.where(Dataset.name.ilike(f"%{search.name}%"))
        if search.status:
            query = query.where(Dataset.status == search.status.value)
        if search.storage_type:
            query = query.where(Dataset.storage_type == search.storage_type.value)
        
        # Count total
        count_query = select(func.count(Dataset.id))
        if search.name:
            count_query = count_query.where(Dataset.name.ilike(f"%{search.name}%"))
        if search.status:
            count_query = count_query.where(Dataset.status == search.status.value)
        if search.storage_type:
            count_query = count_query.where(Dataset.storage_type == search.storage_type.value)
        
        total_result = await self.db.execute(count_query)
        total = total_result.scalar()
        
        # Apply pagination
        query = query.order_by(desc(Dataset.data_criacao))
        query = query.offset((search.page - 1) * search.size).limit(search.size)
        
        result = await self.db.execute(query)
        datasets = result.scalars().all()
        
        # Convert to response format
        items = []
        for dataset in datasets:
            items.append(DatasetResponse(
                id=dataset.id,
                name=dataset.name,
                description=dataset.description,
                storage_type=dataset.storage_type,
                refresh_type=dataset.refresh_type,
                refresh_schedule=dataset.refresh_schedule,
                status=dataset.status,
                version=dataset.version,
                properties=dataset.properties,
                sources=[],
                columns=[],
                data_criacao=dataset.data_criacao,
                data_atualizacao=dataset.data_atualizacao
            ))
        
        return SearchResult(total=total, items=items)
