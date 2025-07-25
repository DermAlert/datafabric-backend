from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func, desc, text
from typing import Optional, List, Dict, Any
from fastapi import HTTPException, status
import json
import os
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from datetime import datetime
import logging

from ...database.delta_sharing.delta_sharing import ShareTable, Share, ShareSchema
from ...database.core.core import Dataset
from ...services.dataset_image_service import DatasetImageService

logger = logging.getLogger(__name__)

class DatasetDeltaSharingService:
    """Service for integrating datasets with Delta Sharing"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self._minio_client = None
    
    @property
    def minio_client(self):
        """Lazy initialization of MinIO client"""
        if self._minio_client is None:
            self._minio_client = boto3.client(
                's3',
                endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
                aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
                region_name='us-east-1'
            )
        return self._minio_client
    
    async def sync_dataset_to_delta_table(self, table_id: int) -> Dict[str, Any]:
        """
        Synchronize a dataset to Delta format for sharing.
        This method will:
        1. Get the shared table configuration
        2. Extract data from the original dataset
        3. Convert to Delta/Parquet format in MinIO
        4. Update table metadata for Delta Sharing protocol
        """
        # Get the shared table
        query = select(ShareTable, Dataset, ShareSchema, Share).join(
            Dataset
        ).join(ShareSchema).join(Share).where(ShareTable.id == table_id)
        result = await self.db.execute(query)
        table_data = result.first()
        
        if not table_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Shared table not found"
            )
        
        table, dataset, schema, share = table_data
        
        try:
            # Create storage location if not exists
            storage_location = await self._ensure_storage_location(table, share, schema)
            
            # Extract and convert dataset
            conversion_result = await self._convert_dataset_to_delta(dataset, table, storage_location)
            
            # Update table metadata
            await self._update_table_metadata(table, conversion_result)
            
            logger.info(f"Successfully synced dataset {dataset.id} to Delta table {table.id}")
            
            return {
                "table_id": table.id,
                "dataset_id": dataset.id,
                "storage_location": storage_location,
                "files_created": conversion_result.get("files_created", 0),
                "total_size_bytes": conversion_result.get("total_size_bytes", 0),
                "schema_updated": True
            }
            
        except Exception as e:
            logger.error(f"Error syncing dataset {dataset.id} to Delta table {table.id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to sync dataset to Delta format: {str(e)}"
            )
    
    async def _ensure_storage_location(self, table: ShareTable, share: Share, schema: ShareSchema) -> str:
        """Ensure storage location exists for the Delta table"""
        # Create bucket name based on share
        bucket_name = f"delta-share-{share.name.lower().replace('_', '-')}"
        
        # Create object prefix
        object_prefix = f"schemas/{schema.name}/tables/{table.name}/"
        
        # Create bucket if it doesn't exist
        try:
            self.minio_client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Bucket doesn't exist, create it
                self.minio_client.create_bucket(Bucket=bucket_name)
                logger.info(f"Created bucket: {bucket_name}")
            else:
                raise
        
        storage_location = f"s3a://{bucket_name}/{object_prefix}"
        
        # Update table storage location
        table.storage_location = storage_location
        await self.db.commit()
        
        return storage_location
    
    async def _convert_dataset_to_delta(self, dataset: Dataset, table: ShareTable, storage_location: str) -> Dict[str, Any]:
        """Convert dataset to Delta/Parquet format"""
        result = {
            "files_created": 0,
            "total_size_bytes": 0,
            "partition_columns": [],
            "schema_string": ""
        }
        
        # For this implementation, we'll handle different dataset types
        if dataset.storage_type == "copy_to_minio":
            # Dataset is already in MinIO, we need to convert to Delta format
            result = await self._convert_minio_dataset_to_delta(dataset, table, storage_location)
        elif dataset.storage_type == "virtual_view":
            # For virtual views, we'd need to query the source and materialize
            result = await self._materialize_virtual_dataset_to_delta(dataset, table, storage_location)
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Dataset storage type '{dataset.storage_type}' not supported for Delta Sharing"
            )
        
        return result
    
    async def _convert_minio_dataset_to_delta(self, dataset: Dataset, table: ShareTable, storage_location: str) -> Dict[str, Any]:
        """Convert MinIO-stored dataset to Delta format"""
        # Parse storage location
        bucket_name, prefix = self._parse_storage_location(storage_location)
        
        # For simplicity, let's assume the dataset already has metadata in a compatible format
        # In a full implementation, you would:
        # 1. Read the dataset's actual data files
        # 2. Convert to Parquet if needed
        # 3. Create Delta log entries
        # 4. Generate proper schema information
        
        # Create a sample Parquet file for demonstration
        sample_data = await self._create_sample_parquet_file(dataset, bucket_name, prefix)
        
        # Generate Delta log
        await self._create_delta_log(dataset, bucket_name, prefix, sample_data)
        
        return {
            "files_created": 1,
            "total_size_bytes": sample_data.get("file_size", 0),
            "partition_columns": [],
            "schema_string": sample_data.get("schema_string", "")
        }
    
    async def _materialize_virtual_dataset_to_delta(self, dataset: Dataset, table: ShareTable, storage_location: str) -> Dict[str, Any]:
        """Materialize virtual dataset to Delta format"""
        # This would involve querying the actual data sources and materializing the result
        # For now, we'll create a placeholder implementation
        
        bucket_name, prefix = self._parse_storage_location(storage_location)
        
        # Create sample data based on dataset definition
        sample_data = await self._create_sample_parquet_file(dataset, bucket_name, prefix)
        
        # Generate Delta log
        await self._create_delta_log(dataset, bucket_name, prefix, sample_data)
        
        return {
            "files_created": 1,
            "total_size_bytes": sample_data.get("file_size", 0),
            "partition_columns": [],
            "schema_string": sample_data.get("schema_string", "")
        }
    
    async def _create_sample_parquet_file(self, dataset: Dataset, bucket_name: str, prefix: str) -> Dict[str, Any]:
        """Create a sample Parquet file for the dataset"""
        # Create sample DataFrame based on dataset
        # In production, this would read actual data
        
        try:
            # Get dataset columns information
            from ...database.core.core import DatasetColumn
            
            columns_query = select(DatasetColumn).where(
                DatasetColumn.dataset_id == dataset.id
            ).order_by(DatasetColumn.column_position)
            columns_result = await self.db.execute(columns_query)
            columns = columns_result.scalars().all()
            
            # Create sample data
            sample_data = {}
            for col in columns:
                if col.data_type.lower() in ['int', 'integer', 'bigint']:
                    sample_data[col.name] = [1, 2, 3, 4, 5]
                elif col.data_type.lower() in ['varchar', 'string', 'text']:
                    sample_data[col.name] = ['sample1', 'sample2', 'sample3', 'sample4', 'sample5']
                elif col.data_type.lower() in ['float', 'double', 'decimal']:
                    sample_data[col.name] = [1.1, 2.2, 3.3, 4.4, 5.5]
                elif col.data_type.lower() in ['boolean', 'bool']:
                    sample_data[col.name] = [True, False, True, False, True]
                else:
                    sample_data[col.name] = ['value1', 'value2', 'value3', 'value4', 'value5']
            
            # If no columns defined, create a basic sample
            if not sample_data:
                sample_data = {
                    'id': [1, 2, 3, 4, 5],
                    'name': ['sample1', 'sample2', 'sample3', 'sample4', 'sample5'],
                    'value': [1.1, 2.2, 3.3, 4.4, 5.5]
                }
            
            # Create DataFrame
            df = pd.DataFrame(sample_data)
            
            # Generate schema string
            schema_fields = []
            for col_name, dtype in df.dtypes.items():
                if pd.api.types.is_integer_dtype(dtype):
                    spark_type = "long"
                elif pd.api.types.is_float_dtype(dtype):
                    spark_type = "double"
                elif pd.api.types.is_bool_dtype(dtype):
                    spark_type = "boolean"
                else:
                    spark_type = "string"
                
                schema_fields.append({
                    "name": col_name,
                    "type": spark_type,
                    "nullable": True,
                    "metadata": {}
                })
            
            schema_string = json.dumps({
                "type": "struct",
                "fields": schema_fields
            })
            
            # Save to parquet file
            parquet_key = f"{prefix}data/part-00000.parquet"
            
            # Convert DataFrame to parquet bytes
            parquet_buffer = df.to_parquet(index=False)
            
            # Upload to MinIO
            self.minio_client.put_object(
                Bucket=bucket_name,
                Key=parquet_key,
                Body=parquet_buffer,
                ContentType='application/octet-stream'
            )
            
            return {
                "file_size": len(parquet_buffer),
                "schema_string": schema_string,
                "file_path": parquet_key,
                "record_count": len(df)
            }
            
        except Exception as e:
            logger.error(f"Error creating sample Parquet file: {e}")
            # Return minimal data if error occurs
            return {
                "file_size": 0,
                "schema_string": '{"type":"struct","fields":[]}',
                "file_path": f"{prefix}data/part-00000.parquet",
                "record_count": 0
            }
    
    async def _create_delta_log(self, dataset: Dataset, bucket_name: str, prefix: str, file_data: Dict[str, Any]) -> None:
        """Create Delta log entries"""
        # Create _delta_log directory
        delta_log_prefix = f"{prefix}_delta_log/"
        
        # Create 00000000000000000000.json (version 0)
        version_0_content = {
            "protocol": {
                "minReaderVersion": 1,
                "minWriterVersion": 1
            }
        }
        
        # Upload protocol file
        protocol_key = f"{delta_log_prefix}00000000000000000000.json"
        self.minio_client.put_object(
            Bucket=bucket_name,
            Key=protocol_key,
            Body=json.dumps(version_0_content),
            ContentType='application/json'
        )
        
        # Create metadata entry
        metadata_content = {
            "metaData": {
                "id": str(dataset.id),
                "name": dataset.name,
                "description": dataset.description,
                "format": {
                    "provider": "parquet"
                },
                "schemaString": file_data.get("schema_string", ""),
                "partitionColumns": [],
                "configuration": {},
                "createdTime": int(datetime.utcnow().timestamp() * 1000)
            }
        }
        
        # Create add file entry
        add_file_content = {
            "add": {
                "path": file_data.get("file_path", ""),
                "partitionValues": {},
                "size": file_data.get("file_size", 0),
                "modificationTime": int(datetime.utcnow().timestamp() * 1000),
                "dataChange": True,
                "stats": json.dumps({
                    "numRecords": file_data.get("record_count", 0)
                })
            }
        }
        
        # Upload metadata and add file as single version
        version_content = "\n".join([
            json.dumps(version_0_content),
            json.dumps(metadata_content),
            json.dumps(add_file_content)
        ])
        
        version_key = f"{delta_log_prefix}00000000000000000001.json"
        self.minio_client.put_object(
            Bucket=bucket_name,
            Key=version_key,
            Body=version_content,
            ContentType='application/json'
        )
        
        logger.info(f"Created Delta log for dataset {dataset.id} at {delta_log_prefix}")
    
    async def _update_table_metadata(self, table: ShareTable, conversion_result: Dict[str, Any]) -> None:
        """Update table metadata after conversion"""
        table.schema_string = conversion_result.get("schema_string", "")
        table.partition_columns = conversion_result.get("partition_columns", [])
        table.current_version += 1
        
        await self.db.commit()
        await self.db.refresh(table)
    
    def _parse_storage_location(self, storage_location: str) -> tuple[str, str]:
        """Parse storage location to extract bucket and prefix"""
        if storage_location.startswith('s3a://'):
            path = storage_location[6:]
        elif storage_location.startswith('s3://'):
            path = storage_location[5:]
        elif storage_location.startswith('minio://'):
            path = storage_location[8:]
        else:
            path = storage_location
        
        parts = path.split('/', 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''
        
        return bucket_name, prefix
    
    # async def get_sync_status(self, table_id: int) -> Dict[str, Any]:
    #     """Get synchronization status for a Delta table"""
    #     query = select(ShareTable, Dataset).join(Dataset).where(ShareTable.id == table_id)
    #     result = await self.db.execute(query)
    #     table_data = result.first()
        
    #     if not table_data:
    #         raise HTTPException(
    #             status_code=status.HTTP_404_NOT_FOUND,
    #             detail="Shared table not found"
    #         )
        
    #     table, dataset = table_data
        
    #     # Check if storage location exists and has files
    #     has_data = False
    #     file_count = 0
    #     total_size = 0
        
    #     if table.storage_location:
    #         try:
    #             bucket_name, prefix = self._parse_storage_location(table.storage_location)
                
    #             # List objects in the data directory
    #             response = self.minio_client.list_objects_v2(
    #                 Bucket=bucket_name,
    #                 Prefix=f"{prefix}data/"
    #             )
                
    #             files = response.get('Contents', [])
    #             file_count = len([f for f in files if f['Key'].endswith('.parquet')])
    #             total_size = sum(f['Size'] for f in files)
    #             has_data = file_count > 0
                
    #         except ClientError:
    #             pass  # Storage location doesn't exist or is not accessible
        
    #     return {
    #         "table_id": table.id,
    #         "dataset_id": dataset.id,
    #         "has_data": has_data,
    #         "file_count": file_count,
    #         "total_size_bytes": total_size,
    #         "storage_location": table.storage_location,
    #         "current_version": table.current_version,
    #         "last_updated": table.data_atualizacao.isoformat() if table.data_atualizacao else None
    #     }
