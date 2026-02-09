import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import and_, or_, func, desc
from fastapi import HTTPException, status
from minio import Minio
from minio.error import S3Error
import os

from ...database.models.core import Dataset
from ...database.models.storage import DatasetStorage
from ...api.schemas.dataset_schemas import (
    DatasetImageResponse, 
    DatasetImageMetadata, 
    SearchDatasetImages,
    DatasetImageStats
)
from ...api.schemas.search import SearchResult

logger = logging.getLogger(__name__)

class DatasetImageService:
    """Service for managing dataset images and generating presigned URLs"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        # Initialize MinIO configuration
        self.minio_config = {
            'endpoint': os.getenv('MINIO_ENDPOINT', 'localhost:9000'),
            'access_key': os.getenv('MINIO_ACCESS_KEY', 'minio'),
            'secret_key': os.getenv('MINIO_SECRET_KEY', 'minio123'),
            'secure': os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        }
        self.minio_client = None
        self._initialize_minio()
    
    def _initialize_minio(self):
        """Initialize MinIO client"""
        try:
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
            
            logger.info("DatasetImageService MinIO client initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize MinIO client: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to initialize storage service: {str(e)}"
            )
    
    async def get_dataset_images_paginated(
        self, 
        dataset_id: int, 
        search_params: SearchDatasetImages
    ) -> SearchResult[DatasetImageResponse]:
        """Get paginated list of dataset images with presigned URLs"""
        try:
            # First, validate that the dataset exists and get its storage info
            dataset = await self._get_dataset_with_storage(dataset_id)
            if not dataset:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Dataset with id {dataset_id} not found"
                )
            
            # Get the storage information
            storage_info = await self._get_dataset_storage(dataset_id)
            if not storage_info or storage_info.storage_type != 'copy_to_minio':
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Dataset must be stored in MinIO to access images"
                )
            
            # Extract bucket name from storage location
            bucket_name = self._extract_bucket_name(storage_info.storage_location)
            if not bucket_name:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Storage location not configured for dataset"
                )
            
            # Check if bucket exists
            if not await asyncio.to_thread(self.minio_client.bucket_exists, bucket_name):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Storage bucket '{bucket_name}' not found"
                )
            
            # List images in the bucket with pagination
            images_data = await self._list_images_in_bucket(
                bucket_name, 
                search_params
            )
            
            # Generate presigned URLs for the images
            image_responses = []
            for image_data in images_data['items']:
                try:
                    presigned_url = await self._generate_presigned_url(
                        bucket_name, 
                        image_data['object_name']
                    )
                    
                    # Create metadata
                    metadata = DatasetImageMetadata(
                        image_id=image_data['object_name'].split('/')[-1],
                        image_name=image_data['object_name'].split('/')[-1],
                        file_path=image_data['object_name'],
                        file_size=image_data.get('size'),
                        content_type=image_data.get('content_type', 'image/jpeg'),
                        data_criacao=image_data.get('last_modified'),
                        data_atualizacao=image_data.get('last_modified'),
                        metadata=image_data.get('metadata', {})
                    )
                    
                    # Create response
                    image_response = DatasetImageResponse(
                        image_id=metadata.image_id,
                        image_name=metadata.image_name,
                        presigned_url=presigned_url,
                        expires_in=3600,  # 1 hour
                        metadata=metadata
                    )
                    
                    image_responses.append(image_response)
                    
                except Exception as e:
                    logger.warning(f"Failed to generate presigned URL for {image_data['object_name']}: {e}")
                    continue
            
            return SearchResult(
                total=images_data['total'],
                items=image_responses
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting paginated dataset images: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error retrieving dataset images: {str(e)}"
            )
    
    def _extract_bucket_name(self, storage_location: str) -> str:
        """Extract bucket name from storage location, removing s3a:// or s3:// prefixes"""
        if not storage_location:
            return ""
        
        # Remove s3a:// or s3:// prefix if present
        if storage_location.startswith('s3a://'):
            bucket_name = storage_location[6:]  # Remove 's3a://'
        elif storage_location.startswith('s3://'):
            bucket_name = storage_location[5:]  # Remove 's3://'
        else:
            bucket_name = storage_location
        
        # Remove any trailing path (keep only bucket name)
        if '/' in bucket_name:
            bucket_name = bucket_name.split('/')[0]
        
        return bucket_name
    
    async def _get_dataset_with_storage(self, dataset_id: int) -> Optional[Dataset]:
        """Get dataset information"""
        query = select(Dataset).where(Dataset.id == dataset_id)
        result = await self.db.execute(query)
        return result.scalar_one_or_none()
    
    async def _get_dataset_storage(self, dataset_id: int) -> Optional[DatasetStorage]:
        """Get dataset storage information"""
        query = select(DatasetStorage).where(DatasetStorage.dataset_id == dataset_id)
        result = await self.db.execute(query)
        return result.scalar_one_or_none()
    
    async def _list_images_in_bucket(
        self, 
        bucket_name: str, 
        search_params: SearchDatasetImages
    ) -> Dict[str, Any]:
        """List images in MinIO bucket with filtering and pagination"""
        try:
            # Define image extensions to filter
            image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp'}
            
            # Get all objects from the bucket (focusing on images folder if it exists)
            objects_generator = await asyncio.to_thread(
                lambda: list(self.minio_client.list_objects(
                    bucket_name, 
                    prefix='images/',  # Look specifically in images folder
                    recursive=True
                ))
            )
            
            # Filter objects
            all_objects = []
            for obj in objects_generator:
                # Filter by file extension (images only)
                obj_name_lower = obj.object_name.lower()
                if any(obj_name_lower.endswith(ext) for ext in image_extensions):
                    content_type = self._get_content_type_from_extension(obj.object_name)
                    
                    # Apply search filters
                    if search_params.image_name:
                        if search_params.image_name.lower() not in obj_name_lower:
                            continue
                    
                    if search_params.content_type:
                        if search_params.content_type.lower() not in content_type.lower():
                            continue
                    
                    if search_params.date_from:
                        if obj.last_modified and obj.last_modified < search_params.date_from:
                            continue
                    
                    if search_params.date_to:
                        if obj.last_modified and obj.last_modified > search_params.date_to:
                            continue
                    
                    all_objects.append({
                        'object_name': obj.object_name,
                        'size': obj.size,
                        'last_modified': obj.last_modified,
                        'etag': obj.etag,
                        'content_type': content_type
                    })
            
            # Sort by last_modified (newest first)
            all_objects.sort(key=lambda x: x.get('last_modified') or datetime.min, reverse=True)
            
            # Apply pagination
            total = len(all_objects)
            start_idx = (search_params.page - 1) * search_params.size
            end_idx = start_idx + search_params.size
            paginated_objects = all_objects[start_idx:end_idx]
            
            return {
                'total': total,
                'items': paginated_objects
            }
            
        except Exception as e:
            logger.error(f"Error listing images in bucket {bucket_name}: {e}")
            raise
    
    def _get_content_type_from_extension(self, filename: str) -> str:
        """Get content type based on file extension"""
        extension = filename.lower().split('.')[-1] if '.' in filename else ''
        content_type_map = {
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'gif': 'image/gif',
            'bmp': 'image/bmp',
            'tiff': 'image/tiff',
            'webp': 'image/webp'
        }
        return content_type_map.get(extension, 'application/octet-stream')
    
    async def _generate_presigned_url(self, bucket_name: str, object_name: str) -> str:
        """Generate presigned URL for object"""
        try:
            # Generate presigned URL valid for 1 hour
            presigned_url = await asyncio.to_thread(
                self.minio_client.presigned_get_object,
                bucket_name,
                object_name,
                expires=timedelta(hours=1)
            )
            
            return presigned_url
            
        except Exception as e:
            logger.error(f"Error generating presigned URL for {object_name}: {e}")
            raise
    
    async def get_single_image_presigned_url(
        self, 
        dataset_id: int, 
        image_path: str
    ) -> DatasetImageResponse:
        """Get presigned URL for a specific image"""
        try:
            # Validate dataset and get storage info
            dataset = await self._get_dataset_with_storage(dataset_id)
            if not dataset:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Dataset with id {dataset_id} not found"
                )
            
            storage_info = await self._get_dataset_storage(dataset_id)
            if not storage_info or storage_info.storage_type != 'copy_to_minio':
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Dataset must be stored in MinIO to access images"
                )
            
            bucket_name = self._extract_bucket_name(storage_info.storage_location)
            
            # Check if object exists
            try:
                await asyncio.to_thread(
                    self.minio_client.stat_object,
                    bucket_name,
                    image_path
                )
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Image '{image_path}' not found"
                    )
                raise
            
            # Generate presigned URL
            presigned_url = await self._generate_presigned_url(bucket_name, image_path)
            
            # Get object metadata
            stat = await asyncio.to_thread(
                self.minio_client.stat_object,
                bucket_name,
                image_path
            )
            
            # Create metadata
            metadata = DatasetImageMetadata(
                image_id=image_path.split('/')[-1],
                image_name=image_path.split('/')[-1],
                file_path=image_path,
                file_size=stat.size,
                content_type=self._get_content_type_from_extension(image_path),
                data_criacao=stat.last_modified,
                data_atualizacao=stat.last_modified,
                metadata={}
            )
            
            return DatasetImageResponse(
                image_id=metadata.image_id,
                image_name=metadata.image_name,
                presigned_url=presigned_url,
                expires_in=3600,
                metadata=metadata
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting single image presigned URL: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error retrieving image: {str(e)}"
            )
