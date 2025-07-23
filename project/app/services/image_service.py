"""
Image Service
Serviço responsável por processar colunas de imagem e baixar imagens do MinIO
"""

import asyncio
import base64
import logging
from typing import Dict, Any, List, Optional, Tuple
from io import BytesIO
import pandas as pd
from minio import Minio
from minio.error import S3Error
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database.core.core import DataConnection, ConnectionType

logger = logging.getLogger(__name__)


class ImageService:
    """Service for handling image operations"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def process_image_columns_in_dataframe(
        self, 
        df: pd.DataFrame, 
        image_columns_info: List[Dict[str, Any]],
        destination_minio_client: Optional[Minio] = None,
        destination_bucket: Optional[str] = None,
        save_images_to_destination: bool = False
    ) -> pd.DataFrame:
        """
        Process image columns in dataframe by downloading images from MinIO and adding as base64
        
        Args:
            df: DataFrame with image path columns
            image_columns_info: List of dicts with column_name, image_connection_id info
            destination_minio_client: Optional MinIO client to save images to dataset bucket
            destination_bucket: Optional destination bucket name
            save_images_to_destination: Whether to save images to destination bucket
        
        Returns:
            DataFrame with additional base64 image columns and updated image paths
        """
        try:
            if not image_columns_info:
                return df
            
            df_processed = df.copy()
            
            for img_col_info in image_columns_info:
                column_name = img_col_info['column_name']
                image_connection_id = img_col_info['image_connection_id']
                
                if column_name not in df_processed.columns:
                    logger.warning(f"Image column {column_name} not found in dataframe")
                    continue
                
                logger.info(f"Processing image column: {column_name}")
                
                # Get MinIO connection details
                minio_client = await self._get_minio_client(image_connection_id)
                if not minio_client:
                    logger.error(f"Could not create MinIO client for connection {image_connection_id}")
                    continue
                
                # Process each image path in the column
                base64_images = []
                new_image_paths = []  # Store new paths if copying to destination
                
                for idx, image_path in enumerate(df_processed[column_name]):
                    try:
                        if pd.isna(image_path) or not image_path:
                            base64_images.append(None)
                            new_image_paths.append(image_path)  # Keep original path
                            continue
                        
                        # Download and encode image
                        base64_img, image_data = await self._download_and_encode_image_with_data(
                            minio_client, str(image_path), image_connection_id
                        )
                        base64_images.append(base64_img)
                        
                        # Copy image to destination bucket if requested
                        new_path = image_path  # Default to original path
                        if save_images_to_destination and destination_minio_client and destination_bucket and image_data:
                            try:
                                new_path = await self._copy_image_to_destination(
                                    image_data, 
                                    str(image_path), 
                                    destination_minio_client, 
                                    destination_bucket
                                )
                                logger.debug(f"Copied image to destination: {new_path}")
                            except Exception as copy_error:
                                logger.warning(f"Failed to copy image {image_path} to destination: {copy_error}")
                                # Keep original path if copy fails
                        
                        new_image_paths.append(new_path)
                        
                        if (idx + 1) % 10 == 0:  # Log progress every 10 images
                            logger.info(f"Processed {idx + 1} images for column {column_name}")
                    
                    except Exception as e:
                        logger.error(f"Error processing image {image_path}: {e}")
                        base64_images.append(None)
                        new_image_paths.append(image_path)  # Keep original path
                
                # Add base64 column to dataframe
                base64_column_name = f"{column_name}_base64"
                df_processed[base64_column_name] = base64_images
                
                # Update original column with new paths if images were copied
                if save_images_to_destination and destination_bucket:
                    df_processed[column_name] = new_image_paths
                    logger.info(f"Updated {len([p for p in new_image_paths if p and p != image_path])} image paths to destination bucket")
                
                logger.info(f"Added {len([img for img in base64_images if img])} base64 images to column {base64_column_name}")
            
            return df_processed
            
        except Exception as e:
            logger.error(f"Error processing image columns: {e}")
            raise
    
    async def _get_minio_client(self, connection_id: int) -> Optional[Minio]:
        """Get MinIO client from connection ID"""
        try:
            # Get connection details
            connection_query = select(DataConnection, ConnectionType).join(
                ConnectionType, DataConnection.connection_type_id == ConnectionType.id
            ).where(
                DataConnection.id == connection_id,
                ConnectionType.name.ilike('%minio%')  # Ensure it's a MinIO connection
            )
            
            connection_result = await self.db.execute(connection_query)
            connection_data = connection_result.first()
            
            if not connection_data:
                logger.error(f"MinIO connection {connection_id} not found")
                return None
            
            connection, connection_type = connection_data
            
            # Extract MinIO connection parameters
            params = connection.connection_params
            endpoint = params.get('endpoint')
            access_key = params.get('access_key')
            secret_key = params.get('secret_key')
            secure = params.get('secure', False)
            region = params.get('region', 'us-east-1')  # Default region for S3 compatibility

            print(f"  ✓ connection_id: '{connection_id}'")
            print(f"  ✓ endpoint: '{endpoint}'")
            print(f"  ✓ access_key: '{access_key}'")
            print(f"  ✓ secret_key: '{secret_key}'")
            print(f"  ✓ secure: '{secure}'")
            print(f"  ✓ region: '{region}'")

            if not all([endpoint, access_key, secret_key]):
                logger.error(f"Missing MinIO connection parameters for connection {connection_id}")
                return None
            
            # Check if this is an S3 endpoint (AWS S3 or S3-compatible)
            is_s3_endpoint = (
                's3.amazonaws.com' in endpoint or 
                's3.' in endpoint and '.amazonaws.com' in endpoint or
                endpoint.startswith('s3.') or
                params.get('is_s3_compatible', False)
            )
            
            # Remove protocol if present for MinIO client (it will add it back)
            if endpoint.startswith('http://'):
                endpoint = endpoint[7:]
                secure = False
            elif endpoint.startswith('https://'):
                endpoint = endpoint[8:]
                secure = True
            
            # For S3 endpoints, use s3.amazonaws.com if it's AWS
            if is_s3_endpoint and 'amazonaws.com' not in endpoint:
                # This might be a custom S3-compatible endpoint
                logger.info(f"Using S3-compatible endpoint: {endpoint}")
            
            # Create MinIO client (works with S3 too)
            minio_client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
                region=region
            )

            print(f"  ✓ minio_client: '{minio_client}'")
            
            logger.info(f"Created MinIO/S3 client for connection {connection_id}:")
            logger.info(f"  - Endpoint: {endpoint}")
            logger.info(f"  - Access Key: {access_key[:4]}***{access_key[-4:] if len(access_key) > 8 else '***'}")
            logger.info(f"  - Secure: {secure}")
            logger.info(f"  - Region: {region}")
            
            # Test basic connectivity
            try:
                logger.info("Testing MinIO/S3 client connectivity...")
                buckets = list(minio_client.list_buckets())
                logger.info(f"Successfully connected! Found {len(buckets)} buckets:")
                for bucket in buckets[:5]:  # Show first 5 buckets
                    logger.info(f"  - {bucket.name} (created: {bucket.creation_date})")
                if len(buckets) > 5:
                    logger.info(f"  ... and {len(buckets) - 5} more buckets")
            except Exception as conn_test_error:
                logger.warning(f"Connectivity test failed: {conn_test_error}")
                # Don't return None here, still try to use the client
            
            return minio_client
            
        except Exception as e:
            logger.error(f"Error creating MinIO client for connection {connection_id}: {e}")
            return None
    
    async def _get_bucket_from_connection(self, connection_id: int) -> Optional[str]:
        """Get bucket name from MinIO connection parameters"""
        try:
            # Get connection details
            connection_query = select(DataConnection).where(DataConnection.id == connection_id)
            connection_result = await self.db.execute(connection_query)
            connection = connection_result.scalars().first()
            
            if not connection:
                logger.error(f"Connection {connection_id} not found")
                return None
            
            params = connection.connection_params
            logger.debug(f"Connection {connection_id} params keys: {list(params.keys()) if params else 'None'}")
            
            # Try to get bucket from different possible parameter names
            bucket_name = (
                params.get('bucket_name') or 
                params.get('bucket') or 
                params.get('default_bucket') or
                params.get('s3_bucket')
            )
            
            if bucket_name:
                logger.info(f"Found bucket '{bucket_name}' for connection {connection_id}")
                return bucket_name
            else:
                logger.warning(f"No bucket specified in connection {connection_id} parameters. Available keys: {list(params.keys()) if params else 'None'}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting bucket from connection {connection_id}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    
    async def _download_and_encode_image_with_data(
        self, 
        minio_client: Minio, 
        image_path: str, 
        image_connection_id: Optional[int] = None
    ) -> Tuple[Optional[str], Optional[bytes]]:
        """Download image from MinIO and encode as base64, returning both base64 and raw data"""
        try:
            # Parse bucket and object path from image_path
            # Handle different formats: s3://bucket/path, bucket/path, /bucket/path
            is_s3_url = image_path.startswith('s3://')
            
            if is_s3_url:
                # Remove s3:// prefix
                path_without_protocol = image_path[5:]
                path_parts = path_without_protocol.split('/', 1)
                bucket_name = path_parts[0]
                object_name = path_parts[1] if len(path_parts) > 1 else ""
                logger.debug(f"Processing S3 URL: {image_path}")
            else:
                # Handle regular paths - need to get bucket from connection
                if image_connection_id:
                    bucket_name = await self._get_bucket_from_connection(image_connection_id)
                    if not bucket_name:
                        logger.error(f"Could not determine bucket for connection {image_connection_id}")
                        return None, None
                    object_name = image_path.strip('/')
                    logger.debug(f"Processing regular path: {image_path} with bucket: {bucket_name}")
                else:
                    # Fallback to old parsing method
                    path_parts = image_path.strip('/').split('/', 1)
                    if len(path_parts) < 2:
                        logger.error(f"Invalid image path format: {image_path}")
                        return None, None
                    bucket_name = path_parts[0]
                    object_name = path_parts[1]
                    logger.debug(f"Processing regular path (fallback): {image_path}")
            
            if not bucket_name or not object_name:
                logger.error(f"Invalid image path format: {image_path}")
                return None, None

            print(f"  ✓ Bucket: '{bucket_name}'")
            print(f"  ✓ Object: '{object_name}'")
            
            # Validate bucket name for local MinIO (S3 has different rules)
            if not is_s3_url and (not bucket_name or not self._is_valid_bucket_name(bucket_name)):
                logger.error(f"Invalid MinIO bucket name '{bucket_name}' from path: {image_path}")
                return None, None
            
            logger.debug(f"Parsed image path '{image_path}' -> bucket: '{bucket_name}', object: '{object_name}'")
            
            # Test bucket accessibility first
            try:
                logger.info(f"Testing bucket access for: {bucket_name}")
                
                # First, try to list the bucket (check if it exists and we have access)
                try:
                    bucket_exists = minio_client.bucket_exists(bucket_name)
                    logger.info(f"Bucket '{bucket_name}' exists: {bucket_exists}")
                    
                    if not bucket_exists:
                        logger.error(f"Bucket '{bucket_name}' does not exist or is not accessible")
                        return None, None
                        
                except Exception as bucket_error:
                    logger.error(f"Error checking bucket existence for '{bucket_name}': {bucket_error}")
                    logger.error(f"MinIO client endpoint: {minio_client._base_url}")
                    return None, None
                
                # Try to list objects in the bucket (test permissions)
                try:
                    objects = list(minio_client.list_objects(bucket_name, prefix=object_name[:10]))
                    logger.info(f"Successfully listed objects in bucket '{bucket_name}'. Found {len(objects)} objects with prefix")
                except Exception as list_error:
                    logger.warning(f"Could not list objects in bucket '{bucket_name}': {list_error}")
                
            except Exception as access_error:
                logger.error(f"Error testing bucket access for '{bucket_name}': {access_error}")
                return None, None
            
            # Check if object exists
            try:
                stat_result = minio_client.stat_object(bucket_name, object_name)
                logger.info(f"Object stat successful for '{object_name}': size={stat_result.size}, etag={stat_result.etag}")
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    logger.warning(f"Image not found: {image_path} (bucket: {bucket_name}, object: {object_name})")
                    
                    # Try to find similar objects
                    try:
                        similar_objects = list(minio_client.list_objects(bucket_name, prefix=object_name[:15]))
                        if similar_objects:
                            logger.info(f"Found {len(similar_objects)} similar objects:")
                            for obj in similar_objects:
                                logger.info(f"  - {obj.object_name}")
                        else:
                            logger.info(f"No objects found with prefix '{object_name[:15]}'")
                    except Exception as search_error:
                        logger.error(f"Error searching for similar objects: {search_error}")
                    
                    return None, None
                elif e.code == 'NoSuchBucket':
                    logger.warning(f"Bucket not found: {bucket_name} for image: {image_path}")
                    return None, None
                else:
                    logger.error(f"S3 error accessing {image_path}: {e}")
                    raise
            
            # Download image
            response = minio_client.get_object(bucket_name, object_name)
            image_data = response.read()
            response.close()
            response.release_conn()
            
            # Encode as base64
            base64_encoded = base64.b64encode(image_data).decode('utf-8')
            
            # Determine MIME type based on file extension
            file_extension = object_name.lower().split('.')[-1]
            mime_type = self._get_mime_type(file_extension)
            
            # Return data URI format and raw data
            return f"data:{mime_type};base64,{base64_encoded}", image_data
            
        except Exception as e:
            logger.error(f"Error downloading/encoding image {image_path}: {e}")
            return None, None
    
    async def _copy_image_to_destination(
        self, 
        image_data: bytes, 
        original_path: str, 
        destination_client: Minio, 
        destination_bucket: str
    ) -> str:
        """Copy image data to destination bucket and return new path"""
        try:
            # Extract filename from original path
            import os
            if original_path.startswith('s3://'):
                # Extract filename from S3 path
                filename = original_path.split('/')[-1]
            else:
                filename = os.path.basename(original_path)
            
            # Create destination path in images folder
            destination_object = f"images/{filename}"
            
            # Create bucket if it doesn't exist
            if not destination_client.bucket_exists(destination_bucket):
                destination_client.make_bucket(destination_bucket)
                logger.info(f"Created destination bucket: {destination_bucket}")
            
            # Upload image data
            destination_client.put_object(
                destination_bucket,
                destination_object,
                BytesIO(image_data),
                length=len(image_data),
                content_type=self._get_mime_type(filename.lower().split('.')[-1])
            )
            
            # Return new S3 path
            new_path = f"s3://{destination_bucket}/{destination_object}"
            logger.debug(f"Successfully copied image to: {new_path}")
            return new_path
            
        except Exception as e:
            logger.error(f"Error copying image to destination: {e}")
            raise

    async def _download_and_encode_image(self, minio_client: Minio, image_path: str, image_connection_id: Optional[int] = None) -> Optional[str]:
        """Download image from MinIO and encode as base64 (legacy method)"""
        base64_result, _ = await self._download_and_encode_image_with_data(minio_client, image_path, image_connection_id)
        return base64_result
    
    def _get_mime_type(self, file_extension: str) -> str:
        """Get MIME type from file extension"""
        mime_types = {
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'gif': 'image/gif',
            'bmp': 'image/bmp',
            'webp': 'image/webp',
            'svg': 'image/svg+xml',
            'tiff': 'image/tiff',
            'tif': 'image/tiff'
        }
        
        return mime_types.get(file_extension, 'image/jpeg')  # Default to JPEG
    
    def _is_valid_bucket_name(self, bucket_name: str) -> bool:
        """Validate MinIO bucket name according to S3 naming rules"""
        if not bucket_name:
            return False
        
        # Check length (3-63 characters)
        if len(bucket_name) < 3 or len(bucket_name) > 63:
            return False
        
        # Check for invalid characters
        import re
        if not re.match(r'^[a-z0-9][a-z0-9\-]*[a-z0-9]$', bucket_name):
            return False
        
        # Check for consecutive periods or hyphens
        if '..' in bucket_name or '--' in bucket_name:
            return False
        
        # Check if it looks like an IP address
        if re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', bucket_name):
            return False
        
        return True
    
    async def get_image_columns_info(self, column_ids: List[int]) -> List[Dict[str, Any]]:
        """Get image column information for the given column IDs"""
        try:
            from ..database.metadata.metadata import ExternalColumn
            
            # Query for image columns
            image_columns_query = select(ExternalColumn).where(
                ExternalColumn.id.in_(column_ids),
                ExternalColumn.is_image_path == True,
                ExternalColumn.image_connection_id.isnot(None)
            )
            
            result = await self.db.execute(image_columns_query)
            image_columns = result.scalars().all()
            
            image_columns_info = []
            for col in image_columns:
                image_columns_info.append({
                    'column_id': col.id,
                    'column_name': col.column_name,
                    'image_connection_id': col.image_connection_id,
                    'table_id': col.table_id
                })
            
            return image_columns_info
            
        except Exception as e:
            logger.error(f"Error getting image columns info: {e}")
            return []
