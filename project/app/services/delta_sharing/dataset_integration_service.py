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
            minio_endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
            endpoint_url = f"http://{minio_endpoint}" if not minio_endpoint.startswith(('http://', 'https://')) else minio_endpoint
            self._minio_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'minio'),
                aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
                region_name='us-east-1'
            )
        return self._minio_client
    
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
