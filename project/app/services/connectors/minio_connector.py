import asyncio
from typing import Dict, Any, Tuple
from minio import Minio
from minio.error import S3Error
import urllib3
from urllib.parse import urlparse
import logging

logger = logging.getLogger(__name__)

async def test_minio_connection(connection_params: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Test MinIO/S3 connection.
    
    Performs a simple connectivity test by checking if the bucket exists.
    Does not create or modify anything - read-only operation.
    
    Args:
        connection_params: Dictionary containing connection parameters
        Expected params:
        - endpoint: MinIO endpoint (e.g., "minio:9000" or "http://minio:9000")
        - access_key: MinIO access key
        - secret_key: MinIO secret key
        - bucket_name: Bucket name to test
        - secure: Optional, use HTTPS (default: False)
        - region: Optional, region (default: "us-east-1")
    
    Returns:
        Tuple of (success, message, details)
        - success=True if connection works and bucket exists
        - success=False if connection fails or bucket doesn't exist
    """
    try:
        # Extract connection parameters
        endpoint = connection_params.get("endpoint")
        access_key = connection_params.get("access_key")
        secret_key = connection_params.get("secret_key")
        bucket_name = connection_params.get("bucket_name")
        secure = connection_params.get("secure", False)
        region = connection_params.get("region", "us-east-1")
        
        # Validate required parameters
        if not endpoint:
            return False, "Missing required parameter: endpoint", {}
        if not access_key:
            return False, "Missing required parameter: access_key", {}
        if not secret_key:
            return False, "Missing required parameter: secret_key", {}
        if not bucket_name:
            return False, "Missing required parameter: bucket_name", {}
        
        # Parse endpoint to remove protocol if present
        parsed_endpoint = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
        clean_endpoint = parsed_endpoint.netloc if parsed_endpoint.netloc else parsed_endpoint.path
        
        # If secure is specified in the endpoint URL, override the parameter
        if parsed_endpoint.scheme == "https":
            secure = True
        elif parsed_endpoint.scheme == "http":
            secure = False
        
        # Disable SSL warnings for development
        if not secure:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Create MinIO client
        client = Minio(
            endpoint=clean_endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region
        )
        
        # Test connection by checking if bucket exists
        bucket_exists = await asyncio.to_thread(client.bucket_exists, bucket_name)
        
        if bucket_exists:
            return True, f"Connection successful. Bucket '{bucket_name}' exists and is accessible.", {
                "bucket_exists": True,
                "bucket_name": bucket_name,
                "endpoint": clean_endpoint,
                "secure": secure,
                "region": region
            }
        else:
            return False, f"Connection successful but bucket '{bucket_name}' does not exist.", {
                "bucket_exists": False,
                "bucket_name": bucket_name,
                "endpoint": clean_endpoint,
                "secure": secure,
                "region": region,
                "note": "Bucket not found - you may need to create it manually"
            }
                    
    except S3Error as e:
        error_msg = f"MinIO S3 error: {e.code} - {e.message}"
        logger.error(f"MinIO connection test failed: {error_msg}")
        return False, error_msg, {
            "error_code": e.code,
            "error_message": e.message,
            "endpoint": connection_params.get("endpoint"),
            "bucket_name": connection_params.get("bucket_name")
        }
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"MinIO connection test failed: {error_msg}")
        return False, error_msg, {
            "error": str(e),
            "endpoint": connection_params.get("endpoint"),
            "bucket_name": connection_params.get("bucket_name")
        }
