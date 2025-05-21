import asyncio
from typing import Dict, Any, Tuple, List
from minio import Minio
from minio.error import S3Error

async def test_minio_connection(connection_params: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Test a MinIO/S3 connection.
    
    Args:
        connection_params: Connection parameters including endpoint, access_key, secret_key, secure
        
    Returns:
        Tuple of (success, message, details)
    """
    try:
        # Extract connection parameters with defaults
        endpoint = connection_params.get("endpoint", "localhost:9000")
        access_key = connection_params.get("access_key")
        secret_key = connection_params.get("secret_key")
        secure = connection_params.get("secure", False)
        region = connection_params.get("region")
        
        # Validate required parameters
        if not all([endpoint, access_key, secret_key]):
            missing = []
            if not endpoint:
                missing.append("endpoint")
            if not access_key:
                missing.append("access_key")
            if not secret_key:
                missing.append("secret_key")
                
            return False, f"Missing required connection parameters: {', '.join(missing)}", {}
        
        # Create MinIO client
        client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region
        )
        
        # Test connection by listing buckets
        buckets = client.list_buckets()
        bucket_count = len(buckets)
        
        # Get server information if available
        server_info = {}
        try:
            # MinIO doesn't have a direct API for server info, but we can get some basic info
            # For actual MinIO servers, admin APIs could be used with proper permissions
            server_info = {
                "endpoint": endpoint,
                "secure": secure,
                "region": region if region else "default"
            }
        except Exception as e:
            # If we can't get server info, just continue
            pass
        
        return True, "Connection successful", {
            "bucket_count": bucket_count,
            "server_info": server_info
        }
        
    except S3Error as e:
        return False, f"Connection failed: {str(e)}", {}
    except Exception as e:
        return False, f"Connection failed: {str(e)}", {}
