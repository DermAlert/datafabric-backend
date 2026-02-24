import re
import unicodedata
from urllib.parse import quote, unquote

def sanitize_s3_path_component(name: str, max_length: int = 255) -> str:
    """
    Sanitize a string to be used as an S3 path component.
    
    Rules:
    - Removes accents and special characters
    - Replaces spaces and invalid chars with underscores
    - Lowercases for consistency
    - Ensures no leading/trailing underscores
    - Limits length
    
    Args:
        name: The string to sanitize
        max_length: Maximum length of the sanitized string
        
    Returns:
        Sanitized string safe for S3 paths
    """
    if not name:
        return "unnamed"
    
    # Normalize unicode (remove accents)
    normalized = unicodedata.normalize('NFKD', str(name))
    # Remove non-ASCII characters
    ascii_only = normalized.encode('ASCII', 'ignore').decode('ASCII')
    
    # Replace spaces and special chars with underscores
    # Keep alphanumeric, hyphens, and underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_-]+', '_', ascii_only)
    
    # Collapse multiple underscores into one
    sanitized = re.sub(r'_+', '_', sanitized)
    
    # Remove leading/trailing underscores and hyphens
    sanitized = sanitized.strip('_-')
    
    # Lowercase for consistency
    sanitized = sanitized.lower()
    
    # Ensure it's not empty
    if not sanitized:
        sanitized = "unnamed"
    
    # Limit length
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length].rstrip('_-')
    
    return sanitized


def sanitize_s3_path(path: str) -> str:
    """
    Sanitize an entire S3 path, preserving the structure.
    
    Args:
        path: Full S3 path (e.g., "s3a://bucket/folder/part_name/")
        
    Returns:
        Sanitized path with all components cleaned
    """
    # Handle s3a:// or s3:// prefix
    if path.startswith('s3a://'):
        prefix = 's3a://'
        rest = path[6:]
    elif path.startswith('s3://'):
        prefix = 's3://'
        rest = path[5:]
    else:
        prefix = ''
        rest = path
    
    # Split into components
    parts = rest.split('/')
    
    # Sanitize each component (except empty parts from trailing slashes)
    sanitized_parts = [
        sanitize_s3_path_component(part) if part else ''
        for part in parts
    ]
    
    # Rejoin
    sanitized_rest = '/'.join(sanitized_parts)
    
    # Remove trailing slashes if they were in the original
    if path.endswith('/'):
        sanitized_rest = sanitized_rest.rstrip('/') + '/'
    
    return prefix + sanitized_rest
