"""MinIO Client"""

from minio import Minio
import logging

logger = logging.getLogger(__name__)


class MinIOClient:
    """Client for MinIO object storage"""
    
    def __init__(self, endpoint: str = "minio:9000", access_key: str = "minioadmin", secret_key: str = "minioadmin"):
        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
    
    async def put_object(self, bucket: str, object_name: str, data: bytes, length: int):
        """Put object in bucket"""
        self.client.put_object(bucket, object_name, data, length)
    
    async def get_object(self, bucket: str, object_name: str) -> bytes:
        """Get object from bucket"""
        response = self.client.get_object(bucket, object_name)
        return response.read()
    
    async def close(self):
        pass 