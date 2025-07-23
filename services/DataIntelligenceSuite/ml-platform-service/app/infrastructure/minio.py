"""
MinIO client for object storage
"""
import logging
from typing import Optional, List, Dict, Any, BinaryIO
import asyncio
from datetime import datetime, timedelta
from minio import Minio
from minio.error import S3Error
import io

logger = logging.getLogger(__name__)


class MinIOClient:
    """
    Async wrapper for MinIO operations
    """
    
    def __init__(self, 
                 endpoint: str,
                 access_key: str,
                 secret_key: str,
                 secure: bool = False):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.client: Optional[Minio] = None
        
    async def initialize(self):
        """Initialize MinIO client"""
        try:
            self.client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure
            )
            logger.info(f"MinIO client initialized: {self.endpoint}")
        except Exception as e:
            logger.error(f"Failed to initialize MinIO client: {str(e)}")
            raise
    
    async def create_bucket(self, bucket_name: str) -> bool:
        """Create a bucket if it doesn't exist"""
        loop = asyncio.get_event_loop()
        
        def _create_bucket():
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                return True
            return False
            
        created = await loop.run_in_executor(None, _create_bucket)
        if created:
            logger.info(f"Bucket created: {bucket_name}")
        return created
    
    async def upload_file(self, 
                         bucket_name: str,
                         object_name: str,
                         file_path: str,
                         metadata: Optional[Dict[str, str]] = None) -> str:
        """Upload a file to MinIO"""
        loop = asyncio.get_event_loop()
        
        def _upload():
            self.client.fput_object(
                bucket_name,
                object_name,
                file_path,
                metadata=metadata
            )
            
        await loop.run_in_executor(None, _upload)
        return f"s3://{bucket_name}/{object_name}"
    
    async def upload_data(self,
                         bucket_name: str,
                         object_name: str,
                         data: bytes,
                         content_type: str = "application/octet-stream",
                         metadata: Optional[Dict[str, str]] = None) -> str:
        """Upload data to MinIO"""
        loop = asyncio.get_event_loop()
        
        def _upload():
            data_stream = io.BytesIO(data)
            self.client.put_object(
                bucket_name,
                object_name,
                data_stream,
                length=len(data),
                content_type=content_type,
                metadata=metadata
            )
            
        await loop.run_in_executor(None, _upload)
        return f"s3://{bucket_name}/{object_name}"
    
    async def download_file(self,
                           bucket_name: str,
                           object_name: str,
                           file_path: str):
        """Download a file from MinIO"""
        loop = asyncio.get_event_loop()
        
        def _download():
            self.client.fget_object(bucket_name, object_name, file_path)
            
        await loop.run_in_executor(None, _download)
    
    async def get_object(self,
                        bucket_name: str,
                        object_name: str) -> bytes:
        """Get object data"""
        loop = asyncio.get_event_loop()
        
        def _get():
            response = self.client.get_object(bucket_name, object_name)
            data = response.read()
            response.close()
            response.release_conn()
            return data
            
        return await loop.run_in_executor(None, _get)
    
    async def list_objects(self,
                          bucket_name: str,
                          prefix: Optional[str] = None,
                          recursive: bool = True) -> List[Dict[str, Any]]:
        """List objects in a bucket"""
        loop = asyncio.get_event_loop()
        
        def _list():
            objects = []
            for obj in self.client.list_objects(bucket_name, prefix=prefix, recursive=recursive):
                objects.append({
                    "name": obj.object_name,
                    "size": obj.size,
                    "last_modified": obj.last_modified,
                    "etag": obj.etag,
                    "content_type": obj.content_type
                })
            return objects
            
        return await loop.run_in_executor(None, _list)
    
    async def delete_object(self, bucket_name: str, object_name: str):
        """Delete an object"""
        loop = asyncio.get_event_loop()
        
        def _delete():
            self.client.remove_object(bucket_name, object_name)
            
        await loop.run_in_executor(None, _delete)
    
    async def stat_object(self, bucket_name: str, object_name: str) -> Any:
        """Get object metadata"""
        loop = asyncio.get_event_loop()
        
        def _stat():
            return self.client.stat_object(bucket_name, object_name)
            
        return await loop.run_in_executor(None, _stat)
    
    async def presigned_get_url(self,
                               bucket_name: str,
                               object_name: str,
                               expires: timedelta = timedelta(days=7)) -> str:
        """Generate presigned URL for download"""
        loop = asyncio.get_event_loop()
        
        def _presign():
            return self.client.presigned_get_object(
                bucket_name,
                object_name,
                expires=expires
            )
            
        return await loop.run_in_executor(None, _presign)
    
    async def close(self):
        """Close MinIO client"""
        # MinIO client doesn't need explicit closing
        logger.info("MinIO client closed") 