"""
MinIO Infrastructure Client

Manages interactions with MinIO object storage
"""

import asyncio
import io
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, AsyncGenerator, BinaryIO
from urllib.parse import urlparse

from minio import Minio
from minio.error import S3Error
from minio.datatypes import Object
from minio.helpers import ObjectWriteResult

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class MinIOClient:
    """Async wrapper for MinIO client with enhanced features"""
    
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool = False,
        region: Optional[str] = None
    ):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.region = region
        
        # Initialize MinIO client
        self._client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region
        )
        
        # Thread pool for async operations
        self._executor = None
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Cleanup if needed
        pass
        
    async def bucket_exists(self, bucket_name: str) -> bool:
        """Check if bucket exists"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._client.bucket_exists,
            bucket_name
        )
        
    async def create_bucket(
        self,
        bucket_name: str,
        location: Optional[str] = None
    ) -> bool:
        """Create a new bucket"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                self._executor,
                self._client.make_bucket,
                bucket_name,
                location or self.region
            )
            
            logger.info("bucket_created",
                       bucket=bucket_name,
                       location=location)
            return True
            
        except S3Error as e:
            if e.code == "BucketAlreadyOwnedByYou":
                return True
            logger.error("failed_to_create_bucket",
                        bucket=bucket_name,
                        error=str(e))
            raise
            
    async def delete_bucket(self, bucket_name: str) -> bool:
        """Delete a bucket"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                self._executor,
                self._client.remove_bucket,
                bucket_name
            )
            
            logger.info("bucket_deleted", bucket=bucket_name)
            return True
            
        except S3Error as e:
            logger.error("failed_to_delete_bucket",
                        bucket=bucket_name,
                        error=str(e))
            raise
            
    async def list_buckets(self) -> List[Dict[str, Any]]:
        """List all buckets"""
        loop = asyncio.get_event_loop()
        buckets = await loop.run_in_executor(
            self._executor,
            self._client.list_buckets
        )
        
        return [
            {
                "name": bucket.name,
                "creation_date": bucket.creation_date
            }
            for bucket in buckets
        ]
        
    async def upload_object(
        self,
        bucket_name: str,
        object_name: str,
        data: BinaryIO,
        length: int = -1,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
        sse: Optional[Any] = None,
        progress: Optional[Any] = None,
        part_size: int = 10 * 1024 * 1024  # 10MB
    ) -> ObjectWriteResult:
        """Upload an object to MinIO"""
        try:
            loop = asyncio.get_event_loop()
            
            # Convert metadata values to strings
            if metadata:
                metadata = {k: str(v) for k, v in metadata.items()}
                
            result = await loop.run_in_executor(
                self._executor,
                self._client.put_object,
                bucket_name,
                object_name,
                data,
                length,
                content_type,
                metadata,
                sse,
                progress,
                part_size
            )
            
            logger.info("object_uploaded",
                       bucket=bucket_name,
                       object=object_name,
                       size=result.object_size)
            
            return result
            
        except S3Error as e:
            logger.error("failed_to_upload_object",
                        bucket=bucket_name,
                        object=object_name,
                        error=str(e))
            raise
            
    async def download_object(
        self,
        bucket_name: str,
        object_name: str,
        chunk_size: int = 32 * 1024  # 32KB
    ) -> AsyncGenerator[bytes, None]:
        """Download an object from MinIO as a stream"""
        try:
            loop = asyncio.get_event_loop()
            
            # Get object in executor
            response = await loop.run_in_executor(
                self._executor,
                self._client.get_object,
                bucket_name,
                object_name
            )
            
            try:
                # Stream data
                while True:
                    chunk = await loop.run_in_executor(
                        self._executor,
                        response.read,
                        chunk_size
                    )
                    if not chunk:
                        break
                    yield chunk
                    
            finally:
                # Close response
                await loop.run_in_executor(
                    self._executor,
                    response.close
                )
                await loop.run_in_executor(
                    self._executor,
                    response.release_conn
                )
                
        except S3Error as e:
            logger.error("failed_to_download_object",
                        bucket=bucket_name,
                        object=object_name,
                        error=str(e))
            raise
            
    async def get_object(
        self,
        bucket_name: str,
        object_name: str
    ) -> bytes:
        """Get entire object content"""
        chunks = []
        async for chunk in self.download_object(bucket_name, object_name):
            chunks.append(chunk)
        return b''.join(chunks)
        
    async def delete_object(
        self,
        bucket_name: str,
        object_name: str
    ) -> bool:
        """Delete an object"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                self._executor,
                self._client.remove_object,
                bucket_name,
                object_name
            )
            
            logger.info("object_deleted",
                       bucket=bucket_name,
                       object=object_name)
            return True
            
        except S3Error as e:
            logger.error("failed_to_delete_object",
                        bucket=bucket_name,
                        object=object_name,
                        error=str(e))
            raise
            
    async def list_objects(
        self,
        bucket_name: str,
        prefix: Optional[str] = None,
        recursive: bool = False,
        include_user_meta: bool = False
    ) -> List[Object]:
        """List objects in a bucket"""
        try:
            loop = asyncio.get_event_loop()
            
            def _list_objects():
                return list(self._client.list_objects(
                    bucket_name,
                    prefix=prefix,
                    recursive=recursive,
                    include_user_meta=include_user_meta
                ))
                
            objects = await loop.run_in_executor(
                self._executor,
                _list_objects
            )
            
            return objects
            
        except S3Error as e:
            logger.error("failed_to_list_objects",
                        bucket=bucket_name,
                        prefix=prefix,
                        error=str(e))
            raise
            
    async def stat_object(
        self,
        bucket_name: str,
        object_name: str
    ) -> Dict[str, Any]:
        """Get object metadata"""
        try:
            loop = asyncio.get_event_loop()
            stat = await loop.run_in_executor(
                self._executor,
                self._client.stat_object,
                bucket_name,
                object_name
            )
            
            return {
                "size": stat.size,
                "etag": stat.etag,
                "content_type": stat.content_type,
                "last_modified": stat.last_modified,
                "metadata": stat.metadata
            }
            
        except S3Error as e:
            if e.code == "NoSuchKey":
                return None
            logger.error("failed_to_stat_object",
                        bucket=bucket_name,
                        object=object_name,
                        error=str(e))
            raise
            
    async def copy_object(
        self,
        source_bucket: str,
        source_object: str,
        dest_bucket: str,
        dest_object: str,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """Copy an object"""
        try:
            loop = asyncio.get_event_loop()
            
            # Create copy source
            from minio.commonconfig import CopySource
            copy_source = CopySource(source_bucket, source_object)
            
            await loop.run_in_executor(
                self._executor,
                self._client.copy_object,
                dest_bucket,
                dest_object,
                copy_source,
                metadata=metadata
            )
            
            logger.info("object_copied",
                       source=f"{source_bucket}/{source_object}",
                       dest=f"{dest_bucket}/{dest_object}")
            return True
            
        except S3Error as e:
            logger.error("failed_to_copy_object",
                        source=f"{source_bucket}/{source_object}",
                        dest=f"{dest_bucket}/{dest_object}",
                        error=str(e))
            raise
            
    async def presigned_get_url(
        self,
        bucket_name: str,
        object_name: str,
        expires: timedelta = timedelta(days=7)
    ) -> str:
        """Generate presigned URL for download"""
        try:
            loop = asyncio.get_event_loop()
            url = await loop.run_in_executor(
                self._executor,
                self._client.presigned_get_object,
                bucket_name,
                object_name,
                expires
            )
            
            return url
            
        except S3Error as e:
            logger.error("failed_to_generate_presigned_url",
                        bucket=bucket_name,
                        object=object_name,
                        error=str(e))
            raise
            
    async def presigned_put_url(
        self,
        bucket_name: str,
        object_name: str,
        expires: timedelta = timedelta(hours=1)
    ) -> str:
        """Generate presigned URL for upload"""
        try:
            loop = asyncio.get_event_loop()
            url = await loop.run_in_executor(
                self._executor,
                self._client.presigned_put_object,
                bucket_name,
                object_name,
                expires
            )
            
            return url
            
        except S3Error as e:
            logger.error("failed_to_generate_presigned_put_url",
                        bucket=bucket_name,
                        object=object_name,
                        error=str(e))
            raise
            
    async def set_bucket_lifecycle(
        self,
        bucket_name: str,
        rules: List[Dict[str, Any]]
    ) -> bool:
        """Set bucket lifecycle rules"""
        # TODO: Implement lifecycle rules
        logger.warning("bucket_lifecycle_not_implemented",
                      bucket=bucket_name)
        return True
        
    async def set_bucket_policy(
        self,
        bucket_name: str,
        policy: Dict[str, Any]
    ) -> bool:
        """Set bucket policy"""
        try:
            import json
            loop = asyncio.get_event_loop()
            
            policy_json = json.dumps(policy)
            
            await loop.run_in_executor(
                self._executor,
                self._client.set_bucket_policy,
                bucket_name,
                policy_json
            )
            
            logger.info("bucket_policy_set", bucket=bucket_name)
            return True
            
        except S3Error as e:
            logger.error("failed_to_set_bucket_policy",
                        bucket=bucket_name,
                        error=str(e))
            raise
            
    async def enable_bucket_versioning(self, bucket_name: str) -> bool:
        """Enable versioning for a bucket"""
        try:
            loop = asyncio.get_event_loop()
            
            from minio.versioningconfig import VersioningConfig, ENABLED
            config = VersioningConfig(ENABLED)
            
            await loop.run_in_executor(
                self._executor,
                self._client.set_bucket_versioning,
                bucket_name,
                config
            )
            
            logger.info("bucket_versioning_enabled", bucket=bucket_name)
            return True
            
        except S3Error as e:
            logger.error("failed_to_enable_versioning",
                        bucket=bucket_name,
                        error=str(e))
            raise
            
    async def get_bucket_size(self, bucket_name: str) -> int:
        """Calculate total size of all objects in bucket"""
        total_size = 0
        
        objects = await self.list_objects(bucket_name, recursive=True)
        for obj in objects:
            total_size += obj.size
            
        return total_size 