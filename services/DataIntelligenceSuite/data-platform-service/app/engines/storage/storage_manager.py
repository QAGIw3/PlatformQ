"""
Storage Manager for multi-backend object storage.
"""

import asyncio
from typing import Dict, List, Optional, Any, Union, AsyncIterator, BinaryIO
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import io
import os
import uuid
import hashlib
from pathlib import Path

from minio import Minio
from minio.error import S3Error
import aiofiles
import magic

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class StorageBackend(str, Enum):
    """Supported storage backends."""
    MINIO = "minio"
    S3 = "s3"
    AZURE = "azure"
    GCS = "gcs"
    LOCAL = "local"


@dataclass
class StorageMetadata:
    """Metadata for stored objects."""
    identifier: str
    filename: str
    size: int
    content_type: str
    checksum: str
    created_at: datetime
    updated_at: datetime
    tenant_id: str
    tags: Dict[str, str] = field(default_factory=dict)
    custom_metadata: Dict[str, Any] = field(default_factory=dict)
    version: Optional[str] = None
    encryption: Optional[str] = None


@dataclass
class StorageObject:
    """Represents a stored object."""
    identifier: str
    data: Optional[bytes] = None
    stream: Optional[BinaryIO] = None
    metadata: Optional[StorageMetadata] = None
    presigned_url: Optional[str] = None


@dataclass
class UploadOptions:
    """Options for uploading objects."""
    chunk_size: int = 5 * 1024 * 1024  # 5MB
    multipart_threshold: int = 15 * 1024 * 1024  # 15MB
    encryption: Optional[str] = None
    storage_class: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    content_type: Optional[str] = None
    ttl: Optional[timedelta] = None


@dataclass
class DownloadOptions:
    """Options for downloading objects."""
    range_start: Optional[int] = None
    range_end: Optional[int] = None
    version_id: Optional[str] = None
    if_modified_since: Optional[datetime] = None
    response_headers: Dict[str, str] = field(default_factory=dict)


class StorageManager:
    """
    Manages object storage across multiple backends.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager,
        backend: StorageBackend = StorageBackend.MINIO,
        config: Optional[Dict[str, Any]] = None
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        self.backend = backend
        self.config = config or {}
        
        # Storage clients
        self.clients: Dict[str, Any] = {}
        
        # Bucket configuration
        self.bucket_prefix = self.config.get("bucket_prefix", "platformq")
        self.tenant_isolation = self.config.get("tenant_isolation", True)
        
        # Performance settings
        self.concurrent_uploads = self.config.get("concurrent_uploads", 10)
        self.concurrent_downloads = self.config.get("concurrent_downloads", 20)
        
        # Background tasks
        self._cleanup_task: Optional[asyncio.Task] = None
        self._monitor_task: Optional[asyncio.Task] = None
        
        logger.info(f"Storage Manager initialized with backend: {backend}")
        
    async def initialize(self):
        """Initialize storage manager."""
        # Initialize storage backend
        await self._initialize_backend()
        
        # Subscribe to events
        await self.event_bus.subscribe("storage.upload.requested", self._handle_upload_request)
        await self.event_bus.subscribe("storage.delete.requested", self._handle_delete_request)
        
        # Start background tasks
        self._cleanup_task = asyncio.create_task(self._cleanup_expired_objects())
        self._monitor_task = asyncio.create_task(self._monitor_storage())
        
        logger.info("Storage Manager ready")
        
    async def cleanup(self):
        """Cleanup storage manager resources."""
        # Cancel background tasks
        if self._cleanup_task:
            self._cleanup_task.cancel()
        if self._monitor_task:
            self._monitor_task.cancel()
        
        # Close clients
        for client in self.clients.values():
            if hasattr(client, 'close'):
                client.close()
        
        logger.info("Storage Manager cleaned up")
        
    async def upload(
        self,
        data: Union[bytes, BinaryIO, str],
        filename: str,
        tenant_id: str,
        options: Optional[UploadOptions] = None
    ) -> str:
        """Upload an object to storage."""
        options = options or UploadOptions()
        
        # Generate identifier
        identifier = self._generate_identifier(filename)
        
        # Determine content type
        if options.content_type:
            content_type = options.content_type
        else:
            content_type = await self._detect_content_type(data, filename)
        
        # Calculate checksum
        checksum = await self._calculate_checksum(data)
        
        # Get bucket name
        bucket = self._get_bucket_name(tenant_id)
        
        # Ensure bucket exists
        await self._ensure_bucket_exists(bucket)
        
        try:
            # Upload based on backend
            if self.backend == StorageBackend.MINIO:
                await self._upload_minio(
                    bucket=bucket,
                    object_name=identifier,
                    data=data,
                    content_type=content_type,
                    options=options
                )
            elif self.backend == StorageBackend.LOCAL:
                await self._upload_local(
                    bucket=bucket,
                    object_name=identifier,
                    data=data,
                    options=options
                )
            else:
                raise NotImplementedError(f"Backend {self.backend} not implemented")
            
            # Create metadata
            metadata = StorageMetadata(
                identifier=identifier,
                filename=filename,
                size=await self._get_data_size(data),
                content_type=content_type,
                checksum=checksum,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                tenant_id=tenant_id,
                tags=options.tags,
                custom_metadata=options.metadata,
                encryption=options.encryption
            )
            
            # Cache metadata
            await self.cache_manager.set(
                f"storage:metadata:{tenant_id}:{identifier}",
                metadata.__dict__,
                ttl=3600  # 1 hour
            )
            
            # Publish event
            await self.event_bus.publish("storage.uploaded", {
                "identifier": identifier,
                "tenant_id": tenant_id,
                "filename": filename,
                "size": metadata.size,
                "content_type": content_type
            })
            
            logger.info(f"Uploaded object: {identifier} for tenant {tenant_id}")
            
            return identifier
            
        except Exception as e:
            logger.error(f"Error uploading object: {e}")
            raise
            
    async def download(
        self,
        identifier: str,
        tenant_id: str,
        options: Optional[DownloadOptions] = None
    ) -> StorageObject:
        """Download an object from storage."""
        options = options or DownloadOptions()
        
        # Get bucket name
        bucket = self._get_bucket_name(tenant_id)
        
        try:
            # Get metadata from cache
            cached_metadata = await self.cache_manager.get(
                f"storage:metadata:{tenant_id}:{identifier}"
            )
            
            metadata = None
            if cached_metadata:
                metadata = StorageMetadata(**cached_metadata)
            
            # Download based on backend
            if self.backend == StorageBackend.MINIO:
                data = await self._download_minio(
                    bucket=bucket,
                    object_name=identifier,
                    options=options
                )
            elif self.backend == StorageBackend.LOCAL:
                data = await self._download_local(
                    bucket=bucket,
                    object_name=identifier,
                    options=options
                )
            else:
                raise NotImplementedError(f"Backend {self.backend} not implemented")
            
            # Create storage object
            obj = StorageObject(
                identifier=identifier,
                data=data,
                metadata=metadata
            )
            
            # Publish event
            await self.event_bus.publish("storage.downloaded", {
                "identifier": identifier,
                "tenant_id": tenant_id,
                "size": len(data) if data else 0
            })
            
            return obj
            
        except Exception as e:
            logger.error(f"Error downloading object {identifier}: {e}")
            raise
            
    async def download_stream(
        self,
        identifier: str,
        tenant_id: str,
        options: Optional[DownloadOptions] = None
    ) -> AsyncIterator[bytes]:
        """Download an object as a stream."""
        options = options or DownloadOptions()
        
        # Get bucket name
        bucket = self._get_bucket_name(tenant_id)
        
        try:
            # Stream based on backend
            if self.backend == StorageBackend.MINIO:
                async for chunk in self._stream_minio(bucket, identifier, options):
                    yield chunk
            elif self.backend == StorageBackend.LOCAL:
                async for chunk in self._stream_local(bucket, identifier, options):
                    yield chunk
            else:
                raise NotImplementedError(f"Backend {self.backend} not implemented")
                
        except Exception as e:
            logger.error(f"Error streaming object {identifier}: {e}")
            raise
            
    async def delete(
        self,
        identifier: str,
        tenant_id: str
    ) -> bool:
        """Delete an object from storage."""
        # Get bucket name
        bucket = self._get_bucket_name(tenant_id)
        
        try:
            # Delete based on backend
            if self.backend == StorageBackend.MINIO:
                await self._delete_minio(bucket, identifier)
            elif self.backend == StorageBackend.LOCAL:
                await self._delete_local(bucket, identifier)
            else:
                raise NotImplementedError(f"Backend {self.backend} not implemented")
            
            # Remove from cache
            await self.cache_manager.delete(
                f"storage:metadata:{tenant_id}:{identifier}"
            )
            
            # Publish event
            await self.event_bus.publish("storage.deleted", {
                "identifier": identifier,
                "tenant_id": tenant_id
            })
            
            logger.info(f"Deleted object: {identifier} for tenant {tenant_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting object {identifier}: {e}")
            return False
            
    async def list_objects(
        self,
        tenant_id: str,
        prefix: Optional[str] = None,
        limit: int = 1000,
        continuation_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """List objects in storage."""
        # Get bucket name
        bucket = self._get_bucket_name(tenant_id)
        
        try:
            # List based on backend
            if self.backend == StorageBackend.MINIO:
                return await self._list_minio(bucket, prefix, limit, continuation_token)
            elif self.backend == StorageBackend.LOCAL:
                return await self._list_local(bucket, prefix, limit, continuation_token)
            else:
                raise NotImplementedError(f"Backend {self.backend} not implemented")
                
        except Exception as e:
            logger.error(f"Error listing objects: {e}")
            raise
            
    async def exists(
        self,
        identifier: str,
        tenant_id: str
    ) -> bool:
        """Check if an object exists."""
        # Get bucket name
        bucket = self._get_bucket_name(tenant_id)
        
        try:
            # Check based on backend
            if self.backend == StorageBackend.MINIO:
                return await self._exists_minio(bucket, identifier)
            elif self.backend == StorageBackend.LOCAL:
                return await self._exists_local(bucket, identifier)
            else:
                raise NotImplementedError(f"Backend {self.backend} not implemented")
                
        except Exception as e:
            logger.error(f"Error checking object existence: {e}")
            return False
            
    async def get_presigned_url(
        self,
        identifier: str,
        tenant_id: str,
        expiry: timedelta = timedelta(hours=1),
        method: str = "GET"
    ) -> str:
        """Generate a presigned URL for an object."""
        # Get bucket name
        bucket = self._get_bucket_name(tenant_id)
        
        try:
            # Generate URL based on backend
            if self.backend == StorageBackend.MINIO:
                return await self._get_presigned_url_minio(
                    bucket, identifier, expiry, method
                )
            else:
                raise NotImplementedError(f"Presigned URLs not supported for {self.backend}")
                
        except Exception as e:
            logger.error(f"Error generating presigned URL: {e}")
            raise
            
    async def copy_object(
        self,
        source_identifier: str,
        source_tenant_id: str,
        dest_identifier: str,
        dest_tenant_id: str
    ) -> str:
        """Copy an object within or across tenants."""
        # Get bucket names
        source_bucket = self._get_bucket_name(source_tenant_id)
        dest_bucket = self._get_bucket_name(dest_tenant_id)
        
        try:
            # Ensure destination bucket exists
            await self._ensure_bucket_exists(dest_bucket)
            
            # Copy based on backend
            if self.backend == StorageBackend.MINIO:
                await self._copy_minio(
                    source_bucket, source_identifier,
                    dest_bucket, dest_identifier
                )
            else:
                # Fallback: download and re-upload
                obj = await self.download(source_identifier, source_tenant_id)
                await self.upload(
                    data=obj.data,
                    filename=dest_identifier,
                    tenant_id=dest_tenant_id
                )
            
            logger.info(f"Copied object from {source_identifier} to {dest_identifier}")
            
            return dest_identifier
            
        except Exception as e:
            logger.error(f"Error copying object: {e}")
            raise
            
    async def get_object_metadata(
        self,
        identifier: str,
        tenant_id: str
    ) -> Optional[StorageMetadata]:
        """Get metadata for an object."""
        # Check cache first
        cached = await self.cache_manager.get(
            f"storage:metadata:{tenant_id}:{identifier}"
        )
        
        if cached:
            return StorageMetadata(**cached)
        
        # Get bucket name
        bucket = self._get_bucket_name(tenant_id)
        
        try:
            # Get metadata based on backend
            if self.backend == StorageBackend.MINIO:
                stat = await self._stat_minio(bucket, identifier)
                if stat:
                    metadata = StorageMetadata(
                        identifier=identifier,
                        filename=identifier,
                        size=stat.size,
                        content_type=stat.content_type,
                        checksum=stat.etag,
                        created_at=stat.last_modified,
                        updated_at=stat.last_modified,
                        tenant_id=tenant_id,
                        tags=stat.metadata or {}
                    )
                    
                    # Cache metadata
                    await self.cache_manager.set(
                        f"storage:metadata:{tenant_id}:{identifier}",
                        metadata.__dict__,
                        ttl=3600
                    )
                    
                    return metadata
                    
            return None
            
        except Exception as e:
            logger.error(f"Error getting object metadata: {e}")
            return None
            
    # Backend-specific implementations
    
    async def _initialize_backend(self):
        """Initialize storage backend."""
        if self.backend == StorageBackend.MINIO:
            self.clients['minio'] = Minio(
                endpoint=self.config.get("endpoint", "minio:9000"),
                access_key=self.config.get("access_key", "minioadmin"),
                secret_key=self.config.get("secret_key", "minioadmin"),
                secure=self.config.get("secure", False)
            )
        elif self.backend == StorageBackend.LOCAL:
            # Create local storage directory
            self.local_path = Path(self.config.get("path", "/tmp/platformq-storage"))
            self.local_path.mkdir(parents=True, exist_ok=True)
            
    def _get_bucket_name(self, tenant_id: str) -> str:
        """Get bucket name for tenant."""
        if self.tenant_isolation:
            return f"{self.bucket_prefix}-{tenant_id}"
        return self.bucket_prefix
        
    def _generate_identifier(self, filename: str) -> str:
        """Generate unique identifier for object."""
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        extension = Path(filename).suffix
        return f"{timestamp}-{unique_id}{extension}"
        
    async def _detect_content_type(
        self,
        data: Union[bytes, BinaryIO, str],
        filename: str
    ) -> str:
        """Detect content type from data or filename."""
        if isinstance(data, bytes):
            mime = magic.from_buffer(data[:1024], mime=True)
            return mime
        elif isinstance(data, str) and os.path.exists(data):
            mime = magic.from_file(data, mime=True)
            return mime
        else:
            # Fallback to extension
            import mimetypes
            content_type, _ = mimetypes.guess_type(filename)
            return content_type or "application/octet-stream"
            
    async def _calculate_checksum(
        self,
        data: Union[bytes, BinaryIO, str]
    ) -> str:
        """Calculate checksum for data."""
        hasher = hashlib.sha256()
        
        if isinstance(data, bytes):
            hasher.update(data)
        elif isinstance(data, str) and os.path.exists(data):
            async with aiofiles.open(data, 'rb') as f:
                while chunk := await f.read(8192):
                    hasher.update(chunk)
        elif hasattr(data, 'read'):
            # Handle file-like objects
            data.seek(0)
            while chunk := data.read(8192):
                hasher.update(chunk)
            data.seek(0)
            
        return hasher.hexdigest()
        
    async def _get_data_size(
        self,
        data: Union[bytes, BinaryIO, str]
    ) -> int:
        """Get size of data."""
        if isinstance(data, bytes):
            return len(data)
        elif isinstance(data, str) and os.path.exists(data):
            return os.path.getsize(data)
        elif hasattr(data, 'seek') and hasattr(data, 'tell'):
            current = data.tell()
            data.seek(0, 2)  # Seek to end
            size = data.tell()
            data.seek(current)  # Restore position
            return size
        return 0
        
    async def _ensure_bucket_exists(self, bucket: str):
        """Ensure bucket exists."""
        if self.backend == StorageBackend.MINIO:
            client = self.clients['minio']
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")
        elif self.backend == StorageBackend.LOCAL:
            bucket_path = self.local_path / bucket
            bucket_path.mkdir(exist_ok=True)
            
    # MinIO implementations
    
    async def _upload_minio(
        self,
        bucket: str,
        object_name: str,
        data: Union[bytes, BinaryIO, str],
        content_type: str,
        options: UploadOptions
    ):
        """Upload to MinIO."""
        client = self.clients['minio']
        
        # Convert data to file-like object if needed
        if isinstance(data, bytes):
            data_stream = io.BytesIO(data)
            length = len(data)
        elif isinstance(data, str) and os.path.exists(data):
            # Upload from file
            client.fput_object(
                bucket_name=bucket,
                object_name=object_name,
                file_path=data,
                content_type=content_type,
                metadata=options.metadata,
                tags=options.tags
            )
            return
        else:
            data_stream = data
            data_stream.seek(0, 2)
            length = data_stream.tell()
            data_stream.seek(0)
        
        # Upload
        client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=data_stream,
            length=length,
            content_type=content_type,
            metadata=options.metadata,
            tags=options.tags
        )
        
    async def _download_minio(
        self,
        bucket: str,
        object_name: str,
        options: DownloadOptions
    ) -> bytes:
        """Download from MinIO."""
        client = self.clients['minio']
        
        try:
            response = client.get_object(
                bucket_name=bucket,
                object_name=object_name,
                offset=options.range_start,
                length=options.range_end - options.range_start if options.range_end else None,
                version_id=options.version_id
            )
            
            data = response.read()
            response.close()
            response.release_conn()
            
            return data
            
        except S3Error as e:
            if e.code == 'NoSuchKey':
                raise FileNotFoundError(f"Object not found: {object_name}")
            raise
            
    async def _stream_minio(
        self,
        bucket: str,
        object_name: str,
        options: DownloadOptions
    ) -> AsyncIterator[bytes]:
        """Stream from MinIO."""
        client = self.clients['minio']
        
        try:
            response = client.get_object(
                bucket_name=bucket,
                object_name=object_name,
                offset=options.range_start,
                length=options.range_end - options.range_start if options.range_end else None,
                version_id=options.version_id
            )
            
            # Stream in chunks
            chunk_size = 1024 * 1024  # 1MB
            for data in response.stream(chunk_size):
                yield data
                
            response.close()
            response.release_conn()
            
        except S3Error as e:
            if e.code == 'NoSuchKey':
                raise FileNotFoundError(f"Object not found: {object_name}")
            raise
            
    async def _delete_minio(self, bucket: str, object_name: str):
        """Delete from MinIO."""
        client = self.clients['minio']
        client.remove_object(bucket, object_name)
        
    async def _list_minio(
        self,
        bucket: str,
        prefix: Optional[str],
        limit: int,
        continuation_token: Optional[str]
    ) -> Dict[str, Any]:
        """List objects in MinIO."""
        client = self.clients['minio']
        
        objects = []
        count = 0
        
        for obj in client.list_objects(
            bucket_name=bucket,
            prefix=prefix,
            recursive=True,
            start_after=continuation_token
        ):
            if count >= limit:
                break
                
            objects.append({
                "identifier": obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified,
                "etag": obj.etag
            })
            count += 1
            
        return {
            "objects": objects,
            "continuation_token": objects[-1]["identifier"] if objects and count >= limit else None
        }
        
    async def _exists_minio(self, bucket: str, object_name: str) -> bool:
        """Check if object exists in MinIO."""
        client = self.clients['minio']
        
        try:
            client.stat_object(bucket, object_name)
            return True
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return False
            raise
            
    async def _stat_minio(self, bucket: str, object_name: str):
        """Get object stats from MinIO."""
        client = self.clients['minio']
        
        try:
            return client.stat_object(bucket, object_name)
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return None
            raise
            
    async def _get_presigned_url_minio(
        self,
        bucket: str,
        object_name: str,
        expiry: timedelta,
        method: str
    ) -> str:
        """Generate presigned URL for MinIO."""
        client = self.clients['minio']
        
        if method == "GET":
            return client.presigned_get_object(
                bucket_name=bucket,
                object_name=object_name,
                expires=expiry
            )
        elif method == "PUT":
            return client.presigned_put_object(
                bucket_name=bucket,
                object_name=object_name,
                expires=expiry
            )
        else:
            raise ValueError(f"Unsupported method: {method}")
            
    async def _copy_minio(
        self,
        source_bucket: str,
        source_object: str,
        dest_bucket: str,
        dest_object: str
    ):
        """Copy object in MinIO."""
        client = self.clients['minio']
        
        client.copy_object(
            bucket_name=dest_bucket,
            object_name=dest_object,
            source=f"{source_bucket}/{source_object}"
        )
        
    # Local filesystem implementations
    
    async def _upload_local(
        self,
        bucket: str,
        object_name: str,
        data: Union[bytes, BinaryIO, str],
        options: UploadOptions
    ):
        """Upload to local filesystem."""
        file_path = self.local_path / bucket / object_name
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        if isinstance(data, bytes):
            async with aiofiles.open(file_path, 'wb') as f:
                await f.write(data)
        elif isinstance(data, str) and os.path.exists(data):
            # Copy file
            import shutil
            shutil.copy2(data, file_path)
        else:
            # Handle file-like objects
            async with aiofiles.open(file_path, 'wb') as f:
                data.seek(0)
                while chunk := data.read(8192):
                    await f.write(chunk)
                    
    async def _download_local(
        self,
        bucket: str,
        object_name: str,
        options: DownloadOptions
    ) -> bytes:
        """Download from local filesystem."""
        file_path = self.local_path / bucket / object_name
        
        if not file_path.exists():
            raise FileNotFoundError(f"Object not found: {object_name}")
            
        async with aiofiles.open(file_path, 'rb') as f:
            if options.range_start:
                await f.seek(options.range_start)
                
            if options.range_end:
                length = options.range_end - (options.range_start or 0)
                return await f.read(length)
            else:
                return await f.read()
                
    async def _stream_local(
        self,
        bucket: str,
        object_name: str,
        options: DownloadOptions
    ) -> AsyncIterator[bytes]:
        """Stream from local filesystem."""
        file_path = self.local_path / bucket / object_name
        
        if not file_path.exists():
            raise FileNotFoundError(f"Object not found: {object_name}")
            
        async with aiofiles.open(file_path, 'rb') as f:
            if options.range_start:
                await f.seek(options.range_start)
                
            chunk_size = 1024 * 1024  # 1MB
            bytes_read = 0
            max_bytes = options.range_end - (options.range_start or 0) if options.range_end else None
            
            while True:
                if max_bytes:
                    read_size = min(chunk_size, max_bytes - bytes_read)
                else:
                    read_size = chunk_size
                    
                chunk = await f.read(read_size)
                if not chunk:
                    break
                    
                yield chunk
                bytes_read += len(chunk)
                
                if max_bytes and bytes_read >= max_bytes:
                    break
                    
    async def _delete_local(self, bucket: str, object_name: str):
        """Delete from local filesystem."""
        file_path = self.local_path / bucket / object_name
        
        if file_path.exists():
            file_path.unlink()
            
    async def _list_local(
        self,
        bucket: str,
        prefix: Optional[str],
        limit: int,
        continuation_token: Optional[str]
    ) -> Dict[str, Any]:
        """List objects in local filesystem."""
        bucket_path = self.local_path / bucket
        
        if not bucket_path.exists():
            return {"objects": [], "continuation_token": None}
            
        objects = []
        count = 0
        skip_until = continuation_token
        
        for file_path in sorted(bucket_path.rglob("*")):
            if file_path.is_file():
                relative_path = str(file_path.relative_to(bucket_path))
                
                # Skip until continuation token
                if skip_until and relative_path <= skip_until:
                    continue
                    
                # Check prefix
                if prefix and not relative_path.startswith(prefix):
                    continue
                    
                if count >= limit:
                    break
                    
                stat = file_path.stat()
                objects.append({
                    "identifier": relative_path,
                    "size": stat.st_size,
                    "last_modified": datetime.fromtimestamp(stat.st_mtime),
                    "etag": str(stat.st_mtime)
                })
                count += 1
                
        return {
            "objects": objects,
            "continuation_token": objects[-1]["identifier"] if objects and count >= limit else None
        }
        
    async def _exists_local(self, bucket: str, object_name: str) -> bool:
        """Check if object exists in local filesystem."""
        file_path = self.local_path / bucket / object_name
        return file_path.exists()
        
    # Background tasks
    
    async def _cleanup_expired_objects(self):
        """Clean up expired objects based on TTL."""
        while True:
            try:
                # This would scan for objects with TTL and delete expired ones
                await asyncio.sleep(3600)  # Run every hour
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}")
                await asyncio.sleep(3600)
                
    async def _monitor_storage(self):
        """Monitor storage usage and health."""
        while True:
            try:
                # Collect storage metrics
                metrics = {
                    "backend": self.backend.value,
                    "buckets": 0,
                    "objects": 0,
                    "total_size": 0
                }
                
                # Publish metrics
                await self.event_bus.publish("storage.metrics", metrics)
                
                await asyncio.sleep(300)  # Run every 5 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitor task: {e}")
                await asyncio.sleep(300)
                
    async def _handle_upload_request(self, event_data: Dict[str, Any]):
        """Handle upload request event."""
        try:
            identifier = await self.upload(
                data=event_data.get("data"),
                filename=event_data.get("filename"),
                tenant_id=event_data.get("tenant_id"),
                options=UploadOptions(**event_data.get("options", {}))
            )
            
            # Publish response
            await self.event_bus.publish("storage.upload.completed", {
                "request_id": event_data.get("request_id"),
                "identifier": identifier
            })
            
        except Exception as e:
            logger.error(f"Error handling upload request: {e}")
            
    async def _handle_delete_request(self, event_data: Dict[str, Any]):
        """Handle delete request event."""
        try:
            success = await self.delete(
                identifier=event_data.get("identifier"),
                tenant_id=event_data.get("tenant_id")
            )
            
            # Publish response
            await self.event_bus.publish("storage.delete.completed", {
                "request_id": event_data.get("request_id"),
                "success": success
            })
            
        except Exception as e:
            logger.error(f"Error handling delete request: {e}")
            
    def get_statistics(self) -> Dict[str, Any]:
        """Get storage statistics."""
        return {
            "backend": self.backend.value,
            "tenant_isolation": self.tenant_isolation,
            "bucket_prefix": self.bucket_prefix,
            "concurrent_uploads": self.concurrent_uploads,
            "concurrent_downloads": self.concurrent_downloads
        } 