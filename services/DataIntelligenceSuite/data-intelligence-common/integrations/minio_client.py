"""
MinIO Client Integration

Provides high-level client for MinIO object storage operations.
"""

import logging
from typing import Any, Dict, List, Optional, Union, BinaryIO, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import asyncio
from pathlib import Path
import io

from minio import Minio
from minio.error import S3Error
from minio.datatypes import Object
from minio.deleteobjects import DeleteObject

logger = logging.getLogger(__name__)


@dataclass
class MinIOConfig:
    """Configuration for MinIO client"""
    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    
    # Connection settings
    secure: bool = False
    region: Optional[str] = None
    http_client: Optional[Any] = None
    
    # SSL/TLS
    cert_check: bool = True
    ssl_context: Optional[Any] = None
    
    # Performance
    part_size: int = 10 * 1024 * 1024  # 10MB
    num_parallel_uploads: int = 10


@dataclass
class ObjectInfo:
    """Object information"""
    bucket: str
    name: str
    size: int
    etag: str
    last_modified: datetime
    content_type: Optional[str] = None
    metadata: Dict[str, str] = field(default_factory=dict)
    version_id: Optional[str] = None
    is_delete_marker: bool = False


@dataclass
class BucketInfo:
    """Bucket information"""
    name: str
    creation_date: datetime


class MinIOClient:
    """
    High-level client for MinIO operations.
    
    Features:
    - Bucket management
    - Object CRUD operations
    - Multipart uploads
    - Presigned URLs
    - Object versioning
    - Lifecycle policies
    """
    
    def __init__(self, config: MinIOConfig):
        self.config = config
        self._client: Optional[Minio] = None
        
    def connect(self):
        """Connect to MinIO server"""
        try:
            self._client = Minio(
                endpoint=self.config.endpoint,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                secure=self.config.secure,
                region=self.config.region,
                http_client=self.config.http_client,
                cert_check=self.config.cert_check
            )
            
            # Test connection by listing buckets
            self._client.list_buckets()
            
            logger.info(f"Connected to MinIO: {self.config.endpoint}")
            
        except Exception as e:
            logger.error(f"Failed to connect to MinIO: {e}")
            raise
            
    def disconnect(self):
        """Disconnect from MinIO"""
        # MinIO client doesn't require explicit disconnect
        self._client = None
        logger.info("Disconnected from MinIO")
        
    # Bucket operations
    
    def create_bucket(
        self,
        bucket_name: str,
        location: Optional[str] = None,
        object_lock: bool = False
    ) -> bool:
        """Create a bucket"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        try:
            self._client.make_bucket(
                bucket_name,
                location=location,
                object_lock=object_lock
            )
            logger.info(f"Created bucket: {bucket_name}")
            return True
        except S3Error as e:
            if e.code == "BucketAlreadyOwnedByYou":
                logger.warning(f"Bucket already exists: {bucket_name}")
                return False
            raise
            
    def delete_bucket(self, bucket_name: str, force: bool = False) -> bool:
        """Delete a bucket"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        try:
            if force:
                # Remove all objects first
                objects = self._client.list_objects(bucket_name, recursive=True)
                for obj in objects:
                    self._client.remove_object(bucket_name, obj.object_name)
                    
            self._client.remove_bucket(bucket_name)
            logger.info(f"Deleted bucket: {bucket_name}")
            return True
        except S3Error as e:
            logger.error(f"Failed to delete bucket: {e}")
            return False
            
    def list_buckets(self) -> List[BucketInfo]:
        """List all buckets"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        buckets = self._client.list_buckets()
        
        return [
            BucketInfo(
                name=bucket.name,
                creation_date=bucket.creation_date
            )
            for bucket in buckets
        ]
        
    def bucket_exists(self, bucket_name: str) -> bool:
        """Check if bucket exists"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        return self._client.bucket_exists(bucket_name)
        
    # Object operations
    
    def put_object(
        self,
        bucket_name: str,
        object_name: str,
        data: Union[BinaryIO, bytes, str],
        length: Optional[int] = None,
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
        sse: Optional[Any] = None,
        progress: Optional[Any] = None,
        part_size: Optional[int] = None
    ) -> ObjectInfo:
        """Upload an object"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        # Convert data to BinaryIO if needed
        if isinstance(data, bytes):
            data = io.BytesIO(data)
            length = len(data.getvalue())
        elif isinstance(data, str):
            data = io.BytesIO(data.encode('utf-8'))
            length = len(data.getvalue())
            
        if length is None:
            # Try to determine length
            data.seek(0, 2)  # Seek to end
            length = data.tell()
            data.seek(0)  # Reset to beginning
            
        result = self._client.put_object(
            bucket_name,
            object_name,
            data,
            length,
            content_type=content_type,
            metadata=metadata,
            sse=sse,
            progress=progress,
            part_size=part_size or self.config.part_size
        )
        
        return ObjectInfo(
            bucket=bucket_name,
            name=object_name,
            size=length,
            etag=result.etag,
            last_modified=datetime.utcnow(),
            content_type=content_type,
            metadata=metadata or {},
            version_id=result.version_id
        )
        
    def get_object(
        self,
        bucket_name: str,
        object_name: str,
        offset: int = 0,
        length: Optional[int] = None,
        version_id: Optional[str] = None,
        sse: Optional[Any] = None
    ) -> bytes:
        """Download an object"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        try:
            response = self._client.get_object(
                bucket_name,
                object_name,
                offset=offset,
                length=length,
                version_id=version_id,
                ssec=sse
            )
            
            # Read data
            data = response.read()
            response.close()
            response.release_conn()
            
            return data
            
        except S3Error as e:
            if e.code == "NoSuchKey":
                raise KeyError(f"Object not found: {object_name}")
            raise
            
    def fget_object(
        self,
        bucket_name: str,
        object_name: str,
        file_path: Union[str, Path],
        version_id: Optional[str] = None,
        sse: Optional[Any] = None,
        progress: Optional[Any] = None
    ) -> ObjectInfo:
        """Download object to file"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        result = self._client.fget_object(
            bucket_name,
            object_name,
            str(file_path),
            version_id=version_id,
            ssec=sse,
            progress=progress
        )
        
        return ObjectInfo(
            bucket=bucket_name,
            name=object_name,
            size=result.size,
            etag=result.etag,
            last_modified=result.last_modified,
            content_type=result.content_type,
            metadata=result.metadata,
            version_id=result.version_id
        )
        
    def fput_object(
        self,
        bucket_name: str,
        object_name: str,
        file_path: Union[str, Path],
        content_type: str = "application/octet-stream",
        metadata: Optional[Dict[str, str]] = None,
        sse: Optional[Any] = None,
        progress: Optional[Any] = None,
        part_size: Optional[int] = None
    ) -> ObjectInfo:
        """Upload file as object"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        result = self._client.fput_object(
            bucket_name,
            object_name,
            str(file_path),
            content_type=content_type,
            metadata=metadata,
            sse=sse,
            progress=progress,
            part_size=part_size or self.config.part_size
        )
        
        return ObjectInfo(
            bucket=bucket_name,
            name=object_name,
            size=Path(file_path).stat().st_size,
            etag=result.etag,
            last_modified=datetime.utcnow(),
            content_type=content_type,
            metadata=metadata or {},
            version_id=result.version_id
        )
        
    def remove_object(
        self,
        bucket_name: str,
        object_name: str,
        version_id: Optional[str] = None
    ) -> bool:
        """Remove an object"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        try:
            self._client.remove_object(
                bucket_name,
                object_name,
                version_id=version_id
            )
            return True
        except S3Error:
            return False
            
    def remove_objects(
        self,
        bucket_name: str,
        object_names: List[str],
        bypass_governance_mode: bool = False
    ) -> List[str]:
        """Remove multiple objects"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        # Create delete objects
        delete_objects = [
            DeleteObject(name) for name in object_names
        ]
        
        errors = self._client.remove_objects(
            bucket_name,
            delete_objects,
            bypass_governance_mode=bypass_governance_mode
        )
        
        # Collect failed deletions
        failed = []
        for error in errors:
            failed.append(error.object_name)
            logger.error(f"Failed to delete {error.object_name}: {error.error}")
            
        return failed
        
    def list_objects(
        self,
        bucket_name: str,
        prefix: Optional[str] = None,
        recursive: bool = False,
        start_after: Optional[str] = None,
        include_user_meta: bool = False,
        include_version: bool = False
    ) -> Iterator[ObjectInfo]:
        """List objects in bucket"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        objects = self._client.list_objects(
            bucket_name,
            prefix=prefix,
            recursive=recursive,
            start_after=start_after,
            include_user_meta=include_user_meta,
            include_version=include_version
        )
        
        for obj in objects:
            yield ObjectInfo(
                bucket=bucket_name,
                name=obj.object_name,
                size=obj.size,
                etag=obj.etag,
                last_modified=obj.last_modified,
                content_type=obj.content_type,
                metadata=obj.metadata or {},
                version_id=obj.version_id,
                is_delete_marker=obj.is_delete_marker
            )
            
    def stat_object(
        self,
        bucket_name: str,
        object_name: str,
        version_id: Optional[str] = None,
        sse: Optional[Any] = None
    ) -> ObjectInfo:
        """Get object information"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        try:
            stat = self._client.stat_object(
                bucket_name,
                object_name,
                version_id=version_id,
                ssec=sse
            )
            
            return ObjectInfo(
                bucket=bucket_name,
                name=object_name,
                size=stat.size,
                etag=stat.etag,
                last_modified=stat.last_modified,
                content_type=stat.content_type,
                metadata=stat.metadata,
                version_id=stat.version_id,
                is_delete_marker=stat.is_delete_marker
            )
            
        except S3Error as e:
            if e.code == "NoSuchKey":
                raise KeyError(f"Object not found: {object_name}")
            raise
            
    def copy_object(
        self,
        bucket_name: str,
        object_name: str,
        source_bucket: str,
        source_object: str,
        source_version_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        sse: Optional[Any] = None
    ) -> ObjectInfo:
        """Copy object"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        from minio.commonconfig import CopySource
        
        source = CopySource(
            source_bucket,
            source_object,
            version_id=source_version_id
        )
        
        result = self._client.copy_object(
            bucket_name,
            object_name,
            source,
            metadata=metadata,
            sse=sse
        )
        
        return ObjectInfo(
            bucket=bucket_name,
            name=object_name,
            size=0,  # Size not returned by copy
            etag=result.etag,
            last_modified=datetime.utcnow(),
            metadata=metadata or {},
            version_id=result.version_id
        )
        
    def presigned_get_object(
        self,
        bucket_name: str,
        object_name: str,
        expires: timedelta = timedelta(days=7),
        response_headers: Optional[Dict[str, str]] = None,
        version_id: Optional[str] = None
    ) -> str:
        """Generate presigned URL for GET"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        return self._client.presigned_get_object(
            bucket_name,
            object_name,
            expires=expires,
            response_headers=response_headers,
            version_id=version_id
        )
        
    def presigned_put_object(
        self,
        bucket_name: str,
        object_name: str,
        expires: timedelta = timedelta(days=7)
    ) -> str:
        """Generate presigned URL for PUT"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        return self._client.presigned_put_object(
            bucket_name,
            object_name,
            expires=expires
        )
        
    def set_bucket_versioning(
        self,
        bucket_name: str,
        enabled: bool
    ):
        """Enable/disable bucket versioning"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        from minio.versioningconfig import VersioningConfig, ENABLED, SUSPENDED
        
        config = VersioningConfig(ENABLED if enabled else SUSPENDED)
        self._client.set_bucket_versioning(bucket_name, config)
        
    def get_bucket_versioning(self, bucket_name: str) -> bool:
        """Get bucket versioning status"""
        if not self._client:
            raise RuntimeError("Not connected to MinIO")
            
        config = self._client.get_bucket_versioning(bucket_name)
        return config.status == "Enabled" 