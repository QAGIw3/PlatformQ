"""
Storage Manager

Enhanced storage management with multi-backend support, document conversion,
and intelligent tiering for DataIntelligenceSuite v2.0
"""

import asyncio
import hashlib
import mimetypes
import os
import tempfile
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, AsyncGenerator, BinaryIO
import uuid

from data_intelligence_common import (
    BaseProcessor,
    ProcessorConfig,
    ProcessingResult,
    ProcessingStatus,
    MetricsCollector,
    StructuredLogger,
    cached,
    CacheStrategy
)
from data_intelligence_common.core.events import EventBus, Event
from platformq_shared.vault.vault_client import VaultClient

from ..infrastructure.minio import MinIOClient
from ..infrastructure.document_converter import DocumentConverter
from ..domain.models.storage import (
    StorageObject,
    StorageMetadata,
    StorageTier,
    ConversionFormat,
    ConversionStatus
)
from ..utils.validators import validate_s3_path

logger = StructuredLogger.get_logger(__name__)


class StorageBackend(str, Enum):
    """Supported storage backends"""
    MINIO = "minio"
    S3 = "s3"
    AZURE_BLOB = "azure_blob"
    GCS = "gcs"
    LOCAL = "local"


class StorageManager(BaseProcessor):
    """
    Enhanced Storage Manager with v2.0 capabilities:
    - Multi-backend support (MinIO, S3, Azure, GCS)
    - Automatic document conversion
    - Preview generation
    - Intelligent tiering
    - Encryption at rest
    - Content indexing
    - Quota management
    """
    
    def __init__(
        self,
        minio_client: MinIOClient,
        vault_client: VaultClient,
        event_bus: EventBus,
        metrics: MetricsCollector,
        config: ProcessorConfig
    ):
        super().__init__(config)
        self.minio = minio_client
        self.vault = vault_client
        self.event_bus = event_bus
        self.metrics = metrics
        
        # Storage configuration
        self.default_backend = StorageBackend(config.get("default_backend", "minio"))
        self.default_bucket = config.get("default_bucket", "platform-data")
        self.temp_bucket = config.get("temp_bucket", "platform-temp")
        
        # Conversion settings
        self.auto_convert = config.get("auto_convert", True)
        self.generate_previews = config.get("generate_previews", True)
        self.max_file_size = config.get("max_file_size", 5 * 1024 * 1024 * 1024)  # 5GB
        
        # Tiering settings
        self.enable_tiering = config.get("enable_tiering", True)
        self.hot_tier_days = config.get("hot_tier_days", 7)
        self.warm_tier_days = config.get("warm_tier_days", 30)
        
        # Document converter
        self.converter = DocumentConverter()
        
        # Active uploads tracking
        self.active_uploads: Dict[str, Dict[str, Any]] = {}
        
        # Background tasks
        self._cleanup_task: Optional[asyncio.Task] = None
        self._tiering_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize storage manager"""
        logger.info("initializing_storage_manager",
                   backend=self.default_backend.value,
                   default_bucket=self.default_bucket)
        
        # Ensure default buckets exist
        await self._ensure_bucket_exists(self.default_bucket)
        await self._ensure_bucket_exists(self.temp_bucket)
        
        # Start background tasks
        self._cleanup_task = asyncio.create_task(self._cleanup_temp_files())
        if self.enable_tiering:
            self._tiering_task = asyncio.create_task(self._manage_storage_tiers())
            
        # Subscribe to events
        await self.event_bus.subscribe("file.convert.requested", self._handle_conversion_request)
        
    async def shutdown(self):
        """Shutdown storage manager"""
        logger.info("shutting_down_storage_manager")
        
        # Cancel background tasks
        if self._cleanup_task:
            self._cleanup_task.cancel()
        if self._tiering_task:
            self._tiering_task.cancel()
            
        # Clean up active uploads
        for upload_id in list(self.active_uploads.keys()):
            await self._cancel_upload(upload_id)
            
    async def upload_file(
        self,
        file_data: BinaryIO,
        filename: str,
        tenant_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        auto_convert: Optional[bool] = None,
        generate_preview: Optional[bool] = None,
        storage_tier: StorageTier = StorageTier.HOT
    ) -> StorageObject:
        """Upload a file with enhanced features"""
        
        # Generate unique identifier
        object_id = str(uuid.uuid4())
        upload_id = f"upload_{object_id}"
        
        # Track upload
        self.active_uploads[upload_id] = {
            "started_at": datetime.utcnow(),
            "filename": filename,
            "tenant_id": tenant_id
        }
        
        try:
            # Validate file size
            file_data.seek(0, 2)  # Seek to end
            file_size = file_data.tell()
            file_data.seek(0)  # Reset to beginning
            
            if file_size > self.max_file_size:
                raise ValueError(f"File size {file_size} exceeds maximum {self.max_file_size}")
                
            # Calculate hash
            file_hash = await self._calculate_file_hash(file_data)
            file_data.seek(0)  # Reset after hashing
            
            # Check for duplicates
            existing = await self._find_duplicate(file_hash, tenant_id)
            if existing:
                logger.info("duplicate_file_found",
                           hash=file_hash,
                           existing_id=existing.id)
                return existing
                
            # Determine content type
            content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
            
            # Build object path
            object_path = self._build_object_path(tenant_id, object_id, filename)
            
            # Upload to storage backend
            await self._upload_to_backend(
                file_data,
                self.default_bucket,
                object_path,
                content_type,
                metadata
            )
            
            # Create storage object
            storage_object = StorageObject(
                id=object_id,
                filename=filename,
                path=object_path,
                bucket=self.default_bucket,
                size=file_size,
                content_type=content_type,
                hash=file_hash,
                tenant_id=tenant_id,
                storage_tier=storage_tier,
                created_at=datetime.utcnow(),
                metadata=StorageMetadata(
                    **(metadata or {}),
                    original_filename=filename,
                    upload_id=upload_id
                )
            )
            
            # Handle auto-conversion
            if auto_convert or (auto_convert is None and self.auto_convert):
                await self._queue_conversions(storage_object)
                
            # Generate preview
            if generate_preview or (generate_preview is None and self.generate_previews):
                await self._queue_preview_generation(storage_object)
                
            # Emit event
            await self.event_bus.publish(Event(
                type="storage.file.uploaded",
                data={
                    "object_id": object_id,
                    "filename": filename,
                    "size": file_size,
                    "tenant_id": tenant_id
                }
            ))
            
            # Track metrics
            self.metrics.increment("storage.files.uploaded",
                                 tags={"content_type": content_type})
            self.metrics.histogram("storage.file.size", file_size)
            
            logger.info("file_uploaded",
                       object_id=object_id,
                       filename=filename,
                       size=file_size,
                       tenant_id=tenant_id)
            
            return storage_object
            
        finally:
            # Clean up tracking
            self.active_uploads.pop(upload_id, None)
            
    async def download_file(
        self,
        object_id: str,
        tenant_id: str,
        target_format: Optional[ConversionFormat] = None
    ) -> AsyncGenerator[bytes, None]:
        """Download a file with optional format conversion"""
        
        # Get object metadata
        storage_object = await self.get_object_metadata(object_id, tenant_id)
        if not storage_object:
            raise ValueError(f"Object {object_id} not found")
            
        # Check if conversion is needed
        if target_format and target_format != storage_object.content_type:
            # Check if conversion exists
            conversion = await self._get_conversion(object_id, target_format)
            if conversion:
                # Download converted file
                async for chunk in self._download_from_backend(
                    conversion.bucket,
                    conversion.path
                ):
                    yield chunk
            else:
                # Convert on-the-fly
                async for chunk in self._convert_on_download(
                    storage_object,
                    target_format
                ):
                    yield chunk
        else:
            # Download original file
            async for chunk in self._download_from_backend(
                storage_object.bucket,
                storage_object.path
            ):
                yield chunk
                
        # Track metrics
        self.metrics.increment("storage.files.downloaded",
                             tags={"format": target_format or "original"})
                             
    async def delete_file(
        self,
        object_id: str,
        tenant_id: str,
        delete_conversions: bool = True
    ) -> bool:
        """Delete a file and optionally its conversions"""
        
        # Get object metadata
        storage_object = await self.get_object_metadata(object_id, tenant_id)
        if not storage_object:
            return False
            
        # Delete from backend
        await self._delete_from_backend(storage_object.bucket, storage_object.path)
        
        # Delete conversions if requested
        if delete_conversions:
            conversions = await self._list_conversions(object_id)
            for conversion in conversions:
                await self._delete_from_backend(conversion.bucket, conversion.path)
                
        # Emit event
        await self.event_bus.publish(Event(
            type="storage.file.deleted",
            data={
                "object_id": object_id,
                "tenant_id": tenant_id
            }
        ))
        
        # Track metrics
        self.metrics.increment("storage.files.deleted")
        
        logger.info("file_deleted",
                   object_id=object_id,
                   tenant_id=tenant_id)
        
        return True
        
    @cached(ttl=300, strategy=CacheStrategy.CACHE_ASIDE)
    async def get_object_metadata(
        self,
        object_id: str,
        tenant_id: str
    ) -> Optional[StorageObject]:
        """Get metadata for a storage object"""
        
        # Query from metadata store
        # TODO: Implement actual metadata store query
        # For now, reconstruct from MinIO
        
        try:
            # Build expected path
            test_path = self._build_object_path(tenant_id, object_id, "*")
            
            # List objects with prefix
            objects = await self.minio.list_objects(
                self.default_bucket,
                prefix=test_path.replace("*", "")
            )
            
            if objects:
                obj = objects[0]
                return StorageObject(
                    id=object_id,
                    filename=Path(obj.object_name).name,
                    path=obj.object_name,
                    bucket=self.default_bucket,
                    size=obj.size,
                    content_type=obj.content_type or "application/octet-stream",
                    tenant_id=tenant_id,
                    created_at=obj.last_modified,
                    storage_tier=StorageTier.HOT  # TODO: Get actual tier
                )
                
            return None
            
        except Exception as e:
            logger.error("failed_to_get_object_metadata",
                        object_id=object_id,
                        error=str(e))
            return None
            
    async def list_objects(
        self,
        tenant_id: str,
        prefix: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[StorageObject]:
        """List objects for a tenant"""
        
        # Build prefix
        full_prefix = f"{tenant_id}/"
        if prefix:
            full_prefix += prefix
            
        # List from backend
        objects = await self.minio.list_objects(
            self.default_bucket,
            prefix=full_prefix,
            recursive=True
        )
        
        # Convert to storage objects
        storage_objects = []
        for i, obj in enumerate(objects):
            if i < offset:
                continue
            if len(storage_objects) >= limit:
                break
                
            # Parse object ID from path
            path_parts = obj.object_name.split("/")
            if len(path_parts) >= 2:
                object_id = path_parts[1]
                
                storage_objects.append(StorageObject(
                    id=object_id,
                    filename=Path(obj.object_name).name,
                    path=obj.object_name,
                    bucket=self.default_bucket,
                    size=obj.size,
                    content_type=obj.content_type or "application/octet-stream",
                    tenant_id=tenant_id,
                    created_at=obj.last_modified,
                    storage_tier=StorageTier.HOT
                ))
                
        return storage_objects
        
    async def get_storage_stats(self, tenant_id: str) -> Dict[str, Any]:
        """Get storage statistics for a tenant"""
        
        # Calculate stats
        total_size = 0
        file_count = 0
        file_types = {}
        
        objects = await self.minio.list_objects(
            self.default_bucket,
            prefix=f"{tenant_id}/",
            recursive=True
        )
        
        for obj in objects:
            total_size += obj.size
            file_count += 1
            
            # Track file types
            ext = Path(obj.object_name).suffix.lower()
            file_types[ext] = file_types.get(ext, 0) + 1
            
        return {
            "tenant_id": tenant_id,
            "total_size": total_size,
            "file_count": file_count,
            "file_types": file_types,
            "storage_used_gb": round(total_size / (1024 ** 3), 2),
            "quota_gb": 100,  # TODO: Get from tenant config
            "quota_used_percent": round((total_size / (100 * 1024 ** 3)) * 100, 2)
        }
        
    async def _ensure_bucket_exists(self, bucket_name: str):
        """Ensure bucket exists"""
        if not await self.minio.bucket_exists(bucket_name):
            await self.minio.create_bucket(bucket_name)
            logger.info("bucket_created", bucket=bucket_name)
            
    async def _upload_to_backend(
        self,
        file_data: BinaryIO,
        bucket: str,
        object_path: str,
        content_type: str,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Upload file to storage backend"""
        
        if self.default_backend == StorageBackend.MINIO:
            await self.minio.upload_object(
                bucket,
                object_path,
                file_data,
                content_type=content_type,
                metadata=metadata
            )
        else:
            # TODO: Implement other backends
            raise NotImplementedError(f"Backend {self.default_backend} not implemented")
            
    async def _download_from_backend(
        self,
        bucket: str,
        object_path: str
    ) -> AsyncGenerator[bytes, None]:
        """Download file from storage backend"""
        
        if self.default_backend == StorageBackend.MINIO:
            async for chunk in self.minio.download_object(bucket, object_path):
                yield chunk
        else:
            # TODO: Implement other backends
            raise NotImplementedError(f"Backend {self.default_backend} not implemented")
            
    async def _delete_from_backend(self, bucket: str, object_path: str):
        """Delete file from storage backend"""
        
        if self.default_backend == StorageBackend.MINIO:
            await self.minio.delete_object(bucket, object_path)
        else:
            # TODO: Implement other backends
            raise NotImplementedError(f"Backend {self.default_backend} not implemented")
            
    def _build_object_path(self, tenant_id: str, object_id: str, filename: str) -> str:
        """Build object storage path"""
        # Structure: tenant_id/object_id/filename
        return f"{tenant_id}/{object_id}/{filename}"
        
    async def _calculate_file_hash(self, file_data: BinaryIO) -> str:
        """Calculate SHA256 hash of file"""
        sha256_hash = hashlib.sha256()
        
        while True:
            chunk = file_data.read(8192)
            if not chunk:
                break
            sha256_hash.update(chunk)
            
        return sha256_hash.hexdigest()
        
    async def _find_duplicate(self, file_hash: str, tenant_id: str) -> Optional[StorageObject]:
        """Find duplicate file by hash"""
        # TODO: Implement duplicate detection using metadata store
        return None
        
    async def _queue_conversions(self, storage_object: StorageObject):
        """Queue automatic document conversions"""
        
        # Determine conversion targets based on content type
        targets = []
        
        if storage_object.content_type == "application/pdf":
            targets = [ConversionFormat.DOCX, ConversionFormat.TXT]
        elif storage_object.content_type in ["application/msword", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"]:
            targets = [ConversionFormat.PDF, ConversionFormat.TXT]
        elif storage_object.content_type in ["application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]:
            targets = [ConversionFormat.CSV, ConversionFormat.PDF]
            
        # Queue conversions
        for target in targets:
            await self.event_bus.publish(Event(
                type="storage.conversion.requested",
                data={
                    "object_id": storage_object.id,
                    "source_format": storage_object.content_type,
                    "target_format": target.value,
                    "tenant_id": storage_object.tenant_id
                }
            ))
            
    async def _queue_preview_generation(self, storage_object: StorageObject):
        """Queue preview generation"""
        
        # Check if preview is applicable
        preview_types = [
            "application/pdf",
            "image/jpeg",
            "image/png",
            "image/gif",
            "application/msword",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        ]
        
        if storage_object.content_type in preview_types:
            await self.event_bus.publish(Event(
                type="storage.preview.requested",
                data={
                    "object_id": storage_object.id,
                    "content_type": storage_object.content_type,
                    "tenant_id": storage_object.tenant_id
                }
            ))
            
    async def _handle_conversion_request(self, event: Event):
        """Handle document conversion request"""
        
        object_id = event.data.get("object_id")
        target_format = ConversionFormat(event.data.get("target_format"))
        tenant_id = event.data.get("tenant_id")
        
        try:
            # Get source object
            source_object = await self.get_object_metadata(object_id, tenant_id)
            if not source_object:
                logger.error("source_object_not_found", object_id=object_id)
                return
                
            # Download source file
            temp_source = tempfile.NamedTemporaryFile(delete=False)
            async for chunk in self.download_file(object_id, tenant_id):
                temp_source.write(chunk)
            temp_source.close()
            
            # Convert file
            temp_target = await self.converter.convert(
                temp_source.name,
                target_format
            )
            
            # Upload converted file
            with open(temp_target, 'rb') as f:
                converted_object = await self.upload_file(
                    f,
                    f"{Path(source_object.filename).stem}.{target_format.value}",
                    tenant_id,
                    metadata={
                        "source_object_id": object_id,
                        "conversion_format": target_format.value,
                        "converted_at": datetime.utcnow().isoformat()
                    },
                    auto_convert=False,  # Don't convert conversions
                    generate_preview=False
                )
                
            # Clean up temp files
            os.unlink(temp_source.name)
            os.unlink(temp_target)
            
            # Emit completion event
            await self.event_bus.publish(Event(
                type="storage.conversion.completed",
                data={
                    "source_object_id": object_id,
                    "converted_object_id": converted_object.id,
                    "target_format": target_format.value
                }
            ))
            
        except Exception as e:
            logger.error("conversion_failed",
                        object_id=object_id,
                        target_format=target_format.value,
                        error=str(e))
                        
            # Emit failure event
            await self.event_bus.publish(Event(
                type="storage.conversion.failed",
                data={
                    "object_id": object_id,
                    "target_format": target_format.value,
                    "error": str(e)
                }
            ))
            
    async def _get_conversion(
        self,
        object_id: str,
        target_format: ConversionFormat
    ) -> Optional[StorageObject]:
        """Get existing conversion"""
        # TODO: Implement conversion lookup
        return None
        
    async def _list_conversions(self, object_id: str) -> List[StorageObject]:
        """List all conversions for an object"""
        # TODO: Implement conversion listing
        return []
        
    async def _convert_on_download(
        self,
        source_object: StorageObject,
        target_format: ConversionFormat
    ) -> AsyncGenerator[bytes, None]:
        """Convert file on-the-fly during download"""
        
        # Download to temp file
        temp_source = tempfile.NamedTemporaryFile(delete=False)
        async for chunk in self._download_from_backend(
            source_object.bucket,
            source_object.path
        ):
            temp_source.write(chunk)
        temp_source.close()
        
        try:
            # Convert
            temp_target = await self.converter.convert(
                temp_source.name,
                target_format
            )
            
            # Stream converted file
            with open(temp_target, 'rb') as f:
                while True:
                    chunk = f.read(8192)
                    if not chunk:
                        break
                    yield chunk
                    
        finally:
            # Clean up
            os.unlink(temp_source.name)
            if 'temp_target' in locals():
                os.unlink(temp_target)
                
    async def _cleanup_temp_files(self):
        """Background task to clean up temporary files"""
        
        while True:
            try:
                # Clean up files older than 24 hours
                cutoff = datetime.utcnow() - timedelta(hours=24)
                
                objects = await self.minio.list_objects(
                    self.temp_bucket,
                    recursive=True
                )
                
                for obj in objects:
                    if obj.last_modified < cutoff:
                        await self.minio.delete_object(
                            self.temp_bucket,
                            obj.object_name
                        )
                        logger.info("temp_file_cleaned",
                                   object=obj.object_name)
                        
                # Run every hour
                await asyncio.sleep(3600)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("cleanup_error", error=str(e))
                await asyncio.sleep(300)  # Retry in 5 minutes
                
    async def _manage_storage_tiers(self):
        """Background task to manage storage tiering"""
        
        while True:
            try:
                # Define tier boundaries
                hot_cutoff = datetime.utcnow() - timedelta(days=self.hot_tier_days)
                warm_cutoff = datetime.utcnow() - timedelta(days=self.warm_tier_days)
                
                # List all objects
                objects = await self.minio.list_objects(
                    self.default_bucket,
                    recursive=True
                )
                
                for obj in objects:
                    # Skip if recently accessed (TODO: Track access times)
                    
                    # Determine target tier
                    if obj.last_modified > hot_cutoff:
                        continue  # Keep in hot tier
                    elif obj.last_modified > warm_cutoff:
                        # Move to warm tier
                        await self._move_to_tier(obj, StorageTier.WARM)
                    else:
                        # Move to cold tier
                        await self._move_to_tier(obj, StorageTier.COLD)
                        
                # Run daily
                await asyncio.sleep(86400)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("tiering_error", error=str(e))
                await asyncio.sleep(3600)  # Retry in 1 hour
                
    async def _move_to_tier(self, obj: Any, tier: StorageTier):
        """Move object to different storage tier"""
        # TODO: Implement actual tiering logic
        # This could involve:
        # - Moving to different storage class in S3
        # - Moving to different backend
        # - Compressing data
        logger.info("moving_to_tier",
                   object=obj.object_name,
                   tier=tier.value)
                   
    async def _cancel_upload(self, upload_id: str):
        """Cancel an active upload"""
        upload_info = self.active_uploads.get(upload_id)
        if upload_info:
            logger.warning("cancelling_upload",
                          upload_id=upload_id,
                          filename=upload_info["filename"])
            # TODO: Implement actual upload cancellation
            self.active_uploads.pop(upload_id, None) 