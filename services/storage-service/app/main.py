"""
Storage Service

Enhanced storage platform for PlatformQ providing:
- Object storage operations (MinIO/S3)
- Document conversion and preview generation
- Multi-tenant file isolation and quotas
- License-based access control
- Streaming and chunked uploads
- Content indexing for search
"""

import asyncio
import logging
import os
import uuid
from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path
import tempfile
import shutil

from fastapi import Depends, HTTPException, UploadFile, File, Query, BackgroundTasks, status
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
import httpx

from platformq_shared import (
    create_base_app,
    EventProcessor,
    event_handler,
    ProcessingResult,
    ProcessingStatus,
    ServiceClients,
    NotFoundError,
    ValidationError
)
from platformq_shared.event_publisher import EventPublisher
from platformq_events import DocumentConversionRequestedEvent, DocumentConversionCompletedEvent

from .api.deps import get_current_tenant_and_user
from .storage.base import BaseStorage
from .storage.factory import get_storage_backend
from .conversion.converter import DocumentConverter, ConversionFormat
from .conversion.async_converter import AsyncDocumentConverter
from .conversion.status_manager import ConversionStatusManager
from .conversion.preview_generator import PreviewGenerator

logger = logging.getLogger(__name__)


# Models
class ConversionRequest(BaseModel):
    """Document conversion request"""
    source_identifier: str = Field(..., description="Storage identifier of source document")
    target_format: ConversionFormat = Field(..., description="Target format")
    options: Optional[Dict[str, Any]] = Field({}, description="Conversion options")
    callback_url: Optional[str] = Field(None, description="Webhook for completion notification")


class ConversionStatus(BaseModel):
    """Conversion job status"""
    job_id: str
    status: str  # pending, processing, completed, failed
    source_identifier: str
    target_format: str
    output_identifier: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    progress: Optional[float] = Field(None, ge=0, le=100)


class BulkConversionRequest(BaseModel):
    """Bulk conversion request"""
    conversions: List[ConversionRequest]
    notify_on_completion: bool = True


class PreviewRequest(BaseModel):
    """Document preview generation request"""
    identifier: str
    preview_type: str = "thumbnail"  # thumbnail, first_page, text_extract
    options: Optional[Dict[str, Any]] = {}


# Event Processor
class ConversionEventProcessor(EventProcessor):
    """Process document conversion events"""
    
    def __init__(self, converter: AsyncDocumentConverter, status_manager: ConversionStatusManager):
        super().__init__(
            service_name="storage-proxy-service",
            pulsar_url="pulsar://pulsar:6650"
        )
        self.converter = converter
        self.status_manager = status_manager
        
    async def on_start(self):
        """Initialize processor"""
        logger.info("Starting conversion event processor")
        
    async def on_stop(self):
        """Cleanup processor"""
        logger.info("Stopping conversion event processor")
        
    @event_handler("persistent://platformq/*/document-conversion-requests", DocumentConversionRequestedEvent)
    async def handle_conversion_request(self, event: DocumentConversionRequestedEvent, msg):
        """Process conversion request from event"""
        try:
            # Create conversion job
            job_id = await self.converter.convert_async(
                source_identifier=event.source_identifier,
                target_format=event.target_format,
                tenant_id=event.tenant_id,
                options=event.options
            )
            
            # Update status
            await self.status_manager.update_status(
                job_id=job_id,
                status="processing"
            )
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing conversion event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                message=str(e)
            )


# Initialize services
document_converter = None
async_converter = None
status_manager = None
preview_generator = None
conversion_processor = None


# Create app
app = create_base_app(
    service_name="storage-service",
    db_session_dependency=lambda: None,
    api_key_crud_dependency=lambda: None,
    user_crud_dependency=lambda: None,
    password_verifier_dependency=lambda: None
)


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global document_converter, async_converter, status_manager, preview_generator, conversion_processor
    
    # Initialize converters
    document_converter = DocumentConverter()
    status_manager = ConversionStatusManager()
    async_converter = AsyncDocumentConverter(
        converter=document_converter,
        status_manager=status_manager,
        storage_backend=get_storage_backend()
    )
    preview_generator = PreviewGenerator()
    
    # Initialize event processor
    conversion_processor = ConversionEventProcessor(
        converter=async_converter,
        status_manager=status_manager
    )
    
    # Start background workers
    asyncio.create_task(async_converter.start_worker())
    
    # Start event processor
    await conversion_processor.start()
    
    logger.info("Storage proxy service initialized with conversion capabilities")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    if conversion_processor:
        await conversion_processor.stop()
        
    if async_converter:
        await async_converter.stop_worker()


# Original storage endpoints

@app.post("/api/v1/upload")
async def upload_file(
    context: dict = Depends(get_current_tenant_and_user),
    file: UploadFile = File(...),
    storage: BaseStorage = Depends(get_storage_backend),
    auto_convert: bool = Query(False, description="Auto-convert to common formats"),
    preview: bool = Query(True, description="Generate preview/thumbnail")
):
    """
    Upload a file to storage with optional auto-conversion and preview generation.
    """
    tenant_id = context.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Tenant ID not found in context.")
        
    # Upload file
    identifier = await storage.upload(file, tenant_id)
    
    result = {
        "identifier": identifier,
        "filename": file.filename,
        "size": file.size,
        "content_type": file.content_type
    }
    
    # Auto-convert if requested
    if auto_convert and document_converter.is_convertible(file.filename):
        # Determine target formats based on source
        target_formats = document_converter.get_recommended_formats(file.filename)
        
        conversions = []
        for target_format in target_formats:
            job_id = await async_converter.convert_async(
                source_identifier=identifier,
                target_format=target_format,
                tenant_id=tenant_id
            )
            conversions.append({
                "job_id": job_id,
                "target_format": target_format
            })
            
        result["conversions"] = conversions
        
    # Generate preview if requested
    if preview and preview_generator.can_generate_preview(file.filename):
        preview_id = await preview_generator.generate_preview_async(
            identifier=identifier,
            tenant_id=tenant_id,
            storage=storage
        )
        result["preview_id"] = preview_id
        
    return result


async def check_license_validity(
    asset_id: str,
    user_address: str,
    license_id: Optional[str] = None
) -> dict:
    """
    Check if user has a valid license for the asset.
    Returns license info if valid, raises exception otherwise.
    """
    # In production, this would call the verifiable-credential-service
    # to check on-chain license validity
    vc_service_url = "http://verifiable-credential-service/api/v1"
    
    try:
        async with httpx.AsyncClient() as client:
            # Check for active licenses
            response = await client.get(
                f"{vc_service_url}/licenses/check",
                params={
                    "asset_id": asset_id,
                    "user_address": user_address,
                    "license_id": license_id
                }
            )
            
            if response.status_code == 200:
                license_data = response.json()
                if license_data.get("valid"):
                    return license_data
                else:
                    raise HTTPException(
                        status_code=403,
                        detail=f"License expired or invalid: {license_data.get('reason')}"
                    )
            else:
                raise HTTPException(
                    status_code=403,
                    detail="No valid license found for this asset"
                )
    except httpx.RequestError as e:
        logger.error(f"Failed to check license: {e}")
        # Fail closed - deny access if we can't verify
        raise HTTPException(
            status_code=503,
            detail="Unable to verify license at this time"
        )


@app.get("/api/v1/download/{identifier}")
async def download_file(
    identifier: str,
    asset_id: Optional[str] = Query(None, description="Asset ID for license verification"),
    license_id: Optional[str] = Query(None, description="Specific license ID to use"),
    context: dict = Depends(get_current_tenant_and_user),
    storage: BaseStorage = Depends(get_storage_backend),
    require_license: bool = Query(False, description="Enforce license check"),
    format: Optional[str] = Query(None, description="Download in specific format")
):
    """
    Download a file from storage with optional format conversion.
    """
    headers = {}
    # Check license if required
    if require_license and asset_id:
        user = context.get("user", {})
        user_blockchain_address = user.get("blockchain_address")

        if not user_blockchain_address:
            raise HTTPException(
                status_code=401,
                detail="User must have blockchain address for licensed content"
            )
        
        # Verify license
        license_info = await check_license_validity(
            asset_id=asset_id,
            user_address=user_blockchain_address,
            license_id=license_id
        )
        
        # Record usage if license has usage limits
        if license_info.get("max_usage") and license_info.get("max_usage") > 0:
            # In production, this would call smart contract to record usage
            logger.info(f"Recording usage for license {license_info.get('license_id')}")

        headers["X-License-Type"] = license_info.get("license_type", "standard")
        headers["X-License-Expires"] = str(license_info.get("expires_at", ""))

    tenant_id = context.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Tenant ID not found in context.")

    # Check if format conversion is requested
    if format:
        # Check if conversion already exists
        converted_identifier = await async_converter.get_converted_identifier(
            source_identifier=identifier,
            target_format=format,
            tenant_id=tenant_id
        )
        
        if converted_identifier:
            # Use pre-converted file
            identifier = converted_identifier
        else:
            # Convert on-the-fly (synchronous for immediate download)
            temp_file = await storage.download_to_temp(identifier, tenant_id)
            converted_file = await document_converter.convert(
                source_path=temp_file,
                target_format=format
            )
            
            # Upload converted file
            with open(converted_file, 'rb') as f:
                converted_identifier = await storage.upload_from_bytes(
                    data=f.read(),
                    filename=f"{Path(identifier).stem}.{format}",
                    tenant_id=tenant_id
                )
                
            # Clean up temp files
            os.unlink(temp_file)
            os.unlink(converted_file)
            
            identifier = converted_identifier

    return StreamingResponse(
        storage.download(identifier, tenant_id),
        media_type="application/octet-stream",
        headers=headers
    )


# Document conversion endpoints

@app.post("/api/v1/convert", response_model=ConversionStatus)
async def convert_document(
    request: ConversionRequest,
    context: dict = Depends(get_current_tenant_and_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Convert a document to a different format asynchronously.
    """
    tenant_id = context.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Tenant ID not found")
        
    # Validate source exists
    storage = get_storage_backend()
    if not await storage.exists(request.source_identifier, tenant_id):
        raise NotFoundError("Source document", request.source_identifier)
        
    # Create conversion job
    job_id = await async_converter.convert_async(
        source_identifier=request.source_identifier,
        target_format=request.target_format,
        tenant_id=tenant_id,
        options=request.options,
        callback_url=request.callback_url
    )
    
    # Get initial status
    status = await status_manager.get_status(job_id)
    
    # Publish event
    event = DocumentConversionRequestedEvent(
        job_id=job_id,
        source_identifier=request.source_identifier,
        target_format=request.target_format.value,
        tenant_id=tenant_id,
        options=request.options
    )
    await app.state.event_publisher.publish_event("document-conversion-requested", event)
    
    return status


@app.post("/api/v1/convert/batch")
async def convert_documents_batch(
    request: BulkConversionRequest,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Convert multiple documents in batch.
    """
    tenant_id = context.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Tenant ID not found")
        
    jobs = []
    for conversion in request.conversions:
        job_id = await async_converter.convert_async(
            source_identifier=conversion.source_identifier,
            target_format=conversion.target_format,
            tenant_id=tenant_id,
            options=conversion.options,
            callback_url=conversion.callback_url
        )
        jobs.append({
            "job_id": job_id,
            "source": conversion.source_identifier,
            "target_format": conversion.target_format.value
        })
        
    return {
        "batch_id": str(uuid.uuid4()),
        "jobs": jobs,
        "notify_on_completion": request.notify_on_completion
    }


@app.get("/api/v1/convert/status/{job_id}", response_model=ConversionStatus)
async def get_conversion_status(
    job_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Get the status of a conversion job.
    """
    status = await status_manager.get_status(job_id)
    
    if not status:
        raise NotFoundError("Conversion job", job_id)
        
    # Verify tenant access
    tenant_id = context.get("tenant_id")
    if status.get("tenant_id") != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied")
        
    return ConversionStatus(**status)


@app.get("/api/v1/convert/formats")
async def get_supported_formats():
    """
    Get list of supported conversion formats.
    """
    return {
        "document_formats": [
            {"format": "pdf", "mime_type": "application/pdf", "extensions": [".pdf"]},
            {"format": "docx", "mime_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "extensions": [".docx"]},
            {"format": "html", "mime_type": "text/html", "extensions": [".html", ".htm"]},
            {"format": "txt", "mime_type": "text/plain", "extensions": [".txt"]},
            {"format": "rtf", "mime_type": "application/rtf", "extensions": [".rtf"]},
            {"format": "odt", "mime_type": "application/vnd.oasis.opendocument.text", "extensions": [".odt"]},
            {"format": "epub", "mime_type": "application/epub+zip", "extensions": [".epub"]}
        ],
        "spreadsheet_formats": [
            {"format": "xlsx", "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "extensions": [".xlsx"]},
            {"format": "csv", "mime_type": "text/csv", "extensions": [".csv"]},
            {"format": "ods", "mime_type": "application/vnd.oasis.opendocument.spreadsheet", "extensions": [".ods"]}
        ],
        "presentation_formats": [
            {"format": "pptx", "mime_type": "application/vnd.openxmlformats-officedocument.presentationml.presentation", "extensions": [".pptx"]},
            {"format": "odp", "mime_type": "application/vnd.oasis.opendocument.presentation", "extensions": [".odp"]}
        ],
        "image_formats": [
            {"format": "png", "mime_type": "image/png", "extensions": [".png"]},
            {"format": "jpg", "mime_type": "image/jpeg", "extensions": [".jpg", ".jpeg"]},
            {"format": "webp", "mime_type": "image/webp", "extensions": [".webp"]},
            {"format": "svg", "mime_type": "image/svg+xml", "extensions": [".svg"]}
        ]
    }


@app.post("/api/v1/preview", response_model=Dict[str, Any])
async def generate_preview(
    request: PreviewRequest,
    context: dict = Depends(get_current_tenant_and_user),
    storage: BaseStorage = Depends(get_storage_backend)
):
    """
    Generate a preview (thumbnail, text extract, etc.) for a document.
    """
    tenant_id = context.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Tenant ID not found")
        
    # Validate source exists
    if not await storage.exists(request.identifier, tenant_id):
        raise NotFoundError("Document", request.identifier)
        
    # Generate preview
    preview_id = await preview_generator.generate_preview_async(
        identifier=request.identifier,
        tenant_id=tenant_id,
        storage=storage,
        preview_type=request.preview_type,
        options=request.options
    )
    
    return {
        "preview_id": preview_id,
        "type": request.preview_type,
        "status": "processing"
    }


# Utility endpoints

@app.get("/api/v1/storage/quota")
async def get_storage_quota(
    context: dict = Depends(get_current_tenant_and_user),
    storage: BaseStorage = Depends(get_storage_backend)
):
    """
    Get storage quota and usage for the tenant.
    """
    tenant_id = context.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Tenant ID not found")
        
    usage = await storage.get_tenant_usage(tenant_id)
    quota = await storage.get_tenant_quota(tenant_id)
    
    return {
        "used_bytes": usage,
        "quota_bytes": quota,
        "used_percentage": (usage / quota * 100) if quota > 0 else 0,
        "remaining_bytes": max(0, quota - usage)
    }


@app.delete("/api/v1/storage/{identifier}")
async def delete_file(
    identifier: str,
    context: dict = Depends(get_current_tenant_and_user),
    storage: BaseStorage = Depends(get_storage_backend)
):
    """
    Delete a file from storage.
    """
    tenant_id = context.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Tenant ID not found")
        
    # Check if file exists
    if not await storage.exists(identifier, tenant_id):
        raise NotFoundError("File", identifier)
        
    # Delete file and any associated conversions
    await storage.delete(identifier, tenant_id)
    await async_converter.delete_conversions(identifier, tenant_id)
    
    return {"status": "deleted", "identifier": identifier}


@app.get("/api/v1/storage/list")
async def list_files(
    prefix: Optional[str] = Query(None, description="Filter by prefix"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    context: dict = Depends(get_current_tenant_and_user),
    storage: BaseStorage = Depends(get_storage_backend)
):
    """
    List files in storage with pagination.
    """
    tenant_id = context.get("tenant_id")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Tenant ID not found")
        
    files = await storage.list_files(
        tenant_id=tenant_id,
        prefix=prefix,
        limit=limit,
        offset=offset
    )
    
    # Enrich with conversion info
    for file in files:
        conversions = await async_converter.get_available_conversions(
            source_identifier=file["identifier"],
            tenant_id=tenant_id
        )
        file["available_formats"] = conversions
        
    return {
        "files": files,
        "total": await storage.count_files(tenant_id, prefix),
        "limit": limit,
        "offset": offset
    }


@app.get("/health")
async def health_check():
    """Enhanced health check with conversion service status"""
    
    health = {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "services": {}
    }
    
    # Check storage backend
    try:
        storage = get_storage_backend()
        await storage.health_check()
        health["services"]["storage"] = "healthy"
    except Exception as e:
        health["services"]["storage"] = f"unhealthy: {str(e)}"
        health["status"] = "degraded"
        
    # Check conversion service
    try:
        if document_converter and document_converter.is_available():
            health["services"]["conversion"] = "healthy"
        else:
            health["services"]["conversion"] = "unavailable"
            health["status"] = "degraded"
    except Exception as e:
        health["services"]["conversion"] = f"unhealthy: {str(e)}"
        health["status"] = "degraded"
        
    # Check async converter worker
    if async_converter and async_converter.is_worker_running():
        health["services"]["async_worker"] = "healthy"
    else:
        health["services"]["async_worker"] = "not running"
        health["status"] = "degraded"
        
    return health


@app.get("/")
async def read_root():
    return {
        "service": "storage-service",
        "version": "3.0.0",
        "status": "operational",
        "description": "Enhanced storage platform for PlatformQ",
        "capabilities": {
            "storage": {
                "backend": "minio-s3",
                "multi_tenancy": True,
                "quota_management": True,
                "streaming_upload": True,
                "chunked_upload": True,
                "versioning": True
            },
            "conversion": {
                "documents": ["pdf", "docx", "xlsx", "pptx", "odt", "rtf"],
                "images": ["jpg", "png", "webp", "svg", "tiff"],
                "async_processing": True,
                "batch_conversion": True
            },
            "preview": {
                "thumbnail_generation": True,
                "text_extraction": True,
                "metadata_extraction": True,
                "configurable_quality": True
            },
            "security": {
                "tenant_isolation": True,
                "license_verification": True,
                "access_control": True,
                "encryption_at_rest": True,
                "signed_urls": True
            },
            "indexing": {
                "content_search": True,
                "metadata_search": True,
                "elasticsearch_integration": True
            }
        }
    }
