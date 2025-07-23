"""
Storage API routes.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Query, Body, BackgroundTasks
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
import io

from app.engines.storage import (
    StorageManager,
    DocumentConverter,
    PreviewGenerator,
    ContentIndexer,
    QuotaManager,
    StorageBackend,
    StorageObject,
    UploadOptions,
    DownloadOptions,
    ConversionFormat,
    ConversionOptions,
    PreviewType,
    PreviewOptions,
    SearchQuery,
    TenantQuota,
    QuotaPolicy
)
from app.core.dependencies import (
    get_storage_manager,
    get_document_converter,
    get_preview_generator,
    get_content_indexer,
    get_quota_manager,
    get_current_tenant_id
)

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter()


# Request/Response Models
class UploadResponse(BaseModel):
    """Upload response."""
    identifier: str
    filename: str
    size: int
    content_type: str
    checksum: str
    created_at: datetime


class ConversionRequest(BaseModel):
    """Document conversion request."""
    source_identifier: str
    target_format: ConversionFormat
    options: ConversionOptions = Field(default_factory=ConversionOptions)


class PreviewRequest(BaseModel):
    """Preview generation request."""
    identifier: str
    preview_type: PreviewType = PreviewType.THUMBNAIL
    options: PreviewOptions = Field(default_factory=PreviewOptions)


class SearchRequest(BaseModel):
    """Search request."""
    query: str
    filters: Dict[str, Any] = Field(default_factory=dict)
    from_: int = Field(0, alias="from")
    size: int = 10
    sort: List[Dict[str, str]] = Field(default_factory=list)
    highlight: bool = True


class QuotaRequest(BaseModel):
    """Quota update request."""
    storage_bytes: int
    bandwidth_bytes: int
    file_count: int
    max_file_size: int
    policy: QuotaPolicy = QuotaPolicy.HARD


# Storage Endpoints
@router.post("/upload", response_model=UploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    tags: Optional[str] = Query(None, description="Comma-separated tags"),
    metadata: Optional[str] = Query(None, description="JSON metadata"),
    auto_convert: bool = Query(False, description="Auto-convert to common formats"),
    generate_preview: bool = Query(True, description="Generate preview"),
    tenant_id: str = Depends(get_current_tenant_id),
    storage_manager: StorageManager = Depends(get_storage_manager),
    document_converter: DocumentConverter = Depends(get_document_converter),
    preview_generator: PreviewGenerator = Depends(get_preview_generator),
    quota_manager: QuotaManager = Depends(get_quota_manager),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Upload a file to storage."""
    try:
        # Check quota
        file_size = file.size or 0
        quota_check = await quota_manager.check_quota(tenant_id, file_size, "upload")
        
        if not quota_check["allowed"]:
            raise HTTPException(status_code=403, detail=quota_check["reason"])
        
        # Parse options
        upload_options = UploadOptions()
        if tags:
            upload_options.tags = {f"tag_{i}": tag for i, tag in enumerate(tags.split(","))}
        if metadata:
            import json
            upload_options.metadata = json.loads(metadata)
        
        # Read file content
        content = await file.read()
        
        # Upload file
        identifier = await storage_manager.upload(
            data=content,
            filename=file.filename,
            tenant_id=tenant_id,
            options=upload_options
        )
        
        # Get metadata
        storage_metadata = await storage_manager.get_object_metadata(identifier, tenant_id)
        
        # Auto-convert if requested
        if auto_convert and file.filename:
            # Check if convertible
            source_format = None
            for format_enum in ConversionFormat:
                if file.filename.lower().endswith(f".{format_enum.value}"):
                    source_format = format_enum
                    break
            
            if source_format:
                # Get recommended formats
                target_formats = document_converter.get_supported_conversions(source_format)[:3]
                
                for target_format in target_formats:
                    background_tasks.add_task(
                        document_converter.convert_async,
                        identifier,
                        target_format,
                        tenant_id
                    )
        
        # Generate preview if requested
        if generate_preview and preview_generator.can_generate_preview(file.filename):
            background_tasks.add_task(
                preview_generator.generate_preview_async,
                identifier,
                PreviewType.THUMBNAIL,
                tenant_id
            )
        
        return UploadResponse(
            identifier=identifier,
            filename=file.filename,
            size=storage_metadata.size,
            content_type=storage_metadata.content_type,
            checksum=storage_metadata.checksum,
            created_at=storage_metadata.created_at
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/download/{identifier}")
async def download_file(
    identifier: str,
    version_id: Optional[str] = Query(None),
    format: Optional[ConversionFormat] = Query(None, description="Download in specific format"),
    tenant_id: str = Depends(get_current_tenant_id),
    storage_manager: StorageManager = Depends(get_storage_manager),
    document_converter: DocumentConverter = Depends(get_document_converter),
    quota_manager: QuotaManager = Depends(get_quota_manager)
):
    """Download a file from storage."""
    try:
        # Check if file exists
        if not await storage_manager.exists(identifier, tenant_id):
            raise HTTPException(status_code=404, detail="File not found")
        
        # Get metadata
        metadata = await storage_manager.get_object_metadata(identifier, tenant_id)
        
        # Check if format conversion is needed
        if format:
            # Check for existing conversion
            conversion_identifier = f"{identifier}.{format.value}"
            
            if await storage_manager.exists(conversion_identifier, tenant_id):
                identifier = conversion_identifier
            else:
                # Convert on-the-fly
                source_obj = await storage_manager.download(identifier, tenant_id)
                
                # Save to temp file
                import tempfile
                with tempfile.NamedTemporaryFile(delete=False, suffix=f".{metadata.filename.split('.')[-1]}") as tmp:
                    tmp.write(source_obj.data)
                    tmp_path = tmp.name
                
                # Convert
                converted_path = await document_converter.convert(
                    tmp_path,
                    format
                )
                
                # Read converted file
                with open(converted_path, 'rb') as f:
                    converted_data = f.read()
                
                # Clean up
                import os
                os.unlink(tmp_path)
                os.unlink(converted_path)
                
                # Return converted data
                return StreamingResponse(
                    io.BytesIO(converted_data),
                    media_type="application/octet-stream",
                    headers={
                        "Content-Disposition": f"attachment; filename={identifier.split('.')[0]}.{format.value}"
                    }
                )
        
        # Download options
        download_options = DownloadOptions(version_id=version_id)
        
        # Stream file
        async def stream_generator():
            async for chunk in storage_manager.download_stream(identifier, tenant_id, download_options):
                yield chunk
                
        # Update bandwidth usage
        await quota_manager.update_usage(
            tenant_id=tenant_id,
            bandwidth_delta=metadata.size
        )
        
        return StreamingResponse(
            stream_generator(),
            media_type=metadata.content_type or "application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename={metadata.filename}",
                "Content-Length": str(metadata.size)
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{identifier}")
async def delete_file(
    identifier: str,
    tenant_id: str = Depends(get_current_tenant_id),
    storage_manager: StorageManager = Depends(get_storage_manager)
):
    """Delete a file from storage."""
    try:
        # Check if file exists
        if not await storage_manager.exists(identifier, tenant_id):
            raise HTTPException(status_code=404, detail="File not found")
        
        # Delete file
        success = await storage_manager.delete(identifier, tenant_id)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to delete file")
        
        return {"status": "deleted", "identifier": identifier}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/list")
async def list_files(
    prefix: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    continuation_token: Optional[str] = Query(None),
    tenant_id: str = Depends(get_current_tenant_id),
    storage_manager: StorageManager = Depends(get_storage_manager)
):
    """List files in storage."""
    try:
        result = await storage_manager.list_objects(
            tenant_id=tenant_id,
            prefix=prefix,
            limit=limit,
            continuation_token=continuation_token
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metadata/{identifier}")
async def get_file_metadata(
    identifier: str,
    tenant_id: str = Depends(get_current_tenant_id),
    storage_manager: StorageManager = Depends(get_storage_manager)
):
    """Get metadata for a file."""
    try:
        metadata = await storage_manager.get_object_metadata(identifier, tenant_id)
        
        if not metadata:
            raise HTTPException(status_code=404, detail="File not found")
        
        return metadata.__dict__
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting metadata: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/presigned-url/{identifier}")
async def get_presigned_url(
    identifier: str,
    expiry_minutes: int = Query(60, ge=1, le=10080),  # Max 1 week
    method: str = Query("GET", regex="^(GET|PUT)$"),
    tenant_id: str = Depends(get_current_tenant_id),
    storage_manager: StorageManager = Depends(get_storage_manager)
):
    """Generate a presigned URL for direct access."""
    try:
        # Check if file exists for GET
        if method == "GET" and not await storage_manager.exists(identifier, tenant_id):
            raise HTTPException(status_code=404, detail="File not found")
        
        url = await storage_manager.get_presigned_url(
            identifier=identifier,
            tenant_id=tenant_id,
            expiry=timedelta(minutes=expiry_minutes),
            method=method
        )
        
        return {
            "url": url,
            "expires_in": expiry_minutes * 60,
            "method": method
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating presigned URL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Document Conversion Endpoints
@router.post("/convert")
async def convert_document(
    request: ConversionRequest,
    tenant_id: str = Depends(get_current_tenant_id),
    document_converter: DocumentConverter = Depends(get_document_converter)
):
    """Convert a document to another format."""
    try:
        job_id = await document_converter.convert_async(
            source_identifier=request.source_identifier,
            target_format=request.target_format,
            tenant_id=tenant_id,
            options=request.options
        )
        
        return {
            "job_id": job_id,
            "status": "queued"
        }
        
    except Exception as e:
        logger.error(f"Error starting conversion: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/convert/status/{job_id}")
async def get_conversion_status(
    job_id: str,
    document_converter: DocumentConverter = Depends(get_document_converter)
):
    """Get status of a conversion job."""
    try:
        job = await document_converter.get_job_status(job_id)
        
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return {
            "job_id": job.job_id,
            "status": job.status.value,
            "source_format": job.source_format.value,
            "target_format": job.target_format.value,
            "progress": job.progress,
            "created_at": job.created_at,
            "completed_at": job.completed_at,
            "error_message": job.error_message,
            "target_identifier": job.metadata.get("target_identifier")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting conversion status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/convert/formats")
async def get_supported_formats():
    """Get supported conversion formats."""
    formats = {}
    
    for format_enum in ConversionFormat:
        formats[format_enum.value] = {
            "name": format_enum.value,
            "category": "document" if format_enum in [
                ConversionFormat.PDF, ConversionFormat.DOCX, ConversionFormat.DOC
            ] else "image" if format_enum in [
                ConversionFormat.PNG, ConversionFormat.JPG, ConversionFormat.WEBP
            ] else "other"
        }
    
    return {"formats": formats}


# Preview Generation Endpoints
@router.post("/preview")
async def generate_preview(
    request: PreviewRequest,
    tenant_id: str = Depends(get_current_tenant_id),
    preview_generator: PreviewGenerator = Depends(get_preview_generator)
):
    """Generate a preview for a file."""
    try:
        preview_id = await preview_generator.generate_preview_async(
            source_identifier=request.identifier,
            preview_type=request.preview_type,
            tenant_id=tenant_id,
            options=request.options
        )
        
        return {
            "preview_id": preview_id,
            "status": "processing"
        }
        
    except Exception as e:
        logger.error(f"Error generating preview: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/preview/{preview_id}")
async def get_preview(
    preview_id: str,
    preview_generator: PreviewGenerator = Depends(get_preview_generator)
):
    """Get a generated preview."""
    try:
        preview = await preview_generator.get_preview(preview_id)
        
        if not preview:
            raise HTTPException(status_code=404, detail="Preview not found")
        
        return {
            "preview_id": preview.preview_id,
            "preview_type": preview.preview_type.value,
            "source_identifier": preview.source_identifier,
            "text_content": preview.text_content,
            "metadata": preview.metadata,
            "created_at": preview.created_at
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting preview: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Search Endpoints
@router.post("/search")
async def search_content(
    request: SearchRequest,
    tenant_id: str = Depends(get_current_tenant_id),
    content_indexer: ContentIndexer = Depends(get_content_indexer)
):
    """Search indexed content."""
    try:
        query = SearchQuery(
            query=request.query,
            tenant_id=tenant_id,
            filters=request.filters,
            from_=request.from_,
            size=request.size,
            sort=request.sort,
            highlight=request.highlight
        )
        
        result = await content_indexer.search(query)
        
        return {
            "total": result.total,
            "hits": result.hits,
            "took": result.took,
            "aggregations": result.aggregations
        }
        
    except Exception as e:
        logger.error(f"Error searching content: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search/suggest")
async def get_suggestions(
    prefix: str = Query(..., min_length=2),
    field: str = Query("content"),
    size: int = Query(5, ge=1, le=20),
    tenant_id: str = Depends(get_current_tenant_id),
    content_indexer: ContentIndexer = Depends(get_content_indexer)
):
    """Get search suggestions."""
    try:
        suggestions = await content_indexer.suggest(
            prefix=prefix,
            tenant_id=tenant_id,
            field=field,
            size=size
        )
        
        return {"suggestions": suggestions}
        
    except Exception as e:
        logger.error(f"Error getting suggestions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Quota Management Endpoints
@router.get("/quota")
async def get_quota(
    tenant_id: str = Depends(get_current_tenant_id),
    quota_manager: QuotaManager = Depends(get_quota_manager)
):
    """Get storage quota and usage."""
    try:
        quota = await quota_manager.get_quota(tenant_id)
        usage = await quota_manager.get_usage(tenant_id)
        
        return {
            "quota": quota.to_dict(),
            "usage": usage.to_dict(),
            "available": {
                "storage_bytes": max(0, quota.storage_bytes - usage.used_storage_bytes),
                "bandwidth_bytes": max(0, quota.bandwidth_bytes - usage.used_bandwidth_bytes),
                "file_count": max(0, quota.file_count - usage.file_count)
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting quota: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/quota/{target_tenant_id}")
async def update_quota(
    target_tenant_id: str,
    request: QuotaRequest,
    tenant_id: str = Depends(get_current_tenant_id),
    quota_manager: QuotaManager = Depends(get_quota_manager)
):
    """Update quota for a tenant (admin only)."""
    try:
        # TODO: Add admin authorization check
        
        quota = TenantQuota(
            tenant_id=target_tenant_id,
            storage_bytes=request.storage_bytes,
            bandwidth_bytes=request.bandwidth_bytes,
            file_count=request.file_count,
            max_file_size=request.max_file_size,
            policy=request.policy
        )
        
        success = await quota_manager.set_quota(quota)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to update quota")
        
        return {"status": "updated", "quota": quota.to_dict()}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating quota: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Statistics Endpoints
@router.get("/stats")
async def get_storage_statistics(
    tenant_id: str = Depends(get_current_tenant_id),
    storage_manager: StorageManager = Depends(get_storage_manager),
    document_converter: DocumentConverter = Depends(get_document_converter),
    preview_generator: PreviewGenerator = Depends(get_preview_generator),
    content_indexer: ContentIndexer = Depends(get_content_indexer),
    quota_manager: QuotaManager = Depends(get_quota_manager)
):
    """Get storage service statistics."""
    try:
        return {
            "storage": storage_manager.get_statistics(),
            "converter": document_converter.get_statistics(),
            "preview": preview_generator.get_statistics(),
            "indexer": content_indexer.get_statistics(),
            "quota": quota_manager.get_statistics()
        }
        
    except Exception as e:
        logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 