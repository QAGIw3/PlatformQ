"""
Storage API Endpoints

RESTful API for storage operations
"""

import io
from typing import List, Optional, Dict, Any
from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query, BackgroundTasks, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from data_intelligence_common import APIResponse, PaginatedResponse

from ...core.storage_manager import StorageManager, StorageBackend
from ...domain.models.storage import (
    StorageObject,
    StorageTier,
    ConversionFormat,
    StorageStats,
    ConversionJob
)
from ..dependencies import get_storage_manager, get_current_user

router = APIRouter(prefix="/storage", tags=["Storage"])


class UploadResponse(BaseModel):
    """Response model for file upload"""
    object_id: str
    filename: str
    size: int
    content_type: str
    path: str
    conversions_queued: List[str] = Field(default_factory=list)
    preview_queued: bool = False


class StorageObjectResponse(BaseModel):
    """Response model for storage object"""
    object: StorageObject
    download_url: Optional[str] = None
    conversions: List[ConversionJob] = Field(default_factory=list)


class ConversionRequest(BaseModel):
    """Request model for document conversion"""
    object_id: str
    target_format: ConversionFormat
    options: Optional[Dict[str, Any]] = None


@router.post("/upload", response_model=APIResponse[UploadResponse])
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    auto_convert: bool = Query(True, description="Automatically convert to common formats"),
    generate_preview: bool = Query(True, description="Generate preview/thumbnail"),
    storage_tier: StorageTier = Query(StorageTier.HOT, description="Initial storage tier"),
    metadata: Optional[str] = Query(None, description="JSON metadata"),
    storage_manager: StorageManager = Depends(get_storage_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Upload a file to storage"""
    try:
        # Parse metadata if provided
        file_metadata = {}
        if metadata:
            import json
            try:
                file_metadata = json.loads(metadata)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid metadata JSON")
                
        # Upload file
        storage_object = await storage_manager.upload_file(
            file_data=file.file,
            filename=file.filename,
            tenant_id=current_user["id"],
            metadata=file_metadata,
            auto_convert=auto_convert,
            generate_preview=generate_preview,
            storage_tier=storage_tier
        )
        
        # Build response
        response = UploadResponse(
            object_id=storage_object.id,
            filename=storage_object.filename,
            size=storage_object.size,
            content_type=storage_object.content_type,
            path=storage_object.path,
            conversions_queued=[],  # TODO: Track queued conversions
            preview_queued=generate_preview
        )
        
        return APIResponse(
            success=True,
            data=response,
            message=f"File '{file.filename}' uploaded successfully"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload file: {str(e)}")


@router.get("/download/{object_id}")
async def download_file(
    object_id: str,
    format: Optional[ConversionFormat] = Query(None, description="Download in specific format"),
    storage_manager: StorageManager = Depends(get_storage_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Download a file from storage"""
    try:
        # Get object metadata
        storage_object = await storage_manager.get_object_metadata(
            object_id,
            current_user["id"]
        )
        
        if not storage_object:
            raise HTTPException(status_code=404, detail=f"Object '{object_id}' not found")
            
        # Determine content type
        if format:
            content_type = f"application/{format.value}"
            filename = f"{storage_object.filename.rsplit('.', 1)[0]}.{format.value}"
        else:
            content_type = storage_object.content_type
            filename = storage_object.filename
            
        # Stream file
        async def stream_content():
            async for chunk in storage_manager.download_file(
                object_id,
                current_user["id"],
                target_format=format
            ):
                yield chunk
                
        return StreamingResponse(
            stream_content(),
            media_type=content_type,
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to download file: {str(e)}")


@router.get("/objects", response_model=PaginatedResponse[StorageObject])
async def list_objects(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    prefix: Optional[str] = Query(None, description="Filter by prefix"),
    storage_manager: StorageManager = Depends(get_storage_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """List storage objects for current user"""
    try:
        # Get objects
        objects = await storage_manager.list_objects(
            tenant_id=current_user["id"],
            prefix=prefix,
            limit=page_size,
            offset=(page - 1) * page_size
        )
        
        # Get total count (simplified - in production, use proper count query)
        all_objects = await storage_manager.list_objects(
            tenant_id=current_user["id"],
            prefix=prefix,
            limit=10000,
            offset=0
        )
        total = len(all_objects)
        
        return PaginatedResponse(
            success=True,
            data=objects,
            total=total,
            page=page,
            page_size=page_size,
            pages=(total + page_size - 1) // page_size
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list objects: {str(e)}")


@router.get("/objects/{object_id}", response_model=APIResponse[StorageObjectResponse])
async def get_object_details(
    object_id: str,
    include_download_url: bool = Query(False, description="Include presigned download URL"),
    storage_manager: StorageManager = Depends(get_storage_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get detailed information about a storage object"""
    try:
        # Get object metadata
        storage_object = await storage_manager.get_object_metadata(
            object_id,
            current_user["id"]
        )
        
        if not storage_object:
            raise HTTPException(status_code=404, detail=f"Object '{object_id}' not found")
            
        # Get download URL if requested
        download_url = None
        if include_download_url:
            download_url = await storage_manager.minio.presigned_get_url(
                storage_object.bucket,
                storage_object.path,
                expires=timedelta(hours=24)
            )
            
        # TODO: Get conversions
        conversions = []
        
        response = StorageObjectResponse(
            object=storage_object,
            download_url=download_url,
            conversions=conversions
        )
        
        return APIResponse(
            success=True,
            data=response,
            message="Object details retrieved successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get object details: {str(e)}")


@router.delete("/objects/{object_id}", response_model=APIResponse[Dict[str, str]])
async def delete_object(
    object_id: str,
    delete_conversions: bool = Query(True, description="Also delete conversions"),
    storage_manager: StorageManager = Depends(get_storage_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Delete a storage object"""
    try:
        # Delete object
        success = await storage_manager.delete_file(
            object_id,
            current_user["id"],
            delete_conversions=delete_conversions
        )
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Object '{object_id}' not found")
            
        return APIResponse(
            success=True,
            data={"object_id": object_id},
            message=f"Object '{object_id}' deleted successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete object: {str(e)}")


@router.post("/convert", response_model=APIResponse[Dict[str, str]])
async def convert_document(
    request: ConversionRequest,
    background_tasks: BackgroundTasks,
    storage_manager: StorageManager = Depends(get_storage_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Request document conversion"""
    try:
        # Verify object exists
        storage_object = await storage_manager.get_object_metadata(
            request.object_id,
            current_user["id"]
        )
        
        if not storage_object:
            raise HTTPException(status_code=404, detail=f"Object '{request.object_id}' not found")
            
        # Queue conversion
        await storage_manager.event_bus.publish({
            "type": "storage.conversion.requested",
            "data": {
                "object_id": request.object_id,
                "source_format": storage_object.content_type,
                "target_format": request.target_format.value,
                "tenant_id": current_user["id"],
                "options": request.options
            }
        })
        
        return APIResponse(
            success=True,
            data={
                "object_id": request.object_id,
                "target_format": request.target_format.value,
                "status": "queued"
            },
            message="Conversion request queued successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to request conversion: {str(e)}")


@router.get("/stats", response_model=APIResponse[StorageStats])
async def get_storage_stats(
    storage_manager: StorageManager = Depends(get_storage_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get storage statistics for current user"""
    try:
        stats = await storage_manager.get_storage_stats(current_user["id"])
        
        return APIResponse(
            success=True,
            data=stats,
            message="Storage statistics retrieved successfully"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get storage stats: {str(e)}")


@router.post("/upload-url", response_model=APIResponse[Dict[str, str]])
async def get_upload_url(
    filename: str = Query(..., description="Filename for upload"),
    expires_hours: int = Query(1, ge=1, le=24, description="URL expiration in hours"),
    storage_manager: StorageManager = Depends(get_storage_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get presigned URL for direct upload"""
    try:
        import uuid
        
        # Generate object path
        object_id = str(uuid.uuid4())
        object_path = storage_manager._build_object_path(
            current_user["id"],
            object_id,
            filename
        )
        
        # Generate presigned URL
        upload_url = await storage_manager.minio.presigned_put_url(
            storage_manager.default_bucket,
            object_path,
            expires=timedelta(hours=expires_hours)
        )
        
        return APIResponse(
            success=True,
            data={
                "upload_url": upload_url,
                "object_id": object_id,
                "expires_in": expires_hours * 3600
            },
            message="Upload URL generated successfully"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate upload URL: {str(e)}")


@router.get("/formats", response_model=APIResponse[Dict[str, List[str]]])
async def get_supported_formats(
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get supported file formats and conversions"""
    
    formats = {
        "upload": [
            "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx",
            "odt", "ods", "odp", "rtf", "txt", "csv", "html",
            "jpg", "jpeg", "png", "gif", "svg", "webp", "tiff",
            "json", "xml", "yaml", "md"
        ],
        "conversions": {
            "documents": ["pdf", "docx", "txt", "html", "rtf", "odt"],
            "spreadsheets": ["xlsx", "csv", "ods"],
            "presentations": ["pptx", "pdf", "odp"],
            "images": ["jpg", "png", "webp", "svg", "pdf"]
        }
    }
    
    return APIResponse(
        success=True,
        data=formats,
        message="Supported formats retrieved successfully"
    ) 