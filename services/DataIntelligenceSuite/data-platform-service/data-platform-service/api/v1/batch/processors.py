"""
Processor API endpoints
"""

from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, Depends, Body, UploadFile, File
from fastapi.responses import JSONResponse

from app.core.auth import verify_token
from app.core.processor_manager import ProcessorManager
from app.core.config import settings

router = APIRouter(prefix="/api/v1/processors", tags=["processors"])

# Global processor manager instance (injected by main.py)
processor_manager = None


def get_processor_manager() -> ProcessorManager:
    """Dependency to get processor manager instance"""
    if processor_manager is None:
        raise RuntimeError("Processor manager not initialized")
    return processor_manager


@router.get("/formats")
async def list_supported_formats(
    processor_manager: ProcessorManager = Depends(get_processor_manager),
    token_data: dict = Depends(verify_token)
):
    """List all supported file formats by processor"""
    try:
        formats = processor_manager.list_supported_formats()
        return {"supported_formats": formats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{processor_type}")
async def get_processor_info(
    processor_type: str,
    processor_manager: ProcessorManager = Depends(get_processor_manager),
    token_data: dict = Depends(verify_token)
):
    """Get information about a specific processor"""
    try:
        info = processor_manager.get_processor_info(processor_type)
        return info
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/process")
async def process_file(
    file_path: str,
    options: Dict[str, Any] = Body(default={}),
    processor_manager: ProcessorManager = Depends(get_processor_manager),
    token_data: dict = Depends(verify_token)
):
    """Process a single file"""
    try:
        # Add tenant ID to options
        options["tenant_id"] = token_data.get("tenant_id", "default")
        
        result = await processor_manager.process_file(file_path, options)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/process/batch")
async def process_batch(
    file_paths: List[str] = Body(...),
    options: Dict[str, Any] = Body(default={}),
    processor_manager: ProcessorManager = Depends(get_processor_manager),
    token_data: dict = Depends(verify_token)
):
    """Process multiple files in batch"""
    try:
        # Add tenant ID to options
        options["tenant_id"] = token_data.get("tenant_id", "default")
        
        results = await processor_manager.process_batch(file_paths, options)
        return {
            "batch_jobs": results,
            "total_files": len(file_paths)
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload/process")
async def upload_and_process(
    file: UploadFile = File(...),
    processor_type: str = None,
    options: Dict[str, Any] = Body(default={}),
    processor_manager: ProcessorManager = Depends(get_processor_manager),
    token_data: dict = Depends(verify_token)
):
    """Upload a file and process it"""
    import os
    import tempfile
    
    try:
        # Save uploaded file
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file.filename)[1]) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        # Add tenant ID to options
        options["tenant_id"] = token_data.get("tenant_id", "default")
        
        # Process the file
        result = await processor_manager.process_file(tmp_path, options)
        
        # Clean up
        os.unlink(tmp_path)
        
        return {
            "filename": file.filename,
            "size": len(content),
            "result": result
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 