"""
Pipeline Template API endpoints

Provides API for pipeline template operations.
"""

from typing import Dict, List, Optional, Any
from fastapi import APIRouter, HTTPException, Path, Body
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter(prefix="/api/v1/templates", tags=["templates"])


# Request/Response Models
class CreateFromTemplateRequest(BaseModel):
    """Create pipeline from template request"""
    template_id: str = Field(..., description="Template ID")
    name: str = Field(..., description="Pipeline name")
    overrides: Optional[Dict[str, Any]] = Field(None, description="Configuration overrides")
    owner: Optional[str] = Field(None, description="Pipeline owner")


# API Endpoints
@router.get("/", response_model=Dict[str, Dict[str, Any]])
async def list_templates():
    """List available pipeline templates"""
    try:
        logger.info("list_templates_requested")
        
        # Get service instance
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get templates
        templates = await service.repository.list_templates()
        
        return templates
        
    except Exception as e:
        logger.error("list_templates_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{template_id}", response_model=Dict[str, Any])
async def get_template(template_id: str = Path(..., description="Template ID")):
    """Get a specific template"""
    try:
        logger.info("get_template_requested", template_id=template_id)
        
        # Get service instance
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get template
        template = await service.repository.get_template(template_id)
        if not template:
            raise HTTPException(status_code=404, detail="Template not found")
        
        return template
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_template_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/create-pipeline")
async def create_from_template(request: CreateFromTemplateRequest):
    """Create a pipeline from a template"""
    try:
        logger.info("create_from_template_requested", 
                   template_id=request.template_id,
                   name=request.name)
        
        # Get service instance
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Create pipeline from template
        pipeline = await service.repository.create_from_template(
            template_id=request.template_id,
            name=request.name,
            overrides=request.overrides,
            owner=request.owner
        )
        
        if not pipeline:
            raise HTTPException(status_code=404, detail="Template not found")
        
        return {
            "pipeline_id": pipeline.id,
            "name": pipeline.name,
            "message": "Pipeline created successfully from template"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("create_from_template_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) 