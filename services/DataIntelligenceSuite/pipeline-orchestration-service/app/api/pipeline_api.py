"""
Pipeline API endpoints

Provides API for pipeline management operations.
"""

from typing import Dict, List, Optional, Any
from fastapi import APIRouter, HTTPException, Query, Path, Body
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger
from ..pipelines import PipelineStatus

logger = StructuredLogger.get_logger(__name__)

router = APIRouter(prefix="/api/v1/pipelines", tags=["pipelines"])


# Request/Response Models
class PipelineCreateRequest(BaseModel):
    """Pipeline creation request"""
    name: str = Field(..., description="Pipeline name")
    type: str = Field(..., description="Pipeline type")
    description: str = Field("", description="Pipeline description")
    config: Dict[str, Any] = Field(..., description="Pipeline configuration")
    schedule: Optional[Dict[str, Any]] = Field(None, description="Schedule configuration")
    dependencies: Optional[List[str]] = Field(None, description="Pipeline dependencies")
    tags: Optional[List[str]] = Field(None, description="Pipeline tags")
    owner: Optional[str] = Field(None, description="Pipeline owner")


class PipelineUpdateRequest(BaseModel):
    """Pipeline update request"""
    name: Optional[str] = None
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    schedule: Optional[Dict[str, Any]] = None
    dependencies: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    status: Optional[str] = None


class PipelineResponse(BaseModel):
    """Pipeline response model"""
    id: str
    name: str
    type: str
    description: str
    config: Dict[str, Any]
    schedule: Dict[str, Any]
    dependencies: List[str]
    tags: List[str]
    owner: Optional[str]
    status: str
    created_at: str
    updated_at: str
    metadata: Dict[str, Any]


# API Endpoints
@router.post("/", response_model=PipelineResponse)
async def create_pipeline(request: PipelineCreateRequest):
    """Create a new pipeline"""
    try:
        logger.info("create_pipeline_requested", name=request.name)
        
        # Get service instance from app state
        from fastapi import Request as FastAPIRequest
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Create pipeline
        pipeline = await service.repository.create_pipeline(
            name=request.name,
            type=request.type,
            config=request.config,
            description=request.description,
            schedule=request.schedule,
            dependencies=request.dependencies,
            tags=request.tags,
            owner=request.owner
        )
        
        return PipelineResponse(
            id=pipeline.id,
            name=pipeline.name,
            type=pipeline.type,
            description=pipeline.description,
            config=pipeline.config,
            schedule=pipeline.schedule,
            dependencies=pipeline.dependencies,
            tags=pipeline.tags,
            owner=pipeline.owner,
            status=pipeline.status.value,
            created_at=pipeline.created_at.isoformat(),
            updated_at=pipeline.updated_at.isoformat(),
            metadata=pipeline.metadata
        )
        
    except Exception as e:
        logger.error("create_pipeline_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=List[PipelineResponse])
async def list_pipelines(
    status: Optional[str] = Query(None, description="Filter by status"),
    type: Optional[str] = Query(None, description="Filter by type"),
    tags: Optional[str] = Query(None, description="Filter by tags (comma-separated)"),
    owner: Optional[str] = Query(None, description="Filter by owner")
):
    """List pipelines with optional filtering"""
    try:
        logger.info("list_pipelines_requested")
        
        # Get service instance
        from fastapi import Request as FastAPIRequest
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Parse parameters
        status_enum = PipelineStatus(status) if status else None
        tag_list = tags.split(",") if tags else None
        
        # Get pipelines
        pipelines = await service.repository.list_pipelines(
            status=status_enum,
            type=type,
            tags=tag_list,
            owner=owner
        )
        
        # Convert to response models
        return [
            PipelineResponse(
                id=p.id,
                name=p.name,
                type=p.type,
                description=p.description,
                config=p.config,
                schedule=p.schedule,
                dependencies=p.dependencies,
                tags=p.tags,
                owner=p.owner,
                status=p.status.value,
                created_at=p.created_at.isoformat(),
                updated_at=p.updated_at.isoformat(),
                metadata=p.metadata
            )
            for p in pipelines
        ]
        
    except Exception as e:
        logger.error("list_pipelines_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(pipeline_id: str = Path(..., description="Pipeline ID")):
    """Get a specific pipeline"""
    try:
        logger.info("get_pipeline_requested", pipeline_id=pipeline_id)
        
        # Get service instance
        from fastapi import Request as FastAPIRequest
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get pipeline
        pipeline = await service.repository.get_pipeline(pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        return PipelineResponse(
            id=pipeline.id,
            name=pipeline.name,
            type=pipeline.type,
            description=pipeline.description,
            config=pipeline.config,
            schedule=pipeline.schedule,
            dependencies=pipeline.dependencies,
            tags=pipeline.tags,
            owner=pipeline.owner,
            status=pipeline.status.value,
            created_at=pipeline.created_at.isoformat(),
            updated_at=pipeline.updated_at.isoformat(),
            metadata=pipeline.metadata
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_pipeline_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{pipeline_id}", response_model=PipelineResponse)
async def update_pipeline(
    pipeline_id: str = Path(..., description="Pipeline ID"),
    request: PipelineUpdateRequest = Body(...)
):
    """Update a pipeline"""
    try:
        logger.info("update_pipeline_requested", pipeline_id=pipeline_id)
        
        # Get service instance
        from fastapi import Request as FastAPIRequest
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Build updates
        updates = request.dict(exclude_unset=True)
        if "status" in updates:
            updates["status"] = PipelineStatus(updates["status"])
        
        # Update pipeline
        pipeline = await service.repository.update_pipeline(pipeline_id, updates)
        if not pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        return PipelineResponse(
            id=pipeline.id,
            name=pipeline.name,
            type=pipeline.type,
            description=pipeline.description,
            config=pipeline.config,
            schedule=pipeline.schedule,
            dependencies=pipeline.dependencies,
            tags=pipeline.tags,
            owner=pipeline.owner,
            status=pipeline.status.value,
            created_at=pipeline.created_at.isoformat(),
            updated_at=pipeline.updated_at.isoformat(),
            metadata=pipeline.metadata
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("update_pipeline_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{pipeline_id}")
async def delete_pipeline(pipeline_id: str = Path(..., description="Pipeline ID")):
    """Delete a pipeline"""
    try:
        logger.info("delete_pipeline_requested", pipeline_id=pipeline_id)
        
        # Get service instance
        from fastapi import Request as FastAPIRequest
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Delete pipeline
        success = await service.repository.delete_pipeline(pipeline_id)
        if not success:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        return {"message": "Pipeline deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("delete_pipeline_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics", response_model=Dict[str, Any])
async def get_pipeline_statistics():
    """Get pipeline statistics"""
    try:
        logger.info("get_statistics_requested")
        
        # Get service instance
        from fastapi import Request as FastAPIRequest
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get statistics
        stats = await service.repository.get_pipeline_statistics()
        
        return stats
        
    except Exception as e:
        logger.error("get_statistics_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) 