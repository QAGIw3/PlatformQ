"""
CDC API Endpoints

RESTful API for Change Data Capture operations
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field

from data_intelligence_common import APIResponse, PaginatedResponse

from ...core.cdc_manager import CDCManager, CDCSourceType, CDCMode
from ...domain.models.cdc import CDCSource, CDCMetrics, CDCConfiguration
from ..dependencies import get_cdc_manager, get_current_user

router = APIRouter(prefix="/cdc", tags=["CDC"])


class CreateCDCSourceRequest(BaseModel):
    """Request model for creating CDC source"""
    name: str = Field(..., description="Name of the CDC source")
    source_type: CDCSourceType = Field(..., description="Type of source database")
    connection_config: Dict[str, Any] = Field(..., description="Connection configuration")
    tables: List[str] = Field(..., description="Tables to capture")
    destination_config: Dict[str, Any] = Field(..., description="Destination configuration")
    mode: CDCMode = Field(default=CDCMode.STREAMING, description="CDC operation mode")
    options: Optional[Dict[str, Any]] = Field(default=None, description="Additional options")


class UpdateCDCSourceRequest(BaseModel):
    """Request model for updating CDC source"""
    tables: Optional[List[str]] = Field(None, description="Updated table list")
    options: Optional[Dict[str, Any]] = Field(None, description="Updated options")


class CDCSourceResponse(BaseModel):
    """Response model for CDC source"""
    source: CDCSource
    metrics: Optional[CDCMetrics] = None


@router.post("/sources", response_model=APIResponse[CDCSource])
async def create_cdc_source(
    request: CreateCDCSourceRequest,
    background_tasks: BackgroundTasks,
    cdc_manager: CDCManager = Depends(get_cdc_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Create a new CDC source"""
    try:
        # Create CDC source
        source = await cdc_manager.create_source(
            name=request.name,
            source_type=request.source_type,
            connection_config=request.connection_config,
            tables=request.tables,
            destination_config=request.destination_config,
            mode=request.mode,
            options=request.options
        )
        
        return APIResponse(
            success=True,
            data=source,
            message=f"CDC source '{request.name}' created successfully"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create CDC source: {str(e)}")


@router.get("/sources", response_model=PaginatedResponse[CDCSource])
async def list_cdc_sources(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    source_type: Optional[CDCSourceType] = None,
    status: Optional[str] = None,
    cdc_manager: CDCManager = Depends(get_cdc_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """List CDC sources with pagination"""
    try:
        # Get all sources
        all_sources = list(cdc_manager.active_sources.values())
        
        # Filter by type if specified
        if source_type:
            all_sources = [s for s in all_sources if s.source_type == source_type.value]
            
        # Filter by status if specified
        if status:
            all_sources = [s for s in all_sources if s.status == status]
            
        # Calculate pagination
        total = len(all_sources)
        start = (page - 1) * page_size
        end = start + page_size
        
        # Get page of sources
        sources = all_sources[start:end]
        
        return PaginatedResponse(
            success=True,
            data=sources,
            total=total,
            page=page,
            page_size=page_size,
            pages=(total + page_size - 1) // page_size
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list CDC sources: {str(e)}")


@router.get("/sources/{source_id}", response_model=APIResponse[CDCSourceResponse])
async def get_cdc_source(
    source_id: str,
    include_metrics: bool = Query(True, description="Include current metrics"),
    cdc_manager: CDCManager = Depends(get_cdc_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get CDC source details"""
    try:
        # Get source
        if source_id not in cdc_manager.active_sources:
            raise HTTPException(status_code=404, detail=f"CDC source '{source_id}' not found")
            
        source = cdc_manager.active_sources[source_id]
        
        # Get metrics if requested
        metrics = None
        if include_metrics:
            metrics_data = await cdc_manager.get_source_metrics(source_id)
            metrics = CDCMetrics(
                source_id=source_id,
                timestamp=metrics_data.get("timestamp"),
                events_processed=metrics_data.get("events_processed", 0),
                bytes_processed=metrics_data.get("bytes_processed", 0),
                latency_ms=metrics_data.get("latency", 0),
                error_count=metrics_data.get("error_count", 0),
                lag_seconds=metrics_data.get("lag_seconds", 0),
                throughput_eps=metrics_data.get("throughput", 0)
            )
            
        response = CDCSourceResponse(source=source, metrics=metrics)
        
        return APIResponse(
            success=True,
            data=response,
            message="CDC source retrieved successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get CDC source: {str(e)}")


@router.patch("/sources/{source_id}", response_model=APIResponse[CDCSource])
async def update_cdc_source(
    source_id: str,
    request: UpdateCDCSourceRequest,
    cdc_manager: CDCManager = Depends(get_cdc_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Update CDC source configuration"""
    try:
        # Check if source exists
        if source_id not in cdc_manager.active_sources:
            raise HTTPException(status_code=404, detail=f"CDC source '{source_id}' not found")
            
        # Build config updates
        config_updates = {}
        if request.tables is not None:
            config_updates["tables"] = request.tables
        if request.options is not None:
            config_updates.update(request.options)
            
        # Update configuration
        await cdc_manager.seatunnel.update_job_config(source_id, config_updates)
        
        # Update local source object
        source = cdc_manager.active_sources[source_id]
        if request.tables is not None:
            source.tables = request.tables
            
        return APIResponse(
            success=True,
            data=source,
            message=f"CDC source '{source_id}' updated successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update CDC source: {str(e)}")


@router.delete("/sources/{source_id}", response_model=APIResponse[Dict[str, str]])
async def delete_cdc_source(
    source_id: str,
    cdc_manager: CDCManager = Depends(get_cdc_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Delete (stop) a CDC source"""
    try:
        # Stop the source
        await cdc_manager.stop_source(source_id)
        
        return APIResponse(
            success=True,
            data={"source_id": source_id},
            message=f"CDC source '{source_id}' stopped and removed successfully"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete CDC source: {str(e)}")


@router.post("/sources/{source_id}/pause", response_model=APIResponse[Dict[str, str]])
async def pause_cdc_source(
    source_id: str,
    cdc_manager: CDCManager = Depends(get_cdc_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Pause a running CDC source"""
    try:
        # Check if source exists
        if source_id not in cdc_manager.active_sources:
            raise HTTPException(status_code=404, detail=f"CDC source '{source_id}' not found")
            
        # Pause the job
        await cdc_manager.seatunnel.update_job_config(source_id, {"paused": True})
        
        # Update status
        cdc_manager.active_sources[source_id].status = "PAUSED"
        
        return APIResponse(
            success=True,
            data={"source_id": source_id},
            message=f"CDC source '{source_id}' paused successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to pause CDC source: {str(e)}")


@router.post("/sources/{source_id}/resume", response_model=APIResponse[Dict[str, str]])
async def resume_cdc_source(
    source_id: str,
    cdc_manager: CDCManager = Depends(get_cdc_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Resume a paused CDC source"""
    try:
        # Check if source exists
        if source_id not in cdc_manager.active_sources:
            raise HTTPException(status_code=404, detail=f"CDC source '{source_id}' not found")
            
        # Resume the job
        await cdc_manager.seatunnel.update_job_config(source_id, {"paused": False})
        
        # Update status
        cdc_manager.active_sources[source_id].status = "RUNNING"
        
        return APIResponse(
            success=True,
            data={"source_id": source_id},
            message=f"CDC source '{source_id}' resumed successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to resume CDC source: {str(e)}")


@router.get("/sources/{source_id}/metrics", response_model=APIResponse[Dict[str, Any]])
async def get_cdc_metrics(
    source_id: str,
    cdc_manager: CDCManager = Depends(get_cdc_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get detailed metrics for a CDC source"""
    try:
        # Get metrics
        metrics = await cdc_manager.get_source_metrics(source_id)
        
        return APIResponse(
            success=True,
            data=metrics,
            message="CDC metrics retrieved successfully"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get CDC metrics: {str(e)}")


@router.post("/sources/{source_id}/restart", response_model=APIResponse[Dict[str, str]])
async def restart_cdc_source(
    source_id: str,
    background_tasks: BackgroundTasks,
    cdc_manager: CDCManager = Depends(get_cdc_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Restart a CDC source"""
    try:
        # Check if source exists
        if source_id not in cdc_manager.active_sources:
            raise HTTPException(status_code=404, detail=f"CDC source '{source_id}' not found")
            
        # Restart in background
        background_tasks.add_task(cdc_manager.seatunnel.restart_job, source_id)
        
        return APIResponse(
            success=True,
            data={"source_id": source_id},
            message=f"CDC source '{source_id}' restart initiated"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to restart CDC source: {str(e)}")


@router.get("/supported-types", response_model=APIResponse[List[str]])
async def get_supported_source_types(
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get list of supported CDC source types"""
    return APIResponse(
        success=True,
        data=[t.value for t in CDCSourceType],
        message="Supported CDC source types retrieved successfully"
    )


@router.get("/modes", response_model=APIResponse[List[str]])
async def get_cdc_modes(
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get list of supported CDC modes"""
    return APIResponse(
        success=True,
        data=[m.value for m in CDCMode],
        message="Supported CDC modes retrieved successfully"
    ) 