"""
Event Mappings API endpoints
"""

from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter()


class EventMappingRequest(BaseModel):
    """Event mapping request"""
    event_type: str = Field(..., description="Event type to map")
    workflow_id: str = Field(..., description="Workflow to trigger")
    mapping_type: str = Field("direct", description="Mapping type")
    conditions: Dict[str, Any] = Field(default={}, description="Trigger conditions")
    correlation_config: Dict[str, Any] = Field(default={}, description="Correlation configuration")


@router.post("/event-mappings", response_model=Dict[str, str])
async def create_event_mapping(request: EventMappingRequest) -> Dict[str, str]:
    """Create event to workflow mapping"""
    try:
        from ..main import event_orchestrator
        from ..engines.event import EventMappingType
        
        if not event_orchestrator:
            raise HTTPException(status_code=503, detail="Event orchestrator not available")
        
        mapping_type = EventMappingType(request.mapping_type)
        
        mapping_id = await event_orchestrator.register_event_mapping(
            request.event_type,
            request.workflow_id,
            mapping_type,
            request.conditions,
            request.correlation_config
        )
        
        return {
            "mapping_id": mapping_id,
            "status": "created",
            "message": "Event mapping created successfully"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating event mapping: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/event-mappings", response_model=List[Dict[str, Any]])
async def list_event_mappings(
    event_type: str = Query(None, description="Filter by event type")
) -> List[Dict[str, Any]]:
    """List event mappings"""
    try:
        from ..main import event_orchestrator
        
        if not event_orchestrator:
            raise HTTPException(status_code=503, detail="Event orchestrator not available")
        
        mappings = await event_orchestrator.get_event_mappings(event_type)
        return mappings
        
    except Exception as e:
        logger.error(f"Error listing event mappings: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/event-mappings/{mapping_id}")
async def remove_event_mapping(mapping_id: str) -> Dict[str, Any]:
    """Remove event mapping"""
    try:
        from ..main import event_orchestrator
        
        if not event_orchestrator:
            raise HTTPException(status_code=503, detail="Event orchestrator not available")
        
        success = await event_orchestrator.remove_event_mapping(mapping_id)
        
        return {
            "mapping_id": mapping_id,
            "removed": success,
            "message": "Event mapping removed successfully"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error removing event mapping: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/event-mappings/{mapping_id}")
async def get_event_mapping(mapping_id: str) -> Dict[str, Any]:
    """Get event mapping details"""
    try:
        from ..main import event_orchestrator
        
        if not event_orchestrator:
            raise HTTPException(status_code=503, detail="Event orchestrator not available")
        
        mapping = event_orchestrator.event_mappings.get(mapping_id)
        if not mapping:
            raise HTTPException(status_code=404, detail="Mapping not found")
        
        return {
            "id": mapping["id"],
            "event_type": mapping["event_type"],
            "workflow_id": mapping["workflow_id"],
            "type": mapping["type"].value,
            "conditions": mapping["conditions"],
            "correlation_config": mapping["correlation_config"],
            "executions": mapping["executions"],
            "created_at": mapping["created_at"].isoformat(),
            "enabled": mapping["enabled"]
        }
        
    except Exception as e:
        logger.error(f"Error getting event mapping: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/event-types")
async def list_subscribed_event_types() -> List[str]:
    """List event types with mappings"""
    try:
        from ..main import event_orchestrator
        
        if not event_orchestrator:
            raise HTTPException(status_code=503, detail="Event orchestrator not available")
        
        event_types = set()
        for mapping in event_orchestrator.event_mappings.values():
            event_types.add(mapping["event_type"])
        
        return sorted(list(event_types))
        
    except Exception as e:
        logger.error(f"Error listing event types: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/event-metrics")
async def get_event_metrics() -> Dict[str, Any]:
    """Get event orchestrator metrics"""
    try:
        from ..main import event_orchestrator
        
        if not event_orchestrator:
            raise HTTPException(status_code=503, detail="Event orchestrator not available")
        
        metrics = await event_orchestrator.get_event_metrics()
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting event metrics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/mapping-types")
async def get_mapping_types() -> List[Dict[str, str]]:
    """Get available mapping types"""
    return [
        {
            "type": "direct",
            "description": "One event directly triggers one workflow"
        },
        {
            "type": "pattern",
            "description": "Complex event pattern triggers workflow"
        },
        {
            "type": "aggregated",
            "description": "Multiple events aggregated trigger workflow"
        },
        {
            "type": "conditional",
            "description": "Event with complex conditions triggers workflow"
        }
    ]


@router.get("/correlation-strategies")
async def get_correlation_strategies() -> List[Dict[str, str]]:
    """Get available correlation strategies"""
    return [
        {
            "strategy": "time_window",
            "description": "Correlate events within a time window"
        },
        {
            "strategy": "count_based",
            "description": "Correlate based on event count"
        },
        {
            "strategy": "sequence",
            "description": "Correlate events in specific sequence"
        },
        {
            "strategy": "custom",
            "description": "Custom correlation logic"
        }
    ] 