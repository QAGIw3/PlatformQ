"""
Event mapping API endpoints
"""

import json
from typing import Dict, Any, List, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Body, Path
from pydantic import BaseModel

from platformq_shared.logging import get_logger
from ..core import EventOrchestrator, EventMappingType, EventCorrelationStrategy

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/event-mappings", tags=["event-mappings"])

# Dependency injection
event_orchestrator: Optional[EventOrchestrator] = None

def set_dependencies(orchestrator: EventOrchestrator):
    """Set API dependencies"""
    global event_orchestrator
    event_orchestrator = orchestrator


# Request/Response models
class CreateEventMappingRequest(BaseModel):
    name: str
    event_type: str
    workflow_id: str
    mapping_type: EventMappingType = EventMappingType.DIRECT
    conditions: Optional[Dict[str, Any]] = None
    correlation: Optional[Dict[str, Any]] = None


class EventMappingResponse(BaseModel):
    id: str
    name: str
    event_type: str
    workflow_id: str
    type: EventMappingType
    conditions: Dict[str, Any]
    correlation: Dict[str, Any]
    created_at: str
    enabled: bool
    execution_count: int
    last_triggered: Optional[str]


class EventStatisticsResponse(BaseModel):
    total_mappings: int
    enabled_mappings: int
    active_correlations: int
    buffered_events: int
    mapping_types: Dict[str, int]
    most_triggered: Optional[Dict[str, Any]]


# API Endpoints
@router.post("", response_model=EventMappingResponse)
async def create_event_mapping(request: CreateEventMappingRequest = Body(...)):
    """Create an event to workflow mapping"""
    if not event_orchestrator:
        raise HTTPException(status_code=503, detail="Event orchestrator not initialized")
        
    try:
        # Validate correlation for pattern mappings
        if request.mapping_type == EventMappingType.PATTERN and not request.correlation:
            raise HTTPException(
                status_code=400,
                detail="Pattern mapping requires correlation configuration"
            )
            
        mapping = await event_orchestrator.create_event_mapping(
            name=request.name,
            event_type=request.event_type,
            workflow_id=request.workflow_id,
            mapping_type=request.mapping_type,
            conditions=request.conditions,
            correlation=request.correlation
        )
        
        return EventMappingResponse(**mapping)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create event mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=List[EventMappingResponse])
async def list_event_mappings(
    event_type: Optional[str] = Query(None),
    workflow_id: Optional[str] = Query(None),
    enabled_only: bool = Query(True),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """List event mappings with filtering"""
    if not event_orchestrator:
        raise HTTPException(status_code=503, detail="Event orchestrator not initialized")
        
    try:
        mappings = await event_orchestrator.list_event_mappings(
            event_type=event_type,
            workflow_id=workflow_id,
            enabled_only=enabled_only
        )
        
        # Sort by creation time (newest first)
        mappings.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        # Apply pagination
        start = offset
        end = offset + limit
        paginated = mappings[start:end]
        
        return [EventMappingResponse(**m) for m in paginated]
        
    except Exception as e:
        logger.error(f"Failed to list event mappings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics", response_model=EventStatisticsResponse)
async def get_event_statistics():
    """Get event processing statistics"""
    if not event_orchestrator:
        raise HTTPException(status_code=503, detail="Event orchestrator not initialized")
        
    try:
        stats = await event_orchestrator.get_event_statistics()
        return EventStatisticsResponse(**stats)
        
    except Exception as e:
        logger.error(f"Failed to get event statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{mapping_id}", response_model=EventMappingResponse)
async def get_event_mapping(mapping_id: str = Path(...)):
    """Get specific event mapping details"""
    if not event_orchestrator:
        raise HTTPException(status_code=503, detail="Event orchestrator not initialized")
        
    try:
        mapping = event_orchestrator.event_mappings.get(mapping_id)
        if not mapping:
            raise HTTPException(status_code=404, detail=f"Event mapping {mapping_id} not found")
            
        return EventMappingResponse(**mapping)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get event mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{mapping_id}")
async def delete_event_mapping(mapping_id: str = Path(...)):
    """Delete an event mapping"""
    if not event_orchestrator:
        raise HTTPException(status_code=503, detail="Event orchestrator not initialized")
        
    try:
        success = await event_orchestrator.delete_event_mapping(mapping_id)
        if not success:
            raise HTTPException(status_code=404, detail=f"Event mapping {mapping_id} not found")
            
        return {"message": f"Event mapping {mapping_id} deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete event mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/{mapping_id}")
async def update_event_mapping(
    mapping_id: str = Path(...),
    enabled: Optional[bool] = Body(None),
    conditions: Optional[Dict[str, Any]] = Body(None),
    correlation: Optional[Dict[str, Any]] = Body(None)
):
    """Update an event mapping"""
    if not event_orchestrator:
        raise HTTPException(status_code=503, detail="Event orchestrator not initialized")
        
    try:
        mapping = event_orchestrator.event_mappings.get(mapping_id)
        if not mapping:
            raise HTTPException(status_code=404, detail=f"Event mapping {mapping_id} not found")
            
        # Update fields
        if enabled is not None:
            mapping['enabled'] = enabled
            
        if conditions is not None:
            mapping['conditions'] = conditions
            
        if correlation is not None:
            mapping['correlation'] = correlation
            
        mapping['updated_at'] = datetime.utcnow().isoformat()
        
        # Persist update
        if event_orchestrator.ignite_client:
            cache = await event_orchestrator.ignite_client.get_or_create_cache("event_mappings")
            await cache.put(mapping_id, json.dumps(mapping))
            
        return {"message": f"Event mapping {mapping_id} updated successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update event mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/correlations/active")
async def get_active_correlations():
    """Get active event correlations"""
    if not event_orchestrator:
        raise HTTPException(status_code=503, detail="Event orchestrator not initialized")
        
    try:
        correlations = []
        
        for key, correlation in event_orchestrator.active_correlations.items():
            correlations.append({
                "correlation_key": key,
                "mapping_id": correlation['mapping_id'],
                "event_count": len(correlation['events']),
                "started_at": correlation['started_at'].isoformat(),
                "strategy": correlation['strategy'],
                "config": correlation['config']
            })
            
        return correlations
        
    except Exception as e:
        logger.error(f"Failed to get active correlations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/event-types")
async def get_subscribed_event_types():
    """Get list of event types with active subscriptions"""
    if not event_orchestrator:
        raise HTTPException(status_code=503, detail="Event orchestrator not initialized")
        
    try:
        # Get unique event types from mappings
        event_types = set()
        
        for mapping in event_orchestrator.event_mappings.values():
            if mapping.get('enabled', True):
                if mapping['type'] == EventMappingType.DIRECT:
                    event_types.add(mapping['event_type'])
                elif mapping['type'] == EventMappingType.PATTERN:
                    for event in mapping.get('correlation', {}).get('pattern', {}).get('events', []):
                        event_types.add(event)
                        
        return {
            "event_types": sorted(list(event_types)),
            "total": len(event_types)
        }
        
    except Exception as e:
        logger.error(f"Failed to get event types: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/test/{mapping_id}")
async def test_event_mapping(
    mapping_id: str = Path(...),
    test_event: Dict[str, Any] = Body(...)
):
    """Test an event mapping with a sample event"""
    if not event_orchestrator:
        raise HTTPException(status_code=503, detail="Event orchestrator not initialized")
        
    try:
        mapping = event_orchestrator.event_mappings.get(mapping_id)
        if not mapping:
            raise HTTPException(status_code=404, detail=f"Event mapping {mapping_id} not found")
            
        # Create test event
        from platformq_events import Event
        event = Event(
            type=test_event.get('type', mapping['event_type']),
            data=test_event.get('data', {}),
            source=test_event.get('source', 'test')
        )
        
        # Check conditions
        conditions_met = await event_orchestrator._check_conditions(event, mapping['conditions'])
        
        result = {
            "mapping_id": mapping_id,
            "mapping_name": mapping['name'],
            "event_type": event.type,
            "conditions_met": conditions_met,
            "would_trigger": conditions_met and mapping.get('enabled', True),
            "workflow_id": mapping['workflow_id']
        }
        
        if not conditions_met:
            result['failed_conditions'] = "Conditions not met"
            
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to test event mapping: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 