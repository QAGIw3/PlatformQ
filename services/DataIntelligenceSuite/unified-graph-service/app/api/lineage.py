"""Lineage tracking API endpoints"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Query, Body
from pydantic import BaseModel, Field

from app.core.config import Settings, get_settings
from app.lineage.lineage_tracker import LineageTracker, LineageType, EntityType
from app.core.cache_manager import CacheManager


router = APIRouter(prefix="/api/v1/lineage", tags=["lineage"])

# Global instances (will be injected)
lineage_tracker: Optional[LineageTracker] = None
cache_manager: Optional[CacheManager] = None


class LineageTrackRequest(BaseModel):
    """Lineage tracking request"""
    entity_id: str = Field(..., description="Entity ID")
    entity_type: str = Field(..., description="Entity type")
    operation: str = Field(..., description="Operation performed")
    metadata: Dict[str, Any] = Field({}, description="Additional metadata")
    parent_ids: Optional[List[str]] = Field(None, description="Parent entity IDs")
    child_ids: Optional[List[str]] = Field(None, description="Child entity IDs")


class ImpactAnalysisRequest(BaseModel):
    """Impact analysis request"""
    entity_id: str = Field(..., description="Entity ID to analyze")
    change_type: str = Field("schema_change", description="Type of change")
    max_depth: int = Field(10, ge=1, le=20, description="Maximum analysis depth")


@router.post("/track")
async def track_lineage(request: LineageTrackRequest,
                      settings: Settings = Depends(get_settings)):
    """Track lineage for an entity"""
    try:
        entity_id = await lineage_tracker.track_lineage(
            request.entity_id,
            request.entity_type,
            request.operation,
            request.metadata,
            request.parent_ids,
            request.child_ids
        )
        
        # Invalidate cache for affected entities
        await cache_manager.clear_pattern(f"lineage:{request.entity_id}")
        if request.parent_ids:
            for parent_id in request.parent_ids:
                await cache_manager.clear_pattern(f"lineage:{parent_id}")
                
        return {
            "entity_id": entity_id,
            "message": "Lineage tracked successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{entity_id}")
async def get_lineage(entity_id: str,
                    direction: str = Query("both", regex="^(upstream|downstream|both)$"),
                    max_depth: int = Query(5, ge=1, le=20),
                    lineage_types: Optional[str] = Query(None, description="Comma-separated lineage types"),
                    settings: Settings = Depends(get_settings)):
    """Get lineage graph for an entity"""
    try:
        # Check cache first
        cached = await cache_manager.get_cached_lineage(entity_id, direction)
        if cached:
            return cached
            
        # Parse lineage types
        type_list = lineage_types.split(',') if lineage_types else None
        
        # Get lineage
        lineage = await lineage_tracker.get_lineage(
            entity_id,
            direction,
            max_depth,
            type_list
        )
        
        # Cache the result
        await cache_manager.cache_lineage(entity_id, direction, lineage)
        
        return lineage
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/impact-analysis")
async def analyze_impact(request: ImpactAnalysisRequest,
                       settings: Settings = Depends(get_settings)):
    """Analyze impact of changes to an entity"""
    try:
        impact = await lineage_tracker.analyze_impact(
            request.entity_id,
            request.change_type,
            request.max_depth
        )
        
        return impact
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{entity_id}/provenance")
async def get_provenance(entity_id: str,
                       settings: Settings = Depends(get_settings)):
    """Get complete provenance chain for an entity"""
    try:
        provenance = await lineage_tracker.get_provenance(entity_id)
        
        return provenance
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/compare")
async def compare_lineages(entity_id1: str = Query(..., description="First entity ID"),
                         entity_id2: str = Query(..., description="Second entity ID"),
                         settings: Settings = Depends(get_settings)):
    """Compare lineages of two entities"""
    try:
        comparison = await lineage_tracker.compare_lineages(entity_id1, entity_id2)
        
        return comparison
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/validate/{entity_id}")
async def validate_lineage(entity_id: str,
                         settings: Settings = Depends(get_settings)):
    """Validate integrity of lineage graph"""
    try:
        validation = await lineage_tracker.validate_lineage_integrity(entity_id)
        
        return validation
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/types")
async def get_lineage_types(settings: Settings = Depends(get_settings)):
    """Get available lineage types"""
    return {
        'lineage_types': [
            {
                'name': lt.value,
                'description': f"Lineage type: {lt.value.replace('_', ' ').title()}"
            }
            for lt in LineageType
        ]
    }


@router.get("/entity-types")
async def get_entity_types(settings: Settings = Depends(get_settings)):
    """Get available entity types"""
    return {
        'entity_types': [
            {
                'name': et.value,
                'description': f"Entity type: {et.value.replace('_', ' ').title()}"
            }
            for et in EntityType
        ]
    }


@router.post("/batch-track")
async def batch_track_lineage(operations: List[LineageTrackRequest] = Body(...),
                            settings: Settings = Depends(get_settings)):
    """Track lineage for multiple operations"""
    try:
        results = []
        errors = []
        
        for op in operations:
            try:
                entity_id = await lineage_tracker.track_lineage(
                    op.entity_id,
                    op.entity_type,
                    op.operation,
                    op.metadata,
                    op.parent_ids,
                    op.child_ids
                )
                results.append({
                    'entity_id': entity_id,
                    'success': True
                })
            except Exception as e:
                errors.append({
                    'entity_id': op.entity_id,
                    'error': str(e)
                })
                
        # Clear cache for all affected entities
        for result in results:
            await cache_manager.clear_pattern(f"lineage:{result['entity_id']}")
            
        return {
            'successful': len(results),
            'failed': len(errors),
            'results': results,
            'errors': errors
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_lineage(entity_type: Optional[str] = Query(None),
                       operation: Optional[str] = Query(None),
                       start_time: Optional[datetime] = Query(None),
                       end_time: Optional[datetime] = Query(None),
                       limit: int = Query(100, ge=1, le=1000),
                       settings: Settings = Depends(get_settings)):
    """Search lineage based on criteria"""
    try:
        # This would search lineage based on filters
        # Placeholder implementation
        return {
            'filters': {
                'entity_type': entity_type,
                'operation': operation,
                'start_time': start_time.isoformat() if start_time else None,
                'end_time': end_time.isoformat() if end_time else None
            },
            'results': [],
            'count': 0,
            'message': "Lineage search not yet implemented"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data-flow/{source_id}/{target_id}")
async def get_data_flow(source_id: str,
                      target_id: str,
                      max_paths: int = Query(5, ge=1, le=20),
                      settings: Settings = Depends(get_settings)):
    """Get data flow paths between two entities"""
    try:
        # Get lineage for both entities
        source_lineage = await lineage_tracker.get_lineage(source_id, "downstream", 10)
        target_lineage = await lineage_tracker.get_lineage(target_id, "upstream", 10)
        
        # Find intersection points
        source_nodes = {n['id'] for n in source_lineage['nodes']}
        target_nodes = {n['id'] for n in target_lineage['nodes']}
        
        common_nodes = source_nodes.intersection(target_nodes)
        
        if common_nodes:
            return {
                'source': source_id,
                'target': target_id,
                'connected': True,
                'common_nodes': list(common_nodes)[:max_paths],
                'message': f"Found {len(common_nodes)} connection points"
            }
        else:
            return {
                'source': source_id,
                'target': target_id,
                'connected': False,
                'message': "No data flow path found"
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def set_dependencies(lt: LineageTracker, cm: CacheManager):
    """Set global dependencies"""
    global lineage_tracker, cache_manager
    lineage_tracker = lt
    cache_manager = cm 