"""Trust management API endpoints"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Depends, Query, Body
from pydantic import BaseModel, Field

from app.core.config import Settings, get_settings
from app.trust.trust_engine import TrustEngine, TrustDimension
from app.core.cache_manager import CacheManager


router = APIRouter(prefix="/api/v1/trust", tags=["trust"])

# Global instances (will be injected)
trust_engine: Optional[TrustEngine] = None
cache_manager: Optional[CacheManager] = None


class TrustScoreRequest(BaseModel):
    """Trust score calculation request"""
    context: Optional[str] = Field(None, description="Trust context (e.g., 'trading', 'technical')")
    dimensions: Optional[List[str]] = Field(None, description="Specific trust dimensions to calculate")


class TrustUpdateRequest(BaseModel):
    """Trust relationship update request"""
    from_id: str = Field(..., description="Trustor entity ID")
    to_id: str = Field(..., description="Trustee entity ID")
    trust_level: float = Field(..., ge=0.0, le=1.0, description="Overall trust level")
    context: Optional[str] = Field(None, description="Trust context")
    dimensions: Optional[Dict[str, float]] = Field(None, description="Trust scores by dimension")


class TrustPropagationRequest(BaseModel):
    """Trust propagation request"""
    source_id: str = Field(..., description="Source entity ID")
    max_depth: Optional[int] = Field(None, ge=1, le=10, description="Maximum propagation depth")


class TrustNetworkRequest(BaseModel):
    """Trust network request"""
    radius: int = Field(2, ge=1, le=5, description="Network radius from center entity")


@router.get("/{entity_id}")
async def get_trust_score(entity_id: str,
                         context: Optional[str] = None,
                         dimensions: Optional[str] = Query(None, description="Comma-separated dimensions"),
                         settings: Settings = Depends(get_settings)):
    """Get trust score for an entity"""
    try:
        # Check cache first
        cached = await cache_manager.get_cached_trust_score(entity_id, context or 'global')
        if cached:
            return cached
            
        # Parse dimensions
        dimension_list = dimensions.split(',') if dimensions else None
        
        # Calculate trust score
        score = await trust_engine.calculate_trust_score(
            entity_id,
            context,
            dimension_list
        )
        
        # Cache the result
        await cache_manager.cache_trust_score(entity_id, context or 'global', score)
        
        return score
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/calculate")
async def calculate_trust(entity_id: str = Body(..., embed=True),
                        request: TrustScoreRequest = Body(...),
                        settings: Settings = Depends(get_settings)):
    """Calculate trust score with specific parameters"""
    try:
        score = await trust_engine.calculate_trust_score(
            entity_id,
            request.context,
            request.dimensions
        )
        
        return score
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/propagate")
async def propagate_trust(request: TrustPropagationRequest,
                        settings: Settings = Depends(get_settings)):
    """Propagate trust from a source entity"""
    try:
        propagated = await trust_engine.propagate_trust(
            request.source_id,
            request.max_depth
        )
        
        # Sort by trust value
        sorted_trust = sorted(
            [(k, v) for k, v in propagated.items()],
            key=lambda x: x[1],
            reverse=True
        )
        
        return {
            'source': request.source_id,
            'propagated_trust': dict(sorted_trust[:100]),  # Top 100
            'total_entities': len(propagated)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/network/{entity_id}")
async def get_trust_network(entity_id: str,
                          radius: int = Query(2, ge=1, le=5),
                          settings: Settings = Depends(get_settings)):
    """Get trust network around an entity"""
    try:
        network = await trust_engine.get_trust_network(entity_id, radius)
        
        return network
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/update")
async def update_trust(request: TrustUpdateRequest,
                     settings: Settings = Depends(get_settings)):
    """Update trust relationship between entities"""
    try:
        success = await trust_engine.update_trust_relationship(
            request.from_id,
            request.to_id,
            request.trust_level,
            request.context,
            request.dimensions
        )
        
        if success:
            return {"message": "Trust relationship updated successfully"}
        else:
            raise HTTPException(status_code=400, detail="Failed to update trust relationship")
            
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recommendations/{entity_id}")
async def get_trust_recommendations(entity_id: str,
                                  min_trust: float = Query(0.6, ge=0.0, le=1.0),
                                  limit: int = Query(10, ge=1, le=50),
                                  settings: Settings = Depends(get_settings)):
    """Get trust-based recommendations for connections"""
    try:
        recommendations = await trust_engine.get_trust_recommendations(
            entity_id,
            min_trust,
            limit
        )
        
        return {
            'entity_id': entity_id,
            'recommendations': recommendations,
            'count': len(recommendations)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dimensions")
async def get_trust_dimensions(settings: Settings = Depends(get_settings)):
    """Get available trust dimensions"""
    return {
        'dimensions': [
            {
                'name': d.value,
                'description': f"Trust dimension: {d.value}"
            }
            for d in TrustDimension
        ]
    }


@router.post("/batch-calculate")
async def batch_calculate_trust(entity_ids: List[str] = Body(..., embed=True),
                              context: Optional[str] = None,
                              settings: Settings = Depends(get_settings)):
    """Calculate trust scores for multiple entities"""
    try:
        results = {}
        errors = []
        
        for entity_id in entity_ids:
            try:
                # Check cache first
                cached = await cache_manager.get_cached_trust_score(entity_id, context or 'global')
                if cached:
                    results[entity_id] = cached
                else:
                    score = await trust_engine.calculate_trust_score(entity_id, context)
                    results[entity_id] = score
                    # Cache the result
                    await cache_manager.cache_trust_score(entity_id, context or 'global', score)
                    
            except Exception as e:
                errors.append({
                    'entity_id': entity_id,
                    'error': str(e)
                })
                
        return {
            'results': results,
            'errors': errors,
            'successful': len(results),
            'failed': len(errors)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_trust_statistics(context: Optional[str] = None,
                             settings: Settings = Depends(get_settings)):
    """Get trust statistics for the graph"""
    try:
        # This would calculate overall trust statistics
        # Placeholder implementation
        return {
            'context': context or 'global',
            'statistics': {
                'average_trust': 0.0,
                'trust_distribution': {},
                'highly_trusted_count': 0,
                'low_trust_count': 0
            },
            'message': "Trust statistics calculation not yet implemented"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/verify")
async def verify_trust_path(from_id: str = Query(..., description="Source entity"),
                          to_id: str = Query(..., description="Target entity"),
                          min_trust: float = Query(0.5, ge=0.0, le=1.0),
                          max_hops: int = Query(3, ge=1, le=6),
                          settings: Settings = Depends(get_settings)):
    """Verify if trust path exists between entities"""
    try:
        # Propagate trust from source
        propagated = await trust_engine.propagate_trust(from_id, max_hops)
        
        if to_id in propagated and propagated[to_id] >= min_trust:
            return {
                'trusted': True,
                'trust_level': propagated[to_id],
                'message': f"Trust path found with level {propagated[to_id]:.3f}"
            }
        else:
            return {
                'trusted': False,
                'trust_level': propagated.get(to_id, 0.0),
                'message': "No sufficient trust path found"
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def set_dependencies(te: TrustEngine, cm: CacheManager):
    """Set global dependencies"""
    global trust_engine, cache_manager
    trust_engine = te
    cache_manager = cm 