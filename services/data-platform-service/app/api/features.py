"""
Feature Store API endpoints
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from fastapi import APIRouter, Request, HTTPException, Query, Body
from pydantic import BaseModel

from ..feature_store.models import (
    FeatureGroup,
    Feature,
    FeatureSet,
    FeatureView,
    FeatureRequest,
    FeatureResponse
)

router = APIRouter(prefix="/features", tags=["feature-store"])


# Request models

class ComputeFeaturesRequest(BaseModel):
    """Request to compute features"""
    feature_group: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    incremental: bool = True


class HistoricalFeaturesRequest(BaseModel):
    """Request for historical features"""
    entity_data: List[Dict[str, Any]]
    feature_set: FeatureSet
    timestamp_column: str = "event_timestamp"


# Endpoints

@router.post("/groups")
async def create_feature_group(
    request: Request,
    feature_group: FeatureGroup
) -> Dict[str, Any]:
    """Create a new feature group"""
    try:
        feature_store = request.app.state.feature_store_manager
        
        result = await feature_store.create_feature_group(feature_group)
        
        return {
            "status": "success",
            **result
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/groups")
async def list_feature_groups(
    request: Request,
    tags: Optional[List[str]] = Query(None),
    owner: Optional[str] = Query(None)
) -> List[Dict[str, Any]]:
    """List all feature groups"""
    try:
        registry = request.app.state.feature_registry
        
        groups = await registry.list_feature_groups(tags=tags, owner=owner)
        
        return groups
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/groups/{name}")
async def get_feature_group(
    request: Request,
    name: str
) -> Dict[str, Any]:
    """Get feature group by name"""
    try:
        registry = request.app.state.feature_registry
        
        group = await registry.get_feature_group(name)
        
        if not group:
            raise HTTPException(status_code=404, detail="Feature group not found")
            
        return group.dict()
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/compute")
async def compute_features(
    request: Request,
    compute_request: ComputeFeaturesRequest
) -> Dict[str, Any]:
    """Compute features for a feature group"""
    try:
        feature_store = request.app.state.feature_store_manager
        
        result = await feature_store.compute_features(
            feature_group_name=compute_request.feature_group,
            start_date=compute_request.start_date,
            end_date=compute_request.end_date,
            incremental=compute_request.incremental
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/serve")
async def get_online_features(
    request: Request,
    feature_request: FeatureRequest
) -> FeatureResponse:
    """Get online features for entities"""
    try:
        start_time = datetime.utcnow()
        
        feature_store = request.app.state.feature_store_manager
        
        # Create feature set from request
        feature_set = FeatureSet(
            name="online_request",
            feature_groups=list(set(f.split(".")[0] for f in feature_request.features)),
            features=feature_request.features,
            entity_keys=list(feature_request.entities.keys()),
            created_by="api"
        )
        
        # Get features
        features_df = await feature_store.get_features(
            feature_set=feature_set,
            entities=feature_request.entities,
            timestamp=feature_request.timestamp
        )
        
        # Convert to response format
        features_dict = features_df.iloc[0].to_dict() if len(features_df) > 0 else {}
        
        # Calculate latency
        latency_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        response = FeatureResponse(
            features=features_dict,
            metadata={
                "feature_count": len(features_dict),
                "timestamp": feature_request.timestamp or datetime.utcnow()
            } if feature_request.include_metadata else None,
            latency_ms=latency_ms
        )
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/historical")
async def get_historical_features(
    request: Request,
    historical_request: HistoricalFeaturesRequest
) -> Dict[str, Any]:
    """Get historical features with point-in-time correctness"""
    try:
        import pandas as pd
        
        feature_store = request.app.state.feature_store_manager
        
        # Convert entity data to DataFrame
        entity_df = pd.DataFrame(historical_request.entity_data)
        
        # Get historical features
        result_df = await feature_store.get_historical_features(
            entity_df=entity_df,
            feature_set=historical_request.feature_set,
            timestamp_column=historical_request.timestamp_column
        )
        
        return {
            "row_count": len(result_df),
            "features": result_df.to_dict(orient="records")
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_features(
    request: Request,
    query: str = Query(..., description="Search query"),
    tags: Optional[List[str]] = Query(None),
    owner: Optional[str] = Query(None)
) -> List[Dict[str, Any]]:
    """Search for features"""
    try:
        registry = request.app.state.feature_registry
        
        features = await registry.search_features(
            query=query,
            tags=tags,
            owner=owner
        )
        
        return features
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lineage/{feature_group}/{feature_name}")
async def get_feature_lineage(
    request: Request,
    feature_group: str,
    feature_name: str
) -> Dict[str, Any]:
    """Get feature lineage"""
    try:
        registry = request.app.state.feature_registry
        
        lineage = await registry.get_feature_lineage(
            feature_name=feature_name,
            feature_group=feature_group
        )
        
        return lineage
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/views")
async def create_feature_view(
    request: Request,
    feature_view: FeatureView
) -> Dict[str, Any]:
    """Create a materialized feature view"""
    try:
        feature_store = request.app.state.feature_store_manager
        
        result = await feature_store.create_feature_view(feature_view)
        
        return {
            "status": "success",
            **result
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/materialize")
async def materialize_features(
    request: Request,
    feature_group: str = Body(...),
    start_date: Optional[datetime] = Body(default=None),
    end_date: Optional[datetime] = Body(default=None),
    write_to_online: bool = Body(default=True),
    write_to_offline: bool = Body(default=True),
    partition_by: Optional[List[str]] = Body(default=None)
) -> Dict[str, Any]:
    """Materialize features to online and offline stores"""
    try:
        feature_store = request.app.state.feature_store_manager
        
        # Get feature group
        group = await feature_store.registry.get_feature_group(feature_group)
        if not group:
            raise HTTPException(status_code=404, detail=f"Feature group not found: {feature_group}")
        
        result = await feature_store.materialize_features(
            feature_group=group,
            start_date=start_date,
            end_date=end_date,
            write_to_online=write_to_online
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cache/statistics")
async def get_cache_statistics(request: Request) -> Dict[str, Any]:
    """Get feature cache statistics"""
    try:
        feature_store = request.app.state.feature_store_manager
        
        # Get Ignite cache stats
        cache_stats = await feature_store.ignite_cache.get_statistics()
        
        # Get feature server stats
        server_stats = feature_store.feature_server.cache_stats
        
        return {
            "ignite_cache": cache_stats,
            "feature_server": server_stats,
            "total_features_served": feature_store.stats["features_served"]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/groups/{group_name}/statistics")
async def get_feature_group_statistics(
    request: Request,
    group_name: str
) -> Dict[str, Any]:
    """Get statistics for a feature group"""
    try:
        feature_store = request.app.state.feature_store_manager
        
        # Get feature group
        group = await feature_store.registry.get_feature_group(group_name)
        if not group:
            raise HTTPException(status_code=404, detail=f"Feature group not found: {group_name}")
        
        # Get statistics from the data lake
        stats = await feature_store.get_feature_statistics(group_name)
        
        return {
            "feature_group": group_name,
            "statistics": stats,
            "features": len(group.features),
            "entity_keys": group.entity_keys
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/groups/{name}/refresh")
async def refresh_feature_group(
    request: Request,
    name: str,
    force: bool = Query(False, description="Force full refresh")
) -> Dict[str, Any]:
    """Refresh features for a group"""
    try:
        feature_store = request.app.state.feature_store_manager
        
        result = await feature_store.compute_features(
            feature_group_name=name,
            incremental=not force
        )
        
        return {
            "status": "success",
            "refreshed": True,
            **result
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 