"""
Feature store API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Body, Request
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from datetime import datetime
import logging

from ..core.feature_store import (
    FeatureStore, 
    FeatureDefinition as CoreFeatureDefinition,
    FeatureType as CoreFeatureType,
    FeatureStatus
)

logger = logging.getLogger(__name__)
router = APIRouter()


class FeatureDefinition(BaseModel):
    """Feature definition API model"""
    name: str = Field(..., description="Feature name")
    description: str = Field(..., description="Feature description")
    feature_type: str = Field(..., description="Feature type (numeric, categorical, embedding, etc.)")
    data_type: str = Field(..., description="Data type (float, int, string, etc.)")
    default_value: Optional[Any] = Field(None, description="Default value if not found")
    tags: List[str] = Field(default_factory=list)
    owner: Optional[str] = Field(None)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class FeatureDefinitionResponse(FeatureDefinition):
    """Feature definition response"""
    version: int
    status: str
    created_at: datetime
    updated_at: datetime


class FeatureValue(BaseModel):
    """Feature value model"""
    entity_id: str
    features: Dict[str, Any]
    event_timestamp: Optional[datetime] = None


class GetFeaturesRequest(BaseModel):
    """Get features request"""
    entity_ids: List[str]
    feature_names: List[str]
    use_default: bool = True


class FeatureStatisticsResponse(BaseModel):
    """Feature statistics response"""
    feature_name: str
    count: int
    mean: Optional[float]
    std: Optional[float]
    min: Optional[float]
    max: Optional[float]
    unique: Optional[int]
    null_count: int
    last_updated: datetime


def get_feature_store(request: Request) -> FeatureStore:
    """Get feature store instance from app state"""
    return request.app.state.feature_store


@router.post("/features/register", response_model=FeatureDefinitionResponse)
async def register_feature(
    feature: FeatureDefinition,
    feature_store: FeatureStore = Depends(get_feature_store)
):
    """Register a new feature or update existing"""
    try:
        # Convert to core feature definition
        core_feature = CoreFeatureDefinition(
            name=feature.name,
            description=feature.description,
            feature_type=CoreFeatureType(feature.feature_type),
            data_type=feature.data_type,
            default_value=feature.default_value,
            tags=feature.tags,
            owner=feature.owner or "system",
            metadata=feature.metadata
        )
        
        success = await feature_store.register_feature(core_feature)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to register feature")
            
        # Get updated feature from registry
        registered = feature_store._feature_registry.get(feature.name)
        
        return FeatureDefinitionResponse(
            **feature.dict(),
            version=registered.version,
            status=registered.status.value,
            created_at=registered.created_at,
            updated_at=registered.updated_at
        )
        
    except Exception as e:
        logger.error(f"Error registering feature: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/features", response_model=List[FeatureDefinitionResponse])
async def list_features(
    tags: Optional[List[str]] = Query(None),
    owner: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    feature_store: FeatureStore = Depends(get_feature_store)
):
    """List all registered features"""
    try:
        features = []
        
        for name, feature_def in feature_store._feature_registry.items():
            # Apply filters
            if tags and not any(tag in feature_def.tags for tag in tags):
                continue
            if owner and feature_def.owner != owner:
                continue
            if status and feature_def.status.value != status:
                continue
                
            features.append(FeatureDefinitionResponse(
                name=feature_def.name,
                description=feature_def.description,
                feature_type=feature_def.feature_type.value,
                data_type=feature_def.data_type,
                default_value=feature_def.default_value,
                tags=feature_def.tags,
                owner=feature_def.owner,
                metadata=feature_def.metadata,
                version=feature_def.version,
                status=feature_def.status.value,
                created_at=feature_def.created_at,
                updated_at=feature_def.updated_at
            ))
            
        return features
        
    except Exception as e:
        logger.error(f"Error listing features: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/features/{feature_name}", response_model=FeatureDefinitionResponse)
async def get_feature_definition(
    feature_name: str,
    feature_store: FeatureStore = Depends(get_feature_store)
):
    """Get feature definition"""
    try:
        feature_def = feature_store._feature_registry.get(feature_name)
        
        if not feature_def:
            raise HTTPException(status_code=404, detail=f"Feature {feature_name} not found")
            
        return FeatureDefinitionResponse(
            name=feature_def.name,
            description=feature_def.description,
            feature_type=feature_def.feature_type.value,
            data_type=feature_def.data_type,
            default_value=feature_def.default_value,
            tags=feature_def.tags,
            owner=feature_def.owner,
            metadata=feature_def.metadata,
            version=feature_def.version,
            status=feature_def.status.value,
            created_at=feature_def.created_at,
            updated_at=feature_def.updated_at
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting feature definition: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/features/set")
async def set_features(
    feature_value: FeatureValue,
    feature_store: FeatureStore = Depends(get_feature_store)
):
    """Set feature values for an entity"""
    try:
        success = await feature_store.set_features(
            entity_id=feature_value.entity_id,
            features=feature_value.features,
            event_timestamp=feature_value.event_timestamp
        )
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to set features")
            
        return {
            "status": "success",
            "entity_id": feature_value.entity_id,
            "features_updated": len(feature_value.features),
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Error setting features: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/features/get")
async def get_features(
    request: GetFeaturesRequest,
    feature_store: FeatureStore = Depends(get_feature_store)
):
    """Get feature values for entities"""
    try:
        # Single entity
        if len(request.entity_ids) == 1:
            features = feature_store.get_features(
                entity_id=request.entity_ids[0],
                feature_names=request.feature_names,
                use_default=request.use_default
            )
            
            return {
                "entity_id": request.entity_ids[0],
                "features": features
            }
        
        # Multiple entities
        else:
            df = feature_store.get_feature_batch(
                entity_ids=request.entity_ids,
                feature_names=request.feature_names,
                use_default=request.use_default
            )
            
            return {
                "features": df.to_dict('records')
            }
            
    except Exception as e:
        logger.error(f"Error getting features: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/features/{feature_name}/statistics", response_model=FeatureStatisticsResponse)
async def get_feature_statistics(
    feature_name: str,
    window_hours: Optional[int] = Query(None, description="Time window in hours"),
    feature_store: FeatureStore = Depends(get_feature_store)
):
    """Get feature statistics"""
    try:
        from datetime import timedelta
        
        window = timedelta(hours=window_hours) if window_hours else None
        stats = feature_store.get_feature_statistics(feature_name, window)
        
        if not stats:
            raise HTTPException(status_code=404, detail=f"No statistics found for feature {feature_name}")
            
        return FeatureStatisticsResponse(
            feature_name=feature_name,
            count=stats.get('count', 0),
            mean=stats.get('mean'),
            std=stats.get('std'),
            min=stats.get('min'),
            max=stats.get('max'),
            unique=stats.get('unique'),
            null_count=stats.get('null_count', 0),
            last_updated=datetime.fromisoformat(stats.get('last_updated', datetime.utcnow().isoformat()))
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting feature statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check(feature_store: FeatureStore = Depends(get_feature_store)):
    """Health check endpoint"""
    try:
        # Check Ignite connection
        ignite_connected = feature_store.ignite_client is not None
        
        # Check Pulsar connection
        pulsar_connected = feature_store.pulsar_client is not None
        
        # Check feature registry
        registry_loaded = len(feature_store._feature_registry) > 0
        
        healthy = ignite_connected and pulsar_connected
        
        return {
            "status": "healthy" if healthy else "unhealthy",
            "checks": {
                "ignite": "connected" if ignite_connected else "disconnected",
                "pulsar": "connected" if pulsar_connected else "disconnected",
                "registry": f"{len(feature_store._feature_registry)} features loaded"
            },
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow()
        } 