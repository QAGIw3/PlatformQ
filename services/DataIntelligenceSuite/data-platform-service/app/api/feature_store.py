"""
Feature Store API routes.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Query, Body
from pydantic import BaseModel, Field
import pandas as pd

from app.engines.feature import (
    FeatureStore,
    FeatureRegistry,
    FeatureServer,
    FeatureCompute,
    FeatureType,
    FeatureStatus,
    FeatureDefinition,
    FeatureValue,
    FeatureSet,
    FeatureView,
    FeatureSchema,
    BatchRequest,
    StreamRequest
)
from app.core.dependencies import get_feature_store, get_feature_registry, get_feature_server, get_feature_compute

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter()


# Request/Response Models
class RegisterFeatureRequest(BaseModel):
    """Request to register a feature."""
    name: str
    description: str
    feature_type: FeatureType
    data_type: str
    shape: Optional[List[int]] = None
    default_value: Any = None
    tags: List[str] = Field(default_factory=list)
    owner: str = ""
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CreateFeatureSetRequest(BaseModel):
    """Request to create a feature set."""
    name: str
    description: str
    features: List[str]
    entity_type: str
    tags: List[str] = Field(default_factory=list)
    owner: str = ""


class CreateFeatureViewRequest(BaseModel):
    """Request to create a feature view."""
    name: str
    description: str
    feature_sets: List[str]
    features: Optional[List[str]] = None
    entity_types: List[str]
    join_keys: Dict[str, str] = Field(default_factory=dict)
    filters: Optional[str] = None


class WriteFeatureValuesRequest(BaseModel):
    """Request to write feature values."""
    values: List[Dict[str, Any]]
    validate: bool = True


class GetOnlineFeaturesRequest(BaseModel):
    """Request to get online features."""
    entity_ids: List[str]
    feature_names: List[str]
    include_metadata: bool = False


class ComputeFeaturesRequest(BaseModel):
    """Request to compute features."""
    data: List[Dict[str, Any]]
    transforms: List[str]
    output_format: str = "dataframe"


class CreatePipelineRequest(BaseModel):
    """Request to create a feature pipeline."""
    name: str
    steps: List[str]
    description: str = ""


# Feature Definition Endpoints
@router.post("/features/register")
async def register_feature(
    request: RegisterFeatureRequest,
    feature_store: FeatureStore = Depends(get_feature_store)
):
    """Register a new feature definition."""
    try:
        feature_def = FeatureDefinition(
            name=request.name,
            description=request.description,
            feature_type=request.feature_type,
            data_type=request.data_type,
            shape=tuple(request.shape) if request.shape else None,
            default_value=request.default_value,
            tags=request.tags,
            owner=request.owner,
            metadata=request.metadata
        )
        
        await feature_store.register_feature(feature_def)
        
        return {
            "status": "success",
            "message": f"Feature {request.name} registered successfully"
        }
        
    except Exception as e:
        logger.error(f"Error registering feature: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/feature-sets/create")
async def create_feature_set(
    request: CreateFeatureSetRequest,
    feature_store: FeatureStore = Depends(get_feature_store)
):
    """Create a feature set."""
    try:
        feature_set = FeatureSet(
            name=request.name,
            description=request.description,
            features=request.features,
            entity_type=request.entity_type,
            tags=request.tags,
            owner=request.owner
        )
        
        await feature_store.create_feature_set(feature_set)
        
        return {
            "status": "success",
            "message": f"Feature set {request.name} created successfully"
        }
        
    except Exception as e:
        logger.error(f"Error creating feature set: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/feature-views/create")
async def create_feature_view(
    request: CreateFeatureViewRequest,
    feature_store: FeatureStore = Depends(get_feature_store)
):
    """Create a feature view."""
    try:
        feature_view = FeatureView(
            name=request.name,
            description=request.description,
            feature_sets=request.feature_sets,
            features=request.features or [],
            entity_types=request.entity_types,
            join_keys=request.join_keys,
            filters=request.filters
        )
        
        await feature_store.create_feature_view(feature_view)
        
        return {
            "status": "success",
            "message": f"Feature view {request.name} created successfully"
        }
        
    except Exception as e:
        logger.error(f"Error creating feature view: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# Feature Value Endpoints
@router.post("/values/write")
async def write_feature_values(
    request: WriteFeatureValuesRequest,
    feature_store: FeatureStore = Depends(get_feature_store)
):
    """Write feature values to online store."""
    try:
        feature_values = []
        for item in request.values:
            fv = FeatureValue(
                entity_id=item["entity_id"],
                feature_name=item["feature_name"],
                value=item["value"],
                event_timestamp=datetime.fromisoformat(item["event_timestamp"]) if "event_timestamp" in item else None,
                metadata=item.get("metadata", {})
            )
            feature_values.append(fv)
        
        count = await feature_store.write_feature_values(feature_values, request.validate)
        
        return {
            "status": "success",
            "values_written": count
        }
        
    except Exception as e:
        logger.error(f"Error writing feature values: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/values/get-online")
async def get_online_features(
    request: GetOnlineFeaturesRequest,
    feature_store: FeatureStore = Depends(get_feature_store)
):
    """Get feature values from online store."""
    try:
        df = await feature_store.get_online_features(
            request.entity_ids,
            request.feature_names,
            request.include_metadata
        )
        
        return {
            "status": "success",
            "data": df.to_dict("records")
        }
        
    except Exception as e:
        logger.error(f"Error getting online features: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/values/vector/{entity_id}")
async def get_feature_vector(
    entity_id: str,
    feature_set: str,
    feature_store: FeatureStore = Depends(get_feature_store)
):
    """Get feature vector for an entity."""
    try:
        vector = await feature_store.get_feature_vector(entity_id, feature_set)
        
        if vector is None:
            raise HTTPException(status_code=404, detail="Feature vector not found")
        
        return {
            "status": "success",
            "entity_id": entity_id,
            "feature_set": feature_set,
            "vector": vector.tolist()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting feature vector: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# Feature Serving Endpoints
@router.get("/serve/online/{entity_id}")
async def serve_online_features(
    entity_id: str,
    features: List[str] = Query(...),
    use_cache: bool = True,
    feature_server: FeatureServer = Depends(get_feature_server)
):
    """Serve features for a single entity with low latency."""
    try:
        vector = await feature_server.get_online_features(
            entity_id,
            features,
            use_cache
        )
        
        if vector is None:
            raise HTTPException(status_code=404, detail="Features not found")
        
        return {
            "status": "success",
            "entity_id": entity_id,
            "features": vector.features,
            "timestamp": vector.timestamp.isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error serving online features: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/serve/batch")
async def serve_batch_features(
    entity_ids: List[str] = Body(...),
    feature_names: List[str] = Body(...),
    feature_view: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
    feature_server: FeatureServer = Depends(get_feature_server)
):
    """Serve features for multiple entities."""
    try:
        request = BatchRequest(
            entity_ids=entity_ids,
            feature_names=feature_names,
            feature_view=feature_view,
            filters=filters
        )
        
        df = await feature_server.get_batch_features(request)
        
        return {
            "status": "success",
            "data": df.to_dict("records"),
            "shape": list(df.shape)
        }
        
    except Exception as e:
        logger.error(f"Error serving batch features: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/serve/preload")
async def preload_features(
    entity_ids: List[str] = Body(...),
    feature_names: List[str] = Body(...),
    feature_server: FeatureServer = Depends(get_feature_server)
):
    """Preload features into hot cache."""
    try:
        await feature_server.preload_features(entity_ids, feature_names)
        
        return {
            "status": "success",
            "message": f"Preloaded {len(entity_ids)} entities with {len(feature_names)} features"
        }
        
    except Exception as e:
        logger.error(f"Error preloading features: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# Feature Compute Endpoints
@router.post("/compute/features")
async def compute_features(
    request: ComputeFeaturesRequest,
    feature_compute: FeatureCompute = Depends(get_feature_compute)
):
    """Compute features using transformations."""
    try:
        df = pd.DataFrame(request.data)
        result = await feature_compute.compute_features(
            df,
            request.transforms,
            request.output_format
        )
        
        if isinstance(result, pd.DataFrame):
            output = result.to_dict("records")
        else:
            output = result
        
        return {
            "status": "success",
            "result": output
        }
        
    except Exception as e:
        logger.error(f"Error computing features: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/compute/pipeline/create")
async def create_pipeline(
    request: CreatePipelineRequest,
    feature_compute: FeatureCompute = Depends(get_feature_compute)
):
    """Create a feature computation pipeline."""
    try:
        pipeline = feature_compute.create_pipeline(
            request.name,
            request.steps,
            request.description
        )
        
        return {
            "status": "success",
            "pipeline": {
                "name": pipeline.name,
                "steps": len(pipeline.steps),
                "input_features": pipeline.input_features,
                "output_features": pipeline.output_features
            }
        }
        
    except Exception as e:
        logger.error(f"Error creating pipeline: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/compute/pipeline/{pipeline_name}/validate")
async def validate_pipeline(
    pipeline_name: str,
    sample_data: List[Dict[str, Any]] = Body(...),
    feature_compute: FeatureCompute = Depends(get_feature_compute)
):
    """Validate a pipeline with sample data."""
    try:
        df = pd.DataFrame(sample_data)
        result = await feature_compute.validate_pipeline(pipeline_name, df)
        
        return {
            "status": "success",
            "validation": result
        }
        
    except Exception as e:
        logger.error(f"Error validating pipeline: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# Feature Registry Endpoints
@router.post("/registry/schema/{feature_name}")
async def register_schema(
    feature_name: str,
    data_type: str = Body(...),
    nullable: bool = True,
    constraints: Dict[str, Any] = Body(default={}),
    description: str = "",
    feature_registry: FeatureRegistry = Depends(get_feature_registry)
):
    """Register or update a feature schema."""
    try:
        schema = FeatureSchema(
            name=feature_name,
            data_type=data_type,
            nullable=nullable,
            constraints=constraints,
            description=description
        )
        
        version = await feature_registry.register_schema(feature_name, schema)
        
        return {
            "status": "success",
            "feature": feature_name,
            "version": version.version
        }
        
    except Exception as e:
        logger.error(f"Error registering schema: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/registry/lineage/{feature_name}")
async def get_feature_lineage(
    feature_name: str,
    feature_registry: FeatureRegistry = Depends(get_feature_registry)
):
    """Get lineage information for a feature."""
    try:
        lineage = await feature_registry.get_lineage(feature_name)
        
        if not lineage:
            raise HTTPException(status_code=404, detail="Feature lineage not found")
        
        return {
            "status": "success",
            "lineage": {
                "feature_name": lineage.feature_name,
                "source_features": lineage.source_features,
                "source_datasets": lineage.source_datasets,
                "transformations": lineage.transformations,
                "downstream_features": lineage.downstream_features
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting lineage: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/registry/dependencies/{feature_name}")
async def get_feature_dependencies(
    feature_name: str,
    recursive: bool = False,
    feature_registry: FeatureRegistry = Depends(get_feature_registry)
):
    """Get feature dependencies."""
    try:
        dependencies = await feature_registry.get_dependencies(
            feature_name,
            recursive
        )
        
        return {
            "status": "success",
            "feature": feature_name,
            "dependencies": dependencies,
            "recursive": recursive
        }
        
    except Exception as e:
        logger.error(f"Error getting dependencies: {e}")
        raise HTTPException(status_code=400, detail=str(e))


# Statistics Endpoints
@router.get("/stats/store")
async def get_store_statistics(
    feature_store: FeatureStore = Depends(get_feature_store)
):
    """Get feature store statistics."""
    try:
        stats = feature_store.get_feature_statistics()
        
        return {
            "status": "success",
            "statistics": stats
        }
        
    except Exception as e:
        logger.error(f"Error getting store statistics: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/stats/serving")
async def get_serving_statistics(
    feature_server: FeatureServer = Depends(get_feature_server)
):
    """Get feature serving statistics."""
    try:
        stats = feature_server.get_statistics()
        
        return {
            "status": "success",
            "statistics": stats
        }
        
    except Exception as e:
        logger.error(f"Error getting serving statistics: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/stats/compute")
async def get_compute_statistics(
    feature_compute: FeatureCompute = Depends(get_feature_compute)
):
    """Get feature compute statistics."""
    try:
        stats = feature_compute.get_statistics()
        
        return {
            "status": "success",
            "statistics": stats
        }
        
    except Exception as e:
        logger.error(f"Error getting compute statistics: {e}")
        raise HTTPException(status_code=400, detail=str(e)) 