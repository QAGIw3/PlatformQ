"""
ML Model Lineage API

Provides RESTful endpoints for ML model lineage tracking and visualization.
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field

from ..ml_model_lineage import (
    MLModelLineageTracker, ModelNode, DatasetNode, LineageEdge,
    ModelRelationType, ArtifactType
)

router = APIRouter(prefix="/api/v1/ml-lineage", tags=["ML Lineage"])


# Request/Response Models
class ModelNodeRequest(BaseModel):
    """Request model for adding a model node"""
    model_id: str
    name: str
    version: str
    algorithm: str
    framework: str
    metrics: Dict[str, float] = Field(default_factory=dict)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list)
    status: str = "active"


class DatasetNodeRequest(BaseModel):
    """Request model for adding a dataset node"""
    dataset_id: str
    name: str
    version: str
    size_bytes: int
    row_count: int
    feature_count: int
    schema_hash: str
    source_type: str = "raw"
    tags: List[str] = Field(default_factory=list)


class LineageRelationshipRequest(BaseModel):
    """Request model for adding a lineage relationship"""
    from_id: str
    to_id: str
    relationship_type: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ImpactAnalysisRequest(BaseModel):
    """Request model for impact analysis"""
    artifact_id: str
    change_type: str = "update"  # update, major_update, delete, minor_update


class SimilaritySearchRequest(BaseModel):
    """Request model for similarity search"""
    model_id: str
    similarity_threshold: float = Field(default=0.7, ge=0.0, le=1.0)


# Dependency to get lineage tracker
async def get_lineage_tracker() -> MLModelLineageTracker:
    """Get ML lineage tracker instance"""
    # In production, this would get the actual instance from app state
    from ..main import ml_lineage_tracker
    return ml_lineage_tracker


@router.post("/models")
async def add_model(
    model: ModelNodeRequest,
    tracker: MLModelLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Add a new model to the lineage graph"""
    try:
        model_node = ModelNode(
            model_id=model.model_id,
            name=model.name,
            version=model.version,
            algorithm=model.algorithm,
            framework=model.framework,
            created_at=datetime.utcnow(),
            metrics=model.metrics,
            parameters=model.parameters,
            tags=model.tags,
            status=model.status
        )
        
        model_id = await tracker.add_model(model_node)
        
        return {
            "status": "success",
            "model_id": model_id,
            "message": "Model added to lineage graph"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/datasets")
async def add_dataset(
    dataset: DatasetNodeRequest,
    tracker: MLModelLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Add a new dataset to the lineage graph"""
    try:
        dataset_node = DatasetNode(
            dataset_id=dataset.dataset_id,
            name=dataset.name,
            version=dataset.version,
            size_bytes=dataset.size_bytes,
            row_count=dataset.row_count,
            feature_count=dataset.feature_count,
            created_at=datetime.utcnow(),
            schema_hash=dataset.schema_hash,
            source_type=dataset.source_type,
            tags=dataset.tags
        )
        
        dataset_id = await tracker.add_dataset(dataset_node)
        
        return {
            "status": "success",
            "dataset_id": dataset_id,
            "message": "Dataset added to lineage graph"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/relationships")
async def add_relationship(
    relationship: LineageRelationshipRequest,
    tracker: MLModelLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Add a lineage relationship between artifacts"""
    try:
        # Validate relationship type
        try:
            rel_type = ModelRelationType(relationship.relationship_type)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid relationship type: {relationship.relationship_type}"
            )
            
        edge = LineageEdge(
            from_id=relationship.from_id,
            to_id=relationship.to_id,
            relationship_type=rel_type,
            created_at=datetime.utcnow(),
            metadata=relationship.metadata
        )
        
        success = await tracker.add_lineage_relationship(edge)
        
        if success:
            return {
                "status": "success",
                "message": f"Relationship added: {relationship.from_id} -> {relationship.to_id}"
            }
        else:
            raise HTTPException(
                status_code=500,
                detail="Failed to add relationship"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/models/{model_id}/lineage")
async def get_model_lineage(
    model_id: str,
    depth: int = Query(default=3, ge=1, le=10),
    tracker: MLModelLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Get complete lineage for a model"""
    try:
        lineage = await tracker.get_model_lineage(model_id, depth)
        
        if not lineage:
            raise HTTPException(
                status_code=404,
                detail=f"Model {model_id} not found"
            )
            
        return lineage
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/impact-analysis")
async def analyze_impact(
    request: ImpactAnalysisRequest,
    tracker: MLModelLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Analyze impact of changes to an artifact"""
    try:
        impact = await tracker.analyze_change_impact(
            request.artifact_id,
            request.change_type
        )
        
        return {
            "artifact_id": request.artifact_id,
            "change_type": request.change_type,
            "impact": {
                "affected_models": impact.affected_models,
                "affected_deployments": impact.affected_deployments,
                "impact_score": impact.impact_score,
                "risk_level": impact.risk_level,
                "recommendations": impact.recommendations
            },
            "analyzed_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/similarity-search")
async def find_similar_models(
    request: SimilaritySearchRequest,
    tracker: MLModelLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Find models similar to a given model"""
    try:
        similar_models = await tracker.find_similar_models(
            request.model_id,
            request.similarity_threshold
        )
        
        return {
            "reference_model": request.model_id,
            "similarity_threshold": request.similarity_threshold,
            "similar_models": similar_models,
            "count": len(similar_models)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/models/{model_name}/evolution")
async def get_model_evolution(
    model_name: str,
    tracker: MLModelLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Track evolution of a model across versions"""
    try:
        evolution = await tracker.get_model_evolution(model_name)
        
        if not evolution["versions"]:
            raise HTTPException(
                status_code=404,
                detail=f"No versions found for model {model_name}"
            )
            
        return evolution
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/models/{model_id}/visualization")
async def visualize_lineage(
    model_id: str,
    format: str = Query(default="cytoscape", regex="^(cytoscape|raw)$"),
    tracker: MLModelLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Generate visualization data for model lineage"""
    try:
        visualization = await tracker.visualize_lineage(model_id, format)
        
        if not visualization.get("elements", {}).get("nodes"):
            raise HTTPException(
                status_code=404,
                detail=f"Model {model_id} not found"
            )
            
        return visualization
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch/models")
async def add_models_batch(
    models: List[ModelNodeRequest],
    tracker: MLModelLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Add multiple models in batch"""
    results = {
        "successful": [],
        "failed": []
    }
    
    for model in models:
        try:
            model_node = ModelNode(
                model_id=model.model_id,
                name=model.name,
                version=model.version,
                algorithm=model.algorithm,
                framework=model.framework,
                created_at=datetime.utcnow(),
                metrics=model.metrics,
                parameters=model.parameters,
                tags=model.tags,
                status=model.status
            )
            
            await tracker.add_model(model_node)
            results["successful"].append(model.model_id)
            
        except Exception as e:
            results["failed"].append({
                "model_id": model.model_id,
                "error": str(e)
            })
            
    return {
        "total": len(models),
        "successful": len(results["successful"]),
        "failed": len(results["failed"]),
        "results": results
    }


@router.post("/batch/relationships")
async def add_relationships_batch(
    relationships: List[LineageRelationshipRequest],
    tracker: MLModelLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Add multiple relationships in batch"""
    results = {
        "successful": [],
        "failed": []
    }
    
    for relationship in relationships:
        try:
            rel_type = ModelRelationType(relationship.relationship_type)
            edge = LineageEdge(
                from_id=relationship.from_id,
                to_id=relationship.to_id,
                relationship_type=rel_type,
                created_at=datetime.utcnow(),
                metadata=relationship.metadata
            )
            
            success = await tracker.add_lineage_relationship(edge)
            if success:
                results["successful"].append(f"{relationship.from_id}->{relationship.to_id}")
            else:
                results["failed"].append({
                    "relationship": f"{relationship.from_id}->{relationship.to_id}",
                    "error": "Failed to create relationship"
                })
                
        except Exception as e:
            results["failed"].append({
                "relationship": f"{relationship.from_id}->{relationship.to_id}",
                "error": str(e)
            })
            
    return {
        "total": len(relationships),
        "successful": len(results["successful"]),
        "failed": len(results["failed"]),
        "results": results
    }


@router.get("/stats")
async def get_lineage_stats(
    tracker: MLModelLineageTracker = Depends(get_lineage_tracker)
) -> Dict[str, Any]:
    """Get statistics about the lineage graph"""
    try:
        # Count different artifact types
        model_count = tracker.g.V().has("label", ArtifactType.MODEL.value).count().next()
        dataset_count = tracker.g.V().has("label", ArtifactType.DATASET.value).count().next()
        feature_count = tracker.g.V().has("label", ArtifactType.FEATURE_SET.value).count().next()
        experiment_count = tracker.g.V().has("label", ArtifactType.EXPERIMENT.value).count().next()
        
        # Count relationships
        total_edges = tracker.g.E().count().next()
        
        # Get active models
        active_models = tracker.g.V().has("label", ArtifactType.MODEL.value) \
            .has("status", "active").count().next()
            
        return {
            "artifacts": {
                "models": model_count,
                "datasets": dataset_count,
                "features": feature_count,
                "experiments": experiment_count
            },
            "relationships": total_edges,
            "active_models": active_models,
            "graph_density": total_edges / max(model_count + dataset_count, 1),
            "generated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 