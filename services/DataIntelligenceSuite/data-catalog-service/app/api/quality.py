"""
Quality Integration API endpoints

Handles data quality scoring, rules, and trust level management.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Path, Body, BackgroundTasks
from pydantic import BaseModel, Field

from app.core.quality_integration import (
    QualityIntegrationEngine,
    QualityDimension,
    TrustLevel,
    QualityProfile,
    QualityRule
)
from app.core.atlas_client import AtlasClient
from platformq_events import EventStream
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/quality", tags=["quality"])

# Dependencies will be injected by the main app
quality_integration: Optional[QualityIntegrationEngine] = None
atlas_client: Optional[AtlasClient] = None
event_stream: Optional[EventStream] = None


def set_quality_deps(**deps):
    """Set dependencies for the quality router"""
    global quality_integration, atlas_client, event_stream
    quality_integration = deps.get("quality_integration")
    atlas_client = deps.get("atlas_client")
    event_stream = deps.get("event_stream")


# Request/Response Models
class QualityAssessmentRequest(BaseModel):
    """Request for quality assessment"""
    dataset_id: str = Field(..., description="Dataset GUID to assess")
    force_refresh: bool = Field(False, description="Force refresh even if cached")


class QualityRuleCreate(BaseModel):
    """Create quality rule request"""
    name: str = Field(..., description="Rule name")
    description: str = Field(..., description="Rule description")
    dimension: QualityDimension = Field(..., description="Quality dimension")
    severity: str = Field(..., pattern="^(critical|high|medium|low)$")
    expression: str = Field(..., description="Rule expression/SQL")
    threshold: float = Field(..., ge=0.0, le=1.0)
    enabled: bool = Field(True)


@router.post("/assess")
async def assess_dataset_quality(
    request: QualityAssessmentRequest,
    background_tasks: BackgroundTasks
):
    """
    Assess quality of a dataset
    
    Returns quality profile with scores, trust level, and recommendations
    """
    if not quality_integration:
        raise HTTPException(status_code=503, detail="Quality integration not initialized")
    
    try:
        profile = await quality_integration.assess_dataset_quality(
            dataset_id=request.dataset_id,
            force_refresh=request.force_refresh
        )
        
        # Emit event in background
        if event_stream:
            background_tasks.add_task(
                event_stream.publish,
                topic="catalog-quality",
                event_type="quality_assessed",
                data={
                    "dataset_id": request.dataset_id,
                    "overall_score": profile.overall_score,
                    "trust_level": profile.trust_level.value,
                    "issues_count": len(profile.issues)
                }
            )
        
        return {
            "dataset_id": profile.dataset_id,
            "overall_score": profile.overall_score,
            "trust_level": profile.trust_level.value,
            "dimensions": {dim.value: score for dim, score in profile.dimensions.items()},
            "issues": profile.issues[:10],  # Top 10 issues
            "recommendations": profile.recommendations,
            "last_assessed": profile.last_assessed.isoformat(),
            "trend": profile.trend,
            "metadata": profile.metadata
        }
        
    except Exception as e:
        logger.error(f"Failed to assess quality: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rules/{dataset_id}")
async def create_quality_rules(
    dataset_id: str,
    auto_generate: bool = Query(True, description="Auto-generate rules from schema")
):
    """
    Create quality rules for a dataset
    
    Can auto-generate rules based on schema or accept custom rules
    """
    if not quality_integration:
        raise HTTPException(status_code=503, detail="Quality integration not initialized")
    
    try:
        rules = await quality_integration.create_quality_rules(
            dataset_id=dataset_id,
            auto_generate=auto_generate
        )
        
        return {
            "dataset_id": dataset_id,
            "rules_created": len(rules),
            "rules": [
                {
                    "rule_id": rule.rule_id,
                    "name": rule.name,
                    "dimension": rule.dimension.value,
                    "severity": rule.severity,
                    "threshold": rule.threshold,
                    "enabled": rule.enabled
                }
                for rule in rules
            ]
        }
        
    except Exception as e:
        logger.error(f"Failed to create quality rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trends/{dataset_id}")
async def get_quality_trends(
    dataset_id: str,
    days: int = Query(30, ge=1, le=365, description="Days of history")
):
    """
    Get quality trends for a dataset
    
    Shows how quality has changed over time
    """
    if not quality_integration:
        raise HTTPException(status_code=503, detail="Quality integration not initialized")
    
    try:
        trends = await quality_integration.get_quality_trends(
            dataset_id=dataset_id,
            days=days
        )
        
        return trends
        
    except Exception as e:
        logger.error(f"Failed to get quality trends: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recommendations/{dataset_id}")
async def get_quality_recommendations(dataset_id: str):
    """
    Get quality improvement recommendations for a dataset
    
    Provides actionable steps to improve data quality
    """
    if not quality_integration:
        raise HTTPException(status_code=503, detail="Quality integration not initialized")
    
    try:
        recommendations = await quality_integration.get_quality_recommendations(
            dataset_id=dataset_id
        )
        
        return {
            "dataset_id": dataset_id,
            "recommendations": recommendations,
            "total": len(recommendations)
        }
        
    except Exception as e:
        logger.error(f"Failed to get recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard")
async def get_quality_dashboard(
    scope: Optional[str] = Query(None, description="Filter by scope (layer/owner/tag)")
):
    """
    Get quality dashboard data
    
    Provides summary statistics and insights across the catalog
    """
    if not quality_integration:
        raise HTTPException(status_code=503, detail="Quality integration not initialized")
    
    try:
        # Parse scope to get catalog subset
        catalog_subset = None
        if scope:
            # This would filter datasets based on scope
            # For now, we'll use all datasets
            pass
        
        dashboard_data = await quality_integration.create_quality_dashboard_data(
            catalog_subset=catalog_subset
        )
        
        return dashboard_data
        
    except Exception as e:
        logger.error(f"Failed to get dashboard data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trust-level/{dataset_id}")
async def update_trust_level(
    dataset_id: str,
    trust_level: TrustLevel = Body(...),
    reason: str = Body(..., description="Reason for manual override")
):
    """
    Manually update trust level for a dataset
    
    Allows authorized users to override automatic trust level
    """
    if not quality_integration:
        raise HTTPException(status_code=503, detail="Quality integration not initialized")
    
    try:
        # Update trust level in catalog
        entity = await atlas_client.get_entity(dataset_id)
        if not entity:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        await atlas_client.partial_update_entity(
            dataset_id,
            {
                "dataTrustLevel": trust_level.value,
                "trustLevelOverride": True,
                "trustLevelReason": reason,
                "trustLevelUpdatedBy": "manual",  # Would get from auth context
                "trustLevelUpdatedAt": datetime.utcnow().isoformat()
            }
        )
        
        # Emit event
        if event_stream:
            await event_stream.publish(
                topic="catalog-quality",
                event_type="trust_level_updated",
                data={
                    "dataset_id": dataset_id,
                    "trust_level": trust_level.value,
                    "reason": reason,
                    "manual_override": True
                }
            )
        
        return {
            "dataset_id": dataset_id,
            "trust_level": trust_level.value,
            "updated": True
        }
        
    except Exception as e:
        logger.error(f"Failed to update trust level: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/score/{dataset_id}")
async def get_quality_score(dataset_id: str):
    """
    Get current quality score for a dataset
    
    Quick endpoint to get just the score without full assessment
    """
    if not quality_integration:
        raise HTTPException(status_code=503, detail="Quality integration not initialized")
    
    try:
        # Try to get from cache first
        if dataset_id in quality_integration.quality_cache:
            cached = quality_integration.quality_cache[dataset_id]
            if (datetime.utcnow() - cached["timestamp"]).seconds < quality_integration.cache_ttl:
                profile = cached["profile"]
                return {
                    "dataset_id": dataset_id,
                    "overall_score": profile.overall_score,
                    "trust_level": profile.trust_level.value,
                    "cached": True,
                    "last_assessed": profile.last_assessed.isoformat()
                }
        
        # Get from catalog
        entity = await atlas_client.get_entity(dataset_id)
        if not entity:
            raise HTTPException(status_code=404, detail="Dataset not found")
        
        attrs = entity.get("attributes", {})
        return {
            "dataset_id": dataset_id,
            "overall_score": attrs.get("dataQualityScore", 0.0),
            "trust_level": attrs.get("dataTrustLevel", "unknown"),
            "cached": False,
            "last_assessed": attrs.get("qualityLastAssessed")
        }
        
    except Exception as e:
        logger.error(f"Failed to get quality score: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch-assess")
async def batch_assess_quality(
    dataset_ids: List[str] = Body(...),
    background_tasks: BackgroundTasks
):
    """
    Assess quality for multiple datasets
    
    Useful for bulk operations or scheduled assessments
    """
    if not quality_integration:
        raise HTTPException(status_code=503, detail="Quality integration not initialized")
    
    async def run_batch_assessment():
        results = []
        for dataset_id in dataset_ids:
            try:
                profile = await quality_integration.assess_dataset_quality(dataset_id)
                results.append({
                    "dataset_id": dataset_id,
                    "status": "success",
                    "overall_score": profile.overall_score,
                    "trust_level": profile.trust_level.value
                })
            except Exception as e:
                results.append({
                    "dataset_id": dataset_id,
                    "status": "failed",
                    "error": str(e)
                })
        
        # Emit completion event
        if event_stream:
            await event_stream.publish(
                topic="catalog-quality",
                event_type="batch_assessment_completed",
                data={
                    "total_datasets": len(dataset_ids),
                    "successful": len([r for r in results if r["status"] == "success"]),
                    "failed": len([r for r in results if r["status"] == "failed"])
                }
            )
    
    background_tasks.add_task(run_batch_assessment)
    
    return {
        "status": "batch_assessment_started",
        "datasets_count": len(dataset_ids),
        "timestamp": datetime.utcnow().isoformat()
    } 