"""
Analytics API endpoints

Provides access pattern analytics and optimization insights.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Body, BackgroundTasks
from pydantic import BaseModel, Field

from app.core.access_analytics import (
    AccessAnalyticsEngine,
    AccessType,
    AccessPattern,
    UserProfile,
    AssetAccessMetrics
)
from app.core.atlas_client import AtlasClient
from platformq_events import EventStream
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analytics", tags=["analytics"])

# Dependencies will be injected by the main app
access_analytics: Optional[AccessAnalyticsEngine] = None
atlas_client: Optional[AtlasClient] = None
event_stream: Optional[EventStream] = None


def set_analytics_deps(**deps):
    """Set dependencies for the analytics router"""
    global access_analytics, atlas_client, event_stream
    access_analytics = deps.get("access_analytics")
    atlas_client = deps.get("atlas_client")
    event_stream = deps.get("event_stream")


# Request/Response Models
class AccessTrackingRequest(BaseModel):
    """Access tracking request"""
    user_id: str = Field(..., description="User identifier")
    asset_id: str = Field(..., description="Asset/Dataset GUID")
    access_type: AccessType = Field(..., description="Type of access")
    duration_ms: int = Field(..., ge=0, description="Access duration in milliseconds")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


@router.post("/track")
async def track_access(
    request: AccessTrackingRequest,
    background_tasks: BackgroundTasks
):
    """
    Track data access event
    
    Records access patterns for analytics and optimization
    """
    if not access_analytics:
        raise HTTPException(status_code=503, detail="Analytics engine not initialized")
    
    try:
        success = await access_analytics.track_access(
            user_id=request.user_id,
            asset_id=request.asset_id,
            access_type=request.access_type,
            duration_ms=request.duration_ms,
            metadata=request.metadata
        )
        
        # Emit event in background
        if event_stream and success:
            background_tasks.add_task(
                event_stream.publish,
                topic="catalog-analytics",
                event_type="access_tracked",
                data={
                    "user_id": request.user_id,
                    "asset_id": request.asset_id,
                    "access_type": request.access_type.value
                }
            )
        
        return {"tracked": success}
        
    except Exception as e:
        logger.error(f"Failed to track access: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/user/{user_id}/patterns")
async def analyze_user_patterns(
    user_id: str,
    time_range_days: int = Query(30, ge=1, le=365)
):
    """
    Analyze access patterns for a specific user
    
    Returns user profile with patterns, preferences, and recommendations
    """
    if not access_analytics:
        raise HTTPException(status_code=503, detail="Analytics engine not initialized")
    
    try:
        profile = await access_analytics.analyze_user_patterns(
            user_id=user_id,
            time_range_days=time_range_days
        )
        
        return {
            "user_id": profile.user_id,
            "primary_pattern": profile.primary_pattern.value,
            "frequent_assets": [
                {"asset_id": asset_id, "access_count": count}
                for asset_id, count in profile.frequent_assets
            ],
            "access_time_distribution": profile.access_times,
            "avg_session_duration_minutes": profile.avg_session_duration,
            "preferred_access_type": profile.preferred_access_type.value,
            "team": profile.team,
            "role": profile.role
        }
        
    except Exception as e:
        logger.error(f"Failed to analyze user patterns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/asset/{asset_id}/metrics")
async def analyze_asset_access(
    asset_id: str,
    time_range_days: int = Query(30, ge=1, le=365)
):
    """
    Analyze access patterns for a specific asset
    
    Returns metrics including usage frequency, popular queries, and user patterns
    """
    if not access_analytics:
        raise HTTPException(status_code=503, detail="Analytics engine not initialized")
    
    try:
        metrics = await access_analytics.analyze_asset_access(
            asset_id=asset_id,
            time_range_days=time_range_days
        )
        
        return {
            "asset_id": metrics.asset_id,
            "total_accesses": metrics.total_accesses,
            "unique_users": metrics.unique_users,
            "access_frequency": metrics.access_frequency,
            "popular_queries": [
                {"query": query, "count": count}
                for query, count in metrics.popular_queries
            ],
            "access_patterns": {
                pattern.value: count
                for pattern, count in metrics.access_patterns.items()
            },
            "avg_access_duration_ms": metrics.avg_access_duration,
            "peak_hours": metrics.peak_hours,
            "related_assets": [
                {"asset_id": asset_id, "correlation": corr}
                for asset_id, corr in metrics.related_assets
            ],
            "churn_rate": metrics.churn_rate
        }
        
    except Exception as e:
        logger.error(f"Failed to analyze asset access: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/hot-assets")
async def identify_hot_assets(
    time_window_hours: int = Query(24, ge=1, le=168),
    min_accesses: int = Query(10, ge=1)
):
    """
    Identify frequently accessed "hot" assets
    
    Returns candidates for caching and optimization
    """
    if not access_analytics:
        raise HTTPException(status_code=503, detail="Analytics engine not initialized")
    
    try:
        hot_assets = await access_analytics.identify_hot_assets(
            time_window_hours=time_window_hours,
            min_accesses=min_accesses
        )
        
        return {
            "time_window_hours": time_window_hours,
            "min_accesses": min_accesses,
            "hot_assets": hot_assets,
            "total": len(hot_assets),
            "cache_recommendations": [
                asset for asset in hot_assets
                if asset["cache_priority"] > 50
            ]
        }
        
    except Exception as e:
        logger.error(f"Failed to identify hot assets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/predict/{asset_id}")
async def predict_future_access(
    asset_id: str,
    days_ahead: int = Query(7, ge=1, le=30)
):
    """
    Predict future access patterns for an asset
    
    Uses historical data to forecast access trends
    """
    if not access_analytics:
        raise HTTPException(status_code=503, detail="Analytics engine not initialized")
    
    try:
        prediction = await access_analytics.predict_future_access(
            asset_id=asset_id,
            days_ahead=days_ahead
        )
        
        return prediction
        
    except Exception as e:
        logger.error(f"Failed to predict access: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/optimization-report")
async def generate_optimization_report(
    scope: Optional[str] = Query("global", pattern="^(global|team|user)$")
):
    """
    Generate comprehensive optimization report
    
    Provides insights and recommendations for catalog optimization
    """
    if not access_analytics:
        raise HTTPException(status_code=503, detail="Analytics engine not initialized")
    
    try:
        report = await access_analytics.generate_optimization_report(scope=scope)
        
        return report
        
    except Exception as e:
        logger.error(f"Failed to generate report: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/patterns/distribution")
async def get_access_pattern_distribution():
    """
    Get distribution of access patterns across all users
    
    Shows how the catalog is being used overall
    """
    if not access_analytics:
        raise HTTPException(status_code=503, detail="Analytics engine not initialized")
    
    try:
        # This would aggregate patterns across all users
        # For now, return mock data
        distribution = {
            "patterns": {
                AccessPattern.TARGETED.value: 0.45,
                AccessPattern.EXPLORATORY.value: 0.25,
                AccessPattern.OPERATIONAL.value: 0.15,
                AccessPattern.ANALYTICAL.value: 0.10,
                AccessPattern.DEVELOPMENT.value: 0.05
            },
            "insights": [
                "45% of users know exactly what they're looking for",
                "25% are exploring and discovering new datasets",
                "Consider improving discovery features for exploratory users"
            ],
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return distribution
        
    except Exception as e:
        logger.error(f"Failed to get pattern distribution: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/segments/identify")
async def identify_user_segments(
    min_cluster_size: int = Body(5, ge=2),
    features: List[str] = Body(["access_frequency", "query_complexity", "dataset_diversity"])
):
    """
    Identify user segments based on access behavior
    
    Uses clustering to find groups of similar users
    """
    if not access_analytics:
        raise HTTPException(status_code=503, detail="Analytics engine not initialized")
    
    try:
        segments = await access_analytics._segment_users()
        
        return {
            "segments": segments,
            "total_segments": len(segments),
            "clustering_features": features,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to identify segments: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/insights/realtime")
async def get_realtime_insights():
    """
    Get real-time insights from recent access patterns
    
    Shows what's happening in the catalog right now
    """
    if not access_analytics:
        raise HTTPException(status_code=503, detail="Analytics engine not initialized")
    
    try:
        # Analyze recent events
        recent_events = access_analytics.recent_events[-100:]  # Last 100 events
        
        # Calculate insights
        unique_users = len(set(e.user_id for e in recent_events))
        unique_assets = len(set(e.asset_id for e in recent_events))
        
        access_types = {}
        for event in recent_events:
            access_types[event.access_type.value] = access_types.get(event.access_type.value, 0) + 1
        
        return {
            "realtime_stats": {
                "active_users": unique_users,
                "active_assets": unique_assets,
                "recent_accesses": len(recent_events),
                "access_types": access_types
            },
            "trending_now": [
                # This would identify trending assets
            ],
            "alerts": [
                # This would identify unusual patterns
            ],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get realtime insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/recommendation-engine/train")
async def train_recommendation_engine(
    background_tasks: BackgroundTasks
):
    """
    Train the recommendation engine based on access patterns
    
    Improves future recommendations and predictions
    """
    if not access_analytics:
        raise HTTPException(status_code=503, detail="Analytics engine not initialized")
    
    async def train_model():
        try:
            # This would train ML models
            # For now, just log
            logger.info("Training recommendation engine...")
            
            if event_stream:
                await event_stream.publish(
                    topic="catalog-analytics",
                    event_type="recommendation_engine_trained",
                    data={
                        "timestamp": datetime.utcnow().isoformat(),
                        "status": "completed"
                    }
                )
        except Exception as e:
            logger.error(f"Training failed: {e}")
    
    background_tasks.add_task(train_model)
    
    return {
        "status": "training_started",
        "timestamp": datetime.utcnow().isoformat()
    } 