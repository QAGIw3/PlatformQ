"""
Intelligent Search API endpoints

Provides AI-powered search capabilities for the data catalog.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Body, BackgroundTasks
from pydantic import BaseModel, Field

from app.core.catalog_search_integration import CatalogSearchIntegration, SearchIntent
from app.core.access_analytics import AccessAnalyticsEngine, AccessType
from app.core.atlas_client import AtlasClient
from platformq_events import EventStream
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/search/intelligent", tags=["intelligent-search"])

# Dependencies will be injected by the main app
catalog_search_integration: Optional[CatalogSearchIntegration] = None
access_analytics: Optional[AccessAnalyticsEngine] = None
atlas_client: Optional[AtlasClient] = None
event_stream: Optional[EventStream] = None


def set_intelligent_search_deps(**deps):
    """Set dependencies for the intelligent search router"""
    global catalog_search_integration, access_analytics, atlas_client, event_stream
    catalog_search_integration = deps.get("catalog_search_integration")
    access_analytics = deps.get("access_analytics")
    atlas_client = deps.get("atlas_client")
    event_stream = deps.get("event_stream")


# Request/Response Models
class IntelligentSearchRequest(BaseModel):
    """Intelligent search request"""
    query: str = Field(..., description="Natural language search query")
    filters: Optional[Dict[str, Any]] = Field(None, description="Additional filters")
    user_context: Optional[Dict[str, Any]] = Field(None, description="User context for personalization")
    include_recommendations: bool = Field(True, description="Include AI recommendations")
    max_results: int = Field(20, ge=1, le=100)


class SearchClickEvent(BaseModel):
    """Search result click event"""
    query: str = Field(..., description="Original search query")
    result_id: str = Field(..., description="Clicked result entity ID")
    position: int = Field(..., description="Position in search results")
    user_id: Optional[str] = Field(None)


@router.post("/search")
async def intelligent_catalog_search(
    request: IntelligentSearchRequest,
    background_tasks: BackgroundTasks
):
    """
    Perform AI-powered intelligent search across the catalog
    
    Features:
    - Natural language understanding
    - Intent detection
    - Personalized results
    - Quality-aware ranking
    - Related entity discovery
    """
    if not catalog_search_integration:
        raise HTTPException(status_code=503, detail="Search integration not initialized")
    
    try:
        # Perform intelligent search
        results = await catalog_search_integration.intelligent_search(
            query=request.query,
            filters=request.filters,
            user_context=request.user_context
        )
        
        # Track search analytics in background
        if access_analytics:
            user_id = request.user_context.get("user_id") if request.user_context else None
            background_tasks.add_task(
                catalog_search_integration.track_search_analytics,
                query=request.query,
                results_count=results["total"],
                user_id=user_id
            )
        
        # Emit search event
        if event_stream:
            background_tasks.add_task(
                event_stream.publish,
                topic="catalog-search",
                event_type="intelligent_search_performed",
                data={
                    "query": request.query,
                    "intent": results["intent"],
                    "results_count": results["total"],
                    "has_filters": bool(request.filters)
                }
            )
        
        return {
            "query": results["query"],
            "intent": results["intent"],
            "results": [
                {
                    "entity_id": r.entity_id,
                    "entity_type": r.entity_type,
                    "name": r.name,
                    "qualified_name": r.qualified_name,
                    "description": r.description,
                    "score": r.score,
                    "highlights": r.highlights,
                    "quality_score": r.quality_score,
                    "trust_level": r.trust_level,
                    "business_terms": r.business_terms,
                    "tags": r.tags,
                    "layer": r.layer,
                    "owner": r.owner,
                    "last_modified": r.last_modified.isoformat(),
                    "lineage_depth": r.lineage_depth,
                    "related_entities": r.related_entities,
                    "suggested_actions": r.suggested_actions
                }
                for r in results["results"]
            ],
            "total": results["total"],
            "facets": results["facets"],
            "insights": results["insights"],
            "suggestions": results["suggestions"],
            "query_analysis": results["query_analysis"]
        }
        
    except Exception as e:
        logger.error(f"Intelligent search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/suggestions")
async def get_search_suggestions(
    prefix: str = Query(..., min_length=2),
    context: Optional[str] = Query(None, description="Current context/page")
):
    """
    Get AI-powered search suggestions based on prefix
    
    Provides context-aware autocomplete suggestions
    """
    if not catalog_search_integration:
        raise HTTPException(status_code=503, detail="Search integration not initialized")
    
    try:
        context_dict = {"page": context} if context else None
        suggestions = await catalog_search_integration.get_search_suggestions(
            prefix=prefix,
            context=context_dict
        )
        
        return {
            "prefix": prefix,
            "suggestions": suggestions,
            "context": context
        }
        
    except Exception as e:
        logger.error(f"Failed to get suggestions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/click")
async def track_search_click(
    event: SearchClickEvent,
    background_tasks: BackgroundTasks
):
    """
    Track search result click for improving relevance
    
    Helps train the search model for better future results
    """
    if not catalog_search_integration:
        raise HTTPException(status_code=503, detail="Search integration not initialized")
    
    try:
        # Track click event
        background_tasks.add_task(
            catalog_search_integration.track_search_analytics,
            query=event.query,
            results_count=0,  # Not needed for click tracking
            clicked_result=event.result_id,
            user_id=event.user_id
        )
        
        # Track access event
        if access_analytics:
            background_tasks.add_task(
                access_analytics.track_access,
                user_id=event.user_id or "anonymous",
                asset_id=event.result_id,
                access_type=AccessType.VIEW,
                duration_ms=0,
                metadata={
                    "source": "intelligent_search",
                    "query": event.query,
                    "position": event.position
                }
            )
        
        return {"status": "tracked"}
        
    except Exception as e:
        logger.error(f"Failed to track click: {e}")
        return {"status": "failed", "error": str(e)}


@router.post("/feedback")
async def submit_search_feedback(
    query: str = Body(...),
    helpful: bool = Body(...),
    comment: Optional[str] = Body(None),
    user_id: Optional[str] = Body(None)
):
    """
    Submit feedback on search results quality
    
    Helps improve search relevance and AI models
    """
    if not event_stream:
        return {"status": "accepted"}
    
    try:
        await event_stream.publish(
            topic="catalog-search",
            event_type="search_feedback",
            data={
                "query": query,
                "helpful": helpful,
                "comment": comment,
                "user_id": user_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        return {"status": "submitted"}
        
    except Exception as e:
        logger.error(f"Failed to submit feedback: {e}")
        return {"status": "failed", "error": str(e)}


@router.get("/intents")
async def get_supported_intents():
    """
    Get list of supported search intents
    
    Helps users understand what types of queries are optimized
    """
    return {
        "intents": [
            {
                "intent": SearchIntent.FIND_DATASET.value,
                "description": "Find specific datasets by name or properties",
                "example_queries": [
                    "customer dataset",
                    "orders table in production",
                    "datasets owned by data team"
                ]
            },
            {
                "intent": SearchIntent.EXPLORE_SCHEMA.value,
                "description": "Explore dataset schemas and structures",
                "example_queries": [
                    "schema for user_events",
                    "columns in customer table",
                    "what fields are in orders"
                ]
            },
            {
                "intent": SearchIntent.TRACK_LINEAGE.value,
                "description": "Track data lineage and dependencies",
                "example_queries": [
                    "lineage for revenue_dashboard",
                    "what feeds into customer_360",
                    "downstream dependencies of orders"
                ]
            },
            {
                "intent": SearchIntent.FIND_QUALITY.value,
                "description": "Find datasets by quality metrics",
                "example_queries": [
                    "high quality customer data",
                    "datasets with quality issues",
                    "trusted revenue data"
                ]
            },
            {
                "intent": SearchIntent.DISCOVER_TERMS.value,
                "description": "Discover business terms and definitions",
                "example_queries": [
                    "what is customer lifetime value",
                    "business terms for revenue",
                    "glossary for product data"
                ]
            },
            {
                "intent": SearchIntent.EXPLORE_RELATIONSHIPS.value,
                "description": "Explore relationships between data assets",
                "example_queries": [
                    "related to customer orders",
                    "connected datasets for marketing",
                    "data assets similar to user_events"
                ]
            }
        ]
    }


@router.post("/explain")
async def explain_search_results(
    query: str = Body(...),
    entity_id: str = Body(..., description="Entity ID to explain ranking for")
):
    """
    Explain why a specific result appeared for a query
    
    Provides transparency into the AI ranking logic
    """
    if not catalog_search_integration:
        raise HTTPException(status_code=503, detail="Search integration not initialized")
    
    try:
        # This would call the search service to explain ranking
        # For now, return a mock explanation
        explanation = {
            "query": query,
            "entity_id": entity_id,
            "ranking_factors": [
                {
                    "factor": "text_relevance",
                    "score": 0.85,
                    "explanation": "Strong keyword match in description"
                },
                {
                    "factor": "semantic_similarity",
                    "score": 0.92,
                    "explanation": "High semantic similarity to query intent"
                },
                {
                    "factor": "quality_boost",
                    "score": 0.1,
                    "explanation": "High quality score (0.95) boosted ranking"
                },
                {
                    "factor": "recency",
                    "score": 0.05,
                    "explanation": "Recently updated (2 days ago)"
                },
                {
                    "factor": "popularity",
                    "score": 0.08,
                    "explanation": "Frequently accessed by similar users"
                }
            ],
            "total_score": 0.89,
            "position": 2
        }
        
        return explanation
        
    except Exception as e:
        logger.error(f"Failed to explain results: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trending")
async def get_trending_searches(
    time_window: str = Query("week", pattern="^(day|week|month)$"),
    limit: int = Query(10, ge=1, le=50)
):
    """
    Get trending search queries in the catalog
    
    Shows what others are searching for
    """
    try:
        # This would query analytics backend
        # For now, return mock data
        trending = {
            "time_window": time_window,
            "trending_queries": [
                {"query": "customer data", "count": 245, "trend": "up"},
                {"query": "revenue metrics", "count": 189, "trend": "up"},
                {"query": "product catalog", "count": 156, "trend": "stable"},
                {"query": "user behavior", "count": 134, "trend": "down"},
                {"query": "sales dashboard", "count": 98, "trend": "up"}
            ][:limit],
            "trending_datasets": [
                {"name": "customer_360", "searches": 89, "trend": "up"},
                {"name": "revenue_dashboard", "searches": 67, "trend": "stable"},
                {"name": "product_metrics", "searches": 45, "trend": "up"}
            ],
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return trending
        
    except Exception as e:
        logger.error(f"Failed to get trending searches: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/personalize")
async def update_personalization_preferences(
    user_id: str = Body(...),
    preferences: Dict[str, Any] = Body(...)
):
    """
    Update search personalization preferences
    
    Allows users to customize their search experience
    """
    try:
        # This would update user preferences
        # For now, just acknowledge
        
        if event_stream:
            await event_stream.publish(
                topic="catalog-search",
                event_type="personalization_updated",
                data={
                    "user_id": user_id,
                    "preferences": preferences
                }
            )
        
        return {
            "status": "updated",
            "user_id": user_id
        }
        
    except Exception as e:
        logger.error(f"Failed to update preferences: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 