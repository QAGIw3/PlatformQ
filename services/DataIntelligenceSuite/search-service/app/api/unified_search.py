"""
Unified Search API endpoints

Provides comprehensive search across all platform services
with AI enhancements and analytics.
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Body, BackgroundTasks
from typing import List, Optional, Dict, Any
from datetime import datetime
import uuid
import logging

from app.dependencies import get_current_user
from app.services.unified_search_integration import UnifiedSearchIntegration
from app.services.ai_search_enhancement import AISearchOrchestrator
from app.services.search_analytics import SearchAnalyticsTracker, SearchAnalyticsAnalyzer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/unified", tags=["unified-search"])


@router.post("/search")
async def unified_search(
    query: str = Body(..., description="Search query"),
    filters: Optional[Dict[str, Any]] = Body(None, description="Search filters"),
    size: int = Body(10, description="Number of results"),
    from_: int = Body(0, description="Offset for pagination", alias="from"),
    include_ai: bool = Body(True, description="Include AI enhancements"),
    user: Dict = Depends(get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Search across all platform services with AI enhancements
    """
    try:
        # Get service instances
        unified_search = router.app.state.unified_search_integration
        ai_orchestrator = router.app.state.ai_search_orchestrator
        analytics_tracker = router.app.state.search_analytics_tracker
        
        session_id = str(uuid.uuid4())
        start_time = datetime.utcnow()
        
        # Process query with AI if enabled
        if include_ai and ai_orchestrator:
            query_data = await ai_orchestrator.process_search_query(
                query,
                user_id=user.get("id"),
                context={
                    "tenant_id": user.get("tenant_id"),
                    "roles": user.get("roles", [])
                }
            )
        else:
            query_data = {"original_query": query}
        
        # Perform search
        results = await unified_search.search_all_services(
            query=query_data.get("query_enhancement", {}).get("enhanced_query", query),
            filters=filters,
            size=size,
            from_=from_,
            tenant_id=user.get("tenant_id")
        )
        
        # Process results with AI if enabled
        if include_ai and ai_orchestrator:
            processed_results = await ai_orchestrator.process_search_results(
                results["results"],
                query_data,
                user_id=user.get("id")
            )
            results["results"] = processed_results["results"]
            results["insights"] = processed_results.get("insights", {})
        
        # Track analytics in background
        response_time_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        
        background_tasks.add_task(
            analytics_tracker.track_search,
            query=query,
            user_id=user.get("id"),
            session_id=session_id,
            result_count=results["total"],
            response_time_ms=response_time_ms,
            filters=filters,
            search_type="unified_ai" if include_ai else "unified",
            context={
                "tenant_id": user.get("tenant_id"),
                "device_type": "web",  # Could be extracted from headers
                "browser": "unknown"   # Could be extracted from user agent
            }
        )
        
        # Update user profile if AI enabled
        if include_ai and ai_orchestrator and user.get("id"):
            background_tasks.add_task(
                ai_orchestrator.personalization.update_user_profile,
                user_id=user["id"],
                action="search",
                data={
                    "query": query,
                    "result_count": results["total"],
                    "filters": filters
                }
            )
        
        return {
            "success": True,
            "query": query,
            "query_analysis": query_data if include_ai else None,
            "results": results["results"],
            "total": results["total"],
            "aggregations": results.get("aggregations", {}),
            "insights": results.get("insights", {}),
            "session_id": session_id,
            "response_time_ms": response_time_ms
        }
        
    except Exception as e:
        logger.error(f"Error in unified search: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search/services/{service_name}")
async def search_specific_service(
    service_name: str,
    entity_type: str = Body(..., description="Entity type to search"),
    query: str = Body(..., description="Search query"),
    filters: Optional[Dict[str, Any]] = Body(None, description="Search filters"),
    size: int = Body(10, description="Number of results"),
    from_: int = Body(0, description="Offset for pagination", alias="from"),
    user: Dict = Depends(get_current_user)
):
    """
    Search within a specific service
    """
    try:
        unified_search = router.app.state.unified_search_integration
        
        # Validate service exists
        if service_name not in unified_search.service_registry:
            raise HTTPException(status_code=404, detail=f"Service {service_name} not found")
        
        # Validate entity type
        service_info = unified_search.service_registry[service_name]
        if entity_type not in service_info["entity_types"]:
            raise HTTPException(
                status_code=400,
                detail=f"Entity type {entity_type} not supported by {service_name}"
            )
        
        # Build targeted query
        targeted_filters = filters or {}
        targeted_filters["service_name"] = service_name
        targeted_filters["entity_type"] = entity_type
        
        # Perform search
        results = await unified_search.search_all_services(
            query=query,
            filters=targeted_filters,
            size=size,
            from_=from_,
            tenant_id=user.get("tenant_id")
        )
        
        return {
            "success": True,
            "service": service_name,
            "entity_type": entity_type,
            "query": query,
            "results": results["results"],
            "total": results["total"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error searching service {service_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/index/service/{service_name}")
async def index_service_data(
    service_name: str,
    entity_type: str = Body(..., description="Entity type to index"),
    force_full_sync: bool = Body(False, description="Force full re-indexing"),
    user: Dict = Depends(get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Trigger indexing of data from a specific service
    """
    try:
        # Check admin permission
        if "admin" not in user.get("roles", []):
            raise HTTPException(status_code=403, detail="Admin permission required")
        
        unified_search = router.app.state.unified_search_integration
        
        # Start indexing in background
        background_tasks.add_task(
            unified_search.index_from_service,
            service_name=service_name,
            entity_type=entity_type,
            force_full_sync=force_full_sync
        )
        
        return {
            "success": True,
            "message": f"Indexing started for {service_name}/{entity_type}",
            "service": service_name,
            "entity_type": entity_type,
            "force_full_sync": force_full_sync
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting indexing: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/click")
async def track_click(
    session_id: str = Body(..., description="Search session ID"),
    query: str = Body(..., description="Original search query"),
    result_id: str = Body(..., description="ID of clicked result"),
    result_type: str = Body(..., description="Type of clicked result"),
    result_position: int = Body(..., description="Position in search results"),
    user: Dict = Depends(get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Track when a user clicks on a search result
    """
    try:
        analytics_tracker = router.app.state.search_analytics_tracker
        ai_orchestrator = router.app.state.ai_search_orchestrator
        
        click_time = datetime.utcnow()
        
        # Track click analytics
        background_tasks.add_task(
            analytics_tracker.track_click,
            user_id=user.get("id"),
            session_id=session_id,
            query=query,
            result_id=result_id,
            result_type=result_type,
            result_position=result_position,
            click_time_ms=0,  # Time from search to click not calculated here
            context={"tenant_id": user.get("tenant_id")}
        )
        
        # Update user profile
        if ai_orchestrator and user.get("id"):
            background_tasks.add_task(
                ai_orchestrator.personalization.update_user_profile,
                user_id=user["id"],
                action="click",
                data={
                    "item_id": result_id,
                    "item_type": result_type,
                    "position": result_position,
                    "query": query
                }
            )
        
        return {
            "success": True,
            "tracked": True
        }
        
    except Exception as e:
        logger.error(f"Error tracking click: {e}")
        # Don't fail the request, just log
        return {"success": False, "error": str(e)}


@router.get("/analytics/metrics")
async def get_search_metrics(
    time_range: str = Query("24h", description="Time range: 24h, 7d, 30d"),
    user: Dict = Depends(get_current_user)
):
    """
    Get search analytics metrics
    """
    try:
        # Check analytics permission
        if "analytics" not in user.get("roles", []) and "admin" not in user.get("roles", []):
            raise HTTPException(status_code=403, detail="Analytics permission required")
        
        analyzer = router.app.state.search_analytics_analyzer
        
        metrics = await analyzer.get_search_metrics(
            time_range=time_range,
            tenant_id=user.get("tenant_id") if "admin" not in user.get("roles", []) else None
        )
        
        return {
            "success": True,
            "metrics": metrics
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analytics/insights")
async def get_search_insights(
    time_range: str = Query("7d", description="Time range: 24h, 7d, 30d"),
    user: Dict = Depends(get_current_user)
):
    """
    Get AI-generated search insights
    """
    try:
        # Check analytics permission
        if "analytics" not in user.get("roles", []) and "admin" not in user.get("roles", []):
            raise HTTPException(status_code=403, detail="Analytics permission required")
        
        insights_generator = router.app.state.search_insights_generator
        
        insights = await insights_generator.generate_insights(time_range)
        recommendations = await insights_generator.generate_optimization_recommendations()
        
        return {
            "success": True,
            "insights": insights,
            "recommendations": recommendations
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analytics/query/{query}")
async def get_query_performance(
    query: str,
    user: Dict = Depends(get_current_user)
):
    """
    Get detailed performance metrics for a specific query
    """
    try:
        # Check analytics permission
        if "analytics" not in user.get("roles", []) and "admin" not in user.get("roles", []):
            raise HTTPException(status_code=403, detail="Analytics permission required")
        
        analyzer = router.app.state.search_analytics_analyzer
        
        performance = await analyzer.get_query_performance(query)
        
        return {
            "success": True,
            "query": query,
            "performance": performance
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting query performance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/suggestions")
async def get_search_suggestions(
    prefix: str = Query(..., description="Query prefix for suggestions"),
    size: int = Query(10, description="Number of suggestions"),
    user: Dict = Depends(get_current_user)
):
    """
    Get search suggestions based on prefix
    """
    try:
        es_client = router.app.state.es_client
        
        # Use completion suggester
        suggest_body = {
            "suggest": {
                "query_suggest": {
                    "prefix": prefix,
                    "completion": {
                        "field": "title.suggest",
                        "size": size,
                        "fuzzy": {
                            "fuzziness": "AUTO"
                        }
                    }
                }
            }
        }
        
        response = await es_client.search(
            index="unified_search_*",
            body=suggest_body
        )
        
        suggestions = []
        for option in response["suggest"]["query_suggest"][0]["options"]:
            suggestions.append({
                "text": option["text"],
                "score": option["_score"]
            })
        
        return {
            "success": True,
            "prefix": prefix,
            "suggestions": suggestions
        }
        
    except Exception as e:
        logger.error(f"Error getting suggestions: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 