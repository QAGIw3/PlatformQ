"""
Monitoring and Statistics API endpoints
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, Path
from pydantic import BaseModel, Field
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

from app.core import AtlasClient, SchemaRegistry, SearchEngine, LineageProcessor
from platformq_shared.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1", tags=["monitoring"])

# Global dependencies
atlas_client: Optional[AtlasClient] = None
schema_registry: Optional[SchemaRegistry] = None
search_engine: Optional[SearchEngine] = None
lineage_processor: Optional[LineageProcessor] = None


def set_dependencies(atlas: AtlasClient, schemas: SchemaRegistry, search: SearchEngine, lineage: LineageProcessor):
    """Set the global dependencies for this router"""
    global atlas_client, schema_registry, search_engine, lineage_processor
    atlas_client = atlas
    schema_registry = schemas
    search_engine = search
    lineage_processor = lineage


# Response Models
class CatalogStats(BaseModel):
    """Catalog statistics"""
    total_entities: int
    entities_by_type: Dict[str, int]
    total_classifications: int
    classified_entities: int
    total_relationships: int
    total_glossary_terms: int
    assigned_terms: int
    last_updated: datetime


class EntityTypeStats(BaseModel):
    """Statistics for a specific entity type"""
    type_name: str
    count: int
    classified_count: int
    tagged_count: int
    with_glossary_terms: int
    created_last_7_days: int
    updated_last_7_days: int
    avg_relationships: float


class UserActivity(BaseModel):
    """User activity information"""
    user: str
    total_actions: int
    entities_created: int
    entities_updated: int
    entities_deleted: int
    searches_performed: int
    last_activity: datetime


class AuditEntry(BaseModel):
    """Audit log entry"""
    timestamp: datetime
    user: str
    action: str
    entity_type: Optional[str]
    entity_guid: Optional[str]
    entity_name: Optional[str]
    details: Dict[str, Any]
    result: str


class SearchMetrics(BaseModel):
    """Search performance metrics"""
    total_searches: int
    avg_response_time_ms: float
    p95_response_time_ms: float
    p99_response_time_ms: float
    popular_queries: List[Dict[str, Any]]
    zero_result_queries: List[str]
    search_errors: int


class LineageMetrics(BaseModel):
    """Lineage processing metrics"""
    total_lineage_entities: int
    total_processes: int
    avg_lineage_depth: float
    max_lineage_depth: int
    orphaned_entities: int
    circular_dependencies: int
    processing_queue_size: int
    processing_lag_seconds: float


class SchemaMetrics(BaseModel):
    """Schema registry metrics"""
    total_schemas: int
    schemas_by_type: Dict[str, int]
    total_versions: int
    compatibility_failures: int
    cache_hit_rate: float
    avg_schema_size_bytes: float


class SystemHealth(BaseModel):
    """System health metrics"""
    atlas_connected: bool
    elasticsearch_connected: bool
    cache_connected: bool
    pulsar_connected: bool
    api_latency_ms: float
    memory_usage_percent: float
    cpu_usage_percent: float
    active_connections: int


# API Endpoints
@router.get("/stats", response_model=CatalogStats)
async def get_catalog_stats():
    """Get overall catalog statistics"""
    try:
        # Get entity counts
        entity_stats = await atlas_client.get_entity_statistics()
        
        # Get classification stats
        classification_stats = await atlas_client.get_classification_statistics()
        
        # Get glossary stats
        glossary_stats = await atlas_client.get_glossary_statistics()
        
        return CatalogStats(
            total_entities=entity_stats["total"],
            entities_by_type=entity_stats["byType"],
            total_classifications=classification_stats["total"],
            classified_entities=classification_stats["assigned"],
            total_relationships=entity_stats["relationships"],
            total_glossary_terms=glossary_stats["terms"],
            assigned_terms=glossary_stats["assigned"],
            last_updated=datetime.utcnow()
        )
        
    except Exception as e:
        logger.error(f"Failed to get catalog stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/entity-types", response_model=List[EntityTypeStats])
async def get_entity_type_stats():
    """Get detailed statistics for each entity type"""
    try:
        type_stats = await atlas_client.get_detailed_type_statistics()
        
        return [
            EntityTypeStats(
                type_name=stats["typeName"],
                count=stats["count"],
                classified_count=stats["classifiedCount"],
                tagged_count=stats["taggedCount"],
                with_glossary_terms=stats["withGlossaryTerms"],
                created_last_7_days=stats["createdLastWeek"],
                updated_last_7_days=stats["updatedLastWeek"],
                avg_relationships=stats["avgRelationships"]
            )
            for stats in type_stats
        ]
        
    except Exception as e:
        logger.error(f"Failed to get entity type stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/audit", response_model=List[AuditEntry])
async def get_audit_log(
    start_time: Optional[datetime] = Query(None, description="Start time for audit entries"),
    end_time: Optional[datetime] = Query(None, description="End time for audit entries"),
    user: Optional[str] = Query(None, description="Filter by user"),
    action: Optional[str] = Query(None, description="Filter by action type"),
    entity_type: Optional[str] = Query(None, description="Filter by entity type"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """
    Get audit log entries
    
    Track all changes to catalog entities including:
    - Creates, updates, deletes
    - Classification assignments
    - Tag changes
    - Glossary term assignments
    """
    try:
        # Default to last 24 hours if no time range specified
        if not start_time:
            start_time = datetime.utcnow() - timedelta(days=1)
        if not end_time:
            end_time = datetime.utcnow()
            
        # Get audit entries
        audit_entries = await atlas_client.get_audit_events(
            start_time=start_time,
            end_time=end_time,
            user=user,
            action=action,
            entity_type=entity_type,
            limit=limit,
            offset=offset
        )
        
        return [
            AuditEntry(
                timestamp=datetime.fromtimestamp(entry["timestamp"] / 1000),
                user=entry["user"],
                action=entry["action"],
                entity_type=entry.get("entityType"),
                entity_guid=entry.get("entityGuid"),
                entity_name=entry.get("entityName"),
                details=entry.get("details", {}),
                result=entry["result"]
            )
            for entry in audit_entries
        ]
        
    except Exception as e:
        logger.error(f"Failed to get audit log: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/activity/users", response_model=List[UserActivity])
async def get_user_activity(
    days: int = Query(7, ge=1, le=90, description="Number of days to look back")
):
    """Get user activity summary"""
    try:
        start_time = datetime.utcnow() - timedelta(days=days)
        
        # Get user activities
        activities = await atlas_client.get_user_activities(start_time)
        
        return [
            UserActivity(
                user=activity["user"],
                total_actions=activity["totalActions"],
                entities_created=activity["entitiesCreated"],
                entities_updated=activity["entitiesUpdated"],
                entities_deleted=activity["entitiesDeleted"],
                searches_performed=activity["searchesPerformed"],
                last_activity=datetime.fromtimestamp(activity["lastActivity"] / 1000)
            )
            for activity in activities
        ]
        
    except Exception as e:
        logger.error(f"Failed to get user activity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics/search", response_model=SearchMetrics)
async def get_search_metrics():
    """Get search performance metrics"""
    try:
        metrics = await search_engine.get_metrics()
        
        return SearchMetrics(
            total_searches=metrics["totalSearches"],
            avg_response_time_ms=metrics["avgResponseTime"],
            p95_response_time_ms=metrics["p95ResponseTime"],
            p99_response_time_ms=metrics["p99ResponseTime"],
            popular_queries=metrics["popularQueries"],
            zero_result_queries=metrics["zeroResultQueries"],
            search_errors=metrics["searchErrors"]
        )
        
    except Exception as e:
        logger.error(f"Failed to get search metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics/lineage", response_model=LineageMetrics)
async def get_lineage_metrics():
    """Get lineage processing metrics"""
    try:
        metrics = await lineage_processor.get_metrics()
        
        return LineageMetrics(
            total_lineage_entities=metrics["total_entities"],
            total_processes=metrics["total_processes"],
            avg_lineage_depth=metrics["avg_lineage_depth"],
            max_lineage_depth=metrics["max_lineage_depth"],
            orphaned_entities=metrics["orphaned_entities"],
            circular_dependencies=metrics["circular_dependencies"],
            processing_queue_size=metrics["processing_queue_size"],
            processing_lag_seconds=metrics["processing_lag_seconds"]
        )
        
    except Exception as e:
        logger.error(f"Failed to get lineage metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics/schemas", response_model=SchemaMetrics)
async def get_schema_metrics():
    """Get schema registry metrics"""
    try:
        metrics = await schema_registry.get_metrics()
        
        return SchemaMetrics(
            total_schemas=metrics["total_schemas"],
            schemas_by_type=metrics["schemas_by_type"],
            total_versions=metrics["total_versions"],
            compatibility_failures=metrics["compatibility_failures"],
            cache_hit_rate=metrics["cache_hit_rate"],
            avg_schema_size_bytes=metrics["avg_schema_size"]
        )
        
    except Exception as e:
        logger.error(f"Failed to get schema metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health/detailed", response_model=SystemHealth)
async def get_system_health():
    """Get detailed system health metrics"""
    try:
        # Check component health
        atlas_health = await atlas_client.check_health()
        search_health = await search_engine.check_health()
        
        # Get system metrics
        import psutil
        memory = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=1)
        
        return SystemHealth(
            atlas_connected=atlas_health,
            elasticsearch_connected=search_health,
            cache_connected=True,  # Simplified for now
            pulsar_connected=True,  # Simplified for now
            api_latency_ms=15.2,  # Would track this properly
            memory_usage_percent=memory.percent,
            cpu_usage_percent=cpu,
            active_connections=42  # Would track this properly
        )
        
    except Exception as e:
        logger.error(f"Failed to get system health: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data-quality/summary")
async def get_data_quality_summary(
    days: int = Query(30, ge=1, le=90, description="Number of days to look back")
):
    """Get data quality summary across catalog"""
    try:
        # This would integrate with the quality service
        # For now, return mock data
        return {
            "period_days": days,
            "total_quality_checks": 15420,
            "passed_checks": 14890,
            "failed_checks": 530,
            "overall_quality_score": 0.966,
            "quality_by_type": {
                "dataset": 0.972,
                "table": 0.965,
                "stream": 0.958
            },
            "top_quality_issues": [
                {"issue": "missing_values", "count": 234},
                {"issue": "invalid_format", "count": 156},
                {"issue": "duplicate_records", "count": 140}
            ]
        }
        
    except Exception as e:
        logger.error(f"Failed to get data quality summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/growth/trends")
async def get_growth_trends(
    metric: str = Query("entities", pattern="^(entities|schemas|lineage|searches)$"),
    period: str = Query("daily", pattern="^(hourly|daily|weekly|monthly)$"),
    lookback: int = Query(30, ge=1, le=365)
):
    """Get growth trends for various metrics"""
    try:
        # This would calculate actual trends
        # For now, return mock trend data
        return {
            "metric": metric,
            "period": period,
            "lookback_days": lookback,
            "data_points": [
                {"timestamp": "2024-01-01T00:00:00Z", "value": 1000},
                {"timestamp": "2024-01-02T00:00:00Z", "value": 1050},
                {"timestamp": "2024-01-03T00:00:00Z", "value": 1075}
                # ... more data points
            ],
            "growth_rate": 0.075,
            "projection_30_days": 1250
        }
        
    except Exception as e:
        logger.error(f"Failed to get growth trends: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Prometheus metrics endpoint
@router.get("/metrics")
async def get_prometheus_metrics():
    """Export Prometheus metrics"""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    ) 