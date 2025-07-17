from platformq_shared.base_service import create_base_app
from fastapi import Depends, HTTPException, Query as QueryParam
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
import logging
import httpx
import pandas as pd
from datetime import datetime, timedelta
import asyncio

logger = logging.getLogger(__name__)

# Unified Data Service client
UNIFIED_DATA_SERVICE_URL = "http://unified-data-service:8000"

# Models
class AnalyticsQuery(BaseModel):
    query_type: str = Field(..., description="Type of analytics: asset_summary, user_behavior, quality_trends, etc.")
    filters: Optional[Dict[str, Any]] = Field({}, description="Query filters")
    time_range: Optional[str] = Field("7d", description="Time range: 1d, 7d, 30d, 90d")
    group_by: Optional[List[str]] = Field([], description="Fields to group by")
    metrics: Optional[List[str]] = Field([], description="Metrics to calculate")

class AnalyticsResult(BaseModel):
    query_type: str
    data: List[Dict[str, Any]]
    summary: Dict[str, Any]
    metadata: Dict[str, Any]

# Create FastAPI app
app = create_base_app(
    service_name="analytics-service",
    db_session_dependency=lambda: None,
    api_key_crud_dependency=lambda: None,
    user_crud_dependency=lambda: None,
    password_verifier_dependency=lambda: None
)

# Analytics query builders
class UnifiedAnalyticsQueryBuilder:
    """Builds queries for the unified data service"""
    
    @staticmethod
    def build_asset_summary_query(filters: Dict[str, Any], time_range: str) -> str:
        """Build query for asset summary analytics"""
        time_filter = UnifiedAnalyticsQueryBuilder._get_time_filter(time_range)
        
        query = f"""
        WITH asset_metrics AS (
            SELECT 
                da.asset_id,
                da.asset_name,
                da.asset_type,
                da.owner_id,
                da.created_at,
                COUNT(DISTINCT l.target_asset_id) as downstream_count,
                COUNT(DISTINCT l2.source_asset_id) as upstream_count,
                AVG(q.metric_value) as avg_quality_score
            FROM cassandra.platformq.digital_assets da
            LEFT JOIN hive.silver.data_lineage l ON da.asset_id = l.source_asset_id
            LEFT JOIN hive.silver.data_lineage l2 ON da.asset_id = l2.target_asset_id
            LEFT JOIN hive.silver.data_quality_metrics q ON da.asset_id = q.asset_id
            WHERE da.created_at >= {time_filter}
        """
        
        if filters:
            conditions = []
            if "asset_type" in filters:
                conditions.append(f"da.asset_type = '{filters['asset_type']}'")
            if "owner_id" in filters:
                conditions.append(f"da.owner_id = '{filters['owner_id']}'")
            
            if conditions:
                query += " AND " + " AND ".join(conditions)
        
        query += """
            GROUP BY da.asset_id, da.asset_name, da.asset_type, da.owner_id, da.created_at
        )
        SELECT 
            asset_type,
            COUNT(*) as asset_count,
            AVG(downstream_count) as avg_downstream,
            AVG(upstream_count) as avg_upstream,
            AVG(avg_quality_score) as avg_quality,
            COUNT(CASE WHEN avg_quality_score >= 0.95 THEN 1 END) as high_quality_count,
            COUNT(CASE WHEN avg_quality_score < 0.8 THEN 1 END) as low_quality_count
        FROM asset_metrics
        GROUP BY asset_type
        ORDER BY asset_count DESC
        """
        
        return query
    
    @staticmethod
    def build_user_behavior_query(filters: Dict[str, Any], time_range: str) -> str:
        """Build query for user behavior analytics"""
        time_filter = UnifiedAnalyticsQueryBuilder._get_time_filter(time_range)
        
        query = f"""
        WITH user_activity AS (
            SELECT 
                user_id,
                DATE(event_timestamp) as activity_date,
                event_type,
                COUNT(*) as event_count
            FROM cassandra.auth_keyspace.activity_stream
            WHERE event_timestamp >= {time_filter}
            GROUP BY user_id, DATE(event_timestamp), event_type
        ),
        user_assets AS (
            SELECT 
                owner_id as user_id,
                COUNT(*) as asset_count,
                COUNT(DISTINCT asset_type) as asset_type_diversity
            FROM cassandra.platformq.digital_assets
            GROUP BY owner_id
        )
        SELECT 
            ua.user_id,
            ua.activity_date,
            SUM(ua.event_count) as total_events,
            COUNT(DISTINCT ua.event_type) as event_diversity,
            COALESCE(uas.asset_count, 0) as owned_assets,
            COALESCE(uas.asset_type_diversity, 0) as asset_diversity,
            SUM(ua.event_count) * 0.3 + 
            COUNT(DISTINCT ua.event_type) * 0.3 + 
            COALESCE(uas.asset_count, 0) * 0.2 + 
            COALESCE(uas.asset_type_diversity, 0) * 0.2 as engagement_score
        FROM user_activity ua
        LEFT JOIN user_assets uas ON ua.user_id = uas.user_id
        GROUP BY ua.user_id, ua.activity_date, uas.asset_count, uas.asset_type_diversity
        ORDER BY engagement_score DESC
        """
        
        return query
    
    @staticmethod
    def build_quality_trends_query(filters: Dict[str, Any], time_range: str) -> str:
        """Build query for data quality trends"""
        time_filter = UnifiedAnalyticsQueryBuilder._get_time_filter(time_range)
        
        query = f"""
        WITH quality_time_series AS (
            SELECT 
                DATE(measured_at) as measurement_date,
                metric_type,
                AVG(metric_value) as avg_score,
                COUNT(*) as check_count,
                COUNT(CASE WHEN passed THEN 1 END) as passed_count
            FROM hive.silver.data_quality_metrics
            WHERE measured_at >= {time_filter}
            GROUP BY DATE(measured_at), metric_type
        )
        SELECT 
            measurement_date,
            metric_type,
            avg_score,
            check_count,
            passed_count,
            CAST(passed_count AS DOUBLE) / check_count as pass_rate,
            avg_score - LAG(avg_score) OVER (PARTITION BY metric_type ORDER BY measurement_date) as score_change
        FROM quality_time_series
        ORDER BY measurement_date DESC, metric_type
        """
        
        return query
    
    @staticmethod
    def _get_time_filter(time_range: str) -> str:
        """Convert time range to SQL filter"""
        mappings = {
            "1d": "CURRENT_DATE - INTERVAL '1' DAY",
            "7d": "CURRENT_DATE - INTERVAL '7' DAY",
            "30d": "CURRENT_DATE - INTERVAL '30' DAY",
            "90d": "CURRENT_DATE - INTERVAL '90' DAY"
        }
        return mappings.get(time_range, "CURRENT_DATE - INTERVAL '7' DAY")

# Analytics endpoints
@app.post("/api/v1/analytics/query", response_model=AnalyticsResult)
async def execute_analytics_query(query: AnalyticsQuery):
    """Execute an analytics query using the unified data service"""
    
    # Build appropriate query based on type
    query_builder = UnifiedAnalyticsQueryBuilder()
    
    if query.query_type == "asset_summary":
        sql_query = query_builder.build_asset_summary_query(query.filters, query.time_range)
    elif query.query_type == "user_behavior":
        sql_query = query_builder.build_user_behavior_query(query.filters, query.time_range)
    elif query.query_type == "quality_trends":
        sql_query = query_builder.build_quality_trends_query(query.filters, query.time_range)
    else:
        raise HTTPException(status_code=400, detail=f"Unknown query type: {query.query_type}")
    
    # Execute via unified data service
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{UNIFIED_DATA_SERVICE_URL}/api/v1/query",
            json={
                "query": sql_query,
                "limit": 10000
            }
        )
        
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail="Query execution failed")
        
        result = response.json()
    
    # Process results
    data = []
    for row in result["data"]:
        data.append(dict(zip([col["name"] for col in result["columns"]], row)))
    
    # Calculate summary statistics
    df = pd.DataFrame(data)
    summary = {}
    
    if not df.empty:
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            summary[col] = {
                "mean": float(df[col].mean()),
                "min": float(df[col].min()),
                "max": float(df[col].max()),
                "std": float(df[col].std())
            }
    
    return AnalyticsResult(
        query_type=query.query_type,
        data=data,
        summary=summary,
        metadata={
            "row_count": len(data),
            "execution_time_ms": result["execution_time_ms"],
            "time_range": query.time_range
        }
    )

@app.get("/api/v1/analytics/dashboards/overview")
async def get_overview_dashboard():
    """Get overview dashboard data using federated queries"""
    
    async with httpx.AsyncClient() as client:
        # Execute multiple queries in parallel
        tasks = [
            client.get(f"{UNIFIED_DATA_SERVICE_URL}/api/v1/federated-views/unified_assets?limit=100"),
            client.get(f"{UNIFIED_DATA_SERVICE_URL}/api/v1/federated-views/unified_user_activity?limit=100"),
            client.get(f"{UNIFIED_DATA_SERVICE_URL}/api/v1/federated-views/data_quality_dashboard?limit=100")
        ]
        
        responses = await asyncio.gather(*tasks)
    
    # Process responses
    assets_data = responses[0].json() if responses[0].status_code == 200 else {"data": []}
    activity_data = responses[1].json() if responses[1].status_code == 200 else {"data": []}
    quality_data = responses[2].json() if responses[2].status_code == 200 else {"data": []}
    
    # Build dashboard
    return {
        "timestamp": datetime.utcnow(),
        "sections": {
            "assets": {
                "total_count": assets_data.get("row_count", 0),
                "by_source": _count_by_field(assets_data["data"], 5),  # Column index for source
                "recent_assets": assets_data["data"][:10]
            },
            "activity": {
                "total_events": activity_data.get("row_count", 0),
                "top_users": _get_top_users(activity_data["data"]),
                "event_distribution": _count_by_field(activity_data["data"], 2)  # event_type column
            },
            "quality": {
                "overall_health": _calculate_overall_quality(quality_data["data"]),
                "by_layer": _group_quality_by_layer(quality_data["data"]),
                "failing_datasets": _get_failing_datasets(quality_data["data"])
            }
        }
    }

@app.get("/api/v1/analytics/reports/lineage-impact")
async def generate_lineage_impact_report(
    asset_id: Optional[str] = QueryParam(None, description="Specific asset to analyze"),
    catalog: Optional[str] = QueryParam(None, description="Filter by catalog")
):
    """Generate lineage impact analysis report"""
    
    # Get lineage data from governance service
    async with httpx.AsyncClient() as client:
        if asset_id:
            response = await client.get(
                f"http://data-governance-service:8000/api/v1/lineage/impact/{asset_id}"
            )
            if response.status_code == 200:
                return response.json()
        else:
            # Get all assets and their lineage
            assets_response = await client.get(
                f"http://data-governance-service:8000/api/v1/assets",
                params={"catalog": catalog} if catalog else {}
            )
            
            if assets_response.status_code != 200:
                raise HTTPException(status_code=500, detail="Failed to fetch assets")
            
            assets = assets_response.json()["assets"]
            
            # Analyze critical paths
            critical_assets = []
            for asset in assets:
                if asset["downstream_count"] > 5:  # Critical threshold
                    impact_response = await client.get(
                        f"http://data-governance-service:8000/api/v1/lineage/impact/{asset['asset_id']}"
                    )
                    if impact_response.status_code == 200:
                        critical_assets.append(impact_response.json())
            
            return {
                "total_assets": len(assets),
                "critical_assets": len(critical_assets),
                "critical_paths": critical_assets,
                "recommendations": _generate_lineage_recommendations(critical_assets)
            }

# Helper functions
def _count_by_field(data: List[List[Any]], field_index: int) -> Dict[str, int]:
    """Count occurrences by field index"""
    counts = {}
    for row in data:
        if field_index < len(row):
            value = row[field_index]
            counts[str(value)] = counts.get(str(value), 0) + 1
    return counts

def _get_top_users(data: List[List[Any]], limit: int = 10) -> List[Dict[str, Any]]:
    """Extract top users by activity"""
    # Assuming columns: timestamp, user_id, event_type, ...
    user_events = {}
    for row in data:
        if len(row) > 1:
            user_id = row[1]
            user_events[user_id] = user_events.get(user_id, 0) + 1
    
    sorted_users = sorted(user_events.items(), key=lambda x: x[1], reverse=True)
    return [{"user_id": u[0], "event_count": u[1]} for u in sorted_users[:limit]]

def _calculate_overall_quality(data: List[List[Any]]) -> Dict[str, Any]:
    """Calculate overall quality metrics"""
    if not data:
        return {"status": "unknown", "score": 0}
    
    # Assuming columns include overall_score
    scores = [row[6] for row in data if len(row) > 6 and row[6] is not None]
    
    if not scores:
        return {"status": "unknown", "score": 0}
    
    avg_score = sum(scores) / len(scores)
    
    return {
        "status": "healthy" if avg_score >= 0.9 else "warning" if avg_score >= 0.8 else "critical",
        "score": avg_score,
        "dataset_count": len(scores)
    }

def _group_quality_by_layer(data: List[List[Any]]) -> Dict[str, Dict[str, Any]]:
    """Group quality metrics by data layer"""
    layer_metrics = {}
    
    for row in data:
        if len(row) > 1:
            layer = row[1]  # Assuming layer is second column
            score = row[6] if len(row) > 6 else None
            
            if layer not in layer_metrics:
                layer_metrics[layer] = {"scores": [], "count": 0}
            
            layer_metrics[layer]["count"] += 1
            if score is not None:
                layer_metrics[layer]["scores"].append(score)
    
    # Calculate averages
    for layer, metrics in layer_metrics.items():
        if metrics["scores"]:
            metrics["average_score"] = sum(metrics["scores"]) / len(metrics["scores"])
        else:
            metrics["average_score"] = 0
    
    return layer_metrics

def _get_failing_datasets(data: List[List[Any]], threshold: float = 0.8) -> List[Dict[str, Any]]:
    """Get datasets failing quality thresholds"""
    failing = []
    
    for row in data:
        if len(row) > 6:
            dataset_name = row[0]
            score = row[6]
            
            if score is not None and score < threshold:
                failing.append({
                    "dataset": dataset_name,
                    "score": score,
                    "layer": row[1] if len(row) > 1 else "unknown"
                })
    
    return sorted(failing, key=lambda x: x["score"])

def _generate_lineage_recommendations(critical_assets: List[Dict[str, Any]]) -> List[str]:
    """Generate recommendations based on lineage analysis"""
    recommendations = []
    
    for asset in critical_assets:
        if asset["total_downstream_impact"] > 10:
            recommendations.append(
                f"Asset '{asset['asset_id']}' has {asset['total_downstream_impact']} downstream dependencies. "
                f"Consider implementing caching or creating materialized views."
            )
        
        if asset["is_critical_path"]:
            recommendations.append(
                f"Asset '{asset['asset_id']}' is on a critical path. "
                f"Ensure high availability and implement quality checks."
            )
    
    return recommendations

# Health check
@app.get("/health")
async def health_check():
    # Check unified data service connectivity
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{UNIFIED_DATA_SERVICE_URL}/health")
            unified_healthy = response.status_code == 200
    except:
        unified_healthy = False
    
    return {
        "status": "healthy" if unified_healthy else "degraded",
        "dependencies": {
            "unified_data_service": "healthy" if unified_healthy else "unhealthy"
        }
    } 