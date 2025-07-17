from platformq_shared.base_service import create_base_app
from fastapi import Depends, HTTPException, Query as QueryParam
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List, Union
import logging
import trino
from trino.auth import BasicAuthentication
import pandas as pd
import asyncio
from datetime import datetime
import json
from concurrent.futures import ThreadPoolExecutor
import hashlib

logger = logging.getLogger(__name__)

# Trino configuration
TRINO_HOST = "trino"
TRINO_PORT = 8081
TRINO_USER = "platformq"
TRINO_CATALOG = "hive"

# Thread pool for async query execution
executor = ThreadPoolExecutor(max_workers=10)

# Models
class UnifiedQuery(BaseModel):
    query: str = Field(..., description="SQL query to execute across data sources")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Query parameters")
    limit: Optional[int] = Field(1000, description="Result limit")
    timeout: Optional[int] = Field(300, description="Query timeout in seconds")
    explain: Optional[bool] = Field(False, description="Return query execution plan")

class QueryResult(BaseModel):
    query_id: str
    status: str
    columns: List[Dict[str, str]]
    data: List[List[Any]]
    row_count: int
    execution_time_ms: int
    query_plan: Optional[str] = None

class DataSourceInfo(BaseModel):
    catalog: str
    schema: str
    tables: List[str]
    description: str
    query_examples: List[str]

class DataLineage(BaseModel):
    dataset_id: str
    upstream_datasets: List[str]
    downstream_datasets: List[str]
    transformations: List[str]
    last_updated: datetime

# Create FastAPI app
app = create_base_app(
    service_name="unified-data-service",
    db_session_dependency=lambda: None,
    api_key_crud_dependency=lambda: None,
    user_crud_dependency=lambda: None,
    password_verifier_dependency=lambda: None
)

# Predefined federated views
FEDERATED_VIEWS = {
    "unified_assets": """
        WITH cassandra_assets AS (
            SELECT 
                cid as asset_id,
                asset_name,
                asset_type,
                owner_id,
                created_at,
                'cassandra' as source
            FROM cassandra.platformq.digital_assets
        ),
        elasticsearch_assets AS (
            SELECT 
                asset_id,
                asset_name,
                asset_type,
                owner_id,
                created_timestamp as created_at,
                'elasticsearch' as source
            FROM elasticsearch.default.assets
        ),
        minio_assets AS (
            SELECT 
                asset_id,
                asset_name,
                asset_type,
                metadata.owner_id as owner_id,
                created_at,
                'minio' as source
            FROM hive.bronze.digital_assets
        )
        SELECT * FROM cassandra_assets
        UNION ALL
        SELECT * FROM elasticsearch_assets
        UNION ALL
        SELECT * FROM minio_assets
    """,
    
    "unified_user_activity": """
        SELECT 
            c.event_timestamp,
            c.user_id,
            c.event_type,
            c.event_source,
            e.event_details,
            e.sentiment_score,
            h.session_duration,
            h.page_views
        FROM cassandra.auth_keyspace.activity_stream c
        LEFT JOIN elasticsearch.default.event_enrichment e
            ON c.event_id = e.event_id
        LEFT JOIN hive.gold.user_behavior_summary h
            ON c.user_id = h.user_id 
            AND DATE(c.event_timestamp) = h.activity_date
    """,
    
    "data_quality_dashboard": """
        WITH latest_quality AS (
            SELECT 
                dataset_name,
                layer,
                MAX(check_timestamp) as latest_check
            FROM hive.silver.data_quality_metrics
            GROUP BY dataset_name, layer
        )
        SELECT 
            q.dataset_name,
            q.layer,
            q.completeness_score,
            q.accuracy_score,
            q.consistency_score,
            q.validity_score,
            q.overall_score,
            q.check_timestamp,
            l.row_count,
            l.file_size_mb
        FROM hive.silver.data_quality_metrics q
        JOIN latest_quality lq 
            ON q.dataset_name = lq.dataset_name 
            AND q.layer = lq.layer
            AND q.check_timestamp = lq.latest_check
        LEFT JOIN hive.metadata.dataset_stats l
            ON q.dataset_name = l.dataset_name
    """
}

def get_trino_connection():
    """Create Trino connection"""
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema="default",
        http_scheme="http",
        auth=None  # Add authentication if needed
    )

async def execute_query_async(query: str, parameters: Dict[str, Any] = None, 
                            limit: int = 1000, explain: bool = False) -> QueryResult:
    """Execute query asynchronously"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        executor, 
        execute_query_sync, 
        query, 
        parameters, 
        limit,
        explain
    )

def execute_query_sync(query: str, parameters: Dict[str, Any] = None, 
                      limit: int = 1000, explain: bool = False) -> QueryResult:
    """Execute query synchronously"""
    start_time = datetime.now()
    query_id = hashlib.md5(f"{query}{datetime.now()}".encode()).hexdigest()[:16]
    
    conn = get_trino_connection()
    cursor = conn.cursor()
    
    try:
        # Add limit if not present
        if limit and "LIMIT" not in query.upper():
            query = f"{query} LIMIT {limit}"
        
        # Handle explain
        if explain:
            query = f"EXPLAIN {query}"
        
        # Execute query
        cursor.execute(query, parameters)
        
        # Fetch results
        columns = [{"name": desc[0], "type": str(desc[1])} for desc in cursor.description]
        data = cursor.fetchall()
        
        execution_time = int((datetime.now() - start_time).total_seconds() * 1000)
        
        return QueryResult(
            query_id=query_id,
            status="completed",
            columns=columns,
            data=data,
            row_count=len(data),
            execution_time_ms=execution_time,
            query_plan="\n".join([str(row[0]) for row in data]) if explain else None
        )
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# API Endpoints
@app.post("/api/v1/query", response_model=QueryResult)
async def execute_unified_query(query_request: UnifiedQuery):
    """Execute a federated query across all data sources"""
    return await execute_query_async(
        query_request.query,
        query_request.parameters,
        query_request.limit,
        query_request.explain
    )

@app.get("/api/v1/federated-views")
async def list_federated_views():
    """List available pre-defined federated views"""
    return {
        "views": [
            {
                "name": name,
                "description": view.strip().split('\n')[0],
                "query": view
            }
            for name, view in FEDERATED_VIEWS.items()
        ]
    }

@app.get("/api/v1/federated-views/{view_name}", response_model=QueryResult)
async def query_federated_view(
    view_name: str,
    limit: int = QueryParam(100, description="Result limit"),
    filters: Optional[str] = QueryParam(None, description="Additional WHERE clause")
):
    """Query a pre-defined federated view"""
    if view_name not in FEDERATED_VIEWS:
        raise HTTPException(status_code=404, detail=f"View '{view_name}' not found")
    
    query = FEDERATED_VIEWS[view_name]
    
    # Add filters if provided
    if filters:
        # Wrap the view in a subquery to apply filters
        query = f"SELECT * FROM ({query}) AS view WHERE {filters}"
    
    return await execute_query_async(query, limit=limit)

@app.get("/api/v1/data-sources")
async def list_data_sources():
    """List all available data sources and their schemas"""
    sources = [
        DataSourceInfo(
            catalog="cassandra",
            schema="platformq",
            tables=["digital_assets", "cad_sessions", "users", "projects"],
            description="Transactional data store for real-time operations",
            query_examples=[
                "SELECT * FROM cassandra.platformq.digital_assets WHERE owner_id = ?",
                "SELECT * FROM cassandra.platformq.cad_sessions WHERE tenant_id = ?"
            ]
        ),
        DataSourceInfo(
            catalog="elasticsearch",
            schema="default",
            tables=["assets", "logs", "events", "documents"],
            description="Full-text search and log analytics",
            query_examples=[
                "SELECT * FROM elasticsearch.default.assets WHERE asset_name LIKE '%model%'",
                "SELECT * FROM elasticsearch.default.logs WHERE level = 'ERROR' AND timestamp > CURRENT_DATE - INTERVAL '1' DAY"
            ]
        ),
        DataSourceInfo(
            catalog="hive",
            schema="bronze",
            tables=["raw_events", "digital_assets", "sensor_data"],
            description="Data lake bronze layer - raw data ingestion",
            query_examples=[
                "SELECT * FROM hive.bronze.raw_events WHERE date = CURRENT_DATE",
                "SELECT * FROM hive.bronze.sensor_data WHERE device_id = ? AND date BETWEEN ? AND ?"
            ]
        ),
        DataSourceInfo(
            catalog="hive",
            schema="silver",
            tables=["cleansed_events", "normalized_assets", "data_quality_metrics"],
            description="Data lake silver layer - cleansed and standardized data",
            query_examples=[
                "SELECT * FROM hive.silver.normalized_assets WHERE quality_score > 0.8",
                "SELECT * FROM hive.silver.data_quality_metrics WHERE layer = 'bronze'"
            ]
        ),
        DataSourceInfo(
            catalog="hive",
            schema="gold",
            tables=["asset_summary", "user_behavior", "ml_features"],
            description="Data lake gold layer - business-ready aggregated data",
            query_examples=[
                "SELECT * FROM hive.gold.asset_summary WHERE month = DATE_FORMAT(CURRENT_DATE, '%Y-%m')",
                "SELECT * FROM hive.gold.ml_features WHERE feature_set = 'user_engagement'"
            ]
        ),
        DataSourceInfo(
            catalog="ignite",
            schema="cache",
            tables=["active_sessions", "real_time_metrics", "hot_data"],
            description="In-memory cache for ultra-low latency access",
            query_examples=[
                "SELECT * FROM ignite.cache.active_sessions WHERE last_activity > CURRENT_TIMESTAMP - INTERVAL '5' MINUTE",
                "SELECT * FROM ignite.cache.real_time_metrics WHERE metric_name = ?"
            ]
        )
    ]
    
    return {"data_sources": sources}

@app.get("/api/v1/schema/{catalog}/{schema}/{table}")
async def get_table_schema(catalog: str, schema: str, table: str):
    """Get schema information for a specific table"""
    query = f"DESCRIBE {catalog}.{schema}.{table}"
    
    try:
        result = await execute_query_async(query)
        return {
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "columns": [
                {
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] if len(row) > 2 else True,
                    "description": row[3] if len(row) > 3 else None
                }
                for row in result.data
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Table not found: {catalog}.{schema}.{table}")

@app.post("/api/v1/lineage/analyze")
async def analyze_query_lineage(query_request: UnifiedQuery):
    """Analyze data lineage for a query"""
    # Use EXPLAIN to get query plan
    explain_result = await execute_query_async(
        query_request.query,
        query_request.parameters,
        explain=True
    )
    
    # Parse the query plan to extract source tables
    # This is a simplified implementation
    source_tables = []
    plan_text = explain_result.query_plan or ""
    
    for line in plan_text.split('\n'):
        if "TableScan" in line or "ScanProject" in line:
            # Extract table references
            import re
            matches = re.findall(r'(\w+)\.(\w+)\.(\w+)', line)
            for match in matches:
                source_tables.append(f"{match[0]}.{match[1]}.{match[2]}")
    
    return {
        "query_id": explain_result.query_id,
        "source_tables": list(set(source_tables)),
        "execution_plan": plan_text,
        "estimated_cost": "N/A"  # Would need to parse from plan
    }

@app.get("/api/v1/statistics/{catalog}/{schema}/{table}")
async def get_table_statistics(catalog: str, schema: str, table: str):
    """Get statistics for a table"""
    queries = {
        "row_count": f"SELECT COUNT(*) as count FROM {catalog}.{schema}.{table}",
        "sample": f"SELECT * FROM {catalog}.{schema}.{table} LIMIT 5"
    }
    
    stats = {}
    
    # Get row count
    try:
        count_result = await execute_query_async(queries["row_count"])
        stats["row_count"] = count_result.data[0][0] if count_result.data else 0
    except:
        stats["row_count"] = "N/A"
    
    # Get sample data
    try:
        sample_result = await execute_query_async(queries["sample"])
        stats["sample_data"] = {
            "columns": [col["name"] for col in sample_result.columns],
            "rows": sample_result.data[:5]
        }
    except:
        stats["sample_data"] = None
    
    return {
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "statistics": stats
    }

# Health check
@app.get("/health")
async def health_check():
    try:
        # Test Trino connectivity
        await execute_query_async("SELECT 1")
        return {
            "status": "healthy",
            "trino": "connected"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        } 