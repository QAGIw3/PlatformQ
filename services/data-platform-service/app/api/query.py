"""
Federated query API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Body
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum
import uuid
import asyncio
import logging

from .. import main

router = APIRouter()
logger = logging.getLogger(__name__)


class QueryEngine(str, Enum):
    TRINO = "trino"
    SPARK_SQL = "spark_sql"
    NATIVE = "native"


class QueryStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class QueryRequest(BaseModel):
    """Query execution request"""
    query: str = Field(..., description="SQL query to execute")
    engine: QueryEngine = Field(QueryEngine.TRINO, description="Query engine to use")
    catalog: Optional[str] = Field(None, description="Default catalog")
    schema: Optional[str] = Field(None, description="Default schema")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Query parameters")
    timeout_seconds: int = Field(300, ge=1, le=3600)
    cache_results: bool = Field(True, description="Cache query results")


class QueryResponse(BaseModel):
    """Query response"""
    query_id: str
    status: QueryStatus
    engine: QueryEngine
    submitted_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    error: Optional[str]
    stats: Dict[str, Any] = Field(default_factory=dict)


class QueryResults(BaseModel):
    """Query results with pagination"""
    query_id: str
    columns: List[Dict[str, str]]
    rows: List[List[Any]]
    next_token: Optional[str]
    has_more: bool
    total_rows: Optional[int]
    execution_stats: Dict[str, Any] = Field(default_factory=dict)


@router.post("/execute", response_model=QueryResponse)
async def execute_query(
    query: QueryRequest,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Execute a federated query"""
    if not main.federated_engine:
        raise HTTPException(status_code=503, detail="Federated query engine not available")
    
    try:
        # Execute query through federated engine
        result = await main.federated_engine.execute_query(
            query=query.query,
            parameters=query.parameters,
            cache_results=query.cache_results,
            timeout=query.timeout_seconds
        )
        
        return QueryResponse(
            query_id=result.get("query_id"),
            status=QueryStatus.RUNNING if result.get("status") == "running" else QueryStatus.COMPLETED,
            engine=query.engine,
            submitted_at=result.get("submitted_at", datetime.utcnow()),
            started_at=result.get("started_at"),
            completed_at=result.get("completed_at"),
            error=result.get("error"),
            stats=result.get("stats", {})
        )
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{query_id}", response_model=QueryResponse)
async def get_query_status(
    query_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get query execution status"""
    if not main.federated_engine:
        raise HTTPException(status_code=503, detail="Federated query engine not available")
    
    try:
        status = await main.federated_engine.get_query_status(query_id)
        if not status:
            raise HTTPException(status_code=404, detail="Query not found")
        
        return QueryResponse(
            query_id=query_id,
            status=QueryStatus(status.get("status", "unknown").lower()),
            engine=QueryEngine.TRINO,
            submitted_at=status.get("submitted_at", datetime.utcnow()),
            started_at=status.get("started_at"),
            completed_at=status.get("completed_at"),
            error=status.get("error"),
            stats=status.get("stats", {})
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get query status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/results/{query_id}", response_model=QueryResults)
async def get_query_results(
    query_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    next_token: Optional[str] = Query(None)
):
    """Get query results"""
    if not main.federated_engine:
        raise HTTPException(status_code=503, detail="Federated query engine not available")
    
    try:
        results = await main.federated_engine.get_query_results(
            query_id=query_id,
            limit=limit,
            offset=offset
        )
        
        if not results:
            raise HTTPException(status_code=404, detail="Query results not found")
        
        return QueryResults(
            query_id=query_id,
            columns=results.get("columns", []),
            rows=results.get("rows", []),
            next_token=results.get("next_token"),
            has_more=results.get("has_more", False),
            total_rows=results.get("total_rows"),
            execution_stats=results.get("stats", {})
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get query results: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cancel/{query_id}")
async def cancel_query(
    query_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Cancel a running query"""
    if not main.federated_engine:
        raise HTTPException(status_code=503, detail="Federated query engine not available")
    
    try:
        result = await main.federated_engine.cancel_query(query_id)
        if not result:
            raise HTTPException(status_code=404, detail="Query not found")
        
        return {"message": f"Query {query_id} cancelled", "success": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history")
async def query_history(
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: Optional[str] = Query(None, description="Filter by user"),
    engine: Optional[QueryEngine] = Query(None),
    status: Optional[QueryStatus] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """Get query execution history"""
    return {
        "queries": [
            {
                "query_id": "query_123",
                "query_text": "SELECT * FROM users LIMIT 10",
                "engine": "trino",
                "status": "completed",
                "user_id": user_id or "user_456",
                "executed_at": datetime.utcnow(),
                "execution_time_ms": 1250,
                "rows_returned": 10
            }
        ],
        "total": 1
    }


@router.post("/explain", response_model=Dict[str, Any])
async def explain_query(
    query: QueryRequest,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get query execution plan"""
    if not main.federated_engine:
        raise HTTPException(status_code=503, detail="Federated query engine not available")
    
    try:
        plan = await main.federated_engine.explain_query(
            query=query.query,
            parameters=query.parameters
        )
        
        return {
            "query": query.query,
            "engine": query.engine,
            "plan": plan
        }
    except Exception as e:
        logger.error(f"Failed to explain query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/catalogs", response_model=List[str])
async def list_catalogs(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """List available catalogs"""
    if not main.federated_engine:
        raise HTTPException(status_code=503, detail="Federated query engine not available")
    
    try:
        catalogs = await main.federated_engine.list_catalogs()
        return catalogs
    except Exception as e:
        logger.error(f"Failed to list catalogs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schemas/{catalog}", response_model=List[str])
async def list_schemas(
    catalog: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """List schemas in a catalog"""
    if not main.federated_engine:
        raise HTTPException(status_code=503, detail="Federated query engine not available")
    
    try:
        schemas = await main.federated_engine.list_schemas(catalog)
        return schemas
    except Exception as e:
        logger.error(f"Failed to list schemas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tables/{catalog}/{schema}", response_model=List[Dict[str, Any]])
async def list_tables(
    catalog: str,
    schema: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """List tables in a schema"""
    if not main.federated_engine:
        raise HTTPException(status_code=503, detail="Federated query engine not available")
    
    try:
        tables = await main.federated_engine.list_tables(catalog, schema)
        return tables
    except Exception as e:
        logger.error(f"Failed to list tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/catalogs/{catalog}/schemas/{schema}/tables/{table}")
async def get_table_metadata(
    catalog: str,
    schema: str,
    table: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get table metadata"""
    return {
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "columns": [
            {
                "name": "id",
                "type": "bigint",
                "nullable": False,
                "partition_key": False
            },
            {
                "name": "name",
                "type": "varchar(255)",
                "nullable": True,
                "partition_key": False
            },
            {
                "name": "created_at",
                "type": "timestamp",
                "nullable": False,
                "partition_key": True
            }
        ],
        "partitions": ["created_at"],
        "properties": {
            "format": "parquet",
            "compression": "snappy",
            "location": "s3://data-lake/schema/table"
        },
        "statistics": {
            "row_count": 1000000,
            "size_bytes": 104857600,
            "last_updated": datetime.utcnow()
        }
    }


@router.post("/saved-queries")
async def save_query(
    name: str = Query(..., description="Query name"),
    description: Optional[str] = Query(None),
    query: str = Body(..., description="Query text"),
    tags: List[str] = Query(default=[]),
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Save a query for reuse"""
    query_id = str(uuid.uuid4())
    
    return {
        "saved_query_id": query_id,
        "name": name,
        "created_at": datetime.utcnow(),
        "created_by": user_id
    }


@router.get("/saved-queries")
async def list_saved_queries(
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: Optional[str] = Query(None),
    tags: Optional[List[str]] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List saved queries"""
    return {
        "saved_queries": [
            {
                "saved_query_id": "sq_123",
                "name": "Daily Active Users",
                "description": "Calculate DAU metrics",
                "query": "SELECT DATE(created_at) as day, COUNT(DISTINCT user_id) as dau FROM events GROUP BY 1",
                "tags": ["analytics", "metrics"],
                "created_by": "user_456",
                "created_at": datetime.utcnow()
            }
        ],
        "total": 1
    } 