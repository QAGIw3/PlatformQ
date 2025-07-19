"""
State Management Service

Centralized distributed state management service using Apache Ignite.
"""

import os
import logging
import asyncio
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional
import uuid

from fastapi import FastAPI, HTTPException, Depends, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from platformq_shared.metrics import MetricsCollector

from .core.ignite_manager import IgniteStateManager, CacheConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Pydantic models
class CreateCacheRequest(BaseModel):
    name: str
    mode: str = "PARTITIONED"
    backups: int = 1
    atomicity: str = "TRANSACTIONAL"
    eviction_policy: str = "LRU"
    eviction_max_size: int = 1000000
    enable_sql: bool = False
    indexes: Optional[List[Dict[str, str]]] = None


class PutRequest(BaseModel):
    value: Any
    ttl: Optional[int] = None


class BulkGetRequest(BaseModel):
    keys: List[str]


class BulkPutRequest(BaseModel):
    items: Dict[str, Any]
    ttl: Optional[int] = None


class QueryRequest(BaseModel):
    sql: str
    params: Optional[List[Any]] = None


class TransactionRequest(BaseModel):
    concurrency: str = "PESSIMISTIC"
    isolation: str = "REPEATABLE_READ"
    timeout: int = 5000


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Starting State Management Service")
    
    # Initialize Ignite manager
    ignite_nodes = os.getenv("IGNITE_NODES", "ignite:10800").split(",")
    nodes = []
    for node in ignite_nodes:
        host, port = node.split(":")
        nodes.append((host, int(port)))
    
    app.state.ignite = IgniteStateManager(nodes)
    await app.state.ignite.connect()
    
    # Initialize metrics
    app.state.metrics = MetricsCollector("state_management")
    
    # Track active transactions
    app.state.transactions = {}
    
    # Create default caches
    default_caches = [
        CacheConfig(
            name="system_state",
            cache_mode="REPLICATED",
            backups=2,
            atomicity_mode="TRANSACTIONAL"
        ),
        CacheConfig(
            name="session_state",
            cache_mode="PARTITIONED",
            backups=1,
            atomicity_mode="ATOMIC",
            eviction_policy="LRU",
            eviction_max_size=100000
        ),
        CacheConfig(
            name="feature_cache",
            cache_mode="PARTITIONED",
            backups=1,
            atomicity_mode="ATOMIC",
            eviction_policy="LRU",
            eviction_max_size=500000
        )
    ]
    
    for cache_config in default_caches:
        try:
            app.state.ignite.create_cache(cache_config)
            logger.info(f"Created default cache: {cache_config.name}")
        except Exception as e:
            logger.warning(f"Could not create default cache {cache_config.name}: {e}")
    
    yield
    
    # Cleanup
    logger.info("Shutting down State Management Service")
    await app.state.ignite.disconnect()


# Create FastAPI app
app = FastAPI(
    title="State Management Service",
    description="Centralized distributed state management using Apache Ignite",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    ignite_health = await app.state.ignite.health_check()
    
    return {
        "status": "healthy" if ignite_health["status"] == "healthy" else "degraded",
        "service": "state-management",
        "version": "1.0.0",
        "ignite": ignite_health
    }


# Cache management endpoints
@app.post("/api/v1/caches")
async def create_cache(request: CreateCacheRequest):
    """Create a new cache region"""
    try:
        # Create cache config
        indexes = []
        if request.indexes:
            indexes = [(idx["field"], idx.get("type", "SORTED")) for idx in request.indexes]
        
        cache_config = CacheConfig(
            name=request.name,
            cache_mode=request.mode,
            backups=request.backups,
            atomicity_mode=request.atomicity,
            eviction_policy=request.eviction_policy,
            eviction_max_size=request.eviction_max_size,
            sql_schema="PUBLIC" if request.enable_sql else None,
            indexes=indexes
        )
        
        # Create cache
        success = app.state.ignite.create_cache(cache_config)
        
        # Track metric
        app.state.metrics.increment("caches_created")
        
        return {
            "success": success,
            "cache_name": request.name
        }
        
    except Exception as e:
        logger.error(f"Failed to create cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/caches")
async def list_caches():
    """List all cache regions"""
    try:
        caches = app.state.ignite.list_caches()
        
        # Get metrics for each cache
        cache_info = []
        for cache_name in caches:
            metrics = await app.state.ignite.get_cache_metrics(cache_name)
            cache_info.append(metrics)
        
        return {
            "total": len(caches),
            "caches": cache_info
        }
        
    except Exception as e:
        logger.error(f"Failed to list caches: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/v1/caches/{cache_name}")
async def delete_cache(cache_name: str):
    """Delete a cache region"""
    try:
        success = await app.state.ignite.clear_cache(cache_name)
        
        # Track metric
        if success:
            app.state.metrics.increment("caches_deleted")
        
        return {"success": success}
        
    except Exception as e:
        logger.error(f"Failed to delete cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Key-value operations
@app.get("/api/v1/caches/{cache_name}/keys/{key}")
async def get_value(cache_name: str, key: str):
    """Get value from cache"""
    try:
        value = await app.state.ignite.get(cache_name, key)
        
        # Track metric
        app.state.metrics.increment("cache_gets", tags={"cache": cache_name})
        
        if value is None:
            raise HTTPException(status_code=404, detail="Key not found")
        
        return {"key": key, "value": value}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get value: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/v1/caches/{cache_name}/keys/{key}")
async def put_value(cache_name: str, key: str, request: PutRequest):
    """Put value into cache"""
    try:
        success = await app.state.ignite.put(cache_name, key, request.value, request.ttl)
        
        # Track metric
        app.state.metrics.increment("cache_puts", tags={"cache": cache_name})
        
        return {"success": success}
        
    except Exception as e:
        logger.error(f"Failed to put value: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/v1/caches/{cache_name}/keys/{key}")
async def delete_value(cache_name: str, key: str):
    """Delete value from cache"""
    try:
        success = await app.state.ignite.delete(cache_name, key)
        
        # Track metric
        app.state.metrics.increment("cache_deletes", tags={"cache": cache_name})
        
        return {"success": success}
        
    except Exception as e:
        logger.error(f"Failed to delete value: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Bulk operations
@app.post("/api/v1/caches/{cache_name}/bulk/get")
async def bulk_get(cache_name: str, request: BulkGetRequest):
    """Get multiple values from cache"""
    try:
        values = await app.state.ignite.get_all(cache_name, request.keys)
        
        # Track metric
        app.state.metrics.increment("cache_bulk_gets", 
                                   tags={"cache": cache_name, "count": str(len(request.keys))})
        
        return {"values": values}
        
    except Exception as e:
        logger.error(f"Failed to bulk get values: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/caches/{cache_name}/bulk/put")
async def bulk_put(cache_name: str, request: BulkPutRequest):
    """Put multiple values into cache"""
    try:
        count = await app.state.ignite.put_all(cache_name, request.items, request.ttl)
        
        # Track metric
        app.state.metrics.increment("cache_bulk_puts", 
                                   tags={"cache": cache_name, "count": str(count)})
        
        return {"count": count}
        
    except Exception as e:
        logger.error(f"Failed to bulk put values: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/caches/{cache_name}/bulk/delete")
async def bulk_delete(cache_name: str, keys: List[str] = Body(...)):
    """Delete multiple values from cache"""
    try:
        count = await app.state.ignite.delete_all(cache_name, keys)
        
        # Track metric
        app.state.metrics.increment("cache_bulk_deletes", 
                                   tags={"cache": cache_name, "count": str(count)})
        
        return {"count": count}
        
    except Exception as e:
        logger.error(f"Failed to bulk delete values: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Query operations
@app.post("/api/v1/caches/{cache_name}/query")
async def query_cache(cache_name: str, request: QueryRequest):
    """Execute SQL query on cache"""
    try:
        rows = await app.state.ignite.query(cache_name, request.sql, request.params)
        
        # Track metric
        app.state.metrics.increment("cache_queries", tags={"cache": cache_name})
        
        return {
            "rows": rows,
            "count": len(rows)
        }
        
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Transaction operations
@app.post("/api/v1/transactions")
async def begin_transaction(request: TransactionRequest):
    """Begin a new transaction"""
    try:
        tx = app.state.ignite.begin_transaction(
            concurrency=request.concurrency,
            isolation=request.isolation,
            timeout=request.timeout
        )
        
        # Generate transaction ID
        tx_id = str(uuid.uuid4())
        app.state.transactions[tx_id] = tx
        
        # Track metric
        app.state.metrics.increment("transactions_started")
        
        # Cleanup old transactions after timeout
        asyncio.create_task(_cleanup_transaction(tx_id, request.timeout / 1000))
        
        return {"transaction_id": tx_id}
        
    except Exception as e:
        logger.error(f"Failed to begin transaction: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/v1/transactions/{tx_id}")
async def update_transaction(tx_id: str, action: str = Body(...)):
    """Commit or rollback a transaction"""
    try:
        if tx_id not in app.state.transactions:
            raise HTTPException(status_code=404, detail="Transaction not found")
        
        tx = app.state.transactions[tx_id]
        
        if action == "commit":
            success = app.state.ignite.commit_transaction(tx)
            app.state.metrics.increment("transactions_committed")
        elif action == "rollback":
            success = app.state.ignite.rollback_transaction(tx)
            app.state.metrics.increment("transactions_rolled_back")
        else:
            raise HTTPException(status_code=400, detail="Invalid action")
        
        # Remove transaction
        del app.state.transactions[tx_id]
        
        return {"success": success}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update transaction: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def _cleanup_transaction(tx_id: str, timeout_seconds: float):
    """Cleanup transaction after timeout"""
    await asyncio.sleep(timeout_seconds)
    
    if tx_id in app.state.transactions:
        try:
            tx = app.state.transactions[tx_id]
            app.state.ignite.rollback_transaction(tx)
            del app.state.transactions[tx_id]
            logger.warning(f"Transaction {tx_id} timed out and was rolled back")
        except Exception as e:
            logger.error(f"Failed to cleanup transaction {tx_id}: {e}")


# Cache metrics endpoints
@app.get("/api/v1/caches/{cache_name}/metrics")
async def get_cache_metrics(cache_name: str):
    """Get metrics for a specific cache"""
    try:
        metrics = await app.state.ignite.get_cache_metrics(cache_name)
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to get cache metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/caches/{cache_name}/size")
async def get_cache_size(cache_name: str):
    """Get size of a cache"""
    try:
        size = await app.state.ignite.get_cache_size(cache_name)
        return {"cache_name": cache_name, "size": size}
        
    except Exception as e:
        logger.error(f"Failed to get cache size: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# System metrics
@app.get("/metrics")
async def get_metrics():
    """Get Prometheus metrics"""
    return app.state.metrics.generate_metrics()


# Cluster information
@app.get("/api/v1/cluster/info")
async def get_cluster_info():
    """Get cluster information"""
    try:
        health = await app.state.ignite.health_check()
        return health
        
    except Exception as e:
        logger.error(f"Failed to get cluster info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 