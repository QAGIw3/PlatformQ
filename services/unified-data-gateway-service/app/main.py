"""
Unified Data Gateway Service

Centralized data access layer providing intelligent routing, caching, and connection pooling
for all data stores in the platform.
"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional
import asyncio

from fastapi import FastAPI, Depends, HTTPException, Query, Body
from fastapi.middleware.cors import CORSMiddleware

from platformq_shared import (
    create_base_app,
    ServiceClients,
    ConfigLoader,
    ErrorCode,
    AppException
)

from .core.config import settings
from .core.connection_manager import UnifiedConnectionManager
from .core.query_router import QueryRouter
from .core.cache_manager import IgniteCacheManager
from .api import query, admin, health
from .repositories import DataRepository
from .middleware.tenant_isolation import TenantIsolationMiddleware
from .monitoring import MetricsCollector

logger = logging.getLogger(__name__)

# Global instances
connection_manager: Optional[UnifiedConnectionManager] = None
query_router: Optional[QueryRouter] = None
cache_manager: Optional[IgniteCacheManager] = None
metrics_collector: Optional[MetricsCollector] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global connection_manager, query_router, cache_manager, metrics_collector
    
    # Startup
    logger.info("Starting Unified Data Gateway Service...")
    
    # Initialize configuration
    config_loader = ConfigLoader()
    config = config_loader.load_settings()
    
    # Initialize metrics collector
    metrics_collector = MetricsCollector()
    await metrics_collector.initialize()
    app.state.metrics = metrics_collector
    
    # Initialize connection manager with all database connections
    connection_manager = UnifiedConnectionManager(config)
    await connection_manager.initialize()
    app.state.connection_manager = connection_manager
    
    # Initialize Ignite cache manager
    cache_manager = IgniteCacheManager(
        ignite_host=config.get("ignite_host", "ignite"),
        ignite_port=int(config.get("ignite_port", 10800)),
        cache_ttl=int(config.get("cache_ttl", 300))
    )
    await cache_manager.connect()
    app.state.cache_manager = cache_manager
    
    # Initialize query router
    query_router = QueryRouter(
        connection_manager=connection_manager,
        cache_manager=cache_manager,
        metrics_collector=metrics_collector
    )
    app.state.query_router = query_router
    
    # Initialize repositories
    app.state.data_repository = DataRepository(
        query_router=query_router,
        cache_manager=cache_manager
    )
    
    # Start background tasks
    asyncio.create_task(connection_manager.health_check_loop())
    asyncio.create_task(cache_manager.cleanup_expired_entries())
    asyncio.create_task(metrics_collector.collect_metrics_loop())
    
    logger.info("Unified Data Gateway Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Unified Data Gateway Service...")
    
    # Stop background tasks
    if connection_manager:
        await connection_manager.close_all()
    
    if cache_manager:
        await cache_manager.disconnect()
        
    if metrics_collector:
        await metrics_collector.shutdown()
    
    logger.info("Unified Data Gateway Service shutdown complete")


# Create app
app = create_base_app(
    service_name="unified-data-gateway-service",
    lifespan=lifespan
)

# Add custom middleware
app.add_middleware(TenantIsolationMiddleware)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(query.router, prefix="/api/v1/query", tags=["query"])
app.include_router(admin.router, prefix="/api/v1/admin", tags=["admin"])
app.include_router(health.router, prefix="/api/v1/health", tags=["health"])

# Root endpoint
@app.get("/")
async def root():
    return {
        "service": "unified-data-gateway-service",
        "version": "1.0.0",
        "status": "operational",
        "features": [
            "multi-database-support",
            "connection-pooling",
            "query-routing",
            "intelligent-caching",
            "tenant-isolation",
            "read-write-splitting",
            "query-optimization"
        ]
    } 