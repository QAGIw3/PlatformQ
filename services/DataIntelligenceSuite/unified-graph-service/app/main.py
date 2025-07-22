"""
Unified Graph Service

Consolidates graph intelligence, processing, analytics, and temporal knowledge 
capabilities with JanusGraph, GraphX, and advanced ML algorithms.
"""

import os
import asyncio
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI
import uvicorn
import hvac
import consul.aio

from data_intelligence_common import (
    DataIntelligenceBaseService,
    ServiceMetadata,
    StructuredLogger,
    create_data_intelligence_app
)

from app.core.config import get_settings
from app.core.cache_manager import CacheManager
from app.graph.janusgraph_client import JanusGraphClient
from app.analytics.graphx_engine import GraphXEngine
from app.temporal.temporal_analysis import TemporalAnalysisEngine
from app.trust.trust_engine import TrustEngine
from app.lineage.lineage_tracker import LineageTracker

# Import API routers
from app.api import graph_operations, analytics, temporal, trust, lineage, health

# Service components
logger = StructuredLogger.get_logger(__name__)
settings = get_settings()

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="unified-graph-service",
    version="1.0.0",
    description="Graph intelligence and analytics platform with JanusGraph and GraphX",
    dependencies=["janusgraph", "cassandra", "elasticsearch", "spark", "ignite"],
    health_checks=["janusgraph", "graphx", "cache"],
    capabilities=["graph-analytics", "temporal-analysis", "trust-scoring", "lineage-tracking"],
    data_sources=["janusgraph", "cassandra", "elasticsearch"],
    data_outputs=["graph-events", "analytics-results"],
    min_memory_mb=4096,
    min_cpu_cores=2
)

# Global components
cache_manager: Optional[CacheManager] = None
graph_client: Optional[JanusGraphClient] = None
graphx_engine: Optional[GraphXEngine] = None
temporal_engine: Optional[TemporalAnalysisEngine] = None
trust_engine: Optional[TrustEngine] = None
lineage_tracker: Optional[LineageTracker] = None


class UnifiedGraphService(DataIntelligenceBaseService):
    """Unified Graph Service implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        self.cache_manager = None
        self.graph_client = None
        self.graphx_engine = None
        self.temporal_engine = None
        self.trust_engine = None
        self.lineage_tracker = None
    
    async def initialize_service(self):
        """Initialize service-specific components"""
        global cache_manager, graph_client, graphx_engine, temporal_engine, trust_engine, lineage_tracker
        
        logger.info("Initializing Unified Graph Service components...")
        
        # Get JanusGraph configuration from Vault
        janusgraph_config = await self.vault_consul.get_secret("graph/janusgraph")
        if janusgraph_config:
            settings.janusgraph_host = janusgraph_config.get("host", settings.janusgraph_host)
            settings.janusgraph_port = janusgraph_config.get("port", settings.janusgraph_port)
        
        # Get Cassandra credentials from Vault (JanusGraph backend)
        cassandra_creds = await self.vault_consul.get_database_credentials("cassandra")
        if cassandra_creds:
            settings.cassandra_username = cassandra_creds.get("username")
            settings.cassandra_password = cassandra_creds.get("password")
            os.environ["CASSANDRA_USERNAME"] = cassandra_creds.get("username", "")
            os.environ["CASSANDRA_PASSWORD"] = cassandra_creds.get("password", "")
        
        # Get Elasticsearch credentials from Vault (JanusGraph indexing)
        es_creds = await self.vault_consul.get_database_credentials("elasticsearch")
        if es_creds:
            settings.elasticsearch_username = es_creds.get("username")
            settings.elasticsearch_password = es_creds.get("password")
        
        # Get Spark configuration from Vault (GraphX)
        spark_config = await self.vault_consul.get_secret("compute/spark")
        if spark_config:
            settings.spark_master = spark_config.get("master", settings.spark_master)
        
        # Initialize cache manager
        cache_manager = CacheManager(settings)
        await cache_manager.connect()
        self.cache_manager = cache_manager
        
        # Initialize JanusGraph client
        graph_client = JanusGraphClient(settings)
        await graph_client.connect()
        await graph_client.create_schema()
        self.graph_client = graph_client
        
        # Initialize GraphX engine
        graphx_engine = GraphXEngine(settings)
        await graphx_engine.initialize()
        self.graphx_engine = graphx_engine
        
        # Initialize temporal analysis engine
        temporal_engine = TemporalAnalysisEngine(graph_client, cache_manager)
        await temporal_engine.initialize()
        self.temporal_engine = temporal_engine
        
        # Initialize trust engine
        trust_engine = TrustEngine(graph_client, cache_manager)
        await trust_engine.initialize()
        self.trust_engine = trust_engine
        
        # Initialize lineage tracker
        lineage_tracker = LineageTracker(graph_client, cache_manager)
        await lineage_tracker.initialize()
        self.lineage_tracker = lineage_tracker
        
        # Inject dependencies into API routers
        graph_operations.graph_client = graph_client
        analytics.graphx_engine = graphx_engine
        analytics.graph_client = graph_client
        temporal.temporal_engine = temporal_engine
        trust.trust_engine = trust_engine
        lineage.lineage_tracker = lineage_tracker
        health.graph_client = graph_client
        health.graphx_engine = graphx_engine
        health.cache_manager = cache_manager
        
        # Register health checks
        self.health_manager.register_check("janusgraph", graph_client.health_check, critical=True)
        self.health_manager.register_check("graphx", graphx_engine.health_check)
        self.health_manager.register_check("cache", cache_manager.health_check)
        
        logger.info("Unified Graph Service initialized successfully")
    
    async def cleanup_service(self):
        """Cleanup service-specific components"""
        logger.info("Cleaning up Unified Graph Service...")
        
        if self.graph_client:
            await self.graph_client.disconnect()
        
        if self.graphx_engine:
            await self.graphx_engine.cleanup()
        
        if self.cache_manager:
            await self.cache_manager.disconnect()
        
        logger.info("Unified Graph Service cleaned up")


# Create FastAPI app
def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    
    # Get configuration from environment
    vault_addr = os.getenv("VAULT_ADDR", "http://localhost:8200")
    vault_token = os.getenv("VAULT_TOKEN")
    consul_host = os.getenv("CONSUL_HOST", "localhost")
    consul_port = int(os.getenv("CONSUL_PORT", "8500"))
    consul_token = os.getenv("CONSUL_TOKEN")
    
    # Create Vault client
    vault_client = hvac.Client(url=vault_addr, token=vault_token)
    
    # Create Consul client
    consul_client = consul.aio.Consul(
        host=consul_host,
        port=consul_port,
        token=consul_token
    )
    
    # Create service instance
    service = UnifiedGraphService(
        vault_client=vault_client,
        consul_client=consul_client
    )
    
    # Create app with common setup
    app = create_data_intelligence_app(
        service_metadata=SERVICE_METADATA,
        service_instance=service,
        title="Unified Graph Service API",
        include_common_middleware=True
    )
    
    # Include API routers
    app.include_router(graph_operations.router, prefix="/api/v1/graph", tags=["graph"])
    app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["analytics"])
    app.include_router(temporal.router, prefix="/api/v1/temporal", tags=["temporal"])
    app.include_router(trust.router, prefix="/api/v1/trust", tags=["trust"])
    app.include_router(lineage.router, prefix="/api/v1/lineage", tags=["lineage"])
    app.include_router(health.router, prefix="/api/v1/health", tags=["health"])
    
    # Add root endpoint
    @app.get("/")
    async def root():
        return {
            "service": SERVICE_METADATA.name,
            "version": SERVICE_METADATA.version,
            "status": "running",
            "endpoints": {
                "graph": "/api/v1/graph",
                "analytics": "/api/v1/analytics",
                "temporal": "/api/v1/temporal",
                "trust": "/api/v1/trust",
                "lineage": "/api/v1/lineage",
                "health": "/api/v1/health"
            }
        }
    
    return app


# Create app instance
app = create_app()


if __name__ == "__main__":
    port = int(os.getenv("SERVICE_PORT", settings.service_port))
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=True
    ) 