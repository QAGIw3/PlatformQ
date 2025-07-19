"""
Data Platform Service

Provides comprehensive data platform capabilities:
- Federated query engine across multiple data sources
- Data lake management with medallion architecture
- Data governance and cataloging
- Data quality profiling and monitoring
- Data lineage tracking
- Pipeline orchestration
- Service integrations for ML, analytics, events, and storage
"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, List
import asyncio
import os

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

# Core infrastructure
from .core.connection_manager import DataConnectionManager
from .core.cache_manager import DataCacheManager
from .core.query_router import QueryRouter

# Federation engine
from .federation.trino_client import TrinoQueryClient
from .federation.federated_query_engine import FederatedQueryEngine

# Lake management
from .lake.medallion_architecture import MedallionLakeManager
from .lake.ingestion_engine import DataIngestionEngine
from .lake.transformation_engine import TransformationEngine

# Governance
from .governance.catalog_manager import DataCatalogManager, CatalogConfig

# Quality
from .quality.profiler import DataQualityProfiler

# Lineage
from .lineage.lineage_tracker import DataLineageTracker

# Pipelines
from .pipelines.seatunnel_manager import SeaTunnelPipelineManager
from .pipelines.pipeline_coordinator import PipelineCoordinator

# Service integrations
from .integrations import (
    MLPlatformIntegration,
    EventRouterIntegration,
    AnalyticsIntegration,
    StorageIntegration,
    ServiceOrchestrator
)

# API routers
from .api import (
    query_router,
    catalog_router,
    governance_router,
    quality_router,
    lineage_router,
    lake_router,
    pipelines_router,
    admin_router,
    integrations_router,
    features_router
)

# Event handlers
from .event_handlers import (
    DataIngestionEventProcessor,
    DataQualityEventProcessor,
    CatalogEventProcessor,
    PipelineEventProcessor
)

# Feature store
from .feature_store import FeatureStoreManager, FeatureRegistry

logger = logging.getLogger(__name__)

# Global instances
connection_manager: Optional[DataConnectionManager] = None
cache_manager: Optional[DataCacheManager] = None
query_router: Optional[QueryRouter] = None
trino_client: Optional[TrinoQueryClient] = None
federated_engine: Optional[FederatedQueryEngine] = None
lake_manager: Optional[MedallionLakeManager] = None
ingestion_engine: Optional[DataIngestionEngine] = None
transformation_engine: Optional[TransformationEngine] = None
catalog_manager: Optional[DataCatalogManager] = None
quality_profiler: Optional[DataQualityProfiler] = None
lineage_tracker: Optional[DataLineageTracker] = None
seatunnel_manager: Optional[SeaTunnelPipelineManager] = None
pipeline_coordinator: Optional[PipelineCoordinator] = None

# Feature store instances
feature_registry: Optional[FeatureRegistry] = None
feature_store_manager: Optional[FeatureStoreManager] = None

# Service integrations
ml_integration: Optional[MLPlatformIntegration] = None
event_integration: Optional[EventRouterIntegration] = None
analytics_integration: Optional[AnalyticsIntegration] = None
storage_integration: Optional[StorageIntegration] = None
service_orchestrator: Optional[ServiceOrchestrator] = None

# Event processors
event_processors: List[Any] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global connection_manager, cache_manager, query_router, trino_client, federated_engine
    global lake_manager, ingestion_engine, transformation_engine, catalog_manager
    global quality_profiler, lineage_tracker, seatunnel_manager, pipeline_coordinator
    global ml_integration, event_integration, analytics_integration, storage_integration
    global service_orchestrator, event_processors
    global feature_registry, feature_store_manager
    
    # Startup
    logger.info("Starting Data Platform Service...")
    
    try:
        # Initialize core infrastructure
        connection_manager = DataConnectionManager()
        await connection_manager.initialize()
        
        cache_manager = DataCacheManager()
        await cache_manager.initialize()
        
        query_router = QueryRouter(connection_manager, cache_manager)
        
        # Initialize Trino client
        trino_client = TrinoQueryClient(
            host=os.getenv("TRINO_HOST", "trino"),
            port=int(os.getenv("TRINO_PORT", 8080)),
            user=os.getenv("TRINO_USER", "data-platform"),
            catalog=os.getenv("TRINO_CATALOG", "iceberg"),
            schema=os.getenv("TRINO_SCHEMA", "default")
        )
        await trino_client.initialize()
        
        # Initialize federated query engine
        federated_engine = FederatedQueryEngine(
            connection_manager=connection_manager,
            cache_manager=cache_manager,
            query_router=query_router,
            trino_client=trino_client
        )
        
        # Initialize lake manager
        lake_manager = MedallionLakeManager(
            spark_config={
                "spark.app.name": "DataPlatformService",
                "spark.master": os.getenv("SPARK_MASTER", "local[*]"),
                "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
                "spark.sql.catalog.spark_catalog.type": "hive",
                "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.local.type": "hadoop",
                "spark.sql.catalog.local.warehouse": os.getenv("ICEBERG_WAREHOUSE", "s3a://data-lake/warehouse")
            },
            storage_config={
                "endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
                "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                "bucket": os.getenv("DATA_LAKE_BUCKET", "data-lake")
            }
        )
        await lake_manager.initialize()
        
        # Initialize ingestion and transformation engines
        ingestion_engine = DataIngestionEngine(lake_manager, connection_manager)
        transformation_engine = TransformationEngine(lake_manager.spark)
        
        # Initialize governance components
        catalog_config = CatalogConfig(
            elasticsearch_host=os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200"),
            auto_discovery_enabled=True,
            classification_enabled=True
        )
        catalog_manager = DataCatalogManager(catalog_config)
        await catalog_manager.initialize()
        
        # Initialize quality profiler
        quality_profiler = DataQualityProfiler(
            connection_manager=connection_manager,
            cache_manager=cache_manager,
            catalog_manager=catalog_manager
        )
        
        # Initialize lineage tracker
        lineage_tracker = DataLineageTracker(
            gremlin_url=os.getenv("JANUSGRAPH_URL", "ws://janusgraph:8182/gremlin"),
            elasticsearch_host=os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
        )
        await lineage_tracker.initialize()
        
        # Initialize pipeline management
        seatunnel_manager = SeaTunnelPipelineManager(
            seatunnel_home=os.getenv("SEATUNNEL_HOME", "/opt/seatunnel"),
            config_dir=os.getenv("SEATUNNEL_CONFIG_DIR", "/etc/seatunnel/pipelines")
        )
        
        pipeline_coordinator = PipelineCoordinator(
            seatunnel_manager=seatunnel_manager,
            lineage_tracker=lineage_tracker,
            catalog_manager=catalog_manager,
            pulsar_url=os.getenv("PULSAR_URL", "pulsar://pulsar:6650")
        )
        await pipeline_coordinator.initialize()
        
        # Initialize service integrations
        ml_integration = MLPlatformIntegration(
            lake_manager=lake_manager,
            transformation_engine=transformation_engine,
            cache_manager=cache_manager,
            federated_engine=federated_engine,
            ml_service_url=os.getenv("ML_PLATFORM_URL", "http://unified-ml-platform-service:8000")
        )
        
        event_integration = EventRouterIntegration(
            ingestion_engine=ingestion_engine,
            pipeline_coordinator=pipeline_coordinator,
            lineage_tracker=lineage_tracker,
            quality_profiler=quality_profiler,
            event_router_url=os.getenv("EVENT_ROUTER_URL", "http://event-router-service:8000"),
            pulsar_url=os.getenv("PULSAR_URL", "pulsar://pulsar:6650")
        )
        await event_integration.initialize()
        
        analytics_integration = AnalyticsIntegration(
            federated_engine=federated_engine,
            lake_manager=lake_manager,
            quality_profiler=quality_profiler,
            cache_manager=cache_manager,
            analytics_service_url=os.getenv("ANALYTICS_URL", "http://analytics-service:8000")
        )
        
        storage_integration = StorageIntegration(
            lake_manager=lake_manager,
            catalog_manager=catalog_manager,
            lineage_tracker=lineage_tracker,
            pipeline_coordinator=pipeline_coordinator,
            storage_service_url=os.getenv("STORAGE_URL", "http://storage-service:8000")
        )
        
        service_orchestrator = ServiceOrchestrator(
            ml_integration=ml_integration,
            event_integration=event_integration,
            analytics_integration=analytics_integration,
            storage_integration=storage_integration
        )
        
        # Initialize feature store
        feature_registry = FeatureRegistry(
            elasticsearch_host=os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
        )
        await feature_registry.initialize()
        
        feature_store_manager = FeatureStoreManager(
            lake_manager=lake_manager,
            lineage_tracker=lineage_tracker,
            quality_profiler=quality_profiler,
            cache_manager=cache_manager,
            registry=feature_registry
        )
        
        # Store instances in app state
        app.state.connection_manager = connection_manager
        app.state.cache_manager = cache_manager
        app.state.query_router = query_router
        app.state.trino_client = trino_client
        app.state.federated_engine = federated_engine
        app.state.lake_manager = lake_manager
        app.state.ingestion_engine = ingestion_engine
        app.state.transformation_engine = transformation_engine
        app.state.catalog_manager = catalog_manager
        app.state.quality_profiler = quality_profiler
        app.state.lineage_tracker = lineage_tracker
        app.state.seatunnel_manager = seatunnel_manager
        app.state.pipeline_coordinator = pipeline_coordinator
        app.state.ml_integration = ml_integration
        app.state.event_integration = event_integration
        app.state.analytics_integration = analytics_integration
        app.state.storage_integration = storage_integration
        app.state.service_orchestrator = service_orchestrator
        app.state.feature_registry = feature_registry
        app.state.feature_store_manager = feature_store_manager
        
        # Initialize event processors
        event_processors = [
            DataIngestionEventProcessor(ingestion_engine, pipeline_coordinator),
            DataQualityEventProcessor(quality_profiler, catalog_manager),
            CatalogEventProcessor(catalog_manager, lineage_tracker),
            PipelineEventProcessor(pipeline_coordinator, lineage_tracker)
        ]
        
        # Start event processors
        for processor in event_processors:
            await processor.start()
            
        # Start background tasks
        asyncio.create_task(catalog_manager.start_auto_discovery())
        asyncio.create_task(quality_profiler.start_continuous_profiling())
        asyncio.create_task(pipeline_coordinator.monitor_pipelines())
        
        logger.info("Data Platform Service started successfully")
        
    except Exception as e:
        logger.error(f"Failed to start Data Platform Service: {e}")
        raise
        
    yield
    
    # Shutdown
    logger.info("Shutting down Data Platform Service...")
    
    try:
        # Stop event processors
        for processor in event_processors:
            await processor.stop()
            
        # Close service integrations
        if ml_integration:
            await ml_integration.close()
        if event_integration:
            await event_integration.close()
        if analytics_integration:
            await analytics_integration.close()
        if storage_integration:
            await storage_integration.close()
            
        # Close other components
        if pipeline_coordinator:
            await pipeline_coordinator.close()
        if lineage_tracker:
            await lineage_tracker.close()
        if catalog_manager:
            await catalog_manager.close()
        if lake_manager:
            lake_manager.close()
        if trino_client:
            await trino_client.close()
        if cache_manager:
            await cache_manager.close()
        if connection_manager:
            await connection_manager.close()
            
        logger.info("Data Platform Service shutdown complete")
        
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")


# Create FastAPI app
app = FastAPI(
    title="Data Platform Service",
    description="Comprehensive data platform with federated queries, lake management, governance, quality, lineage, pipelines, and service integrations",
    version="2.0.0",
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

# Include routers
app.include_router(query_router, prefix="/api/v1")
app.include_router(catalog_router, prefix="/api/v1")
app.include_router(governance_router, prefix="/api/v1")
app.include_router(quality_router, prefix="/api/v1")
app.include_router(lineage_router, prefix="/api/v1")
app.include_router(lake_router, prefix="/api/v1")
app.include_router(pipelines_router, prefix="/api/v1")
app.include_router(admin_router, prefix="/api/v1")
app.include_router(integrations_router, prefix="/api/v1")
app.include_router(features_router, prefix="/api/v1")

# Mount metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    health_status = {
        "status": "healthy",
        "service": "data-platform-service",
        "version": "2.0.0",
        "components": {}
    }
    
    # Check component health
    try:
        if connection_manager:
            health_status["components"]["connections"] = "healthy"
        if federated_engine:
            health_status["components"]["federation"] = "healthy"
        if lake_manager:
            health_status["components"]["lake"] = "healthy"
        if catalog_manager:
            health_status["components"]["catalog"] = "healthy"
        if pipeline_coordinator:
            health_status["components"]["pipelines"] = "healthy"
        if service_orchestrator:
            health_status["components"]["integrations"] = "healthy"
    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["error"] = str(e)
        
    return health_status


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Data Platform Service",
        "version": "2.0.0",
        "description": "Comprehensive data platform with federated queries, lake management, governance, quality, lineage, pipelines, and service integrations",
        "endpoints": {
            "query": "/api/v1/query",
            "catalog": "/api/v1/catalog",
            "governance": "/api/v1/governance",
            "quality": "/api/v1/quality",
            "lineage": "/api/v1/lineage",
            "lake": "/api/v1/lake",
            "pipelines": "/api/v1/pipelines",
            "integrations": "/api/v1/integrations",
            "admin": "/api/v1/admin",
            "health": "/health",
            "metrics": "/metrics",
            "docs": "/docs"
        }
    }


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    ) 