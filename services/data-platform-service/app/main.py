"""
Data Platform Service

Comprehensive data management platform consolidating:
- Federated query execution (Trino)
- Medallion architecture data lake (Bronze/Silver/Gold)
- Data governance and compliance
- Data quality monitoring
- ETL/ELT pipeline orchestration (SeaTunnel)
- Unified connection management
"""

import logging
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, List
from datetime import datetime
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

from platformq_shared import (
    create_base_app,
    ConfigLoader,
    get_pulsar_client,
    get_db
)
from platformq_shared.event_publisher import EventPublisher

# Core components
from .core.config import settings
from .core.connection_manager import UnifiedConnectionManager
from .core.query_router import QueryRouter
from .core.cache_manager import DataCacheManager

# Federation
from .federation.federated_query_engine import FederatedQueryEngine
from .federation.trino_client import TrinoQueryExecutor

# Data Lake
from .lake.medallion_architecture import MedallionLakeManager
from .lake.ingestion_engine import DataIngestionEngine
from .lake.transformation_engine import TransformationEngine

# Governance
from .governance.data_catalog import DataCatalogManager

# Quality
from .quality.profiler import DataQualityProfiler

# Lineage
from .lineage.lineage_tracker import DataLineageTracker

# Pipelines
from .pipelines.seatunnel_manager import SeaTunnelPipelineManager
from .pipelines.pipeline_coordinator import PipelineCoordinator

# Event handlers
from .event_handlers import DataPlatformEventHandler

# API routers
from .api import (
    query,
    catalog,
    governance,
    quality,
    lineage,
    lake,
    pipelines,
    admin
)

logger = logging.getLogger(__name__)

# Global instances
connection_manager: Optional[UnifiedConnectionManager] = None
query_router: Optional[QueryRouter] = None
cache_manager: Optional[DataCacheManager] = None
federated_engine: Optional[FederatedQueryEngine] = None
trino_client: Optional[TrinoQueryExecutor] = None
lake_manager: Optional[MedallionLakeManager] = None
ingestion_engine: Optional[DataIngestionEngine] = None
transformation_engine: Optional[TransformationEngine] = None
catalog_manager: Optional[DataCatalogManager] = None
quality_profiler: Optional[DataQualityProfiler] = None
lineage_tracker: Optional[DataLineageTracker] = None
seatunnel_manager: Optional[SeaTunnelPipelineManager] = None
pipeline_coordinator: Optional[PipelineCoordinator] = None
scheduler: AsyncIOScheduler = AsyncIOScheduler()
event_handler: Optional[DataPlatformEventHandler] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global connection_manager, query_router, cache_manager, federated_engine, trino_client
    global lake_manager, ingestion_engine, transformation_engine
    global catalog_manager, quality_profiler, lineage_tracker
    global seatunnel_manager, pipeline_coordinator, event_handler
    
    # Startup
    logger.info("Starting Data Platform Service...")
    
    # Initialize configuration
    config_loader = ConfigLoader()
    config = config_loader.load_settings()
    
    # Initialize connection manager (unified database connections)
    connection_manager = UnifiedConnectionManager(config)
    await connection_manager.initialize()
    app.state.connection_manager = connection_manager
    
    # Initialize cache manager (Ignite-based)
    cache_manager = DataCacheManager()
    await cache_manager.initialize()
    app.state.cache_manager = cache_manager
    
    # Initialize query router
    query_router = QueryRouter(
        connection_manager=connection_manager,
        cache_manager=cache_manager
    )
    app.state.query_router = query_router
    
    # Initialize Trino client
    trino_client = TrinoQueryExecutor(
        host=settings.trino_host,
        port=settings.trino_port,
        user=settings.trino_user,
        catalog=settings.trino_catalog,
        schema=settings.trino_schema
    )
    app.state.trino_client = trino_client
    
    # Initialize federated query engine (Trino)
    federated_engine = FederatedQueryEngine(
        trino_host=settings.trino_host,
        trino_port=settings.trino_port,
        cache_manager=cache_manager,
        query_router=query_router
    )
    await federated_engine.initialize()
    app.state.federated_engine = federated_engine
    
    # Initialize medallion lake manager
    lake_manager = MedallionLakeManager(
        spark_master=settings.spark_master,
        minio_endpoint=settings.minio_endpoint,
        minio_access_key=settings.minio_access_key,
        minio_secret_key=settings.minio_secret_key
    )
    await lake_manager.initialize()
    app.state.lake_manager = lake_manager
    
    # Initialize ingestion engine
    ingestion_engine = DataIngestionEngine(
        lake_manager=lake_manager,
        pulsar_url=settings.pulsar_url
    )
    await ingestion_engine.initialize()
    app.state.ingestion_engine = ingestion_engine
    
    # Initialize transformation engine
    transformation_engine = TransformationEngine(
        lake_manager=lake_manager
    )
    app.state.transformation_engine = transformation_engine
    
    # Initialize data catalog
    catalog_manager = DataCatalogManager(
        connection_manager=connection_manager,
        atlas_url=settings.atlas_url,
        enable_auto_discovery=settings.enable_auto_discovery
    )
    await catalog_manager.initialize()
    app.state.catalog_manager = catalog_manager
    
    # Initialize data quality profiler
    quality_profiler = DataQualityProfiler(
        spark_session=lake_manager.spark,
        cache_manager=cache_manager
    )
    await quality_profiler.initialize()
    app.state.quality_profiler = quality_profiler
    
    # Initialize lineage tracker
    lineage_tracker = DataLineageTracker(
        janusgraph_host=settings.janusgraph_host,
        janusgraph_port=settings.janusgraph_port,
        elasticsearch_client=connection_manager.get_elasticsearch_client(),
        cache_manager=cache_manager
    )
    await lineage_tracker.initialize()
    app.state.lineage_tracker = lineage_tracker
    
    # Initialize SeaTunnel manager
    seatunnel_manager = SeaTunnelPipelineManager(
        seatunnel_api_url=settings.seatunnel_api_url,
        connection_manager=connection_manager,
        lineage_tracker=lineage_tracker
    )
    app.state.seatunnel_manager = seatunnel_manager
    
    # Initialize pipeline coordinator
    pipeline_coordinator = PipelineCoordinator(
        ingestion_engine=ingestion_engine,
        transformation_engine=transformation_engine,
        quality_profiler=quality_profiler,
        seatunnel_manager=seatunnel_manager,
        lineage_tracker=lineage_tracker,
        cache_manager=cache_manager
    )
    app.state.pipeline_coordinator = pipeline_coordinator
    
    # Initialize event handler
    event_handler = DataPlatformEventHandler(
        service_name="data-platform-service",
        pulsar_url=settings.pulsar_url,
        federated_engine=federated_engine,
        lake_manager=lake_manager,
        ingestion_engine=ingestion_engine,
        transformation_engine=transformation_engine,
        pipeline_coordinator=pipeline_coordinator,
        catalog_manager=catalog_manager,
        quality_profiler=quality_profiler,
        lineage_tracker=lineage_tracker
    )
    await event_handler.initialize()
    app.state.event_handler = event_handler
    
    # Register event handlers
    event_handler.register_handler(
        topic_pattern="persistent://platformq/.*/data-ingestion-requests",
        handler=event_handler.handle_ingestion_request,
        subscription_name="data-platform-ingestion-sub"
    )
    
    event_handler.register_handler(
        topic_pattern="persistent://platformq/.*/pipeline-execution-requests",
        handler=event_handler.handle_pipeline_request,
        subscription_name="data-platform-pipeline-sub"
    )
    
    event_handler.register_handler(
        topic_pattern="persistent://platformq/.*/data-quality-check-requests",
        handler=event_handler.handle_quality_check,
        subscription_name="data-platform-quality-sub"
    )
    
    event_handler.register_handler(
        topic_pattern="persistent://platformq/.*/federated-query-requests",
        handler=event_handler.handle_federated_query,
        subscription_name="data-platform-query-sub"
    )
    
    # Schedule periodic tasks
    scheduler.add_job(
        catalog_manager.health_check,
        CronTrigger(minute="*/5"),  # Every 5 minutes
        id="catalog_health_check"
    )
    
    scheduler.add_job(
        quality_profiler.get_statistics,
        CronTrigger(hour="*/1"),  # Every hour
        id="quality_statistics"
    )
    
    scheduler.add_job(
        lake_manager.optimize_delta_tables,
        CronTrigger(hour=4, minute=0),  # Daily at 4 AM
        id="daily_delta_optimization"
    )
    
    scheduler.add_job(
        lineage_tracker.cleanup_old_lineage,
        CronTrigger(day_of_week="sun", hour=3, minute=0),  # Weekly on Sunday
        id="weekly_lineage_cleanup"
    )
    
    # Start background tasks
    await event_handler.start()
    scheduler.start()
    asyncio.create_task(connection_manager.health_check_loop())
    asyncio.create_task(cache_manager.cleanup_expired_loop())
    
    logger.info("Data Platform Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Data Platform Service...")
    
    # Stop scheduler
    scheduler.shutdown()
    
    # Stop components
    if event_handler:
        await event_handler.stop()
        
    if pipeline_coordinator:
        await pipeline_coordinator.shutdown()
        
    if seatunnel_manager:
        await seatunnel_manager.shutdown()
        
    if lineage_tracker:
        await lineage_tracker.shutdown()
        
    if quality_profiler:
        # No shutdown method needed
        pass
        
    if catalog_manager:
        await catalog_manager.shutdown()
        
    if transformation_engine:
        # No shutdown method needed
        pass
        
    if ingestion_engine:
        await ingestion_engine.shutdown()
        
    if lake_manager:
        await lake_manager.shutdown()
        
    if federated_engine:
        await federated_engine.shutdown()
        
    if trino_client:
        await trino_client.close()
        
    if cache_manager:
        await cache_manager.close()
        
    if connection_manager:
        await connection_manager.close_all()
    
    logger.info("Data Platform Service shutdown complete")


# Create app
app = create_base_app(
    service_name="data-platform-service",
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
app.include_router(query.router, prefix="/api/v1/query", tags=["query"])
app.include_router(catalog.router, prefix="/api/v1/catalog", tags=["catalog"])
app.include_router(governance.router, prefix="/api/v1/governance", tags=["governance"])
app.include_router(quality.router, prefix="/api/v1/quality", tags=["quality"])
app.include_router(lineage.router, prefix="/api/v1/lineage", tags=["lineage"])
app.include_router(lake.router, prefix="/api/v1/lake", tags=["lake"])
app.include_router(pipelines.router, prefix="/api/v1/pipelines", tags=["pipelines"])
app.include_router(admin.router, prefix="/api/v1/admin", tags=["admin"])

# Root endpoint
@app.get("/")
async def root():
    return {
        "service": "data-platform-service",
        "version": "2.0.0",
        "status": "operational",
        "description": "Unified data platform for PlatformQ",
        "capabilities": {
            "federation": {
                "engine": "apache-trino",
                "sources": ["cassandra", "elasticsearch", "hive", "ignite", "postgresql", "mongodb"],
                "caching": True,
                "query_optimization": True
            },
            "data_lake": {
                "architecture": "medallion",
                "layers": ["bronze", "silver", "gold"],
                "format": "delta-lake",
                "storage": "minio-s3"
            },
            "governance": {
                "catalog": True,
                "auto_discovery": True,
                "business_glossary": True,
                "classification": True
            },
            "quality": {
                "profiling": True,
                "anomaly_detection": True,
                "ml_powered": True,
                "quality_dimensions": ["completeness", "accuracy", "consistency", "validity", "uniqueness", "timeliness"]
            },
            "lineage": {
                "tracking": True,
                "impact_analysis": True,
                "visualization": True,
                "real_time": True,
                "column_level": True
            },
            "pipelines": {
                "engine": "apache-seatunnel",
                "connectors": ["jdbc", "kafka", "pulsar", "elasticsearch", "mongodb", "s3"],
                "modes": ["batch", "streaming", "cdc", "incremental"],
                "orchestration": True
            }
        }
    }


# Health check endpoint
@app.get("/health")
async def health_check():
    """Comprehensive health check for all data platform components"""
    health_status = {
        "status": "healthy",
        "components": {}
    }
    
    # Check connection manager
    if connection_manager:
        try:
            conn_health = await connection_manager.health_check()
            health_status["components"]["connections"] = conn_health
        except Exception as e:
            health_status["components"]["connections"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
    
    # Check federated engine
    if federated_engine:
        try:
            await federated_engine.health_check()
            health_status["components"]["federation"] = "healthy"
        except Exception as e:
            health_status["components"]["federation"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
    
    # Check lake manager
    if lake_manager:
        try:
            await lake_manager.health_check()
            health_status["components"]["data_lake"] = "healthy"
        except Exception as e:
            health_status["components"]["data_lake"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
    
    # Check cache
    if cache_manager:
        try:
            cache_status = await cache_manager.get_statistics()
            health_status["components"]["cache"] = {
                "status": "healthy",
                "statistics": cache_status
            }
        except Exception as e:
            health_status["components"]["cache"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
    
    # Check lineage tracker
    if lineage_tracker:
        try:
            lineage_stats = await lineage_tracker.get_statistics()
            health_status["components"]["lineage"] = {
                "status": "healthy",
                "statistics": lineage_stats
            }
        except Exception as e:
            health_status["components"]["lineage"] = f"unhealthy: {str(e)}"
            health_status["status"] = "degraded"
    
    return health_status


# Dashboard endpoint
@app.get("/api/v1/dashboard")
async def data_platform_dashboard():
    """Unified dashboard for data platform metrics"""
    dashboard_data = {
        "timestamp": datetime.utcnow().isoformat(),
        "federation": {},
        "data_lake": {},
        "governance": {},
        "quality": {},
        "lineage": {},
        "pipelines": {}
    }
    
    # Federation metrics
    if federated_engine:
        dashboard_data["federation"] = await federated_engine.get_metrics()
    
    # Data lake metrics
    if lake_manager:
        dashboard_data["data_lake"] = await lake_manager.get_lake_statistics()
    
    # Governance metrics
    if catalog_manager:
        dashboard_data["governance"] = {
            "catalog_statistics": await catalog_manager.get_catalog_statistics(),
            "total_assets": await catalog_manager.get_asset_count()
        }
    
    # Quality metrics
    if quality_profiler:
        dashboard_data["quality"] = quality_profiler.get_statistics()
    
    # Lineage metrics
    if lineage_tracker:
        dashboard_data["lineage"] = await lineage_tracker.get_statistics()
    
    # Pipeline metrics
    if pipeline_coordinator:
        dashboard_data["pipelines"] = pipeline_coordinator.get_statistics()
    
    # Cache metrics
    if cache_manager:
        dashboard_data["cache"] = await cache_manager.get_statistics()
    
    return dashboard_data


# Service status endpoint
@app.get("/api/v1/status")
async def service_status():
    """Get detailed service status"""
    return {
        "service": "data-platform-service",
        "version": "2.0.0",
        "uptime": datetime.utcnow().isoformat(),
        "configuration": {
            "trino_enabled": federated_engine is not None,
            "spark_enabled": lake_manager is not None,
            "cache_enabled": cache_manager is not None,
            "lineage_enabled": lineage_tracker is not None,
            "auto_discovery_enabled": catalog_manager is not None and settings.enable_auto_discovery
        },
        "active_connections": {
            "databases": len(connection_manager.connections) if connection_manager else 0
        },
        "scheduled_jobs": scheduler.get_jobs() if scheduler else []
    } 