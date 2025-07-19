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
import networkx as nx
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
from .governance.policy_manager import DataPolicyManager
from .governance.compliance_checker import ComplianceChecker

# Quality
from .quality.profiler import DataQualityProfiler
from .quality.validator import DataQualityValidator
from .quality.anomaly_detector import AnomalyDetector

# Lineage
from .lineage.lineage_tracker import DataLineageTracker
from .lineage.impact_analyzer import ImpactAnalyzer

# Pipelines
from .pipelines.seatunnel_manager import SeaTunnelManager
from .pipelines.pipeline_orchestrator import PipelineOrchestrator
from .pipelines.pipeline_generator import IntelligentPipelineGenerator

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
lake_manager: Optional[MedallionLakeManager] = None
catalog_manager: Optional[DataCatalogManager] = None
policy_manager: Optional[DataPolicyManager] = None
compliance_checker: Optional[ComplianceChecker] = None
quality_profiler: Optional[DataQualityProfiler] = None
lineage_tracker: Optional[DataLineageTracker] = None
seatunnel_manager: Optional[SeaTunnelManager] = None
pipeline_orchestrator: Optional[PipelineOrchestrator] = None
scheduler: AsyncIOScheduler = AsyncIOScheduler()
event_handler: Optional[DataPlatformEventHandler] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global connection_manager, query_router, cache_manager, federated_engine
    global lake_manager, catalog_manager, policy_manager, compliance_checker
    global quality_profiler, lineage_tracker, seatunnel_manager, pipeline_orchestrator
    global event_handler
    
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
    cache_manager = DataCacheManager(
        ignite_host=config.get("ignite_host", "ignite"),
        ignite_port=int(config.get("ignite_port", 10800)),
        cache_ttl=int(config.get("cache_ttl", 300))
    )
    await cache_manager.connect()
    app.state.cache_manager = cache_manager
    
    # Initialize query router
    query_router = QueryRouter(
        connection_manager=connection_manager,
        cache_manager=cache_manager
    )
    app.state.query_router = query_router
    
    # Initialize federated query engine (Trino)
    federated_engine = FederatedQueryEngine(
        trino_host=config.get("trino_host", "trino-coordinator"),
        trino_port=int(config.get("trino_port", 8080)),
        cache_manager=cache_manager,
        query_router=query_router
    )
    await federated_engine.initialize()
    app.state.federated_engine = federated_engine
    
    # Initialize medallion lake manager
    lake_manager = MedallionLakeManager(
        spark_master=config.get("spark_master", "spark://spark-master:7077"),
        minio_endpoint=config.get("minio_endpoint", "minio:9000"),
        minio_access_key=config.get("minio_access_key"),
        minio_secret_key=config.get("minio_secret_key")
    )
    await lake_manager.initialize()
    app.state.lake_manager = lake_manager
    
    # Initialize data catalog
    catalog_manager = DataCatalogManager(
        connection_manager=connection_manager,
        atlas_url=config.get("atlas_url"),  # Optional Apache Atlas
        enable_auto_discovery=config.get("enable_auto_discovery", True)
    )
    await catalog_manager.initialize()
    app.state.catalog_manager = catalog_manager
    
    # Initialize policy manager
    policy_manager = DataPolicyManager(
        ranger_url=config.get("ranger_url"),  # Optional Apache Ranger
        enable_dynamic_policies=config.get("enable_dynamic_policies", True)
    )
    await policy_manager.initialize()
    app.state.policy_manager = policy_manager
    
    # Initialize compliance checker
    compliance_checker = ComplianceChecker(
        policy_manager=policy_manager,
        supported_regulations=["GDPR", "CCPA", "HIPAA", "SOX", "PCI-DSS"]
    )
    await compliance_checker.initialize()
    app.state.compliance_checker = compliance_checker
    
    # Initialize data quality profiler
    quality_profiler = DataQualityProfiler(
        spark_session=lake_manager.spark_session,
        enable_ml_profiling=config.get("enable_ml_profiling", True)
    )
    await quality_profiler.initialize()
    app.state.quality_profiler = quality_profiler
    
    # Initialize lineage tracker
    lineage_tracker = DataLineageTracker(
        graph_backend="networkx",  # or JanusGraph for large scale
        enable_real_time_tracking=True
    )
    await lineage_tracker.initialize()
    app.state.lineage_tracker = lineage_tracker
    
    # Initialize SeaTunnel manager
    seatunnel_manager = SeaTunnelManager(
        seatunnel_home=config.get("seatunnel_home", "/opt/seatunnel"),
        kubernetes_namespace=config.get("k8s_namespace", "data-pipelines")
    )
    await seatunnel_manager.initialize()
    app.state.seatunnel_manager = seatunnel_manager
    
    # Initialize pipeline orchestrator
    pipeline_orchestrator = PipelineOrchestrator(
        seatunnel_manager=seatunnel_manager,
        lake_manager=lake_manager,
        quality_profiler=quality_profiler,
        lineage_tracker=lineage_tracker
    )
    await pipeline_orchestrator.initialize()
    app.state.pipeline_orchestrator = pipeline_orchestrator
    
    # Initialize event handler
    event_handler = DataPlatformEventHandler(
        service_name="data-platform-service",
        pulsar_url=config.get("pulsar_url", "pulsar://pulsar:6650"),
        federated_engine=federated_engine,
        lake_manager=lake_manager,
        pipeline_orchestrator=pipeline_orchestrator,
        catalog_manager=catalog_manager
    )
    await event_handler.initialize()
    
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
    
    # Schedule periodic tasks
    scheduler.add_job(
        quality_profiler.run_scheduled_profiling,
        CronTrigger(hour=2, minute=0),  # Daily at 2 AM
        id="daily_quality_profiling"
    )
    
    scheduler.add_job(
        compliance_checker.run_compliance_scan,
        CronTrigger(day_of_week="sun", hour=3, minute=0),  # Weekly on Sunday
        id="weekly_compliance_scan"
    )
    
    scheduler.add_job(
        lake_manager.optimize_delta_tables,
        CronTrigger(hour=4, minute=0),  # Daily at 4 AM
        id="daily_delta_optimization"
    )
    
    # Start background tasks
    await event_handler.start()
    scheduler.start()
    asyncio.create_task(connection_manager.health_check_loop())
    asyncio.create_task(cache_manager.cleanup_expired_entries())
    asyncio.create_task(lineage_tracker.process_lineage_events())
    
    logger.info("Data Platform Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Data Platform Service...")
    
    # Stop scheduler
    scheduler.shutdown()
    
    # Stop components
    if event_handler:
        await event_handler.stop()
        
    if pipeline_orchestrator:
        await pipeline_orchestrator.shutdown()
        
    if seatunnel_manager:
        await seatunnel_manager.shutdown()
        
    if lineage_tracker:
        await lineage_tracker.shutdown()
        
    if quality_profiler:
        await quality_profiler.shutdown()
        
    if compliance_checker:
        await compliance_checker.shutdown()
        
    if policy_manager:
        await policy_manager.shutdown()
        
    if catalog_manager:
        await catalog_manager.shutdown()
        
    if lake_manager:
        await lake_manager.shutdown()
        
    if federated_engine:
        await federated_engine.shutdown()
        
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
                "policies": True,
                "compliance": ["GDPR", "CCPA", "HIPAA", "SOX", "PCI-DSS"],
                "classification": True
            },
            "quality": {
                "profiling": True,
                "validation": True,
                "anomaly_detection": True,
                "ml_powered": True
            },
            "lineage": {
                "tracking": True,
                "impact_analysis": True,
                "visualization": True,
                "real_time": True
            },
            "pipelines": {
                "engine": "apache-seatunnel",
                "connectors": "100+",
                "modes": ["batch", "streaming", "cdc"],
                "auto_generation": True
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
            await cache_manager.health_check()
            health_status["components"]["cache"] = "healthy"
        except Exception as e:
            health_status["components"]["cache"] = f"unhealthy: {str(e)}"
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
        "pipelines": {}
    }
    
    # Federation metrics
    if federated_engine:
        dashboard_data["federation"] = await federated_engine.get_metrics()
    
    # Data lake metrics
    if lake_manager:
        dashboard_data["data_lake"] = await lake_manager.get_lake_statistics()
    
    # Governance metrics
    if catalog_manager and policy_manager:
        dashboard_data["governance"] = {
            "total_assets": await catalog_manager.get_asset_count(),
            "active_policies": await policy_manager.get_policy_count(),
            "compliance_score": await compliance_checker.get_overall_score()
        }
    
    # Quality metrics
    if quality_profiler:
        dashboard_data["quality"] = await quality_profiler.get_quality_summary()
    
    # Pipeline metrics
    if pipeline_orchestrator:
        dashboard_data["pipelines"] = await pipeline_orchestrator.get_pipeline_metrics()
    
    return dashboard_data 