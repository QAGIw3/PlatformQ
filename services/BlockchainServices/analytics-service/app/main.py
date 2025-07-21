from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import sys
from typing import Optional
import signal
import asyncio

from pyignite import AsyncClient as IgniteClient
import redis.asyncio as redis
import motor.motor_asyncio
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
import consul.aio

from .config import config
from .api.analytics_endpoints import router as analytics_router
from .core.analytics_engine import AnalyticsEngine


# Configure logging
logging.basicConfig(
    level=getattr(logging, config.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Prometheus metrics
analytics_queries = Counter(
    'analytics_queries_total',
    'Total number of analytics queries',
    ['metric_type', 'chain']
)

query_duration = Histogram(
    'analytics_query_duration_seconds',
    'Time spent executing analytics queries',
    ['metric_type']
)

active_alerts = Gauge(
    'analytics_active_alerts',
    'Number of active analytics alerts',
    ['chain']
)

report_generation = Counter(
    'analytics_reports_generated_total',
    'Total number of reports generated',
    ['report_type', 'format']
)

# Global instances
analytics_engine: Optional[AnalyticsEngine] = None
ignite_client: Optional[IgniteClient] = None
redis_client: Optional[redis.Redis] = None
mongodb_client: Optional[motor.motor_asyncio.AsyncIOMotorClient] = None
consul_client: Optional[consul.aio.Consul] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    global analytics_engine, ignite_client, redis_client, mongodb_client, consul_client
    
    logger.info("Starting Analytics Service")
    
    try:
        # Initialize Consul client
        consul_client = consul.aio.Consul(
            host=config.consul_host,
            port=config.consul_port
        )
        
        # Register service with Consul
        await consul_client.agent.service.register(
            name=config.service_name,
            service_id=f"{config.service_name}-{config.port}",
            address=config.host,
            port=config.port,
            tags=["blockchain", "analytics", "reporting"],
            check={
                "http": f"http://{config.host}:{config.port}/api/v1/analytics/health",
                "interval": f"{config.service_health_interval}s"
            }
        )
        logger.info("Registered with Consul")
        
        # Initialize Ignite client
        ignite_client = IgniteClient()
        await ignite_client.connect([(config.ignite_host, config.ignite_port)])
        logger.info("Connected to Ignite")
        
        # Initialize Redis client
        redis_client = await redis.from_url(
            config.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
        logger.info("Connected to Redis")
        
        # Initialize MongoDB client
        mongodb_client = motor.motor_asyncio.AsyncIOMotorClient(config.mongodb_url)
        logger.info("Connected to MongoDB")
        
        # Initialize analytics engine
        analytics_engine = AnalyticsEngine(
            ignite_client=ignite_client,
            redis_client=redis_client,
            mongodb_client=mongodb_client
        )
        
        await analytics_engine.initialize()
        logger.info("Analytics engine initialized")
        
        # Setup signal handlers
        def signal_handler(sig, frame):
            logger.info(f"Received signal {sig}, initiating shutdown")
            asyncio.create_task(shutdown())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to start service: {e}")
        raise
        
    finally:
        # Cleanup
        logger.info("Shutting down Analytics Service")
        
        if analytics_engine:
            await analytics_engine.shutdown()
        
        if ignite_client:
            await ignite_client.close()
        
        if redis_client:
            await redis_client.close()
        
        if mongodb_client:
            mongodb_client.close()
        
        if consul_client:
            try:
                await consul_client.agent.service.deregister(
                    service_id=f"{config.service_name}-{config.port}"
                )
            except Exception as e:
                logger.error(f"Failed to deregister from Consul: {e}")
            finally:
                await consul_client.close()
        
        logger.info("Shutdown complete")


async def shutdown():
    """Graceful shutdown"""
    logger.info("Initiating graceful shutdown")
    # The lifespan context manager will handle cleanup


# Create FastAPI app
app = FastAPI(
    title="Analytics Service",
    description="Blockchain analytics and reporting service",
    version=config.service_version,
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

# Add routers
app.include_router(analytics_router, prefix=config.api_prefix)


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": config.service_name,
        "version": config.service_version,
        "status": "running"
    }


@app.get("/health")
async def health():
    """Basic health check"""
    health_status = {
        "status": "healthy",
        "service": config.service_name,
        "version": config.service_version
    }
    
    # Check dependencies
    checks = {
        "ignite": ignite_client is not None,
        "redis": redis_client is not None,
        "mongodb": mongodb_client is not None,
        "analytics_engine": analytics_engine is not None
    }
    
    # Check Redis connectivity
    if redis_client:
        try:
            await redis_client.ping()
            checks["redis_connected"] = True
        except:
            checks["redis_connected"] = False
    
    health_status["checks"] = checks
    health_status["healthy"] = all(checks.values())
    
    if not health_status["healthy"]:
        return health_status, 503
    
    return health_status


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    # Update alert metrics
    if analytics_engine:
        alert_counts = {}
        for alert in analytics_engine.active_alerts.values():
            if alert.is_active:
                alert_counts[alert.chain] = alert_counts.get(alert.chain, 0) + 1
        
        for chain, count in alert_counts.items():
            active_alerts.labels(chain=chain).set(count)
    
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """Add request processing time header"""
    import time
    start_time = time.time()
    
    response = await call_next(request)
    
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    
    # Track metrics
    if request.url.path.startswith("/api/v1/analytics/query"):
        query_duration.labels(metric_type="query").observe(process_time)
    
    return response


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """General exception handler"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return {
        "error": "Internal server error",
        "message": str(exc) if config.environment == "development" else "An error occurred"
    }, 500


# Run the application
if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host=config.host,
        port=config.port,
        reload=config.environment == "development",
        log_level=config.log_level.lower()
    ) 