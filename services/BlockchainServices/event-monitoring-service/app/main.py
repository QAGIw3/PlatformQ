from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import sys
from typing import Optional
import signal
import asyncio

import aiopulsar
from pyignite import AsyncClient as IgniteClient
import aioredis
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
import consul.aio

from .config import config
from .api.event_endpoints import router as event_router
from .core.event_processor import EventProcessor


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
events_processed = Counter(
    'blockchain_events_processed_total',
    'Total number of blockchain events processed',
    ['chain', 'event_type']
)

webhooks_delivered = Counter(
    'webhooks_delivered_total',
    'Total number of webhooks delivered',
    ['status']
)

monitor_lag = Gauge(
    'blockchain_monitor_lag_blocks',
    'Number of blocks behind current head',
    ['chain']
)

event_processing_time = Histogram(
    'event_processing_duration_seconds',
    'Time spent processing events',
    ['chain']
)

# Global instances
event_processor: Optional[EventProcessor] = None
pulsar_client: Optional[aiopulsar.Client] = None
ignite_client: Optional[IgniteClient] = None
redis_client: Optional[aioredis.Redis] = None
consul_client: Optional[consul.aio.Consul] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    global event_processor, pulsar_client, ignite_client, redis_client, consul_client
    
    logger.info("Starting Event Monitoring Service")
    
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
            tags=["blockchain", "events", "monitoring", "webhooks"],
            check={
                "http": f"http://{config.host}:{config.port}/api/v1/events/health",
                "interval": f"{config.service_health_interval}s"
            }
        )
        logger.info("Registered with Consul")
        
        # Initialize Pulsar client
        pulsar_client = await aiopulsar.connect(config.pulsar_url)
        logger.info("Connected to Pulsar")
        
        # Initialize Ignite client
        ignite_client = IgniteClient()
        await ignite_client.connect([(config.ignite_host, config.ignite_port)])
        logger.info("Connected to Ignite")
        
        # Initialize Redis client
        redis_client = await aioredis.from_url(
            config.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
        logger.info("Connected to Redis")
        
        # Initialize event processor
        event_processor = EventProcessor(
            pulsar_client=pulsar_client,
            ignite_client=ignite_client,
            redis_client=redis_client
        )
        
        await event_processor.initialize()
        logger.info("Event processor initialized")
        
        # Setup signal handlers
        def signal_handler(sig, frame):
            logger.info(f"Received signal {sig}, initiating shutdown")
            asyncio.create_task(shutdown())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start metrics updater
        asyncio.create_task(update_metrics())
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to start service: {e}")
        raise
        
    finally:
        # Cleanup
        logger.info("Shutting down Event Monitoring Service")
        
        if event_processor:
            await event_processor.shutdown()
        
        if pulsar_client:
            await pulsar_client.close()
        
        if ignite_client:
            await ignite_client.close()
        
        if redis_client:
            await redis_client.close()
        
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


async def update_metrics():
    """Update Prometheus metrics periodically"""
    while True:
        try:
            if event_processor:
                # Update monitor lag metrics
                for chain, monitor in event_processor.monitors.items():
                    status = monitor.get_status()
                    monitor_lag.labels(chain=chain).set(status.blocks_behind)
            
            await asyncio.sleep(30)  # Update every 30 seconds
        except Exception as e:
            logger.error(f"Error updating metrics: {e}")
            await asyncio.sleep(30)


# Create FastAPI app
app = FastAPI(
    title="Event Monitoring Service",
    description="Service for monitoring blockchain events and managing webhooks",
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
app.include_router(event_router, prefix=config.api_prefix)


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
        "pulsar": pulsar_client is not None,
        "ignite": ignite_client is not None,
        "redis": redis_client is not None,
        "event_processor": event_processor is not None
    }
    
    # Check monitors
    if event_processor:
        monitor_checks = {}
        for chain, monitor in event_processor.monitors.items():
            status = monitor.get_status()
            monitor_checks[chain] = status.is_active
        checks["monitors"] = monitor_checks
    
    health_status["checks"] = checks
    health_status["healthy"] = all(checks.values())
    
    if not health_status["healthy"]:
        return health_status, 503
    
    return health_status


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """Add request processing time header"""
    import time
    start_time = time.time()
    
    response = await call_next(request)
    
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    
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