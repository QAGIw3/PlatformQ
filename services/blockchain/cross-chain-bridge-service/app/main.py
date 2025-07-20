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
import httpx
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
import consul.aio

from .config import config
from .api.bridge_endpoints import router as bridge_router
from .core.bridge_manager import BridgeManager


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
transfer_counter = Counter(
    'bridge_transfers_total',
    'Total number of bridge transfers',
    ['bridge_name', 'status']
)

transfer_duration = Histogram(
    'bridge_transfer_duration_seconds',
    'Duration of bridge transfers',
    ['bridge_name']
)

active_transfers = Gauge(
    'bridge_active_transfers',
    'Number of active transfers',
    ['bridge_name']
)

bridge_health = Gauge(
    'bridge_health_status',
    'Bridge health status (1=healthy, 0=unhealthy)',
    ['bridge_name']
)

# Global instances
bridge_manager: Optional[BridgeManager] = None
pulsar_client: Optional[aiopulsar.Client] = None
ignite_client: Optional[IgniteClient] = None
consul_client: Optional[consul.aio.Consul] = None
key_mgmt_client: Optional[httpx.AsyncClient] = None
blockchain_client: Optional[httpx.AsyncClient] = None
tx_processor_client: Optional[httpx.AsyncClient] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    global bridge_manager, pulsar_client, ignite_client, consul_client
    global key_mgmt_client, blockchain_client, tx_processor_client
    
    logger.info("Starting Cross-Chain Bridge Service")
    
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
            tags=["blockchain", "bridge", "cross-chain"],
            check={
                "http": f"http://{config.host}:{config.port}/api/v1/bridge/health",
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
        
        # Initialize HTTP clients
        key_mgmt_client = httpx.AsyncClient(
            base_url=config.key_management_url,
            timeout=config.signing_timeout
        )
        
        blockchain_client = httpx.AsyncClient(
            base_url=config.blockchain_connector_url,
            timeout=30.0
        )
        
        tx_processor_client = httpx.AsyncClient(
            base_url=config.transaction_processor_url,
            timeout=30.0
        )
        
        # Initialize bridge manager
        bridge_manager = BridgeManager(
            pulsar_client=pulsar_client,
            ignite_client=ignite_client,
            key_mgmt_client=key_mgmt_client,
            blockchain_client=blockchain_client,
            tx_processor_client=tx_processor_client
        )
        
        await bridge_manager.initialize()
        logger.info("Bridge manager initialized")
        
        # Setup signal handlers
        def signal_handler(sig, frame):
            logger.info(f"Received signal {sig}, initiating shutdown")
            # The lifespan context manager will handle cleanup
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to start service: {e}")
        raise
        
    finally:
        # Cleanup
        logger.info("Shutting down Cross-Chain Bridge Service")
        
        if bridge_manager:
            await bridge_manager.shutdown()
        
        if pulsar_client:
            await pulsar_client.close()
        
        if ignite_client:
            await ignite_client.close()
        
        if key_mgmt_client:
            await key_mgmt_client.aclose()
        
        if blockchain_client:
            await blockchain_client.aclose()
        
        if tx_processor_client:
            await tx_processor_client.aclose()
        
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
    title="Cross-Chain Bridge Service",
    description="Service for managing cross-chain token transfers",
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
app.include_router(bridge_router, prefix=config.api_prefix)


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
        "bridge_manager": bridge_manager is not None
    }
    
    health_status["checks"] = checks
    health_status["healthy"] = all(checks.values())
    
    if not health_status["healthy"]:
        return health_status, 503
    
    return health_status


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    # Update bridge health metrics
    if bridge_manager:
        for bridge_name in bridge_manager.bridges:
            health = await bridge_manager.get_bridge_health(bridge_name)
            if health:
                bridge_health.labels(bridge_name=bridge_name).set(
                    1 if health.is_operational else 0
                )
    
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