"""AMM Service main application with Consul integration."""

import asyncio
from contextlib import asynccontextmanager
import logging
import os

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator

from app.api import amm
from app.config import Settings
from app.pools.concentrated_liquidity import ConcentratedLiquidityAMM
from app.pools.stableswap import StableSwapAMM
from app.fees.dynamic_fee_manager import DynamicFeeManager
from app.dependencies import init_dependencies

# Import Consul integration
from platformq_consul import ConsulServiceRegistry, ConsulConfigManager, ServiceMeshClient


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Metrics
request_count = Counter("amm_requests_total", "Total requests", ["method", "endpoint"])
request_duration = Histogram("amm_request_duration_seconds", "Request duration")
swap_volume = Counter("amm_swap_volume_total", "Total swap volume", ["pool_id", "direction"])
liquidity_actions = Counter("amm_liquidity_actions_total", "Liquidity actions", ["pool_id", "action"])

# Global instances for Consul
consul_registry: ConsulServiceRegistry = None
consul_config: ConsulConfigManager = None
service_mesh_client: ServiceMeshClient = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management with Consul integration."""
    global consul_registry, consul_config, service_mesh_client
    
    # Startup
    logger.info("Starting AMM Service...")
    
    # Initialize settings
    settings = Settings()
    
    # Initialize Consul integration
    consul_registry = ConsulServiceRegistry(
        consul_host=os.getenv("CONSUL_HOST", "localhost"),
        consul_port=int(os.getenv("CONSUL_PORT", "8500"))
    )
    
    consul_config = ConsulConfigManager(
        consul_host=os.getenv("CONSUL_HOST", "localhost"),
        consul_port=int(os.getenv("CONSUL_PORT", "8500")),
        config_prefix="config/amm-service/"
    )
    
    service_mesh_client = ServiceMeshClient(consul_registry)
    
    # Register service with Consul
    service_port = int(os.getenv("SERVICE_PORT", "8000"))
    await consul_registry.register_service(
        name="amm-service",
        port=service_port,
        tags=["defi", "liquidity", "amm", "api"],
        meta={
            "version": "1.0.0",
            "protocol": "http",
            "pool_types": "concentrated,stableswap"
        }
    )
    
    # Load configuration from Consul (if available)
    consul_settings = await consul_config.get_config("settings")
    if consul_settings:
        logger.info("Loaded configuration from Consul")
        # Merge with local settings
        for key, value in consul_settings.items():
            if hasattr(settings, key):
                setattr(settings, key, value)
    
    # Watch for configuration changes
    async def on_config_change(key: str, value: any):
        logger.info(f"Configuration changed: {key}")
        if key == "settings" and value:
            for k, v in value.items():
                if hasattr(settings, k):
                    setattr(settings, k, v)
                    
    await consul_config.watch_config("settings", on_config_change)
    
    # Initialize components
    concentrated_amm = ConcentratedLiquidityAMM(settings)
    stableswap_amm = StableSwapAMM(settings)
    fee_manager = DynamicFeeManager(settings)
    
    # Initialize dependencies with service mesh client
    init_dependencies(settings, concentrated_amm, stableswap_amm, fee_manager, service_mesh_client)
    
    # Start background tasks
    fee_update_task = asyncio.create_task(fee_update_loop(fee_manager, service_mesh_client))
    
    logger.info("AMM Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down AMM Service...")
    
    # Cancel background tasks
    fee_update_task.cancel()
    try:
        await fee_update_task
    except asyncio.CancelledError:
        pass
    
    # Deregister from Consul
    await consul_registry.deregister_service()
    
    # Close Consul connections
    await consul_config.close()
    await service_mesh_client.close()
    
    logger.info("AMM Service stopped")


async def fee_update_loop(fee_manager: DynamicFeeManager, mesh_client: ServiceMeshClient):
    """Background task to update fees periodically using service mesh."""
    while True:
        try:
            # Get volatility data from oracle service via service mesh
            try:
                response = await mesh_client.get(
                    "oracle-service",
                    "/api/v1/volatility/24h"
                )
                if response.status_code == 200:
                    volatility_data = response.json()
                    # Process volatility data for fee updates
                    logger.info(f"Received volatility data: {volatility_data}")
            except Exception as e:
                logger.error(f"Failed to get volatility data: {e}")
            
            await asyncio.sleep(300)  # 5 minutes
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in fee update loop: {e}")
            await asyncio.sleep(60)


# Create FastAPI app
app = FastAPI(
    title="AMM Service",
    description="Automated Market Maker with concentrated liquidity and dynamic fees",
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

# Add middleware for metrics
@app.middleware("http")
async def track_metrics(request: Request, call_next):
    """Track request metrics."""
    method = request.method
    endpoint = request.url.path
    
    request_count.labels(method=method, endpoint=endpoint).inc()
    
    with request_duration.time():
        response = await call_next(request)
    
    return response

# Add Prometheus instrumentation
instrumentator = Instrumentator()
instrumentator.instrument(app).expose(app)

# Include routers
app.include_router(amm.router, prefix="/api/v1/amm", tags=["amm"])

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint for Consul."""
    # Check if we can reach dependent services
    health_status = {
        "status": "healthy",
        "service": "amm-service",
        "features": [
            "concentrated_liquidity",
            "stableswap",
            "dynamic_fees",
            "multi_pool_types"
        ],
        "dependencies": {}
    }
    
    # Check Oracle service
    if service_mesh_client:
        try:
            response = await service_mesh_client.get("oracle-service", "/health")
            health_status["dependencies"]["oracle-service"] = "healthy" if response.status_code == 200 else "unhealthy"
        except:
            health_status["dependencies"]["oracle-service"] = "unreachable"
    
    return health_status

# Metrics endpoint
@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint."""
    return generate_latest()

# Root endpoint
@app.get("/")
async def root():
    """Service information."""
    return {
        "service": "amm-service",
        "version": "1.0.0",
        "description": "Automated Market Maker with advanced features",
        "consul_registered": consul_registry is not None,
        "service_mesh_enabled": service_mesh_client is not None,
        "endpoints": {
            "pools": "/api/v1/amm/pools",
            "liquidity": "/api/v1/amm/liquidity",
            "swaps": "/api/v1/amm/swap",
            "health": "/health",
            "metrics": "/metrics",
            "docs": "/docs"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 