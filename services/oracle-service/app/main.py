"""
Oracle Service Main Application
"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
import asyncio
import consul

from .config import settings
from .api import measurements, quality, defi_oracles
from .oracles import (
    QuantumOracle, AIOracle, NetworkOracle,
    QualityAggregator, AvailabilityMonitor, 
    PriceAggregator, PerformanceOracle
)
from .utils.blockchain import BlockchainOracle
from . import core
from .models.measurements import OracleHealthResponse


# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Oracle instances
oracles = {}
blockchain_oracle = None
measurement_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    logger.info("Starting Oracle Service")
    
    try:
        # Initialize oracles
        oracles["quantum"] = QuantumOracle()
        await oracles["quantum"].initialize()
        
        oracles["ai"] = AIOracle()
        await oracles["ai"].initialize()
        
        oracles["network"] = NetworkOracle()
        await oracles["network"].initialize()
        
        # Set oracle instances for dependency injection
        core.dependencies.quantum_oracle_instance = oracles["quantum"]
        core.dependencies.ai_oracle_instance = oracles["ai"]
        core.dependencies.network_oracle_instance = oracles["network"]
        
        # Initialize blockchain oracle
        global blockchain_oracle
        blockchain_oracle = BlockchainOracle()
        await blockchain_oracle.initialize()
        
        # Initialize DeFi oracles
        # Quality Aggregator
        oracles["quality_aggregator"] = QualityAggregator(
            blockchain_client=blockchain_oracle.client,
            quantum_oracle=oracles["quantum"],
            ai_oracle=oracles["ai"],
            network_oracle=oracles["network"],
            oracle_contract_address=settings.QUALITY_ORACLE_ADDRESS,
            signing_key=settings.ORACLE_SIGNING_KEY
        )
        
        # Availability Monitor
        oracles["availability_monitor"] = AvailabilityMonitor(
            blockchain_client=blockchain_oracle.client,
            monitor_contract_address=settings.AVAILABILITY_MONITOR_ADDRESS,
            signing_key=settings.ORACLE_SIGNING_KEY,
            check_interval=settings.AVAILABILITY_CHECK_INTERVAL
        )
        await oracles["availability_monitor"].initialize()
        
        # Price Aggregator
        market_addresses = {
            'quantum': settings.QUANTUM_MARKET_ADDRESS,
            'ai': settings.AI_MARKET_ADDRESS,
            'network': settings.NETWORK_MARKET_ADDRESS
        }
        amm_addresses = {
            'quantum': settings.QUANTUM_AMM_ADDRESS,
            'ai': settings.AI_AMM_ADDRESS,
            'network': settings.NETWORK_AMM_ADDRESS
        }
        oracles["price_aggregator"] = PriceAggregator(
            blockchain_client=blockchain_oracle.client,
            oracle_contract_address=settings.PRICE_ORACLE_ADDRESS,
            signing_key=settings.ORACLE_SIGNING_KEY,
            market_addresses=market_addresses,
            amm_addresses=amm_addresses
        )
        await oracles["price_aggregator"].initialize()
        
        # Performance Oracle
        oracles["performance_oracle"] = PerformanceOracle(
            blockchain_client=blockchain_oracle.client,
            oracle_contract_address=settings.PERFORMANCE_ORACLE_ADDRESS,
            signing_key=settings.ORACLE_SIGNING_KEY,
            quantum_oracle=oracles["quantum"],
            ai_oracle=oracles["ai"],
            network_oracle=oracles["network"]
        )
        
        # Set DeFi oracle instances for dependency injection
        core.dependencies.quality_aggregator_instance = oracles["quality_aggregator"]
        core.dependencies.availability_monitor_instance = oracles["availability_monitor"]
        core.dependencies.price_aggregator_instance = oracles["price_aggregator"]
        core.dependencies.performance_oracle_instance = oracles["performance_oracle"]
        
        # Start background measurement task
        global measurement_task
        measurement_task = asyncio.create_task(periodic_measurements())
        
        # Register with Consul
        await register_with_consul()
        
        logger.info("Oracle Service started successfully")
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to start service: {e}")
        raise
    
    finally:
        # Cleanup
        logger.info("Shutting down Oracle Service")
        
        # Cancel background task
        if measurement_task:
            measurement_task.cancel()
            try:
                await measurement_task
            except asyncio.CancelledError:
                pass
        
        # Cleanup oracles
        for oracle in oracles.values():
            if hasattr(oracle, 'cleanup'):
                await oracle.cleanup()
            # Special cleanup for oracles with shutdown methods
            if hasattr(oracle, 'shutdown'):
                await oracle.shutdown()
        
        # Deregister from Consul
        await deregister_from_consul()
        
        logger.info("Oracle Service stopped")


# Create FastAPI app
app = FastAPI(
    title=settings.SERVICE_NAME,
    version=settings.VERSION,
    description="Decentralized oracle service for compute resource quality verification",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Setup Prometheus metrics
if settings.PROMETHEUS_ENABLED:
    instrumentator = Instrumentator()
    instrumentator.instrument(app).expose(app, endpoint="/metrics")

# Include routers
app.include_router(measurements.router, prefix=settings.API_PREFIX)
app.include_router(quality.router, prefix=settings.API_PREFIX)
app.include_router(defi_oracles.router)


# Root endpoint
@app.get("/")
async def root():
    """Service information"""
    return {
        "service": settings.SERVICE_NAME,
        "version": settings.VERSION,
        "status": "healthy",
        "api_docs": "/docs",
        "oracles": [
            "quantum", "ai", "network",
            "quality_aggregator", "availability_monitor",
            "price_aggregator", "performance_oracle"
        ],
        "defi_oracles_api": "/api/v1/defi-oracles"
    }


# Health check endpoint
@app.get("/health", response_model=OracleHealthResponse)
async def health_check():
    """Health check endpoint"""
    try:
        # Check oracle health
        oracle_status = {}
        for name, oracle in oracles.items():
            if oracle and hasattr(oracle, 'ignite_client'):
                oracle_status[name] = "healthy" if oracle.ignite_client else "unhealthy"
            else:
                oracle_status[name] = "unhealthy"
        
        # Calculate measurement rate (placeholder)
        measurement_rate = 60.0  # per minute
        
        # Get active resources (placeholder)
        active_resources = len(oracle_status) * 10
        
        # Check blockchain status
        blockchain_status = None
        if blockchain_oracle and hasattr(blockchain_oracle, '_is_ready'):
            if blockchain_oracle._is_ready():
                blockchain_status = "connected"
            else:
                blockchain_status = "disconnected"
        
        return OracleHealthResponse(
            status="healthy" if all(s == "healthy" for s in oracle_status.values()) else "degraded",
            measurement_rate=measurement_rate,
            active_resources=active_resources,
            last_blockchain_update=None,  # Would track in production
            pending_updates=0  # Would track in production
        )
        
    except Exception as e:
        logger.error(f"Health check error: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")


# Ready check endpoint
@app.get("/ready")
async def ready_check():
    """Readiness check endpoint"""
    if all(oracle is not None for oracle in oracles.values()):
        return {"status": "ready"}
    else:
        raise HTTPException(status_code=503, detail="Service not ready")


async def periodic_measurements():
    """Background task for periodic measurements"""
    logger.info("Starting periodic measurement task")
    
    while True:
        try:
            # Sleep for measurement interval
            await asyncio.sleep(settings.MEASUREMENT_INTERVAL)
            
            # In production, would:
            # 1. Query active resources from market services
            # 2. Perform measurements for each resource
            # 3. Aggregate results
            # 4. Submit to blockchain
            
            logger.debug("Periodic measurement cycle completed")
            
        except asyncio.CancelledError:
            logger.info("Periodic measurement task cancelled")
            break
        except Exception as e:
            logger.error(f"Error in periodic measurements: {e}")
            await asyncio.sleep(60)  # Wait before retry


async def register_with_consul():
    """Register service with Consul"""
    try:
        c = consul.Consul(
            host=settings.CONSUL_HOST,
            port=settings.CONSUL_PORT
        )
        
        # Register service
        c.agent.service.register(
            name=settings.CONSUL_SERVICE_NAME,
            service_id=f"{settings.CONSUL_SERVICE_NAME}-{settings.PORT}",
            address=settings.HOST,
            port=settings.PORT,
            tags=[
                "oracle",
                "measurement",
                "quality",
                f"version:{settings.VERSION}"
            ],
            check=consul.Check.http(
                f"http://{settings.HOST}:{settings.PORT}/health",
                interval=settings.CONSUL_HEALTH_CHECK_INTERVAL,
                timeout="5s"
            )
        )
        
        logger.info(f"Registered with Consul as {settings.CONSUL_SERVICE_NAME}")
        
    except Exception as e:
        logger.error(f"Failed to register with Consul: {e}")


async def deregister_from_consul():
    """Deregister service from Consul"""
    try:
        c = consul.Consul(
            host=settings.CONSUL_HOST,
            port=settings.CONSUL_PORT
        )
        
        c.agent.service.deregister(
            service_id=f"{settings.CONSUL_SERVICE_NAME}-{settings.PORT}"
        )
        
        logger.info("Deregistered from Consul")
        
    except Exception as e:
        logger.error(f"Failed to deregister from Consul: {e}")


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        workers=settings.WORKERS,
        reload=True
    ) 