"""
Core Credential Service - Main Application
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator
import signal
import sys

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from pyignite import Client as IgniteClient
import httpx

from platformq_shared.consul import ConsulClient
from platformq_shared.vault import VaultClient
from platformq_shared.pulsar import PulsarManager

from .config import settings
from .api import credentials_api
from .core.credential_manager import CredentialManager
from .core.cache_manager import CacheManager
from .core.event_publisher import CredentialEventPublisher
from .storage.credential_store import CredentialStore

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global instances
consul_client: ConsulClient = None
vault_client: VaultClient = None
ignite_client: IgniteClient = None
pulsar_manager: PulsarManager = None
credential_manager: CredentialManager = None
cache_manager: CacheManager = None
event_publisher: CredentialEventPublisher = None
http_client: httpx.AsyncClient = None
credential_store: CredentialStore = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    """Application lifespan manager"""
    global consul_client, vault_client, ignite_client, pulsar_manager
    global credential_manager, cache_manager, event_publisher, http_client
    global credential_store
    
    logger.info(f"Starting {settings.service_name} v{settings.service_version}")
    
    try:
        # Initialize Consul client
        consul_client = ConsulClient(
            host=settings.consul_host,
            port=settings.consul_port
        )
        await consul_client.register_service(
            name=settings.service_name,
            service_id=f"{settings.service_name}-{settings.port}",
            address=settings.host,
            port=settings.port,
            health_check_interval=settings.service_health_interval
        )
        
        # Initialize Vault client
        if settings.vault_token:
            vault_client = VaultClient(
                vault_addr=settings.vault_addr,
                vault_token=settings.vault_token,
                namespace=settings.vault_namespace
            )
            await vault_client.initialize()
        
        # Initialize Ignite client for caching
        if settings.enable_cache:
            ignite_client = IgniteClient()
            ignite_client.connect(settings.ignite_host, settings.ignite_port)
            cache_manager = CacheManager(ignite_client, settings.cache_ttl_seconds)
            await cache_manager.initialize()
            logger.info("Connected to Apache Ignite")
        
        # Initialize Pulsar
        pulsar_manager = PulsarManager(settings.pulsar_url)
        await pulsar_manager.connect()
        
        # Initialize HTTP client for service calls
        http_client = httpx.AsyncClient(timeout=30.0)
        
        # Initialize credential store
        credential_store = CredentialStore(
            database_url=settings.database_url,
            storage_service_url=settings.storage_service_url,
            http_client=http_client,
            vault_client=vault_client
        )
        await credential_store.initialize()
        
        # Initialize event publisher
        event_publisher = CredentialEventPublisher(
            pulsar_manager=pulsar_manager,
            topic=settings.credential_events_topic
        )
        
        # Initialize credential manager
        credential_manager = CredentialManager(
            credential_store=credential_store,
            cache_manager=cache_manager,
            event_publisher=event_publisher,
            http_client=http_client,
            key_management_url=settings.key_management_url,
            blockchain_connector_url=settings.blockchain_connector_url,
            did_service_url=settings.did_service_url,
            consul_client=consul_client
        )
        
        # Load dynamic configuration from Consul
        await credential_manager.load_consul_config()
        
        # Store in app state for access in endpoints
        app.state.credential_manager = credential_manager
        app.state.cache_manager = cache_manager
        app.state.http_client = http_client
        
        logger.info("All services initialized successfully")
        
        # Setup signal handlers
        def signal_handler(sig, frame):
            logger.info(f"Received signal {sig}, initiating shutdown...")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise
    finally:
        # Cleanup
        logger.info("Shutting down services...")
        
        if consul_client:
            await consul_client.deregister_service(
                f"{settings.service_name}-{settings.port}"
            )
        
        if ignite_client:
            ignite_client.close()
        
        if pulsar_manager:
            await pulsar_manager.close()
            
        if http_client:
            await http_client.aclose()
            
        if credential_store:
            await credential_store.close()
        
        logger.info("Shutdown complete")


# Create FastAPI app
app = FastAPI(
    title=settings.service_name,
    version=settings.service_version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Include API routes
app.include_router(
    credentials_api.router,
    prefix=settings.api_prefix,
    tags=["credentials"]
)

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    health_status = {
        "status": "healthy",
        "service": settings.service_name,
        "version": settings.service_version,
        "checks": {
            "database": "unknown",
            "cache": "unknown",
            "pulsar": "unknown"
        }
    }
    
    # Check database
    if credential_store:
        try:
            await credential_store.health_check()
            health_status["checks"]["database"] = "healthy"
        except Exception:
            health_status["checks"]["database"] = "unhealthy"
            health_status["status"] = "degraded"
    
    # Check cache
    if cache_manager and settings.enable_cache:
        try:
            await cache_manager.health_check()
            health_status["checks"]["cache"] = "healthy"
        except Exception:
            health_status["checks"]["cache"] = "unhealthy"
    
    # Check Pulsar
    if pulsar_manager:
        try:
            health_status["checks"]["pulsar"] = "healthy" if pulsar_manager.is_connected() else "unhealthy"
        except Exception:
            health_status["checks"]["pulsar"] = "unhealthy"
    
    return health_status


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "status": "running"
    }


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "type": type(exc).__name__
        }
    )


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.environment == "development"
    ) 