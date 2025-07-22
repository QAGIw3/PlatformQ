"""
SoulBound Token Service Main Application
"""

import asyncio
import signal
import sys
from contextlib import asynccontextmanager
from typing import Optional, Any

import httpx
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator

from platformq_consul import ConsulClient
from app.config import settings
from app.core.sbt_manager import SBTManager
from app.core.event_publisher import SBTEventPublisher
from app.storage.sbt_store import SBTStore
from app.api import sbt_api, transfer_api, metadata_api

# Global instances
vault_client: Optional[Any] = None
consul_client: Optional[ConsulClient] = None
http_client: Optional[httpx.AsyncClient] = None
sbt_manager: Optional[SBTManager] = None
event_publisher: Optional[SBTEventPublisher] = None
sbt_store: Optional[SBTStore] = None
shutdown_event = asyncio.Event()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global vault_client, consul_client, http_client, sbt_manager
    global event_publisher, sbt_store
    
    try:
        # Initialize Vault client
        if settings.enable_vault:
            try:
                import hvac
                vault_client = hvac.Client(url=settings.vault_addr, token=settings.vault_token)
                
                # Enable Transit secrets engine if not already enabled
                try:
                    vault_client.sys.enable_secrets_engine(
                        backend_type='transit',
                        path='transit',
                        description='Transit secrets engine for SBT encryption'
                    )
                except Exception:
                    # Already enabled
                    pass
                
                # Create encryption key for SBT data
                try:
                    vault_client.secrets.transit.create_key(
                        name='sbt-encryption',
                        key_type='aes256-gcm96'
                    )
                except Exception:
                    # Key already exists
                    pass
                    
                print("Connected to Vault")
            except Exception as e:
                print(f"Failed to connect to Vault: {str(e)}")
                if settings.require_vault:
                    raise
        
        # Initialize Consul client
        if settings.enable_consul:
            try:
                consul_client = ConsulClient(
                    host=settings.consul_host,
                    port=settings.consul_port
                )
                await consul_client.register_service(
                    name=settings.service_name,
                    service_id=f"{settings.service_name}-{settings.instance_id}",
                    address=settings.service_host,
                    port=settings.service_port,
                    tags=["sbt", "soulbound", "tokens", "credentials"],
                    check={
                        "http": f"http://{settings.service_host}:{settings.service_port}/health",
                        "interval": "10s",
                        "timeout": "5s"
                    }
                )
                print("Registered with Consul")
            except Exception as e:
                print(f"Failed to connect to Consul: {str(e)}")
                if settings.enable_consul_config:
                    raise
        
        # Initialize HTTP client
        http_client = httpx.AsyncClient(timeout=30.0)
        
        # Initialize storage
        sbt_store = SBTStore(
            database_url=settings.database_url,
            vault_client=vault_client
        )
        await sbt_store.initialize()
        
        # Initialize event publisher
        if settings.enable_events:
            event_publisher = SBTEventPublisher(
                pulsar_url=settings.pulsar_url,
                topic_prefix="sbt"
            )
            await event_publisher.connect()
        
        # Initialize SBT manager
        sbt_manager = SBTManager(
            blockchain_connector_url=settings.blockchain_connector_url,
            credential_service_url=settings.credential_service_url,
            storage_service_url=settings.storage_service_url,
            http_client=http_client,
            vault_client=vault_client,
            consul_client=consul_client,
            sbt_store=sbt_store,
            event_publisher=event_publisher
        )
        await sbt_manager.initialize()
        
        print(f"SBT Service started on port {settings.service_port}")
        
        yield
        
    finally:
        # Cleanup
        if consul_client and settings.enable_consul:
            await consul_client.deregister_service(
                f"{settings.service_name}-{settings.instance_id}"
            )
        
        if http_client:
            await http_client.aclose()
        
        if event_publisher:
            await event_publisher.close()
        
        if sbt_store:
            await sbt_store.close()
        
        print("SBT Service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="SoulBound Token Service",
    description="Service for managing SoulBound Tokens (SBTs) and their lifecycle",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add Prometheus metrics
instrumentator = Instrumentator()
instrumentator.instrument(app).expose(app)

# Include routers
app.include_router(sbt_api.router, prefix="/api/v1", tags=["sbt"])
app.include_router(transfer_api.router, prefix="/api/v1", tags=["transfer"])
app.include_router(metadata_api.router, prefix="/api/v1", tags=["metadata"])


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "SoulBound Token Service",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    checks = {
        "service": True,
        "database": await sbt_store.health_check() if sbt_store else False,
        "vault": vault_client is not None and settings.enable_vault,
        "consul": consul_client is not None and settings.enable_consul,
        "blockchain": await sbt_manager.check_blockchain_connection() if sbt_manager else False,
        "events": event_publisher and event_publisher.connected if event_publisher else False
    }
    
    all_healthy = all(checks.values())
    
    return JSONResponse(
        content={
            "status": "healthy" if all_healthy else "degraded",
            "checks": checks
        },
        status_code=200 if all_healthy else 503
    )


@app.get("/ready")
async def readiness_check():
    """Readiness check endpoint"""
    return {
        "ready": sbt_manager is not None,
        "initialized": sbt_manager.initialized if sbt_manager else False
    }


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "type": type(exc).__name__
        }
    )


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    print(f"Received signal {signum}")
    shutdown_event.set()


if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Run the application
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=settings.service_port,
        loop="asyncio",
        access_log=True
    ) 