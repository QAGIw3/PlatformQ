"""
DID Service Main Application
"""

import os
import asyncio
import signal
import sys
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import httpx
from prometheus_client import Counter, Histogram, generate_latest
from prometheus_client.core import CollectorRegistry
import time

from app.config import settings
from app.api import did_api
from app.core.did_manager import DIDManager
from app.storage.did_store import DIDStore
from app.core.cache_manager import DIDCacheManager
from platformq_consul import ConsulClient, ServiceRegistration

# Metrics
REGISTRY = CollectorRegistry()
REQUEST_COUNT = Counter('did_service_requests_total', 'Total requests', ['method', 'endpoint', 'status'], registry=REGISTRY)
REQUEST_DURATION = Histogram('did_service_request_duration_seconds', 'Request duration', ['method', 'endpoint'], registry=REGISTRY)

# Global instances
consul_client: Optional[ConsulClient] = None
did_manager: Optional[DIDManager] = None
did_store: Optional[DIDStore] = None
cache_manager: Optional[DIDCacheManager] = None
vault_client: Optional[Any] = None
http_client: Optional[httpx.AsyncClient] = None


async def initialize_vault():
    """Initialize Vault client"""
    global vault_client
    
    if settings.vault_token:
        import hvac
        vault_client = hvac.Client(
            url=settings.vault_addr,
            token=settings.vault_token,
            namespace=settings.vault_namespace
        )
        
        # Test connection
        if not vault_client.is_authenticated():
            raise Exception("Failed to authenticate with Vault")
            
        print("✓ Vault client initialized")


async def initialize_consul():
    """Initialize Consul client and register service"""
    global consul_client
    
    if settings.enable_consul_config:
        consul_client = ConsulClient(
            host=settings.consul_host,
            port=settings.consul_port
        )
        
        # Register service
        registration = ServiceRegistration(
            name=settings.service_name,
            service_id=f"{settings.service_name}-{os.getenv('HOSTNAME', 'local')}",
            address=os.getenv('SERVICE_ADDRESS', 'localhost'),
            port=settings.port,
            tags=[
                settings.environment,
                "did-service",
                "verifiable-credentials",
                f"version:{settings.service_version}"
            ],
            check={
                "http": f"http://localhost:{settings.port}/health",
                "interval": f"{settings.service_health_interval}s",
                "timeout": "5s"
            }
        )
        
        await consul_client.register_service(registration)
        print("✓ Service registered with Consul")


async def initialize_cache():
    """Initialize Apache Ignite cache manager"""
    global cache_manager
    
    if settings.enable_cache:
        from app.core.cache_manager import DIDCacheManager
        
        cache_manager = DIDCacheManager(
            host=settings.ignite_host,
            port=settings.ignite_port,
            ttl_seconds=settings.cache_ttl_seconds
        )
        
        await cache_manager.connect()
        print("✓ Apache Ignite cache initialized")


async def initialize_storage():
    """Initialize DID document storage"""
    global did_store
    
    from app.storage.did_store import DIDStore
    
    did_store = DIDStore(
        database_url=settings.database_url,
        vault_client=vault_client
    )
    
    await did_store.initialize()
    print("✓ DID storage initialized")


async def initialize_did_manager():
    """Initialize DID manager with resolvers"""
    global did_manager, http_client
    
    # Create HTTP client for external service calls
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(30.0),
        limits=httpx.Limits(max_keepalive_connections=50)
    )
    
    from app.core.did_manager import DIDManager
    
    did_manager = DIDManager(
        did_store=did_store,
        cache_manager=cache_manager,
        http_client=http_client,
        consul_client=consul_client,
        vault_client=vault_client
    )
    
    # Initialize resolvers
    await did_manager.initialize_resolvers()
    print("✓ DID manager initialized")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle manager"""
    print(f"Starting {settings.service_name} v{settings.service_version}")
    
    try:
        # Initialize services
        await initialize_vault()
        await initialize_consul()
        await initialize_cache()
        await initialize_storage()
        await initialize_did_manager()
        
        # Load dynamic configuration from Consul
        if consul_client and settings.enable_consul_config:
            config = await consul_client.get_service_config(settings.service_name)
            if config:
                print(f"Loaded configuration from Consul: {len(config)} keys")
        
        print(f"✓ {settings.service_name} started successfully")
        
        yield
        
    finally:
        # Cleanup
        print(f"Shutting down {settings.service_name}...")
        
        if http_client:
            await http_client.aclose()
            
        if cache_manager:
            await cache_manager.disconnect()
            
        if did_store:
            await did_store.close()
            
        if consul_client:
            await consul_client.deregister_service(
                f"{settings.service_name}-{os.getenv('HOSTNAME', 'local')}"
            )
            await consul_client.close()
            
        print(f"✓ {settings.service_name} stopped")


# Create FastAPI app
app = FastAPI(
    title=settings.service_name,
    version=settings.service_version,
    docs_url=f"{settings.api_prefix}/docs",
    redoc_url=f"{settings.api_prefix}/redoc",
    openapi_url=f"{settings.api_prefix}/openapi.json",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


# Middleware for metrics
@app.middleware("http")
async def metrics_middleware(request, call_next):
    """Track request metrics"""
    start_time = time.time()
    
    response = await call_next(request)
    
    duration = time.time() - start_time
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code
    ).inc()
    REQUEST_DURATION.labels(
        method=request.method,
        endpoint=request.url.path
    ).observe(duration)
    
    return response


# Health check endpoint
@app.get("/health")
async def health_check():
    """Service health check"""
    checks = {
        "status": "healthy",
        "service": settings.service_name,
        "version": settings.service_version,
        "checks": {}
    }
    
    # Check DID store
    if did_store:
        try:
            await did_store.health_check()
            checks["checks"]["did_store"] = "healthy"
        except Exception as e:
            checks["checks"]["did_store"] = f"unhealthy: {str(e)}"
            checks["status"] = "unhealthy"
    
    # Check cache
    if cache_manager and settings.enable_cache:
        try:
            await cache_manager.health_check()
            checks["checks"]["cache"] = "healthy"
        except Exception as e:
            checks["checks"]["cache"] = f"unhealthy: {str(e)}"
            # Cache is optional, don't mark service unhealthy
    
    # Check key management service
    if http_client:
        try:
            resp = await http_client.get(f"{settings.key_management_url}/health")
            if resp.status_code == 200:
                checks["checks"]["key_management"] = "healthy"
            else:
                checks["checks"]["key_management"] = f"unhealthy: status {resp.status_code}"
                checks["status"] = "unhealthy"
        except Exception as e:
            checks["checks"]["key_management"] = f"unhealthy: {str(e)}"
            checks["status"] = "unhealthy"
    
    # Check Vault
    if vault_client:
        try:
            if vault_client.is_authenticated():
                checks["checks"]["vault"] = "healthy"
            else:
                checks["checks"]["vault"] = "unhealthy: not authenticated"
                checks["status"] = "unhealthy"
        except Exception as e:
            checks["checks"]["vault"] = f"unhealthy: {str(e)}"
            checks["status"] = "unhealthy"
    
    status_code = 200 if checks["status"] == "healthy" else 503
    return JSONResponse(content=checks, status_code=status_code)


# Metrics endpoint
@app.get("/metrics")
async def metrics():
    """Prometheus metrics"""
    return generate_latest(REGISTRY)


# Include API routers
app.include_router(
    did_api.router,
    prefix=settings.api_prefix,
    tags=["DIDs"]
)


# Exception handlers
@app.exception_handler(ValueError)
async def value_error_handler(request, exc):
    """Handle validation errors"""
    return JSONResponse(
        status_code=400,
        content={"error": str(exc)}
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions"""
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle unexpected errors"""
    import traceback
    print(f"Unexpected error: {traceback.format_exc()}")
    
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error"}
    )


# Signal handlers
def signal_handler(signum, frame):
    """Handle shutdown signals"""
    print(f"\nReceived signal {signum}, initiating shutdown...")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
        reload=settings.environment == "development"
    ) 