"""FastAPI app factory for DataIntelligenceSuite services."""

from typing import Optional, List, Dict, Any, Callable
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app
import time
import logging

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from platformq_shared.event_publisher import EventPublisher

from .base import DataIntelligenceBaseService, ServiceMetadata
from .config import ServiceConfig
from .health import HealthCheckManager
from .middleware import (
    setup_common_middleware,
    RequestTracingMiddleware,
    MetricsMiddleware,
    ErrorHandlingMiddleware
)

logger = logging.getLogger(__name__)


def create_data_intelligence_app(
    service_metadata: ServiceMetadata,
    service_config: Optional[ServiceConfig] = None,
    vault_client: Optional[VaultClient] = None,
    consul_client: Optional[ConsulClient] = None,
    event_publisher: Optional[EventPublisher] = None,
    include_health_endpoint: bool = True,
    include_metrics_endpoint: bool = True,
    include_ready_endpoint: bool = True,
    cors_origins: List[str] = None,
    additional_middleware: List[Any] = None,
    on_startup: Optional[Callable] = None,
    on_shutdown: Optional[Callable] = None
) -> tuple[FastAPI, DataIntelligenceBaseService]:
    """
    Create a FastAPI app with common DataIntelligenceSuite setup.
    
    Args:
        service_metadata: Service metadata configuration
        service_config: Optional service configuration (will create default if not provided)
        vault_client: Optional Vault client
        consul_client: Optional Consul client
        event_publisher: Optional event publisher
        include_health_endpoint: Include /health endpoint
        include_metrics_endpoint: Include /metrics endpoint
        include_ready_endpoint: Include /ready endpoint
        cors_origins: CORS allowed origins
        additional_middleware: Additional middleware to add
        on_startup: Additional startup handler
        on_shutdown: Additional shutdown handler
        
    Returns:
        Tuple of (FastAPI app, DataIntelligenceBaseService instance)
    """
    
    # Create base service instance
    class ConcreteService(DataIntelligenceBaseService):
        """Concrete implementation of base service."""
        
        async def initialize_service(self):
            """Initialize service-specific components."""
            # Call custom startup handler if provided
            if on_startup:
                await on_startup()
                
        async def cleanup_service(self):
            """Cleanup service-specific components."""
            # Call custom shutdown handler if provided
            if on_shutdown:
                await on_shutdown()
                
    # Create service instance
    service = ConcreteService(
        metadata=service_metadata,
        config=service_config,
        vault_client=vault_client,
        consul_client=consul_client,
        event_publisher=event_publisher
    )
    
    # Create FastAPI app
    app = FastAPI(
        title=service_metadata.name,
        description=service_metadata.description,
        version=service_metadata.version,
        lifespan=service.lifespan
    )
    
    # Store service reference in app state
    app.state.service = service
    service.app = app
    
    # Setup middleware with config
    if service_config:
        setup_common_middleware(
            app, 
            service_metadata.name,
            enable_rate_limiting=service_config.enable_rate_limiting,
            rate_limit_requests=service_config.rate_limit_requests,
            rate_limit_window=service_config.rate_limit_window
        )
    else:
        setup_common_middleware(app, service_metadata.name)
    
    # Add CORS if origins provided (use from config if available)
    cors_origins = cors_origins or (service_config.cors_origins if service_config else ["*"])
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
    # Add additional middleware
    if additional_middleware:
        for middleware in additional_middleware:
            app.add_middleware(middleware)
            
    # Add standard endpoints
    if include_health_endpoint:
        @app.get("/health", tags=["monitoring"])
        async def health_check():
            """Get service health status."""
            health = await service.health_manager.check_health()
            
            # Set appropriate HTTP status
            if health.status.value == "healthy":
                status_code = 200
            elif health.status.value == "degraded":
                status_code = 200  # Still return 200 for degraded
            else:
                status_code = 503
                
            return JSONResponse(
                content=health.to_dict(),
                status_code=status_code
            )
            
    if include_ready_endpoint:
        @app.get("/ready", tags=["monitoring"])
        async def readiness_check():
            """Check if service is ready to handle requests."""
            if service._initialized and not service._shutting_down:
                return {"status": "ready"}
            else:
                return JSONResponse(
                    content={"status": "not_ready"},
                    status_code=503
                )
                
    if include_metrics_endpoint:
        # Mount Prometheus metrics endpoint
        metrics_app = make_asgi_app()
        app.mount("/metrics", metrics_app)
        
    # Add service info endpoint
    @app.get("/info", tags=["monitoring"])
    async def service_info():
        """Get service information."""
        return {
            "service": service_metadata.name,
            "version": service_metadata.version,
            "description": service_metadata.description,
            "capabilities": service_metadata.capabilities,
            "dependencies": service_metadata.dependencies,
            "uptime_seconds": service.health_manager.get_uptime(),
            "metrics": service.get_service_metrics()
        }
        
    # Add configuration endpoint (protected)
    @app.get("/config", tags=["admin"])
    async def get_configuration():
        """Get service configuration (requires authentication)."""
        # In production, this should be protected
        config = {}
        
        # Add safe configuration values
        config["service"] = {
            "name": service_metadata.name,
            "version": service_metadata.version,
            "min_memory_mb": service_metadata.min_memory_mb,
            "min_cpu_cores": service_metadata.min_cpu_cores,
            "max_concurrent_requests": service_metadata.max_concurrent_requests,
            "request_timeout_seconds": service_metadata.request_timeout_seconds
        }
        
        # Add runtime configuration if available
        if service.vault_consul:
            runtime_config = await service.get_config("runtime", {})
            config["runtime"] = runtime_config
            
        return config
        
    # Error handlers
    @app.exception_handler(404)
    async def not_found_handler(request: Request, exc):
        """Handle 404 errors."""
        return JSONResponse(
            status_code=404,
            content={
                "error": "not_found",
                "message": f"Path {request.url.path} not found",
                "service": service_metadata.name
            }
        )
        
    @app.exception_handler(500)
    async def internal_error_handler(request: Request, exc):
        """Handle 500 errors."""
        logger.error(f"Internal error: {exc}")
        return JSONResponse(
            status_code=500,
            content={
                "error": "internal_error",
                "message": "An internal error occurred",
                "service": service_metadata.name
            }
        )
        
    # Add request ID middleware
    @app.middleware("http")
    async def add_request_id(request: Request, call_next):
        """Add request ID to all requests."""
        request_id = request.headers.get("X-Request-ID")
        if not request_id:
            import uuid
            request_id = str(uuid.uuid4())
            
        request.state.request_id = request_id
        
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Service-Name"] = service_metadata.name
        response.headers["X-Service-Version"] = service_metadata.version
        
        return response
        
    # Add timing middleware
    @app.middleware("http")
    async def add_process_time(request: Request, call_next):
        """Add request processing time header."""
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        return response
        
    logger.info(f"Created FastAPI app for {service_metadata.name}")
    
    return app, service 