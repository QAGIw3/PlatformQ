from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trusted_host import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
from fastapi.middleware.requestid import RequestIdMiddleware
from fastapi.middleware.trusted_host import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware
from fastapi.middleware.requestid import RequestIdMiddleware
from .error_handling import ErrorHandlerMiddleware, error_handler
from .event_framework import EventProcessor
from .monitoring import setup_prometheus_metrics
from .config import init_config, get_config, ServiceConfig
from typing import Optional, List, Any
from contextlib import asynccontextmanager


def create_base_app(
    title: str = "PlatformQ Service",
    description: str = "",
    version: str = "1.0.0",
    lifespan: Optional[Any] = None,
    event_processors: Optional[List[EventProcessor]] = None,
    service_config: Optional[ServiceConfig] = None,
    **kwargs
) -> FastAPI:
    """
    Create a FastAPI app with standard PlatformQ configuration.
    
    Args:
        title: API title
        description: API description  
        version: API version
        lifespan: Async context manager for startup/shutdown
        event_processors: List of event processors to manage
        service_config: Optional service configuration object
        **kwargs: Additional FastAPI arguments
    
    Returns:
        Configured FastAPI application
    """
    # Extract service name from title or kwargs
    service_name = kwargs.pop('service_name', title.lower().replace(' ', '-'))
    
    # Initialize configuration
    config_loader = init_config(service_name, service_config)
    
    # Create lifespan context manager that includes config initialization
    @asynccontextmanager
    async def managed_lifespan(app: FastAPI):
        # Initialize configuration
        await config_loader.initialize()
        
        # Store config in app state
        app.state.config = config_loader
        
        # Run user-provided lifespan if any
        if lifespan:
            async with lifespan(app):
                yield
        else:
            yield
        
        # Cleanup configuration
        await config_loader.close()
    
    # Create FastAPI app
    app = FastAPI(
        title=title,
        description=description,
        version=version,
        lifespan=managed_lifespan,
        **kwargs
    )
    
    # Store event processors in app state
    app.state.event_processors = event_processors or []
    
    # Add CORS middleware with config
    cors_origins = config_loader.get("cors_origins", ["*"])
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Add trusted host middleware
    trusted_hosts = config_loader.get("trusted_hosts", [])
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=trusted_hosts,
    )
    
    # Add gzip middleware
    app.add_middleware(
        GZipMiddleware,
        minimum_size=100,
    )
    
    # Add HTTPS redirect middleware
    app.add_middleware(
        HTTPSRedirectMiddleware,
    )
    
    # Add request ID middleware
    app.add_middleware(
        RequestIdMiddleware,
    )
    
    # Add error handler middleware
    app.add_middleware(
        ErrorHandlerMiddleware,
        error_handler=error_handler,
    )
    
    # Setup Prometheus metrics
    setup_prometheus_metrics(app)
    
    return app 