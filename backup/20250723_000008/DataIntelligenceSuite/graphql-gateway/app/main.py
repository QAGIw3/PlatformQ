"""
GraphQL Gateway Service

Provides a unified GraphQL API for all DataIntelligenceSuite services.
"""

import os
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from strawberry import Schema
from strawberry.fastapi import GraphQLRouter
from strawberry.extensions import AddValidationRules, QueryDepthLimiter, MaxTokensLimiter
from strawberry.schema.config import StrawberryConfig

from data_intelligence_common import (
    DataIntelligenceBaseService,
    ServiceMetadata,
    StructuredLogger,
    MetricsCollector,
)
from data_intelligence_common.vault_consul import VaultConsulIntegration
import consul.aio
import hvac

from ..schema import Query, Mutation, Subscription
from ..resolvers import ServiceResolver
from ..dataloaders import DataLoaderRegistry

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="graphql-gateway",
    version="1.0.0",
    description="Unified GraphQL API gateway for DataIntelligenceSuite",
    dependencies=["vault", "consul", "dih-service", "data-quality-service", "pipeline-orchestration-service"],
    health_checks=["service_resolver", "schema"]
)

logger = StructuredLogger.get_logger(__name__)


class GraphQLGatewayService(DataIntelligenceBaseService):
    """GraphQL Gateway Service implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        
        # Service components
        self.service_resolver: Optional[ServiceResolver] = None
        self.dataloader_registry: Optional[DataLoaderRegistry] = None
        self.schema: Optional[Schema] = None
    
    async def initialize_service(self):
        """Initialize service-specific components"""
        logger.info("initializing_graphql_gateway_service")
        
        # Initialize service resolver
        self.service_resolver = ServiceResolver(self.vault_consul)
        await self.service_resolver.initialize()
        
        # Initialize data loaders
        self.dataloader_registry = DataLoaderRegistry(self.service_resolver)
        
        # Create GraphQL schema
        self.schema = self._create_schema()
        
        # Register health checks
        self.health_manager.register_check(
            "service_resolver",
            self._check_resolver_health,
            critical=True
        )
        self.health_manager.register_check(
            "schema",
            self._check_schema_health,
            critical=True
        )
        
        logger.info("graphql_gateway_service_initialized")
    
    async def cleanup_service(self):
        """Cleanup service-specific components"""
        logger.info("cleaning_up_graphql_gateway_service")
        
        if self.service_resolver:
            await self.service_resolver.cleanup()
        
        logger.info("graphql_gateway_service_cleaned_up")
    
    def _create_schema(self) -> Schema:
        """Create GraphQL schema with extensions"""
        config = StrawberryConfig(auto_camel_case=True)
        
        # Create schema with extensions
        schema = Schema(
            query=Query,
            mutation=Mutation,
            subscription=Subscription,
            config=config,
            extensions=[
                QueryDepthLimiter(max_depth=10),
                MaxTokensLimiter(max_token_count=1000),
                AddValidationRules([])
            ]
        )
        
        return schema
    
    async def _check_resolver_health(self) -> bool:
        """Check service resolver health"""
        return self.service_resolver is not None and await self.service_resolver.is_healthy()
    
    async def _check_schema_health(self) -> bool:
        """Check schema health"""
        return self.schema is not None


# Create FastAPI app
def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    
    # Get configuration from environment
    vault_addr = os.getenv("VAULT_ADDR", "http://localhost:8200")
    vault_token = os.getenv("VAULT_TOKEN")
    consul_host = os.getenv("CONSUL_HOST", "localhost")
    consul_port = int(os.getenv("CONSUL_PORT", "8500"))
    consul_token = os.getenv("CONSUL_TOKEN")
    
    # Create Vault client
    vault_client = hvac.Client(url=vault_addr, token=vault_token)
    
    # Create Consul client
    consul_client = consul.aio.Consul(
        host=consul_host,
        port=consul_port,
        token=consul_token
    )
    
    # Create service instance
    service = GraphQLGatewayService(
        vault_client=vault_client,
        consul_client=consul_client
    )
    
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """Application lifespan manager"""
        # Startup
        await service.startup()
        
        # Store service in app state
        app.state.service = service
        
        # Create GraphQL router with context
        graphql_router = GraphQLRouter(
            service.schema,
            context_getter=lambda: {
                "service_resolver": service.service_resolver,
                "dataloader_registry": service.dataloader_registry,
                "metrics": service.metrics_collector
            }
        )
        
        # Add GraphQL route
        app.include_router(graphql_router, prefix="/graphql")
        
        yield
        
        # Shutdown
        await service.shutdown()
    
    # Create FastAPI app
    app = FastAPI(
        title=SERVICE_METADATA.name,
        description=SERVICE_METADATA.description,
        version=SERVICE_METADATA.version,
        lifespan=lifespan
    )
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Add health endpoint
    @app.get("/health")
    async def health():
        """Health check endpoint"""
        health_status = await service.health_manager.check_all()
        return {
            "status": health_status.status.value,
            "checks": {
                name: {
                    "status": result.status.value,
                    "message": result.message
                }
                for name, result in health_status.checks.items()
            }
        }
    
    # Add metrics endpoint
    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics():
        """Prometheus metrics endpoint"""
        if service.metrics_collector:
            return service.metrics_collector.get_metrics()
        return ""
    
    # Add root endpoint
    @app.get("/")
    async def root():
        """Root endpoint"""
        return {
            "service": SERVICE_METADATA.name,
            "version": SERVICE_METADATA.version,
            "description": SERVICE_METADATA.description,
            "graphql_endpoint": "/graphql"
        }
    
    return app


# Import PlainTextResponse
from starlette.responses import PlainTextResponse

# Create app instance
app = create_app()


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("SERVICE_PORT", "8005"))
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=True,
        log_level="info"
    ) 