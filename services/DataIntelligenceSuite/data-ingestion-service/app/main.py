"""
Data Ingestion Service

Unified service for data ingestion from multiple sources,
including CDC, streaming, batch, and schema management.
Powered by Apache SeaTunnel for efficient data integration.
"""

import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, UploadFile, File
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn
import hvac
import consul.aio

from data_intelligence_common import (
    DataIntelligenceBaseService,
    ServiceMetadata,
    StructuredLogger,
    create_data_intelligence_app
)

from app.core.config import settings
from app.core.cdc_manager import CDCManager
from app.core.stream_ingestion import StreamIngestionManager
from app.core.batch_ingestion import BatchIngestionManager
from app.core.schema_registry import SchemaRegistry
from app.core.connector_manager import ConnectorManager
from app.api import ingestion, schemas, health, metrics, connectors
from app.middleware.error_handler import error_handler_middleware
from app.middleware.logging import logging_middleware

# Configure logging
logger = StructuredLogger.get_logger(__name__)

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="data-ingestion-service",
    version="1.0.0",
    description="Unified data ingestion service with CDC, streaming, and batch capabilities",
    dependencies=["seatunnel", "pulsar", "minio", "cassandra", "ignite"],
    health_checks=["cdc", "stream", "batch", "schema_registry"],
    capabilities=["cdc", "streaming", "batch", "schema-registry"],
    data_sources=["postgres", "mysql", "mongodb", "pulsar", "kafka", "files"],
    data_outputs=["data-lake", "hot-storage", "stream-topics"]
)

# Global instances
cdc_manager: Optional[CDCManager] = None
stream_manager: Optional[StreamIngestionManager] = None
batch_manager: Optional[BatchIngestionManager] = None
schema_registry: Optional[SchemaRegistry] = None
connector_manager: Optional[ConnectorManager] = None


class DataIngestionService(DataIntelligenceBaseService):
    """Data Ingestion Service implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        self.cdc_manager = None
        self.stream_manager = None
        self.batch_manager = None
        self.schema_registry = None
        self.connector_manager = None
    
    async def initialize_service(self):
        """Initialize service-specific components"""
        global cdc_manager, stream_manager, batch_manager, schema_registry, connector_manager
        
        logger.info("Initializing Data Ingestion Service components...")
        
        # Get database credentials from Vault
        postgres_creds = await self.vault_consul.get_database_credentials("postgres")
        if postgres_creds:
            os.environ["POSTGRES_USER"] = postgres_creds.get("username", "")
            os.environ["POSTGRES_PASSWORD"] = postgres_creds.get("password", "")
        
        mysql_creds = await self.vault_consul.get_database_credentials("mysql")
        if mysql_creds:
            os.environ["MYSQL_USER"] = mysql_creds.get("username", "")
            os.environ["MYSQL_PASSWORD"] = mysql_creds.get("password", "")
        
        # Get MinIO credentials from Vault
        minio_creds = await self.vault_consul.get_secret("storage/minio")
        if minio_creds:
            settings.minio_access_key = minio_creds.get("access_key", settings.minio_access_key)
            settings.minio_secret_key = minio_creds.get("secret_key", settings.minio_secret_key)
        
        # Initialize components
        cdc_manager = CDCManager(settings)
        self.cdc_manager = cdc_manager
        
        stream_manager = StreamIngestionManager(settings)
        self.stream_manager = stream_manager
        
        batch_manager = BatchIngestionManager(settings)
        self.batch_manager = batch_manager
        
        schema_registry = SchemaRegistry(settings)
        self.schema_registry = schema_registry
        
        # Initialize SeaTunnel manager first (needed by connector manager)
        from app.core.seatunnel_manager import SeaTunnelManager
        seatunnel_manager = SeaTunnelManager(settings)
        await seatunnel_manager.initialize()
        
        connector_manager = ConnectorManager(settings, seatunnel_manager, schema_registry)
        self.connector_manager = connector_manager
        
        # Cross-reference components
        cdc_manager.set_schema_registry(schema_registry)
        stream_manager.set_schema_registry(schema_registry)
        batch_manager.set_schema_registry(schema_registry)
        
        # Initialize components
        await cdc_manager.initialize()
        await stream_manager.initialize()
        await batch_manager.initialize()
        await schema_registry.initialize()
        await connector_manager.initialize()
        
        # Inject managers into API routers
        ingestion.cdc_manager = cdc_manager
        ingestion.stream_manager = stream_manager
        ingestion.batch_manager = batch_manager
        schemas.schema_registry = schema_registry
        connectors.connector_manager = connector_manager
        health.cdc_manager = cdc_manager
        health.stream_manager = stream_manager
        health.batch_manager = batch_manager
        health.schema_registry = schema_registry
        
        # Register health checks
        self.health_manager.register_check("cdc", cdc_manager.health_check)
        self.health_manager.register_check("stream", stream_manager.health_check)
        self.health_manager.register_check("batch", batch_manager.health_check)
        self.health_manager.register_check("schema_registry", schema_registry.health_check)
        self.health_manager.register_check("connectors", lambda: {"status": "healthy", "active_connectors": len(connector_manager.connectors)})
        
        logger.info("Data Ingestion Service initialized successfully")
    
    async def cleanup_service(self):
        """Cleanup service-specific components"""
        logger.info("Cleaning up Data Ingestion Service...")
        
        if self.connector_manager:
            await self.connector_manager.cleanup()
        
        if self.cdc_manager:
            await self.cdc_manager.stop()
        
        if self.stream_manager:
            await self.stream_manager.stop()
        
        if self.batch_manager:
            await self.batch_manager.cleanup()
        
        logger.info("Data Ingestion Service cleaned up")


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
    service = DataIngestionService(
        vault_client=vault_client,
        consul_client=consul_client
    )
    
    # Create app with common setup
    app = create_data_intelligence_app(
        service_metadata=SERVICE_METADATA,
        service_instance=service,
        title="Data Ingestion Service API",
        include_common_middleware=True
    )
    
    # Add custom middleware
    app.middleware("http")(error_handler_middleware)
    app.middleware("http")(logging_middleware)
    
    # Include API routers
    app.include_router(ingestion.router, prefix="/api/v1", tags=["ingestion"])
    app.include_router(schemas.router, prefix="/api/v1/schemas", tags=["schemas"])
    app.include_router(connectors.router, tags=["connectors"])
    app.include_router(health.router, prefix="/api/v1", tags=["health"])
    app.include_router(metrics.router, prefix="/api/v1", tags=["metrics"])
    
    # Add root endpoint
    @app.get("/")
    async def root():
        return {
            "service": SERVICE_METADATA.name,
            "version": SERVICE_METADATA.version,
            "status": "running",
            "endpoints": {
                "cdc": "/api/v1/cdc/sources",
                "streams": "/api/v1/streams",
                "batch": "/api/v1/batch",
                "schemas": "/api/v1/schemas",
                "connectors": "/api/v1/connectors",
                "health": "/api/v1/health",
                "metrics": "/api/v1/metrics"
            }
        }
    
    return app


# Create app instance
app = create_app()


if __name__ == "__main__":
    port = int(os.getenv("SERVICE_PORT", settings.service_port))
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=True,
        log_level=settings.log_level.lower()
    ) 