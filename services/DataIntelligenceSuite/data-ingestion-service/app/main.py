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

# Import new event backends and lakehouse clients
from data_intelligence_common.core.events.backends import (
    PulsarEventBackend,
    KafkaEventBackend
)
from data_intelligence_common.core.lakehouse import (
    IcebergClient,
    DeltaClient,
    LakehouseManager,
    TableDefinition,
    TableSchema,
    PartitionSpec,
    DataType
)

from app.core.config import settings
from app.core.cdc_manager import CDCManager
from app.core.stream_ingestion import StreamIngestionManager
from app.core.batch_ingestion import BatchIngestionManager
from app.core.schema_registry import SchemaRegistry
from app.core.connector_manager import ConnectorManager
from app.core.medallion_architecture import MedallionArchitectureManager
from app.core.lifecycle_manager import DataLifecycleManager
from app.api import ingestion, schemas, health, metrics, connectors, lake, dependencies
from app.middleware.error_handler import error_handler_middleware
from app.middleware.logging import logging_middleware

# Configure logging
logger = StructuredLogger.get_logger(__name__)

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="data-ingestion-service",
    version="2.0.0",
    description="Unified data ingestion service with medallion architecture, lifecycle management, and multiple source capabilities",
    dependencies=["seatunnel", "pulsar", "minio", "cassandra", "ignite", "spark", "delta"],
    health_checks=["cdc", "stream", "batch", "schema_registry", "medallion", "lifecycle"],
    capabilities=["cdc", "streaming", "batch", "schema-registry", "medallion-architecture", "data-lifecycle", "connectors"],
    data_sources=["postgres", "mysql", "mongodb", "pulsar", "kafka", "files", "apis", "webhooks"],
    data_outputs=["data-lake", "hot-storage", "stream-topics", "bronze-layer", "silver-layer", "gold-layer"]
)

# Global instances
cdc_manager: Optional[CDCManager] = None
stream_manager: Optional[StreamIngestionManager] = None
batch_manager: Optional[BatchIngestionManager] = None
schema_registry: Optional[SchemaRegistry] = None
connector_manager: Optional[ConnectorManager] = None
medallion_manager: Optional[MedallionArchitectureManager] = None
lifecycle_manager: Optional[DataLifecycleManager] = None

# New lakehouse and event backend instances
lakehouse_manager: Optional[LakehouseManager] = None
iceberg_client: Optional[IcebergClient] = None
delta_client: Optional[DeltaClient] = None
pulsar_backend: Optional[PulsarEventBackend] = None
kafka_backend: Optional[KafkaEventBackend] = None


class DataIngestionService(DataIntelligenceBaseService):
    """Data Ingestion Service implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        self.cdc_manager = None
        self.stream_manager = None
        self.batch_manager = None
        self.schema_registry = None
        self.connector_manager = None
        self.medallion_manager = None
        self.lifecycle_manager = None
        self.lakehouse_manager = None
        self.iceberg_client = None
        self.delta_client = None
        self.pulsar_backend = None
        self.kafka_backend = None
    
    async def initialize_service(self):
        """Initialize service-specific components"""
        global cdc_manager, stream_manager, batch_manager, schema_registry, connector_manager, medallion_manager, lifecycle_manager
        global lakehouse_manager, iceberg_client, delta_client, pulsar_backend, kafka_backend
        
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
        
        # Initialize Spark session for medallion architecture
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("DataIngestionService") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
            .config("spark.hadoop.fs.s3a.access.key", settings.minio_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", settings.minio_secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{settings.minio_endpoint}") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
        
        medallion_manager = MedallionArchitectureManager(settings, schema_registry, spark)
        self.medallion_manager = medallion_manager
        
        lifecycle_manager = DataLifecycleManager(settings)
        await lifecycle_manager.initialize()
        self.lifecycle_manager = lifecycle_manager
        
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
        
        # Initialize lakehouse clients
        logger.info("Initializing lakehouse clients...")
        
        # Initialize Iceberg client
        iceberg_config = {
            "catalog_type": "hive",
            "catalog_uri": settings.hive_metastore_uri if hasattr(settings, 'hive_metastore_uri') else "thrift://localhost:9083",
            "warehouse_path": f"s3a://lakehouse/iceberg",
            "s3_endpoint": f"http://{settings.minio_endpoint}",
            "s3_access_key": settings.minio_access_key,
            "s3_secret_key": settings.minio_secret_key
        }
        iceberg_client = IcebergClient(iceberg_config)
        await iceberg_client.connect()
        self.iceberg_client = iceberg_client
        
        # Initialize Delta client
        delta_config = {
            "spark_session": spark,  # Use the existing Spark session
            "warehouse_path": f"s3a://lakehouse/delta",
            "s3_endpoint": f"http://{settings.minio_endpoint}",
            "s3_access_key": settings.minio_access_key,
            "s3_secret_key": settings.minio_secret_key
        }
        delta_client = DeltaClient(delta_config)
        await delta_client.connect()
        self.delta_client = delta_client
        
        # Initialize lakehouse manager
        lakehouse_manager = LakehouseManager({
            "default_format": "iceberg",
            "iceberg_client": iceberg_client,
            "delta_client": delta_client,
            "metadata_store": schema_registry
        })
        self.lakehouse_manager = lakehouse_manager
        
        # Initialize event backends
        logger.info("Initializing event backends...")
        
        # Initialize Pulsar backend
        if hasattr(settings, 'pulsar_url'):
            pulsar_config = {
                "service_url": settings.pulsar_url,
                "authentication": None,  # Will be configured from Vault if needed
                "operation_timeout_seconds": 30
            }
            pulsar_backend = PulsarEventBackend(pulsar_config)
            await pulsar_backend.connect()
            self.pulsar_backend = pulsar_backend
            
            # Update stream manager to use new backend
            stream_manager.add_backend("pulsar", pulsar_backend)
        
        # Initialize Kafka backend
        if hasattr(settings, 'kafka_bootstrap_servers'):
            kafka_config = {
                "bootstrap_servers": settings.kafka_bootstrap_servers,
                "security_protocol": "PLAINTEXT",
                "consumer_group": "data-ingestion-service"
            }
            kafka_backend = KafkaEventBackend(kafka_config)
            await kafka_backend.connect()
            self.kafka_backend = kafka_backend
            
            # Update stream manager to use new backend
            stream_manager.add_backend("kafka", kafka_backend)
        
        # Update medallion manager to use lakehouse clients
        medallion_manager.set_lakehouse_manager(lakehouse_manager)
        
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
        
        # Inject medallion and lifecycle managers
        dependencies.medallion_manager = medallion_manager
        dependencies.lifecycle_manager = lifecycle_manager
        dependencies.lakehouse_manager = lakehouse_manager
        
        # Register health checks
        self.health_manager.register_check("cdc", cdc_manager.health_check)
        self.health_manager.register_check("stream", stream_manager.health_check)
        self.health_manager.register_check("batch", batch_manager.health_check)
        self.health_manager.register_check("schema_registry", schema_registry.health_check)
        self.health_manager.register_check("connectors", lambda: {"status": "healthy", "active_connectors": len(connector_manager.connectors)})
        self.health_manager.register_check("medallion", lambda: {"status": "healthy", "layers": ["bronze", "silver", "gold"]})
        self.health_manager.register_check("lifecycle", lambda: {"status": "healthy", "tiers": ["hot", "warm", "cold"]})
        
        logger.info("Data Ingestion Service initialized successfully")
    
    async def cleanup_service(self):
        """Cleanup service-specific components"""
        logger.info("Cleaning up Data Ingestion Service...")
        
        # Cleanup event backends
        if self.pulsar_backend:
            await self.pulsar_backend.disconnect()
        
        if self.kafka_backend:
            await self.kafka_backend.disconnect()
        
        # Cleanup lakehouse clients
        if self.iceberg_client:
            await self.iceberg_client.close()
        
        if self.delta_client:
            await self.delta_client.close()
        
        if self.lifecycle_manager:
            await self.lifecycle_manager.cleanup()
        
        if self.medallion_manager and hasattr(self.medallion_manager, 'spark'):
            self.medallion_manager.spark.stop()
        
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
    app.include_router(lake.router, tags=["data-lake"])
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
                "lake": "/api/v1/lake",
                "lifecycle": "/api/v1/lake/lifecycle",
                "health": "/api/v1/health",
                "metrics": "/api/v1/metrics"
            },
            "features": {
                "medallion_architecture": True,
                "data_lifecycle": True,
                "external_connectors": True,
                "schema_evolution": True
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