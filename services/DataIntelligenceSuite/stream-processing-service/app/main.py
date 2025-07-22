"""
Stream Processing Service

Unified service for all real-time stream processing needs,
consolidating multiple Flink jobs into a single, manageable service.
"""

import os
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
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
from app.core.job_manager import JobManager
from app.core.pattern_library import PatternLibrary
from app.core.state_manager import StateManager
from app.api import jobs, patterns, monitoring, health

# Configure logging
logger = StructuredLogger.get_logger(__name__)

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="stream-processing-service",
    version="1.0.0",
    description="Unified real-time stream processing with Apache Flink",
    dependencies=["flink", "pulsar", "cassandra", "elasticsearch", "ignite"],
    health_checks=["flink", "job_manager", "state_manager"],
    capabilities=["streaming", "cep", "analytics", "stateful-processing"],
    data_sources=["pulsar", "kafka"],
    data_outputs=["cassandra", "elasticsearch", "pulsar"],
    min_memory_mb=4096,
    min_cpu_cores=2
)

# Global instances
job_manager: Optional[JobManager] = None
pattern_library: Optional[PatternLibrary] = None
state_manager: Optional[StateManager] = None


class StreamProcessingService(DataIntelligenceBaseService):
    """Stream Processing Service implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        self.job_manager = None
        self.pattern_library = None
        self.state_manager = None
    
    async def initialize_service(self):
        """Initialize service-specific components"""
        global job_manager, pattern_library, state_manager
        
        logger.info("Initializing Stream Processing Service components...")
        
        # Get Flink configuration from Vault
        flink_config = await self.vault_consul.get_secret("streaming/flink")
        if flink_config:
            settings.flink_rest_url = flink_config.get("rest_url", settings.flink_rest_url)
            settings.flink_jobmanager_rpc_address = flink_config.get("jobmanager_address", settings.flink_jobmanager_rpc_address)
        
        # Get Pulsar credentials from Vault
        pulsar_creds = await self.vault_consul.get_secret("messaging/pulsar")
        if pulsar_creds:
            settings.pulsar_service_url = pulsar_creds.get("service_url", settings.pulsar_service_url)
            settings.pulsar_admin_url = pulsar_creds.get("admin_url", settings.pulsar_admin_url)
        
        # Get Cassandra credentials from Vault
        cassandra_creds = await self.vault_consul.get_database_credentials("cassandra")
        if cassandra_creds:
            os.environ["CASSANDRA_USERNAME"] = cassandra_creds.get("username", "")
            os.environ["CASSANDRA_PASSWORD"] = cassandra_creds.get("password", "")
        
        # Initialize components
        job_manager = JobManager(settings)
        await job_manager.start()
        self.job_manager = job_manager
        
        pattern_library = PatternLibrary(settings)
        await pattern_library.load_patterns()
        self.pattern_library = pattern_library
        
        state_manager = StateManager(settings)
        await state_manager.initialize()
        self.state_manager = state_manager
        
        # Inject dependencies into API routers
        jobs.job_manager = job_manager
        patterns.pattern_library = pattern_library
        monitoring.job_manager = job_manager
        monitoring.state_manager = state_manager
        health.job_manager = job_manager
        health.state_manager = state_manager
        
        # Register health checks
        self.health_manager.register_check("flink", self._check_flink_health, critical=True)
        self.health_manager.register_check("job_manager", job_manager.health_check)
        self.health_manager.register_check("state_manager", state_manager.health_check)
        
        logger.info("Stream Processing Service initialized successfully")
    
    async def cleanup_service(self):
        """Cleanup service-specific components"""
        logger.info("Cleaning up Stream Processing Service...")
        
        if self.job_manager:
            await self.job_manager.stop()
        
        if self.state_manager:
            await self.state_manager.cleanup()
        
        logger.info("Stream Processing Service cleaned up")
    
    async def _check_flink_health(self) -> bool:
        """Check Flink cluster health"""
        if not self.job_manager:
            return False
        return await self.job_manager.check_flink_health()


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
    service = StreamProcessingService(
        vault_client=vault_client,
        consul_client=consul_client
    )
    
    # Create app with common setup
    app = create_data_intelligence_app(
        service_metadata=SERVICE_METADATA,
        service_instance=service,
        title="Stream Processing Service API",
        include_common_middleware=True
    )
    
    # Include API routers
    app.include_router(jobs.router, prefix="/api/v1/jobs", tags=["jobs"])
    app.include_router(patterns.router, prefix="/api/v1/patterns", tags=["patterns"])
    app.include_router(monitoring.router, prefix="/api/v1/monitoring", tags=["monitoring"])
    app.include_router(health.router, prefix="/api/v1/health", tags=["health"])
    
    # Add root endpoint
    @app.get("/")
    async def root():
        return {
            "service": SERVICE_METADATA.name,
            "version": SERVICE_METADATA.version,
            "status": "running",
            "endpoints": {
                "jobs": "/api/v1/jobs",
                "patterns": "/api/v1/patterns",
                "monitoring": "/api/v1/monitoring",
                "health": "/api/v1/health"
            }
        }
    
    return app


# Create app instance
app = create_app()


if __name__ == "__main__":
    port = int(os.getenv("SERVICE_PORT", settings.api_port))
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=True
    ) 