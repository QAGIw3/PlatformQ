from fastapi import FastAPI
from typing import Callable, Optional, List
import logging
import logging.config
import socket
import json

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.grpc import GrpcInstrumentorServer
from opentelemetry.sdk.resources import Resource
import asyncio
from concurrent import futures
import grpc
import os

from .db import CassandraSessionManager
from .event_publisher import EventPublisher
from . import security as shared_security
from .config import get_settings, Settings
from .error_handling import add_error_handlers, ErrorMiddleware
from .event_framework import EventProcessor

logger = logging.getLogger(__name__)

# The path to the projected service account token in a Kubernetes pod.
# This is the standard path for in-cluster authentication.
KUBE_SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"

def setup_structured_logging():
    """
    Configures Python's logging to output logs in a structured JSON format.
    This is essential for modern log aggregation systems like Loki, allowing
    for powerful, queryable logs with key-value pairs.
    """
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
                "format": "%(asctime)s %(name)s %(levelname)s %(message)s"
            }
        },
        "handlers": {
            "json": {
                "class": "logging.StreamHandler",
                "formatter": "json"
            }
        },
        "root": {
            "handlers": ["json"],
            "level": "INFO"
        }
    }
    logging.config.dictConfig(config)

def setup_observability(app: FastAPI, service_name: str, otel_endpoint: str, instrument_grpc: bool = False):
    """
    Set up distributed tracing using OpenTelemetry.
    This allows us to trace requests across all microservices, visualize
    the entire request flow, and pinpoint performance bottlenecks.
    """
    resource = Resource.create({
        "service.name": service_name,
        "service.instance.id": socket.gethostname(),
    })

    trace.set_tracer_provider(TracerProvider(resource=resource))
    tracer_provider = trace.get_tracer_provider()

    # Configure the OTLP exporter, pointing to our Jaeger or OTLP collector.
    otlp_exporter = OTLPSpanExporter(endpoint=otel_endpoint, insecure=True)
    span_processor = BatchSpanProcessor(otlp_exporter)
    tracer_provider.add_span_processor(span_processor)

    # Automatically instrument the FastAPI app.
    FastAPIInstrumentor.instrument_app(app)

    # Optionally instrument gRPC if the service uses it.
    if instrument_grpc:
        GrpcInstrumentorServer().instrument()

def get_kubernetes_token():
    """
    Read the Kubernetes service account token if running in a pod.
    This is used for in-cluster authentication to other services.
    """
    if os.path.exists(KUBE_SA_TOKEN_PATH):
        with open(KUBE_SA_TOKEN_PATH, 'r') as f:
            return f.read().strip()
    return None


def create_base_app(
    service_name: str,
    db_session_dependency: Callable,
    api_key_crud_dependency: Callable,
    user_crud_dependency: Callable,
    password_verifier_dependency: Callable,
    grpc_servicer: object = None,
    grpc_add_servicer_func: Callable = None,
    grpc_port: int = 50051,
    event_processors: Optional[List[EventProcessor]] = None,
    enable_error_handlers: bool = True,
    enable_cors: bool = True,
    cors_origins: List[str] = ["*"],
) -> FastAPI:
    """
    Factory function to create a standardized, production-ready FastAPI application.
    
    This is the heart of our "Golden Path". It encapsulates all the boilerplate
    required to set up a new microservice, ensuring that every service on the
    platform is automatically configured with:
    - Structured Logging
    - OpenTelemetry Tracing
    - Optional gRPC server
    - Centralized Configuration from Consul & Vault
    - Database & Event Publisher connections
    - Standardized Security Dependency wiring
    - Error handling framework
    - Event processors
    """
    
    # --- Step 1: Load All External Configuration ---
    settings = get_settings()

    # --- Step 2: Setup Core Application Concerns ---
    has_grpc = grpc_servicer and grpc_add_servicer_func
    setup_structured_logging()
    app = FastAPI(title=service_name, version="1.0.0")
    
    # --- Step 3: Add Middleware ---
    if enable_cors:
        from fastapi.middleware.cors import CORSMiddleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    
    # Add error handling middleware
    if enable_error_handlers:
        app.add_middleware(ErrorMiddleware, service_name=service_name)
        add_error_handlers(app, service_name)
    
    # --- Step 4: Setup Observability ---
    setup_observability(
        app, 
        service_name, 
        otel_endpoint=settings.otel_exporter_otlp_endpoint, 
        instrument_grpc=has_grpc
    )

    # --- Step 5: Wire up Shared Security Dependencies ---
    # This is a critical step for our "trusted subsystem" model.
    # We use FastAPI's dependency_overrides to "inject" the concrete,
    # service-specific CRUD functions into the generic, shared security
    # dependencies. This allows the shared security logic to remain generic
    # while being used by services with different data models.
    app.dependency_overrides[shared_security.get_db_session_dependency] = db_session_dependency
    app.dependency_overrides[shared_security.get_api_key_crud_dependency] = api_key_crud_dependency
    app.dependency_overrides[shared_security.get_user_crud_dependency] = user_crud_dependency
    app.dependency_overrides[shared_security.get_password_verifier_dependency] = password_verifier_dependency

    # --- Step 6: Initialize Core Components ---
    # Database connection (Cassandra)
    cassandra_manager = CassandraSessionManager(
        contact_points=settings.cassandra_contact_points.split(','),
        keyspace=settings.cassandra_keyspace
    )
    app.state.cassandra_manager = cassandra_manager

    # Event publisher (Pulsar)
    event_publisher = EventPublisher(pulsar_url=settings.pulsar_url)
    event_publisher.connect()
    app.state.event_publisher = event_publisher
    
    # Store event processors
    app.state.event_processors = event_processors or []

    # --- Step 7: Define Application Lifecycle Handlers ---
    @app.on_event("startup")
    async def startup():
        """
        This runs when the application starts.
        Connect to external systems and start background tasks.
        """
        logger.info(f"{service_name} is starting up")
        
        # Connect to Cassandra
        cassandra_manager.connect()
        logger.info("Connected to Cassandra")

        # Optional: Start gRPC server in a background thread
        if has_grpc:
            def run_grpc_server():
                server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
                grpc_add_servicer_func(grpc_servicer, server)
                server.add_insecure_port(f'[::]:{grpc_port}')
                server.start()
                logger.info(f"gRPC server started on port {grpc_port}")
                server.wait_for_termination()

            import threading
            grpc_thread = threading.Thread(target=run_grpc_server, daemon=True)
            grpc_thread.start()
            app.state.grpc_thread = grpc_thread
            
        # Start event processors
        for processor in app.state.event_processors:
            await processor.start()
            logger.info(f"Started event processor: {processor.__class__.__name__}")

    @app.on_event("shutdown")
    async def shutdown():
        """
        This runs when the application shuts down.
        Gracefully disconnect from external systems.
        """
        logger.info(f"{service_name} is shutting down")
        
        # Stop event processors
        for processor in app.state.event_processors:
            await processor.stop()
            logger.info(f"Stopped event processor: {processor.__class__.__name__}")

        # Disconnect from Cassandra
        cassandra_manager.disconnect()
        logger.info("Disconnected from Cassandra")

        # Disconnect from Pulsar
        event_publisher.disconnect()
        logger.info("Disconnected from Pulsar")
        
        # Close all service clients
        from .service_client import close_all_clients
        await close_all_clients()
        logger.info("Closed all service clients")

    # --- Step 8: Add Common Endpoints ---
    @app.get("/health")
    async def health():
        """
        Standard health check endpoint for Kubernetes liveness probes.
        """
        return {"status": "healthy", "service": service_name}

    @app.get("/ready")
    async def ready():
        """
        Standard readiness check endpoint for Kubernetes readiness probes.
        Checks if all dependencies are available.
        """
        checks = {"service": service_name, "status": "ready"}
        
        # Check Cassandra
        try:
            session = cassandra_manager.get_session()
            session.execute("SELECT now() FROM system.local")
            checks["cassandra"] = "ready"
        except Exception as e:
            checks["cassandra"] = f"not ready: {str(e)}"
            checks["status"] = "not ready"
            
        # Check Pulsar
        try:
            # Simple check - verify client is created
            if event_publisher.client:
                checks["pulsar"] = "ready"
            else:
                checks["pulsar"] = "not ready: client not initialized"
                checks["status"] = "not ready"
        except Exception as e:
            checks["pulsar"] = f"not ready: {str(e)}"
            checks["status"] = "not ready"
            
        return checks
        
    @app.get("/metrics")
    async def metrics():
        """
        Prometheus metrics endpoint.
        """
        # This would typically use prometheus_client to expose metrics
        # For now, return basic metrics
        return {
            "service": service_name,
            "uptime_seconds": 0,  # Would track actual uptime
            "request_count": 0,   # Would track actual requests
            "error_count": 0      # Would track actual errors
        }

    logger.info(f"{service_name} app created successfully")
    return app
