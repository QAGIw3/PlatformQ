from fastapi import FastAPI
from typing import Callable
import logging
import logging.config
import socket
import json

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource

from .db import CassandraSessionManager
from .event_publisher import EventPublisher
from . import security as shared_security
from .config import ConfigLoader

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


def setup_observability(app: FastAPI, service_name: str):
    """
    Configures OpenTelemetry for the application.
    This function sets up a "TracerProvider" which manages the lifecycle of traces.
    It exports these traces in OTLP format to our OpenTelemetry Collector.
    Finally, it auto-instruments the FastAPI application to trace all incoming requests.
    """
    resource = Resource(attributes={
        "service.name": service_name
    })
    
    # Set up tracing
    provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="otel-collector:4317", insecure=True))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    
    # Instrument FastAPI
    FastAPIInstrumentor.instrument_app(app)


def create_base_app(
    service_name: str,
    db_session_dependency: Callable,
    api_key_crud_dependency: Callable,
    user_crud_dependency: Callable,
    password_verifier_dependency: Callable,
) -> FastAPI:
    """
    Factory function to create a standardized, production-ready FastAPI application.
    
    This is the heart of our "Golden Path". It encapsulates all the boilerplate
    required to set up a new microservice, ensuring that every service on the
    platform is automatically configured with:
    - Structured Logging
    - OpenTelemetry Tracing
    - Centralized Configuration from Consul & Vault
    - Database & Event Publisher connections
    - Standardized Security Dependency wiring
    """
    
    # --- Step 1: Setup Core Application Concerns ---
    setup_structured_logging()
    app = FastAPI(title=service_name)
    setup_observability(app, service_name)

    # --- Step 2: Load All External Configuration ---
    # This uses our shared ConfigLoader to connect to Consul and Vault
    # and load all necessary runtime configuration and secrets.
    config_loader = ConfigLoader()
    settings = config_loader.load_settings()
    
    # --- Step 3: Wire up Shared Security Dependencies ---
    # This is a critical step for our "trusted subsystem" model.
    # We use FastAPI's dependency_overrides to "inject" the concrete,
    # service-specific CRUD functions into the generic, shared security
    # dependencies. This allows the shared security logic to remain generic
    # while being used by services with different data models.
    app.dependency_overrides[shared_security.get_db_session_dependency] = db_session_dependency
    app.dependency_overrides[shared_security.get_api_key_crud_dependency] = api_key_crud_dependency
    app.dependency_overrides[shared_security.get_user_crud_dependency] = user_crud_dependency
    app.dependency_overrides[shared_security.get_password_verifier_dependency] = password_verifier_dependency

    # --- Step 4: Manage Lifecycle Events ---
    @app.on_event("startup")
    def on_startup():
        """
        Handles application startup logic: service registration and connection pooling.
        """
        # --- Register with Consul for Service Discovery ---
        service_id = f"{service_name}-{socket.gethostname()}"
        app.state.service_id = service_id
        config_loader.consul_client.agent.service.register(
            name=service_name,
            service_id=service_id,
            address=socket.gethostname(),
            port=80,
            tags=["api"],
            check=consul.Check.http(f"http://{socket.gethostname()}:80/health", interval="10s")
        )
        logger.info(f"Service '{service_name}' registered with Consul (ID: {service_id})")

        # --- Initialize Connection Pools ---
        # We store the managers in the app.state to make them available
        # throughout the application's lifecycle, e.g., in dependencies.
        db_manager = CassandraSessionManager(
            hosts=settings["CASSANDRA_HOSTS"],
            port=settings["CASSANDRA_PORT"],
            user=settings["CASSANDRA_USER"],
            password=settings["CASSANDRA_PASSWORD"]
        )
        db_manager.connect()
        app.state.db_manager = db_manager

        publisher = EventPublisher(pulsar_url=settings["PULSAR_URL"])
        publisher.connect()
        app.state.event_publisher = publisher

    @app.on_event("shutdown")
    def on_shutdown():
        """
        Handles graceful shutdown: deregistering from Consul and closing connections.
        """
        logger.info(f"Deregistering service '{app.state.service_id}' from Consul.")
        config_loader.consul_client.agent.service.deregister(service_id=app.state.service_id)
        app.state.db_manager.close()
        app.state.event_publisher.close()
        
    @app.get("/health")
    def health_check():
        """A simple health check endpoint used by Consul and Kubernetes."""
        return {"status": "ok"}

    return app
