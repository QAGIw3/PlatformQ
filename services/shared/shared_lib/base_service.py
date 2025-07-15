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

KUBE_SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"

def setup_structured_logging():
    """Configures logging to output in a structured JSON format."""
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
    """Configures OpenTelemetry for the application."""
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
    Factory function to create a standardized FastAPI application.
    This version uses the ConfigLoader for discovery and configuration.
    """
    
    # --- Setup Logging ---
    setup_structured_logging()

    # --- Load Config & Secrets ---
    config_loader = ConfigLoader()
    settings = config_loader.load_settings()
    
    # --- Create App ---
    app = FastAPI(title=service_name)

    # --- Setup Observability ---
    setup_observability(app, service_name)

    # --- Dependency Overrides ---
    app.dependency_overrides[shared_security.get_db_session_dependency] = db_session_dependency
    app.dependency_overrides[shared_security.get_api_key_crud_dependency] = api_key_crud_dependency
    app.dependency_overrides[shared_security.get_user_crud_dependency] = user_crud_dependency
    app.dependency_overrides[shared_security.get_password_verifier_dependency] = password_verifier_dependency

    @app.on_event("startup")
    def on_startup():
        # --- Register with Consul ---
        service_id = f"{service_name}-{socket.gethostname()}"
        app.state.service_id = service_id
        config_loader.consul_client.agent.service.register(
            name=service_name,
            service_id=service_id,
            address=socket.gethostname(),
            port=80, # Assuming all services run on port 80 in their container
            tags=["api"],
            check=consul.Check.http(f"http://{socket.gethostname()}:80/health", interval="10s")
        )
        logger.info(f"Service '{service_name}' registered with Consul (ID: {service_id})")

        # Set up Cassandra
        db_manager = CassandraSessionManager(
            hosts=settings["CASSANDRA_HOSTS"],
            port=settings["CASSANDRA_PORT"],
            user=settings["CASSANDRA_USER"],
            password=settings["CASSANDRA_PASSWORD"]
        )
        db_manager.connect()
        app.state.db_manager = db_manager

        # Set up Pulsar event publisher
        publisher = EventPublisher(pulsar_url=settings["PULSAR_URL"])
        publisher.connect()
        app.state.event_publisher = publisher

    @app.on_event("shutdown")
    def on_shutdown():
        logger.info(f"Deregistering service '{app.state.service_id}' from Consul.")
        config_loader.consul_client.agent.service.deregister(service_id=app.state.service_id)
        app.state.db_manager.close()
        app.state.event_publisher.close()
        
    @app.get("/health")
    def health_check():
        return {"status": "ok"}

    return app
