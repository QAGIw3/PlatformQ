"""
Data Catalog Hub - Main Entry Point

Simplified entry point using the application factory pattern.
"""

import uvicorn
from dependency_injector import containers

from app.application import create_application
from app.core.config import settings
from app.core.container import Container
from platformq_shared.logging import setup_logging

# Setup logging
setup_logging(service_name="data-catalog-hub")

# Create the application
app = create_application()

# Wire the dependency injection container
containers.wire(
    modules=[
        "app.api.v1.routers.entities",
        "app.api.v1.routers.schemas",
        "app.api.v1.routers.lineage",
        "app.api.v1.routers.classifications",
        "app.api.v1.routers.glossary",
        "app.api.v1.routers.search",
        "app.api.v1.dependencies",
        "app.event_handlers",
    ],
    packages=["app"]
)

if __name__ == "__main__":
    uvicorn.run(
        "app.main_new:app",
        host="0.0.0.0",
        port=settings.SERVICE_PORT,
        reload=settings.ENVIRONMENT == "development",
        log_config=None  # Use our custom logging
    ) 