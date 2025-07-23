"""
API Dependencies

FastAPI dependency injection functions.
"""

from typing import Optional
from fastapi import Depends, HTTPException, Header
from dependency_injector.wiring import inject, Provide

from app.core.container import Container
from app.services.catalog import (
    EntityService,
    SchemaService,
    LineageService,
    ClassificationService,
    GlossaryService
)
from app.services.search import UnifiedSearchService


# User context dependencies
async def get_current_user(
    authorization: Optional[str] = Header(None)
) -> dict:
    """Extract current user from authorization header"""
    # TODO: Implement proper authentication
    return {
        "id": "user123",
        "name": "Test User",
        "roles": ["user"],
        "tenant_id": "default"
    }


async def require_admin(
    user: dict = Depends(get_current_user)
) -> dict:
    """Require admin role"""
    if "admin" not in user.get("roles", []):
        raise HTTPException(status_code=403, detail="Admin role required")
    return user


# Service dependencies
@inject
async def get_entity_service(
    container: Container = Depends(Provide[Container])
) -> EntityService:
    """Get entity service instance"""
    return container.entity_service()


@inject
async def get_schema_service(
    container: Container = Depends(Provide[Container])
) -> SchemaService:
    """Get schema service instance"""
    return container.schema_service()


@inject
async def get_lineage_service(
    container: Container = Depends(Provide[Container])
) -> LineageService:
    """Get lineage service instance"""
    return container.lineage_service()


@inject
async def get_classification_service(
    container: Container = Depends(Provide[Container])
) -> ClassificationService:
    """Get classification service instance"""
    return container.classification_service()


@inject
async def get_glossary_service(
    container: Container = Depends(Provide[Container])
) -> GlossaryService:
    """Get glossary service instance"""
    return container.glossary_service()


@inject
async def get_search_service(
    container: Container = Depends(Provide[Container])
) -> UnifiedSearchService:
    """Get unified search service instance"""
    return container.unified_search_service() 