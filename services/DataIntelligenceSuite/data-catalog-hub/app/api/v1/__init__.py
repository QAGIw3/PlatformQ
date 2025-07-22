"""
API Version 1 Module

Provides the v1 API implementation following clean architecture principles.
"""

from fastapi import APIRouter

from app.api.v1.routers import (
    entities,
    schemas,
    lineage,
    classifications,
    glossary,
    search
)


def create_api_router() -> APIRouter:
    """
    Create and configure the v1 API router.
    
    Returns:
        Configured APIRouter with all v1 endpoints
    """
    api_router = APIRouter()
    
    # Include all routers
    api_router.include_router(
        entities.router,
        prefix="/entities",
        tags=["entities"]
    )
    
    api_router.include_router(
        schemas.router,
        prefix="/schemas",
        tags=["schemas"]
    )
    
    api_router.include_router(
        lineage.router,
        prefix="/lineage",
        tags=["lineage"]
    )
    
    api_router.include_router(
        classifications.router,
        prefix="/classifications",
        tags=["classifications"]
    )
    
    api_router.include_router(
        glossary.router,
        prefix="/glossary",
        tags=["glossary"]
    )
    
    api_router.include_router(
        search.router,
        prefix="/search",
        tags=["search"]
    )
    
    return api_router


__all__ = ['create_api_router'] 