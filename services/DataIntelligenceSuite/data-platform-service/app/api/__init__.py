"""
API routers for Data Platform Service
"""

from .query import router as query_router
from .catalog import router as catalog_router
from .governance import router as governance_router
from .quality import router as quality_router
from .lineage import router as lineage_router
from .lake import router as lake_router
from .pipelines import router as pipelines_router
from .admin import router as admin_router
from .integrations import router as integrations_router
from .features import router as features_router

__all__ = [
    "query_router",
    "catalog_router", 
    "governance_router",
    "quality_router",
    "lineage_router",
    "lake_router",
    "pipelines_router",
    "admin_router",
    "integrations_router",
    "features_router"
]
