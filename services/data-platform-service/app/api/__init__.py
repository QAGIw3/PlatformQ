"""
API routers for Data Platform Service
"""

from . import (
    query,
    catalog,
    governance,
    quality,
    lineage,
    lake,
    pipelines,
    admin
)

__all__ = [
    "query",
    "catalog",
    "governance",
    "quality",
    "lineage",
    "lake",
    "pipelines",
    "admin"
]
