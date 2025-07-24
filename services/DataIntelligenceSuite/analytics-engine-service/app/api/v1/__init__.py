"""
Analytics Engine Service API v1

Export all routers for the analytics engine service.
"""

from .analytics import router as analytics_router
from .query import router as query_router
from .streaming import router as streaming_router

__all__ = [
    'analytics_router',
    'query_router', 
    'streaming_router'
]
