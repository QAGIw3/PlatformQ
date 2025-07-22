"""Data Quality Service API module"""

from .quality_api import router as quality_router
from .rules_api import router as rules_router
from .monitoring_api import router as monitoring_router
from .profiling_api import router as profiling_router

__all__ = [
    'quality_router',
    'rules_router', 
    'monitoring_router',
    'profiling_router'
] 