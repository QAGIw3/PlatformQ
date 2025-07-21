"""
Network Bandwidth Market Service API Routes
"""
from .paths import router as paths_router
from .bandwidth import router as bandwidth_router
from .circuits import router as circuits_router
from .pricing import router as pricing_router
from .latency import router as latency_router

__all__ = [
    "paths_router",
    "bandwidth_router",
    "circuits_router",
    "pricing_router",
    "latency_router"
] 