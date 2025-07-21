"""
Social Trading API Module

Aggregates all social trading API routers.
"""

from .social import router as social_router
from .automated_trading import router as automated_router
from .strategy_markets import router as strategy_markets_router

__all__ = [
    "social_router",
    "automated_router", 
    "strategy_markets_router",
] 