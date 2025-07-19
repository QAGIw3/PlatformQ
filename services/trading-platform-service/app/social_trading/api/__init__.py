"""
Social Trading API Module
"""

from app.api.strategies import router as strategies_router
from app.api.copy_trading import router as copy_trading_router
from app.api.reputation import router as reputation_router
from app.api.analytics import router as analytics_router
from app.api.social import router as social_router
from app.api.strategy_markets import router as strategy_markets_router

__all__ = [
    "strategies_router",
    "copy_trading_router", 
    "reputation_router",
    "analytics_router",
    "social_router",
    "strategy_markets_router"
] 