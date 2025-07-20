"""Dependency injection for Trading Core Service."""

from typing import Optional
from fastapi import Depends, HTTPException, Header, status
import logging

from .config import Settings
from .state import IgniteStateManager
from .events import FlinkEventProcessor
from .core import (
    MatchingEngine, MatchingAlgorithm,
    OrderManager, PositionManager, MarketManager
)


logger = logging.getLogger(__name__)


# Global instances (would be properly initialized in lifespan)
_settings: Optional[Settings] = None
_state_manager: Optional[IgniteStateManager] = None
_event_processor: Optional[FlinkEventProcessor] = None
_matching_engine: Optional[MatchingEngine] = None
_order_manager: Optional[OrderManager] = None
_position_manager: Optional[PositionManager] = None
_market_manager: Optional[MarketManager] = None


def get_settings() -> Settings:
    """Get application settings."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def get_state_manager() -> IgniteStateManager:
    """Get Ignite state manager."""
    global _state_manager
    if _state_manager is None:
        settings = get_settings()
        _state_manager = IgniteStateManager(settings)
    return _state_manager


def get_event_processor() -> FlinkEventProcessor:
    """Get Flink event processor."""
    global _event_processor
    if _event_processor is None:
        settings = get_settings()
        _event_processor = FlinkEventProcessor(settings)
    return _event_processor


def get_matching_engine() -> MatchingEngine:
    """Get matching engine."""
    global _matching_engine
    if _matching_engine is None:
        _matching_engine = MatchingEngine(
            state_manager=get_state_manager(),
            event_processor=get_event_processor(),
            algorithm=MatchingAlgorithm.PRICE_TIME
        )
    return _matching_engine


def get_order_manager() -> OrderManager:
    """Get order manager."""
    global _order_manager
    if _order_manager is None:
        _order_manager = OrderManager(
            state_manager=get_state_manager(),
            matching_engine=get_matching_engine(),
            event_processor=get_event_processor()
        )
    return _order_manager


def get_position_manager() -> PositionManager:
    """Get position manager."""
    global _position_manager
    if _position_manager is None:
        _position_manager = PositionManager(
            state_manager=get_state_manager(),
            event_processor=get_event_processor()
        )
    return _position_manager


def get_market_manager() -> MarketManager:
    """Get market manager."""
    global _market_manager
    if _market_manager is None:
        _market_manager = MarketManager(
            state_manager=get_state_manager(),
            event_processor=get_event_processor()
        )
    return _market_manager


async def get_current_user(
    x_user_id: Optional[str] = Header(None),
    authorization: Optional[str] = Header(None)
) -> str:
    """Get current user from headers."""
    # In production, this would validate JWT token
    # For now, use simple header check
    
    if x_user_id:
        return x_user_id
    
    if authorization and authorization.startswith("Bearer "):
        # Would decode JWT token here
        # For now, extract user ID from token
        token = authorization.replace("Bearer ", "")
        # Simplified - would validate token properly
        if token:
            return f"user_{token[:8]}"
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Not authenticated",
        headers={"WWW-Authenticate": "Bearer"},
    )


async def require_admin(
    current_user: str = Depends(get_current_user)
) -> None:
    """Require admin privileges."""
    # In production, would check user roles
    # For now, simple check
    if not current_user.startswith("admin"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )


async def verify_ws_token(token: Optional[str]) -> Optional[str]:
    """Verify WebSocket token and return user ID."""
    if not token:
        return None
    
    # Would validate token properly
    # For now, simple extraction
    if token.startswith("ws_"):
        return f"user_{token[3:11]}"
    
    return None


def init_dependencies(
    settings: Settings,
    state_manager: IgniteStateManager,
    event_processor: FlinkEventProcessor,
    matching_engine: MatchingEngine,
    order_manager: OrderManager,
    position_manager: PositionManager,
    market_manager: MarketManager
):
    """Initialize global dependencies."""
    global _settings, _state_manager, _event_processor
    global _matching_engine, _order_manager, _position_manager, _market_manager
    
    _settings = settings
    _state_manager = state_manager
    _event_processor = event_processor
    _matching_engine = matching_engine
    _order_manager = order_manager
    _position_manager = position_manager
    _market_manager = market_manager
    
    logger.info("Dependencies initialized")


async def cleanup_dependencies():
    """Clean up dependencies."""
    global _state_manager, _event_processor
    
    if _state_manager:
        await _state_manager.disconnect()
    
    if _event_processor:
        await _event_processor.stop()
    
    logger.info("Dependencies cleaned up") 