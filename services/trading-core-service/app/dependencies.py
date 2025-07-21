"""Application dependencies for dependency injection."""

import logging
from typing import Optional

from fastapi import Depends, HTTPException, Request

from .core import OrderManager, PositionManager, MarketManager, MatchingEngine
from .state import IgniteStateManager
from .events import FlinkEventProcessor
from .integrations import DerivativesAdapter, ComputeMarketAdapter


logger = logging.getLogger(__name__)


# Global instances (initialized in main.py)
_order_manager: Optional[OrderManager] = None
_position_manager: Optional[PositionManager] = None
_market_manager: Optional[MarketManager] = None
_matching_engine: Optional[MatchingEngine] = None
_state_manager: Optional[IgniteStateManager] = None
_event_processor: Optional[FlinkEventProcessor] = None
_derivatives_adapter: Optional[DerivativesAdapter] = None
_compute_adapter: Optional[ComputeMarketAdapter] = None


def init_dependencies(
    order_manager: OrderManager,
    position_manager: PositionManager,
    market_manager: MarketManager,
    matching_engine: MatchingEngine,
    state_manager: IgniteStateManager,
    event_processor: FlinkEventProcessor,
    derivatives_adapter: DerivativesAdapter,
    compute_adapter: ComputeMarketAdapter
):
    """Initialize global dependencies."""
    global _order_manager, _position_manager, _market_manager
    global _matching_engine, _state_manager, _event_processor
    global _derivatives_adapter, _compute_adapter
    
    _order_manager = order_manager
    _position_manager = position_manager
    _market_manager = market_manager
    _matching_engine = matching_engine
    _state_manager = state_manager
    _event_processor = event_processor
    _derivatives_adapter = derivatives_adapter
    _compute_adapter = compute_adapter


def get_order_manager() -> OrderManager:
    """Get order manager instance."""
    if not _order_manager:
        raise HTTPException(status_code=503, detail="Order manager not initialized")
    return _order_manager


def get_position_manager() -> PositionManager:
    """Get position manager instance."""
    if not _position_manager:
        raise HTTPException(status_code=503, detail="Position manager not initialized")
    return _position_manager


def get_market_manager() -> MarketManager:
    """Get market manager instance."""
    if not _market_manager:
        raise HTTPException(status_code=503, detail="Market manager not initialized")
    return _market_manager


def get_matching_engine() -> MatchingEngine:
    """Get matching engine instance."""
    if not _matching_engine:
        raise HTTPException(status_code=503, detail="Matching engine not initialized")
    return _matching_engine


def get_state_manager() -> IgniteStateManager:
    """Get state manager instance."""
    if not _state_manager:
        raise HTTPException(status_code=503, detail="State manager not initialized")
    return _state_manager


def get_event_processor() -> FlinkEventProcessor:
    """Get event processor instance."""
    if not _event_processor:
        raise HTTPException(status_code=503, detail="Event processor not initialized")
    return _event_processor


def get_derivatives_adapter() -> DerivativesAdapter:
    """Get derivatives adapter instance."""
    if not _derivatives_adapter:
        raise HTTPException(status_code=503, detail="Derivatives adapter not initialized")
    return _derivatives_adapter


def get_compute_adapter() -> ComputeMarketAdapter:
    """Get compute market adapter instance."""
    if not _compute_adapter:
        raise HTTPException(status_code=503, detail="Compute adapter not initialized")
    return _compute_adapter


def get_current_user(request: Request) -> dict:
    """Get current user from request headers."""
    # In production, this would validate JWT or session
    return {
        "user_id": request.headers.get("X-User-ID", "test_user"),
        "tenant_id": request.headers.get("X-Tenant-ID", "default")
    } 