"""Futures Service dependencies for FastAPI."""

from typing import Annotated, Optional

from fastapi import Depends, Header, HTTPException

from app.config import Settings
from app.cache.ignite_manager import FuturesCacheManager
from app.core.funding_engine import FundingEngine
from app.core.settlement_engine import SettlementEngine


# Global instances (initialized in main.py)
_settings: Optional[Settings] = None
_cache_manager: Optional[FuturesCacheManager] = None
_funding_engine: Optional[FundingEngine] = None
_settlement_engine: Optional[SettlementEngine] = None


def init_dependencies(
    settings: Settings,
    cache_manager: FuturesCacheManager,
    funding_engine: FundingEngine,
    settlement_engine: SettlementEngine
):
    """Initialize global dependencies."""
    global _settings, _cache_manager, _funding_engine, _settlement_engine
    _settings = settings
    _cache_manager = cache_manager
    _funding_engine = funding_engine
    _settlement_engine = settlement_engine


def get_settings() -> Settings:
    """Get application settings."""
    if _settings is None:
        raise RuntimeError("Dependencies not initialized")
    return _settings


async def get_cache_manager() -> FuturesCacheManager:
    """Get cache manager instance."""
    if _cache_manager is None:
        raise RuntimeError("Dependencies not initialized")
    return _cache_manager


async def get_funding_engine() -> FundingEngine:
    """Get funding engine instance."""
    if _funding_engine is None:
        raise RuntimeError("Dependencies not initialized")
    return _funding_engine


async def get_settlement_engine() -> SettlementEngine:
    """Get settlement engine instance."""
    if _settlement_engine is None:
        raise RuntimeError("Dependencies not initialized")
    return _settlement_engine


async def get_current_user(
    x_user_id: Annotated[Optional[str], Header()] = None
) -> str:
    """Get current user from headers."""
    if not x_user_id:
        raise HTTPException(status_code=401, detail="User ID header required")
    return x_user_id 