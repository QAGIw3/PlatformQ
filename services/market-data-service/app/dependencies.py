"""Market Data Service dependencies for FastAPI."""

from typing import Annotated, Optional

from fastapi import Depends, Header, HTTPException

from app.config import MarketDataConfig
from app.core.aggregator import MarketDataAggregator
from app.oracle.blockchain_oracle_adapter import OracleAggregator
from app.cache.cache_manager import CacheManager


def get_settings() -> MarketDataConfig:
    """Get application settings."""
    return MarketDataConfig()


async def get_aggregator(
    settings: Annotated[MarketDataConfig, Depends(get_settings)]
) -> MarketDataAggregator:
    """Get market data aggregator instance."""
    # In production, this would return a singleton instance
    # For now, we'll create a new instance
    # Note: This would need proper initialization with cache manager and event subscriber
    # aggregator = MarketDataAggregator(settings, cache_manager, event_subscriber)
    # await aggregator.start()
    # return aggregator
    raise NotImplementedError("MarketDataAggregator initialization needs cache_manager and event_subscriber")


async def get_current_user(
    x_user_id: Annotated[Optional[str], Header()] = None
) -> str:
    """Get current user from headers."""
    if not x_user_id:
        raise HTTPException(status_code=401, detail="User ID header required")
    return x_user_id


async def get_oracle_aggregator(
    settings: Annotated[MarketDataConfig, Depends(get_settings)]
) -> OracleAggregator:
    """Get oracle aggregator instance."""
    # In production, this would return a singleton instance
    # For now, we'll create a new instance
    aggregator = OracleAggregator()
    
    # Initialize with Chainlink adapter (example)
    # In production, would get addresses from config
    # chainlink_adapter = ChainlinkAdapter(...)
    # aggregator.add_adapter("chainlink", chainlink_adapter, weight=1.5)
    
    return aggregator


async def get_cache_manager(
    settings: Annotated[MarketDataConfig, Depends(get_settings)]
) -> CacheManager:
    """Get cache manager instance."""
    # In production, this would return a singleton instance
    cache_manager = CacheManager(settings)
    await cache_manager.initialize()
    return cache_manager 