"""Market Data Service dependencies for FastAPI."""

from typing import Annotated, Optional

from fastapi import Depends, Header, HTTPException

from app.config import Settings
from app.core.aggregator import MarketDataAggregator


def get_settings() -> Settings:
    """Get application settings."""
    return Settings()


async def get_aggregator(
    settings: Annotated[Settings, Depends(get_settings)]
) -> MarketDataAggregator:
    """Get market data aggregator instance."""
    # In production, this would return a singleton instance
    # For now, we'll create a new instance
    aggregator = MarketDataAggregator(settings)
    await aggregator.start()
    return aggregator


async def get_current_user(
    x_user_id: Annotated[Optional[str], Header()] = None
) -> str:
    """Get current user from headers."""
    if not x_user_id:
        raise HTTPException(status_code=401, detail="User ID header required")
    return x_user_id 