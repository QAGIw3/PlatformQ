"""Cache management for options service."""

import json
from typing import Any, Optional
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)


class OptionsCacheManager:
    """Manages caching for options data."""
    
    def __init__(self, settings):
        self.settings = settings
        self._cache = {}  # Simple in-memory cache for now
        
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        return self._cache.get(key)
        
    async def set(self, key: str, value: Any, ttl: int = 300):
        """Set value in cache with TTL."""
        self._cache[key] = value
        # In production, would use Redis with actual TTL
        
    async def delete(self, key: str):
        """Delete value from cache."""
        self._cache.pop(key, None)
        
    async def get_option_price(self, option_id: str) -> Optional[float]:
        """Get cached option price."""
        key = f"option_price:{option_id}"
        return await self.get(key)
        
    async def set_option_price(self, option_id: str, price: float, ttl: int = 60):
        """Cache option price."""
        key = f"option_price:{option_id}"
        await self.set(key, price, ttl)
        
    async def get_volatility_surface(self, underlying: str) -> Optional[dict]:
        """Get cached volatility surface."""
        key = f"vol_surface:{underlying}"
        return await self.get(key)
        
    async def set_volatility_surface(self, underlying: str, surface: dict, ttl: int = 3600):
        """Cache volatility surface."""
        key = f"vol_surface:{underlying}"
        await self.set(key, surface, ttl) 