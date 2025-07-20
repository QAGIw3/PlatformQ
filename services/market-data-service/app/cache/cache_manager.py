"""Cache manager for high-performance market data access"""

from typing import Dict, Optional, List, Any
from datetime import datetime, timedelta
import json
import asyncio
from pyignite import AsyncClient
from pyignite.datatypes.cache_config import CacheMode
from decimal import Decimal

from ..models.market_data import OrderBookSnapshot
from ..config import MarketDataConfig


class CacheManager:
    """Manages Ignite cache for market data"""
    
    def __init__(self, config: MarketDataConfig):
        self.config = config
        self.client: Optional[AsyncClient] = None
        
        # Cache names
        self.PRICE_CACHE = "market_prices"
        self.ORDERBOOK_CACHE = "market_orderbooks"
        self.CANDLE_CACHE = "market_candles"
        self.STATS_CACHE = "market_stats"
        self.MARKET_INFO_CACHE = "market_info"
    
    async def initialize(self):
        """Initialize Ignite connection"""
        self.client = AsyncClient()
        await self.client.connect(self.config.IGNITE_ADDRESSES)
        
        # Create caches
        await self._create_caches()
    
    async def _create_caches(self):
        """Create caches with appropriate configurations"""
        # Price cache - replicated for fast reads
        await self.client.get_or_create_cache({
            "name": self.PRICE_CACHE,
            "cache_mode": CacheMode.REPLICATED,
            "expiry_policy": {
                "access": self.config.PRICE_CACHE_TTL_SECONDS * 1000  # milliseconds
            }
        })
        
        # Order book cache - replicated with short TTL
        await self.client.get_or_create_cache({
            "name": self.ORDERBOOK_CACHE,
            "cache_mode": CacheMode.REPLICATED,
            "expiry_policy": {
                "access": self.config.ORDERBOOK_CACHE_TTL_SECONDS * 1000
            }
        })
        
        # Candle cache - partitioned with longer TTL
        await self.client.get_or_create_cache({
            "name": self.CANDLE_CACHE,
            "cache_mode": CacheMode.PARTITIONED,
            "backups": 1,
            "expiry_policy": {
                "access": self.config.CANDLE_CACHE_TTL_SECONDS * 1000
            }
        })
        
        # Stats cache - replicated
        await self.client.get_or_create_cache({
            "name": self.STATS_CACHE,
            "cache_mode": CacheMode.REPLICATED
        })
        
        # Market info cache - replicated, no expiry
        await self.client.get_or_create_cache({
            "name": self.MARKET_INFO_CACHE,
            "cache_mode": CacheMode.REPLICATED
        })
    
    async def set_price(self, market_id: str, price_data: Dict[str, Any]):
        """Set current price for a market"""
        cache = await self.client.get_cache(self.PRICE_CACHE)
        await cache.put(market_id, json.dumps(price_data))
    
    async def get_price(self, market_id: str) -> Optional[Dict[str, Any]]:
        """Get current price for a market"""
        cache = await self.client.get_cache(self.PRICE_CACHE)
        data = await cache.get(market_id)
        return json.loads(data) if data else None
    
    async def get_prices_bulk(self, market_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get prices for multiple markets"""
        cache = await self.client.get_cache(self.PRICE_CACHE)
        
        # Use get_all for bulk operation
        results = await cache.get_all(market_ids)
        
        prices = {}
        for market_id, data in results.items():
            if data:
                prices[market_id] = json.loads(data)
        
        return prices
    
    async def set_orderbook(self, market_id: str, orderbook: OrderBookSnapshot):
        """Set order book for a market"""
        cache = await self.client.get_cache(self.ORDERBOOK_CACHE)
        await cache.put(market_id, json.dumps(orderbook.to_dict()))
    
    async def get_orderbook(self, market_id: str) -> Optional[OrderBookSnapshot]:
        """Get order book for a market"""
        cache = await self.client.get_cache(self.ORDERBOOK_CACHE)
        data = await cache.get(market_id)
        
        if data:
            ob_dict = json.loads(data)
            # Reconstruct OrderBookSnapshot
            return OrderBookSnapshot(
                market_id=ob_dict["market_id"],
                bids=[(Decimal(p), Decimal(q)) for p, q in ob_dict["bids"]],
                asks=[(Decimal(p), Decimal(q)) for p, q in ob_dict["asks"]],
                sequence=ob_dict["sequence"],
                timestamp=datetime.fromisoformat(ob_dict["timestamp"])
            )
        
        return None
    
    async def set_candle(self, market_id: str, interval: str, candle_data: Dict[str, Any]):
        """Set candle data"""
        cache = await self.client.get_cache(self.CANDLE_CACHE)
        key = f"{market_id}:{interval}:{candle_data['open_time']}"
        await cache.put(key, json.dumps(candle_data))
    
    async def get_candles(
        self, 
        market_id: str, 
        interval: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get candles for a time range"""
        cache = await self.client.get_cache(self.CANDLE_CACHE)
        
        # Generate keys for the time range
        # This is simplified - in production would use SQL queries
        candles = []
        
        # TODO: Implement proper range query
        # For now, return empty list
        return candles
    
    async def set_market_stats(self, market_id: str, stats: Dict[str, Any]):
        """Set market statistics"""
        cache = await self.client.get_cache(self.STATS_CACHE)
        await cache.put(market_id, json.dumps(stats))
    
    async def get_market_stats(self, market_id: str) -> Optional[Dict[str, Any]]:
        """Get market statistics"""
        cache = await self.client.get_cache(self.STATS_CACHE)
        data = await cache.get(market_id)
        return json.loads(data) if data else None
    
    async def get_all_market_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get stats for all markets"""
        cache = await self.client.get_cache(self.STATS_CACHE)
        
        stats = {}
        # Scan cache - in production would use SQL query
        async for key, value in cache.scan():
            if value:
                stats[key] = json.loads(value)
        
        return stats
    
    async def set_market_info(self, market_id: str, info: Dict[str, Any]):
        """Set market information"""
        cache = await self.client.get_cache(self.MARKET_INFO_CACHE)
        await cache.put(market_id, json.dumps(info))
    
    async def get_market_info(self, market_id: str) -> Optional[Dict[str, Any]]:
        """Get market information"""
        cache = await self.client.get_cache(self.MARKET_INFO_CACHE)
        data = await cache.get(market_id)
        return json.loads(data) if data else None
    
    async def get_all_markets(self) -> List[Dict[str, Any]]:
        """Get all market information"""
        cache = await self.client.get_cache(self.MARKET_INFO_CACHE)
        
        markets = []
        async for key, value in cache.scan():
            if value:
                markets.append(json.loads(value))
        
        return markets
    
    async def close(self):
        """Close cache connection"""
        if self.client:
            await self.client.close() 