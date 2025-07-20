from typing import Dict, List, Optional
import asyncio
from pyignite import AsyncClient
from pyignite.datatypes import String, DecimalObject, IntObject, LongObject
from pyignite.datatypes.cache_config import CacheMode, CacheAtomicityMode, WriteSynchronizationMode
from pyignite.datatypes.prop_codes import PROP_NAME, PROP_CACHE_MODE, PROP_ATOMICITY_MODE, PROP_WRITE_SYNC_MODE, PROP_BACKUPS_NUMBER
from decimal import Decimal

from ..models.order import Order, Trade, OrderStatus, OrderSide, OrderType
from ..config import OrderMatchingConfig


class IgniteManager:
    """Manages Ignite cache operations for order matching"""
    
    def __init__(self, config: OrderMatchingConfig):
        self.config = config
        self.client: Optional[AsyncClient] = None
        
        # Cache names
        self.ORDERS_CACHE = "orders"
        self.TRADES_CACHE = "trades"
        self.MARKET_STATE_CACHE = "market_state"
        self.TRADER_POSITIONS_CACHE = "trader_positions"
        
    async def initialize(self):
        """Initialize Ignite connection and caches"""
        # Create client
        self.client = AsyncClient()
        
        # Connect to Ignite cluster
        await self.client.connect(self.config.IGNITE_ADDRESSES)
        
        # Create caches with optimized configurations
        await self._create_caches()
    
    async def _create_caches(self):
        """Create caches with proper configurations"""
        # Orders cache - partitioned with backup
        orders_cache_config = {
            PROP_NAME: self.ORDERS_CACHE,
            PROP_CACHE_MODE: CacheMode.PARTITIONED,
            PROP_ATOMICITY_MODE: CacheAtomicityMode.TRANSACTIONAL,
            PROP_WRITE_SYNC_MODE: WriteSynchronizationMode.PRIMARY_SYNC,
            PROP_BACKUPS_NUMBER: self.config.IGNITE_BACKUP_COUNT
        }
        await self.client.get_or_create_cache(orders_cache_config)
        
        # Trades cache - partitioned with backup
        trades_cache_config = {
            PROP_NAME: self.TRADES_CACHE,
            PROP_CACHE_MODE: CacheMode.PARTITIONED,
            PROP_ATOMICITY_MODE: CacheAtomicityMode.ATOMIC,
            PROP_WRITE_SYNC_MODE: WriteSynchronizationMode.PRIMARY_SYNC,
            PROP_BACKUPS_NUMBER: self.config.IGNITE_BACKUP_COUNT
        }
        await self.client.get_or_create_cache(trades_cache_config)
        
        # Market state cache - replicated for fast reads
        market_state_config = {
            PROP_NAME: self.MARKET_STATE_CACHE,
            PROP_CACHE_MODE: CacheMode.REPLICATED,
            PROP_ATOMICITY_MODE: CacheAtomicityMode.ATOMIC,
            PROP_WRITE_SYNC_MODE: WriteSynchronizationMode.FULL_SYNC
        }
        await self.client.get_or_create_cache(market_state_config)
        
        # Trader positions cache - partitioned
        positions_cache_config = {
            PROP_NAME: self.TRADER_POSITIONS_CACHE,
            PROP_CACHE_MODE: CacheMode.PARTITIONED,
            PROP_ATOMICITY_MODE: CacheAtomicityMode.TRANSACTIONAL,
            PROP_WRITE_SYNC_MODE: WriteSynchronizationMode.PRIMARY_SYNC,
            PROP_BACKUPS_NUMBER: self.config.IGNITE_BACKUP_COUNT
        }
        await self.client.get_or_create_cache(positions_cache_config)
    
    async def save_order(self, order: Order):
        """Save order to cache"""
        cache = await self.client.get_cache(self.ORDERS_CACHE)
        
        # Convert order to cache format
        order_data = {
            "order_id": order.order_id,
            "market_id": order.market_id,
            "trader_id": order.trader_id,
            "side": order.side.value,
            "order_type": order.order_type.value,
            "quantity": str(order.quantity),
            "price": str(order.price) if order.price else None,
            "filled_quantity": str(order.filled_quantity),
            "status": order.status.value,
            "created_at_ns": order.created_at_ns,
            "updated_at_ns": order.updated_at_ns,
            "sequence": order._sequence
        }
        
        # Use order_id as key for fast lookup
        await cache.put(order.order_id, order_data)
        
        # Also index by market_id for market queries
        market_key = f"{order.market_id}:{order.order_id}"
        await cache.put(market_key, order.order_id)
    
    async def get_order(self, order_id: str) -> Optional[Dict]:
        """Get order from cache"""
        cache = await self.client.get_cache(self.ORDERS_CACHE)
        return await cache.get(order_id)
    
    async def save_trade(self, trade: Trade):
        """Save trade to cache"""
        cache = await self.client.get_cache(self.TRADES_CACHE)
        
        # Convert trade to cache format
        trade_data = {
            "trade_id": trade.trade_id,
            "market_id": trade.market_id,
            "price": str(trade.price),
            "quantity": str(trade.quantity),
            "buyer_order_id": trade.buyer_order_id,
            "seller_order_id": trade.seller_order_id,
            "buyer_id": trade.buyer_id,
            "seller_id": trade.seller_id,
            "buyer_fee": str(trade.buyer_fee),
            "seller_fee": str(trade.seller_fee),
            "executed_at_ns": trade.executed_at_ns
        }
        
        # Use trade_id as primary key
        await cache.put(trade.trade_id, trade_data)
        
        # Index by market_id for market queries
        market_key = f"{trade.market_id}:{trade.trade_id}"
        await cache.put(market_key, trade.trade_id)
        
        # Index by trader for trader queries
        buyer_key = f"buyer:{trade.buyer_id}:{trade.trade_id}"
        seller_key = f"seller:{trade.seller_id}:{trade.trade_id}"
        await cache.put(buyer_key, trade.trade_id)
        await cache.put(seller_key, trade.trade_id)
    
    async def get_trade(self, trade_id: str) -> Optional[Dict]:
        """Get trade from cache"""
        cache = await self.client.get_cache(self.TRADES_CACHE)
        return await cache.get(trade_id)
    
    async def save_market_state(self, market_id: str, state: Dict):
        """Save market state (last price, volume, etc.)"""
        cache = await self.client.get_cache(self.MARKET_STATE_CACHE)
        await cache.put(market_id, state)
    
    async def get_market_state(self, market_id: str) -> Optional[Dict]:
        """Get market state"""
        cache = await self.client.get_cache(self.MARKET_STATE_CACHE)
        return await cache.get(market_id)
    
    async def update_trader_position(self, trader_id: str, market_id: str, position: Dict):
        """Update trader position"""
        cache = await self.client.get_cache(self.TRADER_POSITIONS_CACHE)
        key = f"{trader_id}:{market_id}"
        await cache.put(key, position)
    
    async def get_trader_position(self, trader_id: str, market_id: str) -> Optional[Dict]:
        """Get trader position"""
        cache = await self.client.get_cache(self.TRADER_POSITIONS_CACHE)
        key = f"{trader_id}:{market_id}"
        return await cache.get(key)
    
    async def get_market_orders(self, market_id: str, limit: int = 100) -> List[Dict]:
        """Get recent orders for a market"""
        cache = await self.client.get_cache(self.ORDERS_CACHE)
        
        # This would use Ignite SQL queries in production
        # For now, simplified implementation
        orders = []
        
        # Scan cache with filter (simplified)
        # In production, use proper SQL queries
        async for key, value in cache.scan():
            if isinstance(value, dict) and value.get("market_id") == market_id:
                orders.append(value)
                if len(orders) >= limit:
                    break
        
        return orders
    
    async def get_market_trades(self, market_id: str, limit: int = 100) -> List[Dict]:
        """Get recent trades for a market"""
        cache = await self.client.get_cache(self.TRADES_CACHE)
        
        # Similar to get_market_orders
        trades = []
        
        async for key, value in cache.scan():
            if isinstance(value, dict) and value.get("market_id") == market_id:
                trades.append(value)
                if len(trades) >= limit:
                    break
        
        # Sort by timestamp
        trades.sort(key=lambda x: x.get("executed_at_ns", 0), reverse=True)
        
        return trades[:limit]
    
    async def close(self):
        """Close Ignite connection"""
        if self.client:
            await self.client.close() 