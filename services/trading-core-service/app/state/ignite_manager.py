"""Apache Ignite state manager."""

import asyncio
import logging
from typing import Dict, Any, Optional, List, TypeVar, Generic
from datetime import datetime
from decimal import Decimal
import json

from pyignite import Client as IgniteClient
from pyignite.datatypes import String, IntObject, DecimalObject
from pyignite.exceptions import CacheError

from ..config import Settings
from .cache_config import CacheType, CACHE_CONFIGS


logger = logging.getLogger(__name__)

T = TypeVar('T')


class IgniteStateManager:
    """Manages state storage using Apache Ignite."""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = IgniteClient()
        self.caches: Dict[CacheType, Any] = {}
        self.connected = False
    
    async def connect(self):
        """Connect to Ignite cluster."""
        try:
            self.client.connect(self.settings.ignite_host, self.settings.ignite_port)
            self.connected = True
            
            # Initialize caches
            for cache_type, config in CACHE_CONFIGS.items():
                cache = self.client.get_or_create_cache(config.name)
                self.caches[cache_type] = cache
            
            logger.info("Connected to Ignite cluster")
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from Ignite cluster."""
        if self.connected:
            self.client.close()
            self.connected = False
            logger.info("Disconnected from Ignite cluster")
    
    # Order operations
    async def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Get order by ID."""
        cache = self.caches.get(CacheType.ORDER)
        if not cache:
            return None
        
        try:
            return cache.get(order_id)
        except CacheError as e:
            logger.error(f"Failed to get order {order_id}: {e}")
            return None
    
    async def put_order(self, order_id: str, order_data: Dict[str, Any]):
        """Store order."""
        cache = self.caches.get(CacheType.ORDER)
        if not cache:
            return
        
        try:
            # Convert decimals to strings for storage
            order_data = self._prepare_for_storage(order_data)
            cache.put(order_id, order_data)
        except CacheError as e:
            logger.error(f"Failed to store order {order_id}: {e}")
            raise
    
    async def update_order(self, order_id: str, updates: Dict[str, Any]):
        """Update order fields."""
        order = await self.get_order(order_id)
        if order:
            order.update(updates)
            order['updated_at'] = datetime.utcnow().isoformat()
            await self.put_order(order_id, order)
    
    async def get_user_orders(self, user_id: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get orders for a user."""
        cache = self.caches.get(CacheType.ORDER)
        if not cache:
            return []
        
        try:
            # Use SQL query
            query = f"SELECT * FROM Order WHERE user_id = ?"
            params = [user_id]
            
            if status:
                query += " AND status = ?"
                params.append(status)
            
            result = cache.query(query, *params)
            return [row[1] for row in result]
        except Exception as e:
            logger.error(f"Failed to query user orders: {e}")
            return []
    
    # Position operations
    async def get_position(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get position by ID."""
        cache = self.caches.get(CacheType.POSITION)
        if not cache:
            return None
        
        try:
            return cache.get(position_id)
        except CacheError as e:
            logger.error(f"Failed to get position {position_id}: {e}")
            return None
    
    async def put_position(self, position_id: str, position_data: Dict[str, Any]):
        """Store position."""
        cache = self.caches.get(CacheType.POSITION)
        if not cache:
            return
        
        try:
            position_data = self._prepare_for_storage(position_data)
            cache.put(position_id, position_data)
        except CacheError as e:
            logger.error(f"Failed to store position {position_id}: {e}")
            raise
    
    async def get_user_positions(self, user_id: str, market_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get positions for a user."""
        cache = self.caches.get(CacheType.POSITION)
        if not cache:
            return []
        
        try:
            query = f"SELECT * FROM Position WHERE user_id = ? AND is_open = true"
            params = [user_id]
            
            if market_id:
                query += " AND market_id = ?"
                params.append(market_id)
            
            result = cache.query(query, *params)
            return [row[1] for row in result]
        except Exception as e:
            logger.error(f"Failed to query user positions: {e}")
            return []
    
    # OrderBook operations
    async def get_orderbook(self, market_id: str) -> Optional[Dict[str, Any]]:
        """Get orderbook for a market."""
        cache = self.caches.get(CacheType.ORDERBOOK)
        if not cache:
            return None
        
        try:
            return cache.get(market_id)
        except CacheError as e:
            logger.error(f"Failed to get orderbook {market_id}: {e}")
            return None
    
    async def put_orderbook(self, market_id: str, orderbook_data: Dict[str, Any]):
        """Store orderbook snapshot."""
        cache = self.caches.get(CacheType.ORDERBOOK)
        if not cache:
            return
        
        try:
            orderbook_data = self._prepare_for_storage(orderbook_data)
            cache.put(market_id, orderbook_data)
        except CacheError as e:
            logger.error(f"Failed to store orderbook {market_id}: {e}")
            raise
    
    # Trade operations
    async def put_trade(self, trade_id: str, trade_data: Dict[str, Any]):
        """Store trade."""
        cache = self.caches.get(CacheType.TRADE)
        if not cache:
            return
        
        try:
            trade_data = self._prepare_for_storage(trade_data)
            cache.put(trade_id, trade_data)
        except CacheError as e:
            logger.error(f"Failed to store trade {trade_id}: {e}")
            raise
    
    async def get_recent_trades(self, market_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent trades for a market."""
        cache = self.caches.get(CacheType.TRADE)
        if not cache:
            return []
        
        try:
            query = f"""
                SELECT * FROM Trade 
                WHERE market_id = ? 
                ORDER BY executed_at DESC 
                LIMIT {limit}
            """
            result = cache.query(query, market_id)
            return [row[1] for row in result]
        except Exception as e:
            logger.error(f"Failed to query recent trades: {e}")
            return []
    
    # Market operations
    async def get_market(self, market_id: str) -> Optional[Dict[str, Any]]:
        """Get market configuration."""
        cache = self.caches.get(CacheType.MARKET)
        if not cache:
            return None
        
        try:
            return cache.get(market_id)
        except CacheError as e:
            logger.error(f"Failed to get market {market_id}: {e}")
            return None
    
    async def put_market(self, market_id: str, market_data: Dict[str, Any]):
        """Store market configuration."""
        cache = self.caches.get(CacheType.MARKET)
        if not cache:
            return
        
        try:
            market_data = self._prepare_for_storage(market_data)
            cache.put(market_id, market_data)
        except CacheError as e:
            logger.error(f"Failed to store market {market_id}: {e}")
            raise
    
    async def get_active_markets(self) -> List[Dict[str, Any]]:
        """Get all active markets."""
        cache = self.caches.get(CacheType.MARKET)
        if not cache:
            return []
        
        try:
            query = "SELECT * FROM Market WHERE is_active = true"
            result = cache.query(query)
            return [row[1] for row in result]
        except Exception as e:
            logger.error(f"Failed to query active markets: {e}")
            return []
    
    # User state operations
    async def get_user_state(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user state."""
        cache = self.caches.get(CacheType.USER_STATE)
        if not cache:
            return None
        
        try:
            return cache.get(user_id)
        except CacheError as e:
            logger.error(f"Failed to get user state {user_id}: {e}")
            return None
    
    async def put_user_state(self, user_id: str, state_data: Dict[str, Any]):
        """Store user state."""
        cache = self.caches.get(CacheType.USER_STATE)
        if not cache:
            return
        
        try:
            state_data = self._prepare_for_storage(state_data)
            cache.put(user_id, state_data)
        except CacheError as e:
            logger.error(f"Failed to store user state {user_id}: {e}")
            raise
    
    # Risk state operations
    async def get_risk_state(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user risk state."""
        cache = self.caches.get(CacheType.RISK_STATE)
        if not cache:
            return None
        
        try:
            return cache.get(user_id)
        except CacheError as e:
            logger.error(f"Failed to get risk state {user_id}: {e}")
            return None
    
    async def put_risk_state(self, user_id: str, risk_data: Dict[str, Any]):
        """Store user risk state."""
        cache = self.caches.get(CacheType.RISK_STATE)
        if not cache:
            return
        
        try:
            risk_data = self._prepare_for_storage(risk_data)
            cache.put(user_id, risk_data)
        except CacheError as e:
            logger.error(f"Failed to store risk state {user_id}: {e}")
            raise
    
    # Batch operations
    async def put_all(self, cache_type: CacheType, items: Dict[str, Any]):
        """Batch put operation."""
        cache = self.caches.get(cache_type)
        if not cache:
            return
        
        try:
            prepared_items = {
                k: self._prepare_for_storage(v) 
                for k, v in items.items()
            }
            cache.put_all(prepared_items)
        except CacheError as e:
            logger.error(f"Failed to batch store in {cache_type}: {e}")
            raise
    
    async def get_all(self, cache_type: CacheType, keys: List[str]) -> Dict[str, Any]:
        """Batch get operation."""
        cache = self.caches.get(cache_type)
        if not cache:
            return {}
        
        try:
            return cache.get_all(keys)
        except CacheError as e:
            logger.error(f"Failed to batch get from {cache_type}: {e}")
            return {}
    
    # Transaction support
    async def execute_transaction(self, operations: List[tuple]):
        """Execute multiple operations in a transaction."""
        # Ignite transactions are not directly supported in pyignite
        # This is a placeholder for transaction logic
        for op, cache_type, key, value in operations:
            if op == "put":
                cache = self.caches.get(cache_type)
                if cache:
                    cache.put(key, self._prepare_for_storage(value))
            elif op == "remove":
                cache = self.caches.get(cache_type)
                if cache:
                    cache.remove(key)
    
    # Helper methods
    def _prepare_for_storage(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare data for storage (convert Decimal to string, etc.)."""
        prepared = {}
        for key, value in data.items():
            if isinstance(value, Decimal):
                prepared[key] = str(value)
            elif isinstance(value, datetime):
                prepared[key] = value.isoformat()
            elif isinstance(value, dict):
                prepared[key] = self._prepare_for_storage(value)
            elif isinstance(value, list):
                prepared[key] = [
                    self._prepare_for_storage(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                prepared[key] = value
        return prepared
    
    async def clear_cache(self, cache_type: CacheType):
        """Clear a specific cache."""
        cache = self.caches.get(cache_type)
        if cache:
            cache.clear()
            logger.info(f"Cleared cache: {cache_type.value}") 