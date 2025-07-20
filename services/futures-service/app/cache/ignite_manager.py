"""Apache Ignite cache manager for Futures Service."""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any
import json
import logging

from pyignite import Client
from pyignite.cache import Cache

from app.config import Settings
from app.models.futures import (
    FuturesContract, FuturesPosition, FuturesOrder,
    FundingRate, SettlementRecord, MarginRequirement,
    FuturesMarketStats
)


logger = logging.getLogger(__name__)


class FuturesCacheManager:
    """Manages caching for futures data using Apache Ignite."""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = None
        self.connected = False
        
        # Cache references
        self.contracts_cache: Optional[Cache] = None
        self.positions_cache: Optional[Cache] = None
        self.orders_cache: Optional[Cache] = None
        self.funding_cache: Optional[Cache] = None
        self.market_data_cache: Optional[Cache] = None
        self.settlement_cache: Optional[Cache] = None
        
    async def connect(self):
        """Connect to Ignite cluster."""
        try:
            self.client = Client()
            self.client.connect(self.settings.ignite_host, self.settings.ignite_port)
            
            # Create or get caches
            self.contracts_cache = self.client.get_or_create_cache("futures_contracts")
            self.positions_cache = self.client.get_or_create_cache("futures_positions")
            self.orders_cache = self.client.get_or_create_cache("futures_orders")
            self.funding_cache = self.client.get_or_create_cache("funding_rates")
            self.market_data_cache = self.client.get_or_create_cache("futures_market_data")
            self.settlement_cache = self.client.get_or_create_cache("settlements")
            
            self.connected = True
            logger.info("Connected to Ignite cache")
            
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
            
    async def disconnect(self):
        """Disconnect from Ignite cluster."""
        if self.client:
            self.client.close()
            self.connected = False
            logger.info("Disconnected from Ignite cache")
            
    # Contract Management
    
    async def get_contract(self, symbol: str) -> Optional[FuturesContract]:
        """Get futures contract by symbol."""
        if not self.connected:
            return None
            
        try:
            data = self.contracts_cache.get(symbol)
            if data:
                return FuturesContract.parse_raw(data)
            return None
        except Exception as e:
            logger.error(f"Error getting contract {symbol}: {e}")
            return None
            
    async def store_contract(self, contract: FuturesContract):
        """Store futures contract."""
        if not self.connected:
            return
            
        try:
            self.contracts_cache.put(
                contract.symbol,
                contract.json(),
                expiry_policy=self._get_expiry_policy(days=30)
            )
        except Exception as e:
            logger.error(f"Error storing contract {contract.symbol}: {e}")
            
    async def update_contract(self, contract: FuturesContract):
        """Update futures contract."""
        await self.store_contract(contract)
        
    async def get_active_contracts(self) -> List[FuturesContract]:
        """Get all active futures contracts."""
        if not self.connected:
            return []
            
        try:
            contracts = []
            # In production, use SQL query on Ignite
            # For now, scan all contracts
            with self.contracts_cache.scan() as cursor:
                for key, value in cursor:
                    contract = FuturesContract.parse_raw(value)
                    if contract.is_active:
                        contracts.append(contract)
            return contracts
        except Exception as e:
            logger.error(f"Error getting active contracts: {e}")
            return []
            
    # Position Management
    
    async def get_position(self, position_id: str) -> Optional[FuturesPosition]:
        """Get position by ID."""
        if not self.connected:
            return None
            
        try:
            data = self.positions_cache.get(position_id)
            if data:
                return FuturesPosition.parse_raw(data)
            return None
        except Exception as e:
            logger.error(f"Error getting position {position_id}: {e}")
            return None
            
    async def get_user_positions(
        self,
        user_id: str,
        symbol: Optional[str] = None
    ) -> List[FuturesPosition]:
        """Get user's positions."""
        if not self.connected:
            return []
            
        try:
            positions = []
            # In production, use SQL query with index on user_id
            with self.positions_cache.scan() as cursor:
                for key, value in cursor:
                    position = FuturesPosition.parse_raw(value)
                    if position.user_id == user_id:
                        if symbol is None or position.symbol == symbol:
                            positions.append(position)
            return positions
        except Exception as e:
            logger.error(f"Error getting user positions: {e}")
            return []
            
    async def get_all_positions(self, symbol: str) -> List[FuturesPosition]:
        """Get all positions for a symbol."""
        if not self.connected:
            return []
            
        try:
            positions = []
            with self.positions_cache.scan() as cursor:
                for key, value in cursor:
                    position = FuturesPosition.parse_raw(value)
                    if position.symbol == symbol:
                        positions.append(position)
            return positions
        except Exception as e:
            logger.error(f"Error getting positions for {symbol}: {e}")
            return []
            
    async def store_position(self, position: FuturesPosition):
        """Store position."""
        if not self.connected:
            return
            
        try:
            self.positions_cache.put(
                position.position_id,
                position.json(),
                expiry_policy=self._get_expiry_policy(seconds=self.settings.position_cache_ttl)
            )
        except Exception as e:
            logger.error(f"Error storing position {position.position_id}: {e}")
            
    async def update_position(self, position: FuturesPosition):
        """Update position."""
        position.updated_at = datetime.utcnow()
        await self.store_position(position)
        
    async def close_position(self, position_id: str):
        """Close and remove position."""
        if not self.connected:
            return
            
        try:
            self.positions_cache.remove(position_id)
        except Exception as e:
            logger.error(f"Error closing position {position_id}: {e}")
            
    # Order Management
    
    async def store_order(self, order: FuturesOrder):
        """Store futures order."""
        if not self.connected:
            return
            
        try:
            self.orders_cache.put(
                order.order_id,
                order.json(),
                expiry_policy=self._get_expiry_policy(hours=24)
            )
        except Exception as e:
            logger.error(f"Error storing order {order.order_id}: {e}")
            
    async def get_order(self, order_id: str) -> Optional[FuturesOrder]:
        """Get order by ID."""
        if not self.connected:
            return None
            
        try:
            data = self.orders_cache.get(order_id)
            if data:
                return FuturesOrder.parse_raw(data)
            return None
        except Exception as e:
            logger.error(f"Error getting order {order_id}: {e}")
            return None
            
    # Funding Rate Management
    
    async def store_funding_rate(self, symbol: str, funding_rate: FundingRate):
        """Store funding rate."""
        if not self.connected:
            return
            
        try:
            # Store current funding rate
            self.funding_cache.put(
                f"current:{symbol}",
                funding_rate.json(),
                expiry_policy=self._get_expiry_policy(seconds=self.settings.funding_rate_cache_ttl)
            )
            
            # Store in history
            history_key = f"history:{symbol}:{funding_rate.timestamp.isoformat()}"
            self.funding_cache.put(
                history_key,
                funding_rate.json(),
                expiry_policy=self._get_expiry_policy(days=30)
            )
        except Exception as e:
            logger.error(f"Error storing funding rate for {symbol}: {e}")
            
    async def get_current_funding_rate(self, symbol: str) -> Optional[FundingRate]:
        """Get current funding rate for symbol."""
        if not self.connected:
            return None
            
        try:
            data = self.funding_cache.get(f"current:{symbol}")
            if data:
                return FundingRate.parse_raw(data)
            return None
        except Exception as e:
            logger.error(f"Error getting funding rate for {symbol}: {e}")
            return None
            
    async def get_funding_history(
        self,
        symbol: str,
        limit: int = 100
    ) -> List[FundingRate]:
        """Get funding rate history."""
        if not self.connected:
            return []
            
        try:
            history = []
            prefix = f"history:{symbol}:"
            
            with self.funding_cache.scan() as cursor:
                for key, value in cursor:
                    if key.startswith(prefix):
                        rate = FundingRate.parse_raw(value)
                        history.append(rate)
                        
            # Sort by timestamp descending
            history.sort(key=lambda x: x.timestamp, reverse=True)
            return history[:limit]
            
        except Exception as e:
            logger.error(f"Error getting funding history for {symbol}: {e}")
            return []
            
    # Market Data
    
    async def get_latest_price(self, symbol: str) -> Optional[float]:
        """Get latest price for symbol."""
        if not self.connected:
            return None
            
        try:
            data = self.market_data_cache.get(f"price:{symbol}")
            if data:
                return float(data)
            return None
        except Exception as e:
            logger.error(f"Error getting price for {symbol}: {e}")
            return None
            
    async def store_latest_price(self, symbol: str, price: Decimal):
        """Store latest price."""
        if not self.connected:
            return
            
        try:
            self.market_data_cache.put(
                f"price:{symbol}",
                str(price),
                expiry_policy=self._get_expiry_policy(seconds=60)
            )
        except Exception as e:
            logger.error(f"Error storing price for {symbol}: {e}")
            
    async def get_market_stats(self, symbol: str) -> Optional[FuturesMarketStats]:
        """Get market statistics."""
        if not self.connected:
            return None
            
        try:
            data = self.market_data_cache.get(f"stats:{symbol}")
            if data:
                return FuturesMarketStats.parse_raw(data)
            return None
        except Exception as e:
            logger.error(f"Error getting market stats for {symbol}: {e}")
            return None
            
    async def store_market_stats(self, stats: FuturesMarketStats):
        """Store market statistics."""
        if not self.connected:
            return
            
        try:
            self.market_data_cache.put(
                f"stats:{stats.symbol}",
                stats.json(),
                expiry_policy=self._get_expiry_policy(seconds=300)
            )
        except Exception as e:
            logger.error(f"Error storing market stats for {stats.symbol}: {e}")
            
    # Settlement Management
    
    async def store_settlement_record(self, settlement: SettlementRecord):
        """Store settlement record."""
        if not self.connected:
            return
            
        try:
            self.settlement_cache.put(
                settlement.settlement_id,
                settlement.json(),
                expiry_policy=self._get_expiry_policy(days=90)
            )
        except Exception as e:
            logger.error(f"Error storing settlement {settlement.settlement_id}: {e}")
            
    async def update_settlement_record(self, settlement: SettlementRecord):
        """Update settlement record."""
        await self.store_settlement_record(settlement)
        
    async def get_settlement_history(
        self,
        symbol: Optional[str] = None,
        limit: int = 100
    ) -> List[SettlementRecord]:
        """Get settlement history."""
        if not self.connected:
            return []
            
        try:
            settlements = []
            with self.settlement_cache.scan() as cursor:
                for key, value in cursor:
                    settlement = SettlementRecord.parse_raw(value)
                    if symbol is None or settlement.symbol == symbol:
                        settlements.append(settlement)
                        
            # Sort by timestamp descending
            settlements.sort(key=lambda x: x.timestamp, reverse=True)
            return settlements[:limit]
            
        except Exception as e:
            logger.error(f"Error getting settlement history: {e}")
            return []
            
    async def store_delivery_instruction(self, instruction: Dict):
        """Store physical delivery instruction."""
        if not self.connected:
            return
            
        try:
            key = f"delivery:{instruction['delivery_id']}"
            self.settlement_cache.put(
                key,
                json.dumps(instruction),
                expiry_policy=self._get_expiry_policy(days=30)
            )
        except Exception as e:
            logger.error(f"Error storing delivery instruction: {e}")
            
    # Utility Methods
    
    def _get_expiry_policy(
        self,
        seconds: int = 0,
        minutes: int = 0,
        hours: int = 0,
        days: int = 0
    ) -> Dict[str, Any]:
        """Create expiry policy for cache entries."""
        total_seconds = seconds + (minutes * 60) + (hours * 3600) + (days * 86400)
        
        return {
            'expiry_policy': {
                'create': total_seconds * 1000,  # Convert to milliseconds
                'update': total_seconds * 1000,
                'access': total_seconds * 1000
            }
        } 