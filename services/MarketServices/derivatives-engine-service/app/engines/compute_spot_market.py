"""
Compute Spot Market Engine

Real-time spot market for immediate compute resource allocation using trading-core-service
"""

from typing import Dict, List, Optional, Tuple, Set, Any
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import logging
import uuid
from collections import defaultdict

from app.integrations import (
    IgniteCache,
    PulsarEventPublisher,
    OracleAggregatorClient
)
from app.integrations.trading_core_integration import TradingCoreIntegration
from app.engines.partner_capacity_manager import PartnerCapacityManager, CapacityTier
from app.engines.cross_service_capacity_coordinator import CrossServiceCapacityCoordinator

logger = logging.getLogger(__name__)


class SpotOrderType(Enum):
    """Types of spot market orders"""
    MARKET = "market"
    LIMIT = "limit"
    IOC = "ioc"  # Immediate or cancel
    FOK = "fok"  # Fill or kill
    POST_ONLY = "post_only"  # Maker only


@dataclass
class SpotResource:
    """Available compute resource in spot market"""
    resource_id: str
    resource_type: str  # GPU, CPU, TPU, etc.
    provider_id: str
    capacity: Decimal
    location: str
    tier: CapacityTier
    available_until: datetime
    min_allocation: Decimal = Decimal("1")
    max_allocation: Optional[Decimal] = None
    pricing_model: str = "per_hour"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SpotTrade:
    """Executed spot market trade"""
    trade_id: str
    buyer_id: str
    seller_id: str
    resource_type: str
    quantity: Decimal
    price: Decimal
    location: str
    execution_time: datetime = field(default_factory=datetime.utcnow)
    allocation_id: Optional[str] = None


class ComputeSpotMarket:
    """
    Spot market for compute resources integrated with trading-core-service
    """
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        oracle: OracleAggregatorClient,
        partner_manager: PartnerCapacityManager,
        capacity_coordinator: CrossServiceCapacityCoordinator
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        self.partner_manager = partner_manager
        self.capacity_coordinator = capacity_coordinator
        
        # Trading core integration
        self.trading_core = TradingCoreIntegration()
        
        # Available resources
        self.resources: Dict[str, SpotResource] = {}
        
        # Trade history (local cache)
        self.recent_trades: List[SpotTrade] = []
        
        # Price indices
        self.spot_prices: Dict[str, Decimal] = {}
        self.price_history: Dict[str, List[Tuple[datetime, Decimal]]] = defaultdict(list)
        
        # Market registration tracking
        self.registered_markets: Set[str] = set()
        
        # Background tasks
        self._price_update_task = None
        self._resource_sync_task = None
        
    async def start(self):
        """Start spot market engine"""
        # Initialize trading core integration
        await self.trading_core.initialize()
        
        # Initialize resources
        await self._sync_available_resources()
        
        # Start background tasks
        self._price_update_task = asyncio.create_task(self._price_update_loop())
        self._resource_sync_task = asyncio.create_task(self._resource_sync_loop())
        
        logger.info("Compute spot market started with trading-core integration")
        
    async def stop(self):
        """Stop spot market engine"""
        if self._price_update_task:
            self._price_update_task.cancel()
        if self._resource_sync_task:
            self._resource_sync_task.cancel()
            
    async def register_market(self, resource_type: str, location: str) -> str:
        """Register a spot market with trading-core"""
        market_id = f"COMPUTE_SPOT_{resource_type}_{location}"
        
        if market_id not in self.registered_markets:
            # Register with trading-core
            success = await self.trading_core.register_compute_market(
                resource_type=resource_type,
                market_type="spot",
                specifications={
                    "location": location,
                    "settlement_type": "physical",
                    "min_quantity": "1",
                    "tick_size": "0.01"
                }
            )
            
            if success:
                self.registered_markets.add(market_id)
                logger.info(f"Registered spot market: {market_id}")
            else:
                logger.error(f"Failed to register spot market: {market_id}")
                
        return market_id
        
    async def submit_order(self, order_data: Dict[str, Any]) -> Dict[str, Any]:
        """Submit order through trading-core"""
        # Ensure market is registered
        resource_type = order_data.get("resource_type", "GPU")
        location = order_data.get("location_preference", "us-east-1")
        market_id = await self.register_market(resource_type, location)
        
        # Prepare order for trading-core
        compute_order = {
            "user_id": order_data.get("user_id"),
            "resource_type": resource_type,
            "market_type": "spot",
            "quantity": str(order_data.get("quantity", 1)),
            "specifications": {
                "location": location,
                "tier": order_data.get("tier", "standard"),
                "duration_hours": order_data.get("duration_hours", 1)
            }
        }
        
        # Submit through trading-core
        result = await self.trading_core.submit_compute_order(**compute_order)
        
        # Handle physical allocation on successful trade
        if result.get("success") and result.get("trades"):
            for trade in result["trades"]:
                await self._handle_spot_allocation(trade, order_data)
                
        return result
        
    async def get_spot_price(self, resource_type: str, location: str) -> Dict[str, Any]:
        """Get current spot price"""
        market_id = f"COMPUTE_SPOT_{resource_type}_{location}"
        
        # Get orderbook from trading-core
        orderbook = await self.trading_core.get_orderbook(market_id, depth=1)
        
        if orderbook:
            best_bid = orderbook.get("bids", [{}])[0].get("price", "0")
            best_ask = orderbook.get("asks", [{}])[0].get("price", "0")
            
            # Calculate mid price
            if best_bid and best_ask:
                mid_price = (Decimal(best_bid) + Decimal(best_ask)) / 2
            else:
                mid_price = self.spot_prices.get(market_id, Decimal("0"))
                
            return {
                "resource_type": resource_type,
                "location": location,
                "spot_price": str(mid_price),
                "best_bid": best_bid,
                "best_ask": best_ask,
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            return {
                "resource_type": resource_type,
                "location": location,
                "spot_price": str(self.spot_prices.get(market_id, Decimal("0"))),
                "timestamp": datetime.utcnow().isoformat()
            }
            
    async def register_resource(self, resource: SpotResource) -> bool:
        """Register available resource"""
        # Store resource locally
        self.resources[resource.resource_id] = resource
        
        # Register provider with trading-core
        success = await self.trading_core.register_compute_provider(
            provider_id=resource.provider_id,
            resources={
                resource.resource_type: {
                    "capacity": str(resource.capacity),
                    "location": resource.location,
                    "tier": resource.tier.value,
                    "pricing_model": resource.pricing_model,
                    "available_until": resource.available_until.isoformat()
                }
            }
        )
        
        if success:
            # Publish resource availability event
            await self.pulsar.publish('compute.spot.resource_available', {
                'resource_id': resource.resource_id,
                'resource_type': resource.resource_type,
                'provider_id': resource.provider_id,
                'capacity': str(resource.capacity),
                'location': resource.location,
                'timestamp': datetime.utcnow().isoformat()
            })
            
        return success
        
    async def _handle_spot_allocation(self, trade: Dict[str, Any], order_data: Dict[str, Any]):
        """Handle physical resource allocation after trade execution"""
        allocation_id = f"ALLOC_{uuid.uuid4().hex[:8]}"
        
        # Coordinate allocation through capacity coordinator
        allocation_result = await self.capacity_coordinator.allocate_capacity({
            "allocation_id": allocation_id,
            "resource_type": order_data.get("resource_type"),
            "quantity": trade.get("quantity"),
            "location": order_data.get("location_preference"),
            "duration": timedelta(hours=order_data.get("duration_hours", 1)),
            "tier": order_data.get("tier", "standard"),
            "user_id": order_data.get("user_id"),
            "trade_id": trade.get("trade_id")
        })
        
        if allocation_result["success"]:
            # Update trade with allocation ID
            trade_record = SpotTrade(
                trade_id=trade.get("trade_id"),
                buyer_id=trade.get("buyer_id"),
                seller_id=trade.get("seller_id", "platform"),
                resource_type=order_data.get("resource_type"),
                quantity=Decimal(trade.get("quantity")),
                price=Decimal(trade.get("price")),
                location=order_data.get("location_preference"),
                allocation_id=allocation_id
            )
            
            self.recent_trades.append(trade_record)
            
            # Publish allocation event
            await self.pulsar.publish('compute.spot.allocated', {
                'allocation_id': allocation_id,
                'trade_id': trade.get("trade_id"),
                'resource_type': order_data.get("resource_type"),
                'quantity': trade.get("quantity"),
                'price': trade.get("price"),
                'user_id': order_data.get("user_id"),
                'timestamp': datetime.utcnow().isoformat()
            })
            
    async def _sync_available_resources(self):
        """Sync available resources from partner manager"""
        try:
            # Get wholesale capacity
            wholesale_capacity = await self.partner_manager.get_available_capacity()
            
            for provider_id, capacities in wholesale_capacity.items():
                for resource_type, capacity_info in capacities.items():
                    for location, specs in capacity_info.items():
                        resource = SpotResource(
                            resource_id=f"{provider_id}_{resource_type}_{location}",
                            resource_type=resource_type,
                            provider_id=provider_id,
                            capacity=Decimal(str(specs.get("available", 0))),
                            location=location,
                            tier=CapacityTier(specs.get("tier", "standard")),
                            available_until=datetime.utcnow() + timedelta(hours=24),
                            pricing_model=specs.get("pricing_model", "per_hour"),
                            metadata=specs.get("metadata", {})
                        )
                        
                        await self.register_resource(resource)
                        
        except Exception as e:
            logger.error(f"Error syncing resources: {e}")
            
    async def _price_update_loop(self):
        """Update spot prices periodically"""
        while True:
            try:
                # Update prices for all registered markets
                for market_id in self.registered_markets:
                    parts = market_id.split("_")
                    if len(parts) >= 4:
                        resource_type = parts[2]
                        location = parts[3]
                        
                        price_data = await self.get_spot_price(resource_type, location)
                        if price_data and price_data.get("spot_price"):
                            self.spot_prices[market_id] = Decimal(price_data["spot_price"])
                            
                            # Update price history
                            self.price_history[market_id].append(
                                (datetime.utcnow(), Decimal(price_data["spot_price"]))
                            )
                            
                            # Trim old history
                            cutoff = datetime.utcnow() - timedelta(hours=24)
                            self.price_history[market_id] = [
                                (ts, price) for ts, price in self.price_history[market_id]
                                if ts > cutoff
                            ]
                            
                await asyncio.sleep(10)  # Update every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in price update loop: {e}")
                await asyncio.sleep(60)
                
    async def _resource_sync_loop(self):
        """Periodically sync available resources"""
        while True:
            try:
                await self._sync_available_resources()
                await asyncio.sleep(300)  # Sync every 5 minutes
                
            except Exception as e:
                logger.error(f"Error in resource sync loop: {e}")
                await asyncio.sleep(600)
                
    async def get_market_metrics(self) -> Dict[str, Any]:
        """Get spot market metrics from trading-core"""
        metrics = await self.trading_core.get_compute_metrics()
        
        # Add local metrics
        metrics.update({
            "registered_markets": len(self.registered_markets),
            "available_resources": len(self.resources),
            "recent_trades": len(self.recent_trades),
            "active_spot_prices": len(self.spot_prices)
        })
        
        return metrics 