"""
Compute Spot Market Engine

Real-time spot market for immediate compute resource allocation with sub-second matching
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
import heapq

from app.integrations import (
    IgniteCache,
    PulsarEventPublisher,
    OracleAggregatorClient
)
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


class ResourceState(Enum):
    """State of compute resources"""
    AVAILABLE = "available"
    RESERVED = "reserved"
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    TERMINATING = "terminating"


@dataclass
class SpotOrder:
    """Spot market order for compute resources"""
    order_id: str
    user_id: str
    order_type: SpotOrderType
    side: str  # "buy" or "sell"
    resource_type: str
    quantity: Decimal
    price: Optional[Decimal]  # None for market orders
    location_preference: Optional[str] = None
    urgency: str = "normal"  # "normal", "urgent", "flexible"
    time_in_force: str = "GTC"  # Good till cancelled
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    filled_quantity: Decimal = field(default=Decimal("0"))
    status: str = "pending"
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SpotResource:
    """Available compute resource in spot market"""
    resource_id: str
    provider_id: str
    resource_type: str
    capacity: Decimal
    available_capacity: Decimal
    location: str
    tier: CapacityTier
    min_price: Decimal
    current_price: Decimal
    state: ResourceState
    performance_score: float = 1.0
    last_update: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SpotTrade:
    """Executed spot trade"""
    trade_id: str
    buy_order_id: str
    sell_order_id: str
    buyer_id: str
    seller_id: str
    resource_type: str
    quantity: Decimal
    price: Decimal
    location: str
    execution_time: datetime = field(default_factory=datetime.utcnow)
    provisioning_time: Optional[datetime] = None
    access_details: Dict[str, Any] = field(default_factory=dict)


class ComputeSpotMarket:
    """
    Real-time spot market for compute resources with ultra-fast matching
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
        
        # Order books by resource type and location
        self.order_books: Dict[str, Dict[str, List[SpotOrder]]] = defaultdict(
            lambda: {"buy": [], "sell": []}
        )
        
        # Available resources
        self.resources: Dict[str, SpotResource] = {}
        
        # Active orders by user
        self.user_orders: Dict[str, Set[str]] = defaultdict(set)
        
        # Trade history
        self.recent_trades: List[SpotTrade] = []
        
        # Price indices
        self.spot_prices: Dict[str, Decimal] = {}
        self.price_history: Dict[str, List[Tuple[datetime, Decimal]]] = defaultdict(list)
        
        # Market metrics
        self.metrics = {
            "total_volume_24h": Decimal("0"),
            "trades_count_24h": 0,
            "avg_execution_time_ms": 0.0,
            "liquidity_depth": {}
        }
        
        # Background tasks
        self._matching_task = None
        self._price_update_task = None
        self._resource_sync_task = None
        
    async def start(self):
        """Start spot market engine"""
        # Load existing state
        await self._load_market_state()
        
        # Start background tasks
        self._matching_task = asyncio.create_task(self._continuous_matching_loop())
        self._price_update_task = asyncio.create_task(self._price_update_loop())
        self._resource_sync_task = asyncio.create_task(self._resource_sync_loop())
        
        logger.info("Compute spot market engine started")
        
    async def stop(self):
        """Stop spot market engine"""
        if self._matching_task:
            self._matching_task.cancel()
        if self._price_update_task:
            self._price_update_task.cancel()
        if self._resource_sync_task:
            self._resource_sync_task.cancel()
            
    async def submit_order(
        self,
        order: SpotOrder
    ) -> Dict[str, Any]:
        """Submit a spot market order"""
        start_time = datetime.utcnow()
        
        # Validate order
        validation = await self._validate_order(order)
        if not validation["valid"]:
            return {
                "success": False,
                "error": validation["error"],
                "order_id": order.order_id
            }
            
        # Store order
        await self._store_order(order)
        
        # Add to order book
        order_book_key = f"{order.resource_type}:{order.location_preference or 'any'}"
        
        if order.order_type == SpotOrderType.MARKET:
            # Execute immediately
            trades = await self._execute_market_order(order)
            execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            return {
                "success": True,
                "order_id": order.order_id,
                "trades": [self._trade_to_dict(t) for t in trades],
                "filled_quantity": sum(t.quantity for t in trades),
                "avg_price": self._calculate_avg_price(trades),
                "execution_time_ms": execution_time
            }
        else:
            # Add to order book
            self._add_to_order_book(order_book_key, order)
            
            # Try immediate match
            trades = await self._try_match_order(order)
            
            return {
                "success": True,
                "order_id": order.order_id,
                "status": order.status,
                "trades": [self._trade_to_dict(t) for t in trades],
                "filled_quantity": order.filled_quantity
            }
            
    async def cancel_order(
        self,
        order_id: str,
        user_id: str
    ) -> Dict[str, Any]:
        """Cancel a spot order"""
        # Load order
        order = await self._load_order(order_id)
        if not order:
            return {"success": False, "error": "Order not found"}
            
        if order.user_id != user_id:
            return {"success": False, "error": "Unauthorized"}
            
        if order.status != "pending":
            return {"success": False, "error": f"Cannot cancel {order.status} order"}
            
        # Remove from order book
        order_book_key = f"{order.resource_type}:{order.location_preference or 'any'}"
        self._remove_from_order_book(order_book_key, order)
        
        # Update status
        order.status = "cancelled"
        await self._update_order(order)
        
        return {
            "success": True,
            "order_id": order_id,
            "cancelled_quantity": order.quantity - order.filled_quantity
        }
        
    async def get_spot_price(
        self,
        resource_type: str,
        location: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get current spot price"""
        key = f"{resource_type}:{location or 'global'}"
        
        # Get from oracle
        oracle_price = await self.oracle.get_aggregated_price(
            f"COMPUTE_{resource_type.upper()}_{location or 'GLOBAL'}"
        )
        
        # Get from recent trades
        market_price = self.spot_prices.get(key)
        
        # Get best bid/ask
        order_book = self.order_books.get(key, {"buy": [], "sell": []})
        best_bid = max(
            (o.price for o in order_book["buy"] if o.price),
            default=None
        )
        best_ask = min(
            (o.price for o in order_book["sell"] if o.price),
            default=None
        )
        
        return {
            "resource_type": resource_type,
            "location": location,
            "oracle_price": str(oracle_price.price) if oracle_price else None,
            "last_trade_price": str(market_price) if market_price else None,
            "best_bid": str(best_bid) if best_bid else None,
            "best_ask": str(best_ask) if best_ask else None,
            "spread": str(best_ask - best_bid) if best_bid and best_ask else None,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    async def get_order_book(
        self,
        resource_type: str,
        location: Optional[str] = None,
        depth: int = 20
    ) -> Dict[str, Any]:
        """Get order book depth"""
        key = f"{resource_type}:{location or 'any'}"
        order_book = self.order_books.get(key, {"buy": [], "sell": []})
        
        # Aggregate by price level
        buy_levels = self._aggregate_orders(order_book["buy"], "buy", depth)
        sell_levels = self._aggregate_orders(order_book["sell"], "sell", depth)
        
        return {
            "resource_type": resource_type,
            "location": location,
            "bids": buy_levels,
            "asks": sell_levels,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    async def get_available_resources(
        self,
        resource_type: Optional[str] = None,
        location: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get available spot resources"""
        resources = []
        
        for resource_id, resource in self.resources.items():
            if resource_type and resource.resource_type != resource_type:
                continue
            if location and resource.location != location:
                continue
            if resource.state != ResourceState.AVAILABLE:
                continue
                
            resources.append({
                "resource_id": resource_id,
                "provider_id": resource.provider_id,
                "resource_type": resource.resource_type,
                "available_capacity": str(resource.available_capacity),
                "location": resource.location,
                "tier": resource.tier.value,
                "current_price": str(resource.current_price),
                "performance_score": resource.performance_score
            })
            
        return sorted(resources, key=lambda r: float(r["current_price"]))
        
    async def _execute_market_order(
        self,
        order: SpotOrder
    ) -> List[SpotTrade]:
        """Execute market order immediately"""
        trades = []
        remaining = order.quantity
        
        order_book_key = f"{order.resource_type}:{order.location_preference or 'any'}"
        order_book = self.order_books.get(order_book_key, {"buy": [], "sell": []})
        
        # Get counter side
        counter_orders = order_book["sell" if order.side == "buy" else "buy"]
        
        # Sort by price (best first)
        if order.side == "buy":
            counter_orders.sort(key=lambda o: o.price or Decimal("999999"))
        else:
            counter_orders.sort(key=lambda o: o.price or Decimal("0"), reverse=True)
            
        # Match against counter orders
        for counter_order in counter_orders[:]:
            if remaining <= 0:
                break
                
            match_quantity = min(
                remaining,
                counter_order.quantity - counter_order.filled_quantity
            )
            
            if match_quantity > 0:
                # Create trade
                trade = await self._create_trade(
                    order,
                    counter_order,
                    match_quantity,
                    counter_order.price
                )
                trades.append(trade)
                
                # Update quantities
                remaining -= match_quantity
                counter_order.filled_quantity += match_quantity
                order.filled_quantity += match_quantity
                
                # Update counter order
                if counter_order.filled_quantity >= counter_order.quantity:
                    counter_order.status = "filled"
                    counter_orders.remove(counter_order)
                    
                await self._update_order(counter_order)
                
        # If still remaining, try spot resources
        if remaining > 0 and order.side == "buy":
            resource_trades = await self._match_with_resources(order, remaining)
            trades.extend(resource_trades)
            
        # Update order status
        if order.filled_quantity >= order.quantity:
            order.status = "filled"
        elif order.filled_quantity > 0:
            order.status = "partially_filled"
            
        await self._update_order(order)
        
        return trades
        
    async def _match_with_resources(
        self,
        order: SpotOrder,
        quantity: Decimal
    ) -> List[SpotTrade]:
        """Match order with available spot resources"""
        trades = []
        remaining = quantity
        
        # Find matching resources
        matching_resources = []
        for resource in self.resources.values():
            if resource.resource_type != order.resource_type:
                continue
            if resource.state != ResourceState.AVAILABLE:
                continue
            if order.location_preference and resource.location != order.location_preference:
                continue
            if resource.available_capacity <= 0:
                continue
                
            matching_resources.append(resource)
            
        # Sort by price
        matching_resources.sort(key=lambda r: r.current_price)
        
        # Allocate from resources
        for resource in matching_resources:
            if remaining <= 0:
                break
                
            allocate_quantity = min(remaining, resource.available_capacity)
            
            # Create synthetic sell order from resource
            sell_order = SpotOrder(
                order_id=f"resource_{resource.resource_id}_{uuid.uuid4().hex[:8]}",
                user_id=resource.provider_id,
                order_type=SpotOrderType.LIMIT,
                side="sell",
                resource_type=resource.resource_type,
                quantity=allocate_quantity,
                price=resource.current_price,
                location_preference=resource.location
            )
            
            # Create trade
            trade = await self._create_trade(
                order,
                sell_order,
                allocate_quantity,
                resource.current_price
            )
            trades.append(trade)
            
            # Update resource
            resource.available_capacity -= allocate_quantity
            resource.state = ResourceState.RESERVED if resource.available_capacity == 0 else resource.state
            await self._update_resource(resource)
            
            # Update quantities
            remaining -= allocate_quantity
            order.filled_quantity += allocate_quantity
            
        return trades
        
    async def _create_trade(
        self,
        buy_order: SpotOrder,
        sell_order: SpotOrder,
        quantity: Decimal,
        price: Decimal
    ) -> SpotTrade:
        """Create and record a trade"""
        trade = SpotTrade(
            trade_id=f"SPOT_{uuid.uuid4().hex}",
            buy_order_id=buy_order.order_id,
            sell_order_id=sell_order.order_id,
            buyer_id=buy_order.user_id,
            seller_id=sell_order.user_id,
            resource_type=buy_order.resource_type,
            quantity=quantity,
            price=price,
            location=sell_order.location_preference or buy_order.location_preference or "any"
        )
        
        # Store trade
        await self._store_trade(trade)
        
        # Update spot price
        key = f"{trade.resource_type}:{trade.location}"
        self.spot_prices[key] = price
        self.price_history[key].append((datetime.utcnow(), price))
        
        # Update metrics
        self.metrics["total_volume_24h"] += quantity * price
        self.metrics["trades_count_24h"] += 1
        
        # Emit trade event
        await self.pulsar.publish('compute.spot.trade', {
            'trade_id': trade.trade_id,
            'resource_type': trade.resource_type,
            'quantity': str(trade.quantity),
            'price': str(trade.price),
            'location': trade.location,
            'buyer_id': trade.buyer_id,
            'seller_id': trade.seller_id,
            'timestamp': trade.execution_time.isoformat()
        })
        
        # Initiate provisioning
        asyncio.create_task(self._initiate_provisioning(trade))
        
        return trade
        
    async def _initiate_provisioning(self, trade: SpotTrade):
        """Initiate resource provisioning after trade"""
        try:
            # Call provisioning service
            provisioning_result = await self._provision_resources(trade)
            
            # Update trade with access details
            trade.provisioning_time = datetime.utcnow()
            trade.access_details = provisioning_result
            await self._update_trade(trade)
            
            # Notify buyer
            await self.pulsar.publish('compute.spot.provisioned', {
                'trade_id': trade.trade_id,
                'buyer_id': trade.buyer_id,
                'access_details': provisioning_result,
                'timestamp': datetime.utcnow().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Provisioning failed for trade {trade.trade_id}: {e}")
            # Handle provisioning failure
            await self._handle_provisioning_failure(trade)
            
    async def _continuous_matching_loop(self):
        """Continuous order matching loop"""
        while True:
            try:
                # Match orders for each order book
                for order_book_key in list(self.order_books.keys()):
                    await self._match_order_book(order_book_key)
                    
                await asyncio.sleep(0.1)  # 100ms matching cycle
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in matching loop: {e}")
                await asyncio.sleep(1)
                
    async def _price_update_loop(self):
        """Update spot prices from various sources"""
        while True:
            try:
                # Update prices for each resource type
                for resource_type in ["gpu", "cpu", "storage", "bandwidth"]:
                    # Get oracle price
                    oracle_price = await self.oracle.get_aggregated_price(
                        f"COMPUTE_{resource_type.upper()}"
                    )
                    
                    if oracle_price:
                        # Update resource prices based on oracle
                        await self._update_resource_prices(
                            resource_type,
                            oracle_price.price
                        )
                        
                # Clean old price history
                await self._clean_price_history()
                
                await asyncio.sleep(5)  # Update every 5 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in price update loop: {e}")
                await asyncio.sleep(10)
                
    async def _resource_sync_loop(self):
        """Sync available resources from partners"""
        while True:
            try:
                # Get inventory from partner manager
                for resource_type in ["gpu", "cpu", "storage", "bandwidth"]:
                    inventory = await self.partner_manager.get_available_inventory(
                        resource_type
                    )
                    
                    # Update resources
                    for inv in inventory:
                        resource_id = f"{inv.provider.value}_{inv.region}_{inv.resource_type}"
                        
                        if resource_id in self.resources:
                            # Update existing
                            resource = self.resources[resource_id]
                            resource.available_capacity = inv.available_capacity
                            resource.current_price = inv.retail_price
                        else:
                            # Add new resource
                            resource = SpotResource(
                                resource_id=resource_id,
                                provider_id=inv.provider.value,
                                resource_type=inv.resource_type,
                                capacity=inv.total_capacity,
                                available_capacity=inv.available_capacity,
                                location=inv.region,
                                tier=CapacityTier.PARTNER_WHOLESALE,
                                min_price=inv.wholesale_price,
                                current_price=inv.retail_price,
                                state=ResourceState.AVAILABLE
                            )
                            self.resources[resource_id] = resource
                            
                        await self._update_resource(resource)
                        
                await asyncio.sleep(30)  # Sync every 30 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in resource sync loop: {e}")
                await asyncio.sleep(60)
                
    def _add_to_order_book(self, key: str, order: SpotOrder):
        """Add order to order book maintaining price-time priority"""
        order_book = self.order_books[key]
        orders = order_book[order.side]
        
        # Insert maintaining price-time priority
        if order.side == "buy":
            # Higher price = higher priority for buy orders
            orders.append(order)
            orders.sort(key=lambda o: (-(o.price or 0), o.created_at))
        else:
            # Lower price = higher priority for sell orders
            orders.append(order)
            orders.sort(key=lambda o: (o.price or float('inf'), o.created_at))
            
    def _remove_from_order_book(self, key: str, order: SpotOrder):
        """Remove order from order book"""
        if key in self.order_books:
            order_book = self.order_books[key]
            if order in order_book[order.side]:
                order_book[order.side].remove(order)
                
    def _aggregate_orders(
        self,
        orders: List[SpotOrder],
        side: str,
        depth: int
    ) -> List[Dict[str, str]]:
        """Aggregate orders by price level"""
        price_levels = defaultdict(Decimal)
        
        for order in orders:
            if order.price:
                remaining = order.quantity - order.filled_quantity
                price_levels[order.price] += remaining
                
        # Sort and limit
        if side == "buy":
            sorted_levels = sorted(price_levels.items(), reverse=True)
        else:
            sorted_levels = sorted(price_levels.items())
            
        return [
            {"price": str(price), "quantity": str(qty)}
            for price, qty in sorted_levels[:depth]
        ]
        
    def _calculate_avg_price(self, trades: List[SpotTrade]) -> Optional[Decimal]:
        """Calculate volume-weighted average price"""
        if not trades:
            return None
            
        total_value = sum(t.price * t.quantity for t in trades)
        total_quantity = sum(t.quantity for t in trades)
        
        return total_value / total_quantity if total_quantity > 0 else None
        
    def _trade_to_dict(self, trade: SpotTrade) -> Dict[str, Any]:
        """Convert trade to dictionary"""
        return {
            "trade_id": trade.trade_id,
            "resource_type": trade.resource_type,
            "quantity": str(trade.quantity),
            "price": str(trade.price),
            "location": trade.location,
            "execution_time": trade.execution_time.isoformat()
        }
        
    async def _validate_order(self, order: SpotOrder) -> Dict[str, Any]:
        """Validate spot order"""
        # Check user authorization
        # Check resource limits
        # Check price bounds
        # etc.
        return {"valid": True}
        
    async def _store_order(self, order: SpotOrder):
        """Store order in cache"""
        await self.ignite.put(f"spot_order:{order.order_id}", order)
        self.user_orders[order.user_id].add(order.order_id)
        
    async def _update_order(self, order: SpotOrder):
        """Update order in cache"""
        await self.ignite.put(f"spot_order:{order.order_id}", order)
        
    async def _load_order(self, order_id: str) -> Optional[SpotOrder]:
        """Load order from cache"""
        return await self.ignite.get(f"spot_order:{order_id}")
        
    async def _store_trade(self, trade: SpotTrade):
        """Store trade in cache"""
        await self.ignite.put(f"spot_trade:{trade.trade_id}", trade)
        self.recent_trades.append(trade)
        
        # Keep only recent trades
        cutoff = datetime.utcnow() - timedelta(hours=24)
        self.recent_trades = [
            t for t in self.recent_trades if t.execution_time > cutoff
        ]
        
    async def _update_trade(self, trade: SpotTrade):
        """Update trade in cache"""
        await self.ignite.put(f"spot_trade:{trade.trade_id}", trade)
        
    async def _update_resource(self, resource: SpotResource):
        """Update resource in cache"""
        await self.ignite.put(f"spot_resource:{resource.resource_id}", resource)
        
    async def _update_resource_prices(
        self,
        resource_type: str,
        oracle_price: Decimal
    ):
        """Update resource prices based on oracle"""
        for resource in self.resources.values():
            if resource.resource_type == resource_type:
                # Adjust price based on tier and performance
                tier_multiplier = {
                    CapacityTier.PLATFORM_OWNED: Decimal("0.9"),
                    CapacityTier.PARTNER_WHOLESALE: Decimal("1.0"),
                    CapacityTier.SPOT_MARKET: Decimal("1.1"),
                    CapacityTier.PEER_TO_PEER: Decimal("1.2")
                }.get(resource.tier, Decimal("1.0"))
                
                performance_adj = Decimal(str(resource.performance_score))
                
                resource.current_price = oracle_price * tier_multiplier * performance_adj
                await self._update_resource(resource)
                
    async def _clean_price_history(self):
        """Clean old price history entries"""
        cutoff = datetime.utcnow() - timedelta(hours=24)
        
        for key in self.price_history:
            self.price_history[key] = [
                (ts, price) for ts, price in self.price_history[key]
                if ts > cutoff
            ]
            
    async def _load_market_state(self):
        """Load market state from cache"""
        # Load active orders
        # Load resources
        # Load recent trades
        pass
        
    async def _match_order_book(self, order_book_key: str):
        """Match orders in a specific order book"""
        order_book = self.order_books.get(order_book_key)
        if not order_book:
            return
            
        buy_orders = order_book["buy"]
        sell_orders = order_book["sell"]
        
        if not buy_orders or not sell_orders:
            return
            
        # Get best bid and ask
        best_bid = buy_orders[0] if buy_orders else None
        best_ask = sell_orders[0] if sell_orders else None
        
        # Check if orders cross
        if best_bid and best_ask and best_bid.price and best_ask.price:
            if best_bid.price >= best_ask.price:
                # Execute trade at mid price
                trade_price = (best_bid.price + best_ask.price) / 2
                trade_quantity = min(
                    best_bid.quantity - best_bid.filled_quantity,
                    best_ask.quantity - best_ask.filled_quantity
                )
                
                if trade_quantity > 0:
                    # Create trade
                    trade = await self._create_trade(
                        best_bid,
                        best_ask,
                        trade_quantity,
                        trade_price
                    )
                    
                    # Update orders
                    best_bid.filled_quantity += trade_quantity
                    best_ask.filled_quantity += trade_quantity
                    
                    if best_bid.filled_quantity >= best_bid.quantity:
                        best_bid.status = "filled"
                        buy_orders.remove(best_bid)
                        
                    if best_ask.filled_quantity >= best_ask.quantity:
                        best_ask.status = "filled"
                        sell_orders.remove(best_ask)
                        
                    await self._update_order(best_bid)
                    await self._update_order(best_ask)
                    
    async def _try_match_order(self, order: SpotOrder) -> List[SpotTrade]:
        """Try to match a newly added order"""
        trades = []
        order_book_key = f"{order.resource_type}:{order.location_preference or 'any'}"
        
        # Simple matching for limit orders
        await self._match_order_book(order_book_key)
        
        return trades
        
    async def _provision_resources(
        self,
        trade: SpotTrade
    ) -> Dict[str, Any]:
        """Provision resources after trade execution"""
        # This would call the provisioning service
        # For now, return mock access details
        return {
            "access_type": "ssh",
            "endpoint": f"{trade.resource_type}-{trade.trade_id}.compute.platformq.io",
            "credentials": {
                "username": "compute",
                "key_id": f"key_{trade.trade_id}"
            },
            "expires_at": (datetime.utcnow() + timedelta(hours=1)).isoformat()
        }
        
    async def _handle_provisioning_failure(self, trade: SpotTrade):
        """Handle provisioning failure"""
        # Reverse the trade
        # Refund the buyer
        # Notify both parties
        pass
        
    async def get_market_stats(self) -> Dict[str, Any]:
        """Get spot market statistics"""
        return {
            "total_volume_24h": str(self.metrics["total_volume_24h"]),
            "trades_count_24h": self.metrics["trades_count_24h"],
            "avg_execution_time_ms": self.metrics["avg_execution_time_ms"],
            "active_orders": sum(
                len(ob["buy"]) + len(ob["sell"])
                for ob in self.order_books.values()
            ),
            "available_resources": len([
                r for r in self.resources.values()
                if r.state == ResourceState.AVAILABLE
            ]),
            "spot_prices": {
                k: str(v) for k, v in self.spot_prices.items()
            }
        } 