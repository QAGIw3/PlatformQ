"""
Unified Order Matching Engine

Provides order matching capabilities for both social trading and prediction markets.
"""

from typing import Dict, List, Optional, Tuple
from decimal import Decimal
from dataclasses import dataclass
from enum import Enum
from datetime import datetime
import uuid
import asyncio
import logging

from pyignite import Client as IgniteClient
import pulsar

from platformq_shared.state_management import StateManagementClient, CacheConfig
from platformq_shared.risk_engine import RiskEngineClient
from .distributed_order_book import DistributedOrderBook

logger = logging.getLogger(__name__)


class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class OrderSide(Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


@dataclass
class Order:
    """Base order structure for all trading types"""
    order_id: str
    market_id: str
    trader_id: str
    side: OrderSide
    order_type: OrderType
    price: Optional[Decimal]
    quantity: Decimal
    filled_quantity: Decimal = Decimal(0)
    status: OrderStatus = OrderStatus.PENDING
    timestamp: datetime = None
    metadata: Dict = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()
        if self.metadata is None:
            self.metadata = {}


class UnifiedMatchingEngine:
    """Unified order matching engine with distributed state management"""
    
    def __init__(self, 
                 state_client: StateManagementClient,  # New dependency
                 pulsar_client: pulsar.Client,
                 risk_engine_client: RiskEngineClient):  # New dependency
        self.state = state_client
        self.pulsar = pulsar_client
        self.risk = risk_engine_client
        
        # Cache configuration for order books
        self.cache_config = CacheConfig(
            name="order_books",
            cache_mode="PARTITIONED",
            backups=2,
            atomicity_mode="TRANSACTIONAL",
            eviction_policy="LRU",
            eviction_max_size=1000000
        )
        
        # Initialize distributed order books
        asyncio.create_task(self._initialize_order_books())
    
    async def _initialize_order_books(self):
        """Initialize distributed order book caches"""
        await self.state.create_cache(self.cache_config)
        
        # Create cache for active orders
        await self.state.create_cache(CacheConfig(
            name="active_orders",
            cache_mode="REPLICATED",  # Replicated for fast reads
            backups=2,
            atomicity_mode="TRANSACTIONAL"
        ))
    
    async def submit_order(self, order: Order) -> str:
        """Submit order with risk validation"""
        # Validate with risk engine first
        risk_assessment = await self.risk.assess_order_risk(
            order_id=order.order_id,
            market_id=order.market_id,
            trader_id=order.trader_id,
            side=order.side.value,
            size=str(order.quantity),
            leverage=order.metadata.get("leverage", 1)
        )
        
        if not risk_assessment["approved"]:
            raise Exception(f"Order rejected by risk engine: {risk_assessment['reason']}")
        
        # Store order in distributed state
        await self.state.put(
            cache_name="active_orders",
            key=order.order_id,
            value=order.dict(),
            ttl=86400  # 24 hour TTL
        )
        
        # Get or create distributed order book
        order_book = await self._get_distributed_order_book(order.market_id)
        
        # Add to order book with transaction
        async with self.state.transaction() as tx:
            await tx.add_order(order_book, order)
            await tx.commit()
        
        # Publish order event
        await self._publish_order_event(order, "submitted")
        
        # Trigger matching
        await self._trigger_distributed_matching(order.market_id)
        
        return order.order_id
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an existing order"""
        order = await self.state.get("active_orders", order_id)
        if not order:
            return False
            
        if order["status"] not in [OrderStatus.OPEN.value, OrderStatus.PARTIALLY_FILLED.value]:
            return False
            
        # Remove from order book
        order_book = await self._get_distributed_order_book(order["market_id"])
        async with self.state.transaction() as tx:
            await tx.remove_order(order_book, order_id)
            await tx.commit()
            
        # Update order status
        order["status"] = OrderStatus.CANCELLED.value
        await self.state.put("active_orders", order_id, order)
        
        # Publish cancellation event
        await self._publish_order_event(order, "cancelled")
        
        return True
        
    async def _process_market_order(self, order: Order, order_book: DistributedOrderBook):
        """Process a market order"""
        matches = await self._find_matches(order, order_book)
        
        for match_order, match_quantity in matches:
            await self._execute_trade(order, match_order, match_quantity)
            
        if order.filled_quantity < order.quantity:
            # Convert remaining to limit order at last traded price
            # or cancel if no trades occurred
            if order.filled_quantity > 0:
                order.order_type = OrderType.LIMIT
                await order_book.add_order(order)
            else:
                order.status = OrderStatus.CANCELLED
                await self.state.put("active_orders", order.order_id, order)
                
    async def _process_limit_order(self, order: Order, order_book: DistributedOrderBook):
        """Process a limit order"""
        matches = await self._find_matches(order, order_book)
        
        for match_order, match_quantity in matches:
            await self._execute_trade(order, match_order, match_quantity)
            
        if order.filled_quantity < order.quantity:
            order.status = OrderStatus.OPEN
            await order_book.add_order(order)
            await self.state.put("active_orders", order.order_id, order)
            
    async def _find_matches(self, order: Order, order_book: DistributedOrderBook) -> List[Tuple[Order, Decimal]]:
        """Find matching orders in the book"""
        matches = []
        remaining_quantity = order.quantity - order.filled_quantity
        
        if order.side == OrderSide.BUY:
            # Match against sell orders
            for sell_order in order_book.sell_orders[:]:
                if order.order_type == OrderType.MARKET or order.price >= sell_order.price:
                    match_quantity = min(
                        remaining_quantity,
                        sell_order.quantity - sell_order.filled_quantity
                    )
                    matches.append((sell_order, match_quantity))
                    remaining_quantity -= match_quantity
                    
                    if remaining_quantity <= 0:
                        break
                        
        else:
            # Match against buy orders
            for buy_order in order_book.buy_orders[:]:
                if order.order_type == OrderType.MARKET or order.price <= buy_order.price:
                    match_quantity = min(
                        remaining_quantity,
                        buy_order.quantity - buy_order.filled_quantity
                    )
                    matches.append((buy_order, match_quantity))
                    remaining_quantity -= match_quantity
                    
                    if remaining_quantity <= 0:
                        break
                        
        return matches
        
    async def _execute_trade(self, order1: Order, order2: Order, quantity: Decimal):
        """Execute a trade between two orders"""
        # Determine trade price (price of order that was in book first)
        trade_price = order2.price
        
        # Update orders
        order1.filled_quantity += quantity
        order2.filled_quantity += quantity
        
        if order1.filled_quantity >= order1.quantity:
            order1.status = OrderStatus.FILLED
        else:
            order1.status = OrderStatus.PARTIALLY_FILLED
            
        if order2.filled_quantity >= order2.quantity:
            order2.status = OrderStatus.FILLED
        else:
            order2.status = OrderStatus.PARTIALLY_FILLED
            
        # Store updated orders
        await self.state.put("active_orders", order1.order_id, order1)
        await self.state.put("active_orders", order2.order_id, order2)
        
        # Remove filled orders from book
        if order2.status == OrderStatus.FILLED:
            order_book = await self._get_distributed_order_book(order2.market_id)
            async with self.state.transaction() as tx:
                await tx.remove_order(order_book, order2.order_id)
                await tx.commit()
            
        # Create and store trade record
        trade = {
            "trade_id": str(uuid.uuid4()),
            "market_id": order1.market_id,
            "buyer_order_id": order1.order_id if order1.side == OrderSide.BUY else order2.order_id,
            "seller_order_id": order2.order_id if order2.side == OrderSide.SELL else order1.order_id,
            "price": str(trade_price),
            "quantity": str(quantity),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        await self.state.put("trades", trade["trade_id"], trade)
        
        # Publish trade event
        await self._publish_trade_event(trade)
        
    async def _continuous_matching(self, market_id: str):
        """Continuous matching loop for a market"""
        while True: # Infinite loop for continuous matching
            try:
                order_book = await self._get_distributed_order_book(market_id)
                
                # Check for crossing orders
                best_bid = await order_book.get_best_bid()
                best_ask = await order_book.get_best_ask()
                
                if best_bid and best_ask and best_bid >= best_ask:
                    # Execute trades for crossing orders
                    buy_order = order_book.buy_orders[0]
                    sell_order = order_book.sell_orders[0]
                    
                    quantity = min(
                        buy_order.quantity - buy_order.filled_quantity,
                        sell_order.quantity - sell_order.filled_quantity
                    )
                    
                    await self._execute_trade(buy_order, sell_order, quantity)
                else:
                    # No crossing orders, wait a bit
                    await asyncio.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"Error in continuous matching for {market_id}: {e}")
                await asyncio.sleep(1)
                
    async def _publish_trade_event(self, trade: dict):
        """Publish trade event to Pulsar"""
        try:
            self.pulsar.create_producer(
                "persistent://platformq/trading/trades"
            ).send(
                trade,
                properties={"market_id": trade["market_id"]}
            )
        except Exception as e:
            logger.error(f"Error publishing trade event: {e}")
            
    async def _publish_order_event(self, order: Order, event_type: str):
        """Publish order event to Pulsar"""
        try:
            event = {
                "event_type": event_type,
                "order_id": order.order_id,
                "market_id": order.market_id,
                "trader_id": order.trader_id,
                "status": order.status.value,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            self.pulsar.create_producer(
                "persistent://platformq/trading/orders"
            ).send(
                event,
                properties={"event_type": event_type, "market_id": order.market_id}
            )
        except Exception as e:
            logger.error(f"Error publishing order event: {e}")
            
    def _validate_order(self, order: Order) -> bool:
        """Validate order parameters"""
        if order.quantity <= 0:
            return False
            
        if order.order_type == OrderType.LIMIT and order.price <= 0:
            return False
            
        return True
        
    def register_matching_rule(self, market_type: str, rule_func: callable):
        """Register custom matching rules for specific market types"""
        # This method is no longer directly applicable with distributed state
        # as order books are managed by the state management service.
        # Custom rules would need to be implemented within the state management service
        # or passed as part of the order submission process.
        logger.warning(f"register_matching_rule is deprecated for distributed matching.")
        
    async def get_order_book_snapshot(self, market_id: str) -> dict:
        """Get current order book snapshot"""
        order_book = await self._get_distributed_order_book(market_id)
        
        async with order_book._lock: # This lock is no longer needed as state is distributed
            bids = [
                {
                    "price": str(order.price),
                    "quantity": str(order.quantity - order.filled_quantity),
                    "order_id": order.order_id
                }
                for order in order_book.buy_orders
                if order.status in [OrderStatus.OPEN.value, OrderStatus.PARTIALLY_FILLED.value]
            ]
            
            asks = [
                {
                    "price": str(order.price),
                    "quantity": str(order.quantity - order.filled_quantity),
                    "order_id": order.order_id
                }
                for order in order_book.sell_orders
                if order.status in [OrderStatus.OPEN.value, OrderStatus.PARTIALLY_FILLED.value]
            ]
            
        return {"bids": bids, "asks": asks} 
            
    async def _get_distributed_order_book(self, market_id: str) -> DistributedOrderBook:
        """Get order book from distributed state"""
        cache_key = f"orderbook:{market_id}"
        
        # Try to get from state
        order_book_data = await self.state.get("order_books", cache_key)
        
        if order_book_data:
            return DistributedOrderBook.from_dict(order_book_data)
        
        # Create new order book
        order_book = DistributedOrderBook(market_id)
        await self.state.put("order_books", cache_key, order_book.to_dict())
        
        return order_book
    
    async def _trigger_distributed_matching(self, market_id: str):
        """Trigger matching across distributed nodes"""
        # Use Ignite compute grid for distributed matching
        await self.state.execute_compute_task(
            task_name="match_orders",
            params={"market_id": market_id},
            affinity_key=market_id  # Execute on node holding this market's data
        ) 