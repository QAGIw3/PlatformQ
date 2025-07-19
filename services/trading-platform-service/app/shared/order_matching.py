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


class OrderBook:
    """Order book for a specific market"""
    
    def __init__(self, market_id: str):
        self.market_id = market_id
        self.buy_orders: List[Order] = []
        self.sell_orders: List[Order] = []
        self._lock = asyncio.Lock()
        
    async def add_order(self, order: Order) -> None:
        """Add order to the book"""
        async with self._lock:
            if order.side == OrderSide.BUY:
                self.buy_orders.append(order)
                self.buy_orders.sort(key=lambda x: (-x.price, x.timestamp))
            else:
                self.sell_orders.append(order)
                self.sell_orders.sort(key=lambda x: (x.price, x.timestamp))
                
    async def remove_order(self, order_id: str) -> Optional[Order]:
        """Remove order from the book"""
        async with self._lock:
            for order in self.buy_orders:
                if order.order_id == order_id:
                    self.buy_orders.remove(order)
                    return order
                    
            for order in self.sell_orders:
                if order.order_id == order_id:
                    self.sell_orders.remove(order)
                    return order
                    
        return None
        
    async def get_best_bid(self) -> Optional[Decimal]:
        """Get best bid price"""
        async with self._lock:
            if self.buy_orders:
                return self.buy_orders[0].price
        return None
        
    async def get_best_ask(self) -> Optional[Decimal]:
        """Get best ask price"""
        async with self._lock:
            if self.sell_orders:
                return self.sell_orders[0].price
        return None


class UnifiedMatchingEngine:
    """
    Unified order matching engine for all trading types.
    Supports both traditional order matching and prediction market mechanisms.
    """
    
    def __init__(self, 
                 ignite_client: IgniteClient,
                 pulsar_client: pulsar.Client):
        self.ignite = ignite_client
        self.pulsar = pulsar_client
        self.order_books: Dict[str, OrderBook] = {}
        self.matching_rules: Dict[str, callable] = {}
        self._matching_tasks: Dict[str, asyncio.Task] = {}
        
        # Initialize caches
        self.orders_cache = self.ignite.get_or_create_cache("orders")
        self.trades_cache = self.ignite.get_or_create_cache("trades")
        
        # Initialize event publisher
        self.trade_producer = self.pulsar.create_producer(
            "persistent://platformq/trading/trades"
        )
        
    async def submit_order(self, order: Order) -> str:
        """Submit a new order to the matching engine"""
        # Validate order
        if not self._validate_order(order):
            raise ValueError("Invalid order")
            
        # Store order in cache
        self.orders_cache.put(order.order_id, order)
        
        # Get or create order book
        if order.market_id not in self.order_books:
            self.order_books[order.market_id] = OrderBook(order.market_id)
            
        order_book = self.order_books[order.market_id]
        
        # Handle different order types
        if order.order_type == OrderType.MARKET:
            await self._process_market_order(order, order_book)
        elif order.order_type == OrderType.LIMIT:
            await self._process_limit_order(order, order_book)
            
        # Start matching if not already running
        if order.market_id not in self._matching_tasks:
            self._matching_tasks[order.market_id] = asyncio.create_task(
                self._continuous_matching(order.market_id)
            )
            
        return order.order_id
        
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an existing order"""
        order = self.orders_cache.get(order_id)
        if not order:
            return False
            
        if order.status not in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
            return False
            
        # Remove from order book
        order_book = self.order_books.get(order.market_id)
        if order_book:
            await order_book.remove_order(order_id)
            
        # Update order status
        order.status = OrderStatus.CANCELLED
        self.orders_cache.put(order_id, order)
        
        # Publish cancellation event
        await self._publish_order_event(order, "cancelled")
        
        return True
        
    async def _process_market_order(self, order: Order, order_book: OrderBook):
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
                self.orders_cache.put(order.order_id, order)
                
    async def _process_limit_order(self, order: Order, order_book: OrderBook):
        """Process a limit order"""
        matches = await self._find_matches(order, order_book)
        
        for match_order, match_quantity in matches:
            await self._execute_trade(order, match_order, match_quantity)
            
        if order.filled_quantity < order.quantity:
            order.status = OrderStatus.OPEN
            await order_book.add_order(order)
            self.orders_cache.put(order.order_id, order)
            
    async def _find_matches(self, order: Order, order_book: OrderBook) -> List[Tuple[Order, Decimal]]:
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
        self.orders_cache.put(order1.order_id, order1)
        self.orders_cache.put(order2.order_id, order2)
        
        # Remove filled orders from book
        if order2.status == OrderStatus.FILLED:
            order_book = self.order_books[order2.market_id]
            await order_book.remove_order(order2.order_id)
            
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
        
        self.trades_cache.put(trade["trade_id"], trade)
        
        # Publish trade event
        await self._publish_trade_event(trade)
        
    async def _continuous_matching(self, market_id: str):
        """Continuous matching loop for a market"""
        while market_id in self.order_books:
            try:
                order_book = self.order_books[market_id]
                
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
            self.trade_producer.send(
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
            
            self.trade_producer.send(
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
        self.matching_rules[market_type] = rule_func
        
    async def get_order_book_snapshot(self, market_id: str) -> dict:
        """Get current order book snapshot"""
        order_book = self.order_books.get(market_id)
        if not order_book:
            return {"bids": [], "asks": []}
            
        async with order_book._lock:
            bids = [
                {
                    "price": str(order.price),
                    "quantity": str(order.quantity - order.filled_quantity),
                    "order_id": order.order_id
                }
                for order in order_book.buy_orders
                if order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]
            ]
            
            asks = [
                {
                    "price": str(order.price),
                    "quantity": str(order.quantity - order.filled_quantity),
                    "order_id": order.order_id
                }
                for order in order_book.sell_orders
                if order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]
            ]
            
        return {"bids": bids, "asks": asks} 