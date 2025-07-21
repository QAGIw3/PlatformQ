"""Unified matching engine for all product types."""

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any, Set
import uuid
import time
from concurrent.futures import ThreadPoolExecutor
import threading
from dataclasses import dataclass, field

from ..models.order import Order, OrderType, OrderSide, OrderStatus
from ..models.trade import Trade, TradeSide
from ..models.orderbook import OrderBook, OrderBookUpdate
from ..state import IgniteStateManager, CacheType
from ..events import FlinkEventProcessor, OrderEvent, TradeEvent, EventType


logger = logging.getLogger(__name__)


class MatchingAlgorithm(str, Enum):
    """Matching algorithms."""
    PRICE_TIME = "price_time"  # First In First Out at price level
    PRO_RATA = "pro_rata"  # Proportional matching
    TIME_WEIGHTED = "time_weighted"  # Time-weighted average
    
    
@dataclass
class MarketConfig:
    """Market configuration."""
    market_id: str
    product_type: str
    tick_size: Decimal
    lot_size: Decimal
    max_order_size: Decimal = Decimal("1000000")
    price_bands: Tuple[Decimal, Decimal] = (Decimal("0.9"), Decimal("1.1"))  # 10% bands
    circuit_breaker_threshold: Decimal = Decimal("0.05")  # 5% move triggers halt
    halt_duration_seconds: int = 300  # 5 minutes


@dataclass
class MatchingMetrics:
    """Performance metrics for matching engine."""
    orders_processed: int = 0
    trades_executed: int = 0
    orders_cancelled: int = 0
    orders_rejected: int = 0
    total_volume: Decimal = Decimal("0")
    latency_histogram: List[float] = field(default_factory=list)
    last_reset: datetime = field(default_factory=datetime.utcnow)


class CircuitBreaker:
    """Circuit breaker for market protection."""
    
    def __init__(self):
        self.halted_markets: Dict[str, datetime] = {}
        self.reference_prices: Dict[str, Decimal] = {}
        self._lock = threading.Lock()
    
    def update_reference_price(self, market_id: str, price: Decimal):
        """Update reference price for circuit breaker monitoring."""
        with self._lock:
            if market_id not in self.reference_prices:
                self.reference_prices[market_id] = price
            else:
                # Update using exponential moving average
                alpha = 0.1
                self.reference_prices[market_id] = (
                    alpha * price + (1 - alpha) * self.reference_prices[market_id]
                )
    
    def check_circuit_breaker(
        self, 
        market_id: str, 
        current_price: Decimal,
        config: MarketConfig
    ) -> bool:
        """Check if circuit breaker should be triggered."""
        with self._lock:
            if market_id not in self.reference_prices:
                self.reference_prices[market_id] = current_price
                return False
            
            ref_price = self.reference_prices[market_id]
            if ref_price == 0:
                return False
            
            price_change = abs((current_price - ref_price) / ref_price)
            
            if price_change > config.circuit_breaker_threshold:
                self.halt_market(market_id, config.halt_duration_seconds)
                return True
            
            return False
    
    def halt_market(self, market_id: str, duration_seconds: int):
        """Halt trading in a market."""
        with self._lock:
            self.halted_markets[market_id] = datetime.utcnow().timestamp() + duration_seconds
            logger.warning(f"Market {market_id} halted for {duration_seconds} seconds")
    
    def is_market_halted(self, market_id: str) -> bool:
        """Check if market is currently halted."""
        with self._lock:
            if market_id in self.halted_markets:
                halt_end = self.halted_markets[market_id]
                if datetime.utcnow().timestamp() < halt_end:
                    return True
                else:
                    # Halt expired
                    del self.halted_markets[market_id]
            return False


class MatchingEngine:
    """High-performance matching engine for all product types."""
    
    def __init__(
        self,
        state_manager: IgniteStateManager,
        event_processor: FlinkEventProcessor,
        algorithm: MatchingAlgorithm = MatchingAlgorithm.PRICE_TIME,
        num_workers: int = 4
    ):
        self.state_manager = state_manager
        self.event_processor = event_processor
        self.algorithm = algorithm
        
        # In-memory order books
        self.order_books: Dict[str, OrderBook] = {}
        
        # Pending orders by market (stop orders)
        self.pending_orders: Dict[str, List[Order]] = {}
        
        # Market configurations
        self.market_configs: Dict[str, MarketConfig] = {}
        
        # Circuit breaker
        self.circuit_breaker = CircuitBreaker()
        
        # Thread pool for parallel processing
        self.executor = ThreadPoolExecutor(max_workers=num_workers)
        
        # Performance metrics by market
        self.metrics: Dict[str, MatchingMetrics] = {}
        self.global_metrics = MatchingMetrics()
        
        # Sequence number for order processing
        self._sequence_lock = threading.Lock()
        self._sequence_number = 0
        
        # Background tasks
        self._running = False
        self._tasks: List[asyncio.Task] = []
    
    async def initialize(self):
        """Initialize matching engine."""
        # Load markets and create order books
        markets = await self.state_manager.get_active_markets()
        for market in markets:
            config = MarketConfig(
                market_id=market['market_id'],
                product_type=market.get('product_type', 'spot'),
                tick_size=Decimal(market['tick_size']),
                lot_size=Decimal(market.get('lot_size', '0.001')),
                max_order_size=Decimal(market.get('max_order_size', '1000000'))
            )
            self.market_configs[market['market_id']] = config
            
            order_book = OrderBook(
                market_id=market['market_id'],
                tick_size=config.tick_size
            )
            self.order_books[market['market_id']] = order_book
            
            # Initialize metrics
            self.metrics[market['market_id']] = MatchingMetrics()
        
        # Start background tasks
        self._running = True
        self._tasks.append(asyncio.create_task(self._market_data_publisher()))
        self._tasks.append(asyncio.create_task(self._metrics_reporter()))
        
        logger.info(f"Initialized matching engine with {len(self.order_books)} markets")
    
    async def process_order(self, order: Order) -> Dict[str, Any]:
        """Process a new order with full metrics."""
        start_time = time.time_ns()
        
        try:
            # Assign sequence number
            with self._sequence_lock:
                self._sequence_number += 1
                order._sequence = self._sequence_number
            
            # Check circuit breaker
            if self.circuit_breaker.is_market_halted(order.market_id):
                order.status = OrderStatus.REJECTED
                self._update_metrics(order.market_id, "orders_rejected", 1)
                return {
                    "success": False,
                    "order_id": order.order_id,
                    "reason": "Market halted",
                    "timestamp": time.time_ns()
                }
            
            # Validate order
            if not await self._validate_order(order):
                order.status = OrderStatus.REJECTED
                await self._publish_order_event(order, EventType.ORDER_CANCEL)
                self._update_metrics(order.market_id, "orders_rejected", 1)
                return {
                    "success": False,
                    "order_id": order.order_id,
                    "reason": "Order validation failed",
                    "timestamp": time.time_ns()
                }
            
            # Get order book
            order_book = self.order_books.get(order.market_id)
            if not order_book:
                logger.error(f"Order book not found for market {order.market_id}")
                order.status = OrderStatus.REJECTED
                self._update_metrics(order.market_id, "orders_rejected", 1)
                return {
                    "success": False,
                    "order_id": order.order_id,
                    "reason": "Market not found",
                    "timestamp": time.time_ns()
                }
            
            # Process order in thread pool for parallelism
            trades = await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self._process_order_sync,
                order,
                order_book
            )
            
            # Check circuit breaker after trades
            if trades:
                last_price = trades[-1].price
                config = self.market_configs.get(order.market_id)
                if config:
                    self.circuit_breaker.check_circuit_breaker(
                        order.market_id,
                        last_price,
                        config
                    )
            
            # Update metrics
            self._update_metrics(order.market_id, "orders_processed", 1)
            self._update_metrics(order.market_id, "trades_executed", len(trades))
            
            # Publish events
            await self._publish_order_event(order, EventType.ORDER_NEW)
            for trade in trades:
                await self._publish_trade_event(trade)
                self._update_metrics(order.market_id, "total_volume", trade.value)
            
            # Record latency
            latency_ns = time.time_ns() - start_time
            self._record_latency(order.market_id, latency_ns)
            
            return {
                "success": True,
                "order_id": order.order_id,
                "status": order.status.value,
                "filled_quantity": str(order.filled_quantity),
                "remaining_quantity": str(order.quantity - order.filled_quantity),
                "trades": [t.dict() for t in trades],
                "latency_ns": latency_ns,
                "timestamp": time.time_ns()
            }
            
        except Exception as e:
            logger.error(f"Error processing order: {e}")
            self._update_metrics(order.market_id, "orders_rejected", 1)
            return {
                "success": False,
                "order_id": order.order_id,
                "reason": str(e),
                "timestamp": time.time_ns()
            }
    
    def _process_order_sync(self, order: Order, order_book: OrderBook) -> List[Trade]:
        """Synchronous order processing for thread pool execution."""
        trades = []
        
        # Handle different order types
        if order.type == OrderType.MARKET:
            trades = self._process_market_order_sync(order, order_book)
        elif order.type == OrderType.LIMIT:
            trades = self._process_limit_order_sync(order, order_book)
        elif order.type == OrderType.STOP:
            self._process_stop_order_sync(order)
        elif order.type == OrderType.STOP_LIMIT:
            self._process_stop_limit_order_sync(order)
        else:
            logger.warning(f"Unsupported order type: {order.type}")
            order.status = OrderStatus.REJECTED
        
        return trades
    
    def _process_market_order_sync(
        self,
        order: Order,
        order_book: OrderBook
    ) -> List[Trade]:
        """Process market order synchronously."""
        trades = []
        remaining_quantity = order.quantity
        
        # Get matches from order book
        matches = order_book.match_order(
            side=order.side.value,
            quantity=remaining_quantity
        )
        
        # Create trades
        for price, quantity in matches:
            trade = self._create_trade_sync(
                order,
                price,
                quantity,
                order_book.market_id
            )
            trades.append(trade)
            
            # Update order
            order.filled_quantity += quantity
            remaining_quantity -= quantity
            
            # Remove liquidity from book
            opposite_side = "sell" if order.side == OrderSide.BUY else "buy"
            order_book.remove_order(opposite_side, price, quantity)
        
        # Update order status
        if order.filled_quantity >= order.quantity:
            order.status = OrderStatus.FILLED
        elif order.filled_quantity > 0:
            order.status = OrderStatus.PARTIALLY_FILLED
        else:
            order.status = OrderStatus.CANCELLED  # No liquidity
        
        return trades
    
    def _process_limit_order_sync(
        self,
        order: Order,
        order_book: OrderBook
    ) -> List[Trade]:
        """Process limit order synchronously."""
        trades = []
        remaining_quantity = order.quantity
        
        # Check for immediate matches
        best_bid, best_ask = order_book.get_best_bid_ask()
        
        can_match = False
        if order.side == OrderSide.BUY and best_ask and order.price >= best_ask:
            can_match = True
        elif order.side == OrderSide.SELL and best_bid and order.price <= best_bid:
            can_match = True
        
        if can_match:
            # Match against book
            matches = order_book.match_order(
                side=order.side.value,
                quantity=remaining_quantity,
                limit_price=order.price
            )
            
            # Create trades
            for price, quantity in matches:
                trade = self._create_trade_sync(
                    order,
                    price,
                    quantity,
                    order_book.market_id
                )
                trades.append(trade)
                
                order.filled_quantity += quantity
                remaining_quantity -= quantity
                
                # Remove liquidity
                opposite_side = "sell" if order.side == OrderSide.BUY else "buy"
                order_book.remove_order(opposite_side, price, quantity)
        
        # Add remaining to book if not filled
        if remaining_quantity > 0:
            if order.type != OrderType.FILL_OR_KILL:
                order_book.add_order(
                    side=order.side.value,
                    price=order.price,
                    quantity=remaining_quantity
                )
                order.status = OrderStatus.OPEN if order.filled_quantity == 0 else OrderStatus.PARTIALLY_FILLED
            else:
                # FOK order - cancel if not fully filled
                order.status = OrderStatus.CANCELLED
        else:
            order.status = OrderStatus.FILLED
        
        return trades
    
    def _process_stop_order_sync(self, order: Order):
        """Process stop order synchronously."""
        # Stop orders are stored and triggered when price reaches stop price
        if order.market_id not in self.pending_orders:
            self.pending_orders[order.market_id] = []
        
        self.pending_orders[order.market_id].append(order)
        order.status = OrderStatus.PENDING
    
    def _process_stop_limit_order_sync(self, order: Order):
        """Process stop-limit order synchronously."""
        # Similar to stop order but converts to limit when triggered
        if order.market_id not in self.pending_orders:
            self.pending_orders[order.market_id] = []
        
        self.pending_orders[order.market_id].append(order)
        order.status = OrderStatus.PENDING
    
    def _create_trade_sync(
        self,
        order: Order,
        price: Decimal,
        quantity: Decimal,
        market_id: str
    ) -> Trade:
        """Create a trade record synchronously."""
        trade = Trade(
            trade_id=str(uuid.uuid4()),
            market_id=market_id,
            product_type=order.product_type,
            taker_order_id=order.order_id,
            maker_order_id="",  # Would be set from matched order
            taker_user_id=order.user_id,
            maker_user_id="",  # Would be set from matched order
            price=price,
            quantity=quantity,
            side=TradeSide.BUY if order.side == OrderSide.BUY else TradeSide.SELL
        )
        
        trade.calculate_value()
        
        return trade
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        # Get order from state
        order_data = await self.state_manager.get_order(order_id)
        if not order_data:
            return False
        
        order = Order(**order_data)
        
        # Check if order can be cancelled
        if order.status in [OrderStatus.FILLED, OrderStatus.CANCELLED]:
            return False
        
        # Remove from order book if active
        if order.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
            order_book = self.order_books.get(order.market_id)
            if order_book:
                remaining = order.quantity - order.filled_quantity
                order_book.remove_order(
                    side=order.side.value,
                    price=order.price,
                    quantity=remaining
                )
        
        # Update order status
        order.status = OrderStatus.CANCELLED
        order.updated_at = datetime.utcnow()
        
        # Store updated order
        await self.state_manager.put_order(order.order_id, order.dict())
        
        # Publish event
        await self._publish_order_event(order, EventType.ORDER_CANCEL)
        
        self._update_metrics(order.market_id, "orders_cancelled", 1)
        
        return True
    
    async def check_stop_orders(self, market_id: str, current_price: Decimal):
        """Check and trigger stop orders."""
        if market_id not in self.pending_orders:
            return
        
        triggered_orders = []
        remaining_orders = []
        
        for order in self.pending_orders[market_id]:
            should_trigger = False
            
            if order.type in [OrderType.STOP, OrderType.STOP_LIMIT]:
                if order.side == OrderSide.BUY and current_price >= order.stop_price:
                    should_trigger = True
                elif order.side == OrderSide.SELL and current_price <= order.stop_price:
                    should_trigger = True
            
            if should_trigger:
                triggered_orders.append(order)
            else:
                remaining_orders.append(order)
        
        # Update pending orders
        self.pending_orders[market_id] = remaining_orders
        
        # Process triggered orders
        for order in triggered_orders:
            order.status = OrderStatus.TRIGGERED
            
            if order.type == OrderType.STOP:
                # Convert to market order
                order.type = OrderType.MARKET
                await self.process_order(order)
            elif order.type == OrderType.STOP_LIMIT:
                # Convert to limit order
                order.type = OrderType.LIMIT
                await self.process_order(order)
    
    async def _validate_order(self, order: Order) -> bool:
        """Validate order before processing."""
        # Get market configuration
        config = self.market_configs.get(order.market_id)
        if not config:
            logger.error(f"Market configuration not found for {order.market_id}")
            return False
        
        # Validate quantity
        if order.quantity < config.lot_size:
            logger.warning(f"Order quantity {order.quantity} below minimum {config.lot_size}")
            return False
        
        if order.quantity > config.max_order_size:
            logger.warning(f"Order quantity {order.quantity} exceeds maximum {config.max_order_size}")
            return False
        
        # Validate price tick size for limit orders
        if order.price:
            if order.price % config.tick_size != 0:
                logger.warning(f"Order price {order.price} not aligned to tick size {config.tick_size}")
                return False
            
            # Check price bands
            if hasattr(order, '_reference_price') and order._reference_price:
                min_price = order._reference_price * config.price_bands[0]
                max_price = order._reference_price * config.price_bands[1]
                if order.price < min_price or order.price > max_price:
                    logger.warning(f"Order price {order.price} outside bands [{min_price}, {max_price}]")
                    return False
        
        return True
    
    async def _publish_order_event(self, order: Order, event_type: EventType):
        """Publish order event."""
        event = OrderEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            order_id=order.order_id,
            user_id=order.user_id,
            market_id=order.market_id,
            product_type=order.product_type,
            order_data=order.dict()
        )
        
        await self.event_processor.publish_order_event(event)
    
    async def _publish_trade_event(self, trade: Trade):
        """Publish trade event."""
        event = TradeEvent(
            event_id=str(uuid.uuid4()),
            event_type=EventType.TRADE_EXECUTE,
            trade_id=trade.trade_id,
            market_id=trade.market_id,
            product_type=trade.product_type,
            taker_order_id=trade.taker_order_id,
            maker_order_id=trade.maker_order_id,
            price=trade.price,
            quantity=trade.quantity,
            trade_data=trade.dict()
        )
        
        await self.event_processor.publish_trade_event(event)
    
    def _update_metrics(self, market_id: str, metric: str, value: Any):
        """Update metrics for a market."""
        # Update market-specific metrics
        if market_id in self.metrics:
            if hasattr(self.metrics[market_id], metric):
                current = getattr(self.metrics[market_id], metric)
                if isinstance(current, (int, float, Decimal)):
                    setattr(self.metrics[market_id], metric, current + value)
        
        # Update global metrics
        if hasattr(self.global_metrics, metric):
            current = getattr(self.global_metrics, metric)
            if isinstance(current, (int, float, Decimal)):
                setattr(self.global_metrics, metric, current + value)
    
    def _record_latency(self, market_id: str, latency_ns: int):
        """Record latency for monitoring."""
        # Convert to milliseconds
        latency_ms = latency_ns / 1_000_000
        
        # Add to market histogram
        if market_id in self.metrics:
            self.metrics[market_id].latency_histogram.append(latency_ms)
            # Keep only last 10000 samples
            if len(self.metrics[market_id].latency_histogram) > 10000:
                self.metrics[market_id].latency_histogram = self.metrics[market_id].latency_histogram[-10000:]
        
        # Add to global histogram
        self.global_metrics.latency_histogram.append(latency_ms)
        if len(self.global_metrics.latency_histogram) > 10000:
            self.global_metrics.latency_histogram = self.global_metrics.latency_histogram[-10000:]
    
    async def _market_data_publisher(self):
        """Publish market data updates periodically."""
        while self._running:
            try:
                for market_id, order_book in self.order_books.items():
                    if self.circuit_breaker.is_market_halted(market_id):
                        continue
                    
                    # Get market snapshot
                    snapshot = order_book.get_snapshot()
                    
                    # Publish to event processor
                    await self.event_processor.publish_market_data(snapshot)
                
                # Sleep for tick interval (100ms)
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error in market data publisher: {e}")
    
    async def _metrics_reporter(self):
        """Report metrics periodically."""
        while self._running:
            try:
                # Report metrics every 60 seconds
                await asyncio.sleep(60)
                
                metrics = self.get_metrics()
                logger.info(f"Matching engine metrics: {metrics}")
                
                # Could publish to monitoring system
                
            except Exception as e:
                logger.error(f"Error in metrics reporter: {e}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get matching engine metrics."""
        # Calculate latency percentiles
        def calculate_percentiles(histogram: List[float]) -> Dict[str, float]:
            if not histogram:
                return {"p50": 0, "p95": 0, "p99": 0}
            
            sorted_hist = sorted(histogram)
            return {
                "p50": sorted_hist[int(len(sorted_hist) * 0.5)],
                "p95": sorted_hist[int(len(sorted_hist) * 0.95)],
                "p99": sorted_hist[int(len(sorted_hist) * 0.99)]
            }
        
        global_latency = calculate_percentiles(self.global_metrics.latency_histogram)
        
        return {
            "global": {
                "orders_processed": self.global_metrics.orders_processed,
                "trades_executed": self.global_metrics.trades_executed,
                "orders_cancelled": self.global_metrics.orders_cancelled,
                "orders_rejected": self.global_metrics.orders_rejected,
                "total_volume": str(self.global_metrics.total_volume),
                "latency_ms": global_latency,
                "active_markets": len(self.order_books),
                "halted_markets": len(self.circuit_breaker.halted_markets),
                "pending_orders": sum(len(orders) for orders in self.pending_orders.values())
            },
            "markets": {
                market_id: {
                    "orders_processed": metrics.orders_processed,
                    "trades_executed": metrics.trades_executed,
                    "total_volume": str(metrics.total_volume),
                    "latency_ms": calculate_percentiles(metrics.latency_histogram)
                }
                for market_id, metrics in self.metrics.items()
            }
        }
    
    async def shutdown(self):
        """Shutdown the matching engine."""
        self._running = False
        
        # Cancel background tasks
        for task in self._tasks:
            task.cancel()
        
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Shutdown thread pool
        self.executor.shutdown(wait=True)
        
        logger.info("Matching engine shut down") 