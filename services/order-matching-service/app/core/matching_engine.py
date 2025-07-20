from decimal import Decimal
from typing import Dict, List, Optional, Set
import asyncio
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
import threading

from ..models.order import Order, OrderStatus, OrderType, Trade
from ..core.order_book import OrderBook
from ..cache.ignite_manager import IgniteManager
from ..config import OrderMatchingConfig
from pulsar import Client as PulsarClient, Producer
import json


class MatchingEngine:
    """High-performance matching engine managing multiple order books"""
    
    def __init__(self, config: OrderMatchingConfig):
        self.config = config
        self.order_books: Dict[str, OrderBook] = {}
        self.active_markets: Set[str] = set()
        
        # Ignite cache manager
        self.ignite = IgniteManager(config)
        
        # Pulsar producers
        self.pulsar_client: Optional[PulsarClient] = None
        self.trade_producer: Optional[Producer] = None
        self.order_producer: Optional[Producer] = None
        self.market_data_producer: Optional[Producer] = None
        
        # Thread pool for parallel processing
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        # Circuit breaker state
        self.halted_markets: Dict[str, int] = {}  # market_id -> halt_end_time
        
        # Performance metrics
        self.metrics = {
            "orders_processed": 0,
            "trades_executed": 0,
            "orders_rejected": 0,
            "latency_histogram": []
        }
        
        # Background tasks
        self._running = False
        self._tasks = []
        
        # Global sequence number
        self._sequence_lock = threading.Lock()
        self._sequence_number = 0
    
    async def initialize(self):
        """Initialize the matching engine"""
        # Initialize Ignite
        await self.ignite.initialize()
        
        # Initialize Pulsar
        self.pulsar_client = PulsarClient(self.config.PULSAR_URL)
        
        self.trade_producer = self.pulsar_client.create_producer(
            self.config.PULSAR_TOPIC_TRADES,
            batching_enabled=True,
            batching_max_messages=self.config.PULSAR_BATCH_SIZE,
            batching_max_allowed_size_in_bytes=1024 * 1024,  # 1MB
            batching_max_publish_delay_ms=self.config.PULSAR_BATCH_TIMEOUT_MS
        )
        
        self.order_producer = self.pulsar_client.create_producer(
            self.config.PULSAR_TOPIC_ORDERS,
            batching_enabled=True,
            batching_max_messages=self.config.PULSAR_BATCH_SIZE,
            batching_max_publish_delay_ms=self.config.PULSAR_BATCH_TIMEOUT_MS
        )
        
        self.market_data_producer = self.pulsar_client.create_producer(
            self.config.PULSAR_TOPIC_MARKET_DATA,
            batching_enabled=True,
            batching_max_messages=1000,
            batching_max_publish_delay_ms=50  # 50ms for market data
        )
        
        # Load active markets from Ignite
        await self._load_active_markets()
        
        # Start background tasks
        self._running = True
        self._tasks.append(asyncio.create_task(self._market_data_publisher()))
        self._tasks.append(asyncio.create_task(self._circuit_breaker_monitor()))
    
    async def submit_order(self, order: Order) -> Dict:
        """Submit an order for matching"""
        start_time = time.time_ns()
        
        try:
            # Assign sequence number
            with self._sequence_lock:
                self._sequence_number += 1
                order._sequence = self._sequence_number
            
            # Validate order
            validation_result = await self._validate_order(order)
            if not validation_result["valid"]:
                self.metrics["orders_rejected"] += 1
                return {
                    "success": False,
                    "order_id": order.order_id,
                    "reason": validation_result["reason"],
                    "timestamp": time.time_ns()
                }
            
            # Check circuit breaker
            if self._is_market_halted(order.market_id):
                return {
                    "success": False,
                    "order_id": order.order_id,
                    "reason": "Market halted",
                    "timestamp": time.time_ns()
                }
            
            # Get or create order book
            order_book = self._get_order_book(order.market_id)
            
            # Submit to order book
            trades = await asyncio.get_running_loop().run_in_executor(
                self.executor,
                order_book.add_order,
                order
            )
            
            # Update metrics
            self.metrics["orders_processed"] += 1
            self.metrics["trades_executed"] += len(trades)
            
            # Persist order to Ignite
            await self.ignite.save_order(order)
            
            # Publish events
            await self._publish_order_event(order, "NEW")
            for trade in trades:
                await self._publish_trade_event(trade)
                await self.ignite.save_trade(trade)
            
            # Record latency
            latency_ns = time.time_ns() - start_time
            self._record_latency(latency_ns)
            
            return {
                "success": True,
                "order_id": order.order_id,
                "status": order.status.value,
                "filled_quantity": str(order.filled_quantity),
                "remaining_quantity": str(order.remaining_quantity),
                "trades": [t.to_dict() for t in trades],
                "latency_ns": latency_ns,
                "timestamp": time.time_ns()
            }
            
        except Exception as e:
            self.metrics["orders_rejected"] += 1
            return {
                "success": False,
                "order_id": order.order_id,
                "reason": str(e),
                "timestamp": time.time_ns()
            }
    
    async def cancel_order(self, market_id: str, order_id: str) -> Dict:
        """Cancel an order"""
        try:
            order_book = self.order_books.get(market_id)
            if not order_book:
                return {
                    "success": False,
                    "reason": "Market not found",
                    "timestamp": time.time_ns()
                }
            
            # Cancel in order book
            order = await asyncio.get_running_loop().run_in_executor(
                self.executor,
                order_book.cancel_order,
                order_id
            )
            
            if not order:
                return {
                    "success": False,
                    "reason": "Order not found",
                    "timestamp": time.time_ns()
                }
            
            # Update order status
            order.status = OrderStatus.CANCELLED
            
            # Persist cancellation
            await self.ignite.save_order(order)
            
            # Publish event
            await self._publish_order_event(order, "CANCELLED")
            
            return {
                "success": True,
                "order_id": order_id,
                "timestamp": time.time_ns()
            }
            
        except Exception as e:
            return {
                "success": False,
                "reason": str(e),
                "timestamp": time.time_ns()
            }
    
    def get_order_book(self, market_id: str, depth: int = 10) -> Optional[Dict]:
        """Get order book snapshot"""
        order_book = self.order_books.get(market_id)
        if not order_book:
            return None
        
        return order_book.get_market_depth(depth)
    
    def _get_order_book(self, market_id: str) -> OrderBook:
        """Get or create order book for market"""
        if market_id not in self.order_books:
            self.order_books[market_id] = OrderBook(market_id, self.config)
            self.active_markets.add(market_id)
        
        return self.order_books[market_id]
    
    async def _validate_order(self, order: Order) -> Dict:
        """Validate order before processing"""
        # Basic validation
        if order.quantity <= 0:
            return {"valid": False, "reason": "Invalid quantity"}
        
        if order.order_type == OrderType.LIMIT and order.price is None:
            return {"valid": False, "reason": "Limit order requires price"}
        
        if order.price and order.price <= 0:
            return {"valid": False, "reason": "Invalid price"}
        
        # Check order size limits
        if order.quantity > self.config.MAX_ORDER_SIZE:
            return {"valid": False, "reason": "Order size exceeds limit"}
        
        # Risk checks would go here
        # - Check trader limits
        # - Check margin requirements
        # - Check position limits
        
        return {"valid": True}
    
    def _is_market_halted(self, market_id: str) -> bool:
        """Check if market is halted"""
        if market_id in self.halted_markets:
            halt_end_time = self.halted_markets[market_id]
            if time.time() < halt_end_time:
                return True
            else:
                # Halt expired
                del self.halted_markets[market_id]
        
        return False
    
    async def _publish_order_event(self, order: Order, event_type: str):
        """Publish order event to Pulsar"""
        event = {
            "event_type": event_type,
            "order": order.to_dict(),
            "timestamp": time.time_ns()
        }
        
        await self.order_producer.send_async(
            json.dumps(event).encode('utf-8'),
            properties={
                "market_id": order.market_id,
                "trader_id": order.trader_id,
                "event_type": event_type
            }
        )
    
    async def _publish_trade_event(self, trade: Trade):
        """Publish trade event to Pulsar"""
        event = {
            "event_type": "TRADE",
            "trade": trade.to_dict(),
            "timestamp": time.time_ns()
        }
        
        await self.trade_producer.send_async(
            json.dumps(event).encode('utf-8'),
            properties={
                "market_id": trade.market_id,
                "buyer_id": trade.buyer_id,
                "seller_id": trade.seller_id
            }
        )
    
    async def _market_data_publisher(self):
        """Publish market data updates periodically"""
        while self._running:
            try:
                for market_id, order_book in self.order_books.items():
                    if self._is_market_halted(market_id):
                        continue
                    
                    # Get market snapshot
                    snapshot = order_book.get_market_depth(5)
                    
                    # Publish to Pulsar
                    await self.market_data_producer.send_async(
                        json.dumps(snapshot).encode('utf-8'),
                        properties={"market_id": market_id}
                    )
                
                # Sleep for tick interval
                await asyncio.sleep(self.config.TICK_INTERVAL_MS / 1000.0)
                
            except Exception as e:
                print(f"Error in market data publisher: {e}")
    
    async def _circuit_breaker_monitor(self):
        """Monitor markets for circuit breaker conditions"""
        while self._running:
            try:
                for market_id, order_book in self.order_books.items():
                    if self._is_market_halted(market_id):
                        continue
                    
                    # Check for price spike
                    # Check for volume spike
                    # Implementation would go here
                    pass
                
                await asyncio.sleep(1)  # Check every second
                
            except Exception as e:
                print(f"Error in circuit breaker monitor: {e}")
    
    async def _load_active_markets(self):
        """Load active markets from Ignite"""
        # Implementation would load market configurations from Ignite
        pass
    
    def _record_latency(self, latency_ns: int):
        """Record latency for monitoring"""
        # Convert to milliseconds
        latency_ms = latency_ns / 1_000_000
        
        # Add to histogram (simplified)
        self.metrics["latency_histogram"].append(latency_ms)
        
        # Keep only last 10000 samples
        if len(self.metrics["latency_histogram"]) > 10000:
            self.metrics["latency_histogram"] = self.metrics["latency_histogram"][-10000:]
    
    def get_metrics(self) -> Dict:
        """Get engine metrics"""
        histogram = self.metrics["latency_histogram"]
        
        if histogram:
            sorted_hist = sorted(histogram)
            p50 = sorted_hist[int(len(sorted_hist) * 0.5)]
            p95 = sorted_hist[int(len(sorted_hist) * 0.95)]
            p99 = sorted_hist[int(len(sorted_hist) * 0.99)]
        else:
            p50 = p95 = p99 = 0
        
        return {
            "orders_processed": self.metrics["orders_processed"],
            "trades_executed": self.metrics["trades_executed"],
            "orders_rejected": self.metrics["orders_rejected"],
            "active_markets": len(self.active_markets),
            "latency_p50_ms": p50,
            "latency_p95_ms": p95,
            "latency_p99_ms": p99
        }
    
    async def shutdown(self):
        """Shutdown the matching engine"""
        self._running = False
        
        # Cancel background tasks
        for task in self._tasks:
            task.cancel()
        
        # Close Pulsar
        if self.trade_producer:
            self.trade_producer.close()
        if self.order_producer:
            self.order_producer.close()
        if self.market_data_producer:
            self.market_data_producer.close()
        if self.pulsar_client:
            self.pulsar_client.close()
        
        # Shutdown thread pool
        self.executor.shutdown(wait=True)
        
        # Close Ignite
        await self.ignite.close() 