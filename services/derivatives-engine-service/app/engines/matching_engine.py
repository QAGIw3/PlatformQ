from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import asyncio
import heapq
from collections import defaultdict
import logging
from sortedcontainers import SortedDict

from app.models.order import Order, OrderStatus, OrderType, OrderSide, Trade
from app.integrations import NeuromorphicServiceClient, IgniteCache, PulsarEventPublisher

logger = logging.getLogger(__name__)

class OrderBook:
    """
    High-performance order book using sorted containers
    """
    def __init__(self, market_id: str):
        self.market_id = market_id
        # Price -> List of orders at that price (sorted by time)
        self.bids = SortedDict()  # Descending by price
        self.asks = SortedDict()  # Ascending by price
        # Order ID -> Order mapping for fast lookup
        self.orders = {}
        
    def add_order(self, order: Order):
        """Add order to book"""
        if order.side == OrderSide.BUY:
            book = self.bids
        else:
            book = self.asks
            
        if order.price not in book:
            book[order.price] = []
        
        book[order.price].append(order)
        self.orders[order.id] = order
        
    def remove_order(self, order_id: str) -> Optional[Order]:
        """Remove order from book"""
        if order_id not in self.orders:
            return None
            
        order = self.orders[order_id]
        
        if order.side == OrderSide.BUY:
            book = self.bids
        else:
            book = self.asks
            
        if order.price in book:
            book[order.price] = [o for o in book[order.price] if o.id != order_id]
            if not book[order.price]:
                del book[order.price]
                
        del self.orders[order_id]
        return order
        
    def get_best_bid(self) -> Optional[Decimal]:
        """Get best bid price"""
        if self.bids:
            return self.bids.keys()[-1]  # Highest price
        return None
        
    def get_best_ask(self) -> Optional[Decimal]:
        """Get best ask price"""
        if self.asks:
            return self.asks.keys()[0]  # Lowest price
        return None
        
    def get_spread(self) -> Optional[Decimal]:
        """Get bid-ask spread"""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        
        if best_bid and best_ask:
            return best_ask - best_bid
        return None


class MatchingEngine:
    """
    Ultra-fast matching engine with neuromorphic acceleration
    """
    
    def __init__(
        self,
        neuromorphic_client: NeuromorphicServiceClient,
        ignite_client: IgniteCache,
        pulsar_client: PulsarEventPublisher
    ):
        self.neuromorphic = neuromorphic_client
        self.ignite = ignite_client
        self.pulsar = pulsar_client
        
        # Market ID -> OrderBook
        self.order_books = defaultdict(lambda: None)
        
        # Pending orders queue for neuromorphic batch processing
        self.pending_orders = asyncio.Queue()
        
        # Performance metrics
        self.metrics = {
            "orders_processed": 0,
            "trades_executed": 0,
            "neuromorphic_matches": 0,
            "avg_latency_us": 0
        }
        
        self.running = False
        
    async def start(self):
        """Start the matching engine"""
        self.running = True
        
        # Start order processing loop
        asyncio.create_task(self._process_orders_loop())
        
        # Start neuromorphic batch processing
        asyncio.create_task(self._neuromorphic_batch_loop())
        
        logger.info("Matching engine started")
        
    async def stop(self):
        """Stop the matching engine"""
        self.running = False
        logger.info("Matching engine stopped")
        
    async def submit_order(self, order: Order) -> Dict:
        """
        Submit order for matching
        """
        start_time = datetime.utcnow()
        
        # Validate order
        validation_result = await self._validate_order(order)
        if not validation_result["valid"]:
            return {
                "success": False,
                "error": validation_result["error"],
                "order_id": order.id
            }
        
        # Initialize order book if needed
        if self.order_books[order.market_id] is None:
            self.order_books[order.market_id] = OrderBook(order.market_id)
        
        # Check for immediate match possibility using neuromorphic prediction
        if await self._predict_immediate_match(order):
            # Fast path: neuromorphic processing
            trades = await self._neuromorphic_match(order)
        else:
            # Normal path: traditional matching
            trades = await self._match_order(order)
        
        # Calculate latency
        latency_us = (datetime.utcnow() - start_time).total_seconds() * 1_000_000
        self._update_metrics(latency_us, len(trades))
        
        # Emit order event
        await self._emit_order_event(order, trades)
        
        return {
            "success": True,
            "order_id": order.id,
            "status": order.status,
            "trades": trades,
            "remaining_size": order.remaining_size,
            "latency_us": latency_us
        }
        
    async def _predict_immediate_match(self, order: Order) -> bool:
        """
        Use neuromorphic processor to predict if order will match immediately
        """
        order_book = self.order_books[order.market_id]
        
        if order.order_type == OrderType.MARKET:
            return True
            
        if order.side == OrderSide.BUY:
            best_ask = order_book.get_best_ask()
            if best_ask and order.price >= best_ask:
                return True
        else:
            best_bid = order_book.get_best_bid()
            if best_bid and order.price <= best_bid:
                return True
                
        # Use neuromorphic prediction for complex scenarios
        features = self._extract_order_features(order, order_book)
        prediction = await self.neuromorphic.process(
            "order_match_prediction",
            features
        )
        
        return prediction.get("will_match_immediately", False)
        
    async def _neuromorphic_match(self, order: Order) -> List[Trade]:
        """
        Ultra-fast matching using neuromorphic processor
        """
        order_book = self.order_books[order.market_id]
        
        # Prepare order book state for neuromorphic processing
        book_state = self._serialize_order_book(order_book)
        
        # Send to neuromorphic processor
        match_result = await self.neuromorphic.process(
            "order_matching",
            {
                "order": order.to_dict(),
                "book_state": book_state,
                "optimization_goal": "minimize_latency"
            }
        )
        
        # Process matches
        trades = []
        for match in match_result.get("matches", []):
            trade = await self._execute_trade(
                order,
                match["matched_order_id"],
                match["price"],
                match["size"]
            )
            trades.append(trade)
            
        self.metrics["neuromorphic_matches"] += 1
        
        return trades
        
    async def _match_order(self, order: Order) -> List[Trade]:
        """
        Traditional order matching algorithm (fallback)
        """
        order_book = self.order_books[order.market_id]
        trades = []
        
        if order.order_type == OrderType.MARKET:
            trades = await self._match_market_order(order, order_book)
        elif order.order_type == OrderType.LIMIT:
            trades = await self._match_limit_order(order, order_book)
        elif order.order_type == OrderType.STOP:
            # Add to stop order monitoring
            await self._add_stop_order(order)
        
        return trades
        
    async def _match_market_order(
        self,
        order: Order,
        order_book: OrderBook
    ) -> List[Trade]:
        """
        Match market order against order book
        """
        trades = []
        
        if order.side == OrderSide.BUY:
            # Match against asks
            for price in order_book.asks.keys():
                if order.remaining_size <= 0:
                    break
                    
                orders_at_price = order_book.asks[price][:]
                for counter_order in orders_at_price:
                    if order.remaining_size <= 0:
                        break
                        
                    trade_size = min(order.remaining_size, counter_order.remaining_size)
                    
                    trade = await self._execute_trade(
                        order,
                        counter_order.id,
                        price,
                        trade_size
                    )
                    trades.append(trade)
                    
        else:  # SELL
            # Match against bids
            for price in reversed(order_book.bids.keys()):
                if order.remaining_size <= 0:
                    break
                    
                orders_at_price = order_book.bids[price][:]
                for counter_order in orders_at_price:
                    if order.remaining_size <= 0:
                        break
                        
                    trade_size = min(order.remaining_size, counter_order.remaining_size)
                    
                    trade = await self._execute_trade(
                        order,
                        counter_order.id,
                        price,
                        trade_size
                    )
                    trades.append(trade)
                    
        # If order not fully filled, reject remaining (market orders are all-or-nothing)
        if order.remaining_size > 0:
            order.status = OrderStatus.REJECTED
            order.reject_reason = "Insufficient liquidity"
            
        return trades
        
    async def _match_limit_order(
        self,
        order: Order,
        order_book: OrderBook
    ) -> List[Trade]:
        """
        Match limit order and add remainder to book
        """
        trades = []
        
        if order.side == OrderSide.BUY:
            # Check if can match against asks
            for price in order_book.asks.keys():
                if price > order.price:
                    break  # No more matches possible
                    
                if order.remaining_size <= 0:
                    break
                    
                orders_at_price = order_book.asks[price][:]
                for counter_order in orders_at_price:
                    if order.remaining_size <= 0:
                        break
                        
                    trade_size = min(order.remaining_size, counter_order.remaining_size)
                    
                    trade = await self._execute_trade(
                        order,
                        counter_order.id,
                        price,  # Trade at maker price
                        trade_size
                    )
                    trades.append(trade)
                    
        else:  # SELL
            # Check if can match against bids
            for price in reversed(order_book.bids.keys()):
                if price < order.price:
                    break  # No more matches possible
                    
                if order.remaining_size <= 0:
                    break
                    
                orders_at_price = order_book.bids[price][:]
                for counter_order in orders_at_price:
                    if order.remaining_size <= 0:
                        break
                        
                    trade_size = min(order.remaining_size, counter_order.remaining_size)
                    
                    trade = await self._execute_trade(
                        order,
                        counter_order.id,
                        price,  # Trade at maker price
                        trade_size
                    )
                    trades.append(trade)
                    
        # Add remaining to order book if not fully filled and not post-only rejected
        if order.remaining_size > 0:
            if order.post_only and len(trades) > 0:
                # Post-only order traded, reject remainder
                order.status = OrderStatus.REJECTED
                order.reject_reason = "Post-only order would have traded"
            else:
                # Add to order book
                order.status = OrderStatus.OPEN
                order_book.add_order(order)
                
        return trades
        
    async def _execute_trade(
        self,
        taker_order: Order,
        maker_order_id: str,
        price: Decimal,
        size: Decimal
    ) -> Trade:
        """
        Execute a trade between two orders
        """
        order_book = self.order_books[taker_order.market_id]
        maker_order = order_book.orders.get(maker_order_id)
        
        if not maker_order:
            raise ValueError(f"Maker order {maker_order_id} not found")
            
        # Create trade
        trade = Trade(
            id=self._generate_trade_id(),
            market_id=taker_order.market_id,
            taker_order_id=taker_order.id,
            maker_order_id=maker_order.id,
            taker_user_id=taker_order.user_id,
            maker_user_id=maker_order.user_id,
            price=price,
            size=size,
            side=taker_order.side,
            timestamp=datetime.utcnow()
        )
        
        # Update order states
        taker_order.filled_size += size
        taker_order.remaining_size -= size
        maker_order.filled_size += size
        maker_order.remaining_size -= size
        
        # Update order statuses
        if taker_order.remaining_size == 0:
            taker_order.status = OrderStatus.FILLED
        else:
            taker_order.status = OrderStatus.PARTIALLY_FILLED
            
        if maker_order.remaining_size == 0:
            maker_order.status = OrderStatus.FILLED
            order_book.remove_order(maker_order.id)
        else:
            maker_order.status = OrderStatus.PARTIALLY_FILLED
            
        # Store trade
        await self._store_trade(trade)
        
        # Emit trade event
        await self._emit_trade_event(trade)
        
        return trade
        
    async def cancel_order(self, order_id: str, user_id: str) -> Dict:
        """
        Cancel an order
        """
        # Find order across all markets
        for market_id, order_book in self.order_books.items():
            if order_book and order_id in order_book.orders:
                order = order_book.orders[order_id]
                
                # Verify ownership
                if order.user_id != user_id:
                    return {
                        "success": False,
                        "error": "Unauthorized"
                    }
                    
                # Remove from book
                order_book.remove_order(order_id)
                
                # Update status
                order.status = OrderStatus.CANCELLED
                
                # Emit cancellation event
                await self._emit_order_cancelled_event(order)
                
                return {
                    "success": True,
                    "order": order
                }
                
        return {
            "success": False,
            "error": "Order not found"
        }
        
    async def get_order_book(self, market_id: str, depth: int = 20) -> Dict:
        """
        Get order book snapshot
        """
        order_book = self.order_books.get(market_id)
        
        if not order_book:
            return {
                "market_id": market_id,
                "bids": [],
                "asks": [],
                "timestamp": datetime.utcnow()
            }
            
        # Aggregate orders by price level
        bids = []
        asks = []
        
        # Process bids (descending)
        for price in list(reversed(order_book.bids.keys()))[:depth]:
            orders = order_book.bids[price]
            total_size = sum(o.remaining_size for o in orders)
            bids.append({
                "price": str(price),
                "size": str(total_size),
                "orders": len(orders)
            })
            
        # Process asks (ascending)
        for price in list(order_book.asks.keys())[:depth]:
            orders = order_book.asks[price]
            total_size = sum(o.remaining_size for o in orders)
            asks.append({
                "price": str(price),
                "size": str(total_size),
                "orders": len(orders)
            })
            
        return {
            "market_id": market_id,
            "bids": bids,
            "asks": asks,
            "best_bid": str(order_book.get_best_bid()) if order_book.get_best_bid() else None,
            "best_ask": str(order_book.get_best_ask()) if order_book.get_best_ask() else None,
            "spread": str(order_book.get_spread()) if order_book.get_spread() else None,
            "timestamp": datetime.utcnow()
        }
        
    def _update_metrics(self, latency_us: float, trades_count: int):
        """Update performance metrics"""
        self.metrics["orders_processed"] += 1
        self.metrics["trades_executed"] += trades_count
        
        # Update average latency (exponential moving average)
        alpha = 0.1
        self.metrics["avg_latency_us"] = (
            alpha * latency_us + 
            (1 - alpha) * self.metrics["avg_latency_us"]
        )
        
    async def _validate_order(self, order: Order) -> Dict:
        """Validate order before processing"""
        # Check market exists
        # Check user permissions
        # Check risk limits
        # etc.
        return {"valid": True}
        
    def _extract_order_features(self, order: Order, order_book: OrderBook) -> Dict:
        """Extract features for neuromorphic processing"""
        return {
            "order_size": float(order.size),
            "order_price": float(order.price) if order.price else 0,
            "best_bid": float(order_book.get_best_bid()) if order_book.get_best_bid() else 0,
            "best_ask": float(order_book.get_best_ask()) if order_book.get_best_ask() else 0,
            "spread": float(order_book.get_spread()) if order_book.get_spread() else 0,
            "book_depth": len(order_book.orders),
            "order_type": order.order_type.value,
            "side": order.side.value
        }
        
    def _serialize_order_book(self, order_book: OrderBook) -> Dict:
        """Serialize order book for neuromorphic processing"""
        return {
            "bids": [
                {"price": float(p), "orders": [o.to_dict() for o in orders]}
                for p, orders in order_book.bids.items()
            ],
            "asks": [
                {"price": float(p), "orders": [o.to_dict() for o in orders]}
                for p, orders in order_book.asks.items()
            ]
        }
        
    async def _process_orders_loop(self):
        """Main order processing loop"""
        while self.running:
            try:
                # Process pending orders
                if not self.pending_orders.empty():
                    order = await self.pending_orders.get()
                    await self.submit_order(order)
                    
                await asyncio.sleep(0.001)  # 1ms
            except Exception as e:
                logger.error(f"Error in order processing loop: {e}")
                
    async def _neuromorphic_batch_loop(self):
        """Batch processing for neuromorphic optimization"""
        while self.running:
            try:
                # Collect orders for batch processing
                batch_size = 100
                batch_timeout = 0.01  # 10ms
                
                orders = []
                start_time = asyncio.get_event_loop().time()
                
                while len(orders) < batch_size and (asyncio.get_event_loop().time() - start_time) < batch_timeout:
                    try:
                        order = await asyncio.wait_for(
                            self.pending_orders.get(),
                            timeout=0.001
                        )
                        orders.append(order)
                    except asyncio.TimeoutError:
                        break
                        
                if orders:
                    # Process batch with neuromorphic processor
                    await self._process_batch_neuromorphic(orders)
                    
                await asyncio.sleep(0.001)
            except Exception as e:
                logger.error(f"Error in neuromorphic batch loop: {e}")
                
    async def _process_batch_neuromorphic(self, orders: List[Order]):
        """Process batch of orders using neuromorphic processor"""
        # Prepare batch data
        batch_data = {
            "orders": [o.to_dict() for o in orders],
            "order_books": {
                market_id: self._serialize_order_book(book)
                for market_id, book in self.order_books.items()
                if book
            }
        }
        
        # Send to neuromorphic processor
        result = await self.neuromorphic.process(
            "batch_order_matching",
            batch_data
        )
        
        # Process results
        for match_result in result.get("matches", []):
            # Apply matches
            pass
            
    async def _emit_order_event(self, order: Order, trades: List[Trade]):
        """Emit order placed event"""
        event = {
            "event_type": "order_placed",
            "order": order.to_dict(),
            "trades": [t.to_dict() for t in trades],
            "timestamp": datetime.utcnow()
        }
        
        await self.pulsar.publish(
            "persistent://public/default/order-events",
            event
        )
        
    async def _emit_trade_event(self, trade: Trade):
        """Emit trade executed event"""
        event = {
            "event_type": "trade_executed",
            "trade": trade.to_dict(),
            "timestamp": datetime.utcnow()
        }
        
        await self.pulsar.publish(
            "persistent://public/default/trade-events",
            event
        )
        
    async def _store_trade(self, trade: Trade):
        """Store trade in Ignite cache"""
        await self.ignite.put_async(
            f"trade:{trade.id}",
            trade.to_dict()
        )
        
    def _generate_trade_id(self) -> str:
        """Generate unique trade ID"""
        import uuid
        return str(uuid.uuid4()) 