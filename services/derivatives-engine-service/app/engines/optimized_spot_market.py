"""
Optimized Compute Spot Market Engine

High-performance spot market with batch operations and caching
"""

from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Set, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
import asyncio
import logging
import heapq
from collections import defaultdict
import numpy as np

from app.integrations.ignite_optimized import (
    get_optimized_cache, cached, cache_invalidate
)

logger = logging.getLogger(__name__)


class OptimizedComputeSpotMarket:
    """Ultra-fast spot market implementation with optimizations"""
    
    def __init__(self, partner_manager=None, capacity_coordinator=None):
        self.partner_manager = partner_manager
        self.capacity_coordinator = capacity_coordinator
        
        # In-memory order books for speed
        self.order_books: Dict[str, Dict[str, List]] = defaultdict(
            lambda: {"buy": [], "sell": []}
        )
        
        # Batch processing queues
        self.order_queue: List[Dict] = []
        self.trade_queue: List[Dict] = []
        self.batch_size = 100
        
        # Performance metrics
        self.metrics = {
            "orders_per_second": 0,
            "avg_match_time_us": 0,
            "batch_efficiency": 0
        }
        
        self._cache = None
        self._processing = False
        
    async def initialize(self):
        """Initialize optimized spot market"""
        self._cache = await get_optimized_cache()
        
        # Warm up caches
        await self._warmup_caches()
        
        # Start batch processors
        asyncio.create_task(self._batch_order_processor())
        asyncio.create_task(self._batch_trade_processor())
        
        logger.info("Optimized spot market initialized")
        
    async def _warmup_caches(self):
        """Warm up caches with initial data"""
        
        # Warm up price cache
        async def get_initial_prices():
            # In production, load from database
            return {
                "GPU_SPOT": {"price": 50.0, "volume": 1000.0},
                "CPU_SPOT": {"price": 5.0, "volume": 10000.0}
            }
            
        await self._cache.warmup_cache("market_data", get_initial_prices)
        
    async def place_order(self, order: Dict) -> str:
        """Place order with batching"""
        order["order_id"] = f"order_{datetime.utcnow().timestamp()}"
        order["timestamp"] = datetime.utcnow()
        
        # Add to batch queue
        self.order_queue.append(order)
        
        # Process immediately if batch is full
        if len(self.order_queue) >= self.batch_size:
            await self._process_order_batch()
            
        return order["order_id"]
        
    async def _batch_order_processor(self):
        """Process orders in batches"""
        while True:
            try:
                await asyncio.sleep(0.01)  # 10ms batching window
                
                if self.order_queue and not self._processing:
                    await self._process_order_batch()
                    
            except Exception as e:
                logger.error(f"Batch order processor error: {e}")
                
    async def _process_order_batch(self):
        """Process a batch of orders"""
        if not self.order_queue or self._processing:
            return
            
        self._processing = True
        start_time = asyncio.get_event_loop().time()
        
        # Get batch
        batch = self.order_queue[:self.batch_size]
        self.order_queue = self.order_queue[self.batch_size:]
        
        # Group by instrument for efficient processing
        orders_by_instrument = defaultdict(list)
        for order in batch:
            orders_by_instrument[order["instrument_id"]].append(order)
            
        # Process each instrument's orders
        trades = []
        
        for instrument_id, orders in orders_by_instrument.items():
            instrument_trades = await self._match_orders_optimized(
                instrument_id, orders
            )
            trades.extend(instrument_trades)
            
        # Batch update caches
        await self._update_caches_batch(orders, trades)
        
        # Add trades to processing queue
        self.trade_queue.extend(trades)
        
        # Update metrics
        elapsed = asyncio.get_event_loop().time() - start_time
        self.metrics["avg_match_time_us"] = elapsed * 1_000_000 / len(batch)
        self.metrics["orders_per_second"] = len(batch) / elapsed
        
        self._processing = False
        
    async def _match_orders_optimized(self, instrument_id: str, 
                                     new_orders: List[Dict]) -> List[Dict]:
        """Optimized order matching using vectorized operations"""
        
        order_book = self.order_books[instrument_id]
        trades = []
        
        # Convert to numpy arrays for fast matching
        buy_orders = []
        sell_orders = []
        
        # Add existing orders
        for order in order_book["buy"]:
            buy_orders.append([
                float(order["price"]), 
                float(order["quantity"]),
                order["timestamp"].timestamp()
            ])
            
        for order in order_book["sell"]:
            sell_orders.append([
                float(order["price"]),
                float(order["quantity"]), 
                order["timestamp"].timestamp()
            ])
            
        # Add new orders
        for order in new_orders:
            if order["side"] == "buy":
                buy_orders.append([
                    float(order["price"]),
                    float(order["quantity"]),
                    order["timestamp"].timestamp()
                ])
            else:
                sell_orders.append([
                    float(order["price"]),
                    float(order["quantity"]),
                    order["timestamp"].timestamp()
                ])
                
        if not buy_orders or not sell_orders:
            return trades
            
        # Convert to numpy arrays
        buy_array = np.array(buy_orders)
        sell_array = np.array(sell_orders)
        
        # Sort by price (descending for buys, ascending for sells)
        buy_indices = np.argsort(-buy_array[:, 0])
        sell_indices = np.argsort(sell_array[:, 0])
        
        buy_array = buy_array[buy_indices]
        sell_array = sell_array[sell_indices]
        
        # Match orders
        buy_idx = 0
        sell_idx = 0
        
        while buy_idx < len(buy_array) and sell_idx < len(sell_array):
            buy_price = buy_array[buy_idx, 0]
            sell_price = sell_array[sell_idx, 0]
            
            if buy_price >= sell_price:
                # Match found
                match_price = (buy_price + sell_price) / 2
                match_quantity = min(buy_array[buy_idx, 1], sell_array[sell_idx, 1])
                
                trade = {
                    "trade_id": f"trade_{datetime.utcnow().timestamp()}",
                    "instrument_id": instrument_id,
                    "price": Decimal(str(match_price)),
                    "quantity": Decimal(str(match_quantity)),
                    "timestamp": datetime.utcnow(),
                    "buy_order": buy_indices[buy_idx],
                    "sell_order": sell_indices[sell_idx]
                }
                
                trades.append(trade)
                
                # Update quantities
                buy_array[buy_idx, 1] -= match_quantity
                sell_array[sell_idx, 1] -= match_quantity
                
                # Move to next order if exhausted
                if buy_array[buy_idx, 1] <= 0:
                    buy_idx += 1
                if sell_array[sell_idx, 1] <= 0:
                    sell_idx += 1
            else:
                # No more matches possible
                break
                
        return trades
        
    async def _update_caches_batch(self, orders: List[Dict], trades: List[Dict]):
        """Batch update caches for efficiency"""
        
        # Prepare batch updates
        market_updates = {}
        orderbook_updates = {}
        
        # Update market data for each instrument
        instruments = set(o["instrument_id"] for o in orders)
        instruments.update(t["instrument_id"] for t in trades)
        
        for instrument_id in instruments:
            # Calculate latest price and volume
            instrument_trades = [t for t in trades if t["instrument_id"] == instrument_id]
            
            if instrument_trades:
                latest_price = instrument_trades[-1]["price"]
                total_volume = sum(t["quantity"] for t in instrument_trades)
                
                market_updates[f"{instrument_id}:latest"] = {
                    "price": float(latest_price),
                    "volume": float(total_volume),
                    "timestamp": datetime.utcnow().isoformat()
                }
                
            # Update order book summary
            order_book = self.order_books[instrument_id]
            
            if order_book["buy"]:
                best_bid = max(o["price"] for o in order_book["buy"])
            else:
                best_bid = Decimal("0")
                
            if order_book["sell"]:
                best_ask = min(o["price"] for o in order_book["sell"])
            else:
                best_ask = Decimal("0")
                
            orderbook_updates[f"{instrument_id}:summary"] = {
                "best_bid": float(best_bid),
                "best_ask": float(best_ask),
                "spread": float(best_ask - best_bid) if best_bid > 0 and best_ask > 0 else 0,
                "depth": len(order_book["buy"]) + len(order_book["sell"])
            }
            
        # Batch update caches
        if market_updates:
            await self._cache.set_all(market_updates, "market_data", ttl=300)
            
        if orderbook_updates:
            await self._cache.set_all(orderbook_updates, "orderbook", ttl=60)
            
    async def _batch_trade_processor(self):
        """Process trades in batches"""
        while True:
            try:
                await asyncio.sleep(0.1)  # 100ms batching for trades
                
                if self.trade_queue:
                    await self._process_trade_batch()
                    
            except Exception as e:
                logger.error(f"Batch trade processor error: {e}")
                
    async def _process_trade_batch(self):
        """Process a batch of trades"""
        if not self.trade_queue:
            return
            
        # Get batch
        batch = self.trade_queue[:self.batch_size]
        self.trade_queue = self.trade_queue[self.batch_size:]
        
        # Prepare volume updates
        volume_updates = defaultdict(lambda: {
            "volume_1m": Decimal("0"),
            "volume_5m": Decimal("0"), 
            "volume_1h": Decimal("0"),
            "trades_count": 0
        })
        
        for trade in batch:
            instrument_id = trade["instrument_id"]
            volume_updates[instrument_id]["volume_1m"] += trade["quantity"]
            volume_updates[instrument_id]["volume_5m"] += trade["quantity"]
            volume_updates[instrument_id]["volume_1h"] += trade["quantity"]
            volume_updates[instrument_id]["trades_count"] += 1
            
        # Batch update volume statistics
        volume_cache_updates = {}
        
        for instrument_id, stats in volume_updates.items():
            volume_cache_updates[f"{instrument_id}:volume_stats"] = {
                "volume_1m": float(stats["volume_1m"]),
                "volume_5m": float(stats["volume_5m"]),
                "volume_1h": float(stats["volume_1h"]),
                "trades_count": stats["trades_count"],
                "timestamp": datetime.utcnow().isoformat()
            }
            
        await self._cache.set_all(volume_cache_updates, "volume_stats", ttl=300)
        
    @cached(cache_name="market_data", ttl=5)
    async def get_spot_price(self, resource_type: str, 
                            location: Optional[str] = None) -> Dict[str, Any]:
        """Get spot price with caching"""
        
        instrument_id = f"{resource_type}_SPOT"
        if location:
            instrument_id += f"_{location}"
            
        # Try cache first (handled by decorator)
        
        # Calculate from order book
        order_book = self.order_books.get(instrument_id, {"buy": [], "sell": []})
        
        if order_book["buy"] and order_book["sell"]:
            best_bid = max(o["price"] for o in order_book["buy"])
            best_ask = min(o["price"] for o in order_book["sell"])
            mid_price = (best_bid + best_ask) / 2
        else:
            # Default price
            mid_price = Decimal("50") if "GPU" in resource_type else Decimal("5")
            
        return {
            "resource_type": resource_type,
            "location": location,
            "last_trade_price": str(mid_price),
            "timestamp": datetime.utcnow().isoformat(),
            "source": "spot_market"
        }
        
    async def get_market_depth(self, resource_type: str,
                              levels: int = 10) -> Dict[str, Any]:
        """Get market depth using SQL query on cache"""
        
        instrument_id = f"{resource_type}_SPOT"
        
        # Query order book cache
        sql = """
        SELECT price, SUM(quantity) as total_quantity
        FROM orderbook
        WHERE instrument_id = ? AND side = ?
        GROUP BY price
        ORDER BY price DESC
        LIMIT ?
        """
        
        buy_levels = await self._cache.query(
            sql, [instrument_id, "buy", levels], "orderbook"
        )
        
        sell_levels = await self._cache.query(
            sql.replace("DESC", "ASC"), [instrument_id, "sell", levels], "orderbook"
        )
        
        return {
            "instrument_id": instrument_id,
            "buy_levels": buy_levels,
            "sell_levels": sell_levels,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    async def calculate_vwap(self, resource_type: str,
                            period: timedelta) -> Decimal:
        """Calculate VWAP using distributed compute"""
        
        instrument_id = f"{resource_type}_SPOT"
        start_time = datetime.utcnow() - period
        
        # Get trades from cache
        trades = await self._cache.query(
            "SELECT price, quantity FROM trades WHERE instrument_id = ? AND timestamp > ?",
            [instrument_id, start_time.isoformat()],
            "market_data"
        )
        
        if not trades:
            return Decimal("0")
            
        # Use map-reduce for VWAP calculation
        def calculate_weighted_value(trade_chunk):
            total_value = 0.0
            total_volume = 0.0
            
            for trade in trade_chunk:
                value = float(trade["price"]) * float(trade["quantity"])
                total_value += value
                total_volume += float(trade["quantity"])
                
            return {"value": total_value, "volume": total_volume}
            
        def reduce_vwap(results):
            total_value = sum(r["value"] for r in results)
            total_volume = sum(r["volume"] for r in results)
            
            return total_value / total_volume if total_volume > 0 else 0.0
            
        vwap = await self._cache.map_reduce(
            trades, calculate_weighted_value, reduce_vwap
        )
        
        return Decimal(str(vwap))
        
    async def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics"""
        
        cache_stats = await self._cache.get_cache_stats()
        
        return {
            "spot_market_metrics": self.metrics,
            "cache_performance": cache_stats,
            "order_queue_size": len(self.order_queue),
            "trade_queue_size": len(self.trade_queue),
            "active_instruments": len(self.order_books)
        } 