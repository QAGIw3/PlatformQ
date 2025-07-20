"""Market data aggregator for real-time processing"""

import asyncio
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Deque
from collections import defaultdict, deque
from dataclasses import dataclass, field
import time
import logging

from platformq_trading_common.events.trading_events import (
    EventSubscriber, EventType, TradeEvent, MarketDataEvent
)
from pulsar import ConsumerType

from ..models.market_data import (
    PriceTick, OrderBookSnapshot, OrderBookUpdate, 
    Candle, MarketStats, AggregatedTrade
)
from ..cache.cache_manager import CacheManager
from ..config import MarketDataConfig


logger = logging.getLogger(__name__)


@dataclass
class MarketState:
    """Current state of a market"""
    market_id: str
    last_price: Decimal = Decimal(0)
    best_bid: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None
    volume_24h: Decimal = Decimal(0)
    high_24h: Decimal = Decimal(0)
    low_24h: Decimal = Decimal(999999999)
    trade_count_24h: int = 0
    last_update: datetime = field(default_factory=datetime.utcnow)
    
    # Candle builders for different intervals
    candle_builders: Dict[str, 'CandleBuilder'] = field(default_factory=dict)
    
    # Recent trades for aggregation
    recent_trades: Deque[AggregatedTrade] = field(
        default_factory=lambda: deque(maxlen=1000)
    )


@dataclass
class CandleBuilder:
    """Builds candles from trades"""
    interval: str
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal = Decimal(0)
    trade_count: int = 0
    
    def update(self, price: Decimal, volume: Decimal):
        """Update candle with new trade"""
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += volume
        self.trade_count += 1
    
    def to_candle(self) -> Candle:
        """Convert to candle"""
        return Candle(
            market_id="",  # Set by caller
            interval=self.interval,
            open_time=self.open_time,
            close_time=self._get_close_time(),
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            trade_count=self.trade_count
        )
    
    def _get_close_time(self) -> datetime:
        """Get candle close time based on interval"""
        interval_map = {
            "1m": timedelta(minutes=1),
            "5m": timedelta(minutes=5),
            "15m": timedelta(minutes=15),
            "30m": timedelta(minutes=30),
            "1h": timedelta(hours=1),
            "4h": timedelta(hours=4),
            "1d": timedelta(days=1),
            "1w": timedelta(weeks=1)
        }
        return self.open_time + interval_map.get(self.interval, timedelta(minutes=1))


class MarketDataAggregator:
    """Aggregates market data from multiple sources"""
    
    def __init__(
        self,
        config: MarketDataConfig,
        cache_manager: CacheManager,
        event_subscriber: EventSubscriber
    ):
        self.config = config
        self.cache = cache_manager
        self.event_subscriber = event_subscriber
        
        # Market states
        self.market_states: Dict[str, MarketState] = {}
        
        # Order book management
        self.orderbooks: Dict[str, OrderBookSnapshot] = {}
        self.orderbook_sequences: Dict[str, int] = defaultdict(int)
        
        # Aggregation buffers
        self.trade_buffer: List[TradeEvent] = []
        self.last_aggregation_time = time.time()
        
        # WebSocket subscribers
        self.price_subscribers: Dict[str, Set[str]] = defaultdict(set)  # market_id -> set of connection_ids
        self.orderbook_subscribers: Dict[str, Set[str]] = defaultdict(set)
        self.trade_subscribers: Dict[str, Set[str]] = defaultdict(set)
        
        # Background tasks
        self._running = False
        self._tasks = []
    
    async def start(self):
        """Start market data aggregation"""
        self._running = True
        
        # Subscribe to events
        await self._subscribe_to_events()
        
        # Start background tasks
        self._tasks.append(
            asyncio.create_task(self._aggregation_loop())
        )
        self._tasks.append(
            asyncio.create_task(self._candle_builder_loop())
        )
        self._tasks.append(
            asyncio.create_task(self._snapshot_publisher_loop())
        )
        self._tasks.append(
            asyncio.create_task(self._stats_calculator_loop())
        )
        
        logger.info("Market data aggregator started")
    
    async def stop(self):
        """Stop market data aggregation"""
        self._running = False
        
        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
        
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        logger.info("Market data aggregator stopped")
    
    async def _subscribe_to_events(self):
        """Subscribe to relevant events"""
        # Subscribe to trade events
        self.event_subscriber.subscribe_to_event(
            EventType.TRADE_EXECUTED,
            self._handle_trade_event
        )
        
        # Subscribe to order book updates
        self.event_subscriber.subscribe_to_event(
            EventType.ORDERBOOK_UPDATE,
            self._handle_orderbook_event
        )
        
        # Start processing events
        asyncio.create_task(self.event_subscriber.process_events())
    
    async def _handle_trade_event(self, event: TradeEvent):
        """Handle incoming trade event"""
        try:
            market_id = event.market_id
            
            # Update market state
            if market_id not in self.market_states:
                self.market_states[market_id] = MarketState(market_id=market_id)
            
            state = self.market_states[market_id]
            price = Decimal(event.price)
            volume = Decimal(event.quantity)
            
            # Update state
            state.last_price = price
            state.volume_24h += volume
            state.trade_count_24h += 1
            state.high_24h = max(state.high_24h, price)
            state.low_24h = min(state.low_24h, price)
            state.last_update = datetime.utcnow()
            
            # Add to recent trades
            agg_trade = AggregatedTrade(
                market_id=market_id,
                trade_id=event.trade_id,
                price=price,
                quantity=volume,
                maker_side=event.maker_side,
                timestamp=event.timestamp
            )
            state.recent_trades.append(agg_trade)
            
            # Update candles
            self._update_candles(state, price, volume)
            
            # Add to buffer for batch processing
            self.trade_buffer.append(event)
            
            # Cache latest price
            await self.cache.set_price(market_id, {
                "price": str(price),
                "volume_24h": str(state.volume_24h),
                "timestamp": event.timestamp.isoformat()
            })
            
        except Exception as e:
            logger.error(f"Error handling trade event: {e}")
    
    async def _handle_orderbook_event(self, event: MarketDataEvent):
        """Handle order book update"""
        try:
            market_id = event.market_id
            
            if event.update_type == "snapshot":
                # Full snapshot
                snapshot = OrderBookSnapshot(
                    market_id=market_id,
                    bids=[(Decimal(p), Decimal(q)) for p, q in event.bids],
                    asks=[(Decimal(p), Decimal(q)) for p, q in event.asks],
                    sequence=event.sequence_number,
                    timestamp=event.timestamp
                )
                self.orderbooks[market_id] = snapshot
                self.orderbook_sequences[market_id] = event.sequence_number
                
                # Update best bid/ask in market state
                if market_id in self.market_states:
                    state = self.market_states[market_id]
                    state.best_bid = snapshot.best_bid
                    state.best_ask = snapshot.best_ask
                
                # Cache order book
                await self.cache.set_orderbook(market_id, snapshot)
                
            else:
                # Delta update
                # TODO: Implement delta updates
                pass
                
        except Exception as e:
            logger.error(f"Error handling orderbook event: {e}")
    
    def _update_candles(self, state: MarketState, price: Decimal, volume: Decimal):
        """Update candle builders with new trade"""
        now = datetime.utcnow()
        
        for interval in self.config.CANDLE_INTERVALS:
            # Get or create candle builder
            if interval not in state.candle_builders:
                state.candle_builders[interval] = self._create_candle_builder(
                    interval, now, price
                )
            
            builder = state.candle_builders[interval]
            
            # Check if we need a new candle
            if now >= builder._get_close_time():
                # Save completed candle
                candle = builder.to_candle()
                candle.market_id = state.market_id
                asyncio.create_task(self._save_candle(candle))
                
                # Create new builder
                state.candle_builders[interval] = self._create_candle_builder(
                    interval, now, price
                )
                builder = state.candle_builders[interval]
            
            # Update candle
            builder.update(price, volume)
    
    def _create_candle_builder(
        self, 
        interval: str, 
        timestamp: datetime,
        initial_price: Decimal
    ) -> CandleBuilder:
        """Create new candle builder"""
        # Align to interval boundary
        open_time = self._align_to_interval(timestamp, interval)
        
        return CandleBuilder(
            interval=interval,
            open_time=open_time,
            open=initial_price,
            high=initial_price,
            low=initial_price,
            close=initial_price
        )
    
    def _align_to_interval(self, timestamp: datetime, interval: str) -> datetime:
        """Align timestamp to interval boundary"""
        # Simple implementation - can be improved
        if interval == "1m":
            return timestamp.replace(second=0, microsecond=0)
        elif interval == "5m":
            minute = timestamp.minute - (timestamp.minute % 5)
            return timestamp.replace(minute=minute, second=0, microsecond=0)
        elif interval == "15m":
            minute = timestamp.minute - (timestamp.minute % 15)
            return timestamp.replace(minute=minute, second=0, microsecond=0)
        elif interval == "30m":
            minute = timestamp.minute - (timestamp.minute % 30)
            return timestamp.replace(minute=minute, second=0, microsecond=0)
        elif interval == "1h":
            return timestamp.replace(minute=0, second=0, microsecond=0)
        elif interval == "4h":
            hour = timestamp.hour - (timestamp.hour % 4)
            return timestamp.replace(hour=hour, minute=0, second=0, microsecond=0)
        elif interval == "1d":
            return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        elif interval == "1w":
            # Start of week (Monday)
            days_since_monday = timestamp.weekday()
            start_of_week = timestamp - timedelta(days=days_since_monday)
            return start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
        
        return timestamp
    
    async def _save_candle(self, candle: Candle):
        """Save completed candle"""
        try:
            # Cache candle
            await self.cache.set_candle(
                candle.market_id,
                candle.interval,
                candle.to_dict()
            )
            
            # TODO: Save to Cassandra for historical data
            
        except Exception as e:
            logger.error(f"Error saving candle: {e}")
    
    async def _aggregation_loop(self):
        """Aggregate trades periodically"""
        while self._running:
            try:
                # Check if we have trades to aggregate
                if self.trade_buffer and (
                    time.time() - self.last_aggregation_time > 
                    self.config.AGGREGATION_WINDOW_MS / 1000
                ):
                    # Process buffer
                    trades = self.trade_buffer.copy()
                    self.trade_buffer.clear()
                    self.last_aggregation_time = time.time()
                    
                    # Aggregate by market
                    market_trades = defaultdict(list)
                    for trade in trades:
                        market_trades[trade.market_id].append(trade)
                    
                    # Publish aggregated data
                    for market_id, trades in market_trades.items():
                        await self._publish_aggregated_trades(market_id, trades)
                
                await asyncio.sleep(self.config.AGGREGATION_WINDOW_MS / 1000)
                
            except Exception as e:
                logger.error(f"Error in aggregation loop: {e}")
                await asyncio.sleep(1)
    
    async def _candle_builder_loop(self):
        """Check and close candles periodically"""
        while self._running:
            try:
                now = datetime.utcnow()
                
                for market_id, state in self.market_states.items():
                    for interval, builder in state.candle_builders.items():
                        if now >= builder._get_close_time():
                            # Close and save candle
                            candle = builder.to_candle()
                            candle.market_id = market_id
                            await self._save_candle(candle)
                            
                            # Create new builder with last close price
                            state.candle_builders[interval] = self._create_candle_builder(
                                interval, now, builder.close
                            )
                
                await asyncio.sleep(1)  # Check every second
                
            except Exception as e:
                logger.error(f"Error in candle builder loop: {e}")
                await asyncio.sleep(1)
    
    async def _snapshot_publisher_loop(self):
        """Publish market snapshots periodically"""
        while self._running:
            try:
                # Publish snapshots for all markets
                for market_id in self.market_states:
                    await self._publish_market_snapshot(market_id)
                
                await asyncio.sleep(self.config.SNAPSHOT_INTERVAL_SECONDS)
                
            except Exception as e:
                logger.error(f"Error in snapshot publisher: {e}")
                await asyncio.sleep(5)
    
    async def _stats_calculator_loop(self):
        """Calculate market statistics periodically"""
        while self._running:
            try:
                # Update 24h stats
                cutoff_time = datetime.utcnow() - timedelta(hours=24)
                
                for market_id, state in self.market_states.items():
                    # Filter recent trades to 24h window
                    trades_24h = [
                        t for t in state.recent_trades 
                        if t.timestamp > cutoff_time
                    ]
                    
                    # Recalculate stats
                    if trades_24h:
                        state.volume_24h = sum(t.quantity for t in trades_24h)
                        state.trade_count_24h = len(trades_24h)
                        prices = [t.price for t in trades_24h]
                        state.high_24h = max(prices)
                        state.low_24h = min(prices)
                
                await asyncio.sleep(60)  # Update every minute
                
            except Exception as e:
                logger.error(f"Error in stats calculator: {e}")
                await asyncio.sleep(60)
    
    async def _publish_aggregated_trades(self, market_id: str, trades: List[TradeEvent]):
        """Publish aggregated trades to subscribers"""
        # TODO: Implement WebSocket publishing
        pass
    
    async def _publish_market_snapshot(self, market_id: str):
        """Publish market snapshot"""
        # TODO: Implement WebSocket publishing
        pass
    
    async def get_market_state(self, market_id: str) -> Optional[MarketState]:
        """Get current market state"""
        return self.market_states.get(market_id)
    
    async def get_orderbook(self, market_id: str) -> Optional[OrderBookSnapshot]:
        """Get current order book"""
        # Try cache first
        cached = await self.cache.get_orderbook(market_id)
        if cached:
            return cached
        
        return self.orderbooks.get(market_id)
    
    async def get_recent_trades(
        self, 
        market_id: str, 
        limit: int = 100
    ) -> List[AggregatedTrade]:
        """Get recent trades for a market"""
        state = self.market_states.get(market_id)
        if not state:
            return []
        
        trades = list(state.recent_trades)
        trades.reverse()  # Most recent first
        return trades[:limit] 