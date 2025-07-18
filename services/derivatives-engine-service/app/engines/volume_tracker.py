"""
Volume and Open Interest Tracking Engine

Tracks trading volume, open interest, and market statistics
"""

from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import asyncio
import logging
from collections import defaultdict, deque
from enum import Enum

# Set high precision
getcontext().prec = 28

logger = logging.getLogger(__name__)


class TimeFrame(Enum):
    """Time frames for volume tracking"""
    MINUTE_1 = "1m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    HOUR_1 = "1h"
    HOUR_4 = "4h"
    DAY_1 = "1d"
    WEEK_1 = "1w"
    MONTH_1 = "1M"


@dataclass
class VolumeBar:
    """OHLCV bar data"""
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trades: int
    buy_volume: Decimal
    sell_volume: Decimal
    
    def update(self, price: Decimal, volume: Decimal, is_buy: bool = True):
        """Update bar with new trade"""
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += volume
        self.trades += 1
        
        if is_buy:
            self.buy_volume += volume
        else:
            self.sell_volume += volume


@dataclass
class MarketStatistics:
    """Market statistics for an instrument"""
    instrument_id: str
    
    # Volume metrics
    volume_24h: Decimal = Decimal("0")
    volume_7d: Decimal = Decimal("0")
    volume_30d: Decimal = Decimal("0")
    
    # Trade metrics
    trades_24h: int = 0
    avg_trade_size: Decimal = Decimal("0")
    
    # Price metrics
    last_price: Decimal = Decimal("0")
    price_change_24h: Decimal = Decimal("0")
    high_24h: Decimal = Decimal("0")
    low_24h: Decimal = Decimal("0")
    vwap_24h: Decimal = Decimal("0")
    
    # Open interest
    open_interest: Decimal = Decimal("0")
    open_interest_change_24h: Decimal = Decimal("0")
    
    # Options specific
    put_call_ratio: Optional[Decimal] = None
    iv_rank: Optional[Decimal] = None  # 0-100 percentile
    
    # Compute specific
    utilization_rate: Optional[Decimal] = None
    avg_duration: Optional[timedelta] = None
    
    last_update: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Trade:
    """Individual trade record"""
    trade_id: str
    instrument_id: str
    price: Decimal
    quantity: Decimal
    side: str  # "buy" or "sell"
    timestamp: datetime
    trader: str
    fee: Decimal = Decimal("0")
    trade_type: str = "regular"  # regular, liquidation, settlement


class VolumeTracker:
    """Tracks volume, open interest, and market statistics"""
    
    def __init__(self, ignite_cache=None, pulsar_publisher=None, 
                 max_history_days: int = 30):
        self.cache = ignite_cache
        self.pulsar = pulsar_publisher
        self.max_history_days = max_history_days
        
        # Volume bars by instrument and timeframe
        self.volume_bars: Dict[str, Dict[TimeFrame, List[VolumeBar]]] = defaultdict(
            lambda: defaultdict(list)
        )
        
        # Current bars being built
        self.current_bars: Dict[str, Dict[TimeFrame, VolumeBar]] = defaultdict(dict)
        
        # Open interest by instrument
        self.open_interest: Dict[str, Decimal] = defaultdict(Decimal)
        
        # Position tracking for OI calculation
        self.positions: Dict[str, Dict[str, Decimal]] = defaultdict(
            lambda: defaultdict(Decimal)
        )  # instrument -> trader -> net_position
        
        # Market statistics
        self.statistics: Dict[str, MarketStatistics] = {}
        
        # Recent trades for analysis
        self.recent_trades: deque = deque(maxlen=10000)
        
        # Trade history by instrument
        self.trade_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=1000)
        )
        
        # Background tasks
        self._running = False
        self._tasks = []
        
    async def start(self):
        """Start volume tracking"""
        self._running = True
        
        # Load historical data
        await self._load_historical_data()
        
        # Start background tasks
        self._tasks.append(asyncio.create_task(self._bar_builder_loop()))
        self._tasks.append(asyncio.create_task(self._statistics_loop()))
        self._tasks.append(asyncio.create_task(self._cleanup_loop()))
        
        logger.info("Volume tracker started")
        
    async def stop(self):
        """Stop volume tracking"""
        self._running = False
        
        for task in self._tasks:
            task.cancel()
            
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
    async def record_trade(
        self,
        instrument_id: str,
        price: Decimal,
        quantity: Decimal,
        side: str,
        trader: str,
        trade_id: Optional[str] = None,
        fee: Decimal = Decimal("0"),
        trade_type: str = "regular"
    ) -> Trade:
        """Record a new trade"""
        
        if not trade_id:
            trade_id = f"{instrument_id}_{datetime.utcnow().timestamp()}"
            
        trade = Trade(
            trade_id=trade_id,
            instrument_id=instrument_id,
            price=price,
            quantity=quantity,
            side=side,
            timestamp=datetime.utcnow(),
            trader=trader,
            fee=fee,
            trade_type=trade_type
        )
        
        # Add to recent trades
        self.recent_trades.append(trade)
        self.trade_history[instrument_id].append(trade)
        
        # Update current bars
        await self._update_bars(trade)
        
        # Update statistics
        await self._update_statistics(instrument_id, trade)
        
        # Emit trade event
        if self.pulsar:
            await self.pulsar.publish(f"trades.{instrument_id}", {
                "trade_id": trade_id,
                "price": str(price),
                "quantity": str(quantity),
                "side": side,
                "timestamp": trade.timestamp.isoformat()
            })
            
        return trade
        
    async def update_open_interest(
        self,
        instrument_id: str,
        trader: str,
        position_change: Decimal
    ):
        """Update open interest based on position changes"""
        
        # Update trader position
        old_position = self.positions[instrument_id][trader]
        new_position = old_position + position_change
        self.positions[instrument_id][trader] = new_position
        
        # Calculate OI change
        # OI increases when new positions are opened
        # OI decreases when positions are closed
        if old_position * new_position < 0:  # Position flip
            oi_change = abs(new_position) - abs(old_position)
        elif abs(new_position) > abs(old_position):  # Position increase
            oi_change = abs(new_position) - abs(old_position)
        else:  # Position decrease
            oi_change = -(abs(old_position) - abs(new_position))
            
        # Update total OI
        self.open_interest[instrument_id] += oi_change
        
        # Update statistics
        if instrument_id in self.statistics:
            self.statistics[instrument_id].open_interest = self.open_interest[instrument_id]
            
        # Store in cache
        if self.cache:
            await self.cache.set(
                f"oi:{instrument_id}",
                self.open_interest[instrument_id]
            )
            
    async def get_volume_bars(
        self,
        instrument_id: str,
        timeframe: TimeFrame,
        limit: int = 100
    ) -> List[VolumeBar]:
        """Get volume bars for an instrument"""
        
        bars = self.volume_bars.get(instrument_id, {}).get(timeframe, [])
        
        # Include current bar if exists
        current = self.current_bars.get(instrument_id, {}).get(timeframe)
        if current:
            bars = bars + [current]
            
        return bars[-limit:]
        
    async def get_statistics(self, instrument_id: str) -> Optional[MarketStatistics]:
        """Get market statistics for an instrument"""
        
        if instrument_id not in self.statistics:
            # Create new statistics
            stats = MarketStatistics(instrument_id=instrument_id)
            self.statistics[instrument_id] = stats
            
        return self.statistics.get(instrument_id)
        
    async def get_volume_profile(
        self,
        instrument_id: str,
        start_time: datetime,
        end_time: datetime,
        price_levels: int = 50
    ) -> Dict[str, Any]:
        """Get volume profile (volume by price level)"""
        
        # Get trades in time range
        trades = [
            t for t in self.trade_history.get(instrument_id, [])
            if start_time <= t.timestamp <= end_time
        ]
        
        if not trades:
            return {"levels": [], "total_volume": "0"}
            
        # Calculate price range
        prices = [t.price for t in trades]
        min_price = min(prices)
        max_price = max(prices)
        price_step = (max_price - min_price) / price_levels
        
        # Build volume profile
        profile = defaultdict(lambda: {"volume": Decimal("0"), "trades": 0})
        
        for trade in trades:
            level = int((trade.price - min_price) / price_step)
            profile[level]["volume"] += trade.quantity
            profile[level]["trades"] += 1
            
        # Convert to list
        levels = []
        for i in range(price_levels):
            price = min_price + (i * price_step)
            data = profile.get(i, {"volume": Decimal("0"), "trades": 0})
            
            levels.append({
                "price": str(price),
                "volume": str(data["volume"]),
                "trades": data["trades"]
            })
            
        return {
            "levels": levels,
            "total_volume": str(sum(t.quantity for t in trades)),
            "total_trades": len(trades),
            "price_range": {
                "min": str(min_price),
                "max": str(max_price)
            }
        }
        
    async def get_market_depth_history(
        self,
        instrument_id: str,
        timeframe: TimeFrame = TimeFrame.MINUTE_5
    ) -> List[Dict[str, Any]]:
        """Get historical market depth metrics"""
        
        bars = await self.get_volume_bars(instrument_id, timeframe, limit=100)
        
        depth_history = []
        for bar in bars:
            buy_ratio = bar.buy_volume / bar.volume if bar.volume > 0 else Decimal("0.5")
            
            depth_history.append({
                "timestamp": bar.timestamp.isoformat(),
                "buy_volume": str(bar.buy_volume),
                "sell_volume": str(bar.sell_volume),
                "buy_ratio": str(buy_ratio),
                "total_volume": str(bar.volume),
                "avg_trade_size": str(bar.volume / bar.trades if bar.trades > 0 else 0)
            })
            
        return depth_history
        
    async def _update_bars(self, trade: Trade):
        """Update volume bars with new trade"""
        
        for timeframe in TimeFrame:
            # Get or create current bar
            current = self.current_bars[trade.instrument_id].get(timeframe)
            
            if current is None or self._should_create_new_bar(current.timestamp, timeframe):
                # Close current bar and start new one
                if current:
                    self.volume_bars[trade.instrument_id][timeframe].append(current)
                    
                current = VolumeBar(
                    timestamp=self._get_bar_timestamp(datetime.utcnow(), timeframe),
                    open=trade.price,
                    high=trade.price,
                    low=trade.price,
                    close=trade.price,
                    volume=Decimal("0"),
                    trades=0,
                    buy_volume=Decimal("0"),
                    sell_volume=Decimal("0")
                )
                self.current_bars[trade.instrument_id][timeframe] = current
                
            # Update bar
            current.update(trade.price, trade.quantity, trade.side == "buy")
            
    async def _update_statistics(self, instrument_id: str, trade: Trade):
        """Update market statistics"""
        
        stats = await self.get_statistics(instrument_id)
        
        # Update last price
        stats.last_price = trade.price
        
        # Get 24h ago price
        cutoff_24h = datetime.utcnow() - timedelta(hours=24)
        trades_24h = [
            t for t in self.trade_history[instrument_id]
            if t.timestamp > cutoff_24h
        ]
        
        if trades_24h:
            # Volume metrics
            stats.volume_24h = sum(t.quantity for t in trades_24h)
            stats.trades_24h = len(trades_24h)
            stats.avg_trade_size = stats.volume_24h / stats.trades_24h
            
            # Price metrics
            prices = [t.price for t in trades_24h]
            stats.high_24h = max(prices)
            stats.low_24h = min(prices)
            
            # Price change
            first_price = trades_24h[0].price
            stats.price_change_24h = (trade.price - first_price) / first_price
            
            # VWAP
            total_value = sum(t.price * t.quantity for t in trades_24h)
            stats.vwap_24h = total_value / stats.volume_24h if stats.volume_24h > 0 else trade.price
            
        stats.last_update = datetime.utcnow()
        
        # Store in cache
        if self.cache:
            await self.cache.set(
                f"stats:{instrument_id}",
                stats
            )
            
    def _should_create_new_bar(self, bar_timestamp: datetime, timeframe: TimeFrame) -> bool:
        """Check if new bar should be created"""
        
        now = datetime.utcnow()
        
        if timeframe == TimeFrame.MINUTE_1:
            return now.minute != bar_timestamp.minute
        elif timeframe == TimeFrame.MINUTE_5:
            return now.minute // 5 != bar_timestamp.minute // 5
        elif timeframe == TimeFrame.MINUTE_15:
            return now.minute // 15 != bar_timestamp.minute // 15
        elif timeframe == TimeFrame.HOUR_1:
            return now.hour != bar_timestamp.hour
        elif timeframe == TimeFrame.HOUR_4:
            return now.hour // 4 != bar_timestamp.hour // 4
        elif timeframe == TimeFrame.DAY_1:
            return now.date() != bar_timestamp.date()
        elif timeframe == TimeFrame.WEEK_1:
            return now.isocalendar()[1] != bar_timestamp.isocalendar()[1]
        elif timeframe == TimeFrame.MONTH_1:
            return now.month != bar_timestamp.month
            
        return False
        
    def _get_bar_timestamp(self, timestamp: datetime, timeframe: TimeFrame) -> datetime:
        """Get normalized bar timestamp"""
        
        if timeframe == TimeFrame.MINUTE_1:
            return timestamp.replace(second=0, microsecond=0)
        elif timeframe == TimeFrame.MINUTE_5:
            minute = (timestamp.minute // 5) * 5
            return timestamp.replace(minute=minute, second=0, microsecond=0)
        elif timeframe == TimeFrame.MINUTE_15:
            minute = (timestamp.minute // 15) * 15
            return timestamp.replace(minute=minute, second=0, microsecond=0)
        elif timeframe == TimeFrame.HOUR_1:
            return timestamp.replace(minute=0, second=0, microsecond=0)
        elif timeframe == TimeFrame.HOUR_4:
            hour = (timestamp.hour // 4) * 4
            return timestamp.replace(hour=hour, minute=0, second=0, microsecond=0)
        elif timeframe == TimeFrame.DAY_1:
            return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        elif timeframe == TimeFrame.WEEK_1:
            # Start of week (Monday)
            days_since_monday = timestamp.weekday()
            return (timestamp - timedelta(days=days_since_monday)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        elif timeframe == TimeFrame.MONTH_1:
            return timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
        return timestamp
        
    async def _bar_builder_loop(self):
        """Background task to close bars on schedule"""
        
        while self._running:
            try:
                # Check each timeframe
                for instrument_id in list(self.current_bars.keys()):
                    for timeframe in TimeFrame:
                        current = self.current_bars[instrument_id].get(timeframe)
                        
                        if current and self._should_create_new_bar(current.timestamp, timeframe):
                            # Close bar
                            self.volume_bars[instrument_id][timeframe].append(current)
                            del self.current_bars[instrument_id][timeframe]
                            
                            # Emit bar close event
                            if self.pulsar:
                                await self.pulsar.publish(f"bars.{instrument_id}.{timeframe.value}", {
                                    "timestamp": current.timestamp.isoformat(),
                                    "open": str(current.open),
                                    "high": str(current.high),
                                    "low": str(current.low),
                                    "close": str(current.close),
                                    "volume": str(current.volume)
                                })
                                
                await asyncio.sleep(1)  # Check every second
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in bar builder loop: {e}")
                await asyncio.sleep(5)
                
    async def _statistics_loop(self):
        """Background task to update statistics"""
        
        while self._running:
            try:
                # Update statistics for all instruments
                for instrument_id in self.statistics:
                    stats = self.statistics[instrument_id]
                    
                    # Calculate additional metrics
                    if instrument_id.endswith("_CALL") or instrument_id.endswith("_PUT"):
                        # Options specific metrics
                        await self._update_options_metrics(instrument_id, stats)
                        
                    # Store updated stats
                    if self.cache:
                        await self.cache.set(f"stats:{instrument_id}", stats)
                        
                await asyncio.sleep(60)  # Update every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in statistics loop: {e}")
                await asyncio.sleep(60)
                
    async def _cleanup_loop(self):
        """Background task to clean old data"""
        
        while self._running:
            try:
                cutoff = datetime.utcnow() - timedelta(days=self.max_history_days)
                
                # Clean volume bars
                for instrument_id in list(self.volume_bars.keys()):
                    for timeframe in TimeFrame:
                        bars = self.volume_bars[instrument_id][timeframe]
                        self.volume_bars[instrument_id][timeframe] = [
                            b for b in bars if b.timestamp > cutoff
                        ]
                        
                await asyncio.sleep(3600)  # Clean every hour
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(3600)
                
    async def _load_historical_data(self):
        """Load historical data from cache"""
        
        if not self.cache:
            return
            
        try:
            # Load open interest
            oi_keys = await self.cache.get_keys("oi:*")
            for key in oi_keys:
                instrument_id = key.split(":", 1)[1]
                self.open_interest[instrument_id] = await self.cache.get(key)
                
            # Load statistics
            stats_keys = await self.cache.get_keys("stats:*")
            for key in stats_keys:
                instrument_id = key.split(":", 1)[1]
                self.statistics[instrument_id] = await self.cache.get(key)
                
            logger.info(f"Loaded historical data for {len(self.statistics)} instruments")
            
        except Exception as e:
            logger.error(f"Error loading historical data: {e}")
            
    async def _update_options_metrics(self, instrument_id: str, stats: MarketStatistics):
        """Update options-specific metrics"""
        
        # Calculate put/call ratio if this is an option
        if "_CALL" in instrument_id:
            put_id = instrument_id.replace("_CALL", "_PUT")
            if put_id in self.statistics:
                call_volume = stats.volume_24h
                put_volume = self.statistics[put_id].volume_24h
                
                if call_volume > 0:
                    stats.put_call_ratio = put_volume / call_volume
                    
    def get_summary(self) -> Dict[str, Any]:
        """Get overall volume tracker summary"""
        
        total_volume_24h = sum(s.volume_24h for s in self.statistics.values())
        total_trades_24h = sum(s.trades_24h for s in self.statistics.values())
        total_open_interest = sum(self.open_interest.values())
        
        return {
            "instruments_tracked": len(self.statistics),
            "total_volume_24h": str(total_volume_24h),
            "total_trades_24h": total_trades_24h,
            "total_open_interest": str(total_open_interest),
            "active_traders": len(set(t.trader for t in self.recent_trades)),
            "recent_trades": len(self.recent_trades)
        } 