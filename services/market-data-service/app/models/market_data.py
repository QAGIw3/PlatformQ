"""Market data models"""

from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any


@dataclass
class PriceTick:
    """Price tick data"""
    market_id: str
    price: Decimal
    timestamp: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "market_id": self.market_id,
            "price": str(self.price),
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class OrderBookSnapshot:
    """Full order book snapshot"""
    market_id: str
    bids: List[Tuple[Decimal, Decimal]]  # [(price, quantity), ...]
    asks: List[Tuple[Decimal, Decimal]]  # [(price, quantity), ...]
    sequence: int
    timestamp: datetime
    
    @property
    def best_bid(self) -> Optional[Decimal]:
        return self.bids[0][0] if self.bids else None
    
    @property
    def best_ask(self) -> Optional[Decimal]:
        return self.asks[0][0] if self.asks else None
    
    @property
    def spread(self) -> Optional[Decimal]:
        if self.best_bid and self.best_ask:
            return self.best_ask - self.best_bid
        return None
    
    @property
    def mid_price(self) -> Optional[Decimal]:
        if self.best_bid and self.best_ask:
            return (self.best_bid + self.best_ask) / 2
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "market_id": self.market_id,
            "bids": [[str(p), str(q)] for p, q in self.bids],
            "asks": [[str(p), str(q)] for p, q in self.asks],
            "sequence": self.sequence,
            "timestamp": self.timestamp.isoformat(),
            "best_bid": str(self.best_bid) if self.best_bid else None,
            "best_ask": str(self.best_ask) if self.best_ask else None,
            "spread": str(self.spread) if self.spread else None,
            "mid_price": str(self.mid_price) if self.mid_price else None
        }


@dataclass
class OrderBookUpdate:
    """Order book delta update"""
    market_id: str
    sequence: int
    timestamp: datetime
    bid_updates: List[Tuple[Decimal, Decimal]]  # [(price, quantity), ...] quantity=0 means remove
    ask_updates: List[Tuple[Decimal, Decimal]]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "market_id": self.market_id,
            "sequence": self.sequence,
            "timestamp": self.timestamp.isoformat(),
            "bid_updates": [[str(p), str(q)] for p, q in self.bid_updates],
            "ask_updates": [[str(p), str(q)] for p, q in self.ask_updates]
        }


@dataclass
class AggregatedTrade:
    """Aggregated trade data"""
    market_id: str
    trade_id: str
    price: Decimal
    quantity: Decimal
    maker_side: str  # "buy" or "sell"
    timestamp: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "market_id": self.market_id,
            "trade_id": self.trade_id,
            "price": str(self.price),
            "quantity": str(self.quantity),
            "maker_side": self.maker_side,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class Candle:
    """OHLCV candle data"""
    market_id: str
    interval: str  # "1m", "5m", "1h", etc.
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trade_count: int
    
    @property
    def change(self) -> Decimal:
        """Price change in the period"""
        return self.close - self.open
    
    @property
    def change_percent(self) -> Decimal:
        """Price change percentage"""
        if self.open == 0:
            return Decimal(0)
        return (self.change / self.open) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "market_id": self.market_id,
            "interval": self.interval,
            "open_time": self.open_time.isoformat(),
            "close_time": self.close_time.isoformat(),
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume),
            "trade_count": self.trade_count,
            "change": str(self.change),
            "change_percent": str(self.change_percent)
        }


@dataclass
class MarketStats:
    """24h market statistics"""
    market_id: str
    last_price: Decimal
    price_change_24h: Decimal
    price_change_percent_24h: Decimal
    high_24h: Decimal
    low_24h: Decimal
    volume_24h: Decimal
    quote_volume_24h: Decimal
    trade_count_24h: int
    open_interest: Optional[Decimal] = None  # For derivatives
    funding_rate: Optional[Decimal] = None  # For perpetuals
    mark_price: Optional[Decimal] = None  # For derivatives
    index_price: Optional[Decimal] = None  # For derivatives
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "market_id": self.market_id,
            "last_price": str(self.last_price),
            "price_change_24h": str(self.price_change_24h),
            "price_change_percent_24h": str(self.price_change_percent_24h),
            "high_24h": str(self.high_24h),
            "low_24h": str(self.low_24h),
            "volume_24h": str(self.volume_24h),
            "quote_volume_24h": str(self.quote_volume_24h),
            "trade_count_24h": self.trade_count_24h,
            "open_interest": str(self.open_interest) if self.open_interest else None,
            "funding_rate": str(self.funding_rate) if self.funding_rate else None,
            "mark_price": str(self.mark_price) if self.mark_price else None,
            "index_price": str(self.index_price) if self.index_price else None,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class MarketInfo:
    """Market metadata"""
    market_id: str
    base_asset: str
    quote_asset: str
    market_type: str  # "spot", "futures", "perpetual", etc.
    status: str  # "active", "suspended", "delisted"
    tick_size: Decimal
    lot_size: Decimal
    min_order_size: Decimal
    max_order_size: Decimal
    maker_fee: Decimal
    taker_fee: Decimal
    
    # Derivatives specific
    contract_size: Optional[Decimal] = None
    expiry_date: Optional[datetime] = None
    settlement_asset: Optional[str] = None
    underlying_asset: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "market_id": self.market_id,
            "base_asset": self.base_asset,
            "quote_asset": self.quote_asset,
            "market_type": self.market_type,
            "status": self.status,
            "tick_size": str(self.tick_size),
            "lot_size": str(self.lot_size),
            "min_order_size": str(self.min_order_size),
            "max_order_size": str(self.max_order_size),
            "maker_fee": str(self.maker_fee),
            "taker_fee": str(self.taker_fee),
            "contract_size": str(self.contract_size) if self.contract_size else None,
            "expiry_date": self.expiry_date.isoformat() if self.expiry_date else None,
            "settlement_asset": self.settlement_asset,
            "underlying_asset": self.underlying_asset
        } 