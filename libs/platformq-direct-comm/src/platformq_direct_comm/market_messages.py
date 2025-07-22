"""
Market-specific message types for direct communication.

Optimized for ultra-low latency market data and trading operations.
"""

from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime
import msgpack
import numpy as np

from .message_types import DirectMessage, MessageType


class MarketMessageType:
    """Market-specific message types"""
    # Market data
    MARKET_DATA_SNAPSHOT = 2001
    ORDERBOOK_UPDATE = 2002
    BEST_BID_ASK = 2003
    TRADE_TICK = 2004
    
    # Trading
    ORDER_REQUEST = 2101
    ORDER_RESPONSE = 2102
    ORDER_STATUS = 2103
    POSITION_UPDATE = 2104
    
    # Risk
    RISK_CHECK_REQUEST = 2201
    RISK_CHECK_RESPONSE = 2202
    MARGIN_UPDATE = 2203
    EXPOSURE_QUERY = 2204
    
    # Market Intelligence
    ML_PREDICTION = 2301
    SIGNAL_ALERT = 2302
    CORRELATION_UPDATE = 2303
    
    # Cross-service coordination
    LIQUIDITY_REQUEST = 2401
    LIQUIDITY_RESPONSE = 2402
    ARBITRAGE_OPPORTUNITY = 2403


@dataclass
class MarketDataSnapshot(DirectMessage):
    """Optimized market data snapshot"""
    message_type: int = MarketMessageType.MARKET_DATA_SNAPSHOT
    market_id: str = ""
    timestamp_ns: int = 0  # Nanosecond precision
    
    # Price data (stored as integers with scaling factor)
    bid_price: int = 0  # Actual price = bid_price / price_scale
    ask_price: int = 0
    last_price: int = 0
    price_scale: int = 100000000  # 8 decimal places
    
    # Volume data
    bid_volume: int = 0
    ask_volume: int = 0
    volume_24h: int = 0
    
    # Additional metrics
    open_interest: int = 0
    funding_rate: int = 0  # For perpetuals
    
    def to_bytes(self) -> bytes:
        """Ultra-fast serialization"""
        # Pack into fixed-size buffer for speed
        data = msgpack.packb({
            't': self.message_type,
            'i': self.service_id,
            'm': self.market_id,
            'ts': self.timestamp_ns,
            'bp': self.bid_price,
            'ap': self.ask_price,
            'lp': self.last_price,
            'ps': self.price_scale,
            'bv': self.bid_volume,
            'av': self.ask_volume,
            'v24': self.volume_24h,
            'oi': self.open_interest,
            'fr': self.funding_rate
        }, use_bin_type=True)
        return data
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'MarketDataSnapshot':
        """Ultra-fast deserialization"""
        msg_data = msgpack.unpackb(data, raw=False)
        return cls(
            service_id=msg_data['i'],
            market_id=msg_data['m'],
            timestamp_ns=msg_data['ts'],
            bid_price=msg_data['bp'],
            ask_price=msg_data['ap'],
            last_price=msg_data['lp'],
            price_scale=msg_data['ps'],
            bid_volume=msg_data['bv'],
            ask_volume=msg_data['av'],
            volume_24h=msg_data['v24'],
            open_interest=msg_data['oi'],
            funding_rate=msg_data['fr']
        )
    
    def get_bid_price_decimal(self) -> Decimal:
        """Get bid price as Decimal"""
        return Decimal(self.bid_price) / Decimal(self.price_scale)
    
    def get_ask_price_decimal(self) -> Decimal:
        """Get ask price as Decimal"""
        return Decimal(self.ask_price) / Decimal(self.price_scale)


@dataclass
class OrderbookUpdate(DirectMessage):
    """Incremental orderbook update"""
    message_type: int = MarketMessageType.ORDERBOOK_UPDATE
    market_id: str = ""
    timestamp_ns: int = 0
    
    # Updates stored as numpy arrays for performance
    bid_updates: Optional[np.ndarray] = None  # Shape: (n, 2) - [price, volume]
    ask_updates: Optional[np.ndarray] = None
    
    # Removed levels
    bid_removes: Optional[List[int]] = None  # Price levels to remove
    ask_removes: Optional[List[int]] = None
    
    def to_bytes(self) -> bytes:
        """Serialize with numpy arrays"""
        data = {
            't': self.message_type,
            'i': self.service_id,
            'm': self.market_id,
            'ts': self.timestamp_ns
        }
        
        if self.bid_updates is not None:
            data['bu'] = self.bid_updates.tobytes()
            data['bu_shape'] = self.bid_updates.shape
            
        if self.ask_updates is not None:
            data['au'] = self.ask_updates.tobytes()
            data['au_shape'] = self.ask_updates.shape
            
        if self.bid_removes:
            data['br'] = self.bid_removes
            
        if self.ask_removes:
            data['ar'] = self.ask_removes
            
        return msgpack.packb(data, use_bin_type=True)
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'OrderbookUpdate':
        """Deserialize with numpy arrays"""
        msg_data = msgpack.unpackb(data, raw=False)
        
        obj = cls(
            service_id=msg_data['i'],
            market_id=msg_data['m'],
            timestamp_ns=msg_data['ts']
        )
        
        if 'bu' in msg_data:
            obj.bid_updates = np.frombuffer(
                msg_data['bu'], dtype=np.int64
            ).reshape(msg_data['bu_shape'])
            
        if 'au' in msg_data:
            obj.ask_updates = np.frombuffer(
                msg_data['au'], dtype=np.int64
            ).reshape(msg_data['au_shape'])
            
        obj.bid_removes = msg_data.get('br')
        obj.ask_removes = msg_data.get('ar')
        
        return obj


@dataclass
class RiskCheckRequest(DirectMessage):
    """Ultra-fast risk check request"""
    message_type: int = MarketMessageType.RISK_CHECK_REQUEST
    user_id: str = ""
    check_id: str = ""  # For response correlation
    
    # Order details
    market_id: str = ""
    side: str = ""  # buy/sell
    size: int = 0  # Size in base units
    price: int = 0  # Price in scaled units
    order_type: str = ""  # market/limit
    
    # Risk parameters
    leverage: int = 1
    reduce_only: bool = False
    
    def to_bytes(self) -> bytes:
        """Compact serialization"""
        # Pack side and order type as single byte
        flags = 0
        if self.side == "buy":
            flags |= 1
        if self.order_type == "market":
            flags |= 2
        if self.reduce_only:
            flags |= 4
            
        return msgpack.packb({
            't': self.message_type,
            'i': self.service_id,
            'u': self.user_id,
            'c': self.check_id,
            'm': self.market_id,
            'f': flags,
            's': self.size,
            'p': self.price,
            'l': self.leverage
        }, use_bin_type=True)


@dataclass
class RiskCheckResponse(DirectMessage):
    """Ultra-fast risk check response"""
    message_type: int = MarketMessageType.RISK_CHECK_RESPONSE
    check_id: str = ""
    
    # Results
    approved: bool = False
    reason: str = ""  # Empty if approved
    
    # Risk metrics
    margin_required: int = 0
    margin_available: int = 0
    position_value: int = 0
    max_size: int = 0  # Maximum allowed size
    
    # Timing
    check_latency_us: int = 0  # Microseconds
    
    def to_bytes(self) -> bytes:
        """Compact response"""
        return msgpack.packb({
            't': self.message_type,
            'i': self.service_id,
            'c': self.check_id,
            'a': self.approved,
            'r': self.reason,
            'mr': self.margin_required,
            'ma': self.margin_available,
            'pv': self.position_value,
            'ms': self.max_size,
            'l': self.check_latency_us
        }, use_bin_type=True)


@dataclass
class MLPrediction(DirectMessage):
    """Machine learning prediction broadcast"""
    message_type: int = MarketMessageType.ML_PREDICTION
    market_id: str = ""
    model_id: str = ""
    timestamp_ns: int = 0
    
    # Predictions
    price_prediction: int = 0  # Predicted price
    confidence: float = 0.0  # 0-1
    direction: str = ""  # up/down/neutral
    horizon_minutes: int = 0
    
    # Feature importance (top features)
    top_features: Optional[Dict[str, float]] = None
    
    def to_bytes(self) -> bytes:
        """Serialize prediction"""
        return msgpack.packb({
            't': self.message_type,
            'i': self.service_id,
            'm': self.market_id,
            'mo': self.model_id,
            'ts': self.timestamp_ns,
            'pp': self.price_prediction,
            'c': self.confidence,
            'd': self.direction,
            'h': self.horizon_minutes,
            'f': self.top_features
        }, use_bin_type=True)


@dataclass
class ArbitrageOpportunity(DirectMessage):
    """Cross-market arbitrage alert"""
    message_type: int = MarketMessageType.ARBITRAGE_OPPORTUNITY
    opportunity_id: str = ""
    timestamp_ns: int = 0
    
    # Markets involved
    buy_market: str = ""
    sell_market: str = ""
    
    # Prices and volumes
    buy_price: int = 0
    sell_price: int = 0
    max_volume: int = 0
    
    # Profitability
    gross_profit_bps: int = 0  # Basis points
    net_profit_bps: int = 0  # After fees
    confidence: float = 0.0
    
    # Timing
    ttl_ms: int = 0  # Time to live in milliseconds
    
    def to_bytes(self) -> bytes:
        """Serialize opportunity"""
        return msgpack.packb({
            't': self.message_type,
            'i': self.service_id,
            'o': self.opportunity_id,
            'ts': self.timestamp_ns,
            'bm': self.buy_market,
            'sm': self.sell_market,
            'bp': self.buy_price,
            'sp': self.sell_price,
            'v': self.max_volume,
            'gp': self.gross_profit_bps,
            'np': self.net_profit_bps,
            'c': self.confidence,
            'ttl': self.ttl_ms
        }, use_bin_type=True)


class MarketMessageRegistry:
    """Registry for market message types"""
    
    _message_classes = {
        MarketMessageType.MARKET_DATA_SNAPSHOT: MarketDataSnapshot,
        MarketMessageType.ORDERBOOK_UPDATE: OrderbookUpdate,
        MarketMessageType.RISK_CHECK_REQUEST: RiskCheckRequest,
        MarketMessageType.RISK_CHECK_RESPONSE: RiskCheckResponse,
        MarketMessageType.ML_PREDICTION: MLPrediction,
        MarketMessageType.ARBITRAGE_OPPORTUNITY: ArbitrageOpportunity
    }
    
    @classmethod
    def register_message_type(cls, message_type: int, message_class):
        """Register a custom message type"""
        cls._message_classes[message_type] = message_class
    
    @classmethod
    def deserialize(cls, data: bytes) -> DirectMessage:
        """Deserialize message based on type"""
        # Peek at message type
        msg_data = msgpack.unpackb(data, raw=False)
        message_type = msg_data.get('t')
        
        message_class = cls._message_classes.get(message_type)
        if message_class:
            return message_class.from_bytes(data)
        else:
            raise ValueError(f"Unknown message type: {message_type}") 