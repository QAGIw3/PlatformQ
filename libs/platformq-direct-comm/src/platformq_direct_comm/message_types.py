"""Message types and data structures for direct communication."""

from dataclasses import dataclass
from enum import IntEnum
from typing import Optional, Any


class MessageType(IntEnum):
    """Binary message types for efficient routing."""
    ORDER_SUBMIT = 1
    ORDER_CANCEL = 2
    ORDER_UPDATE = 3
    TRADE_EXECUTE = 4
    POSITION_UPDATE = 5
    COPY_TRADE = 6
    RISK_CHECK = 7
    MARKET_DATA = 8
    HEARTBEAT = 9
    SYSTEM_STATUS = 10
    
    # Add more as needed
    CUSTOM_START = 1000  # Custom message types start here


@dataclass
class DirectMessage:
    """High-performance message format."""
    msg_type: MessageType
    sender_id: str
    correlation_id: str
    payload: bytes
    timestamp_ns: int
    
    # Optional fields
    priority: int = 0  # 0 = normal, higher = more urgent
    ttl_ms: Optional[int] = None  # Time to live in milliseconds
    metadata: Optional[dict] = None
    
    def is_expired(self, current_time_ns: int) -> bool:
        """Check if message has expired."""
        if self.ttl_ms is None:
            return False
        
        age_ms = (current_time_ns - self.timestamp_ns) / 1_000_000
        return age_ms > self.ttl_ms 