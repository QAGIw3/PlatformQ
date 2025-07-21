"""
Auction data models for DeFi protocol service.
"""

from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any


class AuctionType(Enum):
    """Types of auctions supported"""
    DUTCH = "dutch"
    ENGLISH = "english"
    SEALED_BID = "sealed_bid"
    VICKREY = "vickrey"


class AuctionStatus(Enum):
    """Auction status states"""
    PENDING = "pending"
    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass
class Bid:
    """Represents a bid in an auction"""
    bidder: str
    amount: Decimal
    timestamp: datetime
    tx_hash: str
    is_winning: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Auction:
    """Represents an auction instance"""
    id: str
    type: AuctionType
    chain: str
    token_address: str
    token_id: int
    seller: str
    start_price: Decimal
    current_price: Decimal
    end_price: Optional[Decimal]  # Reserve price or Dutch auction end price
    start_time: datetime
    end_time: datetime
    status: AuctionStatus
    bids: List[Bid] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_active(self) -> bool:
        """Check if auction is currently active"""
        return (
            self.status == AuctionStatus.ACTIVE and
            datetime.utcnow() >= self.start_time and
            datetime.utcnow() < self.end_time
        )
    
    @property
    def time_remaining(self) -> int:
        """Get remaining time in seconds"""
        if not self.is_active:
            return 0
        return int((self.end_time - datetime.utcnow()).total_seconds())
    
    @property
    def highest_bid(self) -> Optional[Bid]:
        """Get the highest bid (for English auctions)"""
        if not self.bids:
            return None
        return max(self.bids, key=lambda b: b.amount)
    
    def add_bid(self, bid: Bid) -> bool:
        """Add a bid to the auction"""
        if self.type == AuctionType.ENGLISH:
            # Check if bid is higher than current
            if self.highest_bid and bid.amount <= self.highest_bid.amount:
                return False
            
            # Mark previous highest bid as not winning
            if self.highest_bid:
                self.highest_bid.is_winning = False
                
            bid.is_winning = True
            self.current_price = bid.amount
            
        self.bids.append(bid)
        return True 