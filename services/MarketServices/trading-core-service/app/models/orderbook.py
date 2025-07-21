"""Order book models."""

from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Optional, Tuple
from pydantic import BaseModel, Field


class OrderBookLevel(BaseModel):
    """Single level in the order book."""
    price: Decimal
    quantity: Decimal
    order_count: int = 1
    
    class Config:
        json_encoders = {
            Decimal: str
        }


class OrderBookSnapshot(BaseModel):
    """Full order book snapshot."""
    market_id: str
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    sequence_number: int
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    @property
    def best_bid(self) -> Optional[OrderBookLevel]:
        """Get best bid."""
        return self.bids[0] if self.bids else None
    
    @property
    def best_ask(self) -> Optional[OrderBookLevel]:
        """Get best ask."""
        return self.asks[0] if self.asks else None
    
    @property
    def spread(self) -> Optional[Decimal]:
        """Calculate spread."""
        if self.best_bid and self.best_ask:
            return self.best_ask.price - self.best_bid.price
        return None
    
    @property
    def mid_price(self) -> Optional[Decimal]:
        """Calculate mid price."""
        if self.best_bid and self.best_ask:
            return (self.best_bid.price + self.best_ask.price) / 2
        return None
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class OrderBookUpdate(BaseModel):
    """Incremental order book update."""
    market_id: str
    update_type: str  # "add", "update", "remove"
    side: str  # "bid" or "ask"
    price: Decimal
    quantity: Decimal
    sequence_number: int
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class OrderBook:
    """In-memory order book implementation."""
    
    def __init__(self, market_id: str, tick_size: Decimal):
        self.market_id = market_id
        self.tick_size = tick_size
        self.bids: Dict[Decimal, OrderBookLevel] = {}
        self.asks: Dict[Decimal, OrderBookLevel] = {}
        self.sequence_number = 0
        self._last_update = datetime.utcnow()
    
    def add_order(self, side: str, price: Decimal, quantity: Decimal) -> OrderBookUpdate:
        """Add order to book."""
        price = self._normalize_price(price)
        book = self.bids if side == "buy" else self.asks
        
        if price in book:
            book[price].quantity += quantity
            book[price].order_count += 1
            update_type = "update"
        else:
            book[price] = OrderBookLevel(
                price=price,
                quantity=quantity,
                order_count=1
            )
            update_type = "add"
        
        self.sequence_number += 1
        self._last_update = datetime.utcnow()
        
        return OrderBookUpdate(
            market_id=self.market_id,
            update_type=update_type,
            side="bid" if side == "buy" else "ask",
            price=price,
            quantity=book[price].quantity,
            sequence_number=self.sequence_number
        )
    
    def remove_order(self, side: str, price: Decimal, quantity: Decimal) -> Optional[OrderBookUpdate]:
        """Remove order from book."""
        price = self._normalize_price(price)
        book = self.bids if side == "buy" else self.asks
        
        if price not in book:
            return None
        
        book[price].quantity -= quantity
        book[price].order_count -= 1
        
        if book[price].quantity <= 0 or book[price].order_count <= 0:
            del book[price]
            update_type = "remove"
            remaining_quantity = Decimal("0")
        else:
            update_type = "update"
            remaining_quantity = book[price].quantity
        
        self.sequence_number += 1
        self._last_update = datetime.utcnow()
        
        return OrderBookUpdate(
            market_id=self.market_id,
            update_type=update_type,
            side="bid" if side == "buy" else "ask",
            price=price,
            quantity=remaining_quantity,
            sequence_number=self.sequence_number
        )
    
    def get_snapshot(self, depth: int = 20) -> OrderBookSnapshot:
        """Get order book snapshot."""
        # Sort bids descending, asks ascending
        sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:depth]
        sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])[:depth]
        
        return OrderBookSnapshot(
            market_id=self.market_id,
            bids=[level for _, level in sorted_bids],
            asks=[level for _, level in sorted_asks],
            sequence_number=self.sequence_number
        )
    
    def get_best_bid_ask(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Get best bid and ask prices."""
        best_bid = max(self.bids.keys()) if self.bids else None
        best_ask = min(self.asks.keys()) if self.asks else None
        return best_bid, best_ask
    
    def match_order(self, side: str, quantity: Decimal, 
                   limit_price: Optional[Decimal] = None) -> List[Tuple[Decimal, Decimal]]:
        """Match order against book and return fills."""
        fills = []
        remaining = quantity
        
        # Get the opposite side book
        book = self.asks if side == "buy" else self.bids
        
        # Sort prices (ascending for asks, descending for bids)
        prices = sorted(book.keys(), reverse=(side == "sell"))
        
        for price in prices:
            # Check price limit
            if limit_price:
                if side == "buy" and price > limit_price:
                    break
                elif side == "sell" and price < limit_price:
                    break
            
            level = book[price]
            fill_quantity = min(remaining, level.quantity)
            
            if fill_quantity > 0:
                fills.append((price, fill_quantity))
                remaining -= fill_quantity
                
                if remaining <= 0:
                    break
        
        return fills
    
    def _normalize_price(self, price: Decimal) -> Decimal:
        """Normalize price to tick size."""
        return (price / self.tick_size).quantize(Decimal('1')) * self.tick_size
    
    def clear(self):
        """Clear the order book."""
        self.bids.clear()
        self.asks.clear()
        self.sequence_number += 1
        self._last_update = datetime.utcnow() 