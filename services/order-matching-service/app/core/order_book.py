from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Deque
from collections import deque, defaultdict
from sortedcontainers import SortedDict
import time
import threading
from ..models.order import Order, OrderSide, Trade
from ..config import OrderMatchingConfig


class PriceLevel:
    """Represents a single price level in the order book"""
    
    def __init__(self, price: Decimal):
        self.price = price
        self.orders: Deque[Order] = deque()
        self.total_quantity = Decimal(0)
        self._lock = threading.RLock()
    
    def add_order(self, order: Order):
        """Add order to price level"""
        with self._lock:
            self.orders.append(order)
            self.total_quantity += order.remaining_quantity
    
    def remove_order(self, order_id: str) -> Optional[Order]:
        """Remove order from price level"""
        with self._lock:
            for i, order in enumerate(self.orders):
                if order.order_id == order_id:
                    self.orders.remove(order)
                    self.total_quantity -= order.remaining_quantity
                    return order
        return None
    
    def peek_order(self) -> Optional[Order]:
        """Get first order without removing"""
        return self.orders[0] if self.orders else None
    
    def is_empty(self) -> bool:
        """Check if price level has no orders"""
        return len(self.orders) == 0


class OrderBook:
    """High-performance order book implementation"""
    
    def __init__(self, market_id: str, config: OrderMatchingConfig):
        self.market_id = market_id
        self.config = config
        
        # Sorted dictionaries for price levels
        # Buy orders: sorted descending (highest price first)
        # Sell orders: sorted ascending (lowest price first)
        self.buy_levels: SortedDict = SortedDict(lambda x: -x)
        self.sell_levels: SortedDict = SortedDict()
        
        # Order lookup for O(1) access
        self.orders: Dict[str, Order] = {}
        self.order_to_price: Dict[str, Decimal] = {}
        
        # Market data
        self.last_trade_price: Optional[Decimal] = None
        self.last_trade_time_ns: Optional[int] = None
        self.best_bid: Optional[Decimal] = None
        self.best_ask: Optional[Decimal] = None
        
        # Statistics
        self.total_volume = Decimal(0)
        self.trade_count = 0
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Pre-allocated trade ID counter
        self._trade_counter = 0
    
    def add_order(self, order: Order) -> List[Trade]:
        """Add order to book and attempt matching"""
        with self._lock:
            # Validate order
            if order.order_id in self.orders:
                raise ValueError(f"Order {order.order_id} already exists")
            
            # Store order
            self.orders[order.order_id] = order
            
            # Attempt to match immediately
            trades = self._match_order(order)
            
            # If order not fully filled, add to book
            if order.remaining_quantity > 0 and order.order_type != "market":
                self._add_to_book(order)
            
            return trades
    
    def cancel_order(self, order_id: str) -> Optional[Order]:
        """Cancel an order"""
        with self._lock:
            order = self.orders.get(order_id)
            if not order:
                return None
            
            # Remove from price level
            if order_id in self.order_to_price:
                price = self.order_to_price[order_id]
                levels = self.buy_levels if order.is_buy else self.sell_levels
                
                if price in levels:
                    level = levels[price]
                    level.remove_order(order_id)
                    
                    # Remove empty level
                    if level.is_empty():
                        del levels[price]
                
                del self.order_to_price[order_id]
            
            # Remove from lookup
            del self.orders[order_id]
            
            # Update best bid/ask
            self._update_best_prices()
            
            return order
    
    def _match_order(self, order: Order) -> List[Trade]:
        """Match order against opposite side of book"""
        trades = []
        
        if order.is_buy:
            levels = self.sell_levels
        else:
            levels = self.buy_levels
        
        # Iterate through price levels
        for price, level in list(levels.items()):
            # Check if order can match at this price
            if not order.can_match_price(price):
                break
            
            # Match against orders at this level
            while level.orders and order.remaining_quantity > 0:
                passive_order = level.peek_order()
                
                # Calculate match quantity
                match_quantity = min(
                    order.remaining_quantity,
                    passive_order.remaining_quantity
                )
                
                # Create trade
                trade = self._create_trade(
                    order,
                    passive_order,
                    price,
                    match_quantity
                )
                trades.append(trade)
                
                # Update orders
                order.filled_quantity += match_quantity
                passive_order.filled_quantity += match_quantity
                
                # Update price level
                level.total_quantity -= match_quantity
                
                # Remove filled passive order
                if passive_order.remaining_quantity == 0:
                    level.orders.popleft()
                    del self.orders[passive_order.order_id]
                    del self.order_to_price[passive_order.order_id]
            
            # Remove empty level
            if level.is_empty():
                del levels[price]
        
        # Update market data
        if trades:
            last_trade = trades[-1]
            self.last_trade_price = last_trade.price
            self.last_trade_time_ns = last_trade.executed_at_ns
            self.total_volume += sum(t.quantity for t in trades)
            self.trade_count += len(trades)
        
        self._update_best_prices()
        
        return trades
    
    def _add_to_book(self, order: Order):
        """Add order to appropriate price level"""
        if order.price is None:
            return
        
        levels = self.buy_levels if order.is_buy else self.sell_levels
        
        # Get or create price level
        if order.price not in levels:
            levels[order.price] = PriceLevel(order.price)
        
        level = levels[order.price]
        level.add_order(order)
        
        # Update lookup
        self.order_to_price[order.order_id] = order.price
        
        # Update best prices
        self._update_best_prices()
    
    def _create_trade(
        self,
        aggressive_order: Order,
        passive_order: Order,
        price: Decimal,
        quantity: Decimal
    ) -> Trade:
        """Create trade record"""
        self._trade_counter += 1
        
        # Determine buyer/seller
        if aggressive_order.is_buy:
            buyer_order = aggressive_order
            seller_order = passive_order
        else:
            buyer_order = passive_order
            seller_order = aggressive_order
        
        # Calculate fees (maker/taker model)
        taker_fee = quantity * Decimal("0.0005")  # 0.05%
        maker_fee = quantity * Decimal("0.0002")  # 0.02%
        
        # Aggressive order pays taker fee
        if aggressive_order.is_buy:
            buyer_fee = taker_fee
            seller_fee = maker_fee
        else:
            buyer_fee = maker_fee
            seller_fee = taker_fee
        
        return Trade(
            trade_id=f"{self.market_id}_{self._trade_counter}",
            market_id=self.market_id,
            price=price,
            quantity=quantity,
            buyer_order_id=buyer_order.order_id,
            seller_order_id=seller_order.order_id,
            buyer_id=buyer_order.trader_id,
            seller_id=seller_order.trader_id,
            buyer_fee=buyer_fee,
            seller_fee=seller_fee
        )
    
    def _update_best_prices(self):
        """Update best bid and ask prices"""
        self.best_bid = None
        self.best_ask = None
        
        if self.buy_levels:
            # Buy levels are sorted descending, so first is highest
            self.best_bid = next(iter(self.buy_levels))
        
        if self.sell_levels:
            # Sell levels are sorted ascending, so first is lowest
            self.best_ask = next(iter(self.sell_levels))
    
    def get_market_depth(self, depth: int = 10) -> Dict:
        """Get order book depth"""
        with self._lock:
            bids = []
            asks = []
            
            # Get bids
            for i, (price, level) in enumerate(self.buy_levels.items()):
                if i >= depth:
                    break
                bids.append({
                    "price": str(price),
                    "quantity": str(level.total_quantity),
                    "orders": len(level.orders)
                })
            
            # Get asks
            for i, (price, level) in enumerate(self.sell_levels.items()):
                if i >= depth:
                    break
                asks.append({
                    "price": str(price),
                    "quantity": str(level.total_quantity),
                    "orders": len(level.orders)
                })
            
            return {
                "market_id": self.market_id,
                "bids": bids,
                "asks": asks,
                "best_bid": str(self.best_bid) if self.best_bid else None,
                "best_ask": str(self.best_ask) if self.best_ask else None,
                "spread": str(self.best_ask - self.best_bid) if self.best_bid and self.best_ask else None,
                "last_price": str(self.last_trade_price) if self.last_trade_price else None,
                "timestamp": time.time_ns()
            }
    
    def get_stats(self) -> Dict:
        """Get order book statistics"""
        with self._lock:
            return {
                "market_id": self.market_id,
                "total_orders": len(self.orders),
                "buy_orders": sum(len(level.orders) for level in self.buy_levels.values()),
                "sell_orders": sum(len(level.orders) for level in self.sell_levels.values()),
                "total_volume": str(self.total_volume),
                "trade_count": self.trade_count,
                "spread_bps": self._calculate_spread_bps()
            }
    
    def _calculate_spread_bps(self) -> Optional[float]:
        """Calculate spread in basis points"""
        if not self.best_bid or not self.best_ask:
            return None
        
        mid_price = (self.best_bid + self.best_ask) / 2
        spread = self.best_ask - self.best_bid
        return float(spread / mid_price * 10000)  # basis points 