"""
Double Auction Clearing Engine

Implements sophisticated auction mechanisms for compute resource markets
"""

from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple, Set, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from scipy.optimize import linprog
import logging
from collections import defaultdict
import heapq

# Set high precision
getcontext().prec = 28

logger = logging.getLogger(__name__)


class AuctionType(Enum):
    """Types of auctions"""
    UNIFORM_PRICE = "uniform_price"  # All pay same clearing price
    PAY_AS_BID = "pay_as_bid"  # Pay your bid price
    VICKREY = "vickrey"  # Second-price sealed bid
    COMBINATORIAL = "combinatorial"  # Package bidding
    DOUBLE_SIDED = "double_sided"  # Both buyers and sellers


@dataclass
class Bid:
    """Bid in the auction"""
    bid_id: str
    participant_id: str
    resource_type: str
    quantity: Decimal
    price_per_unit: Decimal
    is_buy: bool  # True for buy, False for sell
    timestamp: datetime
    location: Optional[str] = None
    quality_tier: Optional[str] = None
    min_quantity: Optional[Decimal] = None  # All-or-nothing constraint
    max_quantity: Optional[Decimal] = None  # Flexible quantity
    bundle_id: Optional[str] = None  # For package bids
    priority: int = 0  # Higher priority filled first at same price
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def total_value(self) -> Decimal:
        return self.quantity * self.price_per_unit
        
    def __lt__(self, other):
        """For heap operations - buy orders by descending price, sell by ascending"""
        if self.is_buy:
            # Higher price is better for buy orders
            if self.price_per_unit != other.price_per_unit:
                return self.price_per_unit > other.price_per_unit
        else:
            # Lower price is better for sell orders
            if self.price_per_unit != other.price_per_unit:
                return self.price_per_unit < other.price_per_unit
                
        # Same price - use priority
        if self.priority != other.priority:
            return self.priority > other.priority
            
        # Same priority - use timestamp (earlier is better)
        return self.timestamp < other.timestamp


@dataclass
class ClearingResult:
    """Result of auction clearing"""
    clearing_price: Decimal
    total_volume: Decimal
    matched_bids: List[Tuple[Bid, Bid, Decimal]]  # (buy_bid, sell_bid, quantity)
    unmatched_bids: List[Bid]
    surplus: Decimal  # Economic surplus generated
    efficiency: Decimal  # Percentage of possible trades executed
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def num_trades(self) -> int:
        return len(self.matched_bids)
        
    @property
    def avg_trade_size(self) -> Decimal:
        if self.num_trades == 0:
            return Decimal("0")
        return self.total_volume / self.num_trades


@dataclass
class MarketDepth:
    """Market depth at various price levels"""
    price_levels: List[Decimal]
    buy_quantities: List[Decimal]
    sell_quantities: List[Decimal]
    
    def get_liquidity_at_price(self, price: Decimal) -> Tuple[Decimal, Decimal]:
        """Get buy/sell liquidity at or better than price"""
        buy_liquidity = sum(q for p, q in zip(self.price_levels, self.buy_quantities) 
                           if p >= price)
        sell_liquidity = sum(q for p, q in zip(self.price_levels, self.sell_quantities)
                            if p <= price)
        return buy_liquidity, sell_liquidity


class AuctionClearingEngine:
    """Advanced auction clearing engine for compute markets"""
    
    def __init__(self):
        self.clearing_history: List[ClearingResult] = []
        self.price_discovery_rounds = 3  # For iterative auctions
        
    def clear_auction(
        self,
        bids: List[Bid],
        auction_type: AuctionType = AuctionType.UNIFORM_PRICE,
        reserve_price: Optional[Decimal] = None,
        price_cap: Optional[Decimal] = None
    ) -> ClearingResult:
        """Clear auction with specified mechanism"""
        
        if auction_type == AuctionType.UNIFORM_PRICE:
            return self._clear_uniform_price(bids, reserve_price, price_cap)
        elif auction_type == AuctionType.PAY_AS_BID:
            return self._clear_pay_as_bid(bids, reserve_price, price_cap)
        elif auction_type == AuctionType.VICKREY:
            return self._clear_vickrey(bids)
        elif auction_type == AuctionType.COMBINATORIAL:
            return self._clear_combinatorial(bids)
        elif auction_type == AuctionType.DOUBLE_SIDED:
            return self._clear_double_sided(bids, reserve_price, price_cap)
        else:
            raise ValueError(f"Unknown auction type: {auction_type}")
            
    def _clear_uniform_price(
        self,
        bids: List[Bid],
        reserve_price: Optional[Decimal] = None,
        price_cap: Optional[Decimal] = None
    ) -> ClearingResult:
        """Clear uniform price auction (all pay same clearing price)"""
        
        # Separate buy and sell bids
        buy_bids = sorted([b for b in bids if b.is_buy], reverse=True)
        sell_bids = sorted([b for b in bids if not b.is_buy])
        
        if not buy_bids or not sell_bids:
            return ClearingResult(
                clearing_price=Decimal("0"),
                total_volume=Decimal("0"),
                matched_bids=[],
                unmatched_bids=bids,
                surplus=Decimal("0"),
                efficiency=Decimal("0")
            )
            
        # Build supply and demand curves
        demand_curve = self._build_curve(buy_bids, is_demand=True)
        supply_curve = self._build_curve(sell_bids, is_demand=False)
        
        # Find intersection point
        clearing_price, clearing_quantity = self._find_intersection(
            demand_curve, supply_curve, reserve_price, price_cap
        )
        
        if clearing_price == 0 or clearing_quantity == 0:
            return ClearingResult(
                clearing_price=Decimal("0"),
                total_volume=Decimal("0"),
                matched_bids=[],
                unmatched_bids=bids,
                surplus=Decimal("0"),
                efficiency=Decimal("0")
            )
            
        # Match bids at clearing price
        matched_bids = []
        unmatched_bids = []
        remaining_quantity = clearing_quantity
        
        # Process buy bids
        buy_quantity = Decimal("0")
        for bid in buy_bids:
            if bid.price_per_unit >= clearing_price and remaining_quantity > 0:
                match_quantity = min(bid.quantity, remaining_quantity)
                if self._check_quantity_constraints(bid, match_quantity):
                    matched_bids.append((bid, None, match_quantity))
                    buy_quantity += match_quantity
                    remaining_quantity -= match_quantity
                else:
                    unmatched_bids.append(bid)
            else:
                unmatched_bids.append(bid)
                
        # Process sell bids
        remaining_quantity = clearing_quantity
        sell_index = 0
        
        for i, (buy_bid, _, buy_quantity) in enumerate(matched_bids):
            while buy_quantity > 0 and sell_index < len(sell_bids):
                sell_bid = sell_bids[sell_index]
                
                if sell_bid.price_per_unit <= clearing_price:
                    match_quantity = min(sell_bid.quantity, buy_quantity)
                    
                    if self._check_quantity_constraints(sell_bid, match_quantity):
                        # Update the matched bid tuple
                        matched_bids[i] = (
                            matched_bids[i][0],  # buy_bid
                            sell_bid,
                            match_quantity
                        )
                        
                        buy_quantity -= match_quantity
                        sell_bid.quantity -= match_quantity
                        
                        if sell_bid.quantity == 0:
                            sell_index += 1
                    else:
                        unmatched_bids.append(sell_bid)
                        sell_index += 1
                else:
                    unmatched_bids.append(sell_bid)
                    sell_index += 1
                    
        # Add remaining sell bids
        unmatched_bids.extend(sell_bids[sell_index:])
        
        # Calculate surplus
        surplus = self._calculate_surplus(matched_bids, clearing_price)
        
        # Calculate efficiency
        max_possible_volume = min(
            sum(b.quantity for b in buy_bids),
            sum(b.quantity for b in sell_bids)
        )
        efficiency = clearing_quantity / max_possible_volume if max_possible_volume > 0 else Decimal("0")
        
        result = ClearingResult(
            clearing_price=clearing_price,
            total_volume=clearing_quantity,
            matched_bids=matched_bids,
            unmatched_bids=unmatched_bids,
            surplus=surplus,
            efficiency=efficiency,
            metadata={
                "num_buyers": len(buy_bids),
                "num_sellers": len(sell_bids),
                "demand_elasticity": self._calculate_elasticity(demand_curve, clearing_price),
                "supply_elasticity": self._calculate_elasticity(supply_curve, clearing_price)
            }
        )
        
        self.clearing_history.append(result)
        return result
        
    def _clear_pay_as_bid(
        self,
        bids: List[Bid],
        reserve_price: Optional[Decimal] = None,
        price_cap: Optional[Decimal] = None
    ) -> ClearingResult:
        """Clear pay-as-bid auction (discriminatory pricing)"""
        
        # Separate and sort bids
        buy_bids = sorted([b for b in bids if b.is_buy], key=lambda x: x.price_per_unit, reverse=True)
        sell_bids = sorted([b for b in bids if not b.is_buy], key=lambda x: x.price_per_unit)
        
        matched_bids = []
        unmatched_bids = []
        total_volume = Decimal("0")
        total_value = Decimal("0")
        
        buy_index = 0
        sell_index = 0
        
        while buy_index < len(buy_bids) and sell_index < len(sell_bids):
            buy_bid = buy_bids[buy_index]
            sell_bid = sell_bids[sell_index]
            
            # Check if trade is profitable
            if buy_bid.price_per_unit >= sell_bid.price_per_unit:
                # Apply price constraints
                trade_price = (buy_bid.price_per_unit + sell_bid.price_per_unit) / 2
                
                if reserve_price and trade_price < reserve_price:
                    unmatched_bids.append(sell_bid)
                    sell_index += 1
                    continue
                    
                if price_cap and trade_price > price_cap:
                    unmatched_bids.append(buy_bid)
                    buy_index += 1
                    continue
                    
                # Determine match quantity
                match_quantity = min(buy_bid.quantity, sell_bid.quantity)
                
                # Check quantity constraints
                if (self._check_quantity_constraints(buy_bid, match_quantity) and
                    self._check_quantity_constraints(sell_bid, match_quantity)):
                    
                    matched_bids.append((buy_bid, sell_bid, match_quantity))
                    total_volume += match_quantity
                    total_value += match_quantity * trade_price
                    
                    # Update remaining quantities
                    buy_bid.quantity -= match_quantity
                    sell_bid.quantity -= match_quantity
                    
                    # Move to next bid if exhausted
                    if buy_bid.quantity == 0:
                        buy_index += 1
                    if sell_bid.quantity == 0:
                        sell_index += 1
                else:
                    # Skip bids that don't meet constraints
                    if buy_bid.quantity < buy_bid.min_quantity:
                        unmatched_bids.append(buy_bid)
                        buy_index += 1
                    if sell_bid.quantity < sell_bid.min_quantity:
                        unmatched_bids.append(sell_bid)
                        sell_index += 1
            else:
                # No more profitable trades
                break
                
        # Add remaining unmatched bids
        unmatched_bids.extend(buy_bids[buy_index:])
        unmatched_bids.extend(sell_bids[sell_index:])
        
        # Calculate average clearing price
        avg_price = total_value / total_volume if total_volume > 0 else Decimal("0")
        
        # Calculate surplus (difference between buyer willingness and seller cost)
        surplus = sum(
            (buy.price_per_unit - sell.price_per_unit) * quantity
            for buy, sell, quantity in matched_bids
        )
        
        return ClearingResult(
            clearing_price=avg_price,
            total_volume=total_volume,
            matched_bids=matched_bids,
            unmatched_bids=unmatched_bids,
            surplus=surplus,
            efficiency=self._calculate_efficiency(bids, total_volume)
        )
        
    def _clear_vickrey(self, bids: List[Bid]) -> ClearingResult:
        """Clear Vickrey (second-price) auction"""
        
        # Simple implementation for single-unit Vickrey
        buy_bids = sorted([b for b in bids if b.is_buy and b.quantity == 1], 
                         key=lambda x: x.price_per_unit, reverse=True)
        sell_bids = sorted([b for b in bids if not b.is_buy and b.quantity == 1],
                          key=lambda x: x.price_per_unit)
        
        if not buy_bids or not sell_bids:
            return ClearingResult(
                clearing_price=Decimal("0"),
                total_volume=Decimal("0"),
                matched_bids=[],
                unmatched_bids=bids,
                surplus=Decimal("0"),
                efficiency=Decimal("0")
            )
            
        # Winner pays second-highest price
        winner_buy = buy_bids[0]
        winner_sell = sell_bids[0]
        
        if winner_buy.price_per_unit >= winner_sell.price_per_unit:
            # Determine Vickrey price
            second_buy_price = buy_bids[1].price_per_unit if len(buy_bids) > 1 else winner_buy.price_per_unit
            second_sell_price = sell_bids[1].price_per_unit if len(sell_bids) > 1 else winner_sell.price_per_unit
            
            vickrey_price = (second_buy_price + second_sell_price) / 2
            
            matched_bids = [(winner_buy, winner_sell, Decimal("1"))]
            unmatched_bids = [b for b in bids if b not in [winner_buy, winner_sell]]
            
            return ClearingResult(
                clearing_price=vickrey_price,
                total_volume=Decimal("1"),
                matched_bids=matched_bids,
                unmatched_bids=unmatched_bids,
                surplus=winner_buy.price_per_unit - winner_sell.price_per_unit,
                efficiency=Decimal("1")
            )
        else:
            return ClearingResult(
                clearing_price=Decimal("0"),
                total_volume=Decimal("0"),
                matched_bids=[],
                unmatched_bids=bids,
                surplus=Decimal("0"),
                efficiency=Decimal("0")
            )
            
    def _clear_combinatorial(self, bids: List[Bid]) -> ClearingResult:
        """Clear combinatorial auction with package bidding"""
        
        # Group bids by bundle
        bundles = defaultdict(list)
        single_bids = []
        
        for bid in bids:
            if bid.bundle_id:
                bundles[bid.bundle_id].append(bid)
            else:
                single_bids.append(bid)
                
        # This is a simplified implementation
        # Full combinatorial clearing requires solving an integer programming problem
        
        # For now, treat bundles as atomic and use greedy allocation
        bundle_values = []
        
        for bundle_id, bundle_bids in bundles.items():
            total_value = sum(b.total_value for b in bundle_bids)
            total_quantity = sum(b.quantity for b in bundle_bids)
            avg_price = total_value / total_quantity if total_quantity > 0 else Decimal("0")
            
            bundle_values.append({
                "bundle_id": bundle_id,
                "bids": bundle_bids,
                "total_value": total_value,
                "avg_price": avg_price,
                "is_buy": bundle_bids[0].is_buy
            })
            
        # Sort bundles by average price
        bundle_values.sort(key=lambda x: x["avg_price"], reverse=True)
        
        # Match bundles using greedy approach
        matched_bundles = []
        unmatched_bids = []
        
        buy_bundles = [b for b in bundle_values if b["is_buy"]]
        sell_bundles = [b for b in bundle_values if not b["is_buy"]]
        
        for buy_bundle in buy_bundles:
            best_sell = None
            best_surplus = Decimal("0")
            
            for sell_bundle in sell_bundles:
                if sell_bundle not in matched_bundles:
                    surplus = buy_bundle["avg_price"] - sell_bundle["avg_price"]
                    if surplus > best_surplus:
                        best_surplus = surplus
                        best_sell = sell_bundle
                        
            if best_sell:
                matched_bundles.append((buy_bundle, best_sell))
                sell_bundles.remove(best_sell)
                
        # Convert back to bid matching
        matched_bids = []
        total_volume = Decimal("0")
        
        for buy_bundle, sell_bundle in matched_bundles:
            # Simple matching within bundles
            for buy_bid in buy_bundle["bids"]:
                for sell_bid in sell_bundle["bids"]:
                    if buy_bid.resource_type == sell_bid.resource_type:
                        match_quantity = min(buy_bid.quantity, sell_bid.quantity)
                        matched_bids.append((buy_bid, sell_bid, match_quantity))
                        total_volume += match_quantity
                        
        # Add unmatched bundles
        for bundle in bundle_values:
            if bundle not in [b for pair in matched_bundles for b in pair]:
                unmatched_bids.extend(bundle["bids"])
                
        unmatched_bids.extend(single_bids)  # Single bids not handled in this simple version
        
        # Calculate clearing price as volume-weighted average
        total_value = sum(
            quantity * (buy.price_per_unit + sell.price_per_unit) / 2
            for buy, sell, quantity in matched_bids
        )
        
        clearing_price = total_value / total_volume if total_volume > 0 else Decimal("0")
        
        return ClearingResult(
            clearing_price=clearing_price,
            total_volume=total_volume,
            matched_bids=matched_bids,
            unmatched_bids=unmatched_bids,
            surplus=sum(best_surplus for _, best_surplus in matched_bundles),
            efficiency=self._calculate_efficiency(bids, total_volume)
        )
        
    def _clear_double_sided(
        self,
        bids: List[Bid],
        reserve_price: Optional[Decimal] = None,
        price_cap: Optional[Decimal] = None
    ) -> ClearingResult:
        """Clear double-sided auction with advanced features"""
        
        # Use network flow optimization for complex constraints
        # This is a simplified version - production would use more sophisticated algorithms
        
        # Build bipartite graph
        buy_bids = [b for b in bids if b.is_buy]
        sell_bids = [b for b in bids if not b.is_buy]
        
        # Create compatibility matrix
        n_buyers = len(buy_bids)
        n_sellers = len(sell_bids)
        
        compatibility = np.zeros((n_buyers, n_sellers))
        trade_surplus = np.zeros((n_buyers, n_sellers))
        
        for i, buy_bid in enumerate(buy_bids):
            for j, sell_bid in enumerate(sell_bids):
                # Check compatibility
                if (buy_bid.resource_type == sell_bid.resource_type and
                    (not buy_bid.location or not sell_bid.location or 
                     buy_bid.location == sell_bid.location) and
                    (not buy_bid.quality_tier or not sell_bid.quality_tier or
                     buy_bid.quality_tier == sell_bid.quality_tier)):
                    
                    if buy_bid.price_per_unit >= sell_bid.price_per_unit:
                        compatibility[i, j] = 1
                        trade_surplus[i, j] = float(buy_bid.price_per_unit - sell_bid.price_per_unit)
                        
        # Solve assignment problem to maximize surplus
        # Using greedy approach for simplicity
        matched_bids = []
        used_buyers = set()
        used_sellers = set()
        
        # Sort by surplus
        trades = []
        for i in range(n_buyers):
            for j in range(n_sellers):
                if compatibility[i, j] == 1:
                    trades.append((trade_surplus[i, j], i, j))
                    
        trades.sort(reverse=True)
        
        # Greedy matching
        for surplus, i, j in trades:
            if i not in used_buyers and j not in used_sellers:
                buy_bid = buy_bids[i]
                sell_bid = sell_bids[j]
                
                match_quantity = min(buy_bid.quantity, sell_bid.quantity)
                
                if (self._check_quantity_constraints(buy_bid, match_quantity) and
                    self._check_quantity_constraints(sell_bid, match_quantity)):
                    
                    matched_bids.append((buy_bid, sell_bid, match_quantity))
                    
                    if match_quantity == buy_bid.quantity:
                        used_buyers.add(i)
                    if match_quantity == sell_bid.quantity:
                        used_sellers.add(j)
                        
        # Calculate results
        total_volume = sum(q for _, _, q in matched_bids)
        total_value = sum(
            q * (buy.price_per_unit + sell.price_per_unit) / 2
            for buy, sell, q in matched_bids
        )
        
        clearing_price = total_value / total_volume if total_volume > 0 else Decimal("0")
        
        # Identify unmatched bids
        matched_buy_bids = {buy for buy, _, _ in matched_bids}
        matched_sell_bids = {sell for _, sell, _ in matched_bids}
        
        unmatched_bids = []
        unmatched_bids.extend([b for b in buy_bids if b not in matched_buy_bids])
        unmatched_bids.extend([b for b in sell_bids if b not in matched_sell_bids])
        
        return ClearingResult(
            clearing_price=clearing_price,
            total_volume=total_volume,
            matched_bids=matched_bids,
            unmatched_bids=unmatched_bids,
            surplus=sum(
                (buy.price_per_unit - sell.price_per_unit) * q
                for buy, sell, q in matched_bids
            ),
            efficiency=self._calculate_efficiency(bids, total_volume),
            metadata={
                "network_density": np.sum(compatibility) / (n_buyers * n_sellers),
                "avg_match_surplus": np.mean(trade_surplus[compatibility == 1])
            }
        )
        
    def _build_curve(self, bids: List[Bid], is_demand: bool) -> List[Tuple[Decimal, Decimal]]:
        """Build supply or demand curve from bids"""
        curve = []
        cumulative_quantity = Decimal("0")
        
        for bid in bids:
            cumulative_quantity += bid.quantity
            curve.append((bid.price_per_unit, cumulative_quantity))
            
        return curve
        
    def _find_intersection(
        self,
        demand_curve: List[Tuple[Decimal, Decimal]],
        supply_curve: List[Tuple[Decimal, Decimal]],
        reserve_price: Optional[Decimal] = None,
        price_cap: Optional[Decimal] = None
    ) -> Tuple[Decimal, Decimal]:
        """Find intersection of supply and demand curves"""
        
        # Simple linear interpolation method
        for i in range(len(demand_curve)):
            demand_price, demand_quantity = demand_curve[i]
            
            # Apply price constraints
            if reserve_price and demand_price < reserve_price:
                continue
            if price_cap and demand_price > price_cap:
                continue
                
            # Find corresponding supply quantity
            supply_quantity = Decimal("0")
            
            for supply_price, cumulative_supply in supply_curve:
                if supply_price <= demand_price:
                    supply_quantity = cumulative_supply
                else:
                    break
                    
            # Check if we have a match
            if supply_quantity >= demand_quantity:
                return demand_price, demand_quantity
            elif i > 0 and supply_quantity > 0:
                # Interpolate between price levels
                prev_price, prev_quantity = demand_curve[i-1]
                
                # Linear interpolation
                price_diff = demand_price - prev_price
                quantity_diff = demand_quantity - prev_quantity
                
                if quantity_diff != 0:
                    interpolated_price = prev_price + (supply_quantity - prev_quantity) * price_diff / quantity_diff
                    return interpolated_price, supply_quantity
                    
        return Decimal("0"), Decimal("0")
        
    def _check_quantity_constraints(self, bid: Bid, match_quantity: Decimal) -> bool:
        """Check if match quantity satisfies bid constraints"""
        if bid.min_quantity and match_quantity < bid.min_quantity:
            return False
        if bid.max_quantity and match_quantity > bid.max_quantity:
            return False
        return True
        
    def _calculate_surplus(
        self,
        matched_bids: List[Tuple[Bid, Optional[Bid], Decimal]],
        clearing_price: Decimal
    ) -> Decimal:
        """Calculate economic surplus from matches"""
        surplus = Decimal("0")
        
        for buy_bid, sell_bid, quantity in matched_bids:
            # Consumer surplus
            surplus += (buy_bid.price_per_unit - clearing_price) * quantity
            
            # Producer surplus
            if sell_bid:
                surplus += (clearing_price - sell_bid.price_per_unit) * quantity
                
        return surplus
        
    def _calculate_efficiency(self, all_bids: List[Bid], matched_volume: Decimal) -> Decimal:
        """Calculate market efficiency"""
        buy_bids = [b for b in all_bids if b.is_buy]
        sell_bids = [b for b in all_bids if not b.is_buy]
        
        max_possible_volume = min(
            sum(b.quantity for b in buy_bids),
            sum(b.quantity for b in sell_bids)
        )
        
        if max_possible_volume == 0:
            return Decimal("0")
            
        return matched_volume / max_possible_volume
        
    def _calculate_elasticity(
        self,
        curve: List[Tuple[Decimal, Decimal]],
        price: Decimal
    ) -> Decimal:
        """Calculate price elasticity at given price point"""
        
        if len(curve) < 2:
            return Decimal("0")
            
        # Find nearest points
        for i in range(1, len(curve)):
            if curve[i][0] <= price <= curve[i-1][0]:
                p1, q1 = curve[i-1]
                p2, q2 = curve[i]
                
                if p1 == p2 or q1 == q2:
                    return Decimal("0")
                    
                # Point elasticity formula
                dq_dp = (q2 - q1) / (p2 - p1)
                elasticity = (dq_dp * price) / ((q1 + q2) / 2)
                
                return abs(elasticity)
                
        return Decimal("0")
        
    def get_market_depth(self, bids: List[Bid]) -> MarketDepth:
        """Calculate market depth from bids"""
        
        # Aggregate by price level
        buy_depth = defaultdict(Decimal)
        sell_depth = defaultdict(Decimal)
        
        for bid in bids:
            if bid.is_buy:
                buy_depth[bid.price_per_unit] += bid.quantity
            else:
                sell_depth[bid.price_per_unit] += bid.quantity
                
        # Create sorted price levels
        all_prices = sorted(set(buy_depth.keys()) | set(sell_depth.keys()))
        
        return MarketDepth(
            price_levels=all_prices,
            buy_quantities=[buy_depth.get(p, Decimal("0")) for p in all_prices],
            sell_quantities=[sell_depth.get(p, Decimal("0")) for p in all_prices]
        )
        
    def simulate_order_impact(
        self,
        current_bids: List[Bid],
        new_bid: Bid,
        auction_type: AuctionType = AuctionType.UNIFORM_PRICE
    ) -> Dict[str, Any]:
        """Simulate impact of new order on market"""
        
        # Clear without new bid
        result_without = self.clear_auction(current_bids, auction_type)
        
        # Clear with new bid
        result_with = self.clear_auction(current_bids + [new_bid], auction_type)
        
        price_impact = result_with.clearing_price - result_without.clearing_price
        volume_impact = result_with.total_volume - result_without.total_volume
        
        return {
            "price_impact": price_impact,
            "volume_impact": volume_impact,
            "price_impact_pct": (price_impact / result_without.clearing_price * 100 
                                if result_without.clearing_price > 0 else Decimal("0")),
            "execution_probability": self._estimate_execution_probability(new_bid, result_with),
            "expected_surplus": self._estimate_bid_surplus(new_bid, result_with)
        }
        
    def _estimate_execution_probability(self, bid: Bid, clearing_result: ClearingResult) -> Decimal:
        """Estimate probability of bid execution"""
        
        if bid.is_buy:
            if bid.price_per_unit > clearing_result.clearing_price:
                return Decimal("1.0")
            elif bid.price_per_unit == clearing_result.clearing_price:
                return Decimal("0.5")  # Simplified
            else:
                return Decimal("0.0")
        else:
            if bid.price_per_unit < clearing_result.clearing_price:
                return Decimal("1.0")
            elif bid.price_per_unit == clearing_result.clearing_price:
                return Decimal("0.5")
            else:
                return Decimal("0.0")
                
    def _estimate_bid_surplus(self, bid: Bid, clearing_result: ClearingResult) -> Decimal:
        """Estimate surplus for a bid"""
        
        if bid.is_buy:
            if bid.price_per_unit >= clearing_result.clearing_price:
                return (bid.price_per_unit - clearing_result.clearing_price) * bid.quantity
        else:
            if bid.price_per_unit <= clearing_result.clearing_price:
                return (clearing_result.clearing_price - bid.price_per_unit) * bid.quantity
                
        return Decimal("0") 