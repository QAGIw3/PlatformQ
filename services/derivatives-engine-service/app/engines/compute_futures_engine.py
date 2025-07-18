"""
Compute Futures Engine

Implements electricity market-style mechanisms for compute resources.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from collections import defaultdict
import logging

from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient
from app.models.market import Market, MarketType

logger = logging.getLogger(__name__)


@dataclass
class ComputeBid:
    """Bid for compute resources"""
    bid_id: str
    user_id: str
    hour: int
    resource_type: str
    quantity: Decimal
    max_price: Decimal
    flexible: bool = False
    submitted_at: datetime = field(default_factory=datetime.utcnow)
    

@dataclass
class ComputeOffer:
    """Offer to provide compute resources"""
    offer_id: str
    provider_id: str
    hour: int
    resource_type: str
    quantity: Decimal
    min_price: Decimal
    ramp_rate: Decimal  # How fast can scale up/down
    location_zone: str
    submitted_at: datetime = field(default_factory=datetime.utcnow)


class DayAheadMarket:
    """
    Day-ahead market for compute resources (similar to electricity DAM)
    """
    
    def __init__(self, delivery_date: datetime, resource_type: str):
        self.delivery_date = delivery_date
        self.resource_type = resource_type
        self.bids: Dict[int, List[ComputeBid]] = defaultdict(list)  # hour -> bids
        self.offers: Dict[int, List[ComputeOffer]] = defaultdict(list)  # hour -> offers
        self.clearing_prices: Dict[int, Decimal] = {}
        self.cleared_quantities: Dict[int, Decimal] = {}
        self.is_cleared = False
        
    async def submit_bid(
        self,
        user_id: str,
        hour: int,
        quantity: Decimal,
        max_price: Decimal,
        flexible: bool = False
    ) -> Dict:
        """Submit a bid for compute resources"""
        if self.is_cleared:
            raise ValueError("Market already cleared")
            
        bid = ComputeBid(
            bid_id=f"bid_{user_id}_{hour}_{datetime.utcnow().timestamp()}",
            user_id=user_id,
            hour=hour,
            resource_type=self.resource_type,
            quantity=quantity,
            max_price=max_price,
            flexible=flexible
        )
        
        self.bids[hour].append(bid)
        
        # Estimate clearing price
        estimated_price = await self._estimate_clearing_price(hour)
        
        return {
            "bid_id": bid.bid_id,
            "estimated_price": estimated_price,
            "delivery_window": {
                "start": self.delivery_date.replace(hour=hour),
                "end": self.delivery_date.replace(hour=hour+1)
            }
        }
        
    async def submit_offer(
        self,
        provider_id: str,
        hour: int,
        quantity: Decimal,
        min_price: Decimal,
        ramp_rate: Decimal,
        location_zone: str
    ) -> Dict:
        """Submit an offer to provide compute resources"""
        if self.is_cleared:
            raise ValueError("Market already cleared")
            
        offer = ComputeOffer(
            offer_id=f"offer_{provider_id}_{hour}_{datetime.utcnow().timestamp()}",
            provider_id=provider_id,
            hour=hour,
            resource_type=self.resource_type,
            quantity=quantity,
            min_price=min_price,
            ramp_rate=ramp_rate,
            location_zone=location_zone
        )
        
        self.offers[hour].append(offer)
        
        return {"offer_id": offer.offer_id}
        
    async def clear_market(self) -> Dict:
        """
        Clear the market using merit order dispatch
        """
        results = {}
        
        for hour in range(24):
            hour_bids = sorted(self.bids[hour], key=lambda x: x.max_price, reverse=True)
            hour_offers = sorted(self.offers[hour], key=lambda x: x.min_price)
            
            # Find market clearing point
            cleared_quantity = Decimal("0")
            clearing_price = Decimal("0")
            
            bid_curve = self._create_demand_curve(hour_bids)
            offer_curve = self._create_supply_curve(hour_offers)
            
            # Find intersection
            clearing_price, cleared_quantity = self._find_intersection(bid_curve, offer_curve)
            
            self.clearing_prices[hour] = clearing_price
            self.cleared_quantities[hour] = cleared_quantity
            
            # Handle flexible bids if needed
            if cleared_quantity < sum(b.quantity for b in hour_bids):
                await self._handle_flexible_bids(hour)
                
            results[hour] = {
                "clearing_price": clearing_price,
                "cleared_quantity": cleared_quantity,
                "accepted_bids": len([b for b in hour_bids if b.max_price >= clearing_price]),
                "accepted_offers": len([o for o in hour_offers if o.min_price <= clearing_price])
            }
            
        self.is_cleared = True
        return results
        
    def _create_demand_curve(self, bids: List[ComputeBid]) -> List[Tuple[Decimal, Decimal]]:
        """Create demand curve from bids"""
        curve = []
        cumulative_quantity = Decimal("0")
        
        for bid in bids:
            curve.append((bid.max_price, cumulative_quantity))
            cumulative_quantity += bid.quantity
            curve.append((bid.max_price, cumulative_quantity))
            
        return curve
        
    def _create_supply_curve(self, offers: List[ComputeOffer]) -> List[Tuple[Decimal, Decimal]]:
        """Create supply curve from offers"""
        curve = []
        cumulative_quantity = Decimal("0")
        
        for offer in offers:
            curve.append((offer.min_price, cumulative_quantity))
            cumulative_quantity += offer.quantity
            curve.append((offer.min_price, cumulative_quantity))
            
        return curve
        
    def _find_intersection(
        self,
        demand: List[Tuple[Decimal, Decimal]],
        supply: List[Tuple[Decimal, Decimal]]
    ) -> Tuple[Decimal, Decimal]:
        """Find intersection of supply and demand curves"""
        # Simplified - in practice would use more sophisticated algorithm
        for i in range(len(demand) - 1):
            for j in range(len(supply) - 1):
                if demand[i][1] >= supply[j][1] and demand[i+1][1] <= supply[j+1][1]:
                    # Found intersection
                    price = (demand[i][0] + supply[j][0]) / 2
                    quantity = min(demand[i][1], supply[j][1])
                    return price, quantity
                    
        return Decimal("0"), Decimal("0")
        
    async def _estimate_clearing_price(self, hour: int) -> Decimal:
        """Estimate clearing price based on current bids/offers"""
        if not self.bids[hour] or not self.offers[hour]:
            return Decimal("10")  # Default price
            
        avg_bid = sum(b.max_price for b in self.bids[hour]) / len(self.bids[hour])
        avg_offer = sum(o.min_price for o in self.offers[hour]) / len(self.offers[hour])
        
        return (avg_bid + avg_offer) / 2
        
    async def _handle_flexible_bids(self, hour: int):
        """Handle flexible bids that can shift hours"""
        flexible_bids = [b for b in self.bids[hour] if b.flexible]
        
        for bid in flexible_bids:
            # Try adjacent hours
            for offset in [-2, -1, 1, 2]:
                target_hour = hour + offset
                if 0 <= target_hour < 24:
                    if self.clearing_prices.get(target_hour, Decimal("999")) <= bid.max_price:
                        # Move bid to target hour
                        self.bids[hour].remove(bid)
                        bid.hour = target_hour
                        self.bids[target_hour].append(bid)
                        break


class CapacityAuction:
    """
    Long-term capacity procurement auction (similar to PJM capacity market)
    """
    
    def __init__(self):
        self.capacity_offers: Dict[int, List[Dict]] = defaultdict(list)  # year -> offers
        self.capacity_requirements: Dict[int, Dict[str, Decimal]] = {}
        
    async def submit_offer(
        self,
        provider_id: str,
        capacity_mw: Decimal,
        delivery_year: int,
        resource_type: str,
        minimum_price: Optional[Decimal] = None
    ) -> Dict:
        """Submit capacity commitment offer"""
        offer = {
            "id": f"cap_{provider_id}_{delivery_year}_{datetime.utcnow().timestamp()}",
            "provider_id": provider_id,
            "capacity_mw": capacity_mw,
            "resource_type": resource_type,
            "minimum_price": minimum_price or Decimal("0"),
            "delivery_year": delivery_year,
            "submitted_at": datetime.utcnow()
        }
        
        self.capacity_offers[delivery_year].append(offer)
        
        # Estimate clearing price
        vrr_curve = await self._get_vrr_curve(delivery_year)
        estimated_price = self._estimate_capacity_price(capacity_mw, vrr_curve)
        
        return {
            "id": offer["id"],
            "auction_date": datetime.utcnow() + timedelta(days=30),
            "price_estimate": estimated_price
        }
        
    async def run_auction(self, delivery_year: int) -> Dict:
        """Run capacity auction for delivery year"""
        # Get capacity requirement
        requirement = await self._calculate_capacity_requirement(delivery_year)
        
        # Create VRR curve
        vrr_curve = await self._get_vrr_curve(delivery_year)
        
        # Sort offers by price
        offers = sorted(
            self.capacity_offers[delivery_year],
            key=lambda x: x["minimum_price"]
        )
        
        # Clear auction
        cleared_offers = []
        total_cleared = Decimal("0")
        clearing_price = Decimal("0")
        
        for offer in offers:
            if total_cleared < requirement["total"]:
                cleared_offers.append(offer)
                total_cleared += offer["capacity_mw"]
                clearing_price = self._get_vrr_price(total_cleared, vrr_curve)
                
        return {
            "clearing_price": clearing_price,
            "total_cleared_mw": total_cleared,
            "cleared_offers": cleared_offers,
            "requirement": requirement
        }
        
    async def _calculate_capacity_requirement(self, year: int) -> Dict:
        """Calculate capacity requirement for year"""
        # Forecast peak demand
        peak_forecast = await self._forecast_peak_demand(year)
        
        # Add reserve margin
        reserve_margin = Decimal("0.15")  # 15%
        total_requirement = peak_forecast * (1 + reserve_margin)
        
        return {
            "total": total_requirement,
            "peak_forecast": peak_forecast,
            "reserve_margin": reserve_margin
        }
        
    async def _get_vrr_curve(self, year: int) -> List[Tuple[Decimal, Decimal]]:
        """Get Variable Resource Requirement curve"""
        requirement = await self._calculate_capacity_requirement(year)
        base = requirement["total"]
        
        # Create VRR curve points
        curve = [
            (base * Decimal("0.8"), Decimal("150")),   # High price at low capacity
            (base * Decimal("0.9"), Decimal("100")),
            (base, Decimal("75")),                      # Target price at requirement
            (base * Decimal("1.1"), Decimal("50")),
            (base * Decimal("1.2"), Decimal("0"))       # Zero price at excess
        ]
        
        return curve
        
    def _get_vrr_price(self, quantity: Decimal, vrr_curve: List[Tuple[Decimal, Decimal]]) -> Decimal:
        """Get price from VRR curve for given quantity"""
        # Linear interpolation between points
        for i in range(len(vrr_curve) - 1):
            if vrr_curve[i][0] <= quantity <= vrr_curve[i+1][0]:
                # Interpolate
                x1, y1 = vrr_curve[i]
                x2, y2 = vrr_curve[i+1]
                price = y1 + (y2 - y1) * (quantity - x1) / (x2 - x1)
                return price
                
        return Decimal("0")
        
    async def _forecast_peak_demand(self, year: int) -> Decimal:
        """Forecast peak compute demand for year"""
        # Simplified - would use ML models in practice
        current_year = datetime.utcnow().year
        years_ahead = year - current_year
        
        # Assume 20% annual growth
        growth_rate = Decimal("0.20")
        current_peak = Decimal("10000")  # MW equivalent
        
        return current_peak * (1 + growth_rate) ** years_ahead
        
    def _estimate_capacity_price(self, capacity: Decimal, vrr_curve: List[Tuple[Decimal, Decimal]]) -> Decimal:
        """Estimate clearing price based on current offers"""
        # Simplified estimation
        total_offered = sum(o["capacity_mw"] for offers in self.capacity_offers.values() for o in offers)
        return self._get_vrr_price(total_offered + capacity, vrr_curve)


class AncillaryServices:
    """
    Ancillary services for compute grid stability
    """
    
    def __init__(self):
        self.service_providers: Dict[str, List[Dict]] = defaultdict(list)
        self.service_requirements = {
            "latency_regulation": {
                "response_time_ms": 100,
                "accuracy": 0.95,
                "min_capacity": Decimal("10")
            },
            "burst_capacity": {
                "activation_time_s": 5,
                "duration_min": 30,
                "min_capacity": Decimal("50")
            },
            "failover_reserve": {
                "activation_time_s": 60,
                "reliability": 0.999,
                "min_capacity": Decimal("100")
            }
        }
        
    async def register_provider(
        self,
        provider_id: str,
        service_type: str,
        capacity: Decimal,
        response_time_ms: int,
        duration_hours: int
    ) -> Dict:
        """Register as ancillary service provider"""
        # Verify qualifications
        qualified = await self._verify_qualifications(
            service_type,
            capacity,
            response_time_ms
        )
        
        if not qualified:
            return {
                "id": None,
                "qualified": False,
                "reason": "Does not meet service requirements"
            }
            
        registration = {
            "id": f"anc_{provider_id}_{service_type}_{datetime.utcnow().timestamp()}",
            "provider_id": provider_id,
            "service_type": service_type,
            "capacity": capacity,
            "response_time_ms": response_time_ms,
            "duration_hours": duration_hours,
            "registered_at": datetime.utcnow()
        }
        
        self.service_providers[service_type].append(registration)
        
        # Calculate compensation
        compensation = await self._calculate_compensation(
            service_type,
            capacity,
            duration_hours
        )
        
        return {
            "id": registration["id"],
            "qualified": True,
            "compensation_estimate": compensation,
            "requirements": self.service_requirements[service_type]
        }
        
    async def _verify_qualifications(
        self,
        service_type: str,
        capacity: Decimal,
        response_time_ms: int
    ) -> bool:
        """Verify provider meets service requirements"""
        reqs = self.service_requirements.get(service_type, {})
        
        if capacity < reqs.get("min_capacity", Decimal("0")):
            return False
            
        if response_time_ms > reqs.get("response_time_ms", float('inf')):
            return False
            
        return True
        
    async def _calculate_compensation(
        self,
        service_type: str,
        capacity: Decimal,
        duration_hours: int
    ) -> Decimal:
        """Calculate compensation for ancillary service"""
        # Base rates per MW per hour
        base_rates = {
            "latency_regulation": Decimal("5"),
            "burst_capacity": Decimal("3"),
            "failover_reserve": Decimal("2")
        }
        
        rate = base_rates.get(service_type, Decimal("1"))
        total = rate * capacity * duration_hours
        
        # Add performance multiplier
        performance_multiplier = Decimal("1.2")  # 20% bonus for high performance
        
        return total * performance_multiplier


class ComputeFuturesEngine:
    """
    Main engine for compute futures markets
    """
    
    def __init__(self):
        self.day_ahead_markets: Dict[str, DayAheadMarket] = {}
        self.capacity_auction = CapacityAuction()
        self.ancillary_services = AncillaryServices()
        self.imbalance_tracker: Dict[str, Dict] = defaultdict(dict)
        
    async def get_day_ahead_market(
        self,
        delivery_date: datetime,
        resource_type: str
    ) -> DayAheadMarket:
        """Get or create day-ahead market"""
        key = f"{delivery_date.date()}_{resource_type}"
        
        if key not in self.day_ahead_markets:
            self.day_ahead_markets[key] = DayAheadMarket(delivery_date, resource_type)
            
        return self.day_ahead_markets[key]
        
    async def get_clearing_results(
        self,
        delivery_date: datetime,
        resource_type: str
    ) -> Dict:
        """Get market clearing results"""
        market = await self.get_day_ahead_market(delivery_date, resource_type)
        
        if not market.is_cleared:
            await market.clear_market()
            
        return {
            "hourly_prices": market.clearing_prices,
            "total_volume": sum(market.cleared_quantities.values()),
            "curves": {
                "supply": "TODO",  # Would include actual curves
                "demand": "TODO"
            },
            "congestion": await self._analyze_congestion(market)
        }
        
    async def get_current_imbalance(self, resource_type: str) -> Dict:
        """Get real-time imbalance for resource type"""
        # In practice, would track actual vs scheduled
        current_hour = datetime.utcnow().hour
        
        # Simulated imbalance
        scheduled = Decimal("1000")
        actual = Decimal("950")
        imbalance = actual - scheduled
        
        # Calculate real-time price
        da_price = Decimal("50")  # Day-ahead price
        
        if imbalance < 0:  # Shortage
            rt_price = da_price * Decimal("1.5")  # 50% premium
        else:  # Surplus
            rt_price = da_price * Decimal("0.7")  # 30% discount
            
        return {
            "quantity": abs(imbalance),
            "direction": "shortage" if imbalance < 0 else "surplus",
            "price": rt_price,
            "da_price": da_price,
            "timestamp": datetime.utcnow()
        }
        
    async def create_futures_contract(
        self,
        creator_id: str,
        resource_type: str,
        quantity: Decimal,
        delivery_start: datetime,
        duration_hours: int,
        contract_months: int,
        location_zone: Optional[str] = None
    ) -> Dict:
        """Create standardized futures contract"""
        # Generate contract specifications
        contract = {
            "id": f"CF_{resource_type}_{delivery_start.strftime('%Y%m')}_{datetime.utcnow().timestamp()}",
            "symbol": f"{resource_type[:3].upper()}-{delivery_start.strftime('%b%y').upper()}",
            "specs": {
                "resource_type": resource_type,
                "quantity_per_contract": quantity,
                "delivery_start": delivery_start.isoformat(),
                "duration_hours": duration_hours,
                "location_zone": location_zone or "ANY",
                "settlement": "physical",
                "currency": "USD"
            },
            "margin_requirement": quantity * Decimal("10"),  # $10 per unit initial margin
            "tick_size": Decimal("0.01"),  # $0.01 minimum price movement
            "contract_months": contract_months,
            "created_by": creator_id,
            "created_at": datetime.utcnow()
        }
        
        # Register contract
        # In practice, would save to database and create market
        
        return contract
        
    async def _analyze_congestion(self, market: DayAheadMarket) -> Dict:
        """Analyze congestion in different zones"""
        # Simplified - would analyze by location in practice
        congestion_zones = {}
        
        for hour in range(24):
            if market.cleared_quantities.get(hour, Decimal("0")) > Decimal("800"):
                congestion_zones[hour] = {
                    "severity": "high",
                    "affected_zones": ["zone_1", "zone_2"],
                    "price_separation": Decimal("15")
                }
                
        return congestion_zones 