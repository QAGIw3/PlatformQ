"""
Compute Futures Engine

Implements electricity market-style mechanisms for compute resources using trading-core-service.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any, Set
from decimal import Decimal
from datetime import datetime, timedelta, date
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from collections import defaultdict
import logging
import httpx

from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient
from app.integrations.trading_core_integration import TradingCoreIntegration
from app.models.market import Market, MarketType

logger = logging.getLogger(__name__)


@dataclass
class MarketClearingResult:
    """Result of market clearing process"""
    clearing_price: Decimal
    total_quantity_cleared: Decimal
    matched_bids: List['ComputeBid']
    matched_offers: List['ComputeOffer']
    clearing_time: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ComputeBid:
    """Bid for compute resources in day-ahead market"""
    bid_id: str
    user_id: str
    resource_type: str  # GPU, CPU, etc.
    quantity: Decimal
    price: Decimal  # Price per unit
    delivery_hour: int  # 0-23
    location: str
    flexibility: str = "fixed"  # fixed, flexible, interruptible
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ComputeOffer:
    """Offer to provide compute resources"""
    offer_id: str
    provider_id: str
    resource_type: str
    quantity: Decimal
    price: Decimal
    delivery_hour: int
    location: str
    reliability_score: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SLARequirement:
    """SLA requirements for compute resources"""
    min_uptime_percent: Decimal = Decimal("99.9")  # 99.9% uptime
    max_latency_ms: Optional[int] = None  # Latency requirement
    min_performance_score: Decimal = Decimal("0.95")  # 95% of advertised performance
    penalty_rate: Decimal = Decimal("0.1")  # 10% penalty per 1% SLA breach


@dataclass 
class ComputeSettlement:
    """Physical settlement record"""
    settlement_id: str
    contract_id: str
    trade_id: str
    buyer_id: str
    seller_id: Optional[str] = None
    resource_type: str
    quantity: Decimal
    delivery_date: datetime
    status: str  # "pending", "provisioned", "failed", "completed"
    sla_violations: List[Dict] = field(default_factory=list)
    failover_used: bool = False
    failover_provider: Optional[str] = None
    settlement_amount: Decimal = Decimal("0")
    penalty_amount: Decimal = Decimal("0")


class ComputeQualityType(Enum):
    """Types of compute quality derivatives"""
    LATENCY_FUTURE = "latency_future"
    UPTIME_SWAP = "uptime_swap"
    PERFORMANCE_BOND = "performance_bond"
    BANDWIDTH_OPTION = "bandwidth_option"
    

@dataclass
class LatencyFuture:
    """Future contract on network latency"""
    contract_id: str
    buyer_id: str
    seller_id: str
    region_pair: Tuple[str, str]  # (source_region, dest_region)
    strike_latency_ms: int
    notional: Decimal
    expiry: datetime
    measurement_frequency: str = field(default="hourly")  # How often to measure
    
    
@dataclass
class UptimeSwap:
    """Swap contract on service uptime"""
    swap_id: str
    buyer_id: str  # Pays fixed, receives floating
    seller_id: str  # Pays floating, receives fixed
    service_id: str
    fixed_uptime_rate: Decimal  # e.g., 99.9%
    notional_per_hour: Decimal  # Payout per hour of downtime
    start_date: datetime
    end_date: datetime
    measurement_period: str = field(default="daily")


@dataclass
class PerformanceBond:
    """Bond that pays based on compute performance"""
    bond_id: str
    issuer_id: str  # Compute provider
    buyer_id: str
    hardware_spec: Dict  # GPU model, CPU specs, etc.
    guaranteed_performance: Decimal  # e.g., 95% of theoretical max
    bond_amount: Decimal
    expiry: datetime
    test_frequency: str = field(default="hourly")


class DayAheadMarket:
    """
    Day-ahead market for compute resources (similar to electricity DAM)
    """
    
    def __init__(self, market_id: str, resource_type: str, delivery_date: date, location: str, trading_core: TradingCoreIntegration):
        self.market_id = market_id
        self.resource_type = resource_type
        self.delivery_date = delivery_date
        self.location = location
        self.trading_core = trading_core
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
            resource_type=self.resource_type,
            quantity=quantity,
            price=max_price,
            delivery_hour=hour,
            location=self.location,
            flexible="flexible" if flexible else "fixed"
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
            resource_type=self.resource_type,
            quantity=quantity,
            price=min_price,
            delivery_hour=hour,
            location=self.location,
            reliability_score=1.0 # Placeholder, would be calculated
        )
        
        self.offers[hour].append(offer)
        
        return {"offer_id": offer.offer_id}
        
    async def clear_market(self) -> Dict:
        """
        Clear the market using merit order dispatch
        """
        results = {}
        
        for hour in range(24):
            hour_bids = sorted(self.bids[hour], key=lambda x: x.price, reverse=True)
            hour_offers = sorted(self.offers[hour], key=lambda x: x.price)
            
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
                "accepted_bids": len([b for b in hour_bids if b.price >= clearing_price]),
                "accepted_offers": len([o for o in hour_offers if o.price <= clearing_price])
            }
            
        self.is_cleared = True
        return results
        
    def _create_demand_curve(self, bids: List[ComputeBid]) -> List[Tuple[Decimal, Decimal]]:
        """Create demand curve from bids"""
        curve = []
        cumulative_quantity = Decimal("0")
        
        for bid in bids:
            curve.append((bid.price, cumulative_quantity))
            cumulative_quantity += bid.quantity
            curve.append((bid.price, cumulative_quantity))
            
        return curve
        
    def _create_supply_curve(self, offers: List[ComputeOffer]) -> List[Tuple[Decimal, Decimal]]:
        """Create supply curve from offers"""
        curve = []
        cumulative_quantity = Decimal("0")
        
        for offer in offers:
            curve.append((offer.price, cumulative_quantity))
            cumulative_quantity += offer.quantity
            curve.append((offer.price, cumulative_quantity))
            
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
            
        avg_bid = sum(b.price for b in self.bids[hour]) / len(self.bids[hour])
        avg_offer = sum(o.price for o in self.offers[hour]) / len(self.offers[hour])
        
        return (avg_bid + avg_offer) / 2
        
    async def _handle_flexible_bids(self, hour: int):
        """Handle flexible bids that can shift hours"""
        flexible_bids = [b for b in self.bids[hour] if b.flexibility == "flexible"]
        
        for bid in flexible_bids:
            # Try adjacent hours
            for offset in [-2, -1, 1, 2]:
                target_hour = hour + offset
                if 0 <= target_hour < 24:
                    if self.clearing_prices.get(target_hour, Decimal("999")) <= bid.price:
                        # Move bid to target hour
                        self.bids[hour].remove(bid)
                        bid.delivery_hour = target_hour
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
    Main engine for compute futures markets with physical settlement using trading-core
    """
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        oracle: OracleAggregatorClient,
        partner_capacity_manager=None
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        self.partner_capacity_manager = partner_capacity_manager
        
        # Trading core integration
        self.trading_core = TradingCoreIntegration()
        
        self.day_ahead_markets: Dict[str, 'DayAheadMarket'] = {}
        self.capacity_auction = CapacityAuction()
        self.ancillary_services = AncillaryServices()
        self.imbalance_tracker: Dict[str, Dict] = defaultdict(dict)
        
        # Physical settlement components
        self.settlements: Dict[str, 'ComputeSettlement'] = {}
        self.sla_monitors: Dict[str, Dict] = {}  # settlement_id -> monitoring data
        self.failover_providers: Dict[str, List[str]] = defaultdict(list)
        
        # Quality derivatives
        self.latency_futures: Dict[str, 'LatencyFuture'] = {}
        self.uptime_swaps: Dict[str, 'UptimeSwap'] = {}
        self.performance_bonds: Dict[str, 'PerformanceBond'] = {}
        
        # HTTP client for provisioning service
        self.http_client = httpx.AsyncClient(
            base_url="http://provisioning-service:8000",
            timeout=30.0
        )
        
        # Provider health tracking
        self.provider_health_scores: Dict[str, float] = {}  # provider_id -> health score (0-100)
        self.provider_health_history: Dict[str, List[Dict]] = defaultdict(list)
        self.provider_last_seen: Dict[str, datetime] = {}
        self.provider_regions: Dict[str, str] = {}  # provider_id -> region
        
        # Registered futures markets
        self.registered_markets: Set[str] = set()
        
        # Background tasks
        self._monitoring_task = None
        self._settlement_task = None
        self._health_monitoring_task = None
        
    async def start(self):
        """Start futures engine"""
        # Initialize trading core
        await self.trading_core.initialize()
        
        # Initialize markets
        await self._initialize_markets()
        
        # Start background tasks
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        self._settlement_task = asyncio.create_task(self._settlement_loop())
        self._health_monitoring_task = asyncio.create_task(self._health_monitoring_loop())
        
        logger.info("Compute futures engine started with trading-core integration")
        
    async def stop(self):
        """Stop background tasks"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
        if self._settlement_task:
            self._settlement_task.cancel()
        if self._health_monitoring_task:
            self._health_monitoring_task.cancel()
        await self.http_client.aclose()
        
    async def create_futures_contract(
        self,
        resource_type: str,
        delivery_date: datetime,
        location: str,
        contract_size: Decimal = Decimal("100"),
        settlement_type: str = "physical"
    ) -> str:
        """Create a new futures contract and register with trading-core"""
        contract_id = f"CF_{resource_type}_{delivery_date.strftime('%Y%m%d')}_{location}"
        
        # Register market with trading-core
        market_id = await self.trading_core.register_compute_market(
            resource_type=resource_type,
            market_type="futures",
            specifications={
                "contract_id": contract_id,
                "delivery_date": delivery_date.isoformat(),
                "location": location,
                "contract_size": str(contract_size),
                "settlement_type": settlement_type,
                "tick_size": "0.01",
                "margin_requirement": "0.15"  # 15% initial margin
            }
        )
        
        if market_id:
            self.registered_markets.add(market_id)
            
            # Publish contract creation event
            await self.pulsar.publish('compute.futures.contract_created', {
                'contract_id': contract_id,
                'market_id': market_id,
                'resource_type': resource_type,
                'delivery_date': delivery_date.isoformat(),
                'location': location,
                'contract_size': str(contract_size),
                'timestamp': datetime.utcnow().isoformat()
            })
            
            logger.info(f"Created futures contract: {contract_id}")
            
        return contract_id
        
    async def submit_futures_order(
        self,
        user_id: str,
        contract_id: str,
        side: str,  # "buy" or "sell"
        quantity: int,
        order_type: str = "limit",
        price: Optional[Decimal] = None
    ) -> Dict[str, Any]:
        """Submit futures order through trading-core"""
        # Extract details from contract ID
        parts = contract_id.split("_")
        if len(parts) < 4:
            return {"success": False, "error": "Invalid contract ID"}
            
        resource_type = parts[1]
        delivery_date = parts[2]
        location = parts[3]
        
        # Submit through trading-core
        result = await self.trading_core.submit_compute_order(
            user_id=user_id,
            resource_type=resource_type,
            market_type="futures",
            quantity=str(quantity),
            specifications={
                "contract_id": contract_id,
                "side": side,
                "order_type": order_type,
                "price": str(price) if price else None,
                "delivery_date": delivery_date,
                "location": location
            }
        )
        
        # Handle physical delivery setup on trade
        if result.get("success") and result.get("trades"):
            for trade in result["trades"]:
                await self._setup_physical_delivery(contract_id, trade)
                
        return result
        
    async def create_day_ahead_market(
        self,
        resource_type: str,
        delivery_date: date,
        location: str
    ) -> DayAheadMarket:
        """Create day-ahead market for hourly compute allocation"""
        market_id = f"DA_{resource_type}_{delivery_date}_{location}"
        
        if market_id not in self.day_ahead_markets:
            market = DayAheadMarket(
                market_id=market_id,
                resource_type=resource_type,
                delivery_date=delivery_date,
                location=location,
                trading_core=self.trading_core
            )
            
            # Register each hourly market with trading-core
            for hour in range(24):
                hourly_market_id = f"{market_id}_H{hour:02d}"
                await self.trading_core.register_compute_market(
                    resource_type=resource_type,
                    market_type="day_ahead",
                    specifications={
                        "market_id": hourly_market_id,
                        "delivery_date": delivery_date.isoformat(),
                        "delivery_hour": hour,
                        "location": location,
                        "settlement_type": "physical",
                        "clearing_mechanism": "uniform_price"
                    }
                )
                
            self.day_ahead_markets[market_id] = market
            
        return self.day_ahead_markets[market_id]
        
    async def submit_day_ahead_bid(
        self,
        bid: ComputeBid
    ) -> Dict[str, Any]:
        """Submit bid to day-ahead market through trading-core"""
        market_id = f"DA_{bid.resource_type}_{datetime.now().date()}_{bid.location}_H{bid.delivery_hour:02d}"
        
        # Submit as order through trading-core
        result = await self.trading_core.submit_compute_order(
            user_id=bid.user_id,
            resource_type=bid.resource_type,
            market_type="day_ahead",
            quantity=str(bid.quantity),
            specifications={
                "bid_id": bid.bid_id,
                "delivery_hour": bid.delivery_hour,
                "price": str(bid.price),
                "location": bid.location,
                "flexibility": bid.flexibility,
                "order_type": "limit"
            }
        )
        
        return result
        
    async def submit_day_ahead_offer(
        self,
        offer: ComputeOffer
    ) -> Dict[str, Any]:
        """Submit offer to day-ahead market through trading-core"""
        market_id = f"DA_{offer.resource_type}_{datetime.now().date()}_{offer.location}_H{offer.delivery_hour:02d}"
        
        # Register provider if needed
        await self.trading_core.register_compute_provider(
            provider_id=offer.provider_id,
            resources={
                offer.resource_type: {
                    "capacity": str(offer.quantity),
                    "location": offer.location,
                    "reliability_score": offer.reliability_score
                }
            }
        )
        
        # Submit as sell order through trading-core
        result = await self.trading_core.submit_compute_order(
            user_id=offer.provider_id,
            resource_type=offer.resource_type,
            market_type="day_ahead",
            quantity=str(offer.quantity),
            specifications={
                "offer_id": offer.offer_id,
                "delivery_hour": offer.delivery_hour,
                "price": str(offer.price),
                "location": offer.location,
                "order_type": "limit",
                "side": "sell"
            }
        )
        
        return result
        
    async def get_futures_price(
        self,
        contract_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get current futures price from trading-core"""
        orderbook = await self.trading_core.get_orderbook(contract_id, depth=1)
        
        if orderbook:
            best_bid = orderbook.get("bids", [{}])[0].get("price")
            best_ask = orderbook.get("asks", [{}])[0].get("price")
            
            return {
                "contract_id": contract_id,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "mid_price": str((Decimal(best_bid or 0) + Decimal(best_ask or 0)) / 2) if best_bid and best_ask else None,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        return None
        
    async def settle_expired_contracts(self):
        """Settle expired futures contracts"""
        current_time = datetime.utcnow()
        
        for market_id in self.registered_markets:
            if "_futures_" in market_id:
                # Check if contract expired
                # This is simplified - in production would check actual expiry
                
                # Get settlement price from oracle
                settlement_price = await self._calculate_settlement_price(market_id)
                
                if settlement_price:
                    # Trigger settlement through trading-core
                    await self.trading_core.trigger_settlement(
                        market_id=market_id,
                        settlement_price=settlement_price
                    )
                    
                    # Handle physical delivery
                    await self._process_physical_settlements(market_id)
                    
    async def _setup_physical_delivery(
        self,
        contract_id: str,
        trade: Dict[str, Any]
    ):
        """Setup physical delivery for futures trade"""
        settlement_id = f"SETTLE_{contract_id}_{trade['trade_id']}"
        
        settlement = ComputeSettlement(
            settlement_id=settlement_id,
            contract_id=contract_id,
            trade_id=trade['trade_id'],
            buyer_id=trade['buyer_id'],
            seller_id=trade.get('seller_id'),
            quantity=Decimal(trade['quantity']),
            delivery_date=self._extract_delivery_date(contract_id),
            status="pending"
        )
        
        self.settlements[settlement_id] = settlement
        
        # Store in Ignite
        await self.ignite.put(f"futures_settlement:{settlement_id}", settlement.__dict__)
        
        # Publish settlement created event
        await self.pulsar.publish('compute.futures.settlement_created', {
            'settlement_id': settlement_id,
            'contract_id': contract_id,
            'trade_id': trade['trade_id'],
            'quantity': str(settlement.quantity),
            'delivery_date': settlement.delivery_date.isoformat(),
            'timestamp': datetime.utcnow().isoformat()
        })
        
    async def _monitoring_loop(self):
        """Monitor futures positions and settlements"""
        while True:
            try:
                # Check for expired contracts
                await self.settle_expired_contracts()
                
                # Monitor active settlements
                await self._monitor_settlements()
                
                # Update provider health scores
                await self._update_provider_health()
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(300)
                
    async def _settlement_loop(self):
        """Process physical settlements"""
        while True:
            try:
                # Process pending settlements
                for settlement_id, settlement in list(self.settlements.items()):
                    if settlement.status == "pending" and settlement.delivery_date <= datetime.utcnow():
                        await self._execute_physical_settlement(settlement)
                        
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logger.error(f"Error in settlement loop: {e}")
                await asyncio.sleep(600)
                
    # Additional helper methods remain largely the same but integrate with trading-core
    # for market data and order management...

    # Physical Settlement Methods
    async def initiate_physical_settlement(
        self,
        contract_id: str,
        buyer_id: str,
        provider_id: str,
        resource_type: str,
        quantity: Decimal,
        delivery_start: datetime,
        duration_hours: int,
        sla_requirements: Optional[SLARequirement] = None
    ) -> ComputeSettlement:
        """Initiate physical settlement of compute contract"""
        settlement_id = f"CS_{contract_id}_{datetime.utcnow().timestamp()}"
        
        settlement = ComputeSettlement(
            settlement_id=settlement_id,
            contract_id=contract_id,
            buyer_id=buyer_id,
            provider_id=provider_id,
            resource_type=resource_type,
            quantity=quantity,
            delivery_date=delivery_start,
            status="pending"
        )
        
        self.settlements[settlement_id] = settlement
        
        # Trigger provisioning through event
        await self.pulsar.publish(
            "persistent://platformq/compute/provisioning-requests",
            {
                "settlement_id": settlement_id,
                "buyer_id": buyer_id,
                "provider_id": provider_id,
                "resource_spec": {
                    "type": resource_type,
                    "quantity": str(quantity),
                    "location_preference": "auto"
                },
                "delivery_start": delivery_start.isoformat(),
                "duration_hours": duration_hours,
                "sla_requirements": sla_requirements.__dict__ if sla_requirements else None
            }
        )
        
        # First try to allocate from partner capacity if available
        allocated = False
        if self.partner_capacity_manager:
            allocation = await self.partner_capacity_manager.allocate_from_inventory(
                resource_type,
                "us-east-1",  # TODO: Get region from settlement
                quantity
            )
            
            if allocation:
                # Use partner capacity
                settlement.provider_id = allocation["provider"].value
                await self._store_settlement(settlement)
                allocated = True
                
                logger.info(f"Allocated from partner {allocation['provider'].value} at ${allocation['wholesale_price']}/unit")
        
        # If not allocated from partners, try regular provisioning
        if not allocated:
            # Call provisioning service API
            try:
                response = await self.http_client.post(
                    "/api/v1/compute/provision",
                    json={
                        "settlement_id": settlement_id,
                        "resource_type": resource_type,
                        "quantity": str(quantity),
                        "duration_hours": duration_hours,
                        "start_time": delivery_start.isoformat(),
                        "provider_id": provider_id,
                        "buyer_id": buyer_id
                    }
                )
                
                if response.status_code == 200:
                    settlement.status = "provisioned"
                    await self._store_settlement(settlement)
                else:
                    logger.error(f"Provisioning failed with status {response.status_code}")
                    await self._handle_provisioning_failure(settlement)
                    
            except Exception as e:
                logger.error(f"Error calling provisioning service: {e}")
                await self._handle_provisioning_failure(settlement)
            
        return settlement
        
    async def _handle_provisioning_failure(
        self,
        settlement: ComputeSettlement
    ):
        """Handle failed provisioning with automatic failover"""
        settlement.status = "failed"
        
        # Get failover providers sorted by health score
        failover_providers = await self._get_healthy_failover_providers(
            settlement.resource_type,
            settlement.provider_id
        )
        
        logger.info(f"Healthy failover providers for {settlement.resource_type}: {failover_providers}")
        
        for provider_id in failover_providers:
            try:
                health_score = self.provider_health_scores.get(provider_id, 100.0)
                logger.info(f"Attempting failover to provider {provider_id} (health: {health_score:.1f}%)")
                
                response = await self.http_client.post(
                    "/api/v1/compute/provision",
                    json={
                        "settlement_id": settlement.settlement_id,
                        "resource_type": settlement.resource_type,
                        "quantity": str(settlement.quantity),
                        "duration_hours": settlement.duration_hours,
                        "start_time": settlement.delivery_date.isoformat(),
                        "provider_id": provider_id,
                        "buyer_id": settlement.buyer_id,
                        "is_failover": True
                    }
                )
                
                if response.status_code == 200:
                    settlement.failover_used = True
                    settlement.failover_provider = provider_id
                    settlement.status = "provisioned"
                    await self._store_settlement(settlement)
                    
                    # Notify about failover
                    await self.pulsar.publish(
                        "persistent://platformq/compute/failover-events",
                        {
                            "settlement_id": settlement.settlement_id,
                            "original_provider": settlement.provider_id,
                            "failover_provider": provider_id,
                            "provider_health_score": health_score,
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )
                    break
                    
            except Exception as e:
                logger.error(f"Failover to {provider_id} failed: {e}")
                continue
                
        if settlement.status == "failed":
            # Apply liquidated damages
            await self._apply_liquidated_damages(settlement)
            
    async def _apply_liquidated_damages(
        self,
        settlement: ComputeSettlement
    ):
        """Apply liquidated damages for failed provisioning"""
        # Platform backfills compute and charges provider penalty
        damage_amount = settlement.quantity * Decimal("50")  # $50 per unit penalty
        
        await self.pulsar.publish(
            "persistent://platformq/compute/liquidated-damages",
            {
                "settlement_id": settlement.settlement_id,
                "provider_id": settlement.provider_id,
                "damage_amount": str(damage_amount),
                "reason": "provisioning_failure",
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        settlement.penalty_amount += damage_amount
        await self._store_settlement(settlement)
        
    # SLA Monitoring
    async def _monitor_sla_loop(self):
        """Background task to monitor SLA compliance"""
        while True:
            try:
                active_settlements = [
                    s for s in self.settlements.values()
                    if s.status == "provisioned"
                    and datetime.utcnow() < s.delivery_date + timedelta(hours=s.duration_hours)
                ]
                
                for settlement in active_settlements:
                    await self._check_sla_compliance(settlement)
                    
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in SLA monitoring: {e}")
                await asyncio.sleep(60)
                
    async def _check_sla_compliance(
        self,
        settlement: ComputeSettlement
    ):
        """Check SLA compliance for active settlement"""
        try:
            # Get metrics from monitoring service
            response = await self.http_client.get(
                f"/api/v1/metrics/compute/{settlement.settlement_id}"
            )
            
            if response.status_code != 200:
                return
                
            metrics = response.json()
            
            # Check uptime
            uptime_percent = Decimal(str(metrics.get("uptime_percent", 100)))
            if uptime_percent < Decimal("99.9"):
                violation = {
                    "type": "uptime",
                    "expected": "99.9",
                    "actual": str(uptime_percent),
                    "timestamp": datetime.utcnow().isoformat()
                }
                settlement.sla_violations.append(violation)
                
            # Check latency if applicable
            if "latency_ms" in metrics:
                latency = metrics["latency_ms"]
                if latency > 100:  # Example threshold
                    violation = {
                        "type": "latency",
                        "expected": "100ms",
                        "actual": f"{latency}ms",
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    settlement.sla_violations.append(violation)
                    
            # Check performance
            performance_score = Decimal(str(metrics.get("performance_score", 1.0)))
            if performance_score < Decimal("0.95"):
                violation = {
                    "type": "performance",
                    "expected": "0.95",
                    "actual": str(performance_score),
                    "timestamp": datetime.utcnow().isoformat()
                }
                settlement.sla_violations.append(violation)
                
            # Apply penalties if violations exist
            if settlement.sla_violations:
                await self._apply_sla_penalties(settlement)
                
        except Exception as e:
            logger.error(f"Error checking SLA for {settlement.settlement_id}: {e}")
            
    async def _apply_sla_penalties(
        self,
        settlement: ComputeSettlement
    ):
        """Apply penalties for SLA violations"""
        total_penalty = Decimal("0")
        
        for violation in settlement.sla_violations[-10:]:  # Last 10 violations
            if violation["type"] == "uptime":
                # 5% penalty for uptime violations
                penalty = settlement.quantity * Decimal("5")
            elif violation["type"] == "latency":
                # 10% penalty for latency violations
                penalty = settlement.quantity * Decimal("10")
            elif violation["type"] == "performance":
                # 15% penalty for performance violations
                penalty = settlement.quantity * Decimal("15")
            else:
                penalty = Decimal("0")
                
            total_penalty += penalty
            
        if total_penalty > Decimal("0"):
            settlement.penalty_amount += total_penalty
            await self._store_settlement(settlement)
            
            # Notify about penalties
            await self.pulsar.publish(
                "persistent://platformq/compute/sla-penalties",
                {
                    "settlement_id": settlement.settlement_id,
                    "violations": settlement.sla_violations[-10:],
                    "penalty_amount": str(total_penalty),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
    # Settlement finalization
    async def _settlement_loop(self):
        """Background task to finalize completed settlements"""
        while True:
            try:
                for settlement in list(self.settlements.values()):
                    if (settlement.status == "provisioned" and
                        datetime.utcnow() >= settlement.delivery_date + timedelta(hours=settlement.duration_hours)):
                        await self._finalize_settlement(settlement)
                        
                await asyncio.sleep(3600)  # Check hourly
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in settlement loop: {e}")
                await asyncio.sleep(300)
                
    async def _finalize_settlement(
        self,
        settlement: ComputeSettlement
    ):
        """Finalize completed settlement"""
        # Calculate final settlement amount
        base_amount = settlement.quantity * Decimal("100")  # Example pricing
        final_amount = base_amount - settlement.penalty_amount
        
        settlement.settlement_amount = final_amount
        settlement.status = "completed"
        
        # Process payment
        await self.pulsar.publish(
            "persistent://platformq/compute/settlement-complete",
            {
                "settlement_id": settlement.settlement_id,
                "buyer_id": settlement.buyer_id,
                "provider_id": settlement.provider_id,
                "amount": str(final_amount),
                "penalties": str(settlement.penalty_amount),
                "sla_violations": len(settlement.sla_violations),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        await self._store_settlement(settlement)
        
    # Compute Quality Derivatives
    async def create_latency_future(
        self,
        buyer_id: str,
        seller_id: str,
        source_region: str,
        dest_region: str,
        strike_latency_ms: int,
        notional: Decimal,
        expiry_days: int
    ) -> LatencyFuture:
        """Create latency future contract"""
        contract_id = f"LF_{source_region}_{dest_region}_{datetime.utcnow().timestamp()}"
        
        future = LatencyFuture(
            contract_id=contract_id,
            buyer_id=buyer_id,
            seller_id=seller_id,
            region_pair=(source_region, dest_region),
            strike_latency_ms=strike_latency_ms,
            notional=notional,
            expiry=datetime.utcnow() + timedelta(days=expiry_days)
        )
        
        self.latency_futures[contract_id] = future
        
        # Store in cache
        await self.ignite.set(f"latency_future:{contract_id}", future.__dict__)
        
        # Publish event
        await self.pulsar.publish(
            "persistent://platformq/compute/latency-future-created",
            {
                "contract_id": contract_id,
                "buyer_id": buyer_id,
                "seller_id": seller_id,
                "regions": f"{source_region}-{dest_region}",
                "strike": strike_latency_ms,
                "notional": str(notional),
                "expiry": future.expiry.isoformat()
            }
        )
        
        return future
        
    async def create_uptime_swap(
        self,
        buyer_id: str,
        seller_id: str,
        service_id: str,
        fixed_uptime_rate: Decimal,
        notional_per_hour: Decimal,
        duration_days: int
    ) -> UptimeSwap:
        """Create uptime swap contract"""
        swap_id = f"US_{service_id}_{datetime.utcnow().timestamp()}"
        
        swap = UptimeSwap(
            swap_id=swap_id,
            buyer_id=buyer_id,
            seller_id=seller_id,
            service_id=service_id,
            fixed_uptime_rate=fixed_uptime_rate,
            notional_per_hour=notional_per_hour,
            start_date=datetime.utcnow(),
            end_date=datetime.utcnow() + timedelta(days=duration_days)
        )
        
        self.uptime_swaps[swap_id] = swap
        
        # Store in cache
        await self.ignite.set(f"uptime_swap:{swap_id}", swap.__dict__)
        
        return swap
        
    async def create_performance_bond(
        self,
        issuer_id: str,
        buyer_id: str,
        hardware_spec: Dict,
        guaranteed_performance: Decimal,
        bond_amount: Decimal,
        expiry_days: int
    ) -> PerformanceBond:
        """Create performance bond"""
        bond_id = f"PB_{hardware_spec.get('gpu_model', 'generic')}_{datetime.utcnow().timestamp()}"
        
        bond = PerformanceBond(
            bond_id=bond_id,
            issuer_id=issuer_id,
            buyer_id=buyer_id,
            hardware_spec=hardware_spec,
            guaranteed_performance=guaranteed_performance,
            bond_amount=bond_amount,
            expiry=datetime.utcnow() + timedelta(days=expiry_days)
        )
        
        self.performance_bonds[bond_id] = bond
        
        # Store in cache
        await self.ignite.set(f"performance_bond:{bond_id}", bond.__dict__)
        
        return bond
        
    async def settle_quality_derivatives(self):
        """Settle expired quality derivatives"""
        current_time = datetime.utcnow()
        
        # Settle latency futures
        for contract_id, future in list(self.latency_futures.items()):
            if current_time >= future.expiry:
                await self._settle_latency_future(future)
                del self.latency_futures[contract_id]
                
        # Settle uptime swaps
        for swap_id, swap in list(self.uptime_swaps.items()):
            if current_time >= swap.end_date:
                await self._settle_uptime_swap(swap)
                del self.uptime_swaps[swap_id]
                
        # Check performance bonds
        for bond_id, bond in list(self.performance_bonds.items()):
            if current_time >= bond.expiry:
                await self._settle_performance_bond(bond)
                del self.performance_bonds[bond_id]
                
    async def _settle_latency_future(self, future: LatencyFuture):
        """Settle expired latency future"""
        # Get actual latency measurements
        measurements = await self._get_latency_measurements(
            future.region_pair[0],
            future.region_pair[1],
            future.expiry - timedelta(days=30),  # Last 30 days
            future.expiry
        )
        
        if measurements:
            avg_latency = sum(measurements) / len(measurements)
            
            # Calculate settlement
            if avg_latency > future.strike_latency_ms:
                # Buyer profits (latency was worse than strike)
                payout = future.notional * Decimal(str((avg_latency - future.strike_latency_ms) / 100))
                winner = future.buyer_id
                loser = future.seller_id
            else:
                # Seller profits (latency was better than strike)
                payout = Decimal("0")
                winner = future.seller_id
                loser = future.buyer_id
                
            # Process settlement
            await self.pulsar.publish(
                "persistent://platformq/compute/latency-future-settled",
                {
                    "contract_id": future.contract_id,
                    "winner": winner,
                    "loser": loser,
                    "payout": str(payout),
                    "avg_latency": avg_latency,
                    "strike_latency": future.strike_latency_ms,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
    async def _get_latency_measurements(
        self,
        source: str,
        dest: str,
        start: datetime,
        end: datetime
    ) -> List[float]:
        """Get historical latency measurements"""
        # In practice, query monitoring system
        # For now, return mock data
        return [45.2, 48.1, 44.5, 52.3, 46.8]  # ms
        
    async def _settle_uptime_swap(self, swap: UptimeSwap):
        """Settle uptime swap"""
        # Get actual uptime data
        uptime_data = await self._get_uptime_data(
            swap.service_id,
            swap.start_date,
            swap.end_date
        )
        
        actual_uptime = Decimal(str(uptime_data.get("uptime_percent", 100))) / Decimal("100")
        downtime_hours = uptime_data.get("downtime_hours", 0)
        
        # Calculate net payment
        if actual_uptime < swap.fixed_uptime_rate:
            # Seller pays buyer for downtime
            payment = swap.notional_per_hour * Decimal(str(downtime_hours))
            payer = swap.seller_id
            receiver = swap.buyer_id
        else:
            # No payment needed
            payment = Decimal("0")
            payer = None
            receiver = None
            
        if payment > Decimal("0"):
            await self.pulsar.publish(
                "persistent://platformq/compute/uptime-swap-settled",
                {
                    "swap_id": swap.swap_id,
                    "payer": payer,
                    "receiver": receiver,
                    "payment": str(payment),
                    "actual_uptime": str(actual_uptime),
                    "fixed_rate": str(swap.fixed_uptime_rate),
                    "downtime_hours": downtime_hours,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
    async def _get_uptime_data(
        self,
        service_id: str,
        start: datetime,
        end: datetime
    ) -> Dict:
        """Get uptime data for service"""
        # In practice, query monitoring system
        return {
            "uptime_percent": 99.5,
            "downtime_hours": 3.6
        }
        
    async def _settle_performance_bond(self, bond: PerformanceBond):
        """Settle performance bond"""
        # Get performance test results
        test_results = await self._get_performance_test_results(
            bond.issuer_id,
            bond.hardware_spec,
            bond.expiry - timedelta(days=7),  # Last week
            bond.expiry
        )
        
        avg_performance = Decimal(str(sum(test_results) / len(test_results))) if test_results else Decimal("0")
        
        if avg_performance >= bond.guaranteed_performance:
            # Performance met, return bond to issuer
            await self.pulsar.publish(
                "persistent://platformq/compute/performance-bond-returned",
                {
                    "bond_id": bond.bond_id,
                    "issuer_id": bond.issuer_id,
                    "bond_amount": str(bond.bond_amount),
                    "avg_performance": str(avg_performance),
                    "guaranteed": str(bond.guaranteed_performance),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
        else:
            # Performance not met, bond goes to buyer
            await self.pulsar.publish(
                "persistent://platformq/compute/performance-bond-claimed",
                {
                    "bond_id": bond.bond_id,
                    "buyer_id": bond.buyer_id,
                    "bond_amount": str(bond.bond_amount),
                    "avg_performance": str(avg_performance),
                    "guaranteed": str(bond.guaranteed_performance),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
    async def _get_performance_test_results(
        self,
        provider_id: str,
        hardware_spec: Dict,
        start: datetime,
        end: datetime
    ) -> List[float]:
        """Get performance test results"""
        # In practice, query benchmark results
        return [0.96, 0.94, 0.97, 0.95, 0.93]  # Performance scores
        
    async def register_failover_provider(
        self,
        resource_type: str,
        provider_id: str,
        priority: int = 100
    ):
        """Register a failover provider for a resource type"""
        # Get current providers as list of dicts
        current_providers = []
        for pid in self.failover_providers[resource_type]:
            current_providers.append({
                "provider_id": pid,
                "priority": 100  # Default priority for existing providers
            })
        
        # Add new provider
        current_providers.append({
            "provider_id": provider_id,
            "priority": priority
        })
        
        # Sort by priority (lower number = higher priority)
        current_providers.sort(key=lambda x: x["priority"])
        
        # Keep only provider IDs in the list
        self.failover_providers[resource_type] = [
            p["provider_id"] for p in current_providers
        ]
        
    async def _store_settlement(self, settlement: ComputeSettlement):
        """Store settlement in cache"""
        await self.ignite.set(
            f"compute_settlement:{settlement.settlement_id}",
            settlement.__dict__
        )
        
    async def _get_healthy_failover_providers(
        self,
        resource_type: str,
        exclude_provider: str
    ) -> List[str]:
        """Get failover providers sorted by health score"""
        all_providers = self.failover_providers.get(resource_type, [])
        
        # Filter out excluded provider and unhealthy providers
        healthy_providers = []
        for provider_id in all_providers:
            if provider_id == exclude_provider:
                continue
                
            health_score = self.provider_health_scores.get(provider_id, 100.0)
            
            # Only consider providers with health score > 50%
            if health_score > 50.0:
                healthy_providers.append((provider_id, health_score))
                
        # Sort by health score (descending)
        healthy_providers.sort(key=lambda x: x[1], reverse=True)
        
        return [provider_id for provider_id, _ in healthy_providers]
        
    async def _monitor_provider_health_loop(self):
        """Background task to monitor provider health"""
        while True:
            try:
                # Subscribe to provider health events
                await self._process_health_events()
                
                # Check for stale providers
                await self._check_stale_providers()
                
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in provider health monitoring: {e}")
                await asyncio.sleep(30)
                
    async def _process_health_events(self):
        """Process health events from Flink CEP"""
        try:
            # In practice, subscribe to Pulsar topic for health events
            # For now, simulate with cache lookups
            
            health_events = await self.ignite.get_all(
                pattern="provider_health:*",
                limit=100
            )
            
            for key, event in health_events.items():
                provider_id = event.get("provider_id")
                health_score = event.get("health_score", 100.0)
                timestamp = datetime.fromisoformat(event.get("timestamp"))
                
                # Update health score
                self.provider_health_scores[provider_id] = health_score
                self.provider_last_seen[provider_id] = timestamp
                
                # Store health history
                history = self.provider_health_history[provider_id]
                history.append({
                    "score": health_score,
                    "timestamp": timestamp
                })
                
                # Keep only last 100 entries
                if len(history) > 100:
                    self.provider_health_history[provider_id] = history[-100:]
                    
        except Exception as e:
            logger.error(f"Error processing health events: {e}")
            
    async def _check_stale_providers(self):
        """Check for providers that haven't reported health recently"""
        now = datetime.utcnow()
        stale_threshold = timedelta(minutes=5)
        
        for provider_id, last_seen in list(self.provider_last_seen.items()):
            if now - last_seen > stale_threshold:
                # Mark provider as unhealthy
                self.provider_health_scores[provider_id] = 0.0
                
                logger.warning(f"Provider {provider_id} marked as unhealthy - no health reports for {(now - last_seen).seconds} seconds")
                
                # Publish provider down event
                await self.pulsar.publish(
                    "persistent://platformq/compute/provider-health",
                    {
                        "provider_id": provider_id,
                        "event_type": "PROVIDER_DOWN",
                        "health_score": 0.0,
                        "reason": "no_health_reports",
                        "timestamp": now.isoformat()
                    }
                )
                
    async def update_provider_health(
        self,
        provider_id: str,
        health_score: float,
        metrics: Optional[Dict] = None
    ):
        """Update provider health score from external monitoring"""
        self.provider_health_scores[provider_id] = health_score
        self.provider_last_seen[provider_id] = datetime.utcnow()
        
        # Store in cache for other services
        await self.ignite.set(
            f"provider_health:{provider_id}",
            {
                "provider_id": provider_id,
                "health_score": health_score,
                "metrics": metrics or {},
                "timestamp": datetime.utcnow().isoformat()
            },
            ttl=300  # 5 minute TTL
        )
        
    async def get_provider_health_trend(
        self,
        provider_id: str,
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get provider health trend over time"""
        history = self.provider_health_history.get(provider_id, [])
        
        if not history:
            return {
                "provider_id": provider_id,
                "current_health": self.provider_health_scores.get(provider_id, 0.0),
                "trend": "unknown",
                "average_health": 0.0,
                "volatility": 0.0
            }
            
        # Filter by time window
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        recent_history = [
            h for h in history 
            if h["timestamp"] > cutoff
        ]
        
        if not recent_history:
            return {
                "provider_id": provider_id,
                "current_health": self.provider_health_scores.get(provider_id, 0.0),
                "trend": "no_data",
                "average_health": 0.0,
                "volatility": 0.0
            }
            
        # Calculate metrics
        scores = [h["score"] for h in recent_history]
        avg_health = sum(scores) / len(scores)
        
        # Calculate trend
        if len(scores) >= 2:
            recent_avg = sum(scores[-5:]) / min(5, len(scores))
            older_avg = sum(scores[:-5]) / max(1, len(scores) - 5)
            
            if recent_avg > older_avg + 5:
                trend = "improving"
            elif recent_avg < older_avg - 5:
                trend = "degrading"
            else:
                trend = "stable"
        else:
            trend = "insufficient_data"
            
        # Calculate volatility (standard deviation)
        mean = avg_health
        variance = sum((x - mean) ** 2 for x in scores) / len(scores)
        volatility = variance ** 0.5
        
        return {
            "provider_id": provider_id,
            "current_health": self.provider_health_scores.get(provider_id, 0.0),
            "trend": trend,
            "average_health": avg_health,
            "volatility": volatility,
            "sample_count": len(recent_history)
        } 