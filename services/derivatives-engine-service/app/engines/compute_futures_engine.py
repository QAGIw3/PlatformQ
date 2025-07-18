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
import httpx

from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient
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
    buyer_id: str
    provider_id: str
    resource_type: str
    quantity: Decimal
    delivery_start: datetime
    duration_hours: int
    provisioning_status: str  # "pending", "provisioned", "failed", "completed"
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
    Main engine for compute futures markets with physical settlement
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
        
        self.day_ahead_markets: Dict[str, DayAheadMarket] = {}
        self.capacity_auction = CapacityAuction()
        self.ancillary_services = AncillaryServices()
        self.imbalance_tracker: Dict[str, Dict] = defaultdict(dict)
        
        # Physical settlement components
        self.settlements: Dict[str, ComputeSettlement] = {}
        self.sla_monitors: Dict[str, Dict] = {}  # settlement_id -> monitoring data
        self.failover_providers: Dict[str, List[str]] = defaultdict(list)
        
        # Quality derivatives
        self.latency_futures: Dict[str, LatencyFuture] = {}
        self.uptime_swaps: Dict[str, UptimeSwap] = {}
        self.performance_bonds: Dict[str, PerformanceBond] = {}
        
        # HTTP client for provisioning service
        self.http_client = httpx.AsyncClient(
            base_url="http://provisioning-service:8000",
            timeout=30.0
        )
        
        # Background tasks
        self._monitoring_task = None
        self._settlement_task = None
        
    async def start(self):
        """Start background monitoring and settlement tasks"""
        self._monitoring_task = asyncio.create_task(self._monitor_sla_loop())
        self._settlement_task = asyncio.create_task(self._settlement_loop())
        
    async def stop(self):
        """Stop background tasks"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
        if self._settlement_task:
            self._settlement_task.cancel()
        await self.http_client.aclose()
        
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
            delivery_start=delivery_start,
            duration_hours=duration_hours,
            provisioning_status="pending"
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
                settlement.provisioning_status = "provisioned"
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
                    settlement.provisioning_status = "provisioned"
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
        settlement.provisioning_status = "failed"
        
        # Try failover providers
        failover_providers = self.failover_providers.get(
            settlement.resource_type, []
        )
        
        # Debug: log the order of providers
        logger.info(f"Failover providers for {settlement.resource_type}: {failover_providers}")
        
        for provider_id in failover_providers:
            if provider_id != settlement.provider_id:
                try:
                    logger.info(f"Attempting failover to provider {provider_id}")
                    
                    response = await self.http_client.post(
                        "/api/v1/compute/provision",
                        json={
                            "settlement_id": settlement.settlement_id,
                            "resource_type": settlement.resource_type,
                            "quantity": str(settlement.quantity),
                            "duration_hours": settlement.duration_hours,
                            "start_time": settlement.delivery_start.isoformat(),
                            "provider_id": provider_id,
                            "buyer_id": settlement.buyer_id,
                            "is_failover": True
                        }
                    )
                    
                    if response.status_code == 200:
                        settlement.failover_used = True
                        settlement.failover_provider = provider_id
                        settlement.provisioning_status = "provisioned"
                        await self._store_settlement(settlement)
                        
                        # Notify about failover
                        await self.pulsar.publish(
                            "persistent://platformq/compute/failover-events",
                            {
                                "settlement_id": settlement.settlement_id,
                                "original_provider": settlement.provider_id,
                                "failover_provider": provider_id,
                                "timestamp": datetime.utcnow().isoformat()
                            }
                        )
                        break
                        
                except Exception as e:
                    logger.error(f"Failover to {provider_id} failed: {e}")
                    continue
                    
        if settlement.provisioning_status == "failed":
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
                    if s.provisioning_status == "provisioned"
                    and datetime.utcnow() < s.delivery_start + timedelta(hours=s.duration_hours)
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
                    if (settlement.provisioning_status == "provisioned" and
                        datetime.utcnow() >= settlement.delivery_start + timedelta(hours=settlement.duration_hours)):
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
        settlement.provisioning_status = "completed"
        
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