"""
Burst Compute Derivatives Engine

Specialized derivatives for handling sudden compute demand spikes and surge capacity
"""

from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import logging
import uuid
import numpy as np
from collections import defaultdict

from app.integrations import (
    IgniteCache,
    PulsarEventPublisher,
    OracleAggregatorClient
)
from app.engines.compute_spot_market import ComputeSpotMarket
from app.engines.compute_futures_engine import ComputeFuturesEngine
from app.engines.partner_capacity_manager import PartnerCapacityManager

logger = logging.getLogger(__name__)


class BurstTriggerType(Enum):
    """Types of burst triggers"""
    DEMAND_SPIKE = "demand_spike"      # Triggered by demand exceeding threshold
    PRICE_SPIKE = "price_spike"        # Triggered by price exceeding threshold
    CAPACITY_DROP = "capacity_drop"    # Triggered by capacity falling below threshold
    TIME_BASED = "time_based"          # Triggered at specific times
    COMPOUND = "compound"              # Multiple conditions


class BurstDerivativeType(Enum):
    """Types of burst compute derivatives"""
    SURGE_SWAP = "surge_swap"          # Swap fixed for floating surge capacity
    SPIKE_OPTION = "spike_option"      # Option to access surge capacity
    BURST_FORWARD = "burst_forward"    # Forward contract on burst capacity
    CAPACITY_WARRANT = "capacity_warrant"  # Warrant for future capacity
    DEMAND_COLLAR = "demand_collar"    # Collar strategy for demand spikes


@dataclass
class BurstTrigger:
    """Trigger conditions for burst activation"""
    trigger_type: BurstTriggerType
    threshold: Decimal
    measurement_window: timedelta
    cooldown_period: timedelta = timedelta(hours=1)
    consecutive_breaches: int = 1  # Number of consecutive breaches required
    
    def evaluate(self, current_value: Decimal, history: List[Tuple[datetime, Decimal]]) -> bool:
        """Evaluate if trigger conditions are met"""
        if self.trigger_type == BurstTriggerType.DEMAND_SPIKE:
            return current_value > self.threshold
        elif self.trigger_type == BurstTriggerType.PRICE_SPIKE:
            return current_value > self.threshold
        elif self.trigger_type == BurstTriggerType.CAPACITY_DROP:
            return current_value < self.threshold
        else:
            return False


@dataclass
class BurstDerivative:
    """Burst compute derivative contract"""
    derivative_id: str
    derivative_type: BurstDerivativeType
    underlying: str  # Resource type
    trigger: BurstTrigger
    notional_capacity: Decimal  # Capacity units
    surge_multiplier: Decimal  # How much capacity increases on trigger
    max_duration: timedelta  # Maximum burst duration
    premium: Decimal  # Premium paid for the derivative
    strike_price: Optional[Decimal] = None  # For options
    expiry: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    creator: Optional[str] = None
    
    @property
    def is_expired(self) -> bool:
        """Check if derivative has expired"""
        if self.expiry:
            return datetime.utcnow() >= self.expiry
        return False
    
    @property
    def surge_capacity(self) -> Decimal:
        """Calculate surge capacity on trigger"""
        return self.notional_capacity * self.surge_multiplier


@dataclass
class BurstActivation:
    """Record of burst activation"""
    activation_id: str
    derivative_id: str
    trigger_time: datetime
    trigger_value: Decimal
    surge_capacity_allocated: Decimal
    actual_duration: Optional[timedelta] = None
    total_cost: Optional[Decimal] = None
    performance_metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SurgePool:
    """Pool of reserved surge capacity"""
    pool_id: str
    resource_type: str
    total_capacity: Decimal
    reserved_capacity: Decimal
    active_capacity: Decimal
    surge_price_multiplier: Decimal  # Price multiplier during surge
    providers: List[str] = field(default_factory=list)
    
    @property
    def available_surge(self) -> Decimal:
        """Available surge capacity"""
        return self.total_capacity - self.reserved_capacity - self.active_capacity


class BurstComputeEngine:
    """
    Engine for burst compute derivatives and surge capacity management
    """
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        oracle: OracleAggregatorClient,
        spot_market: ComputeSpotMarket,
        futures_engine: ComputeFuturesEngine,
        partner_manager: PartnerCapacityManager
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        self.spot_market = spot_market
        self.futures_engine = futures_engine
        self.partner_manager = partner_manager
        
        # Derivatives registry
        self.derivatives: Dict[str, BurstDerivative] = {}
        
        # Surge pools by resource type
        self.surge_pools: Dict[str, SurgePool] = {}
        
        # Active bursts
        self.active_bursts: Dict[str, BurstActivation] = {}
        
        # Historical metrics
        self.demand_history: Dict[str, List[Tuple[datetime, Decimal]]] = defaultdict(list)
        self.price_history: Dict[str, List[Tuple[datetime, Decimal]]] = defaultdict(list)
        self.capacity_history: Dict[str, List[Tuple[datetime, Decimal]]] = defaultdict(list)
        
        # Monitoring parameters
        self.monitoring_interval = 5  # seconds
        self.history_retention = timedelta(days=7)
        
        # Background tasks
        self._monitoring_task = None
        self._surge_management_task = None
        self._settlement_task = None
        
    async def start(self):
        """Start burst compute engine"""
        # Initialize surge pools
        await self._initialize_surge_pools()
        
        # Load active derivatives
        await self._load_active_derivatives()
        
        # Start background tasks
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        self._surge_management_task = asyncio.create_task(self._surge_management_loop())
        self._settlement_task = asyncio.create_task(self._settlement_loop())
        
        logger.info("Burst compute engine started")
        
    async def stop(self):
        """Stop burst compute engine"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
        if self._surge_management_task:
            self._surge_management_task.cancel()
        if self._settlement_task:
            self._settlement_task.cancel()
            
    async def create_burst_derivative(
        self,
        derivative_type: BurstDerivativeType,
        underlying: str,
        trigger: BurstTrigger,
        notional_capacity: Decimal,
        surge_multiplier: Decimal,
        max_duration: timedelta,
        strike_price: Optional[Decimal] = None,
        expiry: Optional[datetime] = None,
        creator: Optional[str] = None
    ) -> BurstDerivative:
        """Create a new burst compute derivative"""
        
        # Calculate premium based on derivative type
        premium = await self._calculate_premium(
            derivative_type,
            underlying,
            trigger,
            notional_capacity,
            surge_multiplier,
            max_duration,
            strike_price
        )
        
        # Create derivative
        derivative = BurstDerivative(
            derivative_id=f"BURST_{uuid.uuid4().hex}",
            derivative_type=derivative_type,
            underlying=underlying,
            trigger=trigger,
            notional_capacity=notional_capacity,
            surge_multiplier=surge_multiplier,
            max_duration=max_duration,
            premium=premium,
            strike_price=strike_price,
            expiry=expiry,
            creator=creator
        )
        
        # Reserve surge capacity
        await self._reserve_surge_capacity(derivative)
        
        # Store derivative
        self.derivatives[derivative.derivative_id] = derivative
        await self._store_derivative(derivative)
        
        # Emit event
        await self.pulsar.publish('compute.burst.derivative_created', {
            'derivative_id': derivative.derivative_id,
            'type': derivative_type.value,
            'underlying': underlying,
            'notional_capacity': str(notional_capacity),
            'surge_multiplier': str(surge_multiplier),
            'premium': str(premium),
            'expiry': expiry.isoformat() if expiry else None
        })
        
        return derivative
        
    async def trigger_burst(
        self,
        derivative_id: str,
        trigger_value: Decimal
    ) -> BurstActivation:
        """Trigger a burst derivative"""
        
        derivative = self.derivatives.get(derivative_id)
        if not derivative:
            raise ValueError(f"Derivative {derivative_id} not found")
            
        if derivative.is_expired:
            raise ValueError("Derivative has expired")
            
        # Check if already active
        if derivative_id in self.active_bursts:
            raise ValueError("Burst already active")
            
        # Verify trigger conditions
        history = self._get_relevant_history(derivative.underlying, derivative.trigger)
        if not derivative.trigger.evaluate(trigger_value, history):
            raise ValueError("Trigger conditions not met")
            
        # Allocate surge capacity
        allocated_capacity = await self._allocate_surge_capacity(
            derivative.underlying,
            derivative.surge_capacity
        )
        
        if allocated_capacity < derivative.surge_capacity:
            logger.warning(
                f"Only allocated {allocated_capacity} of {derivative.surge_capacity} surge capacity"
            )
            
        # Create activation record
        activation = BurstActivation(
            activation_id=f"ACT_{uuid.uuid4().hex}",
            derivative_id=derivative_id,
            trigger_time=datetime.utcnow(),
            trigger_value=trigger_value,
            surge_capacity_allocated=allocated_capacity
        )
        
        # Store activation
        self.active_bursts[derivative_id] = activation
        await self._store_activation(activation)
        
        # Provision surge resources
        await self._provision_surge_resources(derivative, activation)
        
        # Emit event
        await self.pulsar.publish('compute.burst.activated', {
            'activation_id': activation.activation_id,
            'derivative_id': derivative_id,
            'trigger_value': str(trigger_value),
            'surge_capacity': str(allocated_capacity),
            'timestamp': activation.trigger_time.isoformat()
        })
        
        # Schedule deactivation
        asyncio.create_task(
            self._schedule_deactivation(derivative, activation)
        )
        
        return activation
        
    async def create_surge_swap(
        self,
        underlying: str,
        notional_capacity: Decimal,
        fixed_surge_rate: Decimal,  # Fixed rate paid
        floating_benchmark: str,     # Benchmark for floating rate
        tenor_days: int,
        creator: str
    ) -> Dict[str, Any]:
        """
        Create a surge capacity swap
        
        Party A pays fixed surge premium
        Party B provides surge capacity when needed
        """
        
        # Create trigger based on benchmark
        trigger = BurstTrigger(
            trigger_type=BurstTriggerType.DEMAND_SPIKE,
            threshold=await self._get_surge_threshold(underlying, floating_benchmark),
            measurement_window=timedelta(minutes=5),
            cooldown_period=timedelta(hours=2)
        )
        
        # Create swap as a derivative
        derivative = await self.create_burst_derivative(
            derivative_type=BurstDerivativeType.SURGE_SWAP,
            underlying=underlying,
            trigger=trigger,
            notional_capacity=notional_capacity,
            surge_multiplier=Decimal("3"),  # 3x surge on trigger
            max_duration=timedelta(hours=4),
            expiry=datetime.utcnow() + timedelta(days=tenor_days),
            creator=creator
        )
        
        # Calculate swap cash flows
        fixed_payments = self._calculate_fixed_payments(
            notional_capacity,
            fixed_surge_rate,
            tenor_days
        )
        
        expected_floating = await self._estimate_floating_payments(
            underlying,
            floating_benchmark,
            tenor_days
        )
        
        return {
            "derivative_id": derivative.derivative_id,
            "swap_type": "surge_capacity_swap",
            "underlying": underlying,
            "notional_capacity": str(notional_capacity),
            "fixed_rate": str(fixed_surge_rate),
            "floating_benchmark": floating_benchmark,
            "tenor_days": tenor_days,
            "fixed_payments": [
                {"date": p[0].isoformat(), "amount": str(p[1])}
                for p in fixed_payments
            ],
            "expected_floating_value": str(expected_floating),
            "swap_value": str(expected_floating - sum(p[1] for p in fixed_payments))
        }
        
    async def create_spike_option(
        self,
        underlying: str,
        capacity_units: Decimal,
        spike_threshold: Decimal,  # Price level that triggers
        strike_multiplier: Decimal,  # Multiplier over normal price
        expiry_days: int,
        creator: str
    ) -> Dict[str, Any]:
        """
        Create an option triggered by price/demand spikes
        
        Gives right to access capacity at strike when spot exceeds threshold
        """
        
        # Get current spot price
        spot_data = await self.spot_market.get_spot_price(underlying)
        spot_price = Decimal(spot_data.get("last_trade_price", "0"))
        
        # Create price spike trigger
        trigger = BurstTrigger(
            trigger_type=BurstTriggerType.PRICE_SPIKE,
            threshold=spike_threshold,
            measurement_window=timedelta(minutes=1),
            consecutive_breaches=3  # Require sustained spike
        )
        
        # Create spike option
        derivative = await self.create_burst_derivative(
            derivative_type=BurstDerivativeType.SPIKE_OPTION,
            underlying=underlying,
            trigger=trigger,
            notional_capacity=capacity_units,
            surge_multiplier=Decimal("1"),  # No multiplier for options
            max_duration=timedelta(hours=24),  # Can use for 24 hours once triggered
            strike_price=spot_price * strike_multiplier,
            expiry=datetime.utcnow() + timedelta(days=expiry_days),
            creator=creator
        )
        
        # Calculate option value using jump diffusion model
        option_value = await self._price_spike_option(
            spot_price,
            spike_threshold,
            derivative.strike_price,
            expiry_days
        )
        
        return {
            "derivative_id": derivative.derivative_id,
            "option_type": "spike_call_option",
            "underlying": underlying,
            "capacity_units": str(capacity_units),
            "spike_threshold": str(spike_threshold),
            "strike_price": str(derivative.strike_price),
            "current_spot": str(spot_price),
            "expiry_days": expiry_days,
            "premium": str(derivative.premium),
            "theoretical_value": str(option_value),
            "trigger": {
                "type": trigger.trigger_type.value,
                "threshold": str(trigger.threshold),
                "measurement_window": str(trigger.measurement_window)
            }
        }
        
    async def create_demand_collar(
        self,
        underlying: str,
        base_capacity: Decimal,
        min_capacity_utilization: Decimal,  # Floor
        max_price_spike: Decimal,           # Cap
        tenor_days: int,
        creator: str
    ) -> Dict[str, Any]:
        """
        Create a collar strategy for demand management
        
        Provides protection against both low utilization and price spikes
        """
        
        # Create compound trigger
        trigger = BurstTrigger(
            trigger_type=BurstTriggerType.COMPOUND,
            threshold=Decimal("0"),  # Not used for compound
            measurement_window=timedelta(minutes=15)
        )
        
        # Create collar as burst forward
        derivative = await self.create_burst_derivative(
            derivative_type=BurstDerivativeType.DEMAND_COLLAR,
            underlying=underlying,
            trigger=trigger,
            notional_capacity=base_capacity,
            surge_multiplier=Decimal("2"),
            max_duration=timedelta(hours=8),
            expiry=datetime.utcnow() + timedelta(days=tenor_days),
            creator=creator
        )
        
        # Calculate collar parameters
        current_utilization = await self._get_current_utilization(underlying)
        current_price = await self._get_current_price(underlying)
        
        collar_value = await self._price_demand_collar(
            base_capacity,
            current_utilization,
            min_capacity_utilization,
            current_price,
            max_price_spike,
            tenor_days
        )
        
        return {
            "derivative_id": derivative.derivative_id,
            "strategy": "demand_collar",
            "underlying": underlying,
            "base_capacity": str(base_capacity),
            "protection": {
                "utilization_floor": str(min_capacity_utilization),
                "price_cap": str(max_price_spike)
            },
            "current_metrics": {
                "utilization": str(current_utilization),
                "price": str(current_price)
            },
            "tenor_days": tenor_days,
            "collar_cost": str(derivative.premium),
            "collar_value": str(collar_value)
        }
        
    async def get_surge_pool_status(
        self,
        resource_type: str
    ) -> Dict[str, Any]:
        """Get status of surge capacity pool"""
        
        pool = self.surge_pools.get(resource_type)
        if not pool:
            return {"error": f"No surge pool for {resource_type}"}
            
        # Get current metrics
        current_demand = await self._get_current_demand(resource_type)
        surge_probability = await self._calculate_surge_probability(resource_type)
        
        return {
            "pool_id": pool.pool_id,
            "resource_type": resource_type,
            "total_capacity": str(pool.total_capacity),
            "reserved_capacity": str(pool.reserved_capacity),
            "active_capacity": str(pool.active_capacity),
            "available_surge": str(pool.available_surge),
            "surge_price_multiplier": str(pool.surge_price_multiplier),
            "providers": len(pool.providers),
            "current_metrics": {
                "demand": str(current_demand),
                "surge_probability": str(surge_probability),
                "active_bursts": len([
                    b for b in self.active_bursts.values()
                    if self.derivatives[b.derivative_id].underlying == resource_type
                ])
            }
        }
        
    async def _calculate_premium(
        self,
        derivative_type: BurstDerivativeType,
        underlying: str,
        trigger: BurstTrigger,
        notional_capacity: Decimal,
        surge_multiplier: Decimal,
        max_duration: timedelta,
        strike_price: Optional[Decimal]
    ) -> Decimal:
        """Calculate premium for burst derivative"""
        
        # Base premium components
        base_capacity_cost = await self._get_capacity_cost(underlying, notional_capacity)
        surge_premium = base_capacity_cost * (surge_multiplier - 1) * Decimal("0.3")
        
        # Probability of trigger
        trigger_probability = await self._estimate_trigger_probability(
            underlying,
            trigger
        )
        
        # Duration factor
        duration_hours = max_duration.total_seconds() / 3600
        duration_factor = Decimal(str(np.log1p(duration_hours / 24)))
        
        # Type-specific adjustments
        if derivative_type == BurstDerivativeType.SPIKE_OPTION:
            # Option pricing with jump component
            if strike_price:
                moneyness = await self._calculate_moneyness(underlying, strike_price)
                premium = base_capacity_cost * trigger_probability * moneyness
            else:
                premium = base_capacity_cost * trigger_probability
                
        elif derivative_type == BurstDerivativeType.SURGE_SWAP:
            # Swap pricing based on expected surge events
            expected_surges = trigger_probability * 30  # Monthly
            premium = surge_premium * expected_surges * duration_factor
            
        else:
            # Default pricing
            premium = (base_capacity_cost + surge_premium) * trigger_probability * duration_factor
            
        # Add risk premium
        risk_premium = premium * Decimal("0.2")
        
        return premium + risk_premium
        
    async def _reserve_surge_capacity(
        self,
        derivative: BurstDerivative
    ):
        """Reserve surge capacity for derivative"""
        
        pool = self.surge_pools.get(derivative.underlying)
        if not pool:
            # Create pool if doesn't exist
            pool = await self._create_surge_pool(derivative.underlying)
            
        # Reserve capacity
        required_surge = derivative.surge_capacity
        if pool.available_surge >= required_surge:
            pool.reserved_capacity += required_surge
            await self._update_surge_pool(pool)
        else:
            # Try to expand pool
            await self._expand_surge_pool(pool, required_surge)
            
    async def _allocate_surge_capacity(
        self,
        resource_type: str,
        requested_capacity: Decimal
    ) -> Decimal:
        """Allocate surge capacity from pool"""
        
        pool = self.surge_pools.get(resource_type)
        if not pool:
            return Decimal("0")
            
        allocated = min(requested_capacity, pool.available_surge)
        
        if allocated > 0:
            pool.reserved_capacity -= allocated
            pool.active_capacity += allocated
            await self._update_surge_pool(pool)
            
        return allocated
        
    async def _provision_surge_resources(
        self,
        derivative: BurstDerivative,
        activation: BurstActivation
    ):
        """Provision actual surge compute resources"""
        
        # Get surge providers
        pool = self.surge_pools.get(derivative.underlying)
        if not pool:
            return
            
        # Allocate from providers based on priority
        remaining = activation.surge_capacity_allocated
        provisioned_resources = []
        
        for provider in pool.providers:
            if remaining <= 0:
                break
                
            # Try to allocate from provider
            allocation = await self.partner_manager.allocate_from_inventory(
                derivative.underlying,
                provider,
                remaining
            )
            
            if allocation:
                provisioned_resources.append({
                    "provider": provider,
                    "capacity": allocation["quantity"],
                    "access_details": allocation["access_details"]
                })
                remaining -= Decimal(allocation["quantity"])
                
        # Update activation with provisioning details
        activation.performance_metrics["provisioned_resources"] = provisioned_resources
        activation.performance_metrics["provisioning_time"] = (
            datetime.utcnow() - activation.trigger_time
        ).total_seconds()
        
        await self._update_activation(activation)
        
    async def _monitoring_loop(self):
        """Monitor triggers and market conditions"""
        while True:
            try:
                # Update metrics
                for resource_type in ["gpu", "cpu", "storage", "bandwidth"]:
                    # Get current metrics
                    demand = await self._get_current_demand(resource_type)
                    price = await self._get_current_price(resource_type)
                    capacity = await self._get_available_capacity(resource_type)
                    
                    # Store history
                    now = datetime.utcnow()
                    self.demand_history[resource_type].append((now, demand))
                    self.price_history[resource_type].append((now, price))
                    self.capacity_history[resource_type].append((now, capacity))
                    
                    # Clean old history
                    cutoff = now - self.history_retention
                    self.demand_history[resource_type] = [
                        (t, v) for t, v in self.demand_history[resource_type]
                        if t > cutoff
                    ]
                    
                # Check derivative triggers
                for derivative_id, derivative in self.derivatives.items():
                    if derivative.is_expired or derivative_id in self.active_bursts:
                        continue
                        
                    # Get current value for trigger
                    if derivative.trigger.trigger_type == BurstTriggerType.DEMAND_SPIKE:
                        current_value = await self._get_current_demand(derivative.underlying)
                    elif derivative.trigger.trigger_type == BurstTriggerType.PRICE_SPIKE:
                        current_value = await self._get_current_price(derivative.underlying)
                    elif derivative.trigger.trigger_type == BurstTriggerType.CAPACITY_DROP:
                        current_value = await self._get_available_capacity(derivative.underlying)
                    else:
                        continue
                        
                    # Check trigger
                    history = self._get_relevant_history(derivative.underlying, derivative.trigger)
                    if derivative.trigger.evaluate(current_value, history):
                        # Auto-trigger for certain derivative types
                        if derivative.derivative_type in [
                            BurstDerivativeType.SURGE_SWAP,
                            BurstDerivativeType.BURST_FORWARD
                        ]:
                            try:
                                await self.trigger_burst(derivative_id, current_value)
                                logger.info(f"Auto-triggered burst {derivative_id}")
                            except Exception as e:
                                logger.error(f"Failed to auto-trigger {derivative_id}: {e}")
                                
                await asyncio.sleep(self.monitoring_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(30)
                
    async def _surge_management_loop(self):
        """Manage surge pools and capacity"""
        while True:
            try:
                # Update surge pool status
                for pool in self.surge_pools.values():
                    # Check pool health
                    utilization = (pool.reserved_capacity + pool.active_capacity) / pool.total_capacity
                    
                    if utilization > Decimal("0.8"):
                        # Need more surge capacity
                        await self._expand_surge_pool(
                            pool,
                            pool.total_capacity * Decimal("0.2")
                        )
                    elif utilization < Decimal("0.2"):
                        # Can reduce surge capacity
                        await self._contract_surge_pool(pool)
                        
                    # Update pricing
                    await self._update_surge_pricing(pool)
                    
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in surge management loop: {e}")
                await asyncio.sleep(300)
                
    async def _settlement_loop(self):
        """Settle completed bursts"""
        while True:
            try:
                # Check active bursts
                for derivative_id, activation in list(self.active_bursts.items()):
                    derivative = self.derivatives.get(derivative_id)
                    if not derivative:
                        continue
                        
                    # Check if should deactivate
                    elapsed = datetime.utcnow() - activation.trigger_time
                    if elapsed >= derivative.max_duration:
                        await self._deactivate_burst(derivative, activation)
                        
                # Settle expired derivatives
                for derivative_id, derivative in list(self.derivatives.items()):
                    if derivative.is_expired:
                        await self._settle_derivative(derivative)
                        del self.derivatives[derivative_id]
                        
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in settlement loop: {e}")
                await asyncio.sleep(300)
                
    async def _schedule_deactivation(
        self,
        derivative: BurstDerivative,
        activation: BurstActivation
    ):
        """Schedule automatic deactivation"""
        await asyncio.sleep(derivative.max_duration.total_seconds())
        await self._deactivate_burst(derivative, activation)
        
    async def _deactivate_burst(
        self,
        derivative: BurstDerivative,
        activation: BurstActivation
    ):
        """Deactivate a burst and release resources"""
        
        # Calculate actual duration
        activation.actual_duration = datetime.utcnow() - activation.trigger_time
        
        # Calculate total cost
        surge_hours = activation.actual_duration.total_seconds() / 3600
        surge_price = await self._get_surge_price(derivative.underlying)
        activation.total_cost = (
            activation.surge_capacity_allocated * surge_price * Decimal(str(surge_hours))
        )
        
        # Release surge capacity
        pool = self.surge_pools.get(derivative.underlying)
        if pool:
            pool.active_capacity -= activation.surge_capacity_allocated
            await self._update_surge_pool(pool)
            
        # Update activation
        await self._update_activation(activation)
        
        # Remove from active
        if derivative.derivative_id in self.active_bursts:
            del self.active_bursts[derivative.derivative_id]
            
        # Emit event
        await self.pulsar.publish('compute.burst.deactivated', {
            'activation_id': activation.activation_id,
            'derivative_id': derivative.derivative_id,
            'actual_duration': str(activation.actual_duration),
            'total_cost': str(activation.total_cost),
            'performance_metrics': activation.performance_metrics
        })
        
    async def _settle_derivative(self, derivative: BurstDerivative):
        """Settle an expired derivative"""
        
        # Calculate settlement value based on activations
        total_value = Decimal("0")
        activations = await self._get_derivative_activations(derivative.derivative_id)
        
        for activation in activations:
            if activation.total_cost:
                total_value += activation.total_cost
                
        # Compare to premium paid
        pnl = total_value - derivative.premium
        
        # Emit settlement event
        await self.pulsar.publish('compute.burst.settled', {
            'derivative_id': derivative.derivative_id,
            'premium_paid': str(derivative.premium),
            'total_value': str(total_value),
            'pnl': str(pnl),
            'activations': len(activations)
        })
        
    async def _initialize_surge_pools(self):
        """Initialize surge capacity pools"""
        for resource_type in ["gpu", "cpu", "storage", "bandwidth"]:
            pool = SurgePool(
                pool_id=f"SURGE_{resource_type.upper()}",
                resource_type=resource_type,
                total_capacity=Decimal("1000"),  # Initial capacity
                reserved_capacity=Decimal("0"),
                active_capacity=Decimal("0"),
                surge_price_multiplier=Decimal("2.5")  # 2.5x normal price
            )
            self.surge_pools[resource_type] = pool
            await self._store_surge_pool(pool)
            
    async def _create_surge_pool(self, resource_type: str) -> SurgePool:
        """Create a new surge pool"""
        pool = SurgePool(
            pool_id=f"SURGE_{resource_type.upper()}_{uuid.uuid4().hex[:8]}",
            resource_type=resource_type,
            total_capacity=Decimal("100"),
            reserved_capacity=Decimal("0"),
            active_capacity=Decimal("0"),
            surge_price_multiplier=Decimal("3.0")
        )
        
        # Find providers
        inventory = await self.partner_manager.get_available_inventory(resource_type)
        for inv in inventory[:5]:  # Top 5 providers
            pool.providers.append(inv.provider.value)
            
        self.surge_pools[resource_type] = pool
        await self._store_surge_pool(pool)
        
        return pool
        
    async def _expand_surge_pool(
        self,
        pool: SurgePool,
        additional_capacity: Decimal
    ):
        """Expand surge pool capacity"""
        
        # Try to get more capacity from partners
        for provider in pool.providers:
            remaining = additional_capacity - (pool.total_capacity - pool.total_capacity)
            if remaining <= 0:
                break
                
            # Check available inventory
            # In production, negotiate surge agreements
            pool.total_capacity += remaining
            
        await self._update_surge_pool(pool)
        
    async def _contract_surge_pool(self, pool: SurgePool):
        """Reduce surge pool capacity"""
        
        # Only contract unused capacity
        unused = pool.total_capacity - pool.reserved_capacity - pool.active_capacity
        if unused > pool.total_capacity * Decimal("0.3"):
            pool.total_capacity -= unused * Decimal("0.5")
            await self._update_surge_pool(pool)
            
    async def _update_surge_pricing(self, pool: SurgePool):
        """Update surge pricing multiplier"""
        
        utilization = (pool.reserved_capacity + pool.active_capacity) / pool.total_capacity
        
        # Dynamic pricing based on utilization
        if utilization > Decimal("0.9"):
            pool.surge_price_multiplier = Decimal("4.0")
        elif utilization > Decimal("0.7"):
            pool.surge_price_multiplier = Decimal("3.0")
        elif utilization > Decimal("0.5"):
            pool.surge_price_multiplier = Decimal("2.5")
        else:
            pool.surge_price_multiplier = Decimal("2.0")
            
        await self._update_surge_pool(pool)
        
    async def _get_current_demand(self, resource_type: str) -> Decimal:
        """Get current demand for resource"""
        # This would integrate with real metrics
        # For now, simulate
        return Decimal("100") * (Decimal("1") + Decimal(str(np.random.rand())))
        
    async def _get_current_price(self, resource_type: str) -> Decimal:
        """Get current price for resource"""
        spot_data = await self.spot_market.get_spot_price(resource_type)
        return Decimal(spot_data.get("last_trade_price", "10"))
        
    async def _get_available_capacity(self, resource_type: str) -> Decimal:
        """Get available capacity"""
        inventory = await self.partner_manager.get_available_inventory(resource_type)
        return sum(Decimal(inv.available_capacity) for inv in inventory)
        
    async def _get_capacity_cost(
        self,
        resource_type: str,
        capacity: Decimal
    ) -> Decimal:
        """Get cost for capacity"""
        price = await self._get_current_price(resource_type)
        return price * capacity
        
    async def _estimate_trigger_probability(
        self,
        underlying: str,
        trigger: BurstTrigger
    ) -> Decimal:
        """Estimate probability of trigger occurring"""
        
        # Analyze historical data
        if trigger.trigger_type == BurstTriggerType.DEMAND_SPIKE:
            history = self.demand_history.get(underlying, [])
        elif trigger.trigger_type == BurstTriggerType.PRICE_SPIKE:
            history = self.price_history.get(underlying, [])
        else:
            history = []
            
        if not history:
            return Decimal("0.1")  # Default 10%
            
        # Count breaches
        breaches = 0
        for _, value in history:
            if trigger.trigger_type in [BurstTriggerType.DEMAND_SPIKE, BurstTriggerType.PRICE_SPIKE]:
                if value > trigger.threshold:
                    breaches += 1
            elif trigger.trigger_type == BurstTriggerType.CAPACITY_DROP:
                if value < trigger.threshold:
                    breaches += 1
                    
        probability = Decimal(str(breaches)) / Decimal(str(len(history)))
        return min(Decimal("1"), max(Decimal("0"), probability))
        
    async def _calculate_moneyness(
        self,
        underlying: str,
        strike: Decimal
    ) -> Decimal:
        """Calculate option moneyness"""
        spot = await self._get_current_price(underlying)
        return spot / strike if strike > 0 else Decimal("1")
        
    async def _get_surge_threshold(
        self,
        underlying: str,
        benchmark: str
    ) -> Decimal:
        """Get surge threshold based on benchmark"""
        # This would use real benchmark data
        # For now, use simple multiplier
        base_demand = await self._get_current_demand(underlying)
        
        if "high" in benchmark.lower():
            return base_demand * Decimal("1.5")
        elif "extreme" in benchmark.lower():
            return base_demand * Decimal("2.0")
        else:
            return base_demand * Decimal("1.3")
            
    def _calculate_fixed_payments(
        self,
        notional: Decimal,
        rate: Decimal,
        days: int
    ) -> List[Tuple[datetime, Decimal]]:
        """Calculate fixed swap payments"""
        payment_dates = []
        daily_rate = rate / Decimal("365")
        
        for i in range(1, days + 1):
            if i % 30 == 0:  # Monthly payments
                payment_date = datetime.utcnow() + timedelta(days=i)
                payment = notional * daily_rate * Decimal("30")
                payment_dates.append((payment_date, payment))
                
        return payment_dates
        
    async def _estimate_floating_payments(
        self,
        underlying: str,
        benchmark: str,
        days: int
    ) -> Decimal:
        """Estimate floating payments based on surge events"""
        
        # Estimate number of surge events
        surge_probability = await self._calculate_surge_probability(underlying)
        expected_surges = surge_probability * Decimal(str(days / 30))  # Monthly
        
        # Estimate cost per surge
        surge_capacity = Decimal("100")  # Standard surge size
        surge_price = await self._get_surge_price(underlying)
        surge_duration = Decimal("4")  # Hours
        
        cost_per_surge = surge_capacity * surge_price * surge_duration
        
        return expected_surges * cost_per_surge
        
    async def _price_spike_option(
        self,
        spot: Decimal,
        threshold: Decimal,
        strike: Decimal,
        days: int
    ) -> Decimal:
        """Price spike option using jump diffusion"""
        
        # Simplified jump diffusion pricing
        # In production, use proper jump diffusion model
        
        # Base Black-Scholes value
        time_to_expiry = Decimal(str(days / 365.25))
        volatility = Decimal("0.6")  # High vol for compute
        
        # Add jump component
        jump_intensity = await self._estimate_jump_intensity(spot, threshold)
        jump_size = (threshold - spot) / spot
        
        # Approximate option value
        if threshold > spot:
            # Out of the money
            base_value = spot * Decimal("0.1") * time_to_expiry
            jump_value = jump_intensity * jump_size * spot * time_to_expiry
        else:
            # In the money
            base_value = (spot - strike) * Decimal("0.8")
            jump_value = Decimal("0")
            
        return base_value + jump_value
        
    async def _get_current_utilization(self, underlying: str) -> Decimal:
        """Get current capacity utilization"""
        demand = await self._get_current_demand(underlying)
        capacity = await self._get_available_capacity(underlying)
        
        if capacity > 0:
            return demand / capacity
        return Decimal("1")
        
    async def _price_demand_collar(
        self,
        base_capacity: Decimal,
        current_util: Decimal,
        floor_util: Decimal,
        current_price: Decimal,
        price_cap: Decimal,
        days: int
    ) -> Decimal:
        """Price demand collar strategy"""
        
        # Price put option (utilization floor)
        put_value = Decimal("0")
        if current_util < floor_util:
            put_value = (floor_util - current_util) * base_capacity * current_price
            
        # Price call option (price cap)
        call_value = Decimal("0")
        if current_price > price_cap:
            call_value = (current_price - price_cap) * base_capacity
            
        # Time decay
        time_factor = Decimal(str(days / 365.25))
        
        return (put_value + call_value) * time_factor * Decimal("0.3")
        
    async def _calculate_surge_probability(self, resource_type: str) -> Decimal:
        """Calculate probability of surge event"""
        
        # Analyze recent demand patterns
        demand_history = self.demand_history.get(resource_type, [])
        if not demand_history:
            return Decimal("0.05")  # Default 5%
            
        # Calculate demand volatility
        demands = [float(d) for _, d in demand_history[-100:]]
        if len(demands) > 1:
            volatility = Decimal(str(np.std(demands) / np.mean(demands)))
            
            # Higher volatility = higher surge probability
            surge_prob = min(Decimal("0.5"), volatility * Decimal("2"))
            return surge_prob
            
        return Decimal("0.05")
        
    async def _get_surge_price(self, resource_type: str) -> Decimal:
        """Get surge pricing"""
        pool = self.surge_pools.get(resource_type)
        if not pool:
            return await self._get_current_price(resource_type) * Decimal("3")
            
        base_price = await self._get_current_price(resource_type)
        return base_price * pool.surge_price_multiplier
        
    async def _estimate_jump_intensity(
        self,
        current: Decimal,
        threshold: Decimal
    ) -> Decimal:
        """Estimate jump intensity for pricing"""
        distance = abs(threshold - current) / current
        
        # Closer to threshold = higher jump probability
        if distance < Decimal("0.1"):
            return Decimal("0.5")
        elif distance < Decimal("0.2"):
            return Decimal("0.3")
        elif distance < Decimal("0.5"):
            return Decimal("0.1")
        else:
            return Decimal("0.05")
            
    def _get_relevant_history(
        self,
        underlying: str,
        trigger: BurstTrigger
    ) -> List[Tuple[datetime, Decimal]]:
        """Get relevant history for trigger evaluation"""
        
        if trigger.trigger_type == BurstTriggerType.DEMAND_SPIKE:
            history = self.demand_history.get(underlying, [])
        elif trigger.trigger_type == BurstTriggerType.PRICE_SPIKE:
            history = self.price_history.get(underlying, [])
        elif trigger.trigger_type == BurstTriggerType.CAPACITY_DROP:
            history = self.capacity_history.get(underlying, [])
        else:
            history = []
            
        # Filter by measurement window
        cutoff = datetime.utcnow() - trigger.measurement_window
        return [(t, v) for t, v in history if t > cutoff]
        
    async def _store_derivative(self, derivative: BurstDerivative):
        """Store derivative in cache"""
        await self.ignite.put(f"burst_derivative:{derivative.derivative_id}", derivative)
        
    async def _store_activation(self, activation: BurstActivation):
        """Store activation in cache"""
        await self.ignite.put(f"burst_activation:{activation.activation_id}", activation)
        
    async def _update_activation(self, activation: BurstActivation):
        """Update activation in cache"""
        await self.ignite.put(f"burst_activation:{activation.activation_id}", activation)
        
    async def _store_surge_pool(self, pool: SurgePool):
        """Store surge pool in cache"""
        await self.ignite.put(f"surge_pool:{pool.pool_id}", pool)
        
    async def _update_surge_pool(self, pool: SurgePool):
        """Update surge pool in cache"""
        await self.ignite.put(f"surge_pool:{pool.pool_id}", pool)
        
    async def _load_active_derivatives(self):
        """Load active derivatives from cache"""
        # In production, scan cache for active derivatives
        pass
        
    async def _get_derivative_activations(
        self,
        derivative_id: str
    ) -> List[BurstActivation]:
        """Get all activations for a derivative"""
        # In production, query from cache
        return []
        
    async def get_analytics(self) -> Dict[str, Any]:
        """Get burst compute analytics"""
        
        total_derivatives = len(self.derivatives)
        active_bursts = len(self.active_bursts)
        
        # Calculate totals by type
        by_type = defaultdict(int)
        total_notional = Decimal("0")
        total_premium = Decimal("0")
        
        for derivative in self.derivatives.values():
            by_type[derivative.derivative_type.value] += 1
            total_notional += derivative.notional_capacity
            total_premium += derivative.premium
            
        # Surge pool utilization
        pool_utilization = {}
        for resource_type, pool in self.surge_pools.items():
            if pool.total_capacity > 0:
                util = (pool.reserved_capacity + pool.active_capacity) / pool.total_capacity
                pool_utilization[resource_type] = str(util)
                
        return {
            "total_derivatives": total_derivatives,
            "active_bursts": active_bursts,
            "derivatives_by_type": dict(by_type),
            "total_notional_capacity": str(total_notional),
            "total_premium_collected": str(total_premium),
            "surge_pool_utilization": pool_utilization,
            "recent_activations": len([
                a for a in self.active_bursts.values()
                if datetime.utcnow() - a.trigger_time < timedelta(hours=24)
            ])
        } 