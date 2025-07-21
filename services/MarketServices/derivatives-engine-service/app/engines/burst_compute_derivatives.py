"""
Burst Compute Derivatives Engine

Specialized derivatives for handling sudden compute demand spikes and surge capacity using trading-core-service
"""

from typing import Dict, List, Optional, Tuple, Any, Set
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
from app.integrations.trading_core_integration import TradingCoreIntegration
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
    notional_capacity: Decimal
    surge_multiplier: Decimal  # How much capacity multiplies on trigger
    max_duration: timedelta
    premium: Decimal
    strike_price: Optional[Decimal] = None
    expiry: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    creator: Optional[str] = None
    
    @property
    def surge_capacity(self) -> Decimal:
        """Total surge capacity when triggered"""
        return self.notional_capacity * self.surge_multiplier
    
    @property
    def is_expired(self) -> bool:
        """Check if derivative has expired"""
        if self.expiry:
            return datetime.utcnow() >= self.expiry
        return False


@dataclass
class BurstActivation:
    """Record of burst derivative activation"""
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
    """Pool of surge capacity for resource type"""
    pool_id: str
    resource_type: str
    total_capacity: Decimal
    reserved_capacity: Decimal  # Reserved for derivatives
    active_capacity: Decimal    # Currently in use
    surge_price_multiplier: Decimal
    providers: List[str] = field(default_factory=list)
    last_updated: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def available_surge(self) -> Decimal:
        """Available surge capacity"""
        return self.total_capacity - self.reserved_capacity - self.active_capacity


class BurstComputeEngine:
    """
    Engine for burst compute derivatives and surge capacity management integrated with trading-core-service
    """
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        oracle: OracleAggregatorClient,
        partner_manager: PartnerCapacityManager
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        self.partner_manager = partner_manager
        
        # Trading core integration
        self.trading_core = TradingCoreIntegration()
        
        # Derivatives registry
        self.derivatives: Dict[str, BurstDerivative] = {}
        
        # Surge pools by resource type
        self.surge_pools: Dict[str, SurgePool] = {}
        
        # Active bursts
        self.active_bursts: Dict[str, BurstActivation] = {}
        
        # Registered burst markets
        self.registered_burst_markets: Set[str] = set()
        
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
        # Initialize trading core
        await self.trading_core.initialize()
        
        # Initialize surge pools
        await self._initialize_surge_pools()
        
        # Load active derivatives
        await self._load_active_derivatives()
        
        # Start background tasks
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        self._surge_management_task = asyncio.create_task(self._surge_management_loop())
        self._settlement_task = asyncio.create_task(self._settlement_loop())
        
        logger.info("Burst compute engine started with trading-core integration")
        
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
        """Create a new burst compute derivative and register with trading-core"""
        
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
        
        # Register burst market with trading-core
        market_id = await self._register_burst_market(derivative)
        
        # Reserve surge capacity
        await self._reserve_surge_capacity(derivative)
        
        # Store derivative
        self.derivatives[derivative.derivative_id] = derivative
        await self._store_derivative(derivative)
        
        # Emit event
        await self.pulsar.publish('compute.burst.derivative_created', {
            'derivative_id': derivative.derivative_id,
            'market_id': market_id,
            'type': derivative_type.value,
            'underlying': underlying,
            'notional_capacity': str(notional_capacity),
            'surge_multiplier': str(surge_multiplier),
            'premium': str(premium),
            'expiry': expiry.isoformat() if expiry else None
        })
        
        return derivative
        
    async def _register_burst_market(self, derivative: BurstDerivative) -> str:
        """Register burst derivative market with trading-core"""
        market_id = f"BURST_{derivative.underlying}_{derivative.derivative_type.value}_{derivative.derivative_id}"
        
        if market_id not in self.registered_burst_markets:
            # Register as derivatives market
            success = await self.trading_core.register_derivatives_market(
                market_id=market_id,
                market_type="burst_derivative",
                underlying_asset=derivative.underlying,
                specifications={
                    "derivative_type": derivative.derivative_type.value,
                    "trigger_type": derivative.trigger.trigger_type.value,
                    "trigger_threshold": str(derivative.trigger.threshold),
                    "notional_capacity": str(derivative.notional_capacity),
                    "surge_multiplier": str(derivative.surge_multiplier),
                    "max_duration_hours": derivative.max_duration.total_seconds() / 3600,
                    "strike_price": str(derivative.strike_price) if derivative.strike_price else None,
                    "expiry": derivative.expiry.isoformat() if derivative.expiry else None
                }
            )
            
            if success:
                self.registered_burst_markets.add(market_id)
                logger.info(f"Registered burst market: {market_id}")
            else:
                logger.error(f"Failed to register burst market: {market_id}")
                
        return market_id
        
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
            
        # Allocate surge capacity via trading-core
        allocated_capacity = await self._allocate_surge_capacity_via_trading_core(
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
        
        # Get current spot price from trading-core
        spot_price = await self._get_spot_price_from_trading_core(underlying)
        
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
        current_price = await self._get_spot_price_from_trading_core(underlying)
        
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
        
    async def trade_burst_derivative(
        self,
        user_id: str,
        derivative_id: str,
        side: str,  # "buy" or "sell"
        quantity: Decimal = Decimal("1")
    ) -> Dict[str, Any]:
        """Trade a burst derivative through trading-core"""
        
        derivative = self.derivatives.get(derivative_id)
        if not derivative:
            raise ValueError(f"Derivative {derivative_id} not found")
            
        if derivative.is_expired:
            raise ValueError("Derivative has expired")
            
        # Get market ID
        market_id = await self._register_burst_market(derivative)
        
        # Submit order through trading-core
        order_result = await self.trading_core.submit_derivatives_order(
            user_id=user_id,
            market_id=market_id,
            side=side,
            quantity=str(quantity),
            order_type="market",
            metadata={
                "derivative_id": derivative_id,
                "derivative_type": derivative.derivative_type.value,
                "premium": str(derivative.premium),
                "trigger_type": derivative.trigger.trigger_type.value,
                "trigger_threshold": str(derivative.trigger.threshold)
            }
        )
        
        if order_result.get("success"):
            # Emit trade event
            await self.pulsar.publish('compute.burst.traded', {
                'user_id': user_id,
                'derivative_id': derivative_id,
                'market_id': market_id,
                'side': side,
                'quantity': str(quantity),
                'order_result': order_result,
                'timestamp': datetime.utcnow().isoformat()
            })
            
        return order_result
        
    async def _allocate_surge_capacity_via_trading_core(
        self,
        resource_type: str,
        requested_capacity: Decimal
    ) -> Decimal:
        """Allocate surge capacity through trading-core"""
        
        # Submit surge allocation order
        result = await self.trading_core.allocate_compute_resource(
            resource_type=resource_type,
            quantity=str(requested_capacity),
            allocation_type="surge",
            specifications={
                "surge_priority": "high",
                "max_price_multiplier": "3.0",
                "duration_hours": "4"
            }
        )
        
        if result.get("success"):
            allocated = Decimal(result.get("allocated_quantity", "0"))
            
            # Update surge pool tracking
            pool = self.surge_pools.get(resource_type)
            if pool:
                pool.active_capacity += allocated
                await self._update_surge_pool(pool)
                
            return allocated
            
        return Decimal("0")
        
    async def _get_spot_price_from_trading_core(self, underlying: str) -> Decimal:
        """Get current spot price from trading-core"""
        spot_market_id = f"COMPUTE_SPOT_{underlying}_global"
        orderbook = await self.trading_core.get_orderbook(spot_market_id, depth=1)
        
        if orderbook and orderbook.get("bids") and orderbook.get("asks"):
            best_bid = Decimal(orderbook["bids"][0]["price"])
            best_ask = Decimal(orderbook["asks"][0]["price"])
            return (best_bid + best_ask) / 2
        else:
            # Fallback to oracle
            oracle_price = await self.oracle.get_aggregated_price(f"COMPUTE_{underlying}")
            return oracle_price.price if oracle_price else Decimal("10")
            
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
            
    async def _provision_surge_resources(
        self,
        derivative: BurstDerivative,
        activation: BurstActivation
    ):
        """Provision actual surge compute resources via trading-core"""
        
        # Request surge provisioning through trading-core
        result = await self.trading_core.submit_compute_order(
            user_id=f"BURST_ENGINE_{derivative.derivative_id}",
            resource_type=derivative.underlying,
            market_type="surge",
            quantity=str(activation.surge_capacity_allocated),
            specifications={
                "activation_id": activation.activation_id,
                "derivative_id": derivative.derivative_id,
                "priority": "urgent",
                "max_provisioning_time": "300",  # 5 minutes
                "surge_multiplier": str(derivative.surge_multiplier)
            }
        )
        
        if result.get("success"):
            # Update activation with provisioning details
            activation.performance_metrics["provisioning_result"] = result
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
                    # Get current metrics from trading-core
                    compute_metrics = await self.trading_core.get_compute_metrics()
                    
                    if resource_type in compute_metrics:
                        metrics = compute_metrics[resource_type]
                        
                        # Extract values
                        demand = Decimal(metrics.get("demand", "100"))
                        price = Decimal(metrics.get("spot_price", "10"))
                        capacity = Decimal(metrics.get("available_capacity", "1000"))
                        
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
                        current_value = await self._get_spot_price_from_trading_core(derivative.underlying)
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
        """Manage surge capacity pools"""
        while True:
            try:
                for pool in self.surge_pools.values():
                    # Update pricing based on utilization
                    await self._update_surge_pricing(pool)
                    
                    # Expand or contract pool based on demand
                    utilization = (pool.reserved_capacity + pool.active_capacity) / pool.total_capacity
                    
                    if utilization > Decimal("0.8"):
                        # High utilization - try to expand
                        await self._expand_surge_pool(pool, pool.total_capacity * Decimal("0.2"))
                    elif utilization < Decimal("0.2"):
                        # Low utilization - consider contracting
                        await self._contract_surge_pool(pool)
                        
                await asyncio.sleep(300)  # Every 5 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in surge management loop: {e}")
                await asyncio.sleep(600)
                
    async def _settlement_loop(self):
        """Settle completed bursts and expired derivatives"""
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
                        
                        # Trigger settlement in trading-core
                        market_id = await self._register_burst_market(derivative)
                        await self.trading_core.trigger_settlement(
                            market_id=market_id,
                            settlement_price=derivative.premium  # Use premium as settlement
                        )
                        
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
        
    # Helper methods remain largely the same but use trading-core for market data...
    
    async def _get_current_demand(self, resource_type: str) -> Decimal:
        """Get current demand for resource from trading-core metrics"""
        metrics = await self.trading_core.get_compute_metrics()
        if resource_type in metrics:
            return Decimal(metrics[resource_type].get("demand", "100"))
        return Decimal("100")
        
    async def _get_available_capacity(self, resource_type: str) -> Decimal:
        """Get available capacity from trading-core"""
        metrics = await self.trading_core.get_compute_metrics()
        if resource_type in metrics:
            return Decimal(metrics[resource_type].get("available_capacity", "1000"))
        return Decimal("1000")
        
    async def _get_capacity_cost(
        self,
        resource_type: str,
        capacity: Decimal
    ) -> Decimal:
        """Get cost for capacity"""
        price = await self._get_spot_price_from_trading_core(resource_type)
        return price * capacity
        
    async def _calculate_moneyness(
        self,
        underlying: str,
        strike_price: Decimal
    ) -> Decimal:
        """Calculate option moneyness"""
        spot_price = await self._get_spot_price_from_trading_core(underlying)
        return spot_price / strike_price if strike_price > 0 else Decimal("1")
        
    async def _get_surge_price(self, resource_type: str) -> Decimal:
        """Get surge pricing"""
        pool = self.surge_pools.get(resource_type)
        if not pool:
            return await self._get_spot_price_from_trading_core(resource_type) * Decimal("3")
            
        base_price = await self._get_spot_price_from_trading_core(resource_type)
        return base_price * pool.surge_price_multiplier
        
    # Additional helper methods...
    
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
        
    # Storage methods...
    
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
        pool.last_updated = datetime.utcnow()
        await self.ignite.put(f"surge_pool:{pool.pool_id}", pool)
        
    async def _load_active_derivatives(self):
        """Load active derivatives from cache"""
        # In production, scan cache for active derivatives
        pass
        
    async def _get_derivative_activations(self, derivative_id: str) -> List[BurstActivation]:
        """Get all activations for a derivative"""
        # In production, query from cache
        return []
        
    # Additional helper methods remain the same... 