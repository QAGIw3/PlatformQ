"""
Structured Products Engine

Implements complex structured products like autocallables, reverse convertibles, etc.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any, Union
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
import logging

from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient
from app.engines.pricing import BlackScholesEngine, OptionParameters
from app.models.market import Market

logger = logging.getLogger(__name__)


class ProductType(Enum):
    """Types of structured products"""
    AUTOCALLABLE = "autocallable"
    REVERSE_CONVERTIBLE = "reverse_convertible"
    RANGE_ACCRUAL = "range_accrual"
    BARRIER_NOTE = "barrier_note"
    DUAL_CURRENCY = "dual_currency"
    VOLATILITY_TARGET = "volatility_target"
    ACCUMULATOR = "accumulator"


@dataclass
class BarrierObservation:
    """Observation for barrier events"""
    date: datetime
    underlying_price: Decimal
    barrier_breached: bool
    autocall_triggered: bool = False
    coupon_paid: bool = False


@dataclass
class StructuredProduct:
    """Base structured product"""
    product_id: str
    product_type: ProductType
    underlying: Union[str, List[str]]  # Single or basket
    notional: Decimal
    currency: str
    
    # Dates
    issue_date: datetime
    maturity_date: datetime
    observation_dates: List[datetime]
    
    # Pricing
    initial_price: Union[Decimal, List[Decimal]]
    current_price: Union[Decimal, List[Decimal]]
    
    # Terms
    participation_rate: Decimal = Decimal("1.0")
    protection_level: Decimal = Decimal("1.0")  # 1.0 = 100% capital protection
    
    # Status
    is_active: bool = True
    is_knocked_out: bool = False
    final_payoff: Optional[Decimal] = None
    
    # Observations
    observations: List[BarrierObservation] = field(default_factory=list)


@dataclass
class AutocallableNote(StructuredProduct):
    """Autocallable structured note"""
    # Autocall levels (as % of initial)
    autocall_levels: List[Decimal]  # e.g., [1.0, 0.95, 0.90] for each observation
    autocall_coupon: Decimal  # Annual rate
    
    # Barrier
    barrier_level: Decimal  # e.g., 0.7 for 70% barrier
    barrier_type: str = "european"  # "european" or "american"
    
    # Coupon
    coupon_rate: Decimal = Decimal("0")  # Regular coupon if not autocalled
    memory_coupon: bool = True  # Accumulate missed coupons


@dataclass
class ReverseConvertible(StructuredProduct):
    """Reverse convertible note"""
    strike_price: Decimal
    coupon_rate: Decimal  # Annual guaranteed coupon
    barrier_level: Optional[Decimal] = None  # Optional knock-in barrier
    
    # Conversion
    conversion_ratio: Decimal = Decimal("1")  # Shares per note
    worst_of_basket: bool = False  # For basket underlyings


@dataclass
class RangeAccrual(StructuredProduct):
    """Range accrual note"""
    lower_bound: Decimal
    upper_bound: Decimal
    accrual_rate: Decimal  # Daily accrual rate when in range
    
    # Tracking
    days_in_range: int = 0
    total_observation_days: int = 0


class StructuredProductEngine:
    """
    Engine for creating and managing structured products
    """
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        oracle: OracleAggregatorClient,
        pricing_engine: BlackScholesEngine
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        self.pricing_engine = pricing_engine
        
        # Active products
        self.products: Dict[str, StructuredProduct] = {}
        
        # Risk limits
        self.max_notional_per_product = Decimal("50000000")  # $50M
        self.max_total_issuance = Decimal("500000000")  # $500M
        
    async def start(self):
        """Start the structured products engine"""
        asyncio.create_task(self._observation_loop())
        asyncio.create_task(self._settlement_loop())
        logger.info("Structured products engine started")
        
    async def create_autocallable(
        self,
        underlying: str,
        notional: Decimal,
        maturity_months: int,
        autocall_levels: List[Decimal],
        barrier_level: Decimal,
        coupon_rate: Decimal,
        issuer_id: str
    ) -> AutocallableNote:
        """Create an autocallable note"""
        # Validate
        if notional > self.max_notional_per_product:
            raise ValueError(f"Notional exceeds limit: {self.max_notional_per_product}")
            
        # Get initial price
        initial_price = await self.oracle.get_price(underlying)
        if not initial_price:
            raise ValueError(f"Cannot get price for {underlying}")
            
        # Create observation schedule (monthly)
        observation_dates = []
        for i in range(1, maturity_months + 1):
            obs_date = datetime.utcnow() + timedelta(days=30 * i)
            observation_dates.append(obs_date)
            
        # Ensure autocall levels match observations
        if len(autocall_levels) != len(observation_dates):
            autocall_levels = [autocall_levels[0]] * len(observation_dates)
            
        # Create product
        product = AutocallableNote(
            product_id=f"AC_{underlying}_{datetime.utcnow().timestamp()}",
            product_type=ProductType.AUTOCALLABLE,
            underlying=underlying,
            notional=notional,
            currency="USD",
            issue_date=datetime.utcnow(),
            maturity_date=observation_dates[-1],
            observation_dates=observation_dates,
            initial_price=initial_price,
            current_price=initial_price,
            autocall_levels=autocall_levels,
            autocall_coupon=coupon_rate,
            barrier_level=barrier_level,
            coupon_rate=coupon_rate
        )
        
        self.products[product.product_id] = product
        
        # Calculate initial price (discount for embedded options)
        product_price = await self._price_autocallable(product)
        
        # Emit creation event
        await self.pulsar.publish(
            "structured-product-created",
            {
                "product_id": product.product_id,
                "type": "autocallable",
                "underlying": underlying,
                "notional": str(notional),
                "price": str(product_price),
                "maturity_months": maturity_months,
                "issuer": issuer_id
            }
        )
        
        return product
        
    async def create_reverse_convertible(
        self,
        underlying: Union[str, List[str]],
        notional: Decimal,
        maturity_months: int,
        strike_percent: Decimal,  # e.g., 0.9 for 90% strike
        coupon_rate: Decimal,
        barrier_level: Optional[Decimal] = None,
        issuer_id: str = ""
    ) -> ReverseConvertible:
        """Create a reverse convertible note"""
        # Handle basket
        if isinstance(underlying, list):
            initial_prices = []
            for asset in underlying:
                price = await self.oracle.get_price(asset)
                if not price:
                    raise ValueError(f"Cannot get price for {asset}")
                initial_prices.append(price)
            initial_price = initial_prices
            strike_price = min(initial_prices) * strike_percent
            worst_of = True
        else:
            initial_price = await self.oracle.get_price(underlying)
            if not initial_price:
                raise ValueError(f"Cannot get price for {underlying}")
            strike_price = initial_price * strike_percent
            worst_of = False
            
        # Create product
        product = ReverseConvertible(
            product_id=f"RC_{datetime.utcnow().timestamp()}",
            product_type=ProductType.REVERSE_CONVERTIBLE,
            underlying=underlying,
            notional=notional,
            currency="USD",
            issue_date=datetime.utcnow(),
            maturity_date=datetime.utcnow() + timedelta(days=30 * maturity_months),
            observation_dates=[datetime.utcnow() + timedelta(days=30 * maturity_months)],
            initial_price=initial_price,
            current_price=initial_price,
            strike_price=strike_price,
            coupon_rate=coupon_rate,
            barrier_level=barrier_level,
            worst_of_basket=worst_of
        )
        
        self.products[product.product_id] = product
        
        # Price the product
        product_price = await self._price_reverse_convertible(product)
        
        await self.pulsar.publish(
            "structured-product-created",
            {
                "product_id": product.product_id,
                "type": "reverse_convertible",
                "underlying": underlying,
                "notional": str(notional),
                "price": str(product_price),
                "coupon_rate": str(coupon_rate),
                "strike": str(strike_price)
            }
        )
        
        return product
        
    async def create_range_accrual(
        self,
        underlying: str,
        notional: Decimal,
        maturity_months: int,
        lower_bound_percent: Decimal,  # e.g., 0.9 for 90% of initial
        upper_bound_percent: Decimal,  # e.g., 1.1 for 110% of initial
        daily_accrual_rate: Decimal,
        issuer_id: str = ""
    ) -> RangeAccrual:
        """Create a range accrual note"""
        initial_price = await self.oracle.get_price(underlying)
        if not initial_price:
            raise ValueError(f"Cannot get price for {underlying}")
            
        # Daily observations
        observation_dates = []
        maturity_date = datetime.utcnow() + timedelta(days=30 * maturity_months)
        current_date = datetime.utcnow() + timedelta(days=1)
        
        while current_date <= maturity_date:
            observation_dates.append(current_date)
            current_date += timedelta(days=1)
            
        product = RangeAccrual(
            product_id=f"RA_{underlying}_{datetime.utcnow().timestamp()}",
            product_type=ProductType.RANGE_ACCRUAL,
            underlying=underlying,
            notional=notional,
            currency="USD",
            issue_date=datetime.utcnow(),
            maturity_date=maturity_date,
            observation_dates=observation_dates,
            initial_price=initial_price,
            current_price=initial_price,
            lower_bound=initial_price * lower_bound_percent,
            upper_bound=initial_price * upper_bound_percent,
            accrual_rate=daily_accrual_rate
        )
        
        self.products[product.product_id] = product
        
        return product
        
    async def _observation_loop(self):
        """Monitor products for barrier/observation events"""
        while True:
            try:
                current_time = datetime.utcnow()
                
                for product_id, product in self.products.items():
                    if product.is_active:
                        await self._check_observations(product, current_time)
                        
            except Exception as e:
                logger.error(f"Error in observation loop: {e}")
                
            await asyncio.sleep(60)  # Check every minute
            
    async def _check_observations(self, product: StructuredProduct, current_time: datetime):
        """Check if any observations are due"""
        # Update current price
        if isinstance(product.underlying, list):
            current_prices = []
            for asset in product.underlying:
                price = await self.oracle.get_price(asset)
                if price:
                    current_prices.append(price)
            if current_prices:
                product.current_price = current_prices
        else:
            price = await self.oracle.get_price(product.underlying)
            if price:
                product.current_price = price
                
        # Check for observations
        for obs_date in product.observation_dates:
            if abs((obs_date - current_time).total_seconds()) < 3600:  # Within 1 hour
                await self._process_observation(product, obs_date)
                
    async def _process_observation(self, product: StructuredProduct, obs_date: datetime):
        """Process an observation date"""
        if isinstance(product, AutocallableNote):
            await self._process_autocallable_observation(product, obs_date)
        elif isinstance(product, RangeAccrual):
            await self._process_range_accrual_observation(product, obs_date)
            
    async def _process_autocallable_observation(
        self,
        product: AutocallableNote,
        obs_date: datetime
    ):
        """Process autocallable observation"""
        obs_index = product.observation_dates.index(obs_date)
        autocall_level = product.autocall_levels[obs_index]
        
        # Get current price
        current_price = product.current_price
        if isinstance(current_price, list):
            current_price = min(current_price)  # Worst-of
            
        # Check autocall trigger
        autocall_price = product.initial_price * autocall_level
        
        observation = BarrierObservation(
            date=obs_date,
            underlying_price=current_price,
            barrier_breached=current_price < product.initial_price * product.barrier_level,
            autocall_triggered=current_price >= autocall_price
        )
        
        product.observations.append(observation)
        
        if observation.autocall_triggered:
            # Autocall triggered - product terminates
            product.is_knocked_out = True
            product.is_active = False
            
            # Calculate final payoff
            months_elapsed = obs_index + 1
            coupon = product.notional * product.autocall_coupon * Decimal(months_elapsed) / 12
            product.final_payoff = product.notional + coupon
            
            await self.pulsar.publish(
                "autocallable-knocked-out",
                {
                    "product_id": product.product_id,
                    "observation_date": obs_date.isoformat(),
                    "knock_out_level": str(autocall_price),
                    "current_price": str(current_price),
                    "final_payoff": str(product.final_payoff)
                }
            )
            
    async def _process_range_accrual_observation(
        self,
        product: RangeAccrual,
        obs_date: datetime
    ):
        """Process range accrual observation"""
        current_price = product.current_price
        
        # Check if in range
        in_range = product.lower_bound <= current_price <= product.upper_bound
        
        product.total_observation_days += 1
        if in_range:
            product.days_in_range += 1
            
        # Update product
        observation = BarrierObservation(
            date=obs_date,
            underlying_price=current_price,
            barrier_breached=not in_range
        )
        
        product.observations.append(observation)
        
    async def _settlement_loop(self):
        """Process matured products"""
        while True:
            try:
                current_time = datetime.utcnow()
                
                for product_id, product in list(self.products.items()):
                    if product.is_active and current_time >= product.maturity_date:
                        await self._settle_product(product)
                        
            except Exception as e:
                logger.error(f"Error in settlement loop: {e}")
                
            await asyncio.sleep(3600)  # Check hourly
            
    async def _settle_product(self, product: StructuredProduct):
        """Settle a matured product"""
        if isinstance(product, AutocallableNote):
            await self._settle_autocallable(product)
        elif isinstance(product, ReverseConvertible):
            await self._settle_reverse_convertible(product)
        elif isinstance(product, RangeAccrual):
            await self._settle_range_accrual(product)
            
        product.is_active = False
        
    async def _settle_autocallable(self, product: AutocallableNote):
        """Settle autocallable at maturity"""
        if product.is_knocked_out:
            return  # Already settled
            
        # Check final barrier
        final_price = product.current_price
        if isinstance(final_price, list):
            final_price = min(final_price)
            
        barrier_breached = any(obs.barrier_breached for obs in product.observations)
        
        if barrier_breached and product.barrier_type == "american":
            # Barrier breached - loss of capital
            loss_percent = (product.initial_price - final_price) / product.initial_price
            product.final_payoff = product.notional * (1 - loss_percent)
        else:
            # Return principal + final coupon
            product.final_payoff = product.notional + (product.notional * product.coupon_rate * len(product.observation_dates) / 12)
            
        await self.pulsar.publish(
            "structured-product-settled",
            {
                "product_id": product.product_id,
                "type": "autocallable",
                "final_payoff": str(product.final_payoff),
                "barrier_breached": barrier_breached
            }
        )
        
    async def _settle_reverse_convertible(self, product: ReverseConvertible):
        """Settle reverse convertible"""
        # Always pay coupon
        coupon = product.notional * product.coupon_rate * (product.maturity_date - product.issue_date).days / 365
        
        # Check conversion
        final_price = product.current_price
        if isinstance(final_price, list):
            final_price = min(final_price)  # Worst-of
            
        if final_price < product.strike_price:
            # Convert to underlying
            if product.barrier_level and final_price > product.initial_price * product.barrier_level:
                # Barrier not breached - return principal
                product.final_payoff = product.notional + coupon
            else:
                # Deliver shares or cash equivalent
                share_value = final_price * product.conversion_ratio * (product.notional / product.strike_price)
                product.final_payoff = share_value + coupon
        else:
            # Return principal + coupon
            product.final_payoff = product.notional + coupon
            
        await self.pulsar.publish(
            "structured-product-settled",
            {
                "product_id": product.product_id,
                "type": "reverse_convertible",
                "final_payoff": str(product.final_payoff),
                "converted": final_price < product.strike_price
            }
        )
        
    async def _settle_range_accrual(self, product: RangeAccrual):
        """Settle range accrual"""
        # Calculate accrued interest
        accrual_ratio = Decimal(product.days_in_range) / Decimal(product.total_observation_days)
        max_accrual = product.accrual_rate * product.maturity_date.year  # Annualized
        
        interest = product.notional * max_accrual * accrual_ratio
        product.final_payoff = product.notional + interest
        
        await self.pulsar.publish(
            "structured-product-settled",
            {
                "product_id": product.product_id,
                "type": "range_accrual",
                "final_payoff": str(product.final_payoff),
                "days_in_range": product.days_in_range,
                "total_days": product.total_observation_days,
                "accrual_ratio": str(accrual_ratio)
            }
        )
        
    async def _price_autocallable(self, product: AutocallableNote) -> Decimal:
        """Price an autocallable note using Monte Carlo"""
        # Simplified pricing - would use full Monte Carlo in practice
        spot = float(product.initial_price)
        vol = 0.3  # 30% vol assumption
        rate = 0.05
        
        # Discount for knock-out probability
        knock_out_prob = 0.3  # Simplified
        
        # Expected coupons
        expected_coupons = float(product.coupon_rate) * (1 - knock_out_prob)
        
        # Discount for downside risk
        downside_cost = 0.1 * (1 - float(product.barrier_level))
        
        # Fair value as percentage of notional
        fair_value = 1.0 - downside_cost + expected_coupons
        
        return product.notional * Decimal(str(fair_value))
        
    async def _price_reverse_convertible(self, product: ReverseConvertible) -> Decimal:
        """Price a reverse convertible"""
        # Price = Bond - Put Option
        time_to_maturity = (product.maturity_date - product.issue_date).days / 365
        
        # Bond value
        bond_value = product.notional * (1 + float(product.coupon_rate) * time_to_maturity)
        
        # Put option value (simplified)
        spot = float(product.initial_price)
        strike = float(product.strike_price)
        vol = 0.3
        rate = 0.05
        
        option_params = OptionParameters(
            spot=Decimal(str(spot)),
            strike=Decimal(str(strike)),
            time_to_expiry=Decimal(str(time_to_maturity)),
            volatility=Decimal(str(vol)),
            risk_free_rate=Decimal(str(rate)),
            is_call=False
        )
        
        put_value = self.pricing_engine.calculate_option_price(option_params)
        
        # Fair value
        fair_value = Decimal(str(bond_value)) - put_value
        
        return fair_value
        
    async def get_product_valuation(self, product_id: str) -> Dict[str, Any]:
        """Get current valuation of a product"""
        product = self.products.get(product_id)
        if not product:
            return None
            
        # Update pricing
        if isinstance(product, AutocallableNote):
            current_value = await self._price_autocallable(product)
        elif isinstance(product, ReverseConvertible):
            current_value = await self._price_reverse_convertible(product)
        else:
            current_value = product.notional  # Simplified
            
        return {
            "product_id": product_id,
            "product_type": product.product_type.value,
            "notional": str(product.notional),
            "current_value": str(current_value),
            "pnl": str(current_value - product.notional),
            "is_active": product.is_active,
            "days_to_maturity": (product.maturity_date - datetime.utcnow()).days
        } 