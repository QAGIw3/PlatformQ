"""Structured products models."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, List, Union, Dict, Any
from dataclasses import dataclass, field
from pydantic import BaseModel, Field


class ProductType(Enum):
    """Types of structured products."""
    AUTOCALLABLE = "autocallable"
    REVERSE_CONVERTIBLE = "reverse_convertible"
    RANGE_ACCRUAL = "range_accrual"
    BARRIER_NOTE = "barrier_note"
    DUAL_CURRENCY = "dual_currency"
    VOLATILITY_TARGET = "volatility_target"
    ACCUMULATOR = "accumulator"


class BarrierType(Enum):
    """Types of barriers."""
    EUROPEAN = "european"  # Observed only at maturity
    AMERICAN = "american"  # Continuously observed
    DISCRETE = "discrete"  # Observed at specific dates


class SettlementType(Enum):
    """Settlement types."""
    CASH = "cash"
    PHYSICAL = "physical"
    HYBRID = "hybrid"  # Issuer's choice


@dataclass
class BarrierObservation:
    """Observation for barrier events."""
    date: datetime
    underlying_price: Decimal
    barrier_breached: bool
    autocall_triggered: bool = False
    coupon_paid: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "date": self.date.isoformat(),
            "underlying_price": str(self.underlying_price),
            "barrier_breached": self.barrier_breached,
            "autocall_triggered": self.autocall_triggered,
            "coupon_paid": self.coupon_paid
        }


@dataclass
class StructuredProduct:
    """Base structured product."""
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
    
    # Metadata
    issuer_id: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class AutocallableNote(StructuredProduct):
    """Autocallable structured note."""
    # Autocall levels (as % of initial)
    autocall_levels: List[Decimal]  # e.g., [1.0, 0.95, 0.90] for each observation
    autocall_coupon: Decimal  # Annual rate paid on autocall
    
    # Barrier
    barrier_level: Decimal  # e.g., 0.7 for 70% barrier
    barrier_type: BarrierType = BarrierType.EUROPEAN
    
    # Coupon
    coupon_rate: Decimal = Decimal("0")  # Regular coupon if not autocalled
    memory_coupon: bool = True  # Accumulate missed coupons
    
    # Settlement
    settlement_type: SettlementType = SettlementType.CASH


@dataclass
class ReverseConvertible(StructuredProduct):
    """Reverse convertible note."""
    strike_price: Decimal
    coupon_rate: Decimal  # Annual guaranteed coupon
    barrier_level: Optional[Decimal] = None  # Optional knock-in barrier
    
    # Conversion
    conversion_ratio: Decimal = Decimal("1")  # Shares per note
    worst_of_basket: bool = False  # For basket underlyings
    
    # Settlement
    settlement_type: SettlementType = SettlementType.PHYSICAL


@dataclass
class RangeAccrual(StructuredProduct):
    """Range accrual note."""
    lower_bound: Decimal
    upper_bound: Decimal
    daily_accrual_rate: Decimal  # Daily rate when in range
    
    # Tracking
    days_in_range: int = 0
    total_observation_days: int = 0
    accrued_coupon: Decimal = Decimal("0")
    
    # Capital protection
    protection_level: Decimal = Decimal("1.0")  # % of capital protected


@dataclass
class AccumulatorProduct(StructuredProduct):
    """Accumulator (decumulator) product."""
    strike_price: Decimal  # Accumulation price
    knock_out_level: Decimal  # Terminates if reached
    
    # Accumulation terms
    accumulation_frequency: str = "daily"  # daily, weekly, monthly
    base_quantity: Decimal = Decimal("1")  # Base accumulation amount
    leverage: Decimal = Decimal("2")  # Leverage below strike
    
    # Limits
    max_accumulation: Decimal = Decimal("0")  # Maximum total accumulation
    accumulated_quantity: Decimal = Decimal("0")  # Current accumulated
    
    # Status
    is_knocked_out: bool = False


@dataclass
class VolatilityTargetNote(StructuredProduct):
    """Volatility target structured note."""
    target_volatility: Decimal  # Target vol level
    
    # Exposure management
    current_exposure: Decimal = Decimal("1.0")  # Current leverage
    max_exposure: Decimal = Decimal("1.5")  # Maximum leverage
    min_exposure: Decimal = Decimal("0")  # Minimum leverage
    
    # Rebalancing
    rebalance_frequency: str = "daily"  # daily, weekly
    last_rebalance: Optional[datetime] = None
    
    # Performance
    participation_cap: Optional[Decimal] = None  # Cap on upside
    
    
# Pydantic models for API

class ProductPricing(BaseModel):
    """Pricing information for structured product."""
    fair_value: Decimal
    discount_to_par: Decimal
    probability_autocall: Optional[Decimal] = None
    probability_knock_in: Optional[Decimal] = None
    probability_conversion: Optional[Decimal] = None
    expected_return: Decimal
    max_loss: Optional[Decimal] = None
    
    # Greeks (if applicable)
    delta: Optional[Decimal] = None
    gamma: Optional[Decimal] = None
    vega: Optional[Decimal] = None
    theta: Optional[Decimal] = None
    rho: Optional[Decimal] = None


class ProductValuation(BaseModel):
    """Current valuation of a structured product."""
    product_id: str
    current_value: Decimal
    mark_to_market: Decimal
    unrealized_pnl: Decimal
    accrued_coupon: Decimal
    
    # Risk metrics
    current_exposure: Decimal
    potential_loss: Decimal
    
    # Status
    days_to_maturity: int
    next_observation_date: Optional[datetime] = None
    
    
class ObservationResult(BaseModel):
    """Result of a barrier observation."""
    product_id: str
    observation_date: datetime
    underlying_price: Decimal
    
    # Barrier events
    barrier_breached: bool
    autocall_triggered: bool
    coupon_paid: bool
    
    # Updated status
    product_status: str  # active, knocked_out, matured
    payoff_amount: Optional[Decimal] = None
    
    
class RiskMetrics(BaseModel):
    """Risk metrics for a structured product."""
    # Greeks
    delta: Decimal
    gamma: Decimal
    vega: Decimal
    theta: Decimal
    rho: Decimal
    
    # Scenario analysis
    spot_up_10: Decimal  # P&L if spot up 10%
    spot_down_10: Decimal  # P&L if spot down 10%
    vol_up_5: Decimal  # P&L if vol up 5%
    time_decay_30d: Decimal  # P&L from 30d time decay
    
    # Risk measures
    max_loss: Decimal
    value_at_risk_95: Decimal  # 95% VaR
    expected_shortfall_95: Decimal  # 95% CVaR
    
    
class ProductOrder(BaseModel):
    """Order for structured product."""
    order_id: str
    product_id: str
    user_id: str
    
    # Order details
    notional: Decimal
    price: Decimal  # As % of notional
    side: str = Field(..., pattern="^(buy|sell)$")
    
    # Status
    status: str = "pending"  # pending, filled, cancelled
    filled_notional: Decimal = Decimal("0")
    average_price: Optional[Decimal] = None
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    filled_at: Optional[datetime] = None
    
    
class ProductPosition(BaseModel):
    """User's position in structured product."""
    position_id: str
    user_id: str
    product_id: str
    
    # Position details
    notional: Decimal
    entry_price: Decimal  # As % of notional
    current_price: Decimal
    
    # P&L
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    accrued_coupon: Decimal
    
    # Dates
    created_at: datetime
    updated_at: datetime 