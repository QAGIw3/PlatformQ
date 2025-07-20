"""Pricing models for compute resources."""

from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

from .compute_resource import ResourceType


class PriceQuote(BaseModel):
    """Price quote for resource allocation."""
    quote_id: str
    resource_type: ResourceType
    quantity: Decimal
    
    # Pricing
    base_price_per_hour: Decimal
    spot_price_per_hour: Optional[Decimal] = None
    reserved_price_per_hour: Optional[Decimal] = None
    
    # Duration pricing
    hourly_cost: Decimal
    daily_cost: Decimal
    weekly_cost: Decimal
    monthly_cost: Decimal
    
    # Discounts
    volume_discount: Decimal = Decimal("0")
    term_discount: Decimal = Decimal("0")
    total_discount: Decimal = Decimal("0")
    
    # Additional costs
    setup_fee: Decimal = Decimal("0")
    data_transfer_cost: Decimal = Decimal("0")
    
    # Total
    total_estimated_cost: Decimal
    
    # Validity
    valid_until: datetime
    region: str
    qos_level: str
    
    # Provider info
    provider_id: Optional[str] = None
    provider_name: Optional[str] = None
    
    class Config:
        use_enum_values = True
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class SpotPrice(BaseModel):
    """Spot market price for a resource."""
    resource_type: ResourceType
    region: str
    
    # Current price
    current_price: Decimal
    previous_price: Decimal
    price_change: Decimal
    price_change_percent: Decimal
    
    # Statistics
    avg_price_1h: Decimal
    avg_price_24h: Decimal
    avg_price_7d: Decimal
    
    # Volatility
    volatility_1h: Decimal
    volatility_24h: Decimal
    
    # Supply/Demand
    available_capacity: Decimal
    total_demand: Decimal
    utilization_rate: Decimal
    
    # Forecast
    predicted_price_1h: Optional[Decimal] = None
    predicted_price_24h: Optional[Decimal] = None
    confidence_interval: Optional[Dict[str, Decimal]] = None
    
    # Timestamp
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class MarketPrice(BaseModel):
    """Market-wide pricing information."""
    market_id: str
    
    # Spot prices by resource type
    spot_prices: Dict[str, SpotPrice]
    
    # Reserved pricing
    reserved_discounts: Dict[str, Decimal]  # By term length
    
    # Market stats
    total_volume_24h: Decimal
    total_transactions_24h: int
    avg_allocation_duration_hours: Decimal
    
    # Top movers
    biggest_gainers: List[Dict[str, Any]]
    biggest_losers: List[Dict[str, Any]]
    
    # Market sentiment
    market_sentiment: str  # bullish, bearish, neutral
    supply_demand_ratio: Decimal
    
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class PricingHistory(BaseModel):
    """Historical pricing data."""
    resource_type: ResourceType
    region: str
    time_period: str  # 1h, 24h, 7d, 30d
    
    # Price points
    timestamps: List[datetime]
    prices: List[Decimal]
    volumes: List[Decimal]
    
    # Statistics
    high: Decimal
    low: Decimal
    open: Decimal
    close: Decimal
    avg: Decimal
    std_dev: Decimal
    
    # Trends
    trend: str  # up, down, stable
    trend_strength: Decimal  # 0-1
    
    class Config:
        use_enum_values = True
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class DynamicPricing(BaseModel):
    """Dynamic pricing configuration."""
    resource_type: ResourceType
    region: str
    
    # Base pricing
    base_price: Decimal
    min_price: Decimal
    max_price: Decimal
    
    # Factors
    demand_weight: Decimal = Field(default=Decimal("0.4"), ge=0, le=1)
    supply_weight: Decimal = Field(default=Decimal("0.3"), ge=0, le=1)
    time_weight: Decimal = Field(default=Decimal("0.2"), ge=0, le=1)
    competitor_weight: Decimal = Field(default=Decimal("0.1"), ge=0, le=1)
    
    # Adjustments
    peak_hours_multiplier: Decimal = Decimal("1.5")
    off_peak_discount: Decimal = Decimal("0.3")
    
    # Update frequency
    update_interval_seconds: int = 60
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    
    # Rules
    max_price_change_per_update: Decimal = Decimal("0.1")
    smoothing_factor: Decimal = Decimal("0.7")
    
    class Config:
        use_enum_values = True
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        } 