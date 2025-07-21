"""
Infrastructure DeFi models

Models for infrastructure resource tokenization and lending.
"""

from enum import Enum
from typing import Optional
from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel, Field


class ResourceType(str, Enum):
    """Types of infrastructure resources"""
    CPU = "CPU"
    GPU = "GPU"
    STORAGE = "STORAGE"
    BANDWIDTH = "BANDWIDTH"
    MEMORY = "MEMORY"


class ServiceTier(str, Enum):
    """Service quality tiers"""
    STANDARD = "STANDARD"
    PREMIUM = "PREMIUM"
    GUARANTEED = "GUARANTEED"


class ResourceSpec(BaseModel):
    """Specification for a resource token"""
    resource_type: ResourceType
    service_tier: ServiceTier
    region: str
    amount: int = Field(..., gt=0)
    provider: str
    valid_until: datetime
    sla_hash: Optional[str] = None
    metadata_uri: Optional[str] = None


class ResourceLoan(BaseModel):
    """Infrastructure-backed loan"""
    loan_id: int
    borrower: str
    resource_token_id: int
    collateral_amount: int
    collateral_value: Decimal
    loan_amount: Decimal
    interest_rate: Decimal
    total_due: Decimal
    start_time: datetime
    end_time: datetime
    status: str
    payment_token: str
    chain_id: int
    tx_hash: str


class ResourceValuation(BaseModel):
    """Valuation of resource tokens"""
    resource_type: ResourceType
    service_tier: ServiceTier
    region: str
    amount: int
    base_price_per_unit: Decimal
    time_decay_factor: Decimal
    total_value: Decimal
    max_loan_amount: Decimal
    ltv_ratio: Decimal
    days_until_expiry: int


class AMMPool(BaseModel):
    """AMM pool for resource tokens"""
    pool_id: int
    resource_token_id: int
    payment_token: str
    resource_reserves: int
    payment_reserves: Decimal
    total_lp_tokens: Decimal
    fee_rate: int  # Basis points
    k_constant: Decimal
    volume_24h: Decimal
    fees_24h: Decimal
    apy: Decimal


class LiquidityPosition(BaseModel):
    """User's liquidity position in AMM pool"""
    pool_id: int
    user: str
    lp_tokens: Decimal
    resource_amount: int
    payment_amount: Decimal
    share_percentage: Decimal
    unclaimed_fees: Decimal


class ChainId(int, Enum):
    """Supported blockchain networks"""
    ETHEREUM = 1
    POLYGON = 137
    ARBITRUM = 42161
    OPTIMISM = 10
    BSC = 56
    AVALANCHE = 43114 