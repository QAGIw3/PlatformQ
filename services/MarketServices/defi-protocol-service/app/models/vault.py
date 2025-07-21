"""
Vault Models

Data models for infrastructure vaults and strategies.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field
from decimal import Decimal

from platformq_shared.models import ResourceType


class StrategyType(str, Enum):
    """Types of vault strategies"""
    ARBITRAGE = "arbitrage"
    LENDING_OPTIMIZER = "lending_optimizer"
    FLASH_PROVISIONING = "flash_provisioning"
    HEDGED_MINING = "hedged_mining"
    MULTI_STRATEGY = "multi_strategy"


class Vault(BaseModel):
    """Vault information"""
    address: str
    resource_token_id: int
    name: str
    symbol: str
    total_assets: int
    total_debt: int
    price_per_share: int
    management_fee: int  # Basis points
    performance_fee: int  # Basis points
    created_at: datetime
    emergency_shutdown: bool = False
    last_report: Optional[datetime] = None


class VaultStrategy(BaseModel):
    """Vault strategy information"""
    address: str
    vault_address: str
    strategy_type: StrategyType
    debt_ratio: int  # Basis points
    total_debt: int
    total_gain: int
    total_loss: int
    last_report: datetime
    is_active: bool
    min_debt_per_harvest: Optional[int] = None
    max_debt_per_harvest: Optional[int] = None
    performance_fee: Optional[int] = None


class VaultDeposit(BaseModel):
    """Vault deposit record"""
    vault_address: str
    user_address: str
    amount: int
    shares: int
    timestamp: datetime
    price_per_share: Optional[int] = None


class VaultWithdrawal(BaseModel):
    """Vault withdrawal record"""
    vault_address: str
    user_address: str
    shares: int
    amount: int
    timestamp: datetime
    loss: Optional[int] = None


class StrategyReport(BaseModel):
    """Strategy harvest report"""
    strategy_address: str
    profit: int
    loss: int
    debt_payment: int
    total_debt: int
    timestamp: datetime
    gas_used: Optional[int] = None


# Request/Response Models

class CreateVaultRequest(BaseModel):
    """Request to create a vault"""
    resource_token_id: int = Field(..., description="Resource token ID the vault manages")
    name: str = Field(..., max_length=100)
    symbol: str = Field(..., max_length=10)
    management_fee: int = Field(default=200, ge=0, le=1000, description="Annual fee in basis points")
    performance_fee: int = Field(default=1000, ge=0, le=5000, description="Performance fee in basis points")


class AddStrategyRequest(BaseModel):
    """Request to add strategy to vault"""
    vault_address: str
    strategy_type: StrategyType
    strategy_config: Dict[str, Any] = Field(default_factory=dict)
    debt_ratio: int = Field(default=5000, ge=0, le=10000, description="Target allocation in basis points")
    min_debt_per_harvest: int = Field(default=0, ge=0)
    max_debt_per_harvest: int = Field(default=10**18, gt=0)


class DepositRequest(BaseModel):
    """Request to deposit into vault"""
    vault_address: str
    amount: int = Field(..., gt=0)


class WithdrawRequest(BaseModel):
    """Request to withdraw from vault"""
    vault_address: str
    shares: int = Field(..., gt=0)
    max_loss: int = Field(default=100, ge=0, le=10000, description="Max acceptable loss in basis points")


class UpdateStrategyRequest(BaseModel):
    """Request to update strategy parameters"""
    strategy_address: str
    debt_ratio: int = Field(..., ge=0, le=10000)
    min_debt_per_harvest: Optional[int] = None
    max_debt_per_harvest: Optional[int] = None


class HarvestRequest(BaseModel):
    """Request to harvest a strategy"""
    strategy_address: str


# Response Models

class VaultResponse(BaseModel):
    """Vault details response"""
    vault_address: str
    tx_hash: str
    name: str
    symbol: str
    resource_token_id: int


class StrategyResponse(BaseModel):
    """Strategy addition response"""
    strategy_address: str
    tx_hash: str
    strategy_type: str
    debt_ratio: int


class DepositResponse(BaseModel):
    """Deposit response"""
    tx_hash: str
    shares: int
    price_per_share: int
    value: int


class WithdrawResponse(BaseModel):
    """Withdrawal response"""
    tx_hash: str
    amount: int
    shares_burned: int
    loss: Optional[int] = None


class HarvestResponse(BaseModel):
    """Harvest response"""
    tx_hash: str
    profit: int
    loss: int
    apy: float


class VaultStats(BaseModel):
    """Vault statistics"""
    total_assets: int
    total_debt: int
    price_per_share: int
    total_shares: int
    tvl: int  # Total value locked
    apy: float
    active_strategies: int
    management_fee: int
    performance_fee: int
    emergency_shutdown: bool


class UserVaultBalance(BaseModel):
    """User's vault balance"""
    shares: int
    value: int
    price_per_share: int
    profit_loss: Optional[int] = None
    percentage_gain: Optional[float] = None


class StrategyDetails(BaseModel):
    """Detailed strategy information"""
    address: str
    name: str
    strategy_type: StrategyType
    debt_ratio: int
    total_debt: int
    total_gain: int
    total_loss: int
    estimated_apy: float
    last_report: datetime
    health_score: float  # 0-100


class VaultPerformance(BaseModel):
    """Vault performance metrics"""
    vault_address: str
    daily_apy: float
    weekly_apy: float
    monthly_apy: float
    yearly_apy: float
    total_returns: int
    strategy_allocations: Dict[str, float]  # strategy_type -> percentage


class ArbitrageConfig(BaseModel):
    """Configuration for arbitrage strategy"""
    min_profit_bps: int = Field(default=50, ge=10, le=1000)
    max_slippage_bps: int = Field(default=100, ge=10, le=1000)
    routes: List[Dict[str, Any]] = Field(default_factory=list)


class LendingOptimizerConfig(BaseModel):
    """Configuration for lending optimizer strategy"""
    rebalance_threshold_bps: int = Field(default=100, ge=10, le=1000)
    min_apy: int = Field(default=500, ge=0, description="Minimum APY in basis points")
    lending_ratio: int = Field(default=5000, ge=0, le=10000)
    staking_pool_id: int = Field(default=1, ge=1)


class FlashProvisioningConfig(BaseModel):
    """Configuration for flash provisioning strategy"""
    max_provision_amount: int = Field(..., gt=0)
    min_duration: int = Field(default=3600, ge=60)
    target_utilization: int = Field(default=8000, ge=5000, le=9500)


class MultiStrategyConfig(BaseModel):
    """Configuration for multi-strategy vault"""
    strategies: List[Dict[str, Any]] = Field(..., min_items=2)
    rebalance_frequency: int = Field(default=86400, ge=3600)
    risk_limit: int = Field(default=2000, ge=0, le=10000) 