"""
DeFi Protocol Service data models.
"""

from .auction import Auction, Bid, AuctionType, AuctionStatus
from .lending import Loan, LoanOffer, LoanStatus, CollateralType, LiquidationEvent
from .insurance import (
    RiskTier, ClaimStatus, PoolTier, StakePosition, 
    InsuranceClaim, DeficitEvent, InsuranceMetrics
)
from .infrastructure import (
    ResourceType, ServiceTier, ResourceSpec, ResourceLoan,
    ResourceValuation, AMMPool, LiquidityPosition, ChainId
)
from .staking import (
    StakeStatus, StakingPool, DelegationPool, UserStake,
    CreateStakingPoolRequest, CreateDelegationPoolRequest, StakeRequest,
    DelegateStakeRequest, WithdrawRequest as StakeWithdrawRequest,
    ClaimRewardsRequest, AutoCompoundRequest, ExecuteCompoundRequest,
    UpdateDelegationFeeRequest, StakingPoolResponse, DelegationPoolInfo,
    UserStakeResponse, StakingStats, StakeResponse, DelegateResponse,
    WithdrawResponse as StakeWithdrawResponse, ClaimRewardsResponse,
    CompoundResponse, SlashingEvent
)
from .vault import (
    StrategyType, Vault, VaultStrategy, VaultDeposit, VaultWithdrawal,
    StrategyReport, CreateVaultRequest, AddStrategyRequest, DepositRequest,
    WithdrawRequest as VaultWithdrawRequest, UpdateStrategyRequest,
    HarvestRequest, VaultResponse, StrategyResponse, DepositResponse,
    WithdrawResponse as VaultWithdrawResponse, HarvestResponse,
    VaultStats, UserVaultBalance, StrategyDetails, VaultPerformance,
    ArbitrageConfig, LendingOptimizerConfig, FlashProvisioningConfig,
    MultiStrategyConfig
)

__all__ = [
    "Auction", "Bid", "AuctionType", "AuctionStatus",
    "Loan", "LoanOffer", "LoanStatus", "CollateralType", "LiquidationEvent",
    "RiskTier", "ClaimStatus", "PoolTier", "StakePosition",
    "InsuranceClaim", "DeficitEvent", "InsuranceMetrics",
    "ResourceType", "ServiceTier", "ResourceSpec", "ResourceLoan",
    "ResourceValuation", "AMMPool", "LiquidityPosition", "ChainId",
    # Staking models
    "StakeStatus", "StakingPool", "DelegationPool", "UserStake",
    "CreateStakingPoolRequest", "CreateDelegationPoolRequest", "StakeRequest",
    "DelegateStakeRequest", "StakeWithdrawRequest", "ClaimRewardsRequest",
    "AutoCompoundRequest", "ExecuteCompoundRequest", "UpdateDelegationFeeRequest",
    "StakingPoolResponse", "DelegationPoolInfo", "UserStakeResponse",
    "StakingStats", "StakeResponse", "DelegateResponse", "StakeWithdrawResponse",
    "ClaimRewardsResponse", "CompoundResponse", "SlashingEvent",
    # Vault models
    "StrategyType", "Vault", "VaultStrategy", "VaultDeposit", "VaultWithdrawal",
    "StrategyReport", "CreateVaultRequest", "AddStrategyRequest", "DepositRequest",
    "VaultWithdrawRequest", "UpdateStrategyRequest", "HarvestRequest",
    "VaultResponse", "StrategyResponse", "DepositResponse", "VaultWithdrawResponse",
    "HarvestResponse", "VaultStats", "UserVaultBalance", "StrategyDetails",
    "VaultPerformance", "ArbitrageConfig", "LendingOptimizerConfig",
    "FlashProvisioningConfig", "MultiStrategyConfig"
]
