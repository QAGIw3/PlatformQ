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
    VaultInfo, StrategyInfo, UserDepositInfo, VaultStats, StrategyPerformance,
    PerformanceReport, VaultEmergencyShutdown
)
from .derivatives import (
    OptionType, OptionStyle, PositionSide, Option, OptionGreeks,
    OptionOrder, PerpetualPosition, PerpetualMarket, FundingRate,
    PerpetualOrder, OptionsPool, LiquidityPosition, WriteOptionRequest,
    BuyOptionRequest, ExerciseOptionRequest, OpenPerpetualRequest,
    ClosePerpetualRequest, AddMarginRequest, CreateOptionsPoolRequest,
    AddOptionsLiquidityRequest, RemoveOptionsLiquidityRequest,
    OptionResponse, ExerciseResponse, PerpetualPositionResponse,
    PositionInfoResponse, GreeksResponse, OptionsPoolResponse,
    OptionPremiumQuote, MarketDataResponse, DerivativesStats,
    LiquidationEvent
)

__all__ = [
    # Auction models
    "Auction", "Bid", "AuctionType", "AuctionStatus",
    
    # Lending models
    "Loan", "LoanOffer", "LoanStatus", "CollateralType", "LiquidationEvent",
    
    # Insurance models
    "RiskTier", "ClaimStatus", "PoolTier", "StakePosition",
    "InsuranceClaim", "DeficitEvent", "InsuranceMetrics",
    
    # Infrastructure models
    "ResourceType", "ServiceTier", "ResourceSpec", "ResourceLoan",
    "ResourceValuation", "AMMPool", "LiquidityPosition", "ChainId",
    
    # Staking models
    "StakeStatus", "StakingPool", "DelegationPool", "UserStake",
    "CreateStakingPoolRequest", "CreateDelegationPoolRequest", "StakeRequest",
    "DelegateStakeRequest", "StakeWithdrawRequest",
    "ClaimRewardsRequest", "AutoCompoundRequest", "ExecuteCompoundRequest",
    "UpdateDelegationFeeRequest", "StakingPoolResponse", "DelegationPoolInfo",
    "UserStakeResponse", "StakingStats", "StakeResponse", "DelegateResponse",
    "StakeWithdrawResponse", "ClaimRewardsResponse",
    "CompoundResponse", "SlashingEvent",
    
    # Vault models
    "StrategyType", "Vault", "VaultStrategy", "VaultDeposit", "VaultWithdrawal",
    "StrategyReport", "CreateVaultRequest", "AddStrategyRequest", "DepositRequest",
    "VaultWithdrawRequest", "UpdateStrategyRequest",
    "HarvestRequest", "VaultResponse", "StrategyResponse", "DepositResponse",
    "VaultWithdrawResponse", "HarvestResponse",
    "VaultInfo", "StrategyInfo", "UserDepositInfo", "VaultStats", "StrategyPerformance",
    "PerformanceReport", "VaultEmergencyShutdown",
    
    # Derivatives models
    "OptionType", "OptionStyle", "PositionSide", "Option", "OptionGreeks",
    "OptionOrder", "PerpetualPosition", "PerpetualMarket", "FundingRate",
    "PerpetualOrder", "OptionsPool", "LiquidityPosition", "WriteOptionRequest",
    "BuyOptionRequest", "ExerciseOptionRequest", "OpenPerpetualRequest",
    "ClosePerpetualRequest", "AddMarginRequest", "CreateOptionsPoolRequest",
    "AddOptionsLiquidityRequest", "RemoveOptionsLiquidityRequest",
    "OptionResponse", "ExerciseResponse", "PerpetualPositionResponse",
    "PositionInfoResponse", "GreeksResponse", "OptionsPoolResponse",
    "OptionPremiumQuote", "MarketDataResponse", "DerivativesStats",
    "LiquidationEvent"
]
