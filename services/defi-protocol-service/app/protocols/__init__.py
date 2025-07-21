"""
DeFi Protocol implementations.
"""

from .auctions import AuctionProtocol
from .lending import LendingProtocol
from .yield_farming import YieldFarmingProtocol
from .liquidity import LiquidityProtocol
from .insurance import InsuranceProtocol
from .vault_protocol import VaultProtocol
from .staking_protocol import StakingProtocol
from .derivatives_protocol import DerivativesProtocol

# Compute resource protocols
from .compute_resource_vault import ComputeResourceVault, ComputeResourceType, ComputeStrategyType
from .compute_resource_lending import ComputeResourceLending, ComputeCollateralType, ComputeLoanType
from .compute_resource_derivatives import ComputeResourceDerivatives, ComputeDerivativeType, ComputeOptionType
from .compute_resource_insurance import ComputeResourceInsurance, InsuranceCoverageType, ClaimStatus, RiskLevel
from .compute_resource_amm import AMM, PoolType, SwapDirection

__all__ = [
    "AuctionProtocol",
    "LendingProtocol",
    "YieldFarmingProtocol",
    "LiquidityProtocol",
    "InsuranceProtocol",
    "VaultProtocol",
    "StakingProtocol",
    "DerivativesProtocol",
    # Compute protocols
    "ComputeResourceVault",
    "ComputeResourceType",
    "ComputeStrategyType",
    "ComputeResourceLending",
    "ComputeCollateralType",
    "ComputeLoanType",
    "ComputeResourceDerivatives",
    "ComputeDerivativeType",
    "ComputeOptionType",
    "ComputeResourceInsurance",
    "InsuranceCoverageType",
    "ClaimStatus",
    "RiskLevel",
    "AMM",
    "PoolType",
    "SwapDirection"
]
