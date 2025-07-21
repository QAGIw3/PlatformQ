"""
Chain types and related models
"""

from enum import Enum


class ChainType(Enum):
    """Supported blockchain types"""
    ETHEREUM = "ethereum"
    POLYGON = "polygon"
    ARBITRUM = "arbitrum"
    OPTIMISM = "optimism"
    BSC = "bsc"
    AVALANCHE = "avalanche"
    SOLANA = "solana"
    COSMOS = "cosmos"
    NEAR = "near"
    ALGORAND = "algorand"
    TRON = "tron"
    POLKADOT = "polkadot"
    
    # Layer 2s
    ZKSYNC = "zksync"
    STARKNET = "starknet"
    
    # Enterprise
    HYPERLEDGER = "hyperledger"
    CORDA = "corda" 