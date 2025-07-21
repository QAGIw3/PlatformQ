"""
Dependencies for DeFi Protocol Service

Provides dependency injection for protocols and services.
"""

from typing import Optional
from functools import lru_cache
import os

from platformq_shared.blockchain import BlockchainClient
from .protocols.staking_protocol import StakingProtocol
from .protocols.vault_protocol import VaultProtocol
from .protocols.derivatives_protocol import DerivativesProtocol


# Cached instances
_blockchain_client: Optional[BlockchainClient] = None
_staking_protocol: Optional[StakingProtocol] = None
_vault_protocol: Optional[VaultProtocol] = None
_derivatives_protocol: Optional[DerivativesProtocol] = None


@lru_cache()
def get_blockchain_client() -> BlockchainClient:
    """Get blockchain client instance"""
    global _blockchain_client
    
    if _blockchain_client is None:
        _blockchain_client = BlockchainClient(
            rpc_url=os.getenv("BLOCKCHAIN_RPC_URL", "http://localhost:8545"),
            chain_id=int(os.getenv("CHAIN_ID", "31337")),
            private_key=os.getenv("OPERATOR_PRIVATE_KEY")
        )
    
    return _blockchain_client


async def get_staking_protocol() -> StakingProtocol:
    """Get staking protocol instance"""
    global _staking_protocol
    
    if _staking_protocol is None:
        blockchain = get_blockchain_client()
        
        _staking_protocol = StakingProtocol(
            blockchain_client=blockchain,
            staking_contract_address=os.getenv("STAKING_CONTRACT_ADDRESS"),
            resource_token_address=os.getenv("RESOURCE_TOKEN_ADDRESS")
        )
        
        await _staking_protocol.initialize()
    
    return _staking_protocol


async def get_vault_protocol() -> VaultProtocol:
    """Get vault protocol instance"""
    global _vault_protocol
    
    if _vault_protocol is None:
        blockchain = get_blockchain_client()
        
        _vault_protocol = VaultProtocol(
            blockchain_client=blockchain,
            vault_factory_address=os.getenv("VAULT_FACTORY_ADDRESS"),
            resource_token_address=os.getenv("RESOURCE_TOKEN_ADDRESS"),
            amm_address=os.getenv("RESOURCE_AMM_ADDRESS"),
            lending_address=os.getenv("INFRASTRUCTURE_LENDING_ADDRESS"),
            staking_address=os.getenv("STAKING_CONTRACT_ADDRESS")
        )
        
        await _vault_protocol.initialize()
    
    return _vault_protocol


async def get_derivatives_protocol() -> DerivativesProtocol:
    """Get derivatives protocol instance"""
    global _derivatives_protocol
    
    if _derivatives_protocol is None:
        blockchain = get_blockchain_client()
        
        _derivatives_protocol = DerivativesProtocol(
            blockchain_client=blockchain,
            options_address=os.getenv("OPTIONS_CONTRACT_ADDRESS"),
            perpetuals_address=os.getenv("PERPETUALS_CONTRACT_ADDRESS"),
            options_amm_address=os.getenv("OPTIONS_AMM_ADDRESS"),
            resource_token_address=os.getenv("RESOURCE_TOKEN_ADDRESS"),
            settlement_token_address=os.getenv("SETTLEMENT_TOKEN_ADDRESS", os.getenv("USDC_ADDRESS"))
        )
        
        await _derivatives_protocol.initialize()
    
    return _derivatives_protocol


def get_current_user() -> str:
    """
    Get current user address from authentication.
    In production, this would validate JWT and extract address.
    """
    # For development, return a test address
    return os.getenv("DEFAULT_USER_ADDRESS", "0x0000000000000000000000000000000000000000") 