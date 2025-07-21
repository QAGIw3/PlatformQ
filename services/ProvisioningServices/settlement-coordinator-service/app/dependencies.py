"""
Dependencies for Settlement Coordinator Service
"""

from typing import Dict, Any
from fastapi import Depends, HTTPException, Header
import logging

from platformq_shared.blockchain import BlockchainClient
from platformq_shared.config import get_settings
from platformq_shared.oracle import PriceOracle
from .protocols.settlement import SettlementCoordinator
from .services.resource_tokenizer import ResourceTokenizer
from .services.flash_settlement import FlashSettlementService

logger = logging.getLogger(__name__)

# Singleton instances
_settlement_coordinator = None
_blockchain_client = None
_price_oracle = None
_resource_tokenizer = None
_flash_settlement_service = None

settings = get_settings()


async def get_blockchain_client() -> BlockchainClient:
    """Get blockchain client instance"""
    global _blockchain_client
    
    if _blockchain_client is None:
        _blockchain_client = BlockchainClient(
            rpc_url=settings.blockchain_rpc_url,
            chain_id=settings.blockchain_chain_id,
            private_key=settings.blockchain_private_key
        )
        await _blockchain_client.initialize()
        
    return _blockchain_client


async def get_price_oracle() -> PriceOracle:
    """Get price oracle instance"""
    global _price_oracle
    
    if _price_oracle is None:
        blockchain = await get_blockchain_client()
        _price_oracle = PriceOracle(
            blockchain_client=blockchain,
            oracle_address=settings.price_oracle_address
        )
        
    return _price_oracle


async def get_resource_tokenizer() -> ResourceTokenizer:
    """Get resource tokenizer instance"""
    global _resource_tokenizer
    
    if _resource_tokenizer is None:
        blockchain = await get_blockchain_client()
        _resource_tokenizer = ResourceTokenizer(
            blockchain_client=blockchain,
            token_contract_address=settings.resource_token_address,
            amm_contract_address=settings.resource_amm_address
        )
        await _resource_tokenizer.initialize()
        
    return _resource_tokenizer


async def get_settlement_coordinator() -> SettlementCoordinator:
    """Get settlement coordinator instance"""
    global _settlement_coordinator
    
    if _settlement_coordinator is None:
        blockchain = await get_blockchain_client()
        oracle = await get_price_oracle()
        tokenizer = await get_resource_tokenizer()
        
        _settlement_coordinator = SettlementCoordinator(
            blockchain_client=blockchain,
            price_oracle=oracle,
            resource_tokenizer=tokenizer,
            settlement_contract_address=settings.settlement_contract_address
        )
        
        await _settlement_coordinator.initialize()
        
    return _settlement_coordinator


async def get_flash_settlement_service() -> FlashSettlementService:
    """Get flash settlement service instance"""
    global _flash_settlement_service
    
    if _flash_settlement_service is None:
        blockchain = await get_blockchain_client()
        tokenizer = await get_resource_tokenizer()
        
        _flash_settlement_service = FlashSettlementService(
            blockchain_client=blockchain,
            resource_tokenizer=tokenizer,
            flash_provider_address=settings.flash_provider_address
        )
        
        await _flash_settlement_service.initialize()
        
    return _flash_settlement_service


async def get_auth_user(authorization: str = Header(None)) -> Dict[str, Any]:
    """
    Validate authorization and return user info
    
    This is a placeholder - in production would validate JWT
    and return actual user details
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization required")
        
    # Mock user for development
    return {
        "user_id": "user-123",
        "address": "0x123...",
        "is_admin": authorization == "Bearer admin-token"
    } 