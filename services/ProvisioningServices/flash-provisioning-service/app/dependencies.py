"""
Dependencies for Flash Provisioning Service
"""

from typing import Dict, Any
from fastapi import Depends, HTTPException, Header
import logging

from platformq_shared.blockchain import BlockchainClient
from platformq_shared.config import get_settings
from .protocols.flash_provisioning import FlashProvisioningProtocol
from .services.resource_matcher import ResourceMatcher
from .services.capacity_monitor import CapacityMonitor

logger = logging.getLogger(__name__)

# Singleton instances
_flash_protocol = None
_resource_matcher = None
_capacity_monitor = None
_blockchain_client = None

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


async def get_resource_matcher() -> ResourceMatcher:
    """Get resource matcher instance"""
    global _resource_matcher
    
    if _resource_matcher is None:
        _resource_matcher = ResourceMatcher()
        
    return _resource_matcher


async def get_capacity_monitor() -> CapacityMonitor:
    """Get capacity monitor instance"""
    global _capacity_monitor
    
    if _capacity_monitor is None:
        _capacity_monitor = CapacityMonitor()
        await _capacity_monitor.initialize()
        
    return _capacity_monitor


async def get_flash_protocol() -> FlashProvisioningProtocol:
    """Get flash provisioning protocol instance"""
    global _flash_protocol
    
    if _flash_protocol is None:
        blockchain = await get_blockchain_client()
        matcher = await get_resource_matcher()
        monitor = await get_capacity_monitor()
        
        _flash_protocol = FlashProvisioningProtocol(
            blockchain_client=blockchain,
            resource_matcher=matcher,
            capacity_monitor=monitor,
            flash_provider_address=settings.flash_provider_address,
            resource_token_address=settings.resource_token_address
        )
        
        await _flash_protocol.initialize()
        
    return _flash_protocol


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