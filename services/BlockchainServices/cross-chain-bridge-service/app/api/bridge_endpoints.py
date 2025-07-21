from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Optional, Dict, Any
import logging

from ..models.bridge_models import (
    BridgeTransferRequest, BridgeTransfer, TransferStatusResponse,
    BridgeRoute, BridgeStatistics, BridgeHealthStatus,
    TokenMapping, RelayerInfo
)
from ..core.bridge_manager import BridgeManager


router = APIRouter(prefix="/bridge", tags=["Bridge Operations"])
logger = logging.getLogger(__name__)


# Dependency to get bridge manager
async def get_bridge_manager() -> BridgeManager:
    """Get bridge manager instance"""
    from ..main import bridge_manager
    return bridge_manager


@router.post("/transfer/initiate", response_model=BridgeTransfer)
async def initiate_transfer(
    request: BridgeTransferRequest,
    bridge_manager: BridgeManager = Depends(get_bridge_manager)
):
    """
    Initiate a cross-chain transfer
    
    This endpoint starts the process of transferring tokens from one chain to another.
    The transfer will go through multiple stages:
    1. Lock tokens on source chain
    2. Wait for validator attestations
    3. Mint tokens on target chain
    """
    try:
        transfer = await bridge_manager.initiate_transfer(request)
        logger.info(f"Initiated transfer {transfer.transfer_id}")
        return transfer
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error initiating transfer: {e}")
        raise HTTPException(status_code=500, detail="Failed to initiate transfer")


@router.get("/transfer/{transfer_id}/status", response_model=TransferStatusResponse)
async def get_transfer_status(
    transfer_id: str,
    bridge_manager: BridgeManager = Depends(get_bridge_manager)
):
    """
    Get detailed status of a transfer
    
    Returns the current status, events, and estimated completion time
    """
    status = await bridge_manager.get_transfer_status(transfer_id)
    if not status:
        raise HTTPException(status_code=404, detail="Transfer not found")
    return status


@router.get("/routes", response_model=List[BridgeRoute])
async def get_bridge_routes(
    source_chain: Optional[str] = Query(None, description="Filter by source chain"),
    target_chain: Optional[str] = Query(None, description="Filter by target chain"),
    bridge_manager: BridgeManager = Depends(get_bridge_manager)
):
    """
    Get available bridge routes
    
    Returns all configured bridge routes with their parameters
    """
    routes = await bridge_manager.get_bridge_routes()
    
    # Apply filters
    if source_chain:
        routes = [r for r in routes if r.source_chain == source_chain]
    if target_chain:
        routes = [r for r in routes if r.target_chain == target_chain]
    
    return routes


@router.get("/routes/{bridge_name}/statistics", response_model=BridgeStatistics)
async def get_bridge_statistics(
    bridge_name: str,
    bridge_manager: BridgeManager = Depends(get_bridge_manager)
):
    """
    Get statistics for a specific bridge route
    
    Returns transfer counts, volumes, and performance metrics
    """
    stats = await bridge_manager.get_bridge_statistics(bridge_name)
    if not stats:
        raise HTTPException(status_code=404, detail="Bridge not found")
    return stats


@router.get("/routes/{bridge_name}/health", response_model=BridgeHealthStatus)
async def get_bridge_health(
    bridge_name: str,
    bridge_manager: BridgeManager = Depends(get_bridge_manager)
):
    """
    Get health status of a bridge route
    
    Returns operational status, connection health, and any issues
    """
    health = await bridge_manager.get_bridge_health(bridge_name)
    if not health:
        raise HTTPException(status_code=404, detail="Bridge not found")
    return health


@router.post("/transfer/{transfer_id}/estimate-fees", response_model=Dict[str, str])
async def estimate_transfer_fees(
    transfer_id: str,
    bridge_manager: BridgeManager = Depends(get_bridge_manager)
):
    """
    Estimate fees for a transfer
    
    Returns breakdown of lock fee, mint fee, and bridge fee
    """
    transfer = await bridge_manager._load_transfer(transfer_id)
    if not transfer:
        raise HTTPException(status_code=404, detail="Transfer not found")
    
    bridge = bridge_manager.bridges.get(transfer.bridge_name)
    if not bridge:
        raise HTTPException(status_code=404, detail="Bridge not found")
    
    fees = await bridge.estimate_fees(transfer)
    return fees


@router.get("/tokens/mappings", response_model=List[TokenMapping])
async def get_token_mappings(
    source_chain: Optional[str] = Query(None),
    target_chain: Optional[str] = Query(None),
    token_address: Optional[str] = Query(None)
):
    """
    Get token mappings across chains
    
    Returns how tokens are mapped between different chains
    """
    # In production, would load from configuration or database
    mappings = [
        TokenMapping(
            source_chain="ethereum",
            source_token="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
            target_chain="polygon",
            target_token="0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",  # USDC.e
            decimals_source=6,
            decimals_target=6,
            is_wrapped=True
        ),
        TokenMapping(
            source_chain="ethereum",
            source_token="0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
            target_chain="polygon",
            target_token="0xc2132D05D31c914a87C6611C10748AEb04B58e8F",  # USDT
            decimals_source=6,
            decimals_target=6,
            is_wrapped=True
        )
    ]
    
    # Apply filters
    if source_chain:
        mappings = [m for m in mappings if m.source_chain == source_chain]
    if target_chain:
        mappings = [m for m in mappings if m.target_chain == target_chain]
    if token_address:
        mappings = [m for m in mappings if 
                   m.source_token.lower() == token_address.lower() or 
                   m.target_token.lower() == token_address.lower()]
    
    return mappings


@router.get("/relayers", response_model=List[RelayerInfo])
async def get_relayers():
    """
    Get information about bridge relayers
    
    Returns relayer addresses, supported chains, and balances
    """
    # In production, would load from configuration or smart contracts
    relayers = [
        RelayerInfo(
            relayer_id="relayer-1",
            address="0x1234567890123456789012345678901234567890",
            chains=["ethereum", "polygon", "bsc"],
            min_balance_required={
                "ethereum": "1000000000000000000",  # 1 ETH
                "polygon": "1000000000000000000",   # 1 MATIC
                "bsc": "1000000000000000000"        # 1 BNB
            },
            current_balances={
                "ethereum": "2000000000000000000",
                "polygon": "5000000000000000000",
                "bsc": "3000000000000000000"
            },
            is_active=True
        )
    ]
    
    return relayers


@router.get("/validators", response_model=List[str])
async def get_validators(bridge_manager: BridgeManager = Depends(get_bridge_manager)):
    """
    Get list of bridge validators
    
    Returns addresses of nodes that can attest to transfers
    """
    return bridge_manager.validators


@router.get("/transfers/recent", response_model=List[BridgeTransfer])
async def get_recent_transfers(
    limit: int = Query(10, ge=1, le=100),
    status: Optional[str] = Query(None),
    bridge_name: Optional[str] = Query(None)
):
    """
    Get recent transfers
    
    Returns a list of recent transfers with optional filtering
    """
    # In production, would query from database
    # For now, return empty list
    return []


@router.get("/health", response_model=Dict[str, Any])
async def health_check(bridge_manager: BridgeManager = Depends(get_bridge_manager)):
    """
    Health check endpoint
    
    Returns overall service health and bridge statuses
    """
    bridge_statuses = {}
    
    for bridge_name in bridge_manager.bridges:
        health = await bridge_manager.get_bridge_health(bridge_name)
        if health:
            bridge_statuses[bridge_name] = {
                "operational": health.is_operational,
                "issues": health.issues
            }
    
    return {
        "status": "healthy",
        "bridges": bridge_statuses,
        "pulsar_connected": bridge_manager.pulsar_client is not None,
        "ignite_connected": bridge_manager.ignite_client is not None
    } 