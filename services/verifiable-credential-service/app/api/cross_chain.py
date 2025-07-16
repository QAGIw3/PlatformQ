"""
Cross-chain bridge API endpoints for exporting VCs to public blockchains
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from enum import Enum
import logging

from platformq_shared.api.deps import get_current_tenant_and_user
from ..blockchain.cross_chain_bridge import CrossChainBridge, ChainNetwork
from ..services.vc_service import get_credential_by_id

logger = logging.getLogger(__name__)
router = APIRouter()


class ExportCredentialRequest(BaseModel):
    """Request to export a credential to another blockchain"""
    credential_id: str = Field(..., description="ID of the credential to export")
    target_chain: ChainNetwork = Field(..., description="Target blockchain network")
    recipient_address: str = Field(..., description="Recipient address on target chain")
    metadata_uri: Optional[str] = Field(None, description="IPFS URI for metadata")


class BridgeTransferRequest(BaseModel):
    """Request to initiate a cross-chain bridge transfer"""
    credential_hash: str = Field(..., description="Hash of the credential")
    source_chain: ChainNetwork = Field(..., description="Source blockchain")
    target_chain: ChainNetwork = Field(..., description="Target blockchain")
    secret: str = Field(..., description="Secret for HTLC")


class ChainStatus(BaseModel):
    """Status of a blockchain network"""
    network: str
    name: str
    chain_id: int
    connected: bool
    bridge_deployed: bool
    sbt_deployed: bool
    block_number: Optional[int] = None
    gas_price: Optional[int] = None
    error: Optional[str] = None


@router.post("/export")
async def export_credential(
    request: ExportCredentialRequest,
    context: dict = Depends(get_current_tenant_and_user),
    bridge: CrossChainBridge = Depends(lambda: router.app.state.cross_chain_bridge)
):
    """
    Export a verifiable credential to another blockchain as a SoulBound Token.
    
    This endpoint:
    1. Verifies ownership of the credential
    2. Uploads metadata to IPFS if not provided
    3. Issues an SBT on the target chain
    4. Returns the transaction details
    """
    if not bridge:
        raise HTTPException(
            status_code=503,
            detail="Cross-chain bridge not initialized"
        )
    
    # Get the credential
    credential = await get_credential_by_id(
        request.credential_id,
        context["tenant_id"]
    )
    
    if not credential:
        raise HTTPException(
            status_code=404,
            detail="Credential not found"
        )
    
    # Verify ownership
    subject_id = credential.get("credentialSubject", {}).get("id", "")
    user_did = f"did:platformq:{context['tenant_id']}:{context['user'].id}"
    
    if subject_id != user_did:
        raise HTTPException(
            status_code=403,
            detail="You can only export your own credentials"
        )
    
    # Prepare metadata URI
    metadata_uri = request.metadata_uri
    if not metadata_uri:
        # Upload credential metadata to IPFS
        ipfs_storage = router.app.state.ipfs_storage
        if ipfs_storage:
            try:
                cid = await ipfs_storage.store_credential(
                    credential,
                    context["tenant_id"]
                )
                metadata_uri = f"ipfs://{cid}"
            except Exception as e:
                logger.error(f"Failed to store metadata on IPFS: {e}")
                metadata_uri = ""
    
    # Calculate credential hash
    import hashlib
    import json
    credential_str = json.dumps(credential, sort_keys=True)
    credential_hash = hashlib.sha256(credential_str.encode()).hexdigest()
    
    # Export to target chain
    result = await bridge.export_credential_to_chain(
        credential_id=request.credential_id,
        credential_hash=credential_hash,
        metadata_uri=metadata_uri,
        target_chain=request.target_chain,
        recipient_address=request.recipient_address
    )
    
    if not result.get("success"):
        raise HTTPException(
            status_code=500,
            detail=f"Export failed: {result.get('error', 'Unknown error')}"
        )
    
    return {
        "message": "Credential exported successfully",
        "transaction": result
    }


@router.post("/bridge/transfer")
async def initiate_bridge_transfer(
    request: BridgeTransferRequest,
    context: dict = Depends(get_current_tenant_and_user),
    bridge: CrossChainBridge = Depends(lambda: router.app.state.cross_chain_bridge)
):
    """
    Initiate a cross-chain bridge transfer using HTLC.
    
    This is for advanced users who want to manually bridge credentials
    between chains using the atomic swap pattern.
    """
    if not bridge:
        raise HTTPException(
            status_code=503,
            detail="Cross-chain bridge not initialized"
        )
    
    # Generate secret hash
    import hashlib
    secret_hash = hashlib.sha256(request.secret.encode()).hexdigest()
    
    # Initiate transfer
    transfer_id = await bridge.initiate_bridge_transfer(
        credential_hash=request.credential_hash,
        source_chain=request.source_chain,
        target_chain=request.target_chain,
        secret_hash=secret_hash
    )
    
    if not transfer_id:
        raise HTTPException(
            status_code=500,
            detail="Failed to initiate bridge transfer"
        )
    
    return {
        "transfer_id": transfer_id,
        "secret_hash": secret_hash,
        "status": "initiated",
        "message": "Bridge transfer initiated. Complete on target chain within 24 hours."
    }


@router.get("/export/{credential_id}/status")
async def check_export_status(
    credential_id: str,
    target_chain: ChainNetwork = Query(..., description="Target chain to check"),
    context: dict = Depends(get_current_tenant_and_user),
    bridge: CrossChainBridge = Depends(lambda: router.app.state.cross_chain_bridge)
):
    """Check the status of a credential export"""
    if not bridge:
        raise HTTPException(
            status_code=503,
            detail="Cross-chain bridge not initialized"
        )
    
    status = await bridge.check_export_status(credential_id, target_chain)
    
    if not status:
        raise HTTPException(
            status_code=404,
            detail="Export not found or expired"
        )
    
    return status


@router.get("/chains")
async def get_supported_chains(
    bridge: CrossChainBridge = Depends(lambda: router.app.state.cross_chain_bridge)
) -> List[ChainStatus]:
    """Get list of supported blockchain networks and their status"""
    if not bridge:
        raise HTTPException(
            status_code=503,
            detail="Cross-chain bridge not initialized"
        )
    
    chains = await bridge.get_supported_chains()
    
    return [ChainStatus(**chain) for chain in chains]


@router.get("/chains/{network}/fee")
async def estimate_export_fee(
    network: ChainNetwork,
    bridge: CrossChainBridge = Depends(lambda: router.app.state.cross_chain_bridge)
):
    """Estimate the gas fee for exporting to a specific chain"""
    if not bridge:
        raise HTTPException(
            status_code=503,
            detail="Cross-chain bridge not initialized"
        )
    
    try:
        w3 = bridge.connect_to_chain(network)
        gas_price = w3.eth.gas_price
        
        # Estimate gas for SBT issuance (typically ~200k gas)
        estimated_gas = 200000
        
        # Calculate fee in wei and ETH
        fee_wei = gas_price * estimated_gas
        fee_eth = w3.from_wei(fee_wei, 'ether')
        
        return {
            "network": network,
            "gas_price": gas_price,
            "estimated_gas": estimated_gas,
            "fee_wei": str(fee_wei),
            "fee_eth": str(fee_eth),
            "currency": "ETH" if network in ["ethereum", "arbitrum", "optimism"] else "MATIC"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to estimate fee: {str(e)}"
        ) 