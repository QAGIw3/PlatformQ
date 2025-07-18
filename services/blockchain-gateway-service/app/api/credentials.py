"""
Credential anchoring API endpoints for blockchain gateway service.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
import hashlib
import json

from platformq_blockchain_common import ChainType, ChainNotSupportedError

logger = logging.getLogger(__name__)

router = APIRouter()


class CredentialAnchorRequest(BaseModel):
    """Request to anchor a credential on blockchain"""
    credential_id: str = Field(..., description="Unique credential identifier")
    credential_data: Dict[str, Any] = Field(..., description="Credential data to anchor")
    chain: str = Field("ethereum", description="Target blockchain")
    tenant_id: Optional[str] = Field(None, description="Tenant identifier")
    anchor_type: str = Field("credential", description="Type of anchor")


class CredentialAnchorResponse(BaseModel):
    """Response from anchoring a credential"""
    credential_id: str
    hash: str
    transaction_hash: str
    block_number: int
    timestamp: str
    chain: str
    gas_used: Optional[str] = None


class SBTMintRequest(BaseModel):
    """Request to mint a SoulBound Token"""
    credential_id: str = Field(..., description="Credential ID")
    credential_data: Dict[str, Any] = Field(..., description="Credential data")
    owner_address: str = Field(..., description="Token owner address")
    chain: str = Field("ethereum", description="Target blockchain")
    metadata_uri: Optional[str] = Field(None, description="IPFS URI for metadata")


class CredentialRevokeRequest(BaseModel):
    """Request to revoke a credential"""
    credential_id: str = Field(..., description="Credential to revoke")
    reason: str = Field(..., description="Revocation reason")
    chain: str = Field("ethereum", description="Target blockchain")


# Dependency to get gateway
def get_gateway():
    """Get blockchain gateway instance from app state"""
    from fastapi import Request
    
    def _get_gateway(request: Request):
        return request.app.state.gateway
    
    return _get_gateway


@router.post("/anchor", response_model=CredentialAnchorResponse)
async def anchor_credential(
    request: CredentialAnchorRequest,
    gateway=Depends(get_gateway())
):
    """
    Anchor a verifiable credential on blockchain.
    
    Creates an immutable record of the credential hash on the specified blockchain.
    """
    try:
        # Calculate credential hash
        credential_str = json.dumps(request.credential_data, sort_keys=True)
        credential_hash = hashlib.sha256(credential_str.encode()).hexdigest()
        
        # Get chain type
        chain_type = ChainType(request.chain.lower())
        
        # Prepare contract call data
        # This would call a CredentialRegistry contract
        contract_address = _get_credential_registry_address(chain_type)
        
        # Call the contract to anchor credential
        async with gateway.connection_pool.get_connection(chain_type) as adapter:
            # Encode contract call
            method = "anchorCredential"
            params = [
                request.credential_id,
                credential_hash,
                request.tenant_id or "",
                int(datetime.utcnow().timestamp())
            ]
            
            # Get contract ABI (simplified for example)
            abi = _get_credential_registry_abi()
            
            # Send transaction
            result = await adapter.call_contract_method(
                contract_address=contract_address,
                method=method,
                params=params,
                abi=abi,
                sender=_get_service_wallet_address(chain_type)
            )
            
            # Wait for confirmation
            tx_result = await gateway.transaction_manager.wait_for_confirmation(
                result.transaction_hash
            )
            
            # Emit event
            await gateway._emit_event("credential_anchored", {
                "credential_id": request.credential_id,
                "chain": chain_type.value,
                "tx_hash": tx_result.transaction_hash,
                "block_number": tx_result.block_number
            })
            
            return CredentialAnchorResponse(
                credential_id=request.credential_id,
                hash=credential_hash,
                transaction_hash=tx_result.transaction_hash,
                block_number=tx_result.block_number,
                timestamp=datetime.utcnow().isoformat(),
                chain=chain_type.value,
                gas_used=str(tx_result.gas_used) if tx_result.gas_used else None
            )
            
    except ChainNotSupportedError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error anchoring credential: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/verify/{chain}/{credential_id}", response_model=Optional[CredentialAnchorResponse])
async def verify_credential_anchor(
    chain: str,
    credential_id: str,
    gateway=Depends(get_gateway())
):
    """
    Verify a credential anchor exists on blockchain.
    
    Returns anchor details if found, 404 if not found.
    """
    try:
        chain_type = ChainType(chain.lower())
        contract_address = _get_credential_registry_address(chain_type)
        
        async with gateway.connection_pool.get_connection(chain_type) as adapter:
            # Call contract to get credential
            result = await adapter.call_contract(
                contract_address=contract_address,
                method="getCredential",
                params=[credential_id],
                abi=_get_credential_registry_abi()
            )
            
            if not result or result[0] == "":  # Empty hash means not found
                raise HTTPException(status_code=404, detail="Credential anchor not found")
                
            # Parse result
            credential_hash, block_number, timestamp, issuer = result
            
            # Get transaction details
            # This would query events to find the original transaction
            tx_hash = await _find_anchor_transaction(adapter, credential_id, block_number)
            
            return CredentialAnchorResponse(
                credential_id=credential_id,
                hash=credential_hash,
                transaction_hash=tx_hash,
                block_number=block_number,
                timestamp=datetime.fromtimestamp(timestamp).isoformat(),
                chain=chain_type.value
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error verifying credential: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sbt/mint")
async def mint_soulbound_token(
    request: SBTMintRequest,
    gateway=Depends(get_gateway())
):
    """
    Mint a SoulBound Token for a verifiable credential.
    
    SBTs are non-transferable tokens that represent credentials on-chain.
    """
    try:
        chain_type = ChainType(request.chain.lower())
        
        # Get SBT contract address
        sbt_contract = _get_sbt_contract_address(chain_type)
        
        # Prepare metadata
        metadata = {
            "credential_id": request.credential_id,
            "credential_type": request.credential_data.get("type", ["VerifiableCredential"]),
            "issuer": request.credential_data.get("issuer"),
            "issuance_date": request.credential_data.get("issuanceDate"),
            "metadata_uri": request.metadata_uri
        }
        
        async with gateway.connection_pool.get_connection(chain_type) as adapter:
            # Mint SBT
            result = await adapter.call_contract_method(
                contract_address=sbt_contract,
                method="mintSBT",
                params=[
                    request.owner_address,
                    request.credential_id,
                    json.dumps(metadata),
                    request.metadata_uri or ""
                ],
                abi=_get_sbt_abi(),
                sender=_get_service_wallet_address(chain_type)
            )
            
            # Wait for confirmation
            tx_result = await gateway.transaction_manager.wait_for_confirmation(
                result.transaction_hash
            )
            
            # Extract token ID from events
            token_id = await _extract_token_id_from_tx(adapter, tx_result.transaction_hash)
            
            return {
                "token_id": token_id,
                "transaction_hash": tx_result.transaction_hash,
                "owner": request.owner_address,
                "chain": chain_type.value,
                "contract_address": sbt_contract
            }
            
    except Exception as e:
        logger.error(f"Error minting SBT: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/revoke")
async def revoke_credential(
    request: CredentialRevokeRequest,
    gateway=Depends(get_gateway())
):
    """
    Revoke a credential on blockchain.
    
    Marks the credential as revoked in the on-chain registry.
    """
    try:
        chain_type = ChainType(request.chain.lower())
        contract_address = _get_credential_registry_address(chain_type)
        
        async with gateway.connection_pool.get_connection(chain_type) as adapter:
            # Revoke credential
            result = await adapter.call_contract_method(
                contract_address=contract_address,
                method="revokeCredential",
                params=[request.credential_id, request.reason],
                abi=_get_credential_registry_abi(),
                sender=_get_service_wallet_address(chain_type)
            )
            
            # Wait for confirmation
            tx_result = await gateway.transaction_manager.wait_for_confirmation(
                result.transaction_hash
            )
            
            # Emit event
            await gateway._emit_event("credential_revoked", {
                "credential_id": request.credential_id,
                "chain": chain_type.value,
                "reason": request.reason,
                "tx_hash": tx_result.transaction_hash
            })
            
            return {
                "credential_id": request.credential_id,
                "transaction_hash": tx_result.transaction_hash,
                "status": "revoked",
                "chain": chain_type.value
            }
            
    except Exception as e:
        logger.error(f"Error revoking credential: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Helper functions

def _get_credential_registry_address(chain_type: ChainType) -> str:
    """Get credential registry contract address for a chain"""
    # In production, load from configuration
    addresses = {
        ChainType.ETHEREUM: "0x1234567890123456789012345678901234567890",
        ChainType.POLYGON: "0x0987654321098765432109876543210987654321",
        ChainType.ARBITRUM: "0x1111111111111111111111111111111111111111"
    }
    return addresses.get(chain_type, "0x0000000000000000000000000000000000000000")


def _get_sbt_contract_address(chain_type: ChainType) -> str:
    """Get SoulBound Token contract address for a chain"""
    addresses = {
        ChainType.ETHEREUM: "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ChainType.POLYGON: "0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
        ChainType.ARBITRUM: "0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"
    }
    return addresses.get(chain_type, "0x0000000000000000000000000000000000000000")


def _get_service_wallet_address(chain_type: ChainType) -> str:
    """Get service wallet address for a chain"""
    # In production, use secure key management
    return "0xSERVICEWALLETADDRESS"


def _get_credential_registry_abi() -> List[Dict[str, Any]]:
    """Get credential registry contract ABI"""
    # Simplified ABI - in production, load from file
    return [
        {
            "name": "anchorCredential",
            "type": "function",
            "inputs": [
                {"name": "credentialId", "type": "string"},
                {"name": "credentialHash", "type": "string"},
                {"name": "tenantId", "type": "string"},
                {"name": "timestamp", "type": "uint256"}
            ]
        },
        {
            "name": "getCredential",
            "type": "function",
            "inputs": [{"name": "credentialId", "type": "string"}],
            "outputs": [
                {"name": "credentialHash", "type": "string"},
                {"name": "blockNumber", "type": "uint256"},
                {"name": "timestamp", "type": "uint256"},
                {"name": "issuer", "type": "address"}
            ]
        },
        {
            "name": "revokeCredential",
            "type": "function",
            "inputs": [
                {"name": "credentialId", "type": "string"},
                {"name": "reason", "type": "string"}
            ]
        }
    ]


def _get_sbt_abi() -> List[Dict[str, Any]]:
    """Get SoulBound Token contract ABI"""
    return [
        {
            "name": "mintSBT",
            "type": "function",
            "inputs": [
                {"name": "to", "type": "address"},
                {"name": "credentialId", "type": "string"},
                {"name": "metadata", "type": "string"},
                {"name": "uri", "type": "string"}
            ]
        }
    ]


async def _find_anchor_transaction(adapter, credential_id: str, block_number: int) -> str:
    """Find the transaction hash for a credential anchor"""
    # This would query blockchain events
    # For now, return a mock hash
    return f"0x{hashlib.sha256(f'{credential_id}{block_number}'.encode()).hexdigest()}"


async def _extract_token_id_from_tx(adapter, tx_hash: str) -> int:
    """Extract minted token ID from transaction events"""
    # This would parse transaction logs
    # For now, return a mock ID
    return int(hashlib.sha256(tx_hash.encode()).hexdigest()[:8], 16) 