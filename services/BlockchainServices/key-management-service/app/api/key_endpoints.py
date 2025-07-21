"""
Key Management API endpoints
"""

from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel, Field

from ..vault.vault_manager import VaultManager
from ..core.blockchain_signer import BlockchainSigner

router = APIRouter(prefix="/api/v1", tags=["keys"])


# Request/Response Models
class SignTransactionRequest(BaseModel):
    chain: str = Field(..., description="Blockchain identifier")
    address: str = Field(..., description="Signing address")
    transaction: Dict[str, Any] = Field(..., description="Transaction to sign")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class SignMessageRequest(BaseModel):
    chain: str = Field(..., description="Blockchain identifier")
    address: str = Field(..., description="Signing address")
    message: str = Field(..., description="Message to sign")


class VerifyTransactionRequest(BaseModel):
    chain: str = Field(..., description="Blockchain identifier")
    address: str = Field(..., description="Expected signer address")
    transaction: Dict[str, Any] = Field(..., description="Original transaction")
    signed_transaction: str = Field(..., description="Signed transaction")


class CreateAddressRequest(BaseModel):
    chain: str = Field(..., description="Blockchain identifier")
    label: Optional[str] = Field(None, description="Address label")


class PermissionCheckRequest(BaseModel):
    chain: str = Field(..., description="Blockchain identifier")
    address: str = Field(..., description="Address")
    action: str = Field(..., description="Action to check")
    value: Optional[str] = Field(None, description="Transaction value")


# Dependencies
def get_vault_manager(request) -> VaultManager:
    """Get Vault manager instance"""
    return request.app.state.vault_manager


def get_blockchain_signer(request) -> BlockchainSigner:
    """Get blockchain signer instance"""
    return request.app.state.blockchain_signer


def verify_api_key(x_api_key: Optional[str] = Header(None)):
    """Verify API key for authentication"""
    # TODO: Implement proper API key verification
    if not x_api_key:
        raise HTTPException(status_code=401, detail="API key required")
    return x_api_key


# Endpoints
@router.post("/sign/transaction")
async def sign_transaction(
    request: SignTransactionRequest,
    vault_manager: VaultManager = Depends(get_vault_manager),
    blockchain_signer: BlockchainSigner = Depends(get_blockchain_signer),
    api_key: str = Depends(verify_api_key)
) -> dict:
    """Sign a blockchain transaction"""
    try:
        # Check permissions
        has_permission = await blockchain_signer.check_signing_permission(
            request.chain,
            request.address,
            request.transaction.get('value', '0')
        )
        
        if not has_permission:
            raise HTTPException(
                status_code=403,
                detail="Permission denied for signing"
            )
            
        # Sign transaction
        signed_tx = await blockchain_signer.sign_transaction(
            request.chain,
            request.address,
            request.transaction
        )
        
        return {
            "signed_transaction": signed_tx,
            "chain": request.chain,
            "address": request.address
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sign/message")
async def sign_message(
    request: SignMessageRequest,
    blockchain_signer: BlockchainSigner = Depends(get_blockchain_signer),
    api_key: str = Depends(verify_api_key)
) -> dict:
    """Sign a message"""
    try:
        signature = await blockchain_signer.sign_message(
            request.chain,
            request.address,
            request.message
        )
        
        return {
            "signature": signature,
            "chain": request.chain,
            "address": request.address
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/verify/transaction")
async def verify_transaction(
    request: VerifyTransactionRequest,
    blockchain_signer: BlockchainSigner = Depends(get_blockchain_signer),
    api_key: str = Depends(verify_api_key)
) -> dict:
    """Verify a transaction signature"""
    try:
        valid = await blockchain_signer.verify_transaction_signature(
            request.chain,
            request.address,
            request.transaction,
            request.signed_transaction
        )
        
        return {
            "valid": valid,
            "chain": request.chain,
            "address": request.address
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/addresses/{chain}")
async def list_addresses(
    chain: str,
    blockchain_signer: BlockchainSigner = Depends(get_blockchain_signer),
    api_key: str = Depends(verify_api_key)
) -> dict:
    """List addresses for a chain"""
    try:
        addresses = await blockchain_signer.list_addresses(chain)
        
        return {
            "chain": chain,
            "addresses": addresses
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/addresses")
async def create_address(
    request: CreateAddressRequest,
    blockchain_signer: BlockchainSigner = Depends(get_blockchain_signer),
    api_key: str = Depends(verify_api_key)
) -> dict:
    """Create a new blockchain address"""
    try:
        result = await blockchain_signer.create_address(
            request.chain,
            request.label
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/addresses/{chain}/{address}")
async def get_address_info(
    chain: str,
    address: str,
    blockchain_signer: BlockchainSigner = Depends(get_blockchain_signer),
    api_key: str = Depends(verify_api_key)
) -> dict:
    """Get information about an address"""
    try:
        info = await blockchain_signer.get_address_info(chain, address)
        
        if not info:
            raise HTTPException(status_code=404, detail="Address not found")
            
        return info
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/permissions/check")
async def check_permission(
    request: PermissionCheckRequest,
    api_key: str = Depends(verify_api_key)
) -> dict:
    """Check if an action is permitted"""
    # TODO: Implement permission checking logic
    # This would check against policies defined in OPA or Vault
    
    return {
        "permitted": True,
        "chain": request.chain,
        "address": request.address,
        "action": request.action
    }


# Key management endpoints (generic, not blockchain-specific)
@router.post("/keys/{key_name}/encrypt")
async def encrypt_data(
    key_name: str,
    data: Dict[str, str],
    vault_manager: VaultManager = Depends(get_vault_manager),
    api_key: str = Depends(verify_api_key)
) -> dict:
    """Encrypt data using a key"""
    try:
        plaintext = data.get('plaintext', '').encode('utf-8')
        ciphertext = await vault_manager.encrypt_data(key_name, plaintext)
        
        return {
            "ciphertext": ciphertext,
            "key_name": key_name
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/keys/{key_name}/decrypt")
async def decrypt_data(
    key_name: str,
    data: Dict[str, str],
    vault_manager: VaultManager = Depends(get_vault_manager),
    api_key: str = Depends(verify_api_key)
) -> dict:
    """Decrypt data using a key"""
    try:
        ciphertext = data.get('ciphertext', '')
        plaintext = await vault_manager.decrypt_data(key_name, ciphertext)
        
        return {
            "plaintext": plaintext.decode('utf-8'),
            "key_name": key_name
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/keys/{key_name}/rotate")
async def rotate_key(
    key_name: str,
    vault_manager: VaultManager = Depends(get_vault_manager),
    api_key: str = Depends(verify_api_key)
) -> dict:
    """Rotate a key to a new version"""
    try:
        key_info = await vault_manager.rotate_key(key_name)
        
        return {
            "key_name": key_name,
            "latest_version": key_info['latest_version'],
            "min_decryption_version": key_info.get('min_decryption_version'),
            "min_encryption_version": key_info.get('min_encryption_version')
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/keys")
async def list_keys(
    vault_manager: VaultManager = Depends(get_vault_manager),
    api_key: str = Depends(verify_api_key)
) -> dict:
    """List all keys"""
    try:
        keys = await vault_manager.list_keys()
        
        return {
            "keys": keys,
            "total": len(keys)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 