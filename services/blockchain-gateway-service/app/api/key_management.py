"""
Blockchain Key Management API

Provides endpoints for managing blockchain signing keys securely using Vault.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Dict, Any, Optional
from datetime import datetime

from platformq_blockchain_common import ChainType
from platformq_blockchain_common.secure_signing import (
    SecureTransactionSigner, 
    SigningKey,
    KeyType
)
from platformq_shared.vault.vault_client import VaultClient, VaultConfig
from platformq_shared.consul.consul_client import ConsulClient, ConsulConfig

from ..core.auth import get_current_user
from ..core.config import settings

import os
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


def get_vault_client() -> VaultClient:
    """Get Vault client instance"""
    config = VaultConfig(
        url=settings.vault_url,
        token=settings.vault_token,
        app_role_id=settings.vault_app_role_id,
        app_role_secret=settings.vault_app_role_secret
    )
    return VaultClient(config)


def get_consul_client() -> ConsulClient:
    """Get Consul client instance"""
    config = ConsulConfig(
        host=settings.consul_host,
        port=settings.consul_port
    )
    return ConsulClient(config)


def get_transaction_signer() -> SecureTransactionSigner:
    """Get secure transaction signer instance"""
    vault_client = get_vault_client()
    consul_client = get_consul_client()
    return SecureTransactionSigner(vault_client, consul_client)


@router.post("/keys", response_model=Dict[str, Any])
async def create_signing_key(
    chain_type: ChainType,
    key_type: Optional[KeyType] = None,
    metadata: Optional[Dict[str, Any]] = None,
    current_user: Dict = Depends(get_current_user),
    signer: SecureTransactionSigner = Depends(get_transaction_signer)
):
    """
    Create a new signing key for blockchain transactions.
    
    Args:
        chain_type: Blockchain type (ethereum, solana, etc.)
        key_type: Optional key type (defaults based on chain)
        metadata: Optional metadata to store with key
        
    Returns:
        Signing key details including key ID and address
    """
    try:
        await signer.initialize()
        
        # Add user info to metadata
        if metadata is None:
            metadata = {}
        metadata["created_by"] = current_user["user_id"]
        metadata["created_for_tenant"] = current_user.get("tenant_id")
        
        # Create key
        signing_key = await signer.create_signing_key(
            chain_type=chain_type,
            key_type=key_type,
            metadata=metadata
        )
        
        return {
            "key_id": signing_key.key_id,
            "chain_type": signing_key.chain_type.value,
            "key_type": signing_key.key_type.value,
            "address": signing_key.address,
            "created_at": signing_key.created_at.isoformat(),
            "metadata": signing_key.metadata
        }
        
    except Exception as e:
        logger.error(f"Failed to create signing key: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/keys", response_model=List[Dict[str, Any]])
async def list_signing_keys(
    chain_type: Optional[ChainType] = None,
    current_user: Dict = Depends(get_current_user),
    signer: SecureTransactionSigner = Depends(get_transaction_signer)
):
    """
    List signing keys, optionally filtered by chain type.
    
    Args:
        chain_type: Optional filter by blockchain type
        
    Returns:
        List of signing keys
    """
    try:
        await signer.initialize()
        
        # List keys
        keys = await signer.list_signing_keys(chain_type)
        
        # Filter by tenant if not admin
        tenant_id = current_user.get("tenant_id")
        if tenant_id and not current_user.get("is_admin"):
            keys = [k for k in keys if k.metadata.get("created_for_tenant") == tenant_id]
        
        return [
            {
                "key_id": key.key_id,
                "chain_type": key.chain_type.value,
                "key_type": key.key_type.value,
                "address": key.address,
                "created_at": key.created_at.isoformat(),
                "metadata": key.metadata
            }
            for key in keys
        ]
        
    except Exception as e:
        logger.error(f"Failed to list signing keys: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/keys/{key_id}", response_model=Dict[str, Any])
async def get_signing_key(
    key_id: str,
    current_user: Dict = Depends(get_current_user),
    signer: SecureTransactionSigner = Depends(get_transaction_signer)
):
    """
    Get details of a specific signing key.
    
    Args:
        key_id: Signing key ID
        
    Returns:
        Signing key details
    """
    try:
        await signer.initialize()
        
        # Get key
        key = await signer.get_signing_key(key_id)
        if not key:
            raise HTTPException(status_code=404, detail="Signing key not found")
        
        # Check access
        tenant_id = current_user.get("tenant_id")
        if tenant_id and not current_user.get("is_admin"):
            if key.metadata.get("created_for_tenant") != tenant_id:
                raise HTTPException(status_code=403, detail="Access denied")
        
        return {
            "key_id": key.key_id,
            "chain_type": key.chain_type.value,
            "key_type": key.key_type.value,
            "address": key.address,
            "created_at": key.created_at.isoformat(),
            "metadata": key.metadata
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get signing key: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/keys/{key_id}/rotate", response_model=Dict[str, Any])
async def rotate_signing_key(
    key_id: str,
    current_user: Dict = Depends(get_current_user),
    signer: SecureTransactionSigner = Depends(get_transaction_signer)
):
    """
    Rotate a signing key (create new key and mark old as rotated).
    
    Args:
        key_id: Signing key ID to rotate
        
    Returns:
        New signing key details
    """
    try:
        await signer.initialize()
        
        # Get existing key
        old_key = await signer.get_signing_key(key_id)
        if not old_key:
            raise HTTPException(status_code=404, detail="Signing key not found")
        
        # Check access
        tenant_id = current_user.get("tenant_id")
        if tenant_id and not current_user.get("is_admin"):
            if old_key.metadata.get("created_for_tenant") != tenant_id:
                raise HTTPException(status_code=403, detail="Access denied")
        
        # Rotate key
        new_key = await signer.rotate_key(key_id)
        
        return {
            "key_id": new_key.key_id,
            "chain_type": new_key.chain_type.value,
            "key_type": new_key.key_type.value,
            "address": new_key.address,
            "created_at": new_key.created_at.isoformat(),
            "metadata": new_key.metadata,
            "rotated_from": key_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to rotate signing key: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/keys/{key_id}/sign", response_model=Dict[str, Any])
async def sign_message(
    key_id: str,
    message: str,
    current_user: Dict = Depends(get_current_user),
    signer: SecureTransactionSigner = Depends(get_transaction_signer)
):
    """
    Sign an arbitrary message with a signing key.
    
    Args:
        key_id: Signing key ID
        message: Message to sign
        
    Returns:
        Signature
    """
    try:
        await signer.initialize()
        
        # Get key
        key = await signer.get_signing_key(key_id)
        if not key:
            raise HTTPException(status_code=404, detail="Signing key not found")
        
        # Check access
        tenant_id = current_user.get("tenant_id")
        if tenant_id and not current_user.get("is_admin"):
            if key.metadata.get("created_for_tenant") != tenant_id:
                raise HTTPException(status_code=403, detail="Access denied")
        
        # Sign message
        signature = await signer.sign_message(message, key_id)
        
        return {
            "key_id": key_id,
            "message": message,
            "signature": signature,
            "signed_at": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to sign message: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/keys/{key_id}/verify", response_model=Dict[str, Any])
async def verify_signature(
    key_id: str,
    message: str,
    signature: str,
    signer: SecureTransactionSigner = Depends(get_transaction_signer)
):
    """
    Verify a signature against a message.
    
    Args:
        key_id: Signing key ID
        message: Original message
        signature: Signature to verify
        
    Returns:
        Verification result
    """
    try:
        await signer.initialize()
        
        # Verify signature
        is_valid = await signer.verify_signature(message, signature, key_id)
        
        return {
            "key_id": key_id,
            "message": message,
            "signature": signature,
            "is_valid": is_valid,
            "verified_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to verify signature: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/keys/{key_id}/audit", response_model=List[Dict[str, Any]])
async def get_key_audit_log(
    key_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    current_user: Dict = Depends(get_current_user),
    vault_client: VaultClient = Depends(get_vault_client)
):
    """
    Get audit log for a signing key.
    
    Args:
        key_id: Signing key ID
        limit: Maximum number of entries to return
        
    Returns:
        Audit log entries
    """
    try:
        await vault_client.initialize()
        
        # Get key metadata first to check access
        try:
            key_metadata = await vault_client.read_secret(f"blockchain/keys/{key_id}/metadata")
        except:
            raise HTTPException(status_code=404, detail="Signing key not found")
        
        # Check access
        tenant_id = current_user.get("tenant_id")
        if tenant_id and not current_user.get("is_admin"):
            if key_metadata.get("metadata", {}).get("created_for_tenant") != tenant_id:
                raise HTTPException(status_code=403, detail="Access denied")
        
        # Get audit entries
        # In production, this would query Vault's audit backend
        # For now, return empty list
        audit_entries = []
        
        return audit_entries
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get audit log: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 