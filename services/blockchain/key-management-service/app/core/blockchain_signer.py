"""
Blockchain Signer - Handles blockchain-specific signing operations
"""

import logging
from typing import Dict, Any, Optional, List
import json
from eth_account import Account
from eth_account.messages import encode_defunct
from eth_utils import to_checksum_address
from web3 import Web3
from datetime import datetime

from ..vault.vault_manager import VaultManager
from ..config import Settings

logger = logging.getLogger(__name__)


class BlockchainSigner:
    """Handles blockchain-specific signing operations"""
    
    def __init__(self, vault_manager: VaultManager, settings: Settings):
        self.vault = vault_manager
        self.settings = settings
        
    async def sign_transaction(
        self,
        chain: str,
        address: str,
        transaction: Dict[str, Any]
    ) -> str:
        """Sign a blockchain transaction"""
        # Get key name for address
        key_name = self._get_key_name(chain, address)
        
        # Verify key exists
        key_info = await self.vault.get_key_info(key_name)
        if not key_info:
            raise ValueError(f"No key found for {address} on {chain}")
            
        # Sign based on chain type
        if chain in ["ethereum", "polygon", "arbitrum", "optimism", "bsc", "avalanche"]:
            return await self._sign_ethereum_transaction(key_name, transaction)
        else:
            raise NotImplementedError(f"Signing not implemented for chain: {chain}")
            
    async def _sign_ethereum_transaction(
        self,
        key_name: str,
        transaction: Dict[str, Any]
    ) -> str:
        """Sign an Ethereum transaction"""
        try:
            # Serialize transaction
            # EIP-155 transaction format
            tx_fields = [
                transaction.get('nonce', 0),
                transaction.get('gasPrice') or transaction.get('maxFeePerGas', 0),
                transaction.get('gas', 21000),
                Web3.to_bytes(hexstr=transaction.get('to', '0x')),
                transaction.get('value', 0),
                Web3.to_bytes(hexstr=transaction.get('data', '0x')),
            ]
            
            # Add EIP-1559 fields if present
            if 'maxFeePerGas' in transaction:
                tx_fields.extend([
                    transaction.get('maxPriorityFeePerGas', 0),
                    transaction.get('maxFeePerGas', 0),
                ])
                
            # Add chain ID for EIP-155
            chain_id = transaction.get('chainId', 1)
            tx_fields.extend([chain_id, 0, 0])
            
            # RLP encode transaction
            from rlp import encode
            encoded_tx = encode(tx_fields)
            
            # Hash transaction
            tx_hash = Web3.keccak(encoded_tx)
            
            # Sign with Vault
            signature = await self.vault.sign_data(
                key_name=key_name,
                data=tx_hash,
                hash_algorithm="none",  # Already hashed
                signature_algorithm="ecdsa"
            )
            
            # Parse Vault signature format
            # Vault returns: vault:v1:base64_signature
            sig_parts = signature.split(':')
            if len(sig_parts) >= 3:
                sig_data = sig_parts[2]
            else:
                sig_data = signature
                
            # TODO: Convert Vault signature to Ethereum format (v, r, s)
            # This would require parsing the DER-encoded signature from Vault
            # and converting to Ethereum's format
            
            # For now, return the raw signed transaction
            # In production, this would be properly formatted
            return f"0x{encoded_tx.hex()}"
            
        except Exception as e:
            logger.error(f"Error signing Ethereum transaction: {e}")
            raise
            
    async def sign_message(
        self,
        chain: str,
        address: str,
        message: str
    ) -> str:
        """Sign a message"""
        # Get key name for address
        key_name = self._get_key_name(chain, address)
        
        # Verify key exists
        key_info = await self.vault.get_key_info(key_name)
        if not key_info:
            raise ValueError(f"No key found for {address} on {chain}")
            
        # Sign based on chain type
        if chain in ["ethereum", "polygon", "arbitrum", "optimism", "bsc", "avalanche"]:
            return await self._sign_ethereum_message(key_name, message)
        else:
            raise NotImplementedError(f"Message signing not implemented for chain: {chain}")
            
    async def _sign_ethereum_message(
        self,
        key_name: str,
        message: str
    ) -> str:
        """Sign an Ethereum message"""
        try:
            # Encode message using EIP-191
            message_hash = encode_defunct(text=message)
            
            # Sign with Vault
            signature = await self.vault.sign_data(
                key_name=key_name,
                data=message_hash.body,
                hash_algorithm="none",  # Already hashed
                signature_algorithm="ecdsa"
            )
            
            return signature
            
        except Exception as e:
            logger.error(f"Error signing Ethereum message: {e}")
            raise
            
    async def verify_transaction_signature(
        self,
        chain: str,
        address: str,
        transaction: Dict[str, Any],
        signed_transaction: str
    ) -> bool:
        """Verify a transaction signature"""
        # Get key name for address
        key_name = self._get_key_name(chain, address)
        
        # TODO: Implement signature verification
        # This would parse the signed transaction and verify
        # the signature matches the expected signer
        
        return True
        
    async def create_address(
        self,
        chain: str,
        label: Optional[str] = None
    ) -> Dict[str, str]:
        """Create a new blockchain address"""
        try:
            # Generate key name
            import uuid
            key_id = str(uuid.uuid4())
            key_name = f"{chain}-{key_id}"
            
            # Create key in Vault
            if chain in ["ethereum", "polygon", "arbitrum", "optimism", "bsc", "avalanche"]:
                # Use secp256k1 for Ethereum-compatible chains
                key_info = await self.vault.create_key(
                    key_name=key_name,
                    key_type="ecdsa-p256k1",
                    exportable=False  # Never export private keys
                )
            else:
                raise NotImplementedError(f"Address creation not implemented for chain: {chain}")
                
            # Derive address from public key
            # Note: This is simplified - in production, you'd need to properly
            # derive the address from the public key
            address = f"0x{key_id[:40]}"  # Placeholder
            
            # Store address mapping
            await self.vault.store_secret(
                path=f"addresses/{chain}/{address.lower()}",
                data={
                    "key_name": key_name,
                    "chain": chain,
                    "address": address,
                    "label": label,
                    "created_at": datetime.utcnow().isoformat()
                }
            )
            
            logger.info(f"Created address {address} for chain {chain}")
            
            return {
                "chain": chain,
                "address": address,
                "label": label
            }
            
        except Exception as e:
            logger.error(f"Error creating address for {chain}: {e}")
            raise
            
    async def list_addresses(self, chain: str) -> List[str]:
        """List addresses for a chain"""
        try:
            # List all addresses for chain
            # This would query the Vault KV store
            addresses = []
            
            # TODO: Implement proper listing from Vault KV store
            
            return addresses
            
        except Exception as e:
            logger.error(f"Error listing addresses for {chain}: {e}")
            return []
            
    def _get_key_name(self, chain: str, address: str) -> str:
        """Get Vault key name for an address"""
        # In production, this would look up the key name
        # from a mapping stored in Vault KV
        return f"{chain}-{address.lower()}"
        
    async def get_address_info(
        self,
        chain: str,
        address: str
    ) -> Optional[Dict[str, Any]]:
        """Get information about an address"""
        try:
            # Read from Vault KV store
            info = await self.vault.read_secret(
                path=f"addresses/{chain}/{address.lower()}"
            )
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting address info: {e}")
            return None
            
    async def check_signing_permission(
        self,
        chain: str,
        address: str,
        transaction_value: str
    ) -> bool:
        """Check if transaction signing is permitted"""
        try:
            # Check transaction value limit
            if int(transaction_value) > int(self.settings.MAX_TRANSACTION_VALUE_WEI):
                logger.warning(
                    f"Transaction value {transaction_value} exceeds limit "
                    f"{self.settings.MAX_TRANSACTION_VALUE_WEI}"
                )
                return False
                
            # Check address exists
            address_info = await self.get_address_info(chain, address)
            if not address_info:
                logger.warning(f"Address {address} not found for chain {chain}")
                return False
                
            # TODO: Additional permission checks
            # - Rate limiting
            # - Policy-based access control
            # - Multi-signature requirements
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking signing permission: {e}")
            return False 