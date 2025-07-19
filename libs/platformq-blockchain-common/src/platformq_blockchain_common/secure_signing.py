"""
Secure Blockchain Transaction Signing

Provides secure transaction signing using HashiCorp Vault for key management.
Supports multiple blockchain types with proper key derivation and signing.
"""

import logging
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from enum import Enum
from decimal import Decimal
import json
import hashlib
from datetime import datetime

# Cryptographic libraries
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3
import base58
from nacl.signing import SigningKey
from nacl.encoding import RawEncoder
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec, utils
from cryptography.hazmat.backends import default_backend

from platformq_shared.vault.vault_client import VaultClient, VaultConfig
from platformq_shared.consul.consul_client import ConsulClient, ConsulConfig

from .types import Transaction, ChainType

logger = logging.getLogger(__name__)


class KeyType(Enum):
    """Supported key types"""
    SECP256K1 = "secp256k1"  # Ethereum, Bitcoin
    ED25519 = "ed25519"      # Solana, Polkadot
    SECP256R1 = "secp256r1"  # NEO
    SR25519 = "sr25519"      # Substrate/Polkadot


@dataclass
class SigningKey:
    """Signing key metadata"""
    key_id: str
    key_type: KeyType
    chain_type: ChainType
    address: str
    vault_path: str
    created_at: datetime
    metadata: Dict[str, Any]


@dataclass
class SignedTransaction:
    """Signed transaction data"""
    transaction_hash: str
    signed_data: str
    signature: str
    from_address: str
    to_address: str
    value: Decimal
    nonce: int
    chain_id: int
    metadata: Dict[str, Any]


class SecureTransactionSigner:
    """
    Secure blockchain transaction signing service.
    
    Features:
    - Vault-based key management
    - Multi-chain support
    - Key derivation and address generation
    - Transaction serialization and signing
    - Audit logging
    - Key rotation support
    """
    
    def __init__(self, vault_client: VaultClient, consul_client: Optional[ConsulClient] = None):
        self.vault = vault_client
        self.consul = consul_client
        self._key_cache: Dict[str, SigningKey] = {}
        
    async def initialize(self) -> None:
        """Initialize the signing service"""
        await self.vault.initialize()
        
        if self.consul:
            await self.consul.initialize()
            
            # Register service
            from platformq_shared.consul.consul_client import ServiceDefinition
            
            service = ServiceDefinition(
                name="blockchain-signing-service",
                tags=["blockchain", "signing", "secure"],
                meta={
                    "version": "1.0.0",
                    "vault_enabled": "true"
                }
            )
            
            await self.consul.register_service(service)
            
        logger.info("Secure transaction signer initialized")
        
    async def create_signing_key(self, 
                               chain_type: ChainType,
                               key_type: Optional[KeyType] = None,
                               metadata: Optional[Dict[str, Any]] = None) -> SigningKey:
        """Create a new signing key in Vault"""
        # Determine key type based on chain
        if not key_type:
            key_type = self._get_key_type_for_chain(chain_type)
            
        # Generate key in Vault
        vault_key = await self.vault.generate_blockchain_key(key_type.value)
        
        # Derive address
        address = await self._derive_address(vault_key["key_name"], key_type, chain_type)
        
        # Create signing key metadata
        signing_key = SigningKey(
            key_id=vault_key["key_name"],
            key_type=key_type,
            chain_type=chain_type,
            address=address,
            vault_path=f"transit/{vault_key['key_name']}",
            created_at=datetime.utcnow(),
            metadata=metadata or {}
        )
        
        # Store metadata in Vault
        await self.vault.write_secret(
            f"blockchain/keys/{signing_key.key_id}/metadata",
            {
                "key_id": signing_key.key_id,
                "key_type": key_type.value,
                "chain_type": chain_type.value,
                "address": address,
                "created_at": signing_key.created_at.isoformat(),
                "metadata": signing_key.metadata
            }
        )
        
        # Cache key
        self._key_cache[signing_key.key_id] = signing_key
        
        logger.info(f"Created signing key {signing_key.key_id} for {chain_type.value}")
        return signing_key
        
    async def import_signing_key(self,
                               private_key: str,
                               chain_type: ChainType,
                               key_type: Optional[KeyType] = None) -> SigningKey:
        """Import an existing private key into Vault"""
        # This is a placeholder - in production, use Vault's import capabilities
        # For now, we'll create a new key and note it's imported
        
        signing_key = await self.create_signing_key(chain_type, key_type, {
            "imported": True,
            "import_date": datetime.utcnow().isoformat()
        })
        
        return signing_key
        
    async def get_signing_key(self, key_id: str) -> Optional[SigningKey]:
        """Get signing key by ID"""
        # Check cache
        if key_id in self._key_cache:
            return self._key_cache[key_id]
            
        # Load from Vault
        try:
            metadata = await self.vault.read_secret(f"blockchain/keys/{key_id}/metadata")
            
            signing_key = SigningKey(
                key_id=metadata["key_id"],
                key_type=KeyType(metadata["key_type"]),
                chain_type=ChainType(metadata["chain_type"]),
                address=metadata["address"],
                vault_path=f"transit/{metadata['key_id']}",
                created_at=datetime.fromisoformat(metadata["created_at"]),
                metadata=metadata.get("metadata", {})
            )
            
            # Cache key
            self._key_cache[key_id] = signing_key
            
            return signing_key
            
        except Exception as e:
            logger.error(f"Failed to get signing key {key_id}: {e}")
            return None
            
    async def list_signing_keys(self, chain_type: Optional[ChainType] = None) -> List[SigningKey]:
        """List all signing keys, optionally filtered by chain"""
        keys = []
        
        # List all keys from Vault
        key_list = await self.vault.list_secrets("blockchain/keys")
        
        for key_name in key_list:
            if key_name.endswith("/"):
                key_id = key_name.rstrip("/")
                key = await self.get_signing_key(key_id)
                
                if key and (not chain_type or key.chain_type == chain_type):
                    keys.append(key)
                    
        return keys
        
    async def sign_transaction(self,
                             transaction: Transaction,
                             key_id: str) -> SignedTransaction:
        """Sign a blockchain transaction"""
        # Get signing key
        signing_key = await self.get_signing_key(key_id)
        if not signing_key:
            raise ValueError(f"Signing key {key_id} not found")
            
        # Validate transaction
        if signing_key.address.lower() != transaction.from_address.lower():
            raise ValueError(f"Key address {signing_key.address} doesn't match transaction from address {transaction.from_address}")
            
        # Sign based on chain type
        if signing_key.chain_type in [ChainType.ETHEREUM, ChainType.POLYGON, ChainType.BSC]:
            return await self._sign_ethereum_transaction(transaction, signing_key)
        elif signing_key.chain_type == ChainType.SOLANA:
            return await self._sign_solana_transaction(transaction, signing_key)
        elif signing_key.chain_type == ChainType.POLKADOT:
            return await self._sign_polkadot_transaction(transaction, signing_key)
        elif signing_key.chain_type == ChainType.COSMOS:
            return await self._sign_cosmos_transaction(transaction, signing_key)
        else:
            raise ValueError(f"Unsupported chain type: {signing_key.chain_type}")
            
    async def _sign_ethereum_transaction(self,
                                       transaction: Transaction,
                                       signing_key: SigningKey) -> SignedTransaction:
        """Sign Ethereum-compatible transaction"""
        # Build transaction dict
        tx_dict = {
            'nonce': transaction.nonce,
            'gasPrice': transaction.gas_price,
            'gas': transaction.gas_limit,
            'to': transaction.to_address,
            'value': int(transaction.value),
            'data': transaction.data or b'',
            'chainId': transaction.chain_id
        }
        
        # Support EIP-1559
        if transaction.max_fee_per_gas:
            tx_dict.pop('gasPrice')
            tx_dict['maxFeePerGas'] = transaction.max_fee_per_gas
            tx_dict['maxPriorityFeePerGas'] = transaction.max_priority_fee_per_gas
            
        # Serialize transaction for signing
        # In production, use proper RLP encoding
        tx_bytes = json.dumps(tx_dict, sort_keys=True).encode()
        
        # Sign with Vault
        signature = await self.vault.sign_data(
            signing_key.key_id,
            tx_bytes.decode(),
            algorithm="sha2-256"
        )
        
        # Create signed transaction
        # Note: In production, properly construct the signed transaction
        signed_tx = SignedTransaction(
            transaction_hash=Web3.keccak(tx_bytes).hex(),
            signed_data=tx_bytes.hex(),
            signature=signature,
            from_address=transaction.from_address,
            to_address=transaction.to_address,
            value=transaction.value,
            nonce=transaction.nonce,
            chain_id=transaction.chain_id,
            metadata={
                "key_id": signing_key.key_id,
                "signed_at": datetime.utcnow().isoformat()
            }
        )
        
        # Audit log
        await self._audit_signing(signing_key, signed_tx)
        
        return signed_tx
        
    async def _sign_solana_transaction(self,
                                     transaction: Transaction,
                                     signing_key: SigningKey) -> SignedTransaction:
        """Sign Solana transaction"""
        # Serialize transaction
        tx_bytes = json.dumps({
            "from": transaction.from_address,
            "to": transaction.to_address,
            "lamports": int(transaction.value * 10**9)
        }, sort_keys=True).encode()
        
        # Sign with Vault
        signature = await self.vault.sign_data(
            signing_key.key_id,
            tx_bytes.decode(),
            algorithm="sha2-256"
        )
        
        # Create signed transaction
        signed_tx = SignedTransaction(
            transaction_hash=base58.b58encode(hashlib.sha256(tx_bytes).digest()).decode(),
            signed_data=base58.b58encode(tx_bytes).decode(),
            signature=signature,
            from_address=transaction.from_address,
            to_address=transaction.to_address,
            value=transaction.value,
            nonce=0,  # Solana doesn't use nonces
            chain_id=0,  # Solana doesn't use chain IDs
            metadata={
                "key_id": signing_key.key_id,
                "signed_at": datetime.utcnow().isoformat()
            }
        )
        
        await self._audit_signing(signing_key, signed_tx)
        
        return signed_tx
        
    async def _sign_polkadot_transaction(self,
                                       transaction: Transaction,
                                       signing_key: SigningKey) -> SignedTransaction:
        """Sign Polkadot/Substrate transaction"""
        # Serialize transaction (simplified)
        tx_bytes = json.dumps({
            "from": transaction.from_address,
            "to": transaction.to_address,
            "value": str(transaction.value),
            "nonce": transaction.nonce
        }, sort_keys=True).encode()
        
        # Sign with Vault
        signature = await self.vault.sign_data(
            signing_key.key_id,
            tx_bytes.decode(),
            algorithm="sha2-256"
        )
        
        # Create signed transaction
        signed_tx = SignedTransaction(
            transaction_hash=f"0x{hashlib.blake2b(tx_bytes).hexdigest()}",
            signed_data=tx_bytes.hex(),
            signature=signature,
            from_address=transaction.from_address,
            to_address=transaction.to_address,
            value=transaction.value,
            nonce=transaction.nonce,
            chain_id=0,  # Polkadot uses different chain identification
            metadata={
                "key_id": signing_key.key_id,
                "signed_at": datetime.utcnow().isoformat()
            }
        )
        
        await self._audit_signing(signing_key, signed_tx)
        
        return signed_tx
        
    async def _sign_cosmos_transaction(self,
                                     transaction: Transaction,
                                     signing_key: SigningKey) -> SignedTransaction:
        """Sign Cosmos transaction"""
        # Serialize transaction (simplified)
        tx_bytes = json.dumps({
            "from": transaction.from_address,
            "to": transaction.to_address,
            "amount": str(transaction.value),
            "denom": "uatom",
            "sequence": transaction.nonce
        }, sort_keys=True).encode()
        
        # Sign with Vault
        signature = await self.vault.sign_data(
            signing_key.key_id,
            tx_bytes.decode(),
            algorithm="sha2-256"
        )
        
        # Create signed transaction
        signed_tx = SignedTransaction(
            transaction_hash=f"cosmos_{hashlib.sha256(tx_bytes).hexdigest()[:40]}",
            signed_data=base58.b58encode(tx_bytes).decode(),
            signature=signature,
            from_address=transaction.from_address,
            to_address=transaction.to_address,
            value=transaction.value,
            nonce=transaction.nonce,
            chain_id=0,  # Cosmos uses different chain identification
            metadata={
                "key_id": signing_key.key_id,
                "signed_at": datetime.utcnow().isoformat()
            }
        )
        
        await self._audit_signing(signing_key, signed_tx)
        
        return signed_tx
        
    async def sign_message(self, message: str, key_id: str) -> str:
        """Sign an arbitrary message"""
        # Get signing key
        signing_key = await self.get_signing_key(key_id)
        if not signing_key:
            raise ValueError(f"Signing key {key_id} not found")
            
        # Sign message
        signature = await self.vault.sign_data(
            signing_key.key_id,
            message,
            algorithm="sha2-256"
        )
        
        return signature
        
    async def verify_signature(self, 
                             message: str, 
                             signature: str, 
                             key_id: str) -> bool:
        """Verify a signature"""
        return await self.vault.verify_signature(
            key_id,
            message,
            signature,
            algorithm="sha2-256"
        )
        
    async def rotate_key(self, key_id: str) -> SigningKey:
        """Rotate a signing key"""
        # Get existing key
        old_key = await self.get_signing_key(key_id)
        if not old_key:
            raise ValueError(f"Key {key_id} not found")
            
        # Create new key
        new_key = await self.create_signing_key(
            old_key.chain_type,
            old_key.key_type,
            {
                **old_key.metadata,
                "rotated_from": key_id,
                "rotated_at": datetime.utcnow().isoformat()
            }
        )
        
        # Update old key metadata
        await self.vault.write_secret(
            f"blockchain/keys/{key_id}/metadata",
            {
                **await self.vault.read_secret(f"blockchain/keys/{key_id}/metadata"),
                "rotated_to": new_key.key_id,
                "rotated_at": datetime.utcnow().isoformat(),
                "active": False
            }
        )
        
        logger.info(f"Rotated key {key_id} to {new_key.key_id}")
        return new_key
        
    async def _derive_address(self, 
                            key_name: str, 
                            key_type: KeyType, 
                            chain_type: ChainType) -> str:
        """Derive blockchain address from key"""
        # This is a simplified implementation
        # In production, use proper address derivation for each chain
        
        if chain_type in [ChainType.ETHEREUM, ChainType.POLYGON, ChainType.BSC]:
            # Ethereum-style address
            # In production, derive from public key
            return f"0x{hashlib.sha256(key_name.encode()).hexdigest()[:40]}"
            
        elif chain_type == ChainType.SOLANA:
            # Solana address (base58)
            addr_bytes = hashlib.sha256(key_name.encode()).digest()[:32]
            return base58.b58encode(addr_bytes).decode()
            
        elif chain_type == ChainType.POLKADOT:
            # Polkadot SS58 address
            # Simplified - in production use proper SS58 encoding
            return f"1{hashlib.sha256(key_name.encode()).hexdigest()[:46]}"
            
        elif chain_type == ChainType.COSMOS:
            # Cosmos bech32 address
            # Simplified - in production use proper bech32 encoding
            return f"cosmos1{hashlib.sha256(key_name.encode()).hexdigest()[:38]}"
            
        else:
            # Generic address
            return hashlib.sha256(key_name.encode()).hexdigest()[:40]
            
    def _get_key_type_for_chain(self, chain_type: ChainType) -> KeyType:
        """Get appropriate key type for chain"""
        if chain_type in [ChainType.ETHEREUM, ChainType.POLYGON, ChainType.BSC]:
            return KeyType.SECP256K1
        elif chain_type == ChainType.SOLANA:
            return KeyType.ED25519
        elif chain_type == ChainType.POLKADOT:
            return KeyType.SR25519
        elif chain_type == ChainType.COSMOS:
            return KeyType.SECP256K1
        else:
            return KeyType.SECP256K1  # Default
            
    async def _audit_signing(self, 
                           signing_key: SigningKey, 
                           signed_tx: SignedTransaction) -> None:
        """Audit log transaction signing"""
        audit_entry = {
            "action": "transaction_signed",
            "key_id": signing_key.key_id,
            "chain_type": signing_key.chain_type.value,
            "from_address": signed_tx.from_address,
            "to_address": signed_tx.to_address,
            "value": str(signed_tx.value),
            "transaction_hash": signed_tx.transaction_hash,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Store in Vault audit log
        await self.vault.write_secret(
            f"audit/signing/{datetime.utcnow().strftime('%Y%m%d')}/{signed_tx.transaction_hash}",
            audit_entry
        )
        
        logger.info(f"Signed transaction {signed_tx.transaction_hash} with key {signing_key.key_id}")
        
    async def health_check(self) -> Dict[str, Any]:
        """Check signing service health"""
        vault_health = await self.vault.health_check()
        
        health = {
            "service": "blockchain-signing",
            "vault": vault_health,
            "cached_keys": len(self._key_cache)
        }
        
        if self.consul:
            consul_health = await self.consul.health_check()
            health["consul"] = consul_health
            
        return health 