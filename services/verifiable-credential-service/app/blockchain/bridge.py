"""
Cross-Chain Bridge Module for Verifiable Credentials

This module provides functionality to transfer and verify credentials across different blockchain networks.
"""

import asyncio
import hashlib
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import logging
from web3 import Web3
from eth_account.messages import encode_defunct

from .base import BlockchainClient, CredentialAnchor, ChainType
from .ethereum import EthereumClient
from .polygon import PolygonClient
from .fabric import FabricClient

logger = logging.getLogger(__name__)


class BridgeStatus(Enum):
    PENDING = "PENDING"
    LOCKED = "LOCKED"
    TRANSFERRING = "TRANSFERRING"
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"


@dataclass
class BridgeRequest:
    """Represents a cross-chain credential transfer request"""
    request_id: str
    credential_hash: str
    source_chain: ChainType
    target_chain: ChainType
    source_anchor: CredentialAnchor
    status: BridgeStatus
    created_at: datetime
    completed_at: Optional[datetime] = None
    target_anchor: Optional[CredentialAnchor] = None
    metadata: Dict[str, Any] = None


class CrossChainBridge:
    """
    Manages cross-chain credential transfers with atomic guarantees
    """
    
    def __init__(self, blockchain_clients: Dict[str, BlockchainClient]):
        self.clients = blockchain_clients
        self.pending_transfers: Dict[str, BridgeRequest] = {}
        self.bridge_contracts: Dict[Tuple[str, str], str] = {}  # (source, target) -> contract_address
        
    async def initialize_bridge_contracts(self):
        """Deploy or connect to bridge contracts on each supported chain"""
        for chain_name, client in self.clients.items():
            if hasattr(client, 'deploy_bridge_contract'):
                contract_address = await client.deploy_bridge_contract()
                # Store bridge contract addresses for each chain pair
                for target_chain in self.clients:
                    if target_chain != chain_name:
                        self.bridge_contracts[(chain_name, target_chain)] = contract_address
                        
    async def transfer_credential(
        self,
        credential_data: Dict[str, Any],
        source_chain: str,
        target_chain: str,
        tenant_id: str
    ) -> BridgeRequest:
        """
        Initiates a cross-chain credential transfer
        
        Args:
            credential_data: The credential to transfer
            source_chain: Name of the source blockchain
            target_chain: Name of the target blockchain
            tenant_id: Tenant ID for multi-tenancy
            
        Returns:
            BridgeRequest tracking the transfer
        """
        if source_chain not in self.clients or target_chain not in self.clients:
            raise ValueError("Invalid source or target chain")
            
        # Generate request ID and credential hash
        request_id = f"bridge_{datetime.utcnow().timestamp()}_{source_chain}_{target_chain}"
        credential_hash = hashlib.sha256(
            json.dumps(credential_data, sort_keys=True).encode()
        ).hexdigest()
        
        # Create bridge request
        bridge_request = BridgeRequest(
            request_id=request_id,
            credential_hash=credential_hash,
            source_chain=ChainType(source_chain.upper()),
            target_chain=ChainType(target_chain.upper()),
            source_anchor=None,
            status=BridgeStatus.PENDING,
            created_at=datetime.utcnow(),
            metadata={"tenant_id": tenant_id}
        )
        
        self.pending_transfers[request_id] = bridge_request
        
        try:
            # Step 1: Lock credential on source chain
            source_client = self.clients[source_chain]
            await source_client.connect()
            
            lock_result = await self._lock_credential_on_source(
                source_client, credential_hash, target_chain, tenant_id
            )
            bridge_request.source_anchor = lock_result
            bridge_request.status = BridgeStatus.LOCKED
            
            # Step 2: Generate cryptographic proof of lock
            proof = await self._generate_transfer_proof(
                source_client, lock_result, target_chain
            )
            
            # Step 3: Mint credential on target chain with proof
            target_client = self.clients[target_chain]
            await target_client.connect()
            
            bridge_request.status = BridgeStatus.TRANSFERRING
            target_anchor = await self._mint_credential_on_target(
                target_client, credential_data, proof, source_chain, tenant_id
            )
            
            bridge_request.target_anchor = target_anchor
            bridge_request.status = BridgeStatus.CONFIRMED
            bridge_request.completed_at = datetime.utcnow()
            
            # Step 4: Finalize on source chain
            await self._finalize_transfer_on_source(
                source_client, lock_result.transaction_hash, target_anchor.transaction_hash
            )
            
            await source_client.disconnect()
            await target_client.disconnect()
            
            logger.info(f"Successfully bridged credential from {source_chain} to {target_chain}")
            return bridge_request
            
        except Exception as e:
            logger.error(f"Bridge transfer failed: {e}")
            bridge_request.status = BridgeStatus.FAILED
            # Attempt to unlock on source chain if locked
            if bridge_request.source_anchor:
                await self._unlock_credential_on_source(
                    self.clients[source_chain], bridge_request.source_anchor
                )
            raise
            
    async def _lock_credential_on_source(
        self,
        client: BlockchainClient,
        credential_hash: str,
        target_chain: str,
        tenant_id: str
    ) -> CredentialAnchor:
        """Lock credential on source chain for transfer"""
        # This would interact with a bridge smart contract
        # For now, we'll create a special anchor indicating locked status
        metadata = {
            "action": "LOCK_FOR_BRIDGE",
            "target_chain": target_chain,
            "locked_at": datetime.utcnow().isoformat()
        }
        return await client.anchor_credential(
            {"credential_hash": credential_hash, "metadata": metadata},
            tenant_id
        )
        
    async def _generate_transfer_proof(
        self,
        client: BlockchainClient,
        lock_anchor: CredentialAnchor,
        target_chain: str
    ) -> Dict[str, Any]:
        """Generate cryptographic proof of lock for target chain verification"""
        # In a real implementation, this would generate a merkle proof
        # or use a more sophisticated cross-chain messaging protocol
        proof_data = {
            "source_chain": client.chain_type.value,
            "lock_transaction": lock_anchor.transaction_hash,
            "lock_block": lock_anchor.block_number,
            "credential_hash": lock_anchor.credential_hash,
            "timestamp": lock_anchor.timestamp.isoformat(),
            "target_chain": target_chain
        }
        
        # Sign the proof with the source chain's bridge key
        if hasattr(client, 'sign_message'):
            signature = await client.sign_message(json.dumps(proof_data))
            proof_data["signature"] = signature
            
        return proof_data
        
    async def _mint_credential_on_target(
        self,
        client: BlockchainClient,
        credential_data: Dict[str, Any],
        proof: Dict[str, Any],
        source_chain: str,
        tenant_id: str
    ) -> CredentialAnchor:
        """Mint credential on target chain using proof from source"""
        # Verify the proof before minting
        if not await self._verify_transfer_proof(client, proof, source_chain):
            raise ValueError("Invalid transfer proof")
            
        # Add bridge metadata to credential
        bridged_credential = credential_data.copy()
        bridged_credential["bridgeMetadata"] = {
            "sourcehain": source_chain,
            "bridgedAt": datetime.utcnow().isoformat(),
            "proof": proof
        }
        
        return await client.anchor_credential(bridged_credential, tenant_id)
        
    async def _verify_transfer_proof(
        self,
        client: BlockchainClient,
        proof: Dict[str, Any],
        expected_source: str
    ) -> bool:
        """Verify the transfer proof from source chain"""
        # In production, this would verify merkle proofs, signatures, etc.
        return proof.get("source_chain") == expected_source.upper()
        
    async def _finalize_transfer_on_source(
        self,
        client: BlockchainClient,
        source_tx: str,
        target_tx: str
    ):
        """Mark the transfer as complete on source chain"""
        # This would update the bridge contract on source chain
        # to indicate successful transfer
        logger.info(f"Finalizing transfer: source={source_tx}, target={target_tx}")
        
    async def _unlock_credential_on_source(
        self,
        client: BlockchainClient,
        lock_anchor: CredentialAnchor
    ):
        """Unlock credential on source chain if transfer fails"""
        logger.info(f"Unlocking credential: {lock_anchor.transaction_hash}")
        
    async def get_bridge_status(self, request_id: str) -> Optional[BridgeRequest]:
        """Get the status of a bridge transfer request"""
        return self.pending_transfers.get(request_id)
        
    async def verify_bridged_credential(
        self,
        credential_hash: str,
        chain: str
    ) -> Dict[str, Any]:
        """
        Verify a bridged credential exists on the specified chain
        and return its bridge metadata
        """
        if chain not in self.clients:
            raise ValueError(f"Chain {chain} not supported")
            
        client = self.clients[chain]
        await client.connect()
        
        # Check if credential exists and has bridge metadata
        anchor = await client.verify_credential_anchor(credential_hash)
        await client.disconnect()
        
        if not anchor:
            return {"exists": False}
            
        return {
            "exists": True,
            "anchor": anchor.to_dict(),
            "is_bridged": "bridgeMetadata" in anchor.metadata
        }


class AtomicCrossChainBridge(CrossChainBridge):
    """
    Enhanced bridge with atomic swap functionality for guaranteed transfers
    """
    
    def __init__(self, blockchain_clients: Dict[str, BlockchainClient]):
        super().__init__(blockchain_clients)
        self.swap_contracts: Dict[str, str] = {}  # chain -> atomic swap contract
        
    async def atomic_transfer(
        self,
        credential_data: Dict[str, Any],
        source_chain: str,
        target_chain: str,
        tenant_id: str,
        timeout_blocks: int = 100
    ) -> BridgeRequest:
        """
        Perform atomic cross-chain transfer using hash time-locked contracts (HTLC)
        """
        # Generate secret and hash for HTLC
        secret = hashlib.sha256(f"{datetime.utcnow().timestamp()}".encode()).hexdigest()
        secret_hash = hashlib.sha256(secret.encode()).hexdigest()
        
        request_id = f"atomic_{datetime.utcnow().timestamp()}_{source_chain}_{target_chain}"
        credential_hash = hashlib.sha256(
            json.dumps(credential_data, sort_keys=True).encode()
        ).hexdigest()
        
        bridge_request = BridgeRequest(
            request_id=request_id,
            credential_hash=credential_hash,
            source_chain=ChainType(source_chain.upper()),
            target_chain=ChainType(target_chain.upper()),
            source_anchor=None,
            status=BridgeStatus.PENDING,
            created_at=datetime.utcnow(),
            metadata={
                "tenant_id": tenant_id,
                "secret_hash": secret_hash,
                "timeout_blocks": timeout_blocks
            }
        )
        
        try:
            # Create HTLC on both chains
            source_client = self.clients[source_chain]
            target_client = self.clients[target_chain]
            
            await asyncio.gather(
                source_client.connect(),
                target_client.connect()
            )
            
            # Deploy HTLCs in parallel
            source_htlc, target_htlc = await asyncio.gather(
                self._create_htlc(source_client, credential_hash, secret_hash, timeout_blocks),
                self._create_htlc(target_client, credential_hash, secret_hash, timeout_blocks)
            )
            
            bridge_request.status = BridgeStatus.LOCKED
            
            # Reveal secret on target chain first
            target_anchor = await self._reveal_htlc(target_client, target_htlc, secret)
            bridge_request.target_anchor = target_anchor
            
            # Then reveal on source chain
            source_anchor = await self._reveal_htlc(source_client, source_htlc, secret)
            bridge_request.source_anchor = source_anchor
            
            bridge_request.status = BridgeStatus.CONFIRMED
            bridge_request.completed_at = datetime.utcnow()
            
            await asyncio.gather(
                source_client.disconnect(),
                target_client.disconnect()
            )
            
            return bridge_request
            
        except Exception as e:
            logger.error(f"Atomic transfer failed: {e}")
            bridge_request.status = BridgeStatus.FAILED
            # HTLCs will automatically refund after timeout
            raise
            
    async def _create_htlc(
        self,
        client: BlockchainClient,
        credential_hash: str,
        secret_hash: str,
        timeout_blocks: int
    ) -> str:
        """Create a hash time-locked contract"""
        # Implementation would deploy/interact with HTLC smart contract
        return f"htlc_{client.chain_type.value}_{secret_hash[:8]}"
        
    async def _reveal_htlc(
        self,
        client: BlockchainClient,
        htlc_address: str,
        secret: str
    ) -> CredentialAnchor:
        """Reveal secret to claim HTLC"""
        # Implementation would reveal secret to HTLC contract
        return CredentialAnchor(
            transaction_hash=f"reveal_{htlc_address}",
            block_number=0,
            credential_hash="",
            chain_type=client.chain_type,
            timestamp=datetime.utcnow(),
            metadata={"htlc": htlc_address, "revealed": True}
        ) 