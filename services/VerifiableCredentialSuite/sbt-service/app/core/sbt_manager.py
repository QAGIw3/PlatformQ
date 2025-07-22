"""
SoulBound Token Manager
"""

import json
import base64
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
from enum import Enum
import httpx

from app.config import settings
from app.storage.sbt_store import SBTStore
from app.core.event_publisher import SBTEventPublisher
from platformq_consul import ConsulClient


class SBTStatus(str, Enum):
    """SBT status enum"""
    PENDING = "pending"
    MINTED = "minted"
    ACTIVE = "active"
    REVOKED = "revoked"
    EXPIRED = "expired"
    BURNED = "burned"


class TransferAttemptResult(str, Enum):
    """Transfer attempt results"""
    BLOCKED = "blocked"
    FAILED = "failed"
    INVALID = "invalid"


class SBTManager:
    """
    Manages SoulBound Token operations including minting, revocation,
    and transfer prevention
    """
    
    def __init__(
        self,
        blockchain_connector_url: str,
        credential_service_url: str,
        storage_service_url: str,
        http_client: httpx.AsyncClient,
        vault_client: Optional[Any],
        consul_client: Optional[ConsulClient],
        sbt_store: Optional[SBTStore],
        event_publisher: Optional[SBTEventPublisher]
    ):
        self.blockchain_connector_url = blockchain_connector_url
        self.credential_service_url = credential_service_url
        self.storage_service_url = storage_service_url
        self.http_client = http_client
        self.vault_client = vault_client
        self.consul_client = consul_client
        self.sbt_store = sbt_store
        self.event_publisher = event_publisher
        self.initialized = False
        
        # Caches
        self.contract_addresses = {}
        self.chain_configs = {}
        
        # Statistics
        self.total_minted = 0
        self.total_revoked = 0
        self.total_transfer_attempts = 0
    
    async def initialize(self):
        """Initialize the SBT manager"""
        # Load chain configurations from Consul if available
        if self.consul_client and settings.enable_consul_config:
            config = await self.consul_client.get_service_config(settings.service_name)
            if config and "chains" in config:
                self.chain_configs = config["chains"]
        
        # Load default contract addresses
        self.contract_addresses = {
            "ethereum": settings.ethereum_sbt_contract,
            "polygon": settings.polygon_sbt_contract,
            "avalanche": settings.avalanche_sbt_contract,
            "binance": settings.binance_sbt_contract
        }
        
        self.initialized = True
    
    async def mint_sbt(
        self,
        credential_id: str,
        recipient: str,
        chain: str,
        metadata: Dict[str, Any],
        issuer: str,
        options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Mint a new SoulBound Token
        
        Args:
            credential_id: ID of the associated credential
            recipient: Wallet address to receive the SBT
            chain: Blockchain to mint on
            metadata: SBT metadata
            issuer: Issuer DID or address
            options: Additional minting options
            
        Returns:
            Minted SBT details
        """
        # Validate inputs
        if chain not in self.contract_addresses:
            raise ValueError(f"Unsupported chain: {chain}")
        
        # Check if SBT already exists for this credential
        existing = await self.sbt_store.get_by_credential_id(credential_id)
        if existing and existing.status in [SBTStatus.MINTED, SBTStatus.ACTIVE]:
            raise ValueError(f"SBT already exists for credential {credential_id}")
        
        # Get credential details
        credential = await self._get_credential(credential_id)
        if not credential:
            raise ValueError(f"Credential {credential_id} not found")
        
        # Prepare SBT metadata
        sbt_metadata = {
            "credentialId": credential_id,
            "credentialType": credential.get("type", []),
            "issuer": issuer,
            "issuanceDate": datetime.now(timezone.utc).isoformat(),
            "recipient": recipient,
            **metadata
        }
        
        # Upload metadata to storage service
        metadata_uri = await self._upload_metadata(sbt_metadata)
        
        # Create SBT record
        sbt_record = await self.sbt_store.create(
            credential_id=credential_id,
            token_id=None,  # Will be set after minting
            chain=chain,
            contract_address=self.contract_addresses[chain],
            recipient=recipient,
            issuer=issuer,
            metadata_uri=metadata_uri,
            metadata=sbt_metadata,
            status=SBTStatus.PENDING
        )
        
        try:
            # Mint on blockchain
            mint_result = await self._mint_on_chain(
                chain=chain,
                recipient=recipient,
                metadata_uri=metadata_uri,
                sbt_id=sbt_record.id
            )
            
            # Update record with token ID and transaction hash
            sbt_record = await self.sbt_store.update(
                sbt_id=sbt_record.id,
                token_id=mint_result["tokenId"],
                transaction_hash=mint_result["transactionHash"],
                status=SBTStatus.MINTED
            )
            
            # Update statistics
            self.total_minted += 1
            
            # Publish event
            await self._publish_event("sbt_minted", {
                "sbt_id": sbt_record.id,
                "token_id": mint_result["tokenId"],
                "chain": chain,
                "recipient": recipient,
                "credential_id": credential_id,
                "transaction_hash": mint_result["transactionHash"]
            })
            
            return {
                "id": sbt_record.id,
                "tokenId": mint_result["tokenId"],
                "chain": chain,
                "contractAddress": self.contract_addresses[chain],
                "recipient": recipient,
                "metadataUri": metadata_uri,
                "transactionHash": mint_result["transactionHash"],
                "status": SBTStatus.MINTED
            }
            
        except Exception as e:
            # Mark as failed
            await self.sbt_store.update(
                sbt_id=sbt_record.id,
                status=SBTStatus.PENDING,
                error=str(e)
            )
            raise
    
    async def revoke_sbt(
        self,
        sbt_id: str,
        reason: str,
        revoker: str
    ) -> Dict[str, Any]:
        """
        Revoke a SoulBound Token
        
        Args:
            sbt_id: SBT ID to revoke
            reason: Revocation reason
            revoker: DID or address of the revoker
            
        Returns:
            Revocation details
        """
        # Get SBT record
        sbt = await self.sbt_store.get(sbt_id)
        if not sbt:
            raise ValueError(f"SBT {sbt_id} not found")
        
        if sbt.status == SBTStatus.REVOKED:
            raise ValueError(f"SBT {sbt_id} already revoked")
        
        # Check revocation permissions
        if not await self._can_revoke(sbt, revoker):
            raise PermissionError(f"{revoker} cannot revoke this SBT")
        
        try:
            # Revoke on blockchain
            revoke_result = await self._revoke_on_chain(
                chain=sbt.chain,
                token_id=sbt.token_id,
                reason=reason
            )
            
            # Update record
            await self.sbt_store.update(
                sbt_id=sbt_id,
                status=SBTStatus.REVOKED,
                revocation_date=datetime.now(timezone.utc),
                revocation_reason=reason,
                revoked_by=revoker,
                revocation_tx_hash=revoke_result["transactionHash"]
            )
            
            # Update statistics
            self.total_revoked += 1
            
            # Publish event
            await self._publish_event("sbt_revoked", {
                "sbt_id": sbt_id,
                "token_id": sbt.token_id,
                "chain": sbt.chain,
                "reason": reason,
                "revoker": revoker,
                "transaction_hash": revoke_result["transactionHash"]
            })
            
            return {
                "sbtId": sbt_id,
                "tokenId": sbt.token_id,
                "status": SBTStatus.REVOKED,
                "revocationDate": datetime.now(timezone.utc).isoformat(),
                "reason": reason,
                "transactionHash": revoke_result["transactionHash"]
            }
            
        except Exception as e:
            # Log revocation failure
            await self._publish_event("sbt_revocation_failed", {
                "sbt_id": sbt_id,
                "error": str(e)
            })
            raise
    
    async def burn_sbt(
        self,
        sbt_id: str,
        burner: str
    ) -> Dict[str, Any]:
        """
        Burn a SoulBound Token (permanent deletion)
        
        Args:
            sbt_id: SBT ID to burn
            burner: Address requesting the burn
            
        Returns:
            Burn details
        """
        # Get SBT record
        sbt = await self.sbt_store.get(sbt_id)
        if not sbt:
            raise ValueError(f"SBT {sbt_id} not found")
        
        # Only the recipient can burn their own SBT
        if sbt.recipient.lower() != burner.lower():
            raise PermissionError("Only the SBT owner can burn it")
        
        if sbt.status == SBTStatus.BURNED:
            raise ValueError(f"SBT {sbt_id} already burned")
        
        try:
            # Burn on blockchain
            burn_result = await self._burn_on_chain(
                chain=sbt.chain,
                token_id=sbt.token_id,
                owner=burner
            )
            
            # Update record
            await self.sbt_store.update(
                sbt_id=sbt_id,
                status=SBTStatus.BURNED,
                burn_date=datetime.now(timezone.utc),
                burn_tx_hash=burn_result["transactionHash"]
            )
            
            # Publish event
            await self._publish_event("sbt_burned", {
                "sbt_id": sbt_id,
                "token_id": sbt.token_id,
                "chain": sbt.chain,
                "burner": burner,
                "transaction_hash": burn_result["transactionHash"]
            })
            
            return {
                "sbtId": sbt_id,
                "tokenId": sbt.token_id,
                "status": SBTStatus.BURNED,
                "burnDate": datetime.now(timezone.utc).isoformat(),
                "transactionHash": burn_result["transactionHash"]
            }
            
        except Exception as e:
            # Log burn failure
            await self._publish_event("sbt_burn_failed", {
                "sbt_id": sbt_id,
                "error": str(e)
            })
            raise
    
    async def record_transfer_attempt(
        self,
        sbt_id: str,
        from_address: str,
        to_address: str,
        transaction_hash: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Record a blocked transfer attempt
        
        Args:
            sbt_id: SBT that was attempted to transfer
            from_address: Source address
            to_address: Destination address
            transaction_hash: Transaction hash if available
            
        Returns:
            Transfer attempt record
        """
        # Get SBT record
        sbt = await self.sbt_store.get(sbt_id)
        if not sbt:
            raise ValueError(f"SBT {sbt_id} not found")
        
        # Record transfer attempt
        attempt = await self.sbt_store.record_transfer_attempt(
            sbt_id=sbt_id,
            from_address=from_address,
            to_address=to_address,
            result=TransferAttemptResult.BLOCKED,
            transaction_hash=transaction_hash
        )
        
        # Update statistics
        self.total_transfer_attempts += 1
        
        # Publish event
        await self._publish_event("sbt_transfer_blocked", {
            "sbt_id": sbt_id,
            "token_id": sbt.token_id,
            "from": from_address,
            "to": to_address,
            "transaction_hash": transaction_hash
        })
        
        return {
            "attemptId": attempt.id,
            "sbtId": sbt_id,
            "tokenId": sbt.token_id,
            "from": from_address,
            "to": to_address,
            "result": TransferAttemptResult.BLOCKED,
            "timestamp": attempt.timestamp.isoformat()
        }
    
    async def get_sbt_by_credential(self, credential_id: str) -> Optional[Dict[str, Any]]:
        """Get SBT by credential ID"""
        sbt = await self.sbt_store.get_by_credential_id(credential_id)
        if not sbt:
            return None
        
        return self._format_sbt(sbt)
    
    async def get_sbts_by_recipient(
        self,
        recipient: str,
        chain: Optional[str] = None,
        status: Optional[SBTStatus] = None
    ) -> List[Dict[str, Any]]:
        """Get all SBTs for a recipient"""
        sbts = await self.sbt_store.get_by_recipient(recipient, chain, status)
        return [self._format_sbt(sbt) for sbt in sbts]
    
    async def get_sbts_by_issuer(
        self,
        issuer: str,
        chain: Optional[str] = None,
        status: Optional[SBTStatus] = None
    ) -> List[Dict[str, Any]]:
        """Get all SBTs issued by an issuer"""
        sbts = await self.sbt_store.get_by_issuer(issuer, chain, status)
        return [self._format_sbt(sbt) for sbt in sbts]
    
    async def verify_sbt_ownership(
        self,
        sbt_id: str,
        address: str
    ) -> bool:
        """Verify if an address owns an SBT"""
        sbt = await self.sbt_store.get(sbt_id)
        if not sbt or sbt.status not in [SBTStatus.MINTED, SBTStatus.ACTIVE]:
            return False
        
        # Verify on-chain ownership
        return await self._verify_on_chain_ownership(
            chain=sbt.chain,
            token_id=sbt.token_id,
            address=address
        )
    
    async def update_metadata(
        self,
        sbt_id: str,
        metadata_updates: Dict[str, Any],
        updater: str
    ) -> Dict[str, Any]:
        """
        Update SBT metadata (off-chain only)
        
        Args:
            sbt_id: SBT to update
            metadata_updates: Metadata fields to update
            updater: DID or address of updater
            
        Returns:
            Updated SBT details
        """
        # Get SBT record
        sbt = await self.sbt_store.get(sbt_id)
        if not sbt:
            raise ValueError(f"SBT {sbt_id} not found")
        
        # Check update permissions
        if not await self._can_update_metadata(sbt, updater):
            raise PermissionError(f"{updater} cannot update this SBT's metadata")
        
        # Merge metadata
        updated_metadata = {**sbt.metadata, **metadata_updates}
        updated_metadata["lastUpdated"] = datetime.now(timezone.utc).isoformat()
        updated_metadata["updatedBy"] = updater
        
        # Upload new metadata
        new_metadata_uri = await self._upload_metadata(updated_metadata)
        
        # Update record
        await self.sbt_store.update(
            sbt_id=sbt_id,
            metadata=updated_metadata,
            metadata_uri=new_metadata_uri
        )
        
        # Publish event
        await self._publish_event("sbt_metadata_updated", {
            "sbt_id": sbt_id,
            "updater": updater,
            "fields_updated": list(metadata_updates.keys())
        })
        
        return {
            "sbtId": sbt_id,
            "metadataUri": new_metadata_uri,
            "updatedFields": list(metadata_updates.keys()),
            "updatedAt": datetime.now(timezone.utc).isoformat()
        }
    
    # Helper methods
    
    async def _get_credential(self, credential_id: str) -> Optional[Dict[str, Any]]:
        """Get credential from credential service"""
        try:
            response = await self.http_client.get(
                f"{self.credential_service_url}/api/v1/credentials/{credential_id}"
            )
            
            if response.status_code == 200:
                return response.json()
            
            return None
            
        except Exception as e:
            print(f"Failed to get credential: {str(e)}")
            return None
    
    async def _upload_metadata(self, metadata: Dict[str, Any]) -> str:
        """Upload metadata to storage service"""
        try:
            response = await self.http_client.post(
                f"{self.storage_service_url}/api/v1/metadata",
                json={
                    "type": "sbt_metadata",
                    "content": metadata
                }
            )
            
            if response.status_code == 201:
                result = response.json()
                return result["uri"]
            
            raise RuntimeError(f"Failed to upload metadata: {response.text}")
            
        except Exception as e:
            print(f"Failed to upload metadata: {str(e)}")
            raise
    
    async def _mint_on_chain(
        self,
        chain: str,
        recipient: str,
        metadata_uri: str,
        sbt_id: str
    ) -> Dict[str, Any]:
        """Mint SBT on blockchain"""
        try:
            response = await self.http_client.post(
                f"{self.blockchain_connector_url}/api/v1/contracts/execute",
                json={
                    "chain": chain,
                    "contractAddress": self.contract_addresses[chain],
                    "method": "mintSBT",
                    "params": {
                        "to": recipient,
                        "uri": metadata_uri,
                        "sbtId": sbt_id
                    }
                }
            )
            
            if response.status_code == 200:
                return response.json()
            
            raise RuntimeError(f"Blockchain mint failed: {response.text}")
            
        except Exception as e:
            print(f"Failed to mint on chain: {str(e)}")
            raise
    
    async def _revoke_on_chain(
        self,
        chain: str,
        token_id: str,
        reason: str
    ) -> Dict[str, Any]:
        """Revoke SBT on blockchain"""
        try:
            response = await self.http_client.post(
                f"{self.blockchain_connector_url}/api/v1/contracts/execute",
                json={
                    "chain": chain,
                    "contractAddress": self.contract_addresses[chain],
                    "method": "revokeSBT",
                    "params": {
                        "tokenId": token_id,
                        "reason": reason
                    }
                }
            )
            
            if response.status_code == 200:
                return response.json()
            
            raise RuntimeError(f"Blockchain revoke failed: {response.text}")
            
        except Exception as e:
            print(f"Failed to revoke on chain: {str(e)}")
            raise
    
    async def _burn_on_chain(
        self,
        chain: str,
        token_id: str,
        owner: str
    ) -> Dict[str, Any]:
        """Burn SBT on blockchain"""
        try:
            response = await self.http_client.post(
                f"{self.blockchain_connector_url}/api/v1/contracts/execute",
                json={
                    "chain": chain,
                    "contractAddress": self.contract_addresses[chain],
                    "method": "burnSBT",
                    "params": {
                        "tokenId": token_id
                    },
                    "from": owner
                }
            )
            
            if response.status_code == 200:
                return response.json()
            
            raise RuntimeError(f"Blockchain burn failed: {response.text}")
            
        except Exception as e:
            print(f"Failed to burn on chain: {str(e)}")
            raise
    
    async def _verify_on_chain_ownership(
        self,
        chain: str,
        token_id: str,
        address: str
    ) -> bool:
        """Verify token ownership on blockchain"""
        try:
            response = await self.http_client.post(
                f"{self.blockchain_connector_url}/api/v1/contracts/call",
                json={
                    "chain": chain,
                    "contractAddress": self.contract_addresses[chain],
                    "method": "ownerOf",
                    "params": {
                        "tokenId": token_id
                    }
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                owner = result.get("result", "").lower()
                return owner == address.lower()
            
            return False
            
        except Exception:
            return False
    
    async def _can_revoke(self, sbt: Any, revoker: str) -> bool:
        """Check if address can revoke SBT"""
        # Issuer can always revoke
        if sbt.issuer.lower() == revoker.lower():
            return True
        
        # Check if revoker has admin role
        # This would check against governance contracts or role management
        return False
    
    async def _can_update_metadata(self, sbt: Any, updater: str) -> bool:
        """Check if address can update metadata"""
        # Issuer can update
        if sbt.issuer.lower() == updater.lower():
            return True
        
        # Recipient can update certain fields
        if sbt.recipient.lower() == updater.lower():
            return True
        
        return False
    
    def _format_sbt(self, sbt: Any) -> Dict[str, Any]:
        """Format SBT record for API response"""
        return {
            "id": sbt.id,
            "tokenId": sbt.token_id,
            "credentialId": sbt.credential_id,
            "chain": sbt.chain,
            "contractAddress": sbt.contract_address,
            "recipient": sbt.recipient,
            "issuer": sbt.issuer,
            "metadataUri": sbt.metadata_uri,
            "metadata": sbt.metadata,
            "status": sbt.status,
            "mintedAt": sbt.minted_at.isoformat() if sbt.minted_at else None,
            "transactionHash": sbt.transaction_hash,
            "revocationDate": sbt.revocation_date.isoformat() if sbt.revocation_date else None,
            "revocationReason": sbt.revocation_reason
        }
    
    async def _publish_event(self, event_type: str, data: Dict[str, Any]):
        """Publish event to event bus"""
        if self.event_publisher:
            await self.event_publisher.publish(event_type, data)
    
    async def check_blockchain_connection(self) -> bool:
        """Check if blockchain connector is accessible"""
        try:
            response = await self.http_client.get(
                f"{self.blockchain_connector_url}/health"
            )
            return response.status_code == 200
        except Exception:
            return False
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get SBT service statistics"""
        stats = await self.sbt_store.get_statistics()
        
        return {
            "total_minted": self.total_minted,
            "total_revoked": self.total_revoked,
            "total_transfer_attempts": self.total_transfer_attempts,
            "database_stats": stats,
            "supported_chains": list(self.contract_addresses.keys())
        } 