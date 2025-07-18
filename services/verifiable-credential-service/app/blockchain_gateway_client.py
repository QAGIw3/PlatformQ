"""
Blockchain Gateway Client

Client for interacting with blockchain-gateway-service instead of direct blockchain integration.
"""

import logging
import httpx
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class ChainType(Enum):
    """Supported blockchain types"""
    ETHEREUM = "ethereum"
    POLYGON = "polygon"
    ARBITRUM = "arbitrum"
    OPTIMISM = "optimism"
    AVALANCHE = "avalanche"
    BSC = "bsc"
    SOLANA = "solana"
    COSMOS = "cosmos"
    POLKADOT = "polkadot"


@dataclass
class AnchorResult:
    """Result of anchoring a credential on blockchain"""
    credential_id: str
    credential_hash: str
    transaction_hash: str
    block_number: int
    timestamp: datetime
    chain_type: ChainType
    gas_used: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "credentialId": self.credential_id,
            "credentialHash": self.credential_hash,
            "transactionHash": self.transaction_hash,
            "blockNumber": self.block_number,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "chainType": self.chain_type.value,
            "gasUsed": self.gas_used
        }


class BlockchainGatewayClient:
    """Client for interacting with blockchain-gateway-service"""
    
    def __init__(self, gateway_url: str = "http://blockchain-gateway-service:8000"):
        self.gateway_url = gateway_url.rstrip("/")
        self.client = httpx.AsyncClient(timeout=30.0)
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()
        
    async def anchor_credential(
        self,
        credential: Dict[str, Any],
        chain: str = "ethereum",
        tenant_id: Optional[str] = None
    ) -> AnchorResult:
        """
        Anchor a verifiable credential on blockchain via gateway.
        
        Args:
            credential: The verifiable credential to anchor
            chain: Target blockchain (default: ethereum)
            tenant_id: Optional tenant identifier
            
        Returns:
            AnchorResult with transaction details
        """
        try:
            # Prepare anchor request
            request_data = {
                "credential_id": credential.get("id"),
                "credential_data": credential,
                "chain": chain,
                "tenant_id": tenant_id,
                "anchor_type": "credential"
            }
            
            # Call blockchain gateway
            response = await self.client.post(
                f"{self.gateway_url}/api/v1/anchor",
                json=request_data
            )
            response.raise_for_status()
            
            result = response.json()
            
            # Convert to AnchorResult
            return AnchorResult(
                credential_id=result["credential_id"],
                credential_hash=result["hash"],
                transaction_hash=result["transaction_hash"],
                block_number=result["block_number"],
                timestamp=datetime.fromisoformat(result["timestamp"]),
                chain_type=ChainType(chain),
                gas_used=result.get("gas_used")
            )
            
        except httpx.HTTPError as e:
            logger.error(f"Error anchoring credential via gateway: {e}")
            raise
            
    async def verify_anchor(
        self,
        credential_id: str,
        chain: str = "ethereum"
    ) -> Optional[AnchorResult]:
        """
        Verify a credential anchor exists on blockchain.
        
        Args:
            credential_id: The credential ID to verify
            chain: Target blockchain
            
        Returns:
            AnchorResult if found, None otherwise
        """
        try:
            response = await self.client.get(
                f"{self.gateway_url}/api/v1/verify/{chain}/{credential_id}"
            )
            
            if response.status_code == 404:
                return None
                
            response.raise_for_status()
            result = response.json()
            
            return AnchorResult(
                credential_id=result["credential_id"],
                credential_hash=result["hash"],
                transaction_hash=result["transaction_hash"],
                block_number=result["block_number"],
                timestamp=datetime.fromisoformat(result["timestamp"]),
                chain_type=ChainType(chain),
                gas_used=result.get("gas_used")
            )
            
        except httpx.HTTPError as e:
            logger.error(f"Error verifying credential anchor: {e}")
            return None
            
    async def anchor_multiple_chains(
        self,
        credential: Dict[str, Any],
        chains: List[str],
        tenant_id: Optional[str] = None
    ) -> Dict[str, AnchorResult]:
        """
        Anchor credential on multiple blockchains.
        
        Args:
            credential: The verifiable credential to anchor
            chains: List of target blockchains
            tenant_id: Optional tenant identifier
            
        Returns:
            Dictionary mapping chain to AnchorResult
        """
        results = {}
        
        for chain in chains:
            try:
                result = await self.anchor_credential(credential, chain, tenant_id)
                results[chain] = result
            except Exception as e:
                logger.error(f"Failed to anchor on {chain}: {e}")
                # Continue with other chains
                
        return results
        
    async def create_soulbound_token(
        self,
        credential: Dict[str, Any],
        owner_address: str,
        chain: str = "ethereum",
        metadata_uri: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a SoulBound Token for a credential.
        
        Args:
            credential: The verifiable credential
            owner_address: The token owner's address
            chain: Target blockchain
            metadata_uri: Optional IPFS URI for metadata
            
        Returns:
            SBT creation result
        """
        try:
            request_data = {
                "credential_id": credential.get("id"),
                "credential_data": credential,
                "owner_address": owner_address,
                "chain": chain,
                "metadata_uri": metadata_uri
            }
            
            response = await self.client.post(
                f"{self.gateway_url}/api/v1/sbt/mint",
                json=request_data
            )
            response.raise_for_status()
            
            return response.json()
            
        except httpx.HTTPError as e:
            logger.error(f"Error creating SoulBound Token: {e}")
            raise
            
    async def revoke_credential(
        self,
        credential_id: str,
        reason: str,
        chain: str = "ethereum"
    ) -> Dict[str, Any]:
        """
        Revoke a credential on blockchain.
        
        Args:
            credential_id: The credential to revoke
            reason: Revocation reason
            chain: Target blockchain
            
        Returns:
            Revocation transaction details
        """
        try:
            request_data = {
                "credential_id": credential_id,
                "reason": reason,
                "chain": chain
            }
            
            response = await self.client.post(
                f"{self.gateway_url}/api/v1/revoke",
                json=request_data
            )
            response.raise_for_status()
            
            return response.json()
            
        except httpx.HTTPError as e:
            logger.error(f"Error revoking credential: {e}")
            raise
            
    async def get_gas_estimate(
        self,
        operation: str,
        chain: str = "ethereum"
    ) -> Dict[str, Any]:
        """
        Get gas estimate for an operation.
        
        Args:
            operation: Type of operation (anchor, mint_sbt, revoke)
            chain: Target blockchain
            
        Returns:
            Gas estimate details
        """
        try:
            response = await self.client.get(
                f"{self.gateway_url}/api/v1/gas/estimate",
                params={"operation": operation, "chain": chain}
            )
            response.raise_for_status()
            
            return response.json()
            
        except httpx.HTTPError as e:
            logger.error(f"Error getting gas estimate: {e}")
            raise
            
    async def get_supported_chains(self) -> List[str]:
        """Get list of supported blockchain networks"""
        try:
            response = await self.client.get(
                f"{self.gateway_url}/api/v1/chains"
            )
            response.raise_for_status()
            
            return response.json()["chains"]
            
        except httpx.HTTPError as e:
            logger.error(f"Error getting supported chains: {e}")
            return [] 