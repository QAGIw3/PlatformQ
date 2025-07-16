from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Any, Optional, List
from datetime import datetime
import hashlib
import json
import logging

logger = logging.getLogger(__name__)

class ChainType(Enum):
    ETHEREUM = "ethereum"
    POLYGON = "polygon"
    FABRIC = "fabric"
    SUBSTRATE = "substrate"

class CredentialAnchor:
    """Represents a credential anchored on blockchain"""
    def __init__(self, credential_id: str, credential_hash: str, 
                 transaction_hash: str, block_number: int, 
                 timestamp: datetime, chain_type: ChainType):
        self.credential_id = credential_id
        self.credential_hash = credential_hash
        self.transaction_hash = transaction_hash
        self.block_number = block_number
        self.timestamp = timestamp
        self.chain_type = chain_type
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "credential_id": self.credential_id,
            "credential_hash": self.credential_hash,
            "transaction_hash": self.transaction_hash,
            "block_number": self.block_number,
            "timestamp": self.timestamp.isoformat(),
            "chain_type": self.chain_type.value
        }

class BlockchainClient(ABC):
    """Abstract base class for blockchain clients"""
    
    def __init__(self, chain_type: ChainType, config: Dict[str, Any]):
        self.chain_type = chain_type
        self.config = config
        logger.info(f"Initializing {chain_type.value} blockchain client")
    
    @abstractmethod
    async def connect(self) -> bool:
        """Connect to the blockchain network"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the blockchain network"""
        pass
    
    @abstractmethod
    async def anchor_credential(self, credential: Dict[str, Any], 
                              tenant_id: str) -> CredentialAnchor:
        """Anchor a verifiable credential on the blockchain"""
        pass
    
    @abstractmethod
    async def verify_credential_anchor(self, credential_id: str) -> Optional[CredentialAnchor]:
        """Verify a credential anchor exists on chain"""
        pass
    
    @abstractmethod
    async def get_credential_history(self, credential_id: str) -> List[CredentialAnchor]:
        """Get the history of a credential (for revocations/updates)"""
        pass
    
    @abstractmethod
    async def revoke_credential(self, credential_id: str, reason: str) -> str:
        """Revoke a credential on chain"""
        pass
    
    def hash_credential(self, credential: Dict[str, Any]) -> str:
        """Create a hash of the credential for anchoring"""
        # Sort keys for consistent hashing
        credential_str = json.dumps(credential, sort_keys=True)
        return hashlib.sha256(credential_str.encode()).hexdigest()
    
    @abstractmethod
    async def deploy_smart_contract(self, contract_name: str, 
                                  contract_code: str) -> str:
        """Deploy a smart contract for credential verification"""
        pass
    
    @abstractmethod
    async def call_smart_contract(self, contract_address: str, 
                                method: str, params: List[Any]) -> Any:
        """Call a smart contract method"""
        pass 