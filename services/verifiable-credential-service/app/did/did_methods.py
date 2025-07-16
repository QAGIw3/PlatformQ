from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import uuid
import base58
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.backends import default_backend
import hashlib
import logging

from .did_manager import DIDDocument

logger = logging.getLogger(__name__)

class DIDMethod(ABC):
    """Abstract base class for DID methods"""
    
    @abstractmethod
    def create_did(self, options: Dict[str, Any]) -> DIDDocument:
        """Create a new DID using this method"""
        pass
    
    @abstractmethod
    def resolve_did(self, did: str) -> Optional[DIDDocument]:
        """Resolve a DID to its document"""
        pass

class DIDMethodWeb(DIDMethod):
    """did:web method implementation"""
    
    def __init__(self, domain: str = "platformq.com"):
        self.domain = domain
    
    def create_did(self, options: Dict[str, Any]) -> DIDDocument:
        """Create a did:web DID"""
        path = options.get("path", str(uuid.uuid4()))
        
        # Construct the DID
        did = f"did:web:{self.domain}:{path.replace('/', ':')}"
        
        # Create DID document
        did_doc = DIDDocument(did)
        
        # Add controller if specified
        if "controller" in options:
            did_doc.document["controller"] = options["controller"]
        
        # Generate and add a default key
        private_key = ed25519.Ed25519PrivateKey.generate()
        public_key = private_key.public_key()
        public_bytes = public_key.public_bytes_raw()
        
        did_doc.add_verification_method(
            key_id="key-1",
            key_type="Ed25519VerificationKey2020",
            controller=did,
            public_key=public_bytes
        )
        
        logger.info(f"Created did:web DID: {did}")
        return did_doc
    
    def resolve_did(self, did: str) -> Optional[DIDDocument]:
        """Resolve a did:web DID by fetching from web server"""
        # Extract domain and path from DID
        parts = did.split(":")
        if len(parts) < 3 or parts[1] != "web":
            return None
        
        domain = parts[2]
        path = ":".join(parts[3:]).replace(":", "/") if len(parts) > 3 else ""
        
        # Construct URL
        url = f"https://{domain}/.well-known/did.json"
        if path:
            url = f"https://{domain}/{path}/did.json"
        
        # In a real implementation, fetch the document from the URL
        logger.info(f"Would resolve DID from: {url}")
        return None

class DIDMethodKey(DIDMethod):
    """did:key method implementation"""
    
    def create_did(self, options: Dict[str, Any]) -> DIDDocument:
        """Create a did:key DID"""
        # Generate Ed25519 key pair
        private_key = ed25519.Ed25519PrivateKey.generate()
        public_key = private_key.public_key()
        public_bytes = public_key.public_bytes_raw()
        
        # Create multicodec public key
        # 0xed = multicodec for Ed25519 public key
        multicodec_key = b'\xed\x01' + public_bytes
        
        # Base58 encode
        fingerprint = base58.b58encode(multicodec_key).decode()
        
        # Construct DID
        did = f"did:key:z{fingerprint}"
        
        # Create DID document
        did_doc = DIDDocument(did)
        
        # Add verification method
        did_doc.add_verification_method(
            key_id=f"z{fingerprint}",
            key_type="Ed25519VerificationKey2020",
            controller=did,
            public_key=public_bytes
        )
        
        # Store private key in options for caller
        options["private_key"] = private_key
        options["public_key"] = public_key
        
        logger.info(f"Created did:key DID: {did}")
        return did_doc
    
    def resolve_did(self, did: str) -> Optional[DIDDocument]:
        """Resolve a did:key DID (self-contained)"""
        parts = did.split(":")
        if len(parts) != 3 or parts[1] != "key":
            return None
        
        fingerprint = parts[2]
        if not fingerprint.startswith("z"):
            return None
        
        # Decode the public key
        try:
            multicodec_key = base58.b58decode(fingerprint[1:])
            if multicodec_key[0:2] != b'\xed\x01':
                return None
            
            public_bytes = multicodec_key[2:]
            
            # Reconstruct DID document
            did_doc = DIDDocument(did)
            did_doc.add_verification_method(
                key_id=fingerprint,
                key_type="Ed25519VerificationKey2020",
                controller=did,
                public_key=public_bytes
            )
            
            return did_doc
            
        except Exception as e:
            logger.error(f"Error resolving did:key: {e}")
            return None

class DIDMethodEthr(DIDMethod):
    """did:ethr method implementation for Ethereum"""
    
    def __init__(self, web3_client=None, registry_address: Optional[str] = None):
        self.web3_client = web3_client
        self.registry_address = registry_address
    
    def create_did(self, options: Dict[str, Any]) -> DIDDocument:
        """Create a did:ethr DID"""
        # Get or create Ethereum address
        if "address" in options:
            address = options["address"]
        else:
            # Generate new Ethereum account
            from eth_account import Account
            account = Account.create()
            address = account.address
            options["private_key"] = account.key
        
        # Construct DID
        network = options.get("network", "mainnet")
        if network == "mainnet":
            did = f"did:ethr:{address}"
        else:
            did = f"did:ethr:{network}:{address}"
        
        # Create DID document
        did_doc = DIDDocument(did)
        
        # Add Ethereum address as verification method
        did_doc.document["verificationMethod"].append({
            "id": f"{did}#controller",
            "type": "EcdsaSecp256k1RecoveryMethod2020",
            "controller": did,
            "blockchainAccountId": f"eip155:1:{address}"
        })
        
        did_doc.document["authentication"].append(f"{did}#controller")
        
        logger.info(f"Created did:ethr DID: {did}")
        return did_doc
    
    def resolve_did(self, did: str) -> Optional[DIDDocument]:
        """Resolve a did:ethr DID from Ethereum"""
        parts = did.split(":")
        if len(parts) < 3 or parts[1] != "ethr":
            return None
        
        # Extract network and address
        if len(parts) == 3:
            network = "mainnet"
            address = parts[2]
        else:
            network = parts[2]
            address = parts[3]
        
        # In a real implementation, query the EthereumDIDRegistry contract
        # For now, return a basic document
        did_doc = DIDDocument(did)
        
        did_doc.document["verificationMethod"].append({
            "id": f"{did}#controller",
            "type": "EcdsaSecp256k1RecoveryMethod2020",
            "controller": did,
            "blockchainAccountId": f"eip155:1:{address}"
        })
        
        did_doc.document["authentication"].append(f"{did}#controller")
        
        return did_doc
    
    def update_did(self, did: str, attribute: str, value: str) -> bool:
        """Update DID attributes on Ethereum"""
        if not self.web3_client or not self.registry_address:
            logger.error("Web3 client or registry address not configured")
            return False
        
        # In a real implementation, this would call the DID registry contract
        logger.info(f"Would update DID {did} attribute {attribute} on Ethereum")
        return True 