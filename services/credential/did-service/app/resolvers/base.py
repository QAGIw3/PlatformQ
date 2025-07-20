"""
Base DID Resolver Interface
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime


class BaseDIDResolver(ABC):
    """Abstract base class for DID method resolvers"""
    
    def __init__(self, method: str):
        self.method = method
    
    @abstractmethod
    async def create(
        self,
        key_type: str = "Ed25519",
        options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a new DID
        
        Args:
            key_type: Type of cryptographic key
            options: Method-specific options
            
        Returns:
            Dict containing:
            - did: The created DID
            - document: The initial DID document
            - keys: Key information (private keys never returned)
        """
        pass
    
    @abstractmethod
    async def resolve(self, did: str) -> Optional[Dict[str, Any]]:
        """
        Resolve a DID to its document
        
        Args:
            did: The DID to resolve
            
        Returns:
            The DID document or None if not found
        """
        pass
    
    @abstractmethod
    async def update(
        self,
        did: str,
        operations: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Update a DID document
        
        Args:
            did: The DID to update
            operations: List of update operations
            
        Returns:
            The updated DID document
        """
        pass
    
    @abstractmethod
    async def deactivate(self, did: str) -> bool:
        """
        Deactivate a DID
        
        Args:
            did: The DID to deactivate
            
        Returns:
            True if successful
        """
        pass
    
    def validate_did(self, did: str) -> bool:
        """
        Validate DID format
        
        Args:
            did: The DID to validate
            
        Returns:
            True if valid
        """
        parts = did.split(":")
        if len(parts) < 3:
            return False
        
        if parts[0] != "did":
            return False
            
        if parts[1] != self.method:
            return False
            
        return True
    
    def create_did_document(
        self,
        did: str,
        public_key: bytes,
        key_type: str = "Ed25519",
        **kwargs
    ) -> Dict[str, Any]:
        """
        Create a basic DID document
        
        Args:
            did: The DID
            public_key: Public key bytes
            key_type: Type of key
            **kwargs: Additional properties
            
        Returns:
            DID document
        """
        key_id = f"{did}#key-1"
        
        # Base document structure
        document = {
            "@context": [
                "https://www.w3.org/ns/did/v1",
                "https://w3id.org/security/v2"
            ],
            "id": did,
            "verificationMethod": [{
                "id": key_id,
                "type": self._get_verification_method_type(key_type),
                "controller": did,
                "publicKeyBase58": self._encode_public_key(public_key, key_type)
            }],
            "authentication": [key_id],
            "assertionMethod": [key_id],
            "created": datetime.utcnow().isoformat() + "Z",
            "updated": datetime.utcnow().isoformat() + "Z"
        }
        
        # Add any additional properties
        for key, value in kwargs.items():
            if key not in document:
                document[key] = value
                
        return document
    
    def _get_verification_method_type(self, key_type: str) -> str:
        """Get the verification method type for a key type"""
        type_map = {
            "Ed25519": "Ed25519VerificationKey2020",
            "secp256k1": "EcdsaSecp256k1VerificationKey2019",
            "P-256": "JsonWebKey2020",
            "RSA": "RsaVerificationKey2018"
        }
        return type_map.get(key_type, "JsonWebKey2020")
    
    def _encode_public_key(self, public_key: bytes, key_type: str) -> str:
        """Encode public key based on type"""
        import base58
        import base64
        
        if key_type == "Ed25519":
            return base58.b58encode(public_key).decode()
        else:
            return base64.b64encode(public_key).decode()
    
    def add_service_endpoint(
        self,
        document: Dict[str, Any],
        service_id: str,
        service_type: str,
        service_endpoint: str
    ) -> Dict[str, Any]:
        """Add a service endpoint to DID document"""
        if "service" not in document:
            document["service"] = []
            
        document["service"].append({
            "id": f"{document['id']}#{service_id}",
            "type": service_type,
            "serviceEndpoint": service_endpoint
        })
        
        return document 