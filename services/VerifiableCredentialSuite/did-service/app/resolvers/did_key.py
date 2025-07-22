"""
did:key Resolver Implementation

did:key DIDs are deterministic - derived from public keys
"""

import base58
from typing import Dict, Any, Optional, List
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
import multibase
import multicodec

from .base import BaseDIDResolver


class DIDKeyResolver(BaseDIDResolver):
    """Resolver for did:key method"""
    
    def __init__(self, key_management_client):
        super().__init__("key")
        self.key_management_client = key_management_client
        
        # Multicodec prefixes for different key types
        self.key_prefixes = {
            "Ed25519": 0xed,  # ed25519-pub
            "secp256k1": 0xe7,  # secp256k1-pub
        }
    
    async def create(
        self,
        key_type: str = "Ed25519",
        options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a new did:key"""
        
        if key_type not in self.key_prefixes:
            raise ValueError(f"Unsupported key type: {key_type}")
        
        # Generate key via key management service
        key_response = await self._create_key(key_type, options)
        public_key = base58.b58decode(key_response["public_key"])
        
        # Create multicodec encoded public key
        prefix = self.key_prefixes[key_type]
        encoded_key = bytes([prefix]) + public_key
        
        # Create multibase encoded identifier
        identifier = multibase.encode('base58btc', encoded_key).decode()
        
        # Construct DID
        did = f"did:key:{identifier}"
        
        # Create DID document
        document = self.create_did_document(
            did=did,
            public_key=public_key,
            key_type=key_type
        )
        
        return {
            "did": did,
            "document": document,
            "keys": {
                "key_id": key_response["key_id"],
                "key_type": key_type,
                "public_key": key_response["public_key"]
            }
        }
    
    async def resolve(self, did: str) -> Optional[Dict[str, Any]]:
        """Resolve a did:key to its document"""
        
        if not self.validate_did(did):
            return None
        
        try:
            # Extract the multibase encoded identifier
            identifier = did.split(":")[-1]
            
            # Decode from multibase
            decoded = multibase.decode(identifier)
            
            # Extract key type from prefix
            if len(decoded) < 1:
                return None
                
            prefix = decoded[0]
            public_key = decoded[1:]
            
            # Determine key type from prefix
            key_type = None
            for kt, p in self.key_prefixes.items():
                if p == prefix:
                    key_type = kt
                    break
                    
            if not key_type:
                return None
            
            # Create DID document
            document = self.create_did_document(
                did=did,
                public_key=public_key,
                key_type=key_type
            )
            
            return document
            
        except Exception:
            return None
    
    async def update(
        self,
        did: str,
        operations: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Update a did:key document - not supported"""
        raise NotImplementedError("did:key DIDs are immutable")
    
    async def deactivate(self, did: str) -> bool:
        """Deactivate a did:key - not supported"""
        raise NotImplementedError("did:key DIDs cannot be deactivated")
    
    async def _create_key(
        self,
        key_type: str,
        options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create key via key management service"""
        
        # Call key management service to create key
        response = await self.key_management_client.post(
            "/api/v1/keys/create",
            json={
                "key_type": key_type,
                "key_alias": options.get("key_alias") if options else None,
                "metadata": {
                    "did_method": "key",
                    "purpose": "did_key"
                }
            }
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to create key: {response.text}")
            
        return response.json() 