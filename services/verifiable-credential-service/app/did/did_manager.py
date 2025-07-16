from typing import Dict, Any, List, Optional
from datetime import datetime
import json
import uuid
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa
from cryptography.hazmat.backends import default_backend
import base58
import logging

logger = logging.getLogger(__name__)

class DIDDocument:
    """W3C DID Document representation"""
    
    def __init__(self, did: str):
        self.did = did
        self.document = {
            "@context": [
                "https://www.w3.org/ns/did/v1",
                "https://w3id.org/security/v1"
            ],
            "id": did,
            "created": datetime.utcnow().isoformat() + "Z",
            "updated": datetime.utcnow().isoformat() + "Z",
            "verificationMethod": [],
            "authentication": [],
            "assertionMethod": [],
            "keyAgreement": [],
            "capabilityInvocation": [],
            "capabilityDelegation": [],
            "service": []
        }
    
    def add_verification_method(self, key_id: str, key_type: str, 
                              controller: str, public_key: bytes):
        """Add a verification method to the DID document"""
        method = {
            "id": f"{self.did}#{key_id}",
            "type": key_type,
            "controller": controller,
            "publicKeyBase58": base58.b58encode(public_key).decode()
        }
        
        self.document["verificationMethod"].append(method)
        
        # Add to authentication by default
        self.document["authentication"].append(method["id"])
        self.document["assertionMethod"].append(method["id"])
        
        return method
    
    def add_service(self, service_id: str, service_type: str, 
                   service_endpoint: str, **kwargs):
        """Add a service endpoint to the DID document"""
        service = {
            "id": f"{self.did}#{service_id}",
            "type": service_type,
            "serviceEndpoint": service_endpoint,
            **kwargs
        }
        
        self.document["service"].append(service)
        return service
    
    def to_dict(self) -> Dict[str, Any]:
        """Return the DID document as a dictionary"""
        return self.document
    
    def to_json(self) -> str:
        """Return the DID document as JSON"""
        return json.dumps(self.document, indent=2)

class DIDManager:
    """Manager for creating and managing DIDs"""
    
    def __init__(self, default_method: str = "key"):
        self.default_method = default_method
        self.methods = {}
        self._register_default_methods()
        
    def _register_default_methods(self):
        """Register default DID methods"""
        from .did_methods import DIDMethodKey, DIDMethodWeb, DIDMethodEthr
        
        self.register_method("key", DIDMethodKey())
        self.register_method("web", DIDMethodWeb())
        self.register_method("ethr", DIDMethodEthr())
    
    def register_method(self, method_name: str, method_handler):
        """Register a DID method handler"""
        self.methods[method_name] = method_handler
        logger.info(f"Registered DID method: {method_name}")
    
    def create_did(self, method: Optional[str] = None, 
                  options: Optional[Dict[str, Any]] = None) -> DIDDocument:
        """Create a new DID using the specified method"""
        method = method or self.default_method
        options = options or {}
        
        if method not in self.methods:
            raise ValueError(f"Unknown DID method: {method}")
        
        handler = self.methods[method]
        did = handler.create_did(options)
        
        logger.info(f"Created DID: {did.did}")
        return did
    
    def generate_key_pair(self, key_type: str = "Ed25519VerificationKey2020"):
        """Generate a cryptographic key pair"""
        if key_type == "Ed25519VerificationKey2020":
            private_key = ed25519.Ed25519PrivateKey.generate()
            public_key = private_key.public_key()
            
            private_bytes = private_key.private_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PrivateFormat.Raw,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            public_bytes = public_key.public_bytes(
                encoding=serialization.Encoding.Raw,
                format=serialization.PublicFormat.Raw
            )
            
            return {
                "type": key_type,
                "private_key": private_bytes,
                "public_key": public_bytes,
                "private_key_base58": base58.b58encode(private_bytes).decode(),
                "public_key_base58": base58.b58encode(public_bytes).decode()
            }
            
        elif key_type == "RsaVerificationKey2018":
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend()
            )
            public_key = private_key.public_key()
            
            private_pem = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            public_pem = public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
            
            return {
                "type": key_type,
                "private_key": private_pem,
                "public_key": public_pem
            }
        
        else:
            raise ValueError(f"Unsupported key type: {key_type}")
    
    def create_did_for_tenant(self, tenant_id: str, 
                            method: Optional[str] = None) -> DIDDocument:
        """Create a DID specifically for a tenant"""
        options = {
            "tenant_id": tenant_id,
            "path": f"tenants/{tenant_id}"
        }
        
        did_doc = self.create_did(method=method or "web", options=options)
        
        # Add tenant-specific service endpoints
        did_doc.add_service(
            service_id="credentials",
            service_type="CredentialService",
            service_endpoint=f"https://platformq.com/api/v1/tenants/{tenant_id}/credentials"
        )
        
        did_doc.add_service(
            service_id="hub",
            service_type="IdentityHub",
            service_endpoint=f"https://platformq.com/api/v1/tenants/{tenant_id}/hub"
        )
        
        return did_doc
    
    def create_did_for_user(self, tenant_id: str, user_id: str,
                          method: Optional[str] = None) -> DIDDocument:
        """Create a DID for a specific user within a tenant"""
        options = {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "path": f"tenants/{tenant_id}/users/{user_id}"
        }
        
        did_doc = self.create_did(method=method or "web", options=options)
        
        # Generate a key pair for the user
        key_pair = self.generate_key_pair()
        
        # Add verification method
        did_doc.add_verification_method(
            key_id="primary",
            key_type=key_pair["type"],
            controller=did_doc.did,
            public_key=key_pair["public_key"]
        )
        
        # Add user-specific service endpoints
        did_doc.add_service(
            service_id="profile",
            service_type="UserProfile", 
            service_endpoint=f"https://platformq.com/api/v1/users/{user_id}/profile"
        )
        
        return did_doc, key_pair
    
    def update_did_document(self, did: str, updates: Dict[str, Any]) -> DIDDocument:
        """Update an existing DID document"""
        # In a real implementation, this would fetch the existing document
        # from storage and apply updates
        did_doc = DIDDocument(did)
        
        # Apply updates
        for key, value in updates.items():
            if key in did_doc.document:
                did_doc.document[key] = value
        
        did_doc.document["updated"] = datetime.utcnow().isoformat() + "Z"
        
        logger.info(f"Updated DID document: {did}")
        return did_doc
    
    def deactivate_did(self, did: str, reason: Optional[str] = None) -> bool:
        """Deactivate a DID"""
        # In a real implementation, this would update the DID status
        # on the appropriate ledger or registry
        logger.info(f"Deactivated DID: {did} (reason: {reason})")
        return True 