"""
Utility functions for Verifiable Credentials
"""

import hashlib
import json
import uuid
import base64
from typing import Dict, Any, Optional, Union
from datetime import datetime
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ed25519, padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import base58


def create_credential_id(prefix: str = "urn:uuid") -> str:
    """Generate a unique credential ID"""
    return f"{prefix}:{uuid.uuid4()}"


def create_presentation_id(prefix: str = "urn:uuid") -> str:
    """Generate a unique presentation ID"""
    return f"{prefix}:{uuid.uuid4()}"


def canonicalize_credential(credential: Dict[str, Any]) -> str:
    """
    Canonicalize a credential for consistent hashing
    Uses deterministic JSON serialization
    """
    # Remove proof before canonicalization
    credential_copy = credential.copy()
    credential_copy.pop('proof', None)
    
    # Sort keys and serialize
    return json.dumps(credential_copy, sort_keys=True, separators=(',', ':'))


def hash_credential(credential: Dict[str, Any], algorithm: str = "sha256") -> str:
    """
    Create a hash of the credential
    
    Args:
        credential: The credential to hash
        algorithm: Hash algorithm to use (sha256, sha384, sha512)
        
    Returns:
        Hex-encoded hash string
    """
    canonical = canonicalize_credential(credential)
    
    if algorithm == "sha256":
        hasher = hashlib.sha256()
    elif algorithm == "sha384":
        hasher = hashlib.sha384()
    elif algorithm == "sha512":
        hasher = hashlib.sha512()
    else:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")
    
    hasher.update(canonical.encode('utf-8'))
    return hasher.hexdigest()


def create_did_document(did: str, public_key: bytes, key_type: str = "Ed25519") -> Dict[str, Any]:
    """
    Create a basic DID document
    
    Args:
        did: The DID identifier
        public_key: The public key bytes
        key_type: Type of key (Ed25519, secp256k1, etc.)
        
    Returns:
        DID document dict
    """
    key_id = f"{did}#key-1"
    
    # Encode public key based on type
    if key_type == "Ed25519":
        public_key_encoded = base58.b58encode(public_key).decode()
        verification_method_type = "Ed25519VerificationKey2020"
    else:
        public_key_encoded = base64.b64encode(public_key).decode()
        verification_method_type = "JsonWebKey2020"
    
    return {
        "@context": [
            "https://www.w3.org/ns/did/v1",
            "https://w3id.org/security/v2"
        ],
        "id": did,
        "verificationMethod": [{
            "id": key_id,
            "type": verification_method_type,
            "controller": did,
            "publicKeyBase58": public_key_encoded if key_type == "Ed25519" else None,
            "publicKeyJwk": {
                "kty": "OKP" if key_type == "Ed25519" else "EC",
                "crv": "Ed25519" if key_type == "Ed25519" else "secp256k1",
                "x": public_key_encoded
            } if key_type != "Ed25519" else None
        }],
        "authentication": [key_id],
        "assertionMethod": [key_id],
        "created": datetime.utcnow().isoformat() + "Z",
        "updated": datetime.utcnow().isoformat() + "Z"
    }


def sign_credential(
    credential: Dict[str, Any],
    private_key: bytes,
    verification_method: str,
    proof_type: str = "Ed25519Signature2020"
) -> Dict[str, Any]:
    """
    Sign a credential and attach proof
    
    Args:
        credential: The credential to sign
        private_key: Private key bytes
        verification_method: Verification method URL
        proof_type: Type of proof to create
        
    Returns:
        Credential with proof attached
    """
    # Canonicalize credential
    canonical = canonicalize_credential(credential)
    
    # Create signature based on proof type
    if proof_type == "Ed25519Signature2020":
        # Load Ed25519 private key
        private_key_obj = ed25519.Ed25519PrivateKey.from_private_bytes(private_key[:32])
        signature = private_key_obj.sign(canonical.encode('utf-8'))
        proof_value = base58.b58encode(signature).decode()
    else:
        raise ValueError(f"Unsupported proof type: {proof_type}")
    
    # Create proof
    proof = {
        "type": proof_type,
        "created": datetime.utcnow().isoformat() + "Z",
        "verificationMethod": verification_method,
        "proofPurpose": "assertionMethod",
        "proofValue": proof_value
    }
    
    # Attach proof to credential
    credential_with_proof = credential.copy()
    credential_with_proof["proof"] = proof
    
    return credential_with_proof


def verify_credential_signature(
    credential: Dict[str, Any],
    public_key: bytes,
    proof_type: str = "Ed25519Signature2020"
) -> bool:
    """
    Verify a credential signature
    
    Args:
        credential: The credential with proof
        public_key: Public key bytes
        proof_type: Expected proof type
        
    Returns:
        True if signature is valid, False otherwise
    """
    try:
        # Extract proof
        proof = credential.get("proof")
        if not proof:
            return False
        
        if proof.get("type") != proof_type:
            return False
        
        # Get canonical form without proof
        canonical = canonicalize_credential(credential)
        
        # Verify based on proof type
        if proof_type == "Ed25519Signature2020":
            # Decode signature
            signature = base58.b58decode(proof["proofValue"])
            
            # Load public key
            public_key_obj = ed25519.Ed25519PublicKey.from_public_bytes(public_key[:32])
            
            # Verify
            public_key_obj.verify(signature, canonical.encode('utf-8'))
            return True
        else:
            return False
            
    except Exception:
        return False


def verify_presentation_signature(
    presentation: Dict[str, Any],
    public_key: bytes,
    expected_challenge: Optional[str] = None
) -> bool:
    """
    Verify a presentation signature
    
    Args:
        presentation: The presentation with proof
        public_key: Public key bytes
        expected_challenge: Expected challenge value
        
    Returns:
        True if signature is valid, False otherwise
    """
    try:
        proof = presentation.get("proof")
        if not proof:
            return False
        
        # Verify challenge if provided
        if expected_challenge and proof.get("challenge") != expected_challenge:
            return False
        
        # Similar verification logic as credentials
        presentation_copy = presentation.copy()
        presentation_copy.pop('proof', None)
        canonical = json.dumps(presentation_copy, sort_keys=True, separators=(',', ':'))
        
        # Include challenge in signed data
        if proof.get("challenge"):
            canonical = f"{canonical}{proof['challenge']}"
        
        signature = base58.b58decode(proof["proofValue"])
        public_key_obj = ed25519.Ed25519PublicKey.from_public_bytes(public_key[:32])
        
        public_key_obj.verify(signature, canonical.encode('utf-8'))
        return True
        
    except Exception:
        return False


def calculate_credential_expiry(
    issuance_date: datetime,
    validity_days: int = 365
) -> datetime:
    """Calculate credential expiration date"""
    from datetime import timedelta
    return issuance_date + timedelta(days=validity_days)


def is_credential_expired(credential: Dict[str, Any]) -> bool:
    """Check if a credential has expired"""
    expiration = credential.get("expirationDate")
    if not expiration:
        return False
    
    if isinstance(expiration, str):
        # Parse ISO format
        expiration = datetime.fromisoformat(expiration.replace('Z', '+00:00'))
    
    return datetime.utcnow() > expiration


def extract_credential_claims(credential: Dict[str, Any]) -> Dict[str, Any]:
    """Extract claims from credential subject(s)"""
    subject = credential.get("credentialSubject", {})
    
    if isinstance(subject, list):
        # Multiple subjects - merge claims
        claims = {}
        for s in subject:
            if isinstance(s, dict):
                claims.update(s)
        return claims
    elif isinstance(subject, dict):
        return subject.copy()
    else:
        return {}


def merge_contexts(contexts: list) -> list:
    """Merge multiple JSON-LD contexts, removing duplicates while preserving order"""
    seen = set()
    result = []
    
    for context in contexts:
        if isinstance(context, str):
            if context not in seen:
                seen.add(context)
                result.append(context)
        elif isinstance(context, dict):
            # For dict contexts, use JSON representation as key
            key = json.dumps(context, sort_keys=True)
            if key not in seen:
                seen.add(key)
                result.append(context)
        elif isinstance(context, list):
            # Flatten nested lists
            for item in context:
                if isinstance(item, str) and item not in seen:
                    seen.add(item)
                    result.append(item)
    
    return result 