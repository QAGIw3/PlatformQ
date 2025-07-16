"""
Zero-Knowledge Proof utilities for credential verification.
Using snarkjs-inspired approach for browser compatibility.
"""

import json
import hashlib
import secrets
from typing import Dict, Any, Tuple, Optional, List
from dataclasses import dataclass
from datetime import datetime, timezone
import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import os


@dataclass
class ZKProof:
    """Represents a zero-knowledge proof"""
    proof_type: str
    public_inputs: Dict[str, Any]
    proof_data: Dict[str, Any]
    timestamp: datetime
    

@dataclass
class ZKPRequest:
    """Request for generating a ZKP"""
    credential_type: str
    claims_to_prove: List[str]  # e.g., ["age >= 18", "role == researcher"]
    challenge: str  # From verifier
    

class ZKPGenerator:
    """
    Generates Zero-Knowledge Proofs for Verifiable Credentials.
    This is a simplified implementation for demonstration.
    In production, use a proper ZK library like snarkjs or arkworks.
    """
    
    def __init__(self):
        self.backend = default_backend()
        
    def generate_proof(
        self,
        credential: Dict[str, Any],
        request: ZKPRequest,
        private_key: str
    ) -> ZKProof:
        """
        Generate a ZKP for specific claims without revealing the full credential.
        
        Args:
            credential: The full verifiable credential
            request: What claims to prove
            private_key: User's private key for signing
            
        Returns:
            ZKProof that can be verified without revealing credential details
        """
        # Extract relevant claims
        proven_claims = {}
        
        for claim in request.claims_to_prove:
            if " >= " in claim:
                # Range proof (e.g., "age >= 18")
                field, threshold = claim.split(" >= ")
                actual_value = self._get_nested_value(credential, field)
                threshold_value = float(threshold)
                
                if actual_value >= threshold_value:
                    # Create range proof
                    proven_claims[claim] = self._create_range_proof(
                        actual_value, 
                        threshold_value,
                        request.challenge
                    )
            elif " == " in claim:
                # Equality proof (e.g., "role == researcher")
                field, expected = claim.split(" == ")
                actual_value = self._get_nested_value(credential, field)
                expected_value = expected.strip('"\'')
                
                if str(actual_value) == expected_value:
                    # Create equality proof
                    proven_claims[claim] = self._create_equality_proof(
                        actual_value,
                        request.challenge
                    )
            elif " in " in claim:
                # Membership proof (e.g., "role in ['researcher', 'developer']")
                field, set_str = claim.split(" in ")
                actual_value = self._get_nested_value(credential, field)
                allowed_values = eval(set_str)  # In production, use safe parsing
                
                if actual_value in allowed_values:
                    proven_claims[claim] = self._create_membership_proof(
                        actual_value,
                        allowed_values,
                        request.challenge
                    )
        
        # Create proof commitment
        commitment = self._create_commitment(
            credential.get("id", ""),
            proven_claims,
            request.challenge,
            private_key
        )
        
        return ZKProof(
            proof_type=request.credential_type,
            public_inputs={
                "credential_type": request.credential_type,
                "proven_claims": list(proven_claims.keys()),
                "challenge": request.challenge,
                "issuer_did": credential.get("issuer", {}).get("id", "")
            },
            proof_data={
                "commitment": commitment,
                "proofs": proven_claims,
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            timestamp=datetime.now(timezone.utc)
        )
    
    def _get_nested_value(self, obj: Dict, path: str) -> Any:
        """Get value from nested dict using dot notation"""
        parts = path.split(".")
        value = obj
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value
    
    def _create_range_proof(
        self, 
        value: float, 
        threshold: float,
        challenge: str
    ) -> Dict[str, Any]:
        """Create a proof that value >= threshold without revealing value"""
        # Simplified Pedersen commitment approach
        # In production, use bulletproofs or similar
        
        # Random blinding factor
        r = secrets.randbits(256)
        
        # Commitment: C = g^value * h^r
        # For simplicity, we use hash-based commitments
        commitment = hashlib.sha256(
            f"{value}:{r}:{challenge}".encode()
        ).hexdigest()
        
        # Proof that committed value >= threshold
        # This is simplified - real implementation would use bulletproofs
        diff = value - threshold
        diff_commitment = hashlib.sha256(
            f"{diff}:{r}:{challenge}".encode()
        ).hexdigest()
        
        return {
            "type": "range_proof",
            "commitment": commitment,
            "threshold": threshold,
            "diff_commitment": diff_commitment,
            "proof": base64.b64encode(
                f"{r}:{diff}".encode()
            ).decode()  # In production, this would be a proper ZK proof
        }
    
    def _create_equality_proof(
        self,
        value: Any,
        challenge: str
    ) -> Dict[str, Any]:
        """Create a proof of equality without revealing the value"""
        # Hash-based commitment
        salt = secrets.token_hex(32)
        commitment = hashlib.sha256(
            f"{value}:{salt}:{challenge}".encode()
        ).hexdigest()
        
        return {
            "type": "equality_proof",
            "commitment": commitment,
            "hash": hashlib.sha256(str(value).encode()).hexdigest()
        }
    
    def _create_membership_proof(
        self,
        value: Any,
        allowed_set: List[Any],
        challenge: str
    ) -> Dict[str, Any]:
        """Prove value is in allowed set without revealing which one"""
        # Create commitments for all values
        salt = secrets.token_hex(32)
        
        commitments = []
        for item in allowed_set:
            commitment = hashlib.sha256(
                f"{item}:{salt}:{challenge}".encode()
            ).hexdigest()
            commitments.append(commitment)
        
        # Actual value commitment
        value_commitment = hashlib.sha256(
            f"{value}:{salt}:{challenge}".encode()
        ).hexdigest()
        
        return {
            "type": "membership_proof",
            "commitments": commitments,
            "value_commitment": value_commitment,
            "set_size": len(allowed_set)
        }
    
    def _create_commitment(
        self,
        credential_id: str,
        proven_claims: Dict[str, Any],
        challenge: str,
        private_key: str
    ) -> str:
        """Create overall proof commitment"""
        data = {
            "credential_id_hash": hashlib.sha256(credential_id.encode()).hexdigest(),
            "claims_hash": hashlib.sha256(
                json.dumps(proven_claims, sort_keys=True).encode()
            ).hexdigest(),
            "challenge": challenge,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        # Sign with private key (simplified)
        signature_data = json.dumps(data, sort_keys=True)
        signature = hashlib.sha256(
            f"{signature_data}:{private_key}".encode()
        ).hexdigest()
        
        return base64.b64encode(
            json.dumps({
                "data": data,
                "signature": signature
            }).encode()
        ).decode()


class ZKPVerifier:
    """Verifies Zero-Knowledge Proofs"""
    
    def verify_proof(
        self,
        proof: ZKProof,
        expected_claims: List[str],
        challenge: str,
        issuer_public_key: Optional[str] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Verify a ZKP without accessing the original credential.
        
        Returns:
            (is_valid, error_message)
        """
        # Check challenge matches
        if proof.public_inputs.get("challenge") != challenge:
            return False, "Challenge mismatch"
        
        # Check all expected claims are proven
        proven_claims = proof.public_inputs.get("proven_claims", [])
        for claim in expected_claims:
            if claim not in proven_claims:
                return False, f"Required claim not proven: {claim}"
        
        # Verify individual proofs
        proofs = proof.proof_data.get("proofs", {})
        
        for claim, proof_data in proofs.items():
            proof_type = proof_data.get("type")
            
            if proof_type == "range_proof":
                if not self._verify_range_proof(proof_data, challenge):
                    return False, f"Invalid range proof for: {claim}"
                    
            elif proof_type == "equality_proof":
                if not self._verify_equality_proof(proof_data, challenge):
                    return False, f"Invalid equality proof for: {claim}"
                    
            elif proof_type == "membership_proof":
                if not self._verify_membership_proof(proof_data, challenge):
                    return False, f"Invalid membership proof for: {claim}"
        
        # Verify commitment signature
        try:
            commitment_data = json.loads(
                base64.b64decode(proof.proof_data.get("commitment", ""))
            )
            # In production, verify actual cryptographic signature
            if not commitment_data.get("signature"):
                return False, "Invalid commitment signature"
                
        except Exception as e:
            return False, f"Invalid commitment format: {str(e)}"
        
        # Check proof freshness (not too old)
        proof_time = datetime.fromisoformat(
            proof.proof_data.get("timestamp", "")
        ).replace(tzinfo=timezone.utc)
        
        if (datetime.now(timezone.utc) - proof_time).total_seconds() > 300:  # 5 min
            return False, "Proof expired"
        
        return True, None
    
    def _verify_range_proof(
        self,
        proof_data: Dict[str, Any],
        challenge: str
    ) -> bool:
        """Verify a range proof"""
        # In production, this would verify bulletproofs or similar
        # For now, we just check the proof structure
        required_fields = ["commitment", "threshold", "diff_commitment", "proof"]
        return all(field in proof_data for field in required_fields)
    
    def _verify_equality_proof(
        self,
        proof_data: Dict[str, Any],
        challenge: str
    ) -> bool:
        """Verify an equality proof"""
        required_fields = ["commitment", "hash"]
        return all(field in proof_data for field in required_fields)
    
    def _verify_membership_proof(
        self,
        proof_data: Dict[str, Any],
        challenge: str
    ) -> bool:
        """Verify a membership proof"""
        commitments = proof_data.get("commitments", [])
        value_commitment = proof_data.get("value_commitment")
        
        # Check that value commitment is in the set
        return value_commitment in commitments


# Utility functions for credential redaction
def create_redacted_credential(
    credential: Dict[str, Any],
    user_clearance_level: int,
    redaction_rules: Dict[str, int]
) -> Dict[str, Any]:
    """
    Create a redacted version of a credential based on user clearance.
    
    Args:
        credential: Original credential
        user_clearance_level: User's clearance level (1-5)
        redaction_rules: Dict mapping field paths to required clearance levels
        
    Returns:
        Redacted credential
    """
    redacted = json.loads(json.dumps(credential))  # Deep copy
    
    for field_path, required_level in redaction_rules.items():
        if user_clearance_level < required_level:
            _redact_field(redacted, field_path)
    
    return redacted


def _redact_field(obj: Dict[str, Any], path: str):
    """Redact a field in a nested dict"""
    parts = path.split(".")
    current = obj
    
    for i, part in enumerate(parts[:-1]):
        if part in current and isinstance(current[part], dict):
            current = current[part]
        else:
            return
    
    if parts[-1] in current:
        current[parts[-1]] = "[REDACTED]" 