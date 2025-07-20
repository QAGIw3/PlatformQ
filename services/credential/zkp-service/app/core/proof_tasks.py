"""
Proof Generation Tasks for Apache Ignite Compute Grid
"""

import json
import base64
from typing import Dict, Any, List
from datetime import datetime, timezone


def generate_bbs_signature_task(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate BBS+ signature - compute task
    This function runs on the compute grid
    """
    credential = params.get("credential", {})
    options = params.get("options", {})
    
    # Extract messages from credential
    messages = []
    subject = credential.get("credentialSubject", {})
    for key, value in subject.items():
        if key != "id":
            messages.append(f"{key}:{value}")
    
    # Add issuance date
    if "issuanceDate" in credential:
        messages.append(f"issuanceDate:{credential['issuanceDate']}")
    
    # Generate BBS+ signature
    # In a real implementation, this would use py-bbs-signatures library
    signature = _mock_bbs_sign(messages, options.get("private_key", {}))
    
    return {
        "proof": {
            "type": "BbsBlsSignature2020",
            "created": datetime.now(timezone.utc).isoformat(),
            "proofPurpose": "assertionMethod",
            "verificationMethod": options.get("verification_method"),
            "proofValue": base64.b64encode(signature).decode()
        }
    }


def verify_bbs_proof_task(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Verify BBS+ proof - compute task
    """
    proof = params.get("proof", {})
    public_key = params.get("public_key", {})
    messages = params.get("messages", [])
    
    # Extract proof value
    proof_value = base64.b64decode(proof.get("proofValue", ""))
    
    # Verify signature
    is_valid = _mock_bbs_verify(proof_value, messages, public_key)
    
    return {
        "valid": is_valid,
        "verified_at": datetime.now(timezone.utc).isoformat()
    }


def generate_selective_disclosure_task(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate selective disclosure proof - compute task
    """
    credential = params.get("credential", {})
    options = params.get("options", {})
    disclosed_attributes = options.get("disclosed_attributes", [])
    nonce = options.get("nonce", "")
    
    # Extract messages and create disclosure bitmap
    messages = []
    subject = credential.get("credentialSubject", {})
    disclosure_bitmap = []
    
    for key, value in subject.items():
        if key != "id":
            messages.append(f"{key}:{value}")
            disclosure_bitmap.append(key in disclosed_attributes)
    
    # Generate derived proof
    derived_proof = _mock_generate_derived_proof(
        credential.get("proof", {}),
        messages,
        disclosure_bitmap,
        nonce
    )
    
    # Create revealed document
    revealed_doc = {
        "@context": credential.get("@context"),
        "type": credential.get("type"),
        "credentialSubject": {"id": subject.get("id")}
    }
    
    for attr in disclosed_attributes:
        if attr in subject:
            revealed_doc["credentialSubject"][attr] = subject[attr]
    
    return {
        "proof": {
            "type": "BbsBlsSignatureProof2020",
            "created": datetime.now(timezone.utc).isoformat(),
            "proofPurpose": "assertionMethod",
            "verificationMethod": credential.get("proof", {}).get("verificationMethod"),
            "proofValue": base64.b64encode(derived_proof).decode(),
            "nonce": nonce
        },
        "revealedDocument": revealed_doc
    }


def generate_range_proof_task(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate range proof - compute task
    """
    credential = params.get("credential", {})
    options = params.get("options", {})
    
    attribute = options.get("attribute")
    min_value = options.get("min")
    max_value = options.get("max")
    bits = options.get("bits", 32)
    
    # Extract attribute value
    subject = credential.get("credentialSubject", {})
    value = _extract_nested_value(subject, attribute)
    
    if not isinstance(value, (int, float)):
        raise ValueError(f"Attribute {attribute} must be numeric")
    
    # Verify value is in range
    if value < min_value or value > max_value:
        raise ValueError(f"Value {value} not in range [{min_value}, {max_value}]")
    
    # Generate range proof
    proof = _mock_generate_range_proof(value, min_value, max_value, bits)
    
    return {
        "proof": {
            "type": "RangeProof2023",
            "created": datetime.now(timezone.utc).isoformat(),
            "attribute": attribute,
            "range": {
                "min": min_value,
                "max": max_value
            },
            "proofValue": base64.b64encode(proof).decode()
        }
    }


def generate_predicate_proof_task(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate predicate proof - compute task
    """
    credential = params.get("credential", {})
    options = params.get("options", {})
    
    predicate = options.get("predicate", {})
    attribute = predicate.get("attribute")
    operator = predicate.get("operator")
    threshold = predicate.get("value")
    
    # Extract attribute value
    subject = credential.get("credentialSubject", {})
    value = _extract_nested_value(subject, attribute)
    
    # Verify predicate
    if not _evaluate_predicate(value, operator, threshold):
        raise ValueError(f"Predicate {attribute} {operator} {threshold} not satisfied")
    
    # Generate predicate proof
    proof = _mock_generate_predicate_proof(value, operator, threshold)
    
    return {
        "proof": {
            "type": "PredicateProof2023",
            "created": datetime.now(timezone.utc).isoformat(),
            "predicate": predicate,
            "proofValue": base64.b64encode(proof).decode()
        }
    }


def generate_set_membership_proof_task(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate set membership proof - compute task
    """
    credential = params.get("credential", {})
    options = params.get("options", {})
    
    attribute = options.get("attribute")
    allowed_set = options.get("set", [])
    use_bloom_filter = options.get("use_bloom_filter", True)
    
    # Extract attribute value
    subject = credential.get("credentialSubject", {})
    value = _extract_nested_value(subject, attribute)
    
    # Verify membership
    if value not in allowed_set:
        raise ValueError(f"Value not in allowed set")
    
    # Generate membership proof
    if use_bloom_filter:
        proof = _mock_generate_bloom_filter_proof(value, allowed_set)
    else:
        proof = _mock_generate_merkle_proof(value, allowed_set)
    
    return {
        "proof": {
            "type": "SetMembershipProof2023",
            "created": datetime.now(timezone.utc).isoformat(),
            "attribute": attribute,
            "setSize": len(allowed_set),
            "method": "bloom_filter" if use_bloom_filter else "merkle_tree",
            "proofValue": base64.b64encode(proof).decode()
        }
    }


# Helper functions

def _extract_nested_value(obj: Dict[str, Any], path: str) -> Any:
    """Extract value from nested object using dot notation"""
    parts = path.split(".")
    value = obj
    
    for part in parts:
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            return None
    
    return value


def _evaluate_predicate(value: Any, operator: str, threshold: Any) -> bool:
    """Evaluate a predicate"""
    if operator == ">=":
        return value >= threshold
    elif operator == ">":
        return value > threshold
    elif operator == "<=":
        return value <= threshold
    elif operator == "<":
        return value < threshold
    elif operator == "==":
        return value == threshold
    elif operator == "!=":
        return value != threshold
    else:
        raise ValueError(f"Unknown operator: {operator}")


# Mock cryptographic functions
# In production, these would use actual cryptographic libraries

def _mock_bbs_sign(messages: List[str], private_key: Dict[str, Any]) -> bytes:
    """Mock BBS+ signature generation"""
    # Real implementation would use py-bbs-signatures
    data = {
        "messages": messages,
        "key": private_key.get("id", "mock_key"),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    return json.dumps(data).encode()


def _mock_bbs_verify(signature: bytes, messages: List[str], public_key: Dict[str, Any]) -> bool:
    """Mock BBS+ signature verification"""
    # Real implementation would use py-bbs-signatures
    try:
        data = json.loads(signature.decode())
        return data.get("messages") == messages
    except:
        return False


def _mock_generate_derived_proof(
    original_proof: Dict[str, Any],
    messages: List[str],
    disclosure_bitmap: List[bool],
    nonce: str
) -> bytes:
    """Mock derived proof generation for selective disclosure"""
    # Real implementation would use BBS+ derived proof generation
    data = {
        "original": original_proof.get("proofValue"),
        "disclosed_count": sum(disclosure_bitmap),
        "nonce": nonce,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    return json.dumps(data).encode()


def _mock_generate_range_proof(
    value: float,
    min_value: float,
    max_value: float,
    bits: int
) -> bytes:
    """Mock range proof generation"""
    # Real implementation would use bulletproofs or similar
    data = {
        "commitment": base64.b64encode(str(value).encode()).decode(),
        "range": [min_value, max_value],
        "bits": bits,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    return json.dumps(data).encode()


def _mock_generate_predicate_proof(
    value: Any,
    operator: str,
    threshold: Any
) -> bytes:
    """Mock predicate proof generation"""
    # Real implementation would use ZK predicate proofs
    data = {
        "commitment": base64.b64encode(str(value).encode()).decode(),
        "operator": operator,
        "threshold_commitment": base64.b64encode(str(threshold).encode()).decode(),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    return json.dumps(data).encode()


def _mock_generate_bloom_filter_proof(
    value: Any,
    allowed_set: List[Any]
) -> bytes:
    """Mock bloom filter membership proof"""
    # Real implementation would use actual bloom filter
    import hashlib
    
    # Create mock bloom filter
    bloom_size = 1024
    bloom = [0] * bloom_size
    
    # Add all values to bloom filter
    for item in allowed_set:
        hash_val = int(hashlib.sha256(str(item).encode()).hexdigest(), 16)
        bloom[hash_val % bloom_size] = 1
    
    # Check membership
    value_hash = int(hashlib.sha256(str(value).encode()).hexdigest(), 16)
    
    data = {
        "bloom_filter_size": bloom_size,
        "hash_functions": 1,
        "membership_index": value_hash % bloom_size,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    return json.dumps(data).encode()


def _mock_generate_merkle_proof(
    value: Any,
    allowed_set: List[Any]
) -> bytes:
    """Mock Merkle tree membership proof"""
    # Real implementation would use actual Merkle tree
    import hashlib
    
    # Sort set for consistent tree
    sorted_set = sorted([str(item) for item in allowed_set])
    value_index = sorted_set.index(str(value))
    
    # Create mock proof path
    proof_path = []
    current_index = value_index
    tree_size = len(sorted_set)
    
    while tree_size > 1:
        if current_index % 2 == 0:
            # Right sibling
            if current_index + 1 < tree_size:
                sibling = sorted_set[current_index + 1]
                proof_path.append({"position": "right", "hash": hashlib.sha256(sibling.encode()).hexdigest()})
        else:
            # Left sibling
            sibling = sorted_set[current_index - 1]
            proof_path.append({"position": "left", "hash": hashlib.sha256(sibling.encode()).hexdigest()})
        
        current_index //= 2
        tree_size = (tree_size + 1) // 2
    
    data = {
        "value_hash": hashlib.sha256(str(value).encode()).hexdigest(),
        "proof_path": proof_path,
        "root": hashlib.sha256("mock_root".encode()).hexdigest(),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    return json.dumps(data).encode() 