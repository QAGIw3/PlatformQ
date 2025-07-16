from typing import Dict, Any, List, Set, Optional, Tuple
import json
import hashlib
import base64
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class SelectiveDisclosure:
    """Implements selective disclosure for verifiable credentials"""
    
    def __init__(self):
        self.supported_formats = ["json-ld", "jwt", "json"]
        
    def create_disclosure_request(self, 
                                required_attributes: List[str],
                                optional_attributes: Optional[List[str]] = None,
                                constraints: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a disclosure request specifying what attributes are needed"""
        request = {
            "@context": [
                "https://www.w3.org/2018/credentials/v1",
                "https://identity.foundation/presentation-exchange/v1"
            ],
            "type": ["VerifiablePresentationRequest"],
            "query": [{
                "type": "QueryByExample",
                "credentialQuery": {
                    "required": required_attributes,
                    "optional": optional_attributes or [],
                    "constraints": constraints or {}
                }
            }],
            "challenge": self._generate_challenge(),
            "domain": "https://platformq.com",
            "created": datetime.utcnow().isoformat() + "Z"
        }
        
        return request
    
    def create_derived_credential(self,
                                original_credential: Dict[str, Any],
                                disclosed_attributes: List[str],
                                holder_binding: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a derived credential with only disclosed attributes"""
        # Validate that requested attributes exist
        available_attrs = self._get_available_attributes(original_credential)
        invalid_attrs = set(disclosed_attributes) - available_attrs
        if invalid_attrs:
            raise ValueError(f"Requested attributes not found: {invalid_attrs}")
        
        # Create derived credential structure
        derived = {
            "@context": original_credential.get("@context", []),
            "id": self._generate_derived_id(original_credential.get("id")),
            "type": original_credential.get("type", []),
            "issuer": original_credential.get("issuer"),
            "issuanceDate": original_credential.get("issuanceDate"),
            "derivedFrom": original_credential.get("id"),
            "credentialSubject": {}
        }
        
        # Copy only disclosed attributes
        original_subject = original_credential.get("credentialSubject", {})
        for attr in disclosed_attributes:
            if attr in original_subject:
                derived["credentialSubject"][attr] = original_subject[attr]
        
        # Add holder binding if provided
        if holder_binding:
            derived["credentialSubject"]["holder"] = holder_binding
        
        # Add disclosure metadata
        derived["disclosure"] = {
            "type": "SelectiveDisclosure2022",
            "disclosed": disclosed_attributes,
            "hidden": list(available_attrs - set(disclosed_attributes)),
            "method": "derived"
        }
        
        return derived
    
    def create_salted_credential(self, 
                               credential: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, str]]:
        """Create a salted version of the credential for hash-based disclosure"""
        import secrets
        
        salted = credential.copy()
        salts = {}
        
        # Salt each attribute in credentialSubject
        if "credentialSubject" in salted:
            subject = salted["credentialSubject"]
            salted_subject = {}
            
            for key, value in subject.items():
                salt = secrets.token_hex(16)
                salts[key] = salt
                
                # Create salted value
                salted_value = {
                    "_sd": self._hash_value(value, salt),
                    "_salt": salt
                }
                salted_subject[key] = salted_value
            
            salted["credentialSubject"] = salted_subject
        
        return salted, salts
    
    def disclose_salted_attributes(self,
                                 salted_credential: Dict[str, Any],
                                 salts: Dict[str, str],
                                 disclosed_attributes: List[str]) -> Dict[str, Any]:
        """Disclose specific attributes from a salted credential"""
        disclosed = salted_credential.copy()
        
        if "credentialSubject" in disclosed:
            subject = disclosed["credentialSubject"]
            disclosed_subject = {}
            
            for key, value in subject.items():
                if key in disclosed_attributes:
                    # Reveal the actual value and salt
                    disclosed_subject[key] = {
                        "value": self._get_original_value(salted_credential, key),
                        "salt": salts.get(key)
                    }
                else:
                    # Keep only the hash
                    disclosed_subject[key] = {"_sd": value.get("_sd")}
            
            disclosed["credentialSubject"] = disclosed_subject
        
        # Add disclosure metadata
        disclosed["disclosure"] = {
            "type": "SaltedHashDisclosure2022",
            "disclosed": disclosed_attributes,
            "method": "salted-hash"
        }
        
        return disclosed
    
    def create_merkle_disclosure(self, 
                               credential: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Create a Merkle tree based disclosure"""
        # Build Merkle tree of attributes
        attributes = self._flatten_credential(credential)
        tree = self._build_merkle_tree(attributes)
        
        # Create credential with Merkle root
        merkle_credential = credential.copy()
        merkle_credential["proof"] = {
            "type": "MerkleDisclosure2022",
            "merkleRoot": tree["root"],
            "created": datetime.utcnow().isoformat() + "Z"
        }
        
        return merkle_credential, tree
    
    def verify_merkle_disclosure(self,
                               disclosed_attrs: Dict[str, Any],
                               merkle_proof: List[Dict[str, Any]],
                               merkle_root: str) -> bool:
        """Verify a Merkle tree disclosure"""
        # Reconstruct root from disclosed attributes and proof
        computed_root = self._compute_merkle_root(disclosed_attrs, merkle_proof)
        
        return computed_root == merkle_root
    
    def _get_available_attributes(self, credential: Dict[str, Any]) -> Set[str]:
        """Get all available attributes from a credential"""
        attributes = set()
        
        if "credentialSubject" in credential:
            subject = credential["credentialSubject"]
            if isinstance(subject, dict):
                attributes.update(subject.keys())
        
        return attributes
    
    def _generate_challenge(self) -> str:
        """Generate a random challenge"""
        import secrets
        return base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
    
    def _generate_derived_id(self, original_id: Optional[str]) -> str:
        """Generate ID for derived credential"""
        import uuid
        base = original_id or "urn:uuid:" + str(uuid.uuid4())
        return f"{base}#derived-{uuid.uuid4()}"
    
    def _hash_value(self, value: Any, salt: str) -> str:
        """Hash a value with salt"""
        data = json.dumps(value, sort_keys=True) + salt
        return hashlib.sha256(data.encode()).hexdigest()
    
    def _get_original_value(self, credential: Dict[str, Any], key: str) -> Any:
        """Get original value from credential (placeholder)"""
        # In real implementation, would need to store or derive original values
        return f"original_value_of_{key}"
    
    def _flatten_credential(self, credential: Dict[str, Any]) -> List[Tuple[str, Any]]:
        """Flatten credential to list of (path, value) tuples"""
        flattened = []
        
        def flatten_dict(obj, path=""):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_path = f"{path}.{key}" if path else key
                    flatten_dict(value, new_path)
            else:
                flattened.append((path, obj))
        
        flatten_dict(credential)
        return flattened
    
    def _build_merkle_tree(self, attributes: List[Tuple[str, Any]]) -> Dict[str, Any]:
        """Build a Merkle tree from attributes"""
        # Simplified Merkle tree implementation
        leaves = []
        for path, value in attributes:
            leaf = hashlib.sha256(f"{path}:{value}".encode()).hexdigest()
            leaves.append({"path": path, "value": value, "hash": leaf})
        
        # Build tree (simplified - just compute root)
        if not leaves:
            return {"root": "", "leaves": []}
        
        current_level = [leaf["hash"] for leaf in leaves]
        while len(current_level) > 1:
            next_level = []
            for i in range(0, len(current_level), 2):
                if i + 1 < len(current_level):
                    combined = current_level[i] + current_level[i + 1]
                else:
                    combined = current_level[i] + current_level[i]
                next_level.append(hashlib.sha256(combined.encode()).hexdigest())
            current_level = next_level
        
        return {
            "root": current_level[0],
            "leaves": leaves,
            "height": len(leaves).bit_length()
        }
    
    def _compute_merkle_root(self, disclosed_attrs: Dict[str, Any],
                           proof: List[Dict[str, Any]]) -> str:
        """Compute Merkle root from disclosed attributes and proof"""
        # Simplified computation
        # In practice, would follow the proof path to reconstruct root
        combined = json.dumps(disclosed_attrs, sort_keys=True) + json.dumps(proof)
        return hashlib.sha256(combined.encode()).hexdigest() 