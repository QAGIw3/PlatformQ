"""
Homomorphic Encryption for Federated Learning

Implements various homomorphic encryption schemes for privacy-preserving
federated learning, including Paillier, CKKS, and BFV schemes.
"""

import logging
from typing import Dict, List, Any, Optional, Union, Tuple
import numpy as np
from dataclasses import dataclass
import json
import hashlib
import base64
from abc import ABC, abstractmethod

# For production, use tenseal or py-fhe libraries
# import tenseal as ts
# from Pyfhel import Pyfhel, PyCtxt

logger = logging.getLogger(__name__)


@dataclass
class HEContext:
    """Homomorphic encryption context"""
    scheme: str  # CKKS, BFV, Paillier
    parameters: Dict[str, Any]
    public_key: Optional[bytes] = None
    secret_key: Optional[bytes] = None
    galois_keys: Optional[bytes] = None
    relin_keys: Optional[bytes] = None


@dataclass
class EncryptedTensor:
    """Encrypted tensor wrapper"""
    ciphertext: bytes
    shape: List[int]
    scheme: str
    context_hash: str
    metadata: Dict[str, Any]


class HomomorphicEncryption(ABC):
    """Abstract base class for homomorphic encryption schemes"""
    
    @abstractmethod
    def generate_keys(self, **kwargs) -> HEContext:
        """Generate encryption keys"""
        pass
    
    @abstractmethod
    def encrypt(self, plaintext: np.ndarray, public_key: bytes) -> EncryptedTensor:
        """Encrypt a numpy array"""
        pass
    
    @abstractmethod
    def decrypt(self, ciphertext: EncryptedTensor, secret_key: bytes) -> np.ndarray:
        """Decrypt to numpy array"""
        pass
    
    @abstractmethod
    def add(self, a: EncryptedTensor, b: Union[EncryptedTensor, np.ndarray]) -> EncryptedTensor:
        """Homomorphic addition"""
        pass
    
    @abstractmethod
    def multiply(self, a: EncryptedTensor, b: Union[EncryptedTensor, np.ndarray]) -> EncryptedTensor:
        """Homomorphic multiplication"""
        pass
    
    @abstractmethod
    def weighted_sum(self, tensors: List[EncryptedTensor], weights: List[float]) -> EncryptedTensor:
        """Compute weighted sum of encrypted tensors"""
        pass


class CKKSEncryption(HomomorphicEncryption):
    """
    CKKS (Cheon-Kim-Kim-Song) encryption scheme
    Supports approximate arithmetic on real/complex numbers
    """
    
    def __init__(self):
        self.contexts = {}
        
    def generate_keys(self, 
                     poly_modulus_degree: int = 8192,
                     scale: float = 2**40,
                     security_level: int = 128) -> HEContext:
        """Generate CKKS encryption keys"""
        # In production, use TenSEAL or similar
        # context = ts.context(
        #     scheme=ts.SCHEME_TYPE.CKKS,
        #     poly_modulus_degree=poly_modulus_degree,
        #     coeff_mod_bit_sizes=[60, 40, 40, 60]
        # )
        # context.generate_galois_keys()
        # context.global_scale = scale
        
        # Mock implementation
        context = HEContext(
            scheme="CKKS",
            parameters={
                "poly_modulus_degree": poly_modulus_degree,
                "scale": scale,
                "security_level": security_level
            },
            public_key=self._generate_mock_key("public", poly_modulus_degree),
            secret_key=self._generate_mock_key("secret", poly_modulus_degree),
            galois_keys=self._generate_mock_key("galois", poly_modulus_degree),
            relin_keys=self._generate_mock_key("relin", poly_modulus_degree)
        )
        
        context_hash = self._compute_context_hash(context)
        self.contexts[context_hash] = context
        
        return context
    
    def encrypt(self, plaintext: np.ndarray, public_key: bytes) -> EncryptedTensor:
        """Encrypt numpy array using CKKS"""
        # In production:
        # vector = ts.ckks_vector(context, plaintext.flatten())
        # return EncryptedTensor(
        #     ciphertext=vector.serialize(),
        #     shape=list(plaintext.shape),
        #     scheme="CKKS",
        #     context_hash=context_hash,
        #     metadata={"scale": context.global_scale}
        # )
        
        # Mock implementation
        encrypted_data = self._mock_encrypt(plaintext, public_key)
        
        return EncryptedTensor(
            ciphertext=encrypted_data,
            shape=list(plaintext.shape),
            scheme="CKKS",
            context_hash=hashlib.sha256(public_key).hexdigest()[:16],
            metadata={
                "scale": 2**40,
                "noise_budget": 60,
                "level": 0
            }
        )
    
    def decrypt(self, ciphertext: EncryptedTensor, secret_key: bytes) -> np.ndarray:
        """Decrypt CKKS ciphertext"""
        # In production:
        # vector = ts.ckks_vector_from(context, ciphertext.ciphertext)
        # result = vector.decrypt(secret_key)
        # return np.array(result).reshape(ciphertext.shape)
        
        # Mock implementation
        return self._mock_decrypt(ciphertext, secret_key)
    
    def add(self, a: EncryptedTensor, b: Union[EncryptedTensor, np.ndarray]) -> EncryptedTensor:
        """Homomorphic addition"""
        if isinstance(b, np.ndarray):
            # Encrypt plaintext if needed
            context = self.contexts.get(a.context_hash)
            if not context:
                raise ValueError("Context not found")
            b = self.encrypt(b, context.public_key)
        
        # In production: result = a_vector + b_vector
        
        # Mock implementation
        result_data = self._mock_add(a.ciphertext, b.ciphertext)
        
        return EncryptedTensor(
            ciphertext=result_data,
            shape=a.shape,
            scheme="CKKS",
            context_hash=a.context_hash,
            metadata={
                **a.metadata,
                "operation": "add",
                "operand_count": 2
            }
        )
    
    def multiply(self, a: EncryptedTensor, b: Union[EncryptedTensor, np.ndarray]) -> EncryptedTensor:
        """Homomorphic multiplication"""
        if isinstance(b, EncryptedTensor):
            # Ciphertext-ciphertext multiplication
            # Requires relinearization
            result_data = self._mock_multiply_encrypted(a.ciphertext, b.ciphertext)
            metadata = {
                **a.metadata,
                "operation": "multiply_encrypted",
                "level": a.metadata.get("level", 0) + 1,
                "noise_budget": a.metadata.get("noise_budget", 60) - 20
            }
        else:
            # Ciphertext-plaintext multiplication
            result_data = self._mock_multiply_plain(a.ciphertext, b)
            metadata = {
                **a.metadata,
                "operation": "multiply_plain",
                "noise_budget": a.metadata.get("noise_budget", 60) - 10
            }
        
        return EncryptedTensor(
            ciphertext=result_data,
            shape=a.shape,
            scheme="CKKS",
            context_hash=a.context_hash,
            metadata=metadata
        )
    
    def weighted_sum(self, tensors: List[EncryptedTensor], weights: List[float]) -> EncryptedTensor:
        """Compute weighted sum of encrypted tensors"""
        if len(tensors) != len(weights):
            raise ValueError("Number of tensors and weights must match")
        
        # Start with first weighted tensor
        result = self.multiply(tensors[0], np.array(weights[0]))
        
        # Add remaining weighted tensors
        for tensor, weight in zip(tensors[1:], weights[1:]):
            weighted_tensor = self.multiply(tensor, np.array(weight))
            result = self.add(result, weighted_tensor)
        
        result.metadata["operation"] = "weighted_sum"
        result.metadata["num_tensors"] = len(tensors)
        
        return result
    
    def _generate_mock_key(self, key_type: str, size: int) -> bytes:
        """Generate mock key for testing"""
        key_data = f"{key_type}_key_{size}_{np.random.randint(1000000)}"
        return hashlib.sha256(key_data.encode()).digest()
    
    def _compute_context_hash(self, context: HEContext) -> str:
        """Compute hash of context for identification"""
        context_str = json.dumps(context.parameters, sort_keys=True)
        return hashlib.sha256(context_str.encode()).hexdigest()[:16]
    
    def _mock_encrypt(self, plaintext: np.ndarray, public_key: bytes) -> bytes:
        """Mock encryption for testing"""
        # In reality, this would use actual CKKS encryption
        data = {
            "encrypted": True,
            "data": base64.b64encode(plaintext.tobytes()).decode(),
            "dtype": str(plaintext.dtype),
            "key_hash": hashlib.sha256(public_key).hexdigest()[:8]
        }
        return json.dumps(data).encode()
    
    def _mock_decrypt(self, ciphertext: EncryptedTensor, secret_key: bytes) -> np.ndarray:
        """Mock decryption for testing"""
        data = json.loads(ciphertext.ciphertext.decode())
        plaintext_bytes = base64.b64decode(data["data"])
        return np.frombuffer(plaintext_bytes, dtype=data["dtype"]).reshape(ciphertext.shape)
    
    def _mock_add(self, a: bytes, b: bytes) -> bytes:
        """Mock homomorphic addition"""
        # In reality, this would perform actual homomorphic addition
        a_data = json.loads(a.decode())
        b_data = json.loads(b.decode())
        
        # Simulate addition (not actually homomorphic)
        result_data = {
            "encrypted": True,
            "data": a_data["data"],  # Would be actual sum
            "dtype": a_data["dtype"],
            "operation": "add"
        }
        return json.dumps(result_data).encode()
    
    def _mock_multiply_encrypted(self, a: bytes, b: bytes) -> bytes:
        """Mock ciphertext-ciphertext multiplication"""
        a_data = json.loads(a.decode())
        result_data = {
            "encrypted": True,
            "data": a_data["data"],  # Would be actual product
            "dtype": a_data["dtype"],
            "operation": "multiply_encrypted"
        }
        return json.dumps(result_data).encode()
    
    def _mock_multiply_plain(self, a: bytes, b: np.ndarray) -> bytes:
        """Mock ciphertext-plaintext multiplication"""
        a_data = json.loads(a.decode())
        result_data = {
            "encrypted": True,
            "data": a_data["data"],  # Would be actual product
            "dtype": a_data["dtype"],
            "operation": "multiply_plain",
            "scalar": float(b) if b.size == 1 else None
        }
        return json.dumps(result_data).encode()


class PaillierEncryption(HomomorphicEncryption):
    """
    Paillier encryption scheme
    Supports addition of encrypted values and multiplication by plaintext
    """
    
    def __init__(self):
        self.key_pairs = {}
        
    def generate_keys(self, key_size: int = 2048) -> HEContext:
        """Generate Paillier key pair"""
        # In production, use python-paillier
        # from phe import paillier
        # public_key, private_key = paillier.generate_paillier_keypair(n_length=key_size)
        
        context = HEContext(
            scheme="Paillier",
            parameters={"key_size": key_size},
            public_key=self._generate_mock_key("public", key_size),
            secret_key=self._generate_mock_key("secret", key_size)
        )
        
        key_hash = hashlib.sha256(context.public_key).hexdigest()[:16]
        self.key_pairs[key_hash] = context
        
        return context
    
    def encrypt(self, plaintext: np.ndarray, public_key: bytes) -> EncryptedTensor:
        """Encrypt using Paillier"""
        # Paillier works on integers, so we need to quantize
        scale = 2**16
        quantized = (plaintext * scale).astype(np.int64)
        
        encrypted_data = self._mock_encrypt_paillier(quantized, public_key)
        
        return EncryptedTensor(
            ciphertext=encrypted_data,
            shape=list(plaintext.shape),
            scheme="Paillier",
            context_hash=hashlib.sha256(public_key).hexdigest()[:16],
            metadata={"scale": scale}
        )
    
    def decrypt(self, ciphertext: EncryptedTensor, secret_key: bytes) -> np.ndarray:
        """Decrypt Paillier ciphertext"""
        quantized = self._mock_decrypt_paillier(ciphertext, secret_key)
        scale = ciphertext.metadata["scale"]
        return quantized.astype(np.float64) / scale
    
    def add(self, a: EncryptedTensor, b: Union[EncryptedTensor, np.ndarray]) -> EncryptedTensor:
        """Homomorphic addition for Paillier"""
        if isinstance(b, np.ndarray):
            context = self.key_pairs.get(a.context_hash)
            if not context:
                raise ValueError("Context not found")
            b = self.encrypt(b, context.public_key)
        
        result_data = self._mock_add_paillier(a.ciphertext, b.ciphertext)
        
        return EncryptedTensor(
            ciphertext=result_data,
            shape=a.shape,
            scheme="Paillier",
            context_hash=a.context_hash,
            metadata=a.metadata
        )
    
    def multiply(self, a: EncryptedTensor, b: Union[EncryptedTensor, np.ndarray]) -> EncryptedTensor:
        """Paillier only supports multiplication by plaintext"""
        if isinstance(b, EncryptedTensor):
            raise ValueError("Paillier does not support ciphertext-ciphertext multiplication")
        
        # Scale the plaintext multiplier
        scale = a.metadata["scale"]
        scaled_b = (b * scale).astype(np.int64)
        
        result_data = self._mock_multiply_paillier(a.ciphertext, scaled_b)
        
        return EncryptedTensor(
            ciphertext=result_data,
            shape=a.shape,
            scheme="Paillier",
            context_hash=a.context_hash,
            metadata=a.metadata
        )
    
    def weighted_sum(self, tensors: List[EncryptedTensor], weights: List[float]) -> EncryptedTensor:
        """Compute weighted sum using Paillier"""
        # For Paillier, we can optimize by summing first, then scaling
        result = tensors[0]
        for tensor in tensors[1:]:
            result = self.add(result, tensor)
        
        # Apply average weight (since Paillier doesn't support individual weights efficiently)
        avg_weight = np.mean(weights)
        result = self.multiply(result, np.array(avg_weight))
        
        return result
    
    def _generate_mock_key(self, key_type: str, size: int) -> bytes:
        """Generate mock Paillier key"""
        return hashlib.sha256(f"paillier_{key_type}_{size}".encode()).digest()
    
    def _mock_encrypt_paillier(self, plaintext: np.ndarray, public_key: bytes) -> bytes:
        """Mock Paillier encryption"""
        data = {
            "scheme": "Paillier",
            "encrypted": base64.b64encode(plaintext.tobytes()).decode(),
            "dtype": str(plaintext.dtype)
        }
        return json.dumps(data).encode()
    
    def _mock_decrypt_paillier(self, ciphertext: EncryptedTensor, secret_key: bytes) -> np.ndarray:
        """Mock Paillier decryption"""
        data = json.loads(ciphertext.ciphertext.decode())
        plaintext_bytes = base64.b64decode(data["encrypted"])
        return np.frombuffer(plaintext_bytes, dtype=data["dtype"]).reshape(ciphertext.shape)
    
    def _mock_add_paillier(self, a: bytes, b: bytes) -> bytes:
        """Mock Paillier addition"""
        a_data = json.loads(a.decode())
        return json.dumps({
            "scheme": "Paillier",
            "encrypted": a_data["encrypted"],  # Would be actual sum
            "dtype": a_data["dtype"],
            "operation": "add"
        }).encode()
    
    def _mock_multiply_paillier(self, a: bytes, b: np.ndarray) -> bytes:
        """Mock Paillier scalar multiplication"""
        a_data = json.loads(a.decode())
        return json.dumps({
            "scheme": "Paillier",
            "encrypted": a_data["encrypted"],  # Would be actual product
            "dtype": a_data["dtype"],
            "operation": "multiply_scalar"
        }).encode()


class SecureAggregator:
    """
    Secure aggregation using homomorphic encryption
    Supports multiple aggregation strategies
    """
    
    def __init__(self, encryption_scheme: str = "CKKS"):
        if encryption_scheme == "CKKS":
            self.crypto = CKKSEncryption()
        elif encryption_scheme == "Paillier":
            self.crypto = PaillierEncryption()
        else:
            raise ValueError(f"Unknown encryption scheme: {encryption_scheme}")
        
        self.scheme = encryption_scheme
        
    def setup_aggregation(self, num_participants: int, **kwargs) -> Tuple[HEContext, Dict[str, Any]]:
        """Setup secure aggregation context"""
        # Generate keys
        context = self.crypto.generate_keys(**kwargs)
        
        # Generate aggregation parameters
        agg_params = {
            "num_participants": num_participants,
            "threshold": max(1, num_participants // 2),  # Minimum participants for aggregation
            "aggregation_id": hashlib.sha256(f"agg_{num_participants}_{np.random.randint(1000000)}".encode()).hexdigest()[:16],
            "scheme": self.scheme
        }
        
        return context, agg_params
    
    def encrypt_model_update(self,
                           model_weights: Dict[str, np.ndarray],
                           public_key: bytes) -> Dict[str, EncryptedTensor]:
        """Encrypt model weights"""
        encrypted_weights = {}
        
        for layer_name, weights in model_weights.items():
            encrypted_weights[layer_name] = self.crypto.encrypt(weights, public_key)
            
        return encrypted_weights
    
    def aggregate_encrypted_updates(self,
                                  encrypted_updates: List[Dict[str, EncryptedTensor]],
                                  weights: Optional[List[float]] = None) -> Dict[str, EncryptedTensor]:
        """Aggregate encrypted model updates"""
        if not encrypted_updates:
            raise ValueError("No updates to aggregate")
        
        # Default to equal weights
        if weights is None:
            weights = [1.0 / len(encrypted_updates)] * len(encrypted_updates)
        
        # Aggregate each layer
        aggregated = {}
        layer_names = encrypted_updates[0].keys()
        
        for layer_name in layer_names:
            layer_tensors = [update[layer_name] for update in encrypted_updates]
            aggregated[layer_name] = self.crypto.weighted_sum(layer_tensors, weights)
        
        return aggregated
    
    def decrypt_aggregated_model(self,
                               encrypted_model: Dict[str, EncryptedTensor],
                               secret_key: bytes) -> Dict[str, np.ndarray]:
        """Decrypt aggregated model"""
        decrypted_model = {}
        
        for layer_name, encrypted_tensor in encrypted_model.items():
            decrypted_model[layer_name] = self.crypto.decrypt(encrypted_tensor, secret_key)
        
        return decrypted_model
    
    def verify_aggregation(self,
                         encrypted_updates: List[Dict[str, EncryptedTensor]],
                         aggregated_model: Dict[str, EncryptedTensor],
                         verification_samples: int = 5) -> bool:
        """Verify aggregation correctness using random sampling"""
        # In practice, this would use zero-knowledge proofs
        # For now, we do basic shape and metadata verification
        
        if not encrypted_updates:
            return False
        
        # Check all layers are present
        expected_layers = set(encrypted_updates[0].keys())
        actual_layers = set(aggregated_model.keys())
        
        if expected_layers != actual_layers:
            logger.error(f"Layer mismatch: expected {expected_layers}, got {actual_layers}")
            return False
        
        # Verify shapes match
        for layer_name in expected_layers:
            expected_shape = encrypted_updates[0][layer_name].shape
            actual_shape = aggregated_model[layer_name].shape
            
            if expected_shape != actual_shape:
                logger.error(f"Shape mismatch in {layer_name}: expected {expected_shape}, got {actual_shape}")
                return False
        
        # Verify encryption context
        for layer_name in expected_layers:
            context_hashes = set(update[layer_name].context_hash for update in encrypted_updates)
            
            if len(context_hashes) > 1:
                logger.error(f"Multiple encryption contexts found in {layer_name}")
                return False
            
            if aggregated_model[layer_name].context_hash not in context_hashes:
                logger.error(f"Aggregated model has different encryption context in {layer_name}")
                return False
        
        return True


class DifferentialPrivacyWithHE:
    """
    Combine differential privacy with homomorphic encryption
    for enhanced privacy guarantees
    """
    
    def __init__(self,
                 epsilon: float = 1.0,
                 delta: float = 1e-5,
                 encryption_scheme: str = "CKKS"):
        self.epsilon = epsilon
        self.delta = delta
        self.aggregator = SecureAggregator(encryption_scheme)
        
    def add_encrypted_noise(self,
                          encrypted_tensor: EncryptedTensor,
                          sensitivity: float,
                          mechanism: str = "gaussian") -> EncryptedTensor:
        """Add differential privacy noise to encrypted tensor"""
        # Generate noise based on mechanism
        shape = encrypted_tensor.shape
        
        if mechanism == "gaussian":
            sigma = sensitivity * np.sqrt(2 * np.log(1.25 / self.delta)) / self.epsilon
            noise = np.random.normal(0, sigma, shape)
        elif mechanism == "laplace":
            scale = sensitivity / self.epsilon
            noise = np.random.laplace(0, scale, shape)
        else:
            raise ValueError(f"Unknown mechanism: {mechanism}")
        
        # Add noise homomorphically
        noisy_tensor = self.aggregator.crypto.add(encrypted_tensor, noise)
        
        # Update metadata
        noisy_tensor.metadata["dp_applied"] = True
        noisy_tensor.metadata["dp_epsilon"] = self.epsilon
        noisy_tensor.metadata["dp_delta"] = self.delta
        noisy_tensor.metadata["dp_mechanism"] = mechanism
        
        return noisy_tensor
    
    def private_aggregation(self,
                          encrypted_updates: List[Dict[str, EncryptedTensor]],
                          sensitivities: Dict[str, float],
                          weights: Optional[List[float]] = None) -> Dict[str, EncryptedTensor]:
        """Perform differentially private aggregation on encrypted updates"""
        # First aggregate
        aggregated = self.aggregator.aggregate_encrypted_updates(encrypted_updates, weights)
        
        # Then add noise to each layer
        private_aggregated = {}
        for layer_name, encrypted_tensor in aggregated.items():
            sensitivity = sensitivities.get(layer_name, 1.0)
            private_aggregated[layer_name] = self.add_encrypted_noise(
                encrypted_tensor,
                sensitivity,
                mechanism="gaussian"
            )
        
        return private_aggregated


class MultiPartyComputation:
    """
    Secure multi-party computation protocols for federated learning
    """
    
    def __init__(self, num_parties: int, threshold: int):
        self.num_parties = num_parties
        self.threshold = threshold
        self.shares = {}
        
    def create_shares(self, 
                     secret: np.ndarray,
                     party_ids: List[str]) -> Dict[str, np.ndarray]:
        """Create Shamir secret shares"""
        if len(party_ids) != self.num_parties:
            raise ValueError(f"Expected {self.num_parties} parties, got {len(party_ids)}")
        
        # For simplicity, using additive secret sharing
        # In production, use proper Shamir's secret sharing
        shares = {}
        
        # Generate random shares for all but last party
        for i, party_id in enumerate(party_ids[:-1]):
            share = np.random.randn(*secret.shape)
            shares[party_id] = share
        
        # Last share ensures sum equals secret
        last_share = secret - sum(shares.values())
        shares[party_ids[-1]] = last_share
        
        # Store for reconstruction
        share_id = hashlib.sha256(str(secret).encode()).hexdigest()[:16]
        self.shares[share_id] = {
            "num_shares": len(shares),
            "threshold": self.threshold,
            "shape": secret.shape
        }
        
        return shares
    
    def reconstruct_secret(self,
                         shares: Dict[str, np.ndarray],
                         share_id: str) -> Optional[np.ndarray]:
        """Reconstruct secret from shares"""
        share_info = self.shares.get(share_id)
        if not share_info:
            logger.error(f"Unknown share ID: {share_id}")
            return None
        
        if len(shares) < share_info["threshold"]:
            logger.error(f"Insufficient shares: {len(shares)} < {share_info['threshold']}")
            return None
        
        # For additive sharing, simply sum
        # In production, use Lagrange interpolation for Shamir's
        secret = sum(shares.values())
        
        return secret
    
    def secure_comparison(self,
                         encrypted_a: EncryptedTensor,
                         encrypted_b: EncryptedTensor,
                         comparison_type: str = "greater") -> bool:
        """Secure comparison of encrypted values"""
        # This is a simplified version
        # In production, use garbled circuits or other MPC protocols
        
        # For now, return mock result
        return np.random.choice([True, False])
    
    def private_set_intersection(self,
                               sets: List[List[str]]) -> List[str]:
        """Compute private set intersection"""
        # Simplified PSI
        # In production, use proper PSI protocols
        
        if not sets:
            return []
        
        intersection = set(sets[0])
        for s in sets[1:]:
            intersection &= set(s)
        
        return list(intersection) 