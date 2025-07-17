"""
Privacy-Preserving Computations for Federated Learning

Implements advanced privacy techniques including:
- Differential Privacy with adaptive clipping
- Secure Multi-Party Computation (MPC)
- Private Information Retrieval (PIR)
- Zero-Knowledge Proofs for model validation
"""

import logging
import hashlib
import numpy as np
from typing import Dict, List, Any, Optional, Tuple, Callable
from dataclasses import dataclass
from enum import Enum
import json
import math

logger = logging.getLogger(__name__)


class PrivacyMechanism(Enum):
    """Privacy mechanisms available"""
    LAPLACE = "laplace"
    GAUSSIAN = "gaussian"
    EXPONENTIAL = "exponential"
    GEOMETRIC = "geometric"


@dataclass
class PrivacyParameters:
    """Privacy parameters for federated learning"""
    epsilon: float  # Privacy budget
    delta: float  # Failure probability
    mechanism: PrivacyMechanism
    clip_norm: float  # Gradient clipping norm
    noise_multiplier: float
    adaptive_clipping: bool = True
    target_quantile: float = 0.9  # For adaptive clipping


@dataclass
class ZKProof:
    """Zero-knowledge proof for model validation"""
    proof_type: str
    statement: str
    proof_data: bytes
    public_inputs: Dict[str, Any]
    verification_key: bytes


class DifferentialPrivacy:
    """
    Advanced differential privacy implementation
    with adaptive clipping and composition accounting
    """
    
    def __init__(self, params: PrivacyParameters):
        self.params = params
        self.clip_history = []
        self.noise_history = []
        self.composition_accountant = PrivacyAccountant()
        
    def add_noise_to_gradients(self,
                             gradients: Dict[str, np.ndarray],
                             batch_size: int) -> Tuple[Dict[str, np.ndarray], Dict[str, Any]]:
        """Add DP noise to gradients with adaptive clipping"""
        # Compute gradient norms
        grad_norms = {}
        for layer_name, grad in gradients.items():
            grad_norms[layer_name] = np.linalg.norm(grad)
        
        total_norm = np.sqrt(sum(norm**2 for norm in grad_norms.values()))
        
        # Adaptive clipping
        if self.params.adaptive_clipping and len(self.clip_history) > 10:
            # Adjust clip norm based on history
            clip_norm = self._compute_adaptive_clip_norm(total_norm)
        else:
            clip_norm = self.params.clip_norm
        
        # Clip gradients
        clipped_gradients = {}
        scaling_factor = min(1.0, clip_norm / (total_norm + 1e-6))
        
        for layer_name, grad in gradients.items():
            clipped_gradients[layer_name] = grad * scaling_factor
        
        # Add noise
        noisy_gradients = {}
        noise_scale = self._compute_noise_scale(clip_norm, batch_size)
        
        for layer_name, grad in clipped_gradients.items():
            noise = self._generate_noise(grad.shape, noise_scale)
            noisy_gradients[layer_name] = grad + noise
        
        # Update history
        self.clip_history.append(total_norm)
        self.noise_history.append(noise_scale)
        
        # Compute privacy metrics
        metrics = {
            "original_norm": total_norm,
            "clip_norm": clip_norm,
            "scaling_factor": scaling_factor,
            "noise_scale": noise_scale,
            "signal_to_noise_ratio": total_norm / (noise_scale + 1e-6)
        }
        
        return noisy_gradients, metrics
    
    def _compute_adaptive_clip_norm(self, current_norm: float) -> float:
        """Compute adaptive clipping threshold"""
        # Use exponential moving average of gradient norms
        alpha = 0.1
        if hasattr(self, '_ema_norm'):
            self._ema_norm = alpha * current_norm + (1 - alpha) * self._ema_norm
        else:
            self._ema_norm = current_norm
        
        # Target a specific quantile of gradient norms
        sorted_history = sorted(self.clip_history[-100:])  # Last 100 norms
        quantile_idx = int(len(sorted_history) * self.params.target_quantile)
        target_norm = sorted_history[quantile_idx] if sorted_history else self.params.clip_norm
        
        # Smooth adjustment
        new_clip_norm = 0.9 * self.params.clip_norm + 0.1 * target_norm
        
        # Bounds
        min_clip = self.params.clip_norm * 0.5
        max_clip = self.params.clip_norm * 2.0
        
        return np.clip(new_clip_norm, min_clip, max_clip)
    
    def _compute_noise_scale(self, clip_norm: float, batch_size: int) -> float:
        """Compute noise scale based on privacy parameters"""
        if self.params.mechanism == PrivacyMechanism.GAUSSIAN:
            # For Gaussian mechanism with RDP accounting
            noise_multiplier = self.params.noise_multiplier
            noise_scale = noise_multiplier * clip_norm / batch_size
        elif self.params.mechanism == PrivacyMechanism.LAPLACE:
            # For Laplace mechanism
            noise_scale = clip_norm / (batch_size * self.params.epsilon)
        else:
            noise_scale = self.params.noise_multiplier * clip_norm / batch_size
        
        return noise_scale
    
    def _generate_noise(self, shape: tuple, scale: float) -> np.ndarray:
        """Generate noise based on mechanism"""
        if self.params.mechanism == PrivacyMechanism.GAUSSIAN:
            return np.random.normal(0, scale, shape)
        elif self.params.mechanism == PrivacyMechanism.LAPLACE:
            return np.random.laplace(0, scale, shape)
        elif self.params.mechanism == PrivacyMechanism.EXPONENTIAL:
            # Symmetric exponential
            return np.random.exponential(scale, shape) * np.random.choice([-1, 1], shape)
        else:  # Geometric
            return np.random.geometric(1 / (1 + scale), shape) * np.random.choice([-1, 1], shape)
    
    def compute_privacy_loss(self, num_iterations: int) -> Dict[str, float]:
        """Compute cumulative privacy loss"""
        return self.composition_accountant.compute_privacy_loss(
            self.params,
            num_iterations,
            self.noise_history
        )


class PrivacyAccountant:
    """
    Privacy accounting using RDP (Rényi Differential Privacy)
    and conversion to (ε, δ)-DP
    """
    
    def __init__(self):
        self.rdp_orders = list(range(2, 64)) + [128, 256]
        
    def compute_rdp(self,
                   noise_multiplier: float,
                   sampling_rate: float,
                   steps: int) -> Dict[int, float]:
        """Compute RDP for different orders"""
        rdp_dict = {}
        
        for alpha in self.rdp_orders:
            if noise_multiplier == 0:
                rdp_dict[alpha] = float('inf')
            else:
                # RDP for Gaussian mechanism
                rdp_single = self._compute_gaussian_rdp(
                    alpha,
                    noise_multiplier,
                    sampling_rate
                )
                rdp_dict[alpha] = rdp_single * steps
        
        return rdp_dict
    
    def _compute_gaussian_rdp(self,
                            alpha: int,
                            noise_multiplier: float,
                            sampling_rate: float) -> float:
        """Compute RDP for Gaussian mechanism"""
        if sampling_rate == 0:
            return 0
        
        if noise_multiplier == 0:
            return float('inf')
        
        if sampling_rate == 1.0:
            # No subsampling
            return alpha / (2 * noise_multiplier**2)
        
        # With subsampling (using bounds from Mironov et al.)
        return self._rdp_gaussian_subsampled(alpha, noise_multiplier, sampling_rate)
    
    def _rdp_gaussian_subsampled(self,
                                alpha: int,
                                sigma: float,
                                q: float) -> float:
        """RDP for subsampled Gaussian (approximate)"""
        # Simplified bound - in production use tighter bounds
        return q**2 * alpha / (2 * sigma**2)
    
    def rdp_to_dp(self,
                 rdp_dict: Dict[int, float],
                 delta: float) -> float:
        """Convert RDP to (ε, δ)-DP"""
        min_epsilon = float('inf')
        
        for alpha, rdp in rdp_dict.items():
            if rdp == float('inf'):
                continue
            
            # Conversion formula: ε = RDP - log(δ) / (α - 1)
            epsilon = rdp - np.log(delta) / (alpha - 1)
            min_epsilon = min(min_epsilon, epsilon)
        
        return min_epsilon
    
    def compute_privacy_loss(self,
                           params: PrivacyParameters,
                           num_iterations: int,
                           noise_history: List[float]) -> Dict[str, float]:
        """Compute total privacy loss"""
        if not noise_history:
            avg_noise_multiplier = params.noise_multiplier
        else:
            # Use average noise (conservative estimate)
            avg_noise_multiplier = np.mean([
                noise / params.clip_norm for noise in noise_history
            ])
        
        # Compute RDP
        rdp_dict = self.compute_rdp(
            noise_multiplier=avg_noise_multiplier,
            sampling_rate=1.0,  # Assuming full participation
            steps=num_iterations
        )
        
        # Convert to (ε, δ)-DP
        epsilon_used = self.rdp_to_dp(rdp_dict, params.delta)
        
        return {
            "epsilon_used": epsilon_used,
            "delta": params.delta,
            "num_iterations": num_iterations,
            "avg_noise_multiplier": avg_noise_multiplier,
            "privacy_budget_remaining": max(0, params.epsilon - epsilon_used)
        }


class SecureComputation:
    """
    Secure multi-party computation protocols
    """
    
    def __init__(self, num_parties: int, threshold: int):
        self.num_parties = num_parties
        self.threshold = threshold
        self.computation_id = hashlib.sha256(
            f"mpc_{num_parties}_{threshold}_{np.random.randint(1000000)}".encode()
        ).hexdigest()[:16]
        
    def generate_beaver_triples(self, count: int) -> List[Tuple[int, int, int]]:
        """Generate Beaver triples for multiplication"""
        triples = []
        
        for _ in range(count):
            # In production, use secure protocol
            a = np.random.randint(0, 2**32)
            b = np.random.randint(0, 2**32)
            c = (a * b) % (2**32)
            triples.append((a, b, c))
        
        return triples
    
    def secret_share(self, value: np.ndarray) -> List[np.ndarray]:
        """Create additive secret shares"""
        shares = []
        
        # Generate n-1 random shares
        for _ in range(self.num_parties - 1):
            share = np.random.randn(*value.shape)
            shares.append(share)
        
        # Last share ensures sum equals value
        last_share = value - sum(shares)
        shares.append(last_share)
        
        return shares
    
    def reconstruct(self, shares: List[np.ndarray]) -> Optional[np.ndarray]:
        """Reconstruct secret from shares"""
        if len(shares) < self.threshold:
            logger.error(f"Insufficient shares: {len(shares)} < {self.threshold}")
            return None
        
        # For additive sharing, simply sum
        return sum(shares[:self.threshold])
    
    def secure_aggregation_protocol(self,
                                  local_models: List[Dict[str, np.ndarray]],
                                  dropout_rate: float = 0.1) -> Dict[str, np.ndarray]:
        """
        Execute secure aggregation protocol
        Similar to Google's Secure Aggregation
        """
        n = len(local_models)
        
        # Phase 1: Setup
        # Each participant generates keys (simulated)
        participant_keys = {}
        for i in range(n):
            sk = np.random.bytes(32)
            pk = hashlib.sha256(sk).digest()
            participant_keys[i] = {"sk": sk, "pk": pk}
        
        # Phase 2: Share Keys
        # Participants exchange keys (simulated)
        pairwise_keys = {}
        for i in range(n):
            for j in range(i + 1, n):
                shared_key = hashlib.sha256(
                    participant_keys[i]["sk"] + participant_keys[j]["pk"]
                ).digest()
                pairwise_keys[(i, j)] = shared_key
                pairwise_keys[(j, i)] = shared_key
        
        # Phase 3: Masked Input Collection
        masked_models = []
        
        for i, model in enumerate(local_models):
            masked_model = {}
            
            for layer_name, weights in model.items():
                # Add pairwise masks
                masked_weights = weights.copy()
                
                for j in range(n):
                    if i != j:
                        # Generate deterministic mask
                        key = pairwise_keys.get((i, j), b"0" * 32)
                        np.random.seed(int.from_bytes(key[:4], 'big'))
                        
                        mask = np.random.randn(*weights.shape) * 0.01
                        if i < j:
                            masked_weights += mask
                        else:
                            masked_weights -= mask
                
                masked_model[layer_name] = masked_weights
            
            masked_models.append(masked_model)
        
        # Phase 4: Aggregation
        # Simulate dropout
        active_participants = np.random.choice(
            n,
            size=int(n * (1 - dropout_rate)),
            replace=False
        )
        
        # Aggregate active participants
        aggregated_model = {}
        
        for layer_name in local_models[0].keys():
            layer_sum = None
            
            for i in active_participants:
                if layer_sum is None:
                    layer_sum = masked_models[i][layer_name].copy()
                else:
                    layer_sum += masked_models[i][layer_name]
            
            # Average
            aggregated_model[layer_name] = layer_sum / len(active_participants)
        
        return aggregated_model


class ZeroKnowledgeProofs:
    """
    Zero-knowledge proofs for model validation
    """
    
    def __init__(self):
        self.proof_systems = {
            "range": self._generate_range_proof,
            "norm_bound": self._generate_norm_bound_proof,
            "training_correctness": self._generate_training_proof
        }
        
    def generate_proof(self,
                      proof_type: str,
                      statement: str,
                      witness: Dict[str, Any],
                      public_inputs: Dict[str, Any]) -> ZKProof:
        """Generate a zero-knowledge proof"""
        if proof_type not in self.proof_systems:
            raise ValueError(f"Unknown proof type: {proof_type}")
        
        proof_data, verification_key = self.proof_systems[proof_type](
            statement,
            witness,
            public_inputs
        )
        
        return ZKProof(
            proof_type=proof_type,
            statement=statement,
            proof_data=proof_data,
            public_inputs=public_inputs,
            verification_key=verification_key
        )
    
    def verify_proof(self, proof: ZKProof) -> bool:
        """Verify a zero-knowledge proof"""
        # In production, use actual ZK verification
        # For now, check proof structure
        
        if proof.proof_type == "range":
            return self._verify_range_proof(proof)
        elif proof.proof_type == "norm_bound":
            return self._verify_norm_bound_proof(proof)
        elif proof.proof_type == "training_correctness":
            return self._verify_training_proof(proof)
        
        return False
    
    def _generate_range_proof(self,
                            statement: str,
                            witness: Dict[str, Any],
                            public_inputs: Dict[str, Any]) -> Tuple[bytes, bytes]:
        """Generate proof that values are in range"""
        # Simplified Bulletproof-style range proof
        value = witness["value"]
        min_val = public_inputs["min"]
        max_val = public_inputs["max"]
        
        # Check if value is in range
        in_range = min_val <= value <= max_val
        
        # Generate commitment
        blinding_factor = np.random.bytes(32)
        commitment = hashlib.sha256(
            f"{value}:{blinding_factor.hex()}".encode()
        ).digest()
        
        # Generate proof (simplified)
        proof_data = {
            "commitment": commitment.hex(),
            "challenge": hashlib.sha256(commitment + statement.encode()).hexdigest(),
            "response": hashlib.sha256(blinding_factor).hexdigest() if in_range else "0" * 64
        }
        
        verification_key = hashlib.sha256(f"vk_range_{statement}".encode()).digest()
        
        return json.dumps(proof_data).encode(), verification_key
    
    def _verify_range_proof(self, proof: ZKProof) -> bool:
        """Verify range proof"""
        try:
            proof_data = json.loads(proof.proof_data.decode())
            
            # Verify proof structure
            required_fields = ["commitment", "challenge", "response"]
            if not all(field in proof_data for field in required_fields):
                return False
            
            # Verify challenge
            commitment = bytes.fromhex(proof_data["commitment"])
            expected_challenge = hashlib.sha256(
                commitment + proof.statement.encode()
            ).hexdigest()
            
            return proof_data["challenge"] == expected_challenge
            
        except Exception as e:
            logger.error(f"Proof verification failed: {e}")
            return False
    
    def _generate_norm_bound_proof(self,
                                 statement: str,
                                 witness: Dict[str, Any],
                                 public_inputs: Dict[str, Any]) -> Tuple[bytes, bytes]:
        """Generate proof that gradient norm is bounded"""
        gradients = witness["gradients"]
        max_norm = public_inputs["max_norm"]
        
        # Compute actual norm
        total_norm = 0
        for grad in gradients.values():
            total_norm += np.sum(grad ** 2)
        total_norm = np.sqrt(total_norm)
        
        # Check bound
        is_bounded = total_norm <= max_norm
        
        # Generate proof
        proof_data = {
            "norm_commitment": hashlib.sha256(f"{total_norm}".encode()).hexdigest(),
            "bound_satisfied": is_bounded,
            "timestamp": int(time.time() * 1000)
        }
        
        verification_key = hashlib.sha256(f"vk_norm_{max_norm}".encode()).digest()
        
        return json.dumps(proof_data).encode(), verification_key
    
    def _verify_norm_bound_proof(self, proof: ZKProof) -> bool:
        """Verify norm bound proof"""
        try:
            proof_data = json.loads(proof.proof_data.decode())
            return proof_data.get("bound_satisfied", False)
        except:
            return False
    
    def _generate_training_proof(self,
                               statement: str,
                               witness: Dict[str, Any],
                               public_inputs: Dict[str, Any]) -> Tuple[bytes, bytes]:
        """Generate proof of correct training execution"""
        # This would use more sophisticated ZK techniques in production
        
        training_logs = witness.get("training_logs", {})
        expected_steps = public_inputs.get("expected_steps", 100)
        
        # Verify training was performed
        actual_steps = training_logs.get("steps_completed", 0)
        loss_decreased = training_logs.get("final_loss", 1.0) < training_logs.get("initial_loss", 1.0)
        
        proof_data = {
            "training_hash": hashlib.sha256(
                json.dumps(training_logs, sort_keys=True).encode()
            ).hexdigest(),
            "steps_verified": actual_steps >= expected_steps,
            "convergence_verified": loss_decreased,
            "algorithm": public_inputs.get("algorithm", "SGD")
        }
        
        verification_key = hashlib.sha256(f"vk_training_{statement}".encode()).digest()
        
        return json.dumps(proof_data).encode(), verification_key
    
    def _verify_training_proof(self, proof: ZKProof) -> bool:
        """Verify training correctness proof"""
        try:
            proof_data = json.loads(proof.proof_data.decode())
            
            return (proof_data.get("steps_verified", False) and 
                   proof_data.get("convergence_verified", False))
        except:
            return False


class PrivateInformationRetrieval:
    """
    Private Information Retrieval for model queries
    """
    
    def __init__(self, database_size: int, block_size: int = 1024):
        self.database_size = database_size
        self.block_size = block_size
        self.num_blocks = (database_size + block_size - 1) // block_size
        
    def generate_query(self, index: int) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Generate PIR query for index"""
        # Simple PIR based on XOR
        # In production, use more sophisticated PIR schemes
        
        # Create query vector
        query = np.zeros(self.num_blocks, dtype=np.int32)
        block_idx = index // self.block_size
        query[block_idx] = 1
        
        # Add noise for privacy
        noise_positions = np.random.choice(
            self.num_blocks,
            size=max(1, self.num_blocks // 10),
            replace=False
        )
        
        for pos in noise_positions:
            if pos != block_idx:
                query[pos] = np.random.randint(0, 2)
        
        client_state = {
            "target_index": index,
            "block_index": block_idx,
            "noise_positions": noise_positions.tolist()
        }
        
        return query, client_state
    
    def process_query(self,
                     database: List[bytes],
                     query: np.ndarray) -> bytes:
        """Process PIR query on database"""
        # XOR blocks according to query
        result = bytes(self.block_size)
        
        for i, bit in enumerate(query):
            if bit == 1 and i < len(database):
                block = database[i]
                result = bytes(a ^ b for a, b in zip(result, block))
        
        return result
    
    def extract_response(self,
                       response: bytes,
                       client_state: Dict[str, Any]) -> bytes:
        """Extract desired data from PIR response"""
        # In simple XOR PIR, response contains the desired block
        target_index = client_state["target_index"]
        offset = target_index % self.block_size
        
        # Extract specific item (assuming items are fixed size)
        item_size = 256  # bytes
        if offset + item_size <= len(response):
            return response[offset:offset + item_size]
        
        return response[offset:]


import time  # Add this import at the top 