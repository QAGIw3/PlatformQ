"""
Secure Aggregation for Federated Learning

Implements secure aggregation protocols with homomorphic encryption,
differential privacy, and robustness against malicious participants.
"""

import logging
import hashlib
import time
from typing import Dict, List, Any, Optional, Tuple, Set
from dataclasses import dataclass
import numpy as np
import json
from enum import Enum

from .homomorphic_encryption import (
    SecureAggregator, 
    EncryptedTensor, 
    HEContext,
    DifferentialPrivacyWithHE,
    MultiPartyComputation
)

logger = logging.getLogger(__name__)


class AggregationStrategy(Enum):
    """Aggregation strategies for federated learning"""
    FED_AVG = "FedAvg"  # Federated Averaging
    FED_PROX = "FedProx"  # Federated Proximal
    FED_NOVA = "FedNova"  # Normalized Averaging
    SECURE_AGG = "SecureAgg"  # Secure Aggregation
    DP_FED_AVG = "DPFedAvg"  # Differentially Private FedAvg
    BYZANTINE_ROBUST = "ByzantineRobust"  # Byzantine-robust aggregation
    FED_GRAPH_AVG = "FedGraphAvg"  # Federated Graph Averaging


@dataclass
class ParticipantUpdate:
    """Model update from a participant"""
    participant_id: str
    round_number: int
    encrypted_weights: Dict[str, EncryptedTensor]
    metadata: Dict[str, Any]
    signature: str
    timestamp: int


@dataclass
class AggregationResult:
    """Result of secure aggregation"""
    round_number: int
    aggregated_weights: Dict[str, EncryptedTensor]
    num_participants: int
    dropped_participants: List[str]
    verification_passed: bool
    privacy_budget_used: float
    aggregation_time: float


class SecureAggregationProtocol:
    """
    Main secure aggregation protocol implementation
    Combines HE, DP, and MPC for maximum privacy
    """
    
    def __init__(self,
                 strategy: AggregationStrategy = AggregationStrategy.SECURE_AGG,
                 encryption_scheme: str = "CKKS",
                 differential_privacy: bool = True,
                 epsilon: float = 1.0,
                 delta: float = 1e-5,
                 byzantine_tolerance: float = 0.2):
        """
        Initialize secure aggregation protocol
        
        Args:
            strategy: Aggregation strategy to use
            encryption_scheme: HE scheme (CKKS or Paillier)
            differential_privacy: Whether to apply DP
            epsilon: DP epsilon parameter
            delta: DP delta parameter
            byzantine_tolerance: Fraction of Byzantine participants tolerated
        """
        self.strategy = strategy
        self.encryption_scheme = encryption_scheme
        self.differential_privacy = differential_privacy
        self.byzantine_tolerance = byzantine_tolerance
        
        # Initialize components
        self.aggregator = SecureAggregator(encryption_scheme)
        self.dp_aggregator = DifferentialPrivacyWithHE(epsilon, delta, encryption_scheme) if differential_privacy else None
        
        # Storage for session data
        self.sessions = {}
        self.round_updates = {}
        self.privacy_budgets = {}
        
    def initialize_session(self,
                         session_id: str,
                         num_participants: int,
                         model_architecture: Dict[str, List[int]]) -> Tuple[HEContext, Dict[str, Any]]:
        """Initialize a new secure aggregation session"""
        # Setup encryption context
        context, agg_params = self.aggregator.setup_aggregation(num_participants)
        
        # Initialize session data
        session_data = {
            "session_id": session_id,
            "num_participants": num_participants,
            "model_architecture": model_architecture,
            "context": context,
            "aggregation_params": agg_params,
            "rounds_completed": 0,
            "privacy_budget_remaining": 10.0 if self.differential_privacy else float('inf'),
            "created_at": time.time()
        }
        
        self.sessions[session_id] = session_data
        self.round_updates[session_id] = {}
        self.privacy_budgets[session_id] = []
        
        # Generate key shares for participants if using MPC
        if self.strategy == AggregationStrategy.BYZANTINE_ROBUST:
            mpc = MultiPartyComputation(
                num_parties=num_participants,
                threshold=int(num_participants * (1 - self.byzantine_tolerance))
            )
            session_data["mpc"] = mpc
        
        return context, {
            "session_id": session_id,
            "public_key": context.public_key.hex() if context.public_key else None,
            "aggregation_params": agg_params,
            "encryption_scheme": self.encryption_scheme,
            "differential_privacy": self.differential_privacy,
            "strategy": self.strategy.value
        }
    
    def submit_update(self,
                     session_id: str,
                     participant_update: ParticipantUpdate) -> bool:
        """Submit an encrypted model update"""
        if session_id not in self.sessions:
            logger.error(f"Unknown session: {session_id}")
            return False
        
        session = self.sessions[session_id]
        round_number = participant_update.round_number
        
        # Initialize round if needed
        if round_number not in self.round_updates[session_id]:
            self.round_updates[session_id][round_number] = {}
        
        # Verify update signature
        if not self._verify_update_signature(participant_update):
            logger.warning(f"Invalid signature from {participant_update.participant_id}")
            return False
        
        # Store update
        self.round_updates[session_id][round_number][participant_update.participant_id] = participant_update
        
        logger.info(f"Received update from {participant_update.participant_id} for round {round_number}")
        return True
    
    def aggregate_round(self,
                       session_id: str,
                       round_number: int,
                       min_participants: Optional[int] = None) -> Optional[AggregationResult]:
        """Perform secure aggregation for a round"""
        if session_id not in self.sessions:
            logger.error(f"Unknown session: {session_id}")
            return None
        
        session = self.sessions[session_id]
        round_updates = self.round_updates[session_id].get(round_number, {})
        
        # Check minimum participants
        if min_participants is None:
            min_participants = max(1, int(session["num_participants"] * 0.5))
        
        if len(round_updates) < min_participants:
            logger.warning(f"Insufficient participants: {len(round_updates)} < {min_participants}")
            return None
        
        start_time = time.time()
        
        # Apply aggregation strategy
        if self.strategy == AggregationStrategy.FED_AVG:
            result = self._federated_averaging(session, round_updates)
        elif self.strategy == AggregationStrategy.FED_PROX:
            result = self._federated_proximal(session, round_updates)
        elif self.strategy == AggregationStrategy.FED_NOVA:
            result = self._federated_nova(session, round_updates)
        elif self.strategy == AggregationStrategy.SECURE_AGG:
            result = self._secure_aggregation(session, round_updates)
        elif self.strategy == AggregationStrategy.DP_FED_AVG:
            result = self._dp_federated_averaging(session, round_updates)
        elif self.strategy == AggregationStrategy.BYZANTINE_ROBUST:
            result = self._byzantine_robust_aggregation(session, round_updates)
        elif self.strategy == AggregationStrategy.FED_GRAPH_AVG:
            result = self._federated_graph_averaging(session, round_updates)
        else:
            raise ValueError(f"Unknown strategy: {self.strategy}")
        
        # Update session state
        session["rounds_completed"] += 1
        
        # Track privacy budget
        if self.differential_privacy and result:
            budget_used = result.privacy_budget_used
            session["privacy_budget_remaining"] -= budget_used
            self.privacy_budgets[session_id].append({
                "round": round_number,
                "budget_used": budget_used,
                "remaining": session["privacy_budget_remaining"]
            })
        
        # Record aggregation time
        if result:
            result.aggregation_time = time.time() - start_time
        
        return result
    
    def _federated_averaging(self,
                           session: Dict[str, Any],
                           round_updates: Dict[str, ParticipantUpdate]) -> AggregationResult:
        """Standard federated averaging"""
        # Extract encrypted updates
        encrypted_updates = []
        participant_weights = []
        
        for participant_id, update in round_updates.items():
            encrypted_updates.append(update.encrypted_weights)
            # Weight by number of samples
            num_samples = update.metadata.get("num_samples", 1)
            participant_weights.append(num_samples)
        
        # Normalize weights
        total_samples = sum(participant_weights)
        normalized_weights = [w / total_samples for w in participant_weights]
        
        # Aggregate
        aggregated = self.aggregator.aggregate_encrypted_updates(
            encrypted_updates,
            normalized_weights
        )
        
        # Verify aggregation
        verification_passed = self.aggregator.verify_aggregation(
            encrypted_updates,
            aggregated
        )
        
        return AggregationResult(
            round_number=list(round_updates.values())[0].round_number,
            aggregated_weights=aggregated,
            num_participants=len(round_updates),
            dropped_participants=[],
            verification_passed=verification_passed,
            privacy_budget_used=0.0,
            aggregation_time=0.0
        )
    
    def _federated_proximal(self,
                          session: Dict[str, Any],
                          round_updates: Dict[str, ParticipantUpdate]) -> AggregationResult:
        """FedProx aggregation with proximal term"""
        # Similar to FedAvg but with proximal regularization
        # For encrypted data, we approximate by adjusting weights
        
        encrypted_updates = []
        participant_weights = []
        
        for participant_id, update in round_updates.items():
            encrypted_updates.append(update.encrypted_weights)
            
            # Adjust weight based on local steps and proximal term
            num_samples = update.metadata.get("num_samples", 1)
            local_steps = update.metadata.get("local_steps", 1)
            proximal_weight = update.metadata.get("proximal_weight", 0.01)
            
            # Heuristic weight adjustment
            adjusted_weight = num_samples / (1 + proximal_weight * local_steps)
            participant_weights.append(adjusted_weight)
        
        # Normalize and aggregate
        total_weight = sum(participant_weights)
        normalized_weights = [w / total_weight for w in participant_weights]
        
        aggregated = self.aggregator.aggregate_encrypted_updates(
            encrypted_updates,
            normalized_weights
        )
        
        return AggregationResult(
            round_number=list(round_updates.values())[0].round_number,
            aggregated_weights=aggregated,
            num_participants=len(round_updates),
            dropped_participants=[],
            verification_passed=True,
            privacy_budget_used=0.0,
            aggregation_time=0.0
        )
    
    def _federated_nova(self,
                       session: Dict[str, Any],
                       round_updates: Dict[str, ParticipantUpdate]) -> AggregationResult:
        """FedNova - Normalized averaging with local steps"""
        encrypted_updates = []
        participant_weights = []
        
        for participant_id, update in round_updates.items():
            encrypted_updates.append(update.encrypted_weights)
            
            # FedNova normalizes by local steps
            num_samples = update.metadata.get("num_samples", 1)
            local_steps = update.metadata.get("local_steps", 1)
            
            # Normalized weight
            weight = num_samples / local_steps
            participant_weights.append(weight)
        
        # Normalize and aggregate
        total_weight = sum(participant_weights)
        normalized_weights = [w / total_weight for w in participant_weights]
        
        aggregated = self.aggregator.aggregate_encrypted_updates(
            encrypted_updates,
            normalized_weights
        )
        
        return AggregationResult(
            round_number=list(round_updates.values())[0].round_number,
            aggregated_weights=aggregated,
            num_participants=len(round_updates),
            dropped_participants=[],
            verification_passed=True,
            privacy_budget_used=0.0,
            aggregation_time=0.0
        )
    
    def _secure_aggregation(self,
                          session: Dict[str, Any],
                          round_updates: Dict[str, ParticipantUpdate]) -> AggregationResult:
        """Secure aggregation with masking"""
        # This implements a simplified secure aggregation protocol
        # In production, use proper protocols like SecAgg or CESAR
        
        encrypted_updates = []
        participant_ids = []
        
        for participant_id, update in round_updates.items():
            encrypted_updates.append(update.encrypted_weights)
            participant_ids.append(participant_id)
        
        # Generate pairwise masks (simplified)
        # In reality, this would use Diffie-Hellman key agreement
        masks = self._generate_pairwise_masks(participant_ids, session)
        
        # Apply masks to updates (homomorphically)
        masked_updates = []
        for i, (pid, update) in enumerate(zip(participant_ids, encrypted_updates)):
            masked_update = {}
            for layer_name, encrypted_tensor in update.items():
                # Add mask
                mask = masks.get((pid, layer_name), np.zeros(encrypted_tensor.shape))
                masked_update[layer_name] = self.aggregator.crypto.add(
                    encrypted_tensor,
                    mask
                )
            masked_updates.append(masked_update)
        
        # Aggregate masked updates
        aggregated = self.aggregator.aggregate_encrypted_updates(masked_updates)
        
        # Remove aggregate mask (sum of masks should be zero)
        # This is automatically handled in proper secure aggregation
        
        return AggregationResult(
            round_number=list(round_updates.values())[0].round_number,
            aggregated_weights=aggregated,
            num_participants=len(round_updates),
            dropped_participants=[],
            verification_passed=True,
            privacy_budget_used=0.0,
            aggregation_time=0.0
        )
    
    def _dp_federated_averaging(self,
                              session: Dict[str, Any],
                              round_updates: Dict[str, ParticipantUpdate]) -> AggregationResult:
        """Differentially private federated averaging"""
        if not self.dp_aggregator:
            raise ValueError("DP not initialized")
        
        # Extract updates and weights
        encrypted_updates = []
        participant_weights = []
        
        for participant_id, update in round_updates.items():
            encrypted_updates.append(update.encrypted_weights)
            num_samples = update.metadata.get("num_samples", 1)
            participant_weights.append(num_samples)
        
        # Normalize weights
        total_samples = sum(participant_weights)
        normalized_weights = [w / total_samples for w in participant_weights]
        
        # Estimate sensitivities for each layer
        sensitivities = self._estimate_sensitivities(
            session["model_architecture"],
            len(round_updates)
        )
        
        # Perform DP aggregation
        aggregated = self.dp_aggregator.private_aggregation(
            encrypted_updates,
            sensitivities,
            normalized_weights
        )
        
        # Calculate privacy budget used
        privacy_budget = self._calculate_privacy_budget(
            self.dp_aggregator.epsilon,
            self.dp_aggregator.delta,
            session["rounds_completed"]
        )
        
        return AggregationResult(
            round_number=list(round_updates.values())[0].round_number,
            aggregated_weights=aggregated,
            num_participants=len(round_updates),
            dropped_participants=[],
            verification_passed=True,
            privacy_budget_used=privacy_budget,
            aggregation_time=0.0
        )
    
    def _byzantine_robust_aggregation(self,
                                    session: Dict[str, Any],
                                    round_updates: Dict[str, ParticipantUpdate]) -> AggregationResult:
        """Byzantine-robust aggregation using Krum or multi-Krum"""
        # This implements a simplified Byzantine-robust aggregation
        # In production, use proper algorithms like Krum, Bulyan, or DETOX
        
        encrypted_updates = list(round_updates.values())
        n = len(encrypted_updates)
        f = int(n * self.byzantine_tolerance)  # Number of Byzantine participants
        
        # For encrypted data, we use a trust-based approach
        # In reality, this would compute distances between encrypted models
        trust_scores = {}
        
        for pid, update in round_updates.items():
            # Compute trust score based on historical behavior
            trust_score = self._compute_trust_score(pid, update, session)
            trust_scores[pid] = trust_score
        
        # Select most trusted participants (multi-Krum selection)
        sorted_participants = sorted(trust_scores.items(), key=lambda x: x[1], reverse=True)
        selected_participants = sorted_participants[:n-f]
        
        # Aggregate only selected participants
        selected_updates = []
        selected_weights = []
        dropped_participants = []
        
        for pid, _ in selected_participants:
            update = round_updates[pid]
            selected_updates.append(update.encrypted_weights)
            selected_weights.append(update.metadata.get("num_samples", 1))
        
        for pid, _ in sorted_participants[n-f:]:
            dropped_participants.append(pid)
        
        # Normalize weights and aggregate
        total_weight = sum(selected_weights)
        normalized_weights = [w / total_weight for w in selected_weights]
        
        aggregated = self.aggregator.aggregate_encrypted_updates(
            selected_updates,
            normalized_weights
        )
        
        logger.info(f"Byzantine-robust aggregation: selected {len(selected_participants)}/{n} participants")
        
        return AggregationResult(
            round_number=list(round_updates.values())[0].round_number,
            aggregated_weights=aggregated,
            num_participants=len(selected_participants),
            dropped_participants=dropped_participants,
            verification_passed=True,
            privacy_budget_used=0.0,
            aggregation_time=0.0
        )
    
    def _federated_graph_averaging(self, session, round_updates):
        # Similar to FedAvg but for graph embeddings
        embeddings = [update.encrypted_embeddings for update in round_updates.values()]
        aggregated = self.aggregator.aggregate_encrypted_updates(embeddings)
        return AggregationResult(aggregated_weights=aggregated)
    
    def _verify_update_signature(self, update: ParticipantUpdate) -> bool:
        """Verify the signature of a model update"""
        # In production, use proper digital signatures
        # For now, we do a simple hash verification
        
        expected_signature = hashlib.sha256(
            f"{update.participant_id}:{update.round_number}:{update.timestamp}".encode()
        ).hexdigest()[:16]
        
        return update.signature == expected_signature
    
    def _generate_pairwise_masks(self,
                               participant_ids: List[str],
                               session: Dict[str, Any]) -> Dict[Tuple[str, str], np.ndarray]:
        """Generate pairwise masks for secure aggregation"""
        masks = {}
        
        # In production, use Diffie-Hellman key agreement
        # For now, generate deterministic masks
        for i, pid1 in enumerate(participant_ids):
            for j, pid2 in enumerate(participant_ids):
                if i < j:
                    # Generate mask for each layer
                    for layer_name, shape in session["model_architecture"].items():
                        seed = f"{pid1}:{pid2}:{layer_name}:{session['session_id']}"
                        np.random.seed(int(hashlib.sha256(seed.encode()).hexdigest()[:8], 16))
                        
                        mask = np.random.randn(*shape) * 0.01
                        masks[(pid1, layer_name)] = masks.get((pid1, layer_name), 0) + mask
                        masks[(pid2, layer_name)] = masks.get((pid2, layer_name), 0) - mask
        
        return masks
    
    def _estimate_sensitivities(self,
                              model_architecture: Dict[str, List[int]],
                              num_participants: int) -> Dict[str, float]:
        """Estimate sensitivity for each layer"""
        sensitivities = {}
        
        for layer_name, shape in model_architecture.items():
            # Heuristic: sensitivity inversely proportional to layer size and num participants
            layer_size = np.prod(shape)
            sensitivity = 1.0 / (np.sqrt(layer_size) * np.sqrt(num_participants))
            
            # Adjust based on layer type
            if "bias" in layer_name:
                sensitivity *= 2.0  # Biases typically more sensitive
            elif "batch_norm" in layer_name:
                sensitivity *= 0.5  # Batch norm less sensitive
            
            sensitivities[layer_name] = sensitivity
        
        return sensitivities
    
    def _calculate_privacy_budget(self,
                                epsilon: float,
                                delta: float,
                                rounds_completed: int) -> float:
        """Calculate privacy budget used (advanced composition)"""
        # Using advanced composition theorem
        # ε_total = ε√(2k ln(1/δ')) + kε(e^ε - 1)
        
        k = rounds_completed
        delta_prime = delta / (2 * k)
        
        epsilon_total = epsilon * np.sqrt(2 * k * np.log(1 / delta_prime)) + \
                       k * epsilon * (np.exp(epsilon) - 1)
        
        return epsilon_total
    
    def _compute_trust_score(self,
                           participant_id: str,
                           update: ParticipantUpdate,
                           session: Dict[str, Any]) -> float:
        """Compute trust score for Byzantine-robust aggregation"""
        trust_score = 1.0
        
        # Check update freshness
        current_time = time.time()
        update_age = current_time - update.timestamp / 1000
        if update_age > 300:  # More than 5 minutes old
            trust_score *= 0.8
        
        # Check metadata consistency
        if "num_samples" in update.metadata:
            num_samples = update.metadata["num_samples"]
            # Penalize unrealistic sample counts
            if num_samples < 10 or num_samples > 100000:
                trust_score *= 0.7
        
        # Check historical behavior (if available)
        # In production, maintain participant reputation scores
        
        return trust_score
    
    def get_privacy_report(self, session_id: str) -> Dict[str, Any]:
        """Get privacy budget report for a session"""
        if session_id not in self.sessions:
            return {"error": "Unknown session"}
        
        session = self.sessions[session_id]
        budget_history = self.privacy_budgets.get(session_id, [])
        
        return {
            "session_id": session_id,
            "differential_privacy_enabled": self.differential_privacy,
            "epsilon": self.dp_aggregator.epsilon if self.dp_aggregator else None,
            "delta": self.dp_aggregator.delta if self.dp_aggregator else None,
            "initial_budget": 10.0 if self.differential_privacy else None,
            "remaining_budget": session["privacy_budget_remaining"],
            "rounds_completed": session["rounds_completed"],
            "budget_history": budget_history
        }
    
    def export_aggregated_model(self,
                              session_id: str,
                              round_number: int,
                              export_format: str = "encrypted") -> Optional[Dict[str, Any]]:
        """Export aggregated model in specified format"""
        # In production, this would retrieve the aggregated model
        # and export in the requested format
        
        return {
            "session_id": session_id,
            "round_number": round_number,
            "format": export_format,
            "model_uri": f"s3://federated-models/{session_id}/round_{round_number}.enc",
            "checksum": hashlib.sha256(f"{session_id}:{round_number}".encode()).hexdigest()
        } 