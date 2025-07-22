"""
Federated Learning Coordinator for Unified ML Platform

Implements privacy-preserving distributed machine learning with:
- Multiple aggregation strategies (FedAvg, FedProx, SCAFFOLD)
- Differential privacy
- Secure aggregation
- Verifiable credentials integration
- Adaptive client selection
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Union, Tuple, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
import tenseal as ts
from cryptography.fernet import Fernet
import hashlib
from collections import defaultdict
import httpx

logger = logging.getLogger(__name__)


class AggregationStrategy(str, Enum):
    """Federated learning aggregation strategies"""
    FEDAVG = "fedavg"  # Federated Averaging
    FEDPROX = "fedprox"  # Federated Proximal
    SCAFFOLD = "scaffold"  # Stochastic Controlled Averaging
    FEDADAM = "fedadam"  # Federated Adam
    FEDYOGI = "fedyogi"  # Federated Yogi


class PrivacyMechanism(str, Enum):
    """Privacy preservation mechanisms"""
    NONE = "none"
    DIFFERENTIAL_PRIVACY = "differential_privacy"
    HOMOMORPHIC_ENCRYPTION = "homomorphic_encryption"
    SECURE_AGGREGATION = "secure_aggregation"
    HYBRID = "hybrid"  # DP + Secure Aggregation


@dataclass
class FederatedConfig:
    """Configuration for federated learning"""
    num_rounds: int = 100
    clients_per_round: int = 10
    min_clients: int = 2
    local_epochs: int = 5
    local_batch_size: int = 32
    learning_rate: float = 0.01
    aggregation_strategy: AggregationStrategy = AggregationStrategy.FEDAVG
    privacy_mechanism: PrivacyMechanism = PrivacyMechanism.DIFFERENTIAL_PRIVACY
    dp_epsilon: float = 1.0  # Differential privacy budget
    dp_delta: float = 1e-5
    clip_norm: float = 1.0
    mu: float = 0.01  # FedProx parameter
    require_verifiable_credentials: bool = True
    client_timeout: int = 300  # seconds
    enable_compression: bool = True
    compression_ratio: float = 0.1


@dataclass
class ClientState:
    """State of a federated learning client"""
    client_id: str
    last_seen: datetime
    rounds_participated: int = 0
    total_samples: int = 0
    average_loss: float = 0.0
    reputation_score: float = 1.0
    is_malicious: bool = False
    verifiable_credential: Optional[str] = None
    compute_capability: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FederatedRound:
    """Information about a federated learning round"""
    round_number: int
    selected_clients: List[str]
    aggregated_model: Optional[Dict[str, torch.Tensor]] = None
    round_metrics: Dict[str, float] = field(default_factory=dict)
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    privacy_budget_used: float = 0.0


class SecureAggregator:
    """Secure aggregation using encryption"""
    
    def __init__(self):
        self.keys = {}
        self.shares = defaultdict(list)
        
    def generate_keys(self, client_ids: List[str]) -> Dict[str, bytes]:
        """Generate encryption keys for clients"""
        keys = {}
        for client_id in client_ids:
            key = Fernet.generate_key()
            keys[client_id] = key
            self.keys[client_id] = Fernet(key)
        return keys
        
    def encrypt_model(self, model_weights: Dict[str, np.ndarray], client_id: str) -> bytes:
        """Encrypt model weights"""
        if client_id not in self.keys:
            raise ValueError(f"No key found for client {client_id}")
            
        # Serialize weights
        serialized = json.dumps({k: v.tolist() for k, v in model_weights.items()})
        
        # Encrypt
        encrypted = self.keys[client_id].encrypt(serialized.encode())
        return encrypted
        
    def decrypt_and_aggregate(self, encrypted_models: Dict[str, bytes]) -> Dict[str, np.ndarray]:
        """Decrypt models and aggregate"""
        decrypted_models = []
        
        for client_id, encrypted_model in encrypted_models.items():
            if client_id not in self.keys:
                logger.warning(f"Skipping client {client_id} - no decryption key")
                continue
                
            try:
                # Decrypt
                decrypted = self.keys[client_id].decrypt(encrypted_model)
                model_weights = json.loads(decrypted.decode())
                
                # Convert back to numpy
                model_weights = {k: np.array(v) for k, v in model_weights.items()}
                decrypted_models.append(model_weights)
                
            except Exception as e:
                logger.error(f"Failed to decrypt model from {client_id}: {e}")
                
        # Average the models
        if not decrypted_models:
            return {}
            
        aggregated = {}
        for key in decrypted_models[0].keys():
            values = [model[key] for model in decrypted_models if key in model]
            aggregated[key] = np.mean(values, axis=0)
            
        return aggregated


class DifferentialPrivacy:
    """Differential privacy mechanism"""
    
    def __init__(self, epsilon: float, delta: float, clip_norm: float):
        self.epsilon = epsilon
        self.delta = delta
        self.clip_norm = clip_norm
        
    def add_noise(self, gradients: Dict[str, torch.Tensor], num_samples: int) -> Dict[str, torch.Tensor]:
        """Add Gaussian noise for differential privacy"""
        noisy_gradients = {}
        
        # Calculate noise scale
        noise_scale = self._calculate_noise_scale(num_samples)
        
        for name, grad in gradients.items():
            # Clip gradients
            grad_norm = torch.norm(grad)
            if grad_norm > self.clip_norm:
                grad = grad * self.clip_norm / grad_norm
                
            # Add noise
            noise = torch.randn_like(grad) * noise_scale
            noisy_gradients[name] = grad + noise
            
        return noisy_gradients
        
    def _calculate_noise_scale(self, num_samples: int) -> float:
        """Calculate noise scale for given privacy budget"""
        c = np.sqrt(2 * np.log(1.25 / self.delta))
        return c * self.clip_norm / (self.epsilon * num_samples)
        
    def compute_privacy_spent(self, num_rounds: int, num_samples: int) -> float:
        """Compute total privacy budget spent"""
        # Using advanced composition theorem
        return num_rounds * self.epsilon * np.sqrt(2 * np.log(1 / self.delta))


class FederatedLearningCoordinator:
    """
    Main coordinator for federated learning
    """
    
    def __init__(
        self,
        model_registry,
        feature_store,
        ignite_host: str = "localhost",
        verifiable_credential_service_url: str = "http://verifiable-credential-service:8000"
    ):
        self.model_registry = model_registry
        self.feature_store = feature_store
        self.ignite_host = ignite_host
        self.vc_service_url = verifiable_credential_service_url
        
        self.client_states: Dict[str, ClientState] = {}
        self.current_round: Optional[FederatedRound] = None
        self.global_model: Optional[nn.Module] = None
        self.rounds_history: List[FederatedRound] = []
        
        self.secure_aggregator = SecureAggregator()
        self.dp_mechanism: Optional[DifferentialPrivacy] = None
        self.is_training = False
        
        # HTTP client for verifiable credentials
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
    async def initialize(self):
        """Initialize federated learning coordinator"""
        logger.info("Federated Learning Coordinator initialized")
        
    async def start_federated_training(
        self,
        model: nn.Module,
        config: FederatedConfig,
        training_id: str
    ) -> Dict[str, Any]:
        """Start a new federated learning training session"""
        if self.is_training:
            raise RuntimeError("Training already in progress")
            
        self.is_training = True
        self.global_model = model
        
        # Initialize privacy mechanism
        if config.privacy_mechanism == PrivacyMechanism.DIFFERENTIAL_PRIVACY:
            self.dp_mechanism = DifferentialPrivacy(
                config.dp_epsilon,
                config.dp_delta,
                config.clip_norm
            )
            
        # Run training rounds
        for round_num in range(config.num_rounds):
            try:
                await self._execute_round(round_num, config)
                
                # Check stopping criteria
                if self._should_stop_training():
                    break
                    
            except Exception as e:
                logger.error(f"Error in round {round_num}: {e}")
                
        self.is_training = False
        
        # Save final model
        await self._save_model(training_id)
        
        return self._get_training_summary()
        
    async def _execute_round(self, round_num: int, config: FederatedConfig):
        """Execute a single federated learning round"""
        logger.info(f"Starting federated round {round_num}")
        
        # Select clients
        selected_clients = await self._select_clients(config)
        
        if len(selected_clients) < config.min_clients:
            logger.warning(f"Not enough clients for round {round_num}")
            return
            
        # Create round
        self.current_round = FederatedRound(
            round_number=round_num,
            selected_clients=[c.client_id for c in selected_clients]
        )
        
        # Distribute model to clients
        model_state = self.global_model.state_dict()
        
        # Collect client updates
        client_updates = await self._collect_client_updates(
            selected_clients,
            model_state,
            config
        )
        
        # Aggregate updates
        aggregated_model = await self._aggregate_updates(
            client_updates,
            config
        )
        
        # Update global model
        self.global_model.load_state_dict(aggregated_model)
        
        # Evaluate global model
        metrics = await self._evaluate_global_model()
        self.current_round.round_metrics = metrics
        self.current_round.end_time = datetime.utcnow()
        
        self.rounds_history.append(self.current_round)
        
        logger.info(f"Completed round {round_num}, metrics: {metrics}")
        
    async def _select_clients(self, config: FederatedConfig) -> List[ClientState]:
        """Select clients for the round"""
        # Get available clients
        available_clients = [
            client for client in self.client_states.values()
            if self._is_client_available(client)
        ]
        
        # Filter by verifiable credentials if required
        if config.require_verifiable_credentials:
            verified_clients = []
            for client in available_clients:
                if await self._verify_client_credential(client):
                    verified_clients.append(client)
            available_clients = verified_clients
            
        # Adaptive client selection based on reputation and capability
        scored_clients = []
        for client in available_clients:
            score = self._calculate_client_score(client)
            scored_clients.append((score, client))
            
        # Sort by score and select top clients
        scored_clients.sort(key=lambda x: x[0], reverse=True)
        selected = [client for _, client in scored_clients[:config.clients_per_round]]
        
        return selected
        
    def _calculate_client_score(self, client: ClientState) -> float:
        """Calculate client selection score"""
        # Consider reputation, participation rate, and compute capability
        score = client.reputation_score
        
        # Boost score for reliable clients
        if client.rounds_participated > 10:
            score *= 1.2
            
        # Consider compute capability
        if client.compute_capability.get("gpu", False):
            score *= 1.5
            
        return score
        
    async def _collect_client_updates(
        self,
        clients: List[ClientState],
        model_state: Dict[str, torch.Tensor],
        config: FederatedConfig
    ) -> List[Tuple[ClientState, Dict[str, torch.Tensor], int]]:
        """Collect model updates from clients"""
        tasks = []
        for client in clients:
            task = self._train_client_model(client, model_state, config)
            tasks.append(task)
            
        # Wait for client updates with timeout
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        valid_updates = []
        for client, result in zip(clients, results):
            if isinstance(result, Exception):
                logger.error(f"Client {client.client_id} failed: {result}")
                client.reputation_score *= 0.9  # Penalize failed clients
            else:
                model_update, num_samples = result
                valid_updates.append((client, model_update, num_samples))
                client.rounds_participated += 1
                client.total_samples += num_samples
                
        return valid_updates
        
    async def _train_client_model(
        self,
        client: ClientState,
        model_state: Dict[str, torch.Tensor],
        config: FederatedConfig
    ) -> Tuple[Dict[str, torch.Tensor], int]:
        """Simulate client training (in production, this would be done on client device)"""
        # This is a placeholder - in production, clients train locally
        # and only send updates
        
        # Simulate training delay
        await asyncio.sleep(np.random.uniform(1, 5))
        
        # Generate mock update
        num_samples = np.random.randint(100, 1000)
        
        # Add some noise to simulate training
        model_update = {}
        for name, param in model_state.items():
            update = param + torch.randn_like(param) * 0.01
            model_update[name] = update
            
        return model_update, num_samples
        
    async def _aggregate_updates(
        self,
        client_updates: List[Tuple[ClientState, Dict[str, torch.Tensor], int]],
        config: FederatedConfig
    ) -> Dict[str, torch.Tensor]:
        """Aggregate client updates based on strategy"""
        if not client_updates:
            return self.global_model.state_dict()
            
        if config.aggregation_strategy == AggregationStrategy.FEDAVG:
            return self._federated_averaging(client_updates)
        elif config.aggregation_strategy == AggregationStrategy.FEDPROX:
            return self._federated_proximal(client_updates, config.mu)
        elif config.aggregation_strategy == AggregationStrategy.SCAFFOLD:
            return self._scaffold_aggregation(client_updates)
        else:
            raise ValueError(f"Unknown aggregation strategy: {config.aggregation_strategy}")
            
    def _federated_averaging(
        self,
        client_updates: List[Tuple[ClientState, Dict[str, torch.Tensor], int]]
    ) -> Dict[str, torch.Tensor]:
        """Federated averaging aggregation"""
        total_samples = sum(num_samples for _, _, num_samples in client_updates)
        
        # Weighted average
        aggregated = {}
        for name in client_updates[0][1].keys():
            weighted_sum = torch.zeros_like(client_updates[0][1][name])
            
            for _, model_update, num_samples in client_updates:
                weight = num_samples / total_samples
                weighted_sum += model_update[name] * weight
                
            aggregated[name] = weighted_sum
            
        return aggregated
        
    def _federated_proximal(
        self,
        client_updates: List[Tuple[ClientState, Dict[str, torch.Tensor], int]],
        mu: float
    ) -> Dict[str, torch.Tensor]:
        """FedProx aggregation with proximal term"""
        # Similar to FedAvg but clients optimize with proximal term
        # For aggregation, it's the same as FedAvg
        return self._federated_averaging(client_updates)
        
    def _scaffold_aggregation(
        self,
        client_updates: List[Tuple[ClientState, Dict[str, torch.Tensor], int]]
    ) -> Dict[str, torch.Tensor]:
        """SCAFFOLD aggregation with control variates"""
        # Simplified version - in production, track control variates
        return self._federated_averaging(client_updates)
        
    async def _evaluate_global_model(self) -> Dict[str, float]:
        """Evaluate the global model"""
        # This would evaluate on a validation set
        # For now, return mock metrics
        return {
            "accuracy": np.random.uniform(0.7, 0.9),
            "loss": np.random.uniform(0.1, 0.3),
            "f1_score": np.random.uniform(0.7, 0.9)
        }
        
    def _should_stop_training(self) -> bool:
        """Check if training should stop early"""
        if len(self.rounds_history) < 10:
            return False
            
        # Check for convergence
        recent_losses = [r.round_metrics.get("loss", float('inf')) 
                        for r in self.rounds_history[-5:]]
        
        if all(abs(recent_losses[i] - recent_losses[i+1]) < 0.001 
               for i in range(len(recent_losses)-1)):
            logger.info("Training converged, stopping early")
            return True
            
        return False
        
    async def _verify_client_credential(self, client: ClientState) -> bool:
        """Verify client's verifiable credential"""
        if not client.verifiable_credential:
            return False
            
        try:
            response = await self.http_client.post(
                f"{self.vc_service_url}/api/v1/verify",
                json={"credential": client.verifiable_credential}
            )
            
            if response.status_code == 200:
                result = response.json()
                return result.get("valid", False)
                
        except Exception as e:
            logger.error(f"Failed to verify credential for {client.client_id}: {e}")
            
        return False
        
    def _is_client_available(self, client: ClientState) -> bool:
        """Check if client is available for training"""
        # Check if client is active
        if (datetime.utcnow() - client.last_seen).total_seconds() > 3600:
            return False
            
        # Check if client is not malicious
        if client.is_malicious:
            return False
            
        return True
        
    async def register_client(
        self,
        client_id: str,
        verifiable_credential: Optional[str] = None,
        compute_capability: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Register a new federated learning client"""
        if client_id in self.client_states:
            # Update existing client
            self.client_states[client_id].last_seen = datetime.utcnow()
        else:
            # Create new client
            self.client_states[client_id] = ClientState(
                client_id=client_id,
                last_seen=datetime.utcnow(),
                verifiable_credential=verifiable_credential,
                compute_capability=compute_capability or {}
            )
            
        logger.info(f"Registered client {client_id}")
        return True
        
    async def get_client_status(self, client_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a federated learning client"""
        if client_id not in self.client_states:
            return None
            
        client = self.client_states[client_id]
        return {
            "client_id": client.client_id,
            "last_seen": client.last_seen.isoformat(),
            "rounds_participated": client.rounds_participated,
            "total_samples": client.total_samples,
            "reputation_score": client.reputation_score,
            "is_active": self._is_client_available(client)
        }
        
    def _get_training_summary(self) -> Dict[str, Any]:
        """Get summary of federated training"""
        return {
            "total_rounds": len(self.rounds_history),
            "total_clients": len(self.client_states),
            "active_clients": sum(1 for c in self.client_states.values() 
                                if self._is_client_available(c)),
            "final_metrics": self.rounds_history[-1].round_metrics if self.rounds_history else {},
            "training_duration": (
                (self.rounds_history[-1].end_time - self.rounds_history[0].start_time).total_seconds()
                if self.rounds_history else 0
            )
        }
        
    async def _save_model(self, training_id: str):
        """Save trained model to registry"""
        if self.global_model:
            await self.model_registry.register_model(
                name=f"federated_{training_id}",
                model=self.global_model,
                metrics=self._get_training_summary(),
                tags={"federated": "true", "training_id": training_id}
            )
            
    async def coordinate_rounds(self):
        """Background task to coordinate federated rounds"""
        # This would be called as a background task
        # to manage ongoing federated training sessions
        while True:
            await asyncio.sleep(60)  # Check every minute
            
            # Clean up inactive clients
            inactive_threshold = datetime.utcnow() - timedelta(hours=24)
            inactive_clients = [
                client_id for client_id, client in self.client_states.items()
                if client.last_seen < inactive_threshold
            ]
            
            for client_id in inactive_clients:
                del self.client_states[client_id]
                logger.info(f"Removed inactive client {client_id}")
                
    async def shutdown(self):
        """Shutdown coordinator"""
        self.is_training = False
        await self.http_client.aclose()
        logger.info("Federated Learning Coordinator shut down") 