"""
Federated Learning Framework

Provides infrastructure for privacy-preserving distributed machine learning.
"""

from typing import Any, Dict, List, Optional, Union, Callable, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import asyncio
import numpy as np
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import hashlib
import json

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ....monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class AggregationStrategy(str, Enum):
    """Model aggregation strategies"""
    FEDERATED_AVERAGING = "fedavg"
    FEDERATED_SGD = "fedsgd"
    SECURE_AGGREGATION = "secagg"
    DIFFERENTIAL_PRIVACY = "dp"
    HOMOMORPHIC = "homomorphic"


class ClientSelectionStrategy(str, Enum):
    """Client selection strategies"""
    RANDOM = "random"
    ROUND_ROBIN = "round_robin"
    CONTRIBUTION_BASED = "contribution"
    RESOURCE_AWARE = "resource_aware"


class PrivacyMechanism(str, Enum):
    """Privacy preservation mechanisms"""
    NONE = "none"
    DIFFERENTIAL_PRIVACY = "dp"
    SECURE_MULTIPARTY = "smpc"
    HOMOMORPHIC_ENCRYPTION = "he"
    SECURE_AGGREGATION = "secagg"


@dataclass
class ModelUpdate:
    """Model update from client"""
    client_id: str
    round_number: int
    model_weights: Dict[str, np.ndarray]
    num_samples: int
    metrics: Dict[str, float]
    timestamp: datetime = field(default_factory=datetime.now)
    
    def get_weight_hash(self) -> str:
        """Get hash of model weights"""
        hasher = hashlib.sha256()
        for key in sorted(self.model_weights.keys()):
            hasher.update(key.encode())
            hasher.update(self.model_weights[key].tobytes())
        return hasher.hexdigest()


@dataclass
class FederatedRound:
    """Federated learning round"""
    round_number: int
    selected_clients: List[str]
    global_model: Dict[str, np.ndarray]
    client_updates: List[ModelUpdate] = field(default_factory=list)
    aggregated_model: Optional[Dict[str, np.ndarray]] = None
    metrics: Dict[str, float] = field(default_factory=dict)
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    
    @property
    def is_complete(self) -> bool:
        return len(self.client_updates) == len(self.selected_clients)
    
    @property
    def duration(self) -> Optional[float]:
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


@dataclass
class ClientConfig:
    """Client configuration"""
    client_id: str
    data_size: int
    compute_capacity: float  # 0-1
    network_bandwidth: float  # Mbps
    reliability: float  # 0-1
    privacy_requirements: List[PrivacyMechanism]
    
    def can_participate(self, min_requirements: Dict[str, float]) -> bool:
        """Check if client meets participation requirements"""
        return (
            self.compute_capacity >= min_requirements.get("compute", 0) and
            self.network_bandwidth >= min_requirements.get("bandwidth", 0) and
            self.reliability >= min_requirements.get("reliability", 0)
        )


@dataclass
class FederatedConfig:
    """Federated learning configuration"""
    num_rounds: int
    clients_per_round: int
    min_clients: int
    aggregation_strategy: AggregationStrategy
    selection_strategy: ClientSelectionStrategy
    privacy_mechanism: PrivacyMechanism
    
    # Privacy parameters
    epsilon: float = 1.0  # Differential privacy
    delta: float = 1e-5
    clip_norm: float = 1.0
    noise_multiplier: float = 0.1
    
    # Training parameters
    local_epochs: int = 1
    local_batch_size: int = 32
    learning_rate: float = 0.01
    
    # System parameters
    round_timeout: int = 300  # seconds
    checkpoint_frequency: int = 10
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "num_rounds": self.num_rounds,
            "clients_per_round": self.clients_per_round,
            "min_clients": self.min_clients,
            "aggregation_strategy": self.aggregation_strategy.value,
            "selection_strategy": self.selection_strategy.value,
            "privacy_mechanism": self.privacy_mechanism.value,
            "epsilon": self.epsilon,
            "delta": self.delta,
            "local_epochs": self.local_epochs,
            "learning_rate": self.learning_rate
        }


class ModelAggregator(ABC):
    """Abstract model aggregator"""
    
    @abstractmethod
    async def aggregate(
        self,
        updates: List[ModelUpdate],
        config: FederatedConfig
    ) -> Dict[str, np.ndarray]:
        """Aggregate model updates"""
        pass


class FederatedAveraging(ModelAggregator):
    """Federated averaging aggregator"""
    
    async def aggregate(
        self,
        updates: List[ModelUpdate],
        config: FederatedConfig
    ) -> Dict[str, np.ndarray]:
        """Aggregate using weighted average"""
        if not updates:
            raise ValueError("No updates to aggregate")
        
        # Calculate total samples
        total_samples = sum(u.num_samples for u in updates)
        
        # Initialize aggregated weights
        aggregated = {}
        
        # Weighted average
        for key in updates[0].model_weights.keys():
            aggregated[key] = np.zeros_like(updates[0].model_weights[key])
            
            for update in updates:
                weight = update.num_samples / total_samples
                aggregated[key] += weight * update.model_weights[key]
        
        return aggregated


class SecureAggregator(ModelAggregator):
    """Secure aggregation using secret sharing"""
    
    def __init__(self, threshold: int):
        self.threshold = threshold
    
    async def aggregate(
        self,
        updates: List[ModelUpdate],
        config: FederatedConfig
    ) -> Dict[str, np.ndarray]:
        """Aggregate using secure multi-party computation"""
        # Simplified implementation
        # In practice, this would use proper SMPC protocols
        
        if len(updates) < self.threshold:
            raise ValueError(f"Need at least {self.threshold} updates")
        
        # For demo, just average (real implementation would use secret sharing)
        aggregator = FederatedAveraging()
        return await aggregator.aggregate(updates, config)


class DifferentialPrivacyAggregator(ModelAggregator):
    """Aggregator with differential privacy"""
    
    async def aggregate(
        self,
        updates: List[ModelUpdate],
        config: FederatedConfig
    ) -> Dict[str, np.ndarray]:
        """Aggregate with differential privacy noise"""
        # First, do standard aggregation
        base_aggregator = FederatedAveraging()
        aggregated = await base_aggregator.aggregate(updates, config)
        
        # Add Gaussian noise for differential privacy
        for key in aggregated.keys():
            sensitivity = config.clip_norm / len(updates)
            noise_scale = sensitivity * config.noise_multiplier / config.epsilon
            
            noise = np.random.normal(
                0,
                noise_scale,
                aggregated[key].shape
            )
            
            aggregated[key] += noise
        
        return aggregated


class ClientSelector(ABC):
    """Abstract client selector"""
    
    @abstractmethod
    async def select_clients(
        self,
        available_clients: List[ClientConfig],
        num_clients: int,
        round_number: int
    ) -> List[str]:
        """Select clients for round"""
        pass


class RandomSelector(ClientSelector):
    """Random client selection"""
    
    async def select_clients(
        self,
        available_clients: List[ClientConfig],
        num_clients: int,
        round_number: int
    ) -> List[str]:
        """Randomly select clients"""
        import random
        
        if len(available_clients) < num_clients:
            return [c.client_id for c in available_clients]
        
        selected = random.sample(available_clients, num_clients)
        return [c.client_id for c in selected]


class ContributionBasedSelector(ClientSelector):
    """Select clients based on contribution"""
    
    def __init__(self):
        self.client_scores: Dict[str, float] = {}
    
    async def select_clients(
        self,
        available_clients: List[ClientConfig],
        num_clients: int,
        round_number: int
    ) -> List[str]:
        """Select clients with highest contribution scores"""
        # Score based on data size and reliability
        scored_clients = []
        for client in available_clients:
            score = self.client_scores.get(client.client_id, 0.5)
            score *= client.reliability
            score *= min(client.data_size / 1000, 1.0)  # Normalize data size
            scored_clients.append((score, client))
        
        # Sort by score
        scored_clients.sort(key=lambda x: x[0], reverse=True)
        
        # Select top clients
        selected = [c[1].client_id for c in scored_clients[:num_clients]]
        return selected
    
    def update_scores(self, round_result: FederatedRound):
        """Update client contribution scores"""
        for update in round_result.client_updates:
            # Update based on metrics
            quality_score = update.metrics.get("validation_accuracy", 0)
            self.client_scores[update.client_id] = (
                0.9 * self.client_scores.get(update.client_id, 0.5) +
                0.1 * quality_score
            )


class FederatedServer:
    """Federated learning server"""
    
    def __init__(
        self,
        config: FederatedConfig,
        aggregator: ModelAggregator,
        selector: ClientSelector,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None
    ):
        self.config = config
        self.aggregator = aggregator
        self.selector = selector
        self.vault_client = vault_client
        self.consul_client = consul_client
        
        self.global_model: Optional[Dict[str, np.ndarray]] = None
        self.rounds: List[FederatedRound] = []
        self.clients: Dict[str, ClientConfig] = {}
        self.current_round: Optional[FederatedRound] = None
    
    async def register_client(self, client: ClientConfig) -> bool:
        """Register new client"""
        try:
            self.clients[client.client_id] = client
            
            # Store in Consul
            if self.consul_client:
                await self.consul_client.put(
                    f"federated/clients/{client.client_id}",
                    json.dumps({
                        "client_id": client.client_id,
                        "data_size": client.data_size,
                        "compute_capacity": client.compute_capacity,
                        "network_bandwidth": client.network_bandwidth,
                        "reliability": client.reliability,
                        "privacy_requirements": [p.value for p in client.privacy_requirements]
                    })
                )
            
            logger.info(f"Registered client: {client.client_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register client: {e}")
            return False
    
    async def initialize_model(self, model: Dict[str, np.ndarray]):
        """Initialize global model"""
        self.global_model = model
        logger.info("Initialized global model")
    
    async def start_round(self) -> FederatedRound:
        """Start new federated round"""
        if self.current_round and not self.current_round.is_complete:
            raise RuntimeError("Previous round not complete")
        
        round_number = len(self.rounds) + 1
        
        # Select clients
        available_clients = [
            c for c in self.clients.values()
            if c.can_participate({"compute": 0.3, "bandwidth": 1.0, "reliability": 0.8})
        ]
        
        selected_client_ids = await self.selector.select_clients(
            available_clients,
            self.config.clients_per_round,
            round_number
        )
        
        # Create round
        self.current_round = FederatedRound(
            round_number=round_number,
            selected_clients=selected_client_ids,
            global_model=self.global_model.copy()
        )
        
        logger.info(
            f"Started round {round_number} with "
            f"{len(selected_client_ids)} clients"
        )
        
        return self.current_round
    
    async def receive_update(self, update: ModelUpdate) -> bool:
        """Receive client update"""
        if not self.current_round:
            logger.error("No active round")
            return False
        
        if update.client_id not in self.current_round.selected_clients:
            logger.error(f"Client {update.client_id} not selected for round")
            return False
        
        if update.round_number != self.current_round.round_number:
            logger.error(f"Update for wrong round: {update.round_number}")
            return False
        
        # Validate update
        if not self._validate_update(update):
            logger.error(f"Invalid update from {update.client_id}")
            return False
        
        # Add update
        self.current_round.client_updates.append(update)
        
        logger.info(
            f"Received update from {update.client_id} "
            f"({len(self.current_round.client_updates)}/"
            f"{len(self.current_round.selected_clients)})"
        )
        
        # Check if round complete
        if self.current_round.is_complete:
            await self._complete_round()
        
        return True
    
    def _validate_update(self, update: ModelUpdate) -> bool:
        """Validate client update"""
        # Check model structure
        if not self.global_model:
            return False
        
        for key in self.global_model.keys():
            if key not in update.model_weights:
                return False
            
            if update.model_weights[key].shape != self.global_model[key].shape:
                return False
        
        # Check metrics
        if "loss" not in update.metrics:
            return False
        
        return True
    
    async def _complete_round(self):
        """Complete current round"""
        if not self.current_round:
            return
        
        try:
            # Aggregate updates
            self.current_round.aggregated_model = await self.aggregator.aggregate(
                self.current_round.client_updates,
                self.config
            )
            
            # Update global model
            self.global_model = self.current_round.aggregated_model
            
            # Calculate round metrics
            self.current_round.metrics = self._calculate_round_metrics()
            
            # Mark complete
            self.current_round.completed_at = datetime.now()
            
            # Save round
            self.rounds.append(self.current_round)
            
            # Update client scores if using contribution-based selection
            if isinstance(self.selector, ContributionBasedSelector):
                self.selector.update_scores(self.current_round)
            
            # Checkpoint if needed
            if len(self.rounds) % self.config.checkpoint_frequency == 0:
                await self._checkpoint()
            
            logger.info(
                f"Completed round {self.current_round.round_number} "
                f"in {self.current_round.duration:.2f}s"
            )
            
            # Clear current round
            self.current_round = None
            
        except Exception as e:
            logger.error(f"Failed to complete round: {e}")
            raise
    
    def _calculate_round_metrics(self) -> Dict[str, float]:
        """Calculate round metrics"""
        metrics = {}
        
        # Average client metrics
        for key in self.current_round.client_updates[0].metrics.keys():
            values = [u.metrics[key] for u in self.current_round.client_updates]
            metrics[f"avg_{key}"] = np.mean(values)
            metrics[f"std_{key}"] = np.std(values)
        
        # Participation rate
        metrics["participation_rate"] = (
            len(self.current_round.client_updates) /
            len(self.current_round.selected_clients)
        )
        
        return metrics
    
    async def _checkpoint(self):
        """Save model checkpoint"""
        if not self.global_model:
            return
        
        checkpoint = {
            "round": len(self.rounds),
            "model": {k: v.tolist() for k, v in self.global_model.items()},
            "metrics": self.rounds[-1].metrics if self.rounds else {},
            "timestamp": datetime.now().isoformat()
        }
        
        # Save to Consul
        if self.consul_client:
            await self.consul_client.put(
                f"federated/checkpoints/round_{len(self.rounds)}",
                json.dumps(checkpoint)
            )
        
        logger.info(f"Saved checkpoint at round {len(self.rounds)}")
    
    async def get_global_model(self) -> Optional[Dict[str, np.ndarray]]:
        """Get current global model"""
        return self.global_model.copy() if self.global_model else None
    
    async def get_round_history(self) -> List[Dict[str, Any]]:
        """Get round history"""
        history = []
        
        for round in self.rounds:
            history.append({
                "round_number": round.round_number,
                "num_clients": len(round.selected_clients),
                "num_updates": len(round.client_updates),
                "metrics": round.metrics,
                "duration": round.duration,
                "completed_at": round.completed_at.isoformat() if round.completed_at else None
            })
        
        return history


class FederatedClient:
    """Federated learning client"""
    
    def __init__(
        self,
        client_id: str,
        config: ClientConfig,
        privacy_mechanism: Optional[PrivacyMechanism] = None
    ):
        self.client_id = client_id
        self.config = config
        self.privacy_mechanism = privacy_mechanism or PrivacyMechanism.NONE
        self.local_model: Optional[Dict[str, np.ndarray]] = None
    
    async def receive_global_model(self, model: Dict[str, np.ndarray]):
        """Receive global model from server"""
        self.local_model = {k: v.copy() for k, v in model.items()}
        logger.info(f"Client {self.client_id} received global model")
    
    async def train_local_model(
        self,
        data_loader: Any,
        epochs: int,
        learning_rate: float
    ) -> ModelUpdate:
        """Train on local data"""
        if not self.local_model:
            raise RuntimeError("No model to train")
        
        # Simulate training (in practice, use actual ML framework)
        initial_weights = {k: v.copy() for k, v in self.local_model.items()}
        
        # Training loop
        total_loss = 0
        num_batches = 0
        
        for epoch in range(epochs):
            epoch_loss = 0
            
            # Simulate batches
            for batch_idx in range(10):  # Dummy batches
                # Simulate gradient update
                for key in self.local_model.keys():
                    gradient = np.random.randn(*self.local_model[key].shape) * 0.01
                    self.local_model[key] -= learning_rate * gradient
                
                # Simulate loss
                batch_loss = np.random.random() * 0.5
                epoch_loss += batch_loss
                num_batches += 1
            
            total_loss += epoch_loss
        
        # Calculate update (difference from initial)
        model_update = {}
        for key in self.local_model.keys():
            model_update[key] = self.local_model[key] - initial_weights[key]
        
        # Apply privacy mechanism
        if self.privacy_mechanism == PrivacyMechanism.DIFFERENTIAL_PRIVACY:
            model_update = await self._apply_differential_privacy(model_update)
        
        # Create update
        update = ModelUpdate(
            client_id=self.client_id,
            round_number=1,  # Should be provided by server
            model_weights=model_update,
            num_samples=self.config.data_size,
            metrics={
                "loss": total_loss / num_batches,
                "validation_accuracy": 0.85 + np.random.random() * 0.1
            }
        )
        
        return update
    
    async def _apply_differential_privacy(
        self,
        model_update: Dict[str, np.ndarray],
        clip_norm: float = 1.0,
        noise_scale: float = 0.1
    ) -> Dict[str, np.ndarray]:
        """Apply differential privacy to model update"""
        # Clip gradients
        total_norm = 0
        for key in model_update.keys():
            total_norm += np.sum(model_update[key] ** 2)
        total_norm = np.sqrt(total_norm)
        
        if total_norm > clip_norm:
            scale = clip_norm / total_norm
            for key in model_update.keys():
                model_update[key] *= scale
        
        # Add noise
        for key in model_update.keys():
            noise = np.random.normal(
                0,
                noise_scale * clip_norm,
                model_update[key].shape
            )
            model_update[key] += noise
        
        return model_update


# Example usage

async def example_usage():
    """Example of federated learning"""
    
    # Create configuration
    config = FederatedConfig(
        num_rounds=10,
        clients_per_round=5,
        min_clients=3,
        aggregation_strategy=AggregationStrategy.DIFFERENTIAL_PRIVACY,
        selection_strategy=ClientSelectionStrategy.CONTRIBUTION_BASED,
        privacy_mechanism=PrivacyMechanism.DIFFERENTIAL_PRIVACY,
        epsilon=1.0,
        local_epochs=5,
        learning_rate=0.01
    )
    
    # Create aggregator
    aggregator = DifferentialPrivacyAggregator()
    
    # Create selector
    selector = ContributionBasedSelector()
    
    # Create server
    server = FederatedServer(config, aggregator, selector)
    
    # Register clients
    for i in range(10):
        client_config = ClientConfig(
            client_id=f"client_{i}",
            data_size=1000 + i * 100,
            compute_capacity=0.7 + np.random.random() * 0.3,
            network_bandwidth=10 + np.random.random() * 90,
            reliability=0.9 + np.random.random() * 0.1,
            privacy_requirements=[PrivacyMechanism.DIFFERENTIAL_PRIVACY]
        )
        await server.register_client(client_config)
    
    # Initialize model
    initial_model = {
        "layer1": np.random.randn(100, 50),
        "layer2": np.random.randn(50, 10),
        "bias1": np.zeros(50),
        "bias2": np.zeros(10)
    }
    
    await server.initialize_model(initial_model)
    
    # Run federated learning rounds
    for round_num in range(3):
        print(f"\n--- Round {round_num + 1} ---")
        
        # Start round
        round = await server.start_round()
        print(f"Selected clients: {round.selected_clients}")
        
        # Simulate client training
        for client_id in round.selected_clients:
            # Create client
            client_config = server.clients[client_id]
            client = FederatedClient(
                client_id,
                client_config,
                PrivacyMechanism.DIFFERENTIAL_PRIVACY
            )
            
            # Receive global model
            global_model = await server.get_global_model()
            await client.receive_global_model(global_model)
            
            # Train locally
            update = await client.train_local_model(
                data_loader=None,  # Dummy
                epochs=config.local_epochs,
                learning_rate=config.learning_rate
            )
            
            # Update round number
            update.round_number = round.round_number
            
            # Send update to server
            await server.receive_update(update)
        
        # Print round metrics
        if server.rounds:
            last_round = server.rounds[-1]
            print(f"Round {last_round.round_number} metrics:")
            for key, value in last_round.metrics.items():
                print(f"  {key}: {value:.4f}")
    
    # Get history
    history = await server.get_round_history()
    print(f"\nTraining history: {len(history)} rounds completed")


if __name__ == "__main__":
    asyncio.run(example_usage()) 