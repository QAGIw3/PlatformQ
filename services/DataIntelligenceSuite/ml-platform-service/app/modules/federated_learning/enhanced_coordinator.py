"""
Enhanced Federated Learning Coordinator

Uses the new federated learning framework from data-intelligence-common
to provide advanced federated learning capabilities.
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Union, Tuple, Set
from datetime import datetime, timedelta
import json
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader

from data_intelligence_common.integrations.federated_learning import (
    FederatedLearningFramework,
    FederationConfig,
    ClientConfig,
    TrainingRound,
    ModelUpdate,
    AggregationStrategy,
    PrivacyConfig,
    PrivacyMechanism,
    SecureAggregationProtocol,
    DifferentialPrivacyConfig,
    ClientSelection,
    ClientMetrics
)

from .federated_coordinator import (
    FederatedConfig,
    ClientState,
    FederatedRound,
    FederatedLearningCoordinator as BaseFederatedLearningCoordinator
)

logger = logging.getLogger(__name__)


class EnhancedFederatedLearningCoordinator(BaseFederatedLearningCoordinator):
    """
    Enhanced Federated Learning Coordinator using the new framework
    """
    
    def __init__(
        self,
        model_registry,
        feature_store,
        ignite_host: str = "localhost",
        verifiable_credential_service_url: str = "http://verifiable-credential-service:8000",
        vault_client=None,
        consul_client=None
    ):
        super().__init__(
            model_registry,
            feature_store,
            ignite_host,
            verifiable_credential_service_url
        )
        
        # Initialize the new federated learning framework
        self.fl_framework: Optional[FederatedLearningFramework] = None
        self.vault_client = vault_client
        self.consul_client = consul_client
        
    async def initialize(self):
        """Initialize enhanced federated learning coordinator"""
        await super().initialize()
        
        # Create federation config
        federation_config = FederationConfig(
            federation_id="ml-platform-federation",
            min_clients=2,
            max_clients=100,
            rounds_per_epoch=10,
            client_fraction=0.1,
            aggregation_strategy=AggregationStrategy.FEDAVG,
            vault_client=self.vault_client,
            consul_client=self.consul_client
        )
        
        # Initialize the framework
        self.fl_framework = FederatedLearningFramework(federation_config)
        await self.fl_framework.initialize()
        
        logger.info("Enhanced Federated Learning Coordinator initialized")
    
    async def start_federated_training(
        self,
        model: nn.Module,
        config: FederatedConfig,
        dataset_id: str,
        num_rounds: int,
        min_clients: int
    ) -> str:
        """
        Start federated training using the enhanced framework
        
        Args:
            model: PyTorch model to train
            config: Federated learning configuration
            dataset_id: ID of the dataset in feature store
            num_rounds: Number of training rounds
            min_clients: Minimum number of clients required
            
        Returns:
            Training job ID
        """
        try:
            # Convert to framework's aggregation strategy
            aggregation_strategy = self._convert_aggregation_strategy(config.aggregation_strategy)
            
            # Create privacy config if needed
            privacy_config = None
            if config.privacy_mechanism != "none":
                privacy_config = PrivacyConfig(
                    mechanism=self._convert_privacy_mechanism(config.privacy_mechanism),
                    differential_privacy=DifferentialPrivacyConfig(
                        epsilon=config.differential_privacy.epsilon if config.differential_privacy else 1.0,
                        delta=config.differential_privacy.delta if config.differential_privacy else 1e-5,
                        clip_norm=config.differential_privacy.clip_norm if config.differential_privacy else 1.0
                    ) if config.privacy_mechanism == "differential_privacy" else None,
                    secure_aggregation=SecureAggregationProtocol(
                        protocol="shamir",
                        threshold=min_clients // 2,
                        key_size=2048
                    ) if config.privacy_mechanism in ["secure_aggregation", "hybrid"] else None
                )
            
            # Update federation config
            self.fl_framework.config.aggregation_strategy = aggregation_strategy
            self.fl_framework.config.min_clients = min_clients
            self.fl_framework.config.privacy_config = privacy_config
            
            # Register clients
            for client_id, client_state in self.client_states.items():
                client_config = ClientConfig(
                    client_id=client_id,
                    compute_capability=client_state.compute_capability,
                    data_size=client_state.data_size,
                    network_bandwidth=client_state.compute_capability.get("network_bandwidth", 100),
                    battery_level=client_state.compute_capability.get("battery_level", 100),
                    reliability_score=client_state.reputation_score
                )
                await self.fl_framework.register_client(client_config)
            
            # Start training
            training_job_id = await self.fl_framework.start_training(
                model_architecture=self._serialize_model(model),
                num_rounds=num_rounds,
                round_timeout=config.round_timeout
            )
            
            # Monitor training progress
            asyncio.create_task(self._monitor_training_progress(training_job_id))
            
            return training_job_id
            
        except Exception as e:
            logger.error(f"Failed to start federated training: {e}")
            raise
    
    async def _monitor_training_progress(self, training_job_id: str):
        """Monitor training progress and update internal state"""
        try:
            while True:
                # Get current round info
                round_info = await self.fl_framework.get_current_round()
                if not round_info:
                    break
                
                # Update current round
                self.current_round = FederatedRound(
                    round_number=round_info.round_number,
                    selected_clients=round_info.selected_clients,
                    round_metrics=round_info.metrics
                )
                
                # Check if training is complete
                status = await self.fl_framework.get_training_status(training_job_id)
                if status in ["completed", "failed", "cancelled"]:
                    break
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
        except Exception as e:
            logger.error(f"Error monitoring training progress: {e}")
    
    async def submit_client_update(
        self,
        client_id: str,
        model_update: Dict[str, torch.Tensor],
        metrics: Dict[str, float],
        num_samples: int
    ) -> bool:
        """
        Submit client update using the enhanced framework
        
        Args:
            client_id: Client identifier
            model_update: Model weight updates
            metrics: Training metrics
            num_samples: Number of samples used
            
        Returns:
            Success status
        """
        try:
            # Convert torch tensors to numpy arrays
            numpy_update = {k: v.cpu().numpy() for k, v in model_update.items()}
            
            # Create model update
            update = ModelUpdate(
                client_id=client_id,
                round_number=self.current_round.round_number if self.current_round else 0,
                model_weights=numpy_update,
                metrics=ClientMetrics(
                    loss=metrics.get("loss", 0.0),
                    accuracy=metrics.get("accuracy", 0.0),
                    num_samples=num_samples,
                    training_time=metrics.get("training_time", 0.0),
                    custom_metrics=metrics
                ),
                timestamp=datetime.utcnow()
            )
            
            # Submit update
            success = await self.fl_framework.submit_update(update)
            
            # Update client state
            if client_id in self.client_states:
                self.client_states[client_id].last_update = datetime.utcnow()
                self.client_states[client_id].rounds_participated += 1
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to submit client update: {e}")
            return False
    
    async def get_global_model(self) -> Optional[Dict[str, np.ndarray]]:
        """Get the current global model"""
        try:
            global_model = await self.fl_framework.get_global_model()
            return global_model
        except Exception as e:
            logger.error(f"Failed to get global model: {e}")
            return None
    
    async def evaluate_global_model(
        self,
        test_data: DataLoader,
        metrics_fn: callable
    ) -> Dict[str, float]:
        """
        Evaluate the global model
        
        Args:
            test_data: Test data loader
            metrics_fn: Function to compute metrics
            
        Returns:
            Evaluation metrics
        """
        try:
            # Get global model
            global_weights = await self.get_global_model()
            if not global_weights:
                return {}
            
            # Convert to torch tensors
            torch_weights = {k: torch.from_numpy(v) for k, v in global_weights.items()}
            
            # Load into model
            self.global_model.load_state_dict(torch_weights)
            
            # Evaluate
            metrics = metrics_fn(self.global_model, test_data)
            
            # Report to framework
            await self.fl_framework.report_evaluation_metrics(metrics)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to evaluate global model: {e}")
            return {}
    
    async def get_training_analytics(self) -> Dict[str, Any]:
        """Get comprehensive training analytics"""
        try:
            # Get analytics from framework
            analytics = await self.fl_framework.get_training_analytics()
            
            # Add coordinator-specific analytics
            analytics.update({
                "total_clients": len(self.client_states),
                "active_clients": len([c for c in self.client_states.values() if c.is_active]),
                "rounds_completed": len(self.rounds_history),
                "current_round": self.current_round.round_number if self.current_round else 0
            })
            
            return analytics
            
        except Exception as e:
            logger.error(f"Failed to get training analytics: {e}")
            return {}
    
    def _convert_aggregation_strategy(self, strategy: str) -> AggregationStrategy:
        """Convert string strategy to enum"""
        mapping = {
            "fedavg": AggregationStrategy.FEDAVG,
            "fedprox": AggregationStrategy.FEDPROX,
            "scaffold": AggregationStrategy.SCAFFOLD,
            "fedadam": AggregationStrategy.FEDADAM,
            "fedyogi": AggregationStrategy.FEDYOGI
        }
        return mapping.get(strategy.lower(), AggregationStrategy.FEDAVG)
    
    def _convert_privacy_mechanism(self, mechanism: str) -> PrivacyMechanism:
        """Convert string privacy mechanism to enum"""
        mapping = {
            "none": PrivacyMechanism.NONE,
            "differential_privacy": PrivacyMechanism.DIFFERENTIAL_PRIVACY,
            "homomorphic_encryption": PrivacyMechanism.HOMOMORPHIC_ENCRYPTION,
            "secure_aggregation": PrivacyMechanism.SECURE_AGGREGATION,
            "hybrid": PrivacyMechanism.HYBRID
        }
        return mapping.get(mechanism.lower(), PrivacyMechanism.NONE)
    
    def _serialize_model(self, model: nn.Module) -> Dict[str, Any]:
        """Serialize PyTorch model for framework"""
        return {
            "architecture": str(model),
            "state_dict": {k: v.cpu().numpy().tolist() for k, v in model.state_dict().items()},
            "config": {
                "input_shape": getattr(model, "input_shape", None),
                "output_shape": getattr(model, "output_shape", None)
            }
        }
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.fl_framework:
            await self.fl_framework.shutdown()
        await super().cleanup() 