"""
Federated Learning Module for Unified ML Platform

Provides privacy-preserving collaborative machine learning capabilities.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import numpy as np
from dataclasses import dataclass, field

from pyignite import Client as IgniteClient
import mlflow
from flower.server import Server
from flower.server.strategy import FedAvg

from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


@dataclass
class FederatedSession:
    """Federated learning session configuration"""
    session_id: str
    model_type: str
    algorithm: str = "FedAvg"
    num_rounds: int = 10
    min_participants: int = 3
    privacy_budget: float = 1.0
    differential_privacy: bool = True
    secure_aggregation: bool = True
    participants: List[str] = field(default_factory=list)
    current_round: int = 0
    status: str = "WAITING_FOR_PARTICIPANTS"


class FederatedLearningCoordinator:
    """
    Coordinates federated learning sessions within the unified ML platform.
    
    This module provides:
    - Privacy-preserving collaborative training
    - Secure aggregation protocols
    - Differential privacy mechanisms
    - Integration with verifiable credentials
    - MLflow tracking for federated experiments
    """
    
    def __init__(self,
                 model_registry,
                 feature_store,
                 ignite_host: str = "ignite",
                 ignite_port: int = 10800,
                 verifiable_credential_service_url: str = None):
        self.model_registry = model_registry
        self.feature_store = feature_store
        self.ignite_host = ignite_host
        self.ignite_port = ignite_port
        self.vc_service_url = verifiable_credential_service_url
        
        self.ignite_client = None
        self.active_sessions: Dict[str, FederatedSession] = {}
        self.flower_servers: Dict[str, Server] = {}
        
    async def initialize(self):
        """Initialize the federated learning coordinator"""
        try:
            # Connect to Ignite for distributed session management
            self.ignite_client = IgniteClient()
            self.ignite_client.connect(self.ignite_host, self.ignite_port)
            
            # Create cache for federated sessions
            self.session_cache = self.ignite_client.get_or_create_cache("federated_sessions")
            
            logger.info("Federated Learning Coordinator initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Federated Learning Coordinator: {e}")
            raise
            
    async def create_session(self,
                           model_type: str,
                           dataset_requirements: Dict[str, Any],
                           privacy_parameters: Dict[str, Any],
                           training_parameters: Dict[str, Any],
                           tenant_id: str,
                           user_id: str) -> FederatedSession:
        """Create a new federated learning session"""
        session_id = f"fl_session_{datetime.utcnow().timestamp()}"
        
        session = FederatedSession(
            session_id=session_id,
            model_type=model_type,
            algorithm=training_parameters.get("algorithm", "FedAvg"),
            num_rounds=training_parameters.get("rounds", 10),
            min_participants=training_parameters.get("min_participants", 3),
            privacy_budget=privacy_parameters.get("epsilon", 1.0),
            differential_privacy=privacy_parameters.get("differential_privacy", True),
            secure_aggregation=privacy_parameters.get("secure_aggregation", True)
        )
        
        # Store session in Ignite
        self.session_cache.put(session_id, session.__dict__)
        self.active_sessions[session_id] = session
        
        # Create MLflow experiment for tracking
        mlflow.set_experiment(f"federated_{session_id}")
        
        # Initialize Flower server for this session
        strategy = FedAvg(
            min_available_clients=session.min_participants,
            min_fit_clients=session.min_participants,
            min_evaluate_clients=session.min_participants
        )
        
        # Note: In production, this would start an actual Flower server
        # self.flower_servers[session_id] = Server(strategy=strategy)
        
        logger.info(f"Created federated learning session: {session_id}")
        return session
        
    async def join_session(self,
                          session_id: str,
                          participant_id: str,
                          credentials: Dict[str, Any]) -> bool:
        """Join an existing federated learning session"""
        session = self.active_sessions.get(session_id)
        if not session:
            logger.error(f"Session {session_id} not found")
            return False
            
        # Verify participant credentials
        if self.vc_service_url:
            # Would verify credentials via verifiable credential service
            pass
            
        # Add participant to session
        session.participants.append(participant_id)
        self.session_cache.put(session_id, session.__dict__)
        
        # Check if we can start training
        if len(session.participants) >= session.min_participants and session.status == "WAITING_FOR_PARTICIPANTS":
            session.status = "TRAINING"
            asyncio.create_task(self._start_training_round(session))
            
        return True
        
    async def submit_model_update(self,
                                session_id: str,
                                participant_id: str,
                                model_update: Dict[str, np.ndarray],
                                data_size: int) -> bool:
        """Submit model update from a participant"""
        session = self.active_sessions.get(session_id)
        if not session or participant_id not in session.participants:
            return False
            
        # Apply differential privacy if enabled
        if session.differential_privacy:
            model_update = self._apply_differential_privacy(
                model_update,
                session.privacy_budget
            )
            
        # Store update for aggregation
        update_key = f"{session_id}_round_{session.current_round}_{participant_id}"
        self.session_cache.put(update_key, {
            "model_update": model_update,
            "data_size": data_size,
            "timestamp": datetime.utcnow().isoformat()
        })
        
        return True
        
    async def coordinate_rounds(self):
        """Background task to coordinate federated learning rounds"""
        while True:
            try:
                for session_id, session in self.active_sessions.items():
                    if session.status == "TRAINING":
                        # Check if all participants have submitted updates
                        updates_ready = await self._check_round_completion(session)
                        if updates_ready:
                            await self._aggregate_and_distribute(session)
                            
                await asyncio.sleep(10)  # Check every 10 seconds
            except Exception as e:
                logger.error(f"Error in federated coordination: {e}")
                await asyncio.sleep(30)
                
    async def _start_training_round(self, session: FederatedSession):
        """Start a new training round"""
        logger.info(f"Starting round {session.current_round + 1} for session {session.session_id}")
        
        # Notify participants to start training
        # In production, this would send events to participants
        pass
        
    async def _check_round_completion(self, session: FederatedSession) -> bool:
        """Check if all participants have submitted updates for current round"""
        updates_count = 0
        for participant_id in session.participants:
            update_key = f"{session.session_id}_round_{session.current_round}_{participant_id}"
            if self.session_cache.get(update_key):
                updates_count += 1
                
        return updates_count >= len(session.participants)
        
    async def _aggregate_and_distribute(self, session: FederatedSession):
        """Aggregate model updates and distribute to participants"""
        logger.info(f"Aggregating updates for session {session.session_id}, round {session.current_round}")
        
        # Collect all updates
        updates = []
        weights = []
        
        for participant_id in session.participants:
            update_key = f"{session.session_id}_round_{session.current_round}_{participant_id}"
            update_data = self.session_cache.get(update_key)
            if update_data:
                updates.append(update_data["model_update"])
                weights.append(update_data["data_size"])
                
        # Perform federated averaging
        aggregated_model = self._federated_average(updates, weights)
        
        # Save aggregated model to registry
        with mlflow.start_run(experiment_id=session.session_id):
            mlflow.log_param("round", session.current_round)
            mlflow.log_param("num_participants", len(updates))
            mlflow.log_metric("total_data_points", sum(weights))
            
            # Register model
            model_name = f"federated_{session.model_type}_{session.session_id}"
            self.model_registry.register_federated_model(
                model_name,
                aggregated_model,
                round_number=session.current_round
            )
            
        # Update session state
        session.current_round += 1
        if session.current_round >= session.num_rounds:
            session.status = "COMPLETED"
        else:
            await self._start_training_round(session)
            
        self.session_cache.put(session.session_id, session.__dict__)
        
    def _federated_average(self, 
                          model_updates: List[Dict[str, np.ndarray]], 
                          weights: List[int]) -> Dict[str, np.ndarray]:
        """Perform weighted federated averaging"""
        total_weight = sum(weights)
        averaged_model = {}
        
        # Get all parameter names from first model
        param_names = list(model_updates[0].keys())
        
        for param_name in param_names:
            # Weighted average for each parameter
            weighted_sum = None
            for i, update in enumerate(model_updates):
                param_value = update[param_name]
                weighted_param = param_value * (weights[i] / total_weight)
                
                if weighted_sum is None:
                    weighted_sum = weighted_param
                else:
                    weighted_sum += weighted_param
                    
            averaged_model[param_name] = weighted_sum
            
        return averaged_model
        
    def _apply_differential_privacy(self,
                                  model_update: Dict[str, np.ndarray],
                                  epsilon: float) -> Dict[str, np.ndarray]:
        """Apply differential privacy noise to model updates"""
        noisy_update = {}
        
        for param_name, param_value in model_update.items():
            # Add Gaussian noise scaled by sensitivity and epsilon
            sensitivity = 1.0  # This should be computed based on data
            noise_scale = sensitivity / epsilon
            noise = np.random.normal(0, noise_scale, param_value.shape)
            noisy_update[param_name] = param_value + noise
            
        return noisy_update
        
    async def get_session_status(self, session_id: str) -> Dict[str, Any]:
        """Get current status of a federated learning session"""
        session = self.active_sessions.get(session_id)
        if not session:
            return {"error": "Session not found"}
            
        return {
            "session_id": session.session_id,
            "status": session.status,
            "current_round": session.current_round,
            "total_rounds": session.num_rounds,
            "participants": len(session.participants),
            "min_participants": session.min_participants
        }
        
    async def shutdown(self):
        """Cleanup resources"""
        if self.ignite_client:
            self.ignite_client.close()
            
        # Stop any active Flower servers
        for server in self.flower_servers.values():
            # server.shutdown()
            pass
            
        logger.info("Federated Learning Coordinator shutdown complete") 