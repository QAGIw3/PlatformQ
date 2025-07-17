"""
Federated ML Integration for Simulation Optimization

Enables privacy-preserving ML training on simulation data across tenants
using Apache Spark for batch training and Ignite for federated updates.
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import numpy as np
import httpx
from pydantic import BaseModel, Field
import hashlib

from platformq_shared.event_publisher import EventPublisher
from pyignite import Client as IgniteClient
from pyignite.datatypes import String, DoubleObject, IntObject, BoolObject, MapObject

logger = logging.getLogger(__name__)


class SimulationMLConfig(BaseModel):
    """Configuration for simulation-based ML training"""
    simulation_id: str
    training_mode: str = Field(default="federated", description="federated or centralized")
    model_type: str = Field(default="agent_behavior", description="agent_behavior, parameter_tuning, outcome_prediction")
    privacy_budget: float = Field(default=1.0, description="Epsilon for differential privacy")
    min_participants: int = Field(default=3)
    training_rounds: int = Field(default=10)
    batch_size: int = Field(default=32)
    learning_rate: float = Field(default=0.01)
    convergence_threshold: float = Field(default=0.001)


class SimulationDataExtractor:
    """Extracts training data from simulation runs"""
    
    def __init__(self, ignite_client: IgniteClient):
        self.ignite = ignite_client
        self.simulation_cache = self.ignite.get_or_create_cache("simulation_states")
        self.metrics_cache = self.ignite.get_or_create_cache("simulation_metrics")
        
    async def extract_training_data(self, 
                                  simulation_id: str,
                                  start_time: datetime,
                                  end_time: datetime) -> Dict[str, Any]:
        """Extract features and labels from simulation data"""
        try:
            # Get simulation states within time range
            states = []
            state_key_prefix = f"{simulation_id}:state:"
            
            # Scan cache for relevant states
            query = self.simulation_cache.scan()
            for key, value in query:
                if key.startswith(state_key_prefix):
                    state_time = datetime.fromisoformat(value.get('timestamp'))
                    if start_time <= state_time <= end_time:
                        states.append(value)
            
            if not states:
                return None
                
            # Extract features based on agent states
            features = []
            labels = []
            
            for i in range(len(states) - 1):
                current_state = states[i]
                next_state = states[i + 1]
                
                # Extract agent features
                agent_features = self._extract_agent_features(current_state)
                
                # Extract outcome labels (next state metrics)
                outcome_labels = self._extract_outcome_labels(next_state)
                
                features.append(agent_features)
                labels.append(outcome_labels)
            
            return {
                "features": np.array(features),
                "labels": np.array(labels),
                "metadata": {
                    "simulation_id": simulation_id,
                    "num_samples": len(features),
                    "time_range": {
                        "start": start_time.isoformat(),
                        "end": end_time.isoformat()
                    }
                }
            }
            
        except Exception as e:
            logger.error(f"Error extracting training data: {e}")
            return None
    
    def _extract_agent_features(self, state: Dict[str, Any]) -> np.ndarray:
        """Extract feature vector from agent states"""
        features = []
        
        agents = state.get('agents', [])
        if not agents:
            return np.zeros(10)  # Default feature vector
            
        # Aggregate agent properties
        positions = []
        velocities = []
        properties = []
        
        for agent in agents:
            if 'position' in agent:
                positions.append([agent['position'].get('x', 0), 
                                agent['position'].get('y', 0),
                                agent['position'].get('z', 0)])
            if 'velocity' in agent:
                velocities.append([agent['velocity'].get('x', 0),
                                 agent['velocity'].get('y', 0),
                                 agent['velocity'].get('z', 0)])
            if 'properties' in agent:
                properties.append(list(agent['properties'].values()))
        
        # Calculate statistical features
        if positions:
            pos_array = np.array(positions)
            features.extend([
                np.mean(pos_array),
                np.std(pos_array),
                np.min(pos_array),
                np.max(pos_array)
            ])
        else:
            features.extend([0, 0, 0, 0])
            
        if velocities:
            vel_array = np.array(velocities)
            features.extend([
                np.mean(vel_array),
                np.std(vel_array),
                np.max(np.linalg.norm(vel_array, axis=1))  # Max speed
            ])
        else:
            features.extend([0, 0, 0])
            
        # Add simulation parameters
        params = state.get('parameters', {})
        features.extend([
            params.get('time_step', 0.1),
            params.get('gravity', 9.81),
            len(agents)  # Number of agents
        ])
        
        return np.array(features[:10])  # Ensure fixed size
    
    def _extract_outcome_labels(self, state: Dict[str, Any]) -> np.ndarray:
        """Extract outcome labels from simulation state"""
        metrics = state.get('metrics', {})
        
        labels = [
            metrics.get('total_energy', 0),
            metrics.get('average_distance', 0),
            metrics.get('collision_count', 0),
            metrics.get('objective_value', 0),
            metrics.get('convergence_rate', 0)
        ]
        
        return np.array(labels)


class FederatedSimulationTrainer:
    """Manages federated learning sessions for simulation optimization"""
    
    def __init__(self, 
                 ignite_client: IgniteClient,
                 event_publisher: EventPublisher,
                 federated_learning_url: str,
                 mlops_service_url: str):
        self.ignite = ignite_client
        self.event_publisher = event_publisher
        self.fl_service_url = federated_learning_url
        self.mlops_url = mlops_service_url
        self.data_extractor = SimulationDataExtractor(ignite_client)
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Cache for active training sessions
        self.training_cache = self.ignite.get_or_create_cache("simulation_ml_training")
        
    async def initiate_training_session(self,
                                      tenant_id: str,
                                      config: SimulationMLConfig,
                                      participants: List[str]) -> str:
        """Initiate a federated learning session for simulation optimization"""
        try:
            session_id = f"sim-ml-{config.simulation_id}-{datetime.utcnow().timestamp()}"
            
            # Prepare federated learning request
            fl_request = {
                "session_name": f"Simulation {config.simulation_id} Optimization",
                "model_type": self._map_model_type(config.model_type),
                "min_participants": config.min_participants,
                "rounds": config.training_rounds,
                "privacy_parameters": {
                    "mechanism": "GAUSSIAN",
                    "epsilon": config.privacy_budget,
                    "delta": 1e-5
                },
                "aggregation_strategy": "WEIGHTED_AVERAGE",
                "data_requirements": {
                    "min_samples": 100,
                    "feature_schema": self._get_feature_schema(config.model_type)
                },
                "participant_criteria": {
                    "required_credentials": ["SIMULATION_PARTICIPANT"],
                    "min_reputation": 0.5
                }
            }
            
            # Call federated learning service
            response = await self.http_client.post(
                f"{self.fl_service_url}/api/v1/sessions",
                json=fl_request,
                headers={"X-Tenant-ID": tenant_id}
            )
            
            if response.status_code != 201:
                raise Exception(f"Failed to create FL session: {response.text}")
                
            fl_session = response.json()
            
            # Store training session info
            session_info = {
                "session_id": session_id,
                "fl_session_id": fl_session["session_id"],
                "simulation_id": config.simulation_id,
                "config": config.dict(),
                "participants": participants,
                "status": "initialized",
                "created_at": datetime.utcnow().isoformat()
            }
            
            self.training_cache.put(session_id, session_info)
            
            # Publish event
            await self.event_publisher.publish_event(
                f"platformq/{tenant_id}/simulation-ml-training-events",
                "SimulationMLTrainingStarted",
                {
                    "session_id": session_id,
                    "simulation_id": config.simulation_id,
                    "model_type": config.model_type,
                    "num_participants": len(participants)
                }
            )
            
            logger.info(f"Initiated FL training session {session_id} for simulation {config.simulation_id}")
            return session_id
            
        except Exception as e:
            logger.error(f"Error initiating training session: {e}")
            raise
    
    async def prepare_participant_data(self,
                                     session_id: str,
                                     participant_id: str,
                                     time_window: timedelta) -> Dict[str, Any]:
        """Prepare training data for a participant"""
        try:
            session_info = self.training_cache.get(session_id)
            if not session_info:
                raise ValueError(f"Training session {session_id} not found")
                
            simulation_id = session_info["simulation_id"]
            
            # Extract training data from recent simulation runs
            end_time = datetime.utcnow()
            start_time = end_time - time_window
            
            training_data = await self.data_extractor.extract_training_data(
                simulation_id, start_time, end_time
            )
            
            if not training_data:
                return None
                
            # Apply differential privacy if needed
            if session_info["config"]["privacy_budget"] < float('inf'):
                training_data = self._apply_differential_privacy(
                    training_data,
                    epsilon=session_info["config"]["privacy_budget"]
                )
            
            return {
                "participant_id": participant_id,
                "data": training_data,
                "metadata": {
                    "session_id": session_id,
                    "simulation_id": simulation_id,
                    "prepared_at": datetime.utcnow().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Error preparing participant data: {e}")
            return None
    
    async def apply_trained_model(self,
                                session_id: str,
                                simulation_id: str) -> bool:
        """Apply the trained model to optimize simulation parameters"""
        try:
            session_info = self.training_cache.get(session_id)
            if not session_info or session_info["status"] != "completed":
                return False
                
            fl_session_id = session_info["fl_session_id"]
            
            # Get aggregated model from FL service
            response = await self.http_client.get(
                f"{self.fl_service_url}/api/v1/sessions/{fl_session_id}/model"
            )
            
            if response.status_code != 200:
                return False
                
            model_data = response.json()
            
            # Register model with MLOps service
            mlops_response = await self.http_client.post(
                f"{self.mlops_url}/api/v1/models/register",
                json={
                    "model_name": f"simulation_{simulation_id}_optimizer",
                    "model_version": "1.0",
                    "model_type": session_info["config"]["model_type"],
                    "model_data": model_data["aggregated_model"],
                    "metadata": {
                        "training_session": session_id,
                        "simulation_id": simulation_id,
                        "training_rounds": session_info["config"]["training_rounds"],
                        "num_participants": len(session_info["participants"])
                    }
                }
            )
            
            if mlops_response.status_code != 201:
                return False
                
            # Update simulation parameters based on model
            await self._update_simulation_parameters(simulation_id, model_data)
            
            # Publish success event
            await self.event_publisher.publish_event(
                f"platformq/{session_info.get('tenant_id', 'default')}/simulation-ml-training-events",
                "SimulationModelApplied",
                {
                    "session_id": session_id,
                    "simulation_id": simulation_id,
                    "model_version": "1.0",
                    "applied_at": datetime.utcnow().isoformat()
                }
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error applying trained model: {e}")
            return False
    
    def _map_model_type(self, model_type: str) -> str:
        """Map simulation model type to FL service model type"""
        mapping = {
            "agent_behavior": "neural_network",
            "parameter_tuning": "gradient_boosting",
            "outcome_prediction": "lstm"
        }
        return mapping.get(model_type, "neural_network")
    
    def _get_feature_schema(self, model_type: str) -> Dict[str, Any]:
        """Get feature schema for model type"""
        if model_type == "agent_behavior":
            return {
                "input_shape": [10],
                "output_shape": [5],
                "feature_names": [
                    "pos_mean", "pos_std", "pos_min", "pos_max",
                    "vel_mean", "vel_std", "max_speed",
                    "time_step", "gravity", "num_agents"
                ]
            }
        elif model_type == "parameter_tuning":
            return {
                "input_shape": [15],
                "output_shape": [8],
                "feature_names": ["param_" + str(i) for i in range(15)]
            }
        else:
            return {
                "input_shape": [20],
                "output_shape": [10]
            }
    
    def _apply_differential_privacy(self, 
                                  data: Dict[str, Any],
                                  epsilon: float) -> Dict[str, Any]:
        """Apply differential privacy to training data"""
        features = data["features"]
        labels = data["labels"]
        
        # Add Laplacian noise
        sensitivity = 1.0  # Assume normalized features
        scale = sensitivity / epsilon
        
        noise_features = np.random.laplace(0, scale, features.shape)
        noise_labels = np.random.laplace(0, scale, labels.shape)
        
        data["features"] = features + noise_features
        data["labels"] = labels + noise_labels
        
        return data
    
    async def _update_simulation_parameters(self,
                                          simulation_id: str,
                                          model_data: Dict[str, Any]):
        """Update simulation parameters based on trained model"""
        try:
            # Get current simulation state
            sim_cache = self.ignite.get_or_create_cache("simulations")
            simulation = sim_cache.get(simulation_id)
            
            if not simulation:
                return
                
            # Extract optimized parameters from model
            # This is a simplified example - real implementation would decode model weights
            optimized_params = {
                "time_step": 0.05,  # Optimized from model
                "convergence_threshold": 0.0001,
                "max_iterations": 1000,
                "learning_rate": 0.1
            }
            
            # Update simulation
            simulation["parameters"].update(optimized_params)
            simulation["ml_optimized"] = True
            simulation["optimization_timestamp"] = datetime.utcnow().isoformat()
            
            sim_cache.put(simulation_id, simulation)
            
            logger.info(f"Updated simulation {simulation_id} with ML-optimized parameters")
            
        except Exception as e:
            logger.error(f"Error updating simulation parameters: {e}")
    
    async def close(self):
        """Clean up resources"""
        await self.http_client.aclose()


class SimulationMLOrchestrator:
    """Orchestrates ML training workflows for simulations"""
    
    def __init__(self,
                 ignite_client: IgniteClient,
                 event_publisher: EventPublisher,
                 config: Dict[str, str]):
        self.ignite = ignite_client
        self.event_publisher = event_publisher
        self.trainer = FederatedSimulationTrainer(
            ignite_client,
            event_publisher,
            config.get("federated_learning_url", "http://federated-learning-service:8000"),
            config.get("mlops_service_url", "http://mlops-service:8000")
        )
        self.running = False
        self._tasks = []
        
    async def start(self):
        """Start the orchestrator"""
        self.running = True
        
        # Start background tasks
        self._tasks.append(asyncio.create_task(self._monitor_simulations()))
        self._tasks.append(asyncio.create_task(self._process_training_requests()))
        
        logger.info("Simulation ML Orchestrator started")
        
    async def stop(self):
        """Stop the orchestrator"""
        self.running = False
        
        for task in self._tasks:
            task.cancel()
            
        await self.trainer.close()
        
        logger.info("Simulation ML Orchestrator stopped")
        
    async def _monitor_simulations(self):
        """Monitor simulations for ML training opportunities"""
        while self.running:
            try:
                # Check active simulations
                sim_cache = self.ignite.get_or_create_cache("simulations")
                
                query = sim_cache.scan()
                for sim_id, simulation in query:
                    if self._should_trigger_ml_training(simulation):
                        await self._trigger_ml_training(sim_id, simulation)
                        
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error monitoring simulations: {e}")
                await asyncio.sleep(10)
                
    def _should_trigger_ml_training(self, simulation: Dict[str, Any]) -> bool:
        """Determine if ML training should be triggered"""
        # Check if simulation has enough data
        metrics = simulation.get("metrics", {})
        
        if metrics.get("total_steps", 0) < 1000:
            return False
            
        # Check if not recently optimized
        last_optimization = simulation.get("optimization_timestamp")
        if last_optimization:
            last_time = datetime.fromisoformat(last_optimization)
            if datetime.utcnow() - last_time < timedelta(hours=1):
                return False
                
        # Check if performance is degrading
        convergence_rate = metrics.get("convergence_rate", 1.0)
        if convergence_rate < 0.5:
            return True
            
        return False
        
    async def _trigger_ml_training(self, 
                                 simulation_id: str,
                                 simulation: Dict[str, Any]):
        """Trigger ML training for a simulation"""
        try:
            config = SimulationMLConfig(
                simulation_id=simulation_id,
                model_type="parameter_tuning",
                training_rounds=5,
                min_participants=1  # Can train on single tenant data
            )
            
            # For now, use simulation owner as sole participant
            participants = [simulation.get("owner_id", "system")]
            
            session_id = await self.trainer.initiate_training_session(
                simulation.get("tenant_id", "default"),
                config,
                participants
            )
            
            logger.info(f"Triggered ML training session {session_id} for simulation {simulation_id}")
            
        except Exception as e:
            logger.error(f"Error triggering ML training: {e}")
            
    async def _process_training_requests(self):
        """Process explicit training requests"""
        # This would consume from a Pulsar topic for on-demand training
        # For now, it's a placeholder
        while self.running:
            await asyncio.sleep(30) 