"""
Federated Learning Coordinator for distributed model training
"""
from typing import Dict, List, Any, Optional, Set
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import numpy as np
from platformq_shared.utils.logger import get_logger
from platformq_shared.cache.ignite_client import IgniteClient
from platformq_shared.events import EventPublisher
from platformq_shared.errors import ValidationError

logger = get_logger(__name__)


class FederatedStrategy(Enum):
    FEDAVG = "fedavg"  # Federated Averaging
    FEDPROX = "fedprox"  # Federated Proximal
    SCAFFOLD = "scaffold"  # SCAFFOLD algorithm
    FEDOPT = "fedopt"  # Federated Optimization


class PrivacyMechanism(Enum):
    NONE = "none"
    DIFFERENTIAL_PRIVACY = "differential_privacy"
    SECURE_AGGREGATION = "secure_aggregation"
    HOMOMORPHIC_ENCRYPTION = "homomorphic_encryption"


class FederatedLearningCoordinator:
    """Coordinator for federated learning sessions"""
    
    def __init__(self):
        self.cache = IgniteClient()
        self.event_publisher = EventPublisher()
        self.sessions: Dict[str, Dict] = {}
        self.participants: Dict[str, Set[str]] = {}  # session_id -> set of participant_ids
        
    async def create_session(self, session_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new federated learning session"""
        try:
            # Validate session configuration
            required_fields = ["name", "model_config", "training_config", "min_participants"]
            for field in required_fields:
                if field not in session_data:
                    raise ValidationError(f"Missing required field: {field}")
            
            # Generate session ID
            session_id = f"fl_session_{session_data['name']}_{datetime.utcnow().timestamp()}"
            
            # Create session
            session = {
                "session_id": session_id,
                "created_at": datetime.utcnow().isoformat(),
                "status": "waiting_for_participants",
                "current_round": 0,
                "global_model": None,
                "participants": [],
                "strategy": session_data.get("strategy", FederatedStrategy.FEDAVG.value),
                "privacy": session_data.get("privacy", PrivacyMechanism.NONE.value),
                **session_data
            }
            
            # Initialize global model
            session["global_model"] = await self._initialize_model(session["model_config"])
            
            # Store session
            self.sessions[session_id] = session
            self.participants[session_id] = set()
            await self.cache.put(f"fl:session:{session_id}", session)
            
            # Publish event
            await self.event_publisher.publish("federated.session.created", {
                "session_id": session_id,
                "min_participants": session_data["min_participants"]
            })
            
            logger.info(f"Created federated learning session: {session_id}")
            return session
            
        except Exception as e:
            logger.error(f"Failed to create session: {str(e)}")
            raise
    
    async def join_session(self, 
                         session_id: str,
                         participant_data: Dict[str, Any]) -> Dict[str, Any]:
        """Join a federated learning session"""
        try:
            # Get session
            session = await self._get_session(session_id)
            
            if session["status"] not in ["waiting_for_participants", "training"]:
                raise ValidationError(f"Session {session_id} is not accepting participants")
            
            # Validate participant
            required_fields = ["participant_id", "data_size", "compute_capacity"]
            for field in required_fields:
                if field not in participant_data:
                    raise ValidationError(f"Missing required field: {field}")
            
            # Add participant
            participant_id = participant_data["participant_id"]
            self.participants[session_id].add(participant_id)
            
            participant = {
                "joined_at": datetime.utcnow().isoformat(),
                "status": "ready",
                "rounds_completed": 0,
                "contribution_score": 0.0,
                **participant_data
            }
            
            session["participants"].append(participant)
            
            # Check if we can start training
            if (len(session["participants"]) >= session["min_participants"] and 
                session["status"] == "waiting_for_participants"):
                session["status"] = "training"
                asyncio.create_task(self._start_training(session_id))
            
            # Update session
            await self._update_session(session_id, session)
            
            # Send initial model to participant
            return {
                "session_id": session_id,
                "participant_id": participant_id,
                "status": "joined",
                "global_model": session["global_model"],
                "current_round": session["current_round"]
            }
            
        except Exception as e:
            logger.error(f"Failed to join session: {str(e)}")
            raise
    
    async def submit_update(self,
                          session_id: str,
                          participant_id: str,
                          update_data: Dict[str, Any]) -> Dict[str, Any]:
        """Submit model update from participant"""
        try:
            session = await self._get_session(session_id)
            
            if participant_id not in self.participants[session_id]:
                raise ValidationError(f"Participant {participant_id} not in session")
            
            # Validate update
            required_fields = ["model_update", "metrics", "data_samples"]
            for field in required_fields:
                if field not in update_data:
                    raise ValidationError(f"Missing required field: {field}")
            
            # Apply privacy mechanism if configured
            if session["privacy"] != PrivacyMechanism.NONE.value:
                update_data = await self._apply_privacy(update_data, session["privacy"])
            
            # Store update
            update_key = f"fl:update:{session_id}:{session['current_round']}:{participant_id}"
            await self.cache.put(update_key, update_data, ttl=3600)
            
            # Update participant info
            for participant in session["participants"]:
                if participant["participant_id"] == participant_id:
                    participant["rounds_completed"] += 1
                    participant["last_update"] = datetime.utcnow().isoformat()
                    break
            
            await self._update_session(session_id, session)
            
            # Check if round is complete
            await self._check_round_completion(session_id)
            
            return {
                "session_id": session_id,
                "participant_id": participant_id,
                "round": session["current_round"],
                "status": "update_received"
            }
            
        except Exception as e:
            logger.error(f"Failed to submit update: {str(e)}")
            raise
    
    async def get_global_model(self, session_id: str) -> Dict[str, Any]:
        """Get current global model"""
        session = await self._get_session(session_id)
        
        return {
            "session_id": session_id,
            "global_model": session["global_model"],
            "current_round": session["current_round"],
            "participants": len(session["participants"]),
            "status": session["status"]
        }
    
    async def apply_privacy_technique(self,
                                    session_id: str,
                                    technique: str,
                                    params: Dict[str, Any]) -> Dict[str, Any]:
        """Apply privacy-preserving technique to session"""
        try:
            session = await self._get_session(session_id)
            
            if technique not in [p.value for p in PrivacyMechanism]:
                raise ValidationError(f"Unknown privacy technique: {technique}")
            
            # Update session privacy settings
            session["privacy"] = technique
            session["privacy_params"] = params
            
            await self._update_session(session_id, session)
            
            # Publish event
            await self.event_publisher.publish("federated.privacy.updated", {
                "session_id": session_id,
                "technique": technique
            })
            
            return {
                "session_id": session_id,
                "privacy_technique": technique,
                "params": params,
                "status": "applied"
            }
            
        except Exception as e:
            logger.error(f"Failed to apply privacy technique: {str(e)}")
            raise
    
    async def _start_training(self, session_id: str):
        """Start federated training rounds"""
        try:
            session = await self._get_session(session_id)
            training_config = session["training_config"]
            
            for round_num in range(training_config["num_rounds"]):
                session["current_round"] = round_num + 1
                await self._update_session(session_id, session)
                
                # Notify participants to start training
                await self.event_publisher.publish("federated.round.started", {
                    "session_id": session_id,
                    "round": round_num + 1,
                    "participants": list(self.participants[session_id])
                })
                
                # Wait for updates with timeout
                timeout = training_config.get("round_timeout", 300)  # 5 minutes default
                await asyncio.sleep(timeout)
                
                # Aggregate updates
                aggregated_model = await self._aggregate_updates(session_id)
                
                if aggregated_model:
                    session["global_model"] = aggregated_model
                    
                    # Evaluate global model
                    metrics = await self._evaluate_global_model(session)
                    
                    # Check stopping criteria
                    if self._should_stop_training(metrics, training_config):
                        break
                
                await self._update_session(session_id, session)
            
            # Complete session
            session["status"] = "completed"
            session["completed_at"] = datetime.utcnow().isoformat()
            await self._update_session(session_id, session)
            
            # Publish completion event
            await self.event_publisher.publish("federated.session.completed", {
                "session_id": session_id,
                "rounds_completed": session["current_round"],
                "final_metrics": session.get("metrics", {})
            })
            
        except Exception as e:
            logger.error(f"Training failed for session {session_id}: {str(e)}")
            session["status"] = "failed"
            session["error"] = str(e)
            await self._update_session(session_id, session)
    
    async def _aggregate_updates(self, session_id: str) -> Optional[Dict]:
        """Aggregate model updates from participants"""
        session = await self._get_session(session_id)
        current_round = session["current_round"]
        strategy = session["strategy"]
        
        # Collect updates
        updates = []
        weights = []
        
        for participant_id in self.participants[session_id]:
            update_key = f"fl:update:{session_id}:{current_round}:{participant_id}"
            update = await self.cache.get(update_key)
            
            if update:
                updates.append(update["model_update"])
                weights.append(update["data_samples"])
        
        if not updates:
            return None
        
        # Apply aggregation strategy
        if strategy == FederatedStrategy.FEDAVG.value:
            return self._federated_averaging(updates, weights)
        elif strategy == FederatedStrategy.FEDPROX.value:
            return self._federated_proximal(updates, weights, session["global_model"])
        elif strategy == FederatedStrategy.SCAFFOLD.value:
            return self._scaffold_aggregation(updates, weights, session)
        else:
            return self._federated_averaging(updates, weights)
    
    def _federated_averaging(self, updates: List[Dict], weights: List[int]) -> Dict:
        """Federated averaging aggregation"""
        total_samples = sum(weights)
        aggregated = {}
        
        # Average each parameter weighted by data samples
        for key in updates[0].keys():
            weighted_sum = sum(
                update[key] * weight / total_samples 
                for update, weight in zip(updates, weights)
            )
            aggregated[key] = weighted_sum
        
        return aggregated
    
    def _federated_proximal(self, updates: List[Dict], weights: List[int], 
                          global_model: Dict) -> Dict:
        """FedProx aggregation with proximal term"""
        # Similar to FedAvg but with proximal regularization
        aggregated = self._federated_averaging(updates, weights)
        
        # Add proximal term
        mu = 0.01  # Proximal parameter
        for key in aggregated.keys():
            if global_model and key in global_model:
                aggregated[key] = (1 - mu) * aggregated[key] + mu * global_model[key]
        
        return aggregated
    
    def _scaffold_aggregation(self, updates: List[Dict], weights: List[int], 
                            session: Dict) -> Dict:
        """SCAFFOLD aggregation with control variates"""
        # Simplified SCAFFOLD implementation
        return self._federated_averaging(updates, weights)
    
    async def _apply_privacy(self, update_data: Dict, privacy_mechanism: str) -> Dict:
        """Apply privacy-preserving mechanism to update"""
        if privacy_mechanism == PrivacyMechanism.DIFFERENTIAL_PRIVACY.value:
            # Add Gaussian noise for differential privacy
            epsilon = 1.0  # Privacy budget
            sensitivity = 1.0  # L2 sensitivity
            
            for key in update_data["model_update"]:
                noise = np.random.normal(0, sensitivity / epsilon, 
                                       update_data["model_update"][key].shape)
                update_data["model_update"][key] += noise
        
        elif privacy_mechanism == PrivacyMechanism.SECURE_AGGREGATION.value:
            # In production, implement secure multi-party computation
            pass
        
        elif privacy_mechanism == PrivacyMechanism.HOMOMORPHIC_ENCRYPTION.value:
            # In production, implement homomorphic encryption
            pass
        
        return update_data
    
    async def _check_round_completion(self, session_id: str):
        """Check if current round is complete"""
        session = await self._get_session(session_id)
        current_round = session["current_round"]
        
        # Count updates received
        updates_received = 0
        for participant_id in self.participants[session_id]:
            update_key = f"fl:update:{session_id}:{current_round}:{participant_id}"
            if await self.cache.get(update_key):
                updates_received += 1
        
        # Check if enough updates received
        min_updates = int(len(self.participants[session_id]) * 0.8)  # 80% threshold
        if updates_received >= min_updates:
            # Trigger aggregation
            await self.event_publisher.publish("federated.round.ready", {
                "session_id": session_id,
                "round": current_round,
                "updates_received": updates_received
            })
    
    async def _initialize_model(self, model_config: Dict) -> Dict:
        """Initialize global model"""
        # In production, initialize actual model architecture
        return {
            "architecture": model_config.get("architecture"),
            "parameters": {},
            "version": "1.0"
        }
    
    async def _evaluate_global_model(self, session: Dict) -> Dict:
        """Evaluate global model performance"""
        # In production, evaluate on validation set
        return {
            "accuracy": 0.85 + session["current_round"] * 0.01,  # Mock improvement
            "loss": 0.5 - session["current_round"] * 0.05
        }
    
    def _should_stop_training(self, metrics: Dict, training_config: Dict) -> bool:
        """Check if training should stop"""
        # Check convergence criteria
        if "target_accuracy" in training_config:
            if metrics.get("accuracy", 0) >= training_config["target_accuracy"]:
                return True
        
        if "max_loss" in training_config:
            if metrics.get("loss", float('inf')) <= training_config["max_loss"]:
                return True
        
        return False
    
    async def _get_session(self, session_id: str) -> Dict[str, Any]:
        """Get session by ID"""
        session = self.sessions.get(session_id)
        if not session:
            cached = await self.cache.get(f"fl:session:{session_id}")
            if not cached:
                raise ValidationError(f"Session {session_id} not found")
            session = cached
            self.sessions[session_id] = session
        
        return session
    
    async def _update_session(self, session_id: str, session: Dict):
        """Update session"""
        session["updated_at"] = datetime.utcnow().isoformat()
        self.sessions[session_id] = session
        await self.cache.put(f"fl:session:{session_id}", session) 