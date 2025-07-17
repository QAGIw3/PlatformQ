import uuid
from datetime import datetime
import json
import logging
from typing import Dict, Any

from pyignite import Client as IgniteClient
from platformq.shared.event_publisher import EventPublisher

from ..core.secure_aggregation import SecureAggregationProtocol, AggregationStrategy

logger = logging.getLogger(__name__)

class SessionService:
    def __init__(self, ignite_client: IgniteClient, event_publisher: EventPublisher):
        self.ignite_client = ignite_client
        self.event_publisher = event_publisher
        self.secure_aggregation_protocols = {}
        self.sessions = {}

    def create_session(self, request: Dict[str, Any], tenant_id: str, user_id: str) -> Dict[str, Any]:
        """Create a new federated learning session"""
        session_id = f"fl_session_{uuid.uuid4()}"
        
        session_data = {
            "session_id": session_id,
            "tenant_id": tenant_id,
            "created_by": user_id,
            "model_type": request.model_type,
            "algorithm": request.algorithm,
            "dataset_requirements": request.dataset_requirements,
            "privacy_parameters": request.privacy_parameters,
            "training_parameters": request.training_parameters,
            "participation_criteria": request.participation_criteria,
            "data_quality_requirements": request.data_quality_requirements.dict(),
            "status": "WAITING_FOR_PARTICIPANTS",
            "current_round": 0,
            "total_rounds": request.training_parameters.get("rounds", 10),
            "participants": [],
            "created_at": datetime.utcnow().isoformat(),
            "coordinator_endpoint": f"https://federated-learning.platformq.io/api/v1/sessions/{session_id}"
        }
        
        self.create_session_in_ignite(session_data)
        
        if request.privacy_parameters.get("homomorphic_encryption", False):
            strategy = AggregationStrategy(request.privacy_parameters.get("aggregation_strategy", "SECURE_AGG"))
            
            protocol = SecureAggregationProtocol(
                strategy=strategy,
                encryption_scheme=request.privacy_parameters.get("encryption_scheme", "CKKS"),
                differential_privacy=request.privacy_parameters.get("differential_privacy_enabled", True),
                epsilon=request.privacy_parameters.get("epsilon", 1.0),
                delta=request.privacy_parameters.get("delta", 1e-5),
                byzantine_tolerance=request.privacy_parameters.get("byzantine_tolerance", 0.2)
            )
            
            model_architecture = {
                "layer1_weights": [100, 50],
                "layer1_bias": [50],
                "layer2_weights": [50, 10],
                "layer2_bias": [10]
            }
            
            he_context, he_params = protocol.initialize_session(
                session_id,
                request.training_parameters.get("min_participants", 2),
                model_architecture
            )
            
            self.secure_aggregation_protocols[session_id] = protocol
            self.sessions[session_id] = {
                "context": he_context,
                "he_params": he_params,
                **session_data
            }
            
            session_data["homomorphic_encryption"] = {
                "enabled": True,
                "scheme": request.privacy_parameters.get("encryption_scheme", "CKKS"),
                "public_key": he_params["public_key"]
            }
        
        event_data = {
            "session_id": session_id,
            "tenant_id": tenant_id,
            "model_type": request.model_type,
            "algorithm": request.algorithm,
            "dataset_requirements": request.dataset_requirements,
            "privacy_parameters": request.privacy_parameters,
            "training_parameters": request.training_parameters,
            "participation_criteria": request.participation_criteria,
            "data_quality_requirements": request.data_quality_requirements.dict(),
            "coordinator_endpoint": session_data["coordinator_endpoint"],
            "created_by": user_id,
            "timestamp": int(datetime.utcnow().timestamp() * 1000)
        }
        
        self.event_publisher.publish(
            topic=f"persistent://platformq/public/federated-learning-initiated",
            schema_path="federated_learning_initiated.avsc",
            data=event_data
        )
        
        return session_data

    def create_session_in_ignite(self, session_data: Dict[str, Any]):
        """Store session in Ignite cache"""
        cache = self.ignite_client.get_or_create_cache("fl_sessions")
        cache.put(session_data["session_id"], json.dumps(session_data))

    def update_session_in_ignite(self, session_id: str, updates: Dict[str, Any]):
        """Update session in Ignite"""
        cache = self.ignite_client.get_cache("fl_sessions")
        session_data = json.loads(cache.get(session_id))
        session_data.update(updates)
        cache.put(session_id, json.dumps(session_data))

    def get_session(self, session_id: str) -> Dict[str, Any]:
        cache = self.ignite_client.get_cache("fl_sessions")
        session_json = cache.get(session_id)
        if not session_json:
            return None
        return json.loads(session_json) 