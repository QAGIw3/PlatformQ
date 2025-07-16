"""
Federated Learning Coordinator Service

This service orchestrates privacy-preserving federated learning sessions
across multiple tenants with verifiable credential-based access control.
"""

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from fastapi import Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
import pulsar
from pulsar.schema import AvroSchema
from pyignite import Client as IgniteClient
import httpx

from platformq_shared.base_service import create_base_app
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.config import ConfigLoader
from platformq_shared.jwt import get_current_tenant_and_user

logger = logging.getLogger(__name__)

# Initialize base app
app = create_base_app(service_name="federated-learning-service")

# Configuration
config_loader = ConfigLoader()
settings = config_loader.load_settings()

PULSAR_URL = settings.get("PULSAR_URL", "pulsar://pulsar:6650")
VC_SERVICE_URL = settings.get("VC_SERVICE_URL", "http://verifiable-credential-service:80")
AUTH_SERVICE_URL = settings.get("AUTH_SERVICE_URL", "http://auth-service:80")
SPARK_MASTER_URL = settings.get("SPARK_MASTER_URL", "spark://spark-master:7077")
IGNITE_NODES = settings.get("IGNITE_NODES", "ignite-0.ignite:10800,ignite-1.ignite:10800").split(",")

# Pydantic models
class FederatedLearningSessionRequest(BaseModel):
    """Request to create a new federated learning session"""
    model_type: str = Field(..., description="Type of model: CLASSIFICATION, REGRESSION, etc.")
    algorithm: str = Field(..., description="Algorithm: LogisticRegression, RandomForest, etc.")
    dataset_requirements: Dict[str, Any] = Field(..., description="Dataset schema and requirements")
    privacy_parameters: Dict[str, Any] = Field(default_factory=lambda: {
        "differential_privacy_enabled": True,
        "epsilon": 1.0,
        "delta": 1e-5,
        "secure_aggregation": True,
        "homomorphic_encryption": False
    })
    training_parameters: Dict[str, Any] = Field(..., description="Training configuration")
    participation_criteria: Dict[str, Any] = Field(default_factory=lambda: {
        "required_credentials": [],
        "min_reputation_score": None,
        "allowed_tenants": None
    })

class ParticipantJoinRequest(BaseModel):
    """Request to join a federated learning session"""
    session_id: str
    dataset_stats: Dict[str, Any]
    compute_capabilities: Dict[str, Any]
    public_key: str = Field(..., description="Public key for secure aggregation")

class ModelUpdateSubmission(BaseModel):
    """Model update submission from a participant"""
    session_id: str
    round_number: int
    update_uri: str = Field(..., description="URI to encrypted model update in MinIO")
    metrics: Dict[str, float]
    zkp: Dict[str, str] = Field(..., description="Zero-knowledge proof of correct training")

class SessionStatus(BaseModel):
    """Current status of a federated learning session"""
    session_id: str
    status: str
    current_round: int
    total_rounds: int
    participants: List[Dict[str, Any]]
    convergence_metrics: Optional[Dict[str, float]]
    last_updated: datetime


# Global state managers
class FederatedLearningCoordinator:
    """Manages federated learning sessions"""
    
    def __init__(self):
        self.event_publisher = None
        self.ignite_client = None
        self.http_client = httpx.AsyncClient()
        self.active_sessions = {}
        
    async def initialize(self):
        """Initialize connections"""
        # Initialize Pulsar
        self.event_publisher = EventPublisher(pulsar_url=PULSAR_URL)
        self.event_publisher.connect()
        
        # Initialize Ignite
        self.ignite_client = IgniteClient()
        self.ignite_client.connect([(node.split(":")[0], int(node.split(":")[1])) 
                                   for node in IGNITE_NODES])
        
        logger.info("Federated Learning Coordinator initialized")
    
    async def close(self):
        """Close connections"""
        if self.event_publisher:
            self.event_publisher.close()
        if self.ignite_client:
            self.ignite_client.close()
        await self.http_client.aclose()
    
    async def verify_participant_credentials(self, 
                                           participant_id: str,
                                           tenant_id: str,
                                           required_credentials: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Verify participant's credentials via VC service"""
        verified_credentials = []
        
        for req_cred in required_credentials:
            try:
                # Query VC service for credentials
                response = await self.http_client.get(
                    f"{VC_SERVICE_URL}/api/v1/dids/{participant_id}/credentials",
                    params={"credential_type": req_cred["credential_type"]},
                    headers={"X-Tenant-ID": tenant_id}
                )
                
                if response.status_code == 200:
                    creds = response.json()
                    
                    # Verify each credential
                    for cred in creds:
                        verify_response = await self.http_client.post(
                            f"{VC_SERVICE_URL}/api/v1/verify",
                            json={"credential": cred},
                            headers={"X-Tenant-ID": tenant_id}
                        )
                        
                        if verify_response.status_code == 200:
                            result = verify_response.json()
                            if result.get("verified", False):
                                # Check additional requirements
                                if req_cred.get("issuer") and cred.get("issuer") != req_cred["issuer"]:
                                    continue
                                
                                # Check trust score if required
                                if req_cred.get("min_trust_score"):
                                    trust_score = cred.get("credentialSubject", {}).get("trustScore", 0)
                                    if trust_score < req_cred["min_trust_score"]:
                                        continue
                                
                                verified_credentials.append({
                                    "credential_id": cred.get("id"),
                                    "credential_type": req_cred["credential_type"],
                                    "issuer": cred.get("issuer"),
                                    "verification_timestamp": int(time.time() * 1000)
                                })
                                break
                
            except Exception as e:
                logger.error(f"Failed to verify credential {req_cred}: {e}")
        
        return verified_credentials
    
    async def get_user_reputation(self, user_id: str, tenant_id: str) -> Optional[int]:
        """Get user's reputation score from auth service"""
        try:
            response = await self.http_client.get(
                f"{AUTH_SERVICE_URL}/api/v1/users/{user_id}/reputation",
                headers={"X-Tenant-ID": tenant_id}
            )
            
            if response.status_code == 200:
                return response.json().get("reputation_score", 0)
        except Exception as e:
            logger.error(f"Failed to get reputation score: {e}")
        
        return None
    
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
    
    def add_participant_to_ignite(self, session_id: str, participant_data: Dict[str, Any]):
        """Add participant to Ignite cache"""
        cache = self.ignite_client.get_or_create_cache("fl_participants")
        key = f"{session_id}:{participant_data['participant_id']}"
        cache.put(key, json.dumps(participant_data))
    
    async def submit_spark_job(self, job_type: str, config: Dict[str, Any]) -> str:
        """Submit Spark job for federated learning or aggregation"""
        spark_submit_cmd = [
            "spark-submit",
            "--master", SPARK_MASTER_URL,
            "--deploy-mode", "cluster",
            "--conf", "spark.executor.memory=4g",
            "--conf", "spark.executor.cores=4",
            "--conf", "spark.sql.adaptive.enabled=true",
            "--packages", "org.apache.ignite:ignite-spark:2.14.0",
            "--py-files", "/app/processing/spark/ml/federated_learning.py"
        ]
        
        if job_type == "training":
            spark_submit_cmd.extend([
                "/app/processing/spark/ml/federated_learning.py",
                config["session_id"],
                config["tenant_id"],
                config["participant_id"],
                json.dumps(config["training_config"])
            ])
        elif job_type == "aggregation":
            spark_submit_cmd.extend([
                "/app/processing/spark/ml/federated_aggregator.py",
                config["session_id"],
                str(config["round_number"]),
                json.dumps(config["aggregation_config"])
            ])
        
        # In production, use proper Spark job submission API
        # For now, return a mock job ID
        job_id = f"{job_type}_{config['session_id']}_{uuid.uuid4()}"
        logger.info(f"Submitted Spark job {job_id}: {' '.join(spark_submit_cmd)}")
        
        return job_id
    
    async def start_training_round(self, session_id: str, round_number: int):
        """Start a new training round"""
        # Get session data from Ignite
        cache = self.ignite_client.get_cache("fl_sessions")
        session_data = json.loads(cache.get(session_id))
        
        # Get participants
        participants_cache = self.ignite_client.get_cache("fl_participants")
        participant_keys = [k for k in participants_cache.keys() if k.startswith(f"{session_id}:")]
        
        # Publish event to start training
        event_data = {
            "session_id": session_id,
            "round_number": round_number,
            "training_config": {
                "model_type": session_data["model_type"],
                "algorithm": session_data["algorithm"],
                "dataset_requirements": session_data["dataset_requirements"],
                "privacy_parameters": session_data["privacy_parameters"],
                "training_parameters": session_data["training_parameters"]
            },
            "timestamp": int(time.time() * 1000)
        }
        
        # Publish to each participant's topic
        for key in participant_keys:
            participant_data = json.loads(participants_cache.get(key))
            tenant_id = participant_data["tenant_id"]
            
            self.event_publisher.publish(
                topic=f"persistent://platformq/{tenant_id}/federated-learning-training",
                schema_path="federated_training_round_started.avsc",
                data=event_data
            )
        
        # Update session status
        self.update_session_in_ignite(session_id, {
            "status": "TRAINING",
            "current_round": round_number,
            "round_start_time": datetime.utcnow().isoformat()
        })
    
    async def aggregate_model_updates(self, session_id: str, round_number: int, update_uris: List[str]):
        """Trigger model aggregation"""
        config = {
            "session_id": session_id,
            "round_number": round_number,
            "aggregation_config": {
                "update_uris": update_uris,
                "aggregation_strategy": "FedAvg"
            }
        }
        
        job_id = await self.submit_spark_job("aggregation", config)
        
        # Update session status
        self.update_session_in_ignite(session_id, {
            "status": "AGGREGATING",
            "aggregation_job_id": job_id
        })
        
        return job_id


# Initialize coordinator
coordinator = FederatedLearningCoordinator()

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    await coordinator.initialize()

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    await coordinator.close()


# API Endpoints
@app.post("/api/v1/sessions", response_model=Dict[str, Any])
async def create_federated_learning_session(
    request: FederatedLearningSessionRequest,
    background_tasks: BackgroundTasks,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Create a new federated learning session"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    
    # Generate session ID
    session_id = f"fl_session_{uuid.uuid4()}"
    
    # Create session data
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
        "status": "WAITING_FOR_PARTICIPANTS",
        "current_round": 0,
        "total_rounds": request.training_parameters.get("rounds", 10),
        "participants": [],
        "created_at": datetime.utcnow().isoformat(),
        "coordinator_endpoint": f"https://federated-learning.platformq.io/api/v1/sessions/{session_id}"
    }
    
    # Store in Ignite
    coordinator.create_session_in_ignite(session_data)
    
    # Publish session creation event
    event_data = {
        "session_id": session_id,
        "tenant_id": tenant_id,
        "model_type": request.model_type,
        "algorithm": request.algorithm,
        "dataset_requirements": request.dataset_requirements,
        "privacy_parameters": request.privacy_parameters,
        "training_parameters": request.training_parameters,
        "participation_criteria": request.participation_criteria,
        "coordinator_endpoint": session_data["coordinator_endpoint"],
        "created_by": user_id,
        "timestamp": int(time.time() * 1000)
    }
    
    coordinator.event_publisher.publish(
        topic=f"persistent://platformq/public/federated-learning-initiated",
        schema_path="federated_learning_initiated.avsc",
        data=event_data
    )
    
    # Schedule session start
    min_participants = request.training_parameters.get("min_participants", 2)
    background_tasks.add_task(
        monitor_and_start_session,
        session_id,
        min_participants,
        request.training_parameters.get("start_timeout_seconds", 3600)
    )
    
    return {
        "session_id": session_id,
        "status": "CREATED",
        "coordinator_endpoint": session_data["coordinator_endpoint"],
        "min_participants": min_participants,
        "total_rounds": session_data["total_rounds"]
    }


@app.post("/api/v1/sessions/{session_id}/join", response_model=Dict[str, Any])
async def join_federated_learning_session(
    session_id: str,
    request: ParticipantJoinRequest,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Join an existing federated learning session"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    participant_id = f"{tenant_id}:{user_id}"
    
    # Get session from Ignite
    cache = coordinator.ignite_client.get_cache("fl_sessions")
    session_json = cache.get(session_id)
    
    if not session_json:
        raise HTTPException(status_code=404, detail="Session not found")
    
    session_data = json.loads(session_json)
    
    # Check if session is accepting participants
    if session_data["status"] not in ["WAITING_FOR_PARTICIPANTS", "TRAINING"]:
        raise HTTPException(status_code=400, detail="Session is not accepting participants")
    
    # Verify participation criteria
    criteria = session_data["participation_criteria"]
    
    # Check credentials
    required_credentials = criteria.get("required_credentials", [])
    if required_credentials:
        verified_creds = await coordinator.verify_participant_credentials(
            user_id,
            tenant_id,
            required_credentials
        )
        
        if len(verified_creds) < len(required_credentials):
            raise HTTPException(
                status_code=403,
                detail="Missing required credentials for participation"
            )
    
    # Check reputation score
    min_reputation = criteria.get("min_reputation_score")
    if min_reputation is not None:
        reputation = await coordinator.get_user_reputation(user_id, tenant_id)
        if reputation is None or reputation < min_reputation:
            raise HTTPException(
                status_code=403,
                detail=f"Reputation score {reputation} is below required {min_reputation}"
            )
    
    # Check allowed tenants
    allowed_tenants = criteria.get("allowed_tenants")
    if allowed_tenants and tenant_id not in allowed_tenants:
        raise HTTPException(
            status_code=403,
            detail="Your tenant is not allowed to participate"
        )
    
    # Create participant record
    participant_data = {
        "participant_id": participant_id,
        "tenant_id": tenant_id,
        "user_id": user_id,
        "session_id": session_id,
        "dataset_stats": request.dataset_stats,
        "compute_capabilities": request.compute_capabilities,
        "public_key": request.public_key,
        "verified_credentials": verified_creds if required_credentials else [],
        "reputation_score": await coordinator.get_user_reputation(user_id, tenant_id),
        "joined_at": datetime.utcnow().isoformat(),
        "status": "ACTIVE"
    }
    
    # Store participant in Ignite
    coordinator.add_participant_to_ignite(session_id, participant_data)
    
    # Update session participant count
    session_data["participants"].append(participant_id)
    coordinator.update_session_in_ignite(session_id, {
        "participants": session_data["participants"],
        "participant_count": len(session_data["participants"])
    })
    
    # Publish participant joined event
    event_data = {
        "session_id": session_id,
        "participant_id": participant_id,
        "tenant_id": tenant_id,
        "dataset_stats": request.dataset_stats,
        "compute_capabilities": request.compute_capabilities,
        "verified_credentials": participant_data["verified_credentials"],
        "reputation_score": participant_data["reputation_score"],
        "public_key": request.public_key,
        "timestamp": int(time.time() * 1000)
    }
    
    coordinator.event_publisher.publish(
        topic=f"persistent://platformq/{session_data['tenant_id']}/federated-participant-joined",
        schema_path="federated_participant_joined.avsc",
        data=event_data
    )
    
    return {
        "session_id": session_id,
        "participant_id": participant_id,
        "status": "JOINED",
        "current_round": session_data["current_round"],
        "total_rounds": session_data["total_rounds"]
    }


@app.post("/api/v1/sessions/{session_id}/updates", response_model=Dict[str, Any])
async def submit_model_update(
    session_id: str,
    request: ModelUpdateSubmission,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Submit a model update for the current round"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    participant_id = f"{tenant_id}:{user_id}"
    
    # Verify participant is in session
    participants_cache = coordinator.ignite_client.get_cache("fl_participants")
    participant_key = f"{session_id}:{participant_id}"
    participant_json = participants_cache.get(participant_key)
    
    if not participant_json:
        raise HTTPException(status_code=403, detail="Not a participant in this session")
    
    # Create update record
    update_id = f"update_{session_id}_round{request.round_number}_{participant_id}"
    update_data = {
        "update_id": update_id,
        "session_id": session_id,
        "round_number": request.round_number,
        "participant_id": participant_id,
        "update_uri": request.update_uri,
        "metrics": request.metrics,
        "zkp": request.zkp,
        "submitted_at": datetime.utcnow().isoformat()
    }
    
    # Store in Ignite
    updates_cache = coordinator.ignite_client.get_or_create_cache(f"fl_model_updates_{session_id}")
    updates_cache.put(update_id, json.dumps(update_data))
    
    # Publish model update event
    event_data = {
        "session_id": session_id,
        "round_number": request.round_number,
        "participant_id": participant_id,
        "update_id": update_id,
        "model_weights_uri": request.update_uri,
        "model_metrics": {
            "loss": request.metrics.get("loss", 0.0),
            "accuracy": request.metrics.get("accuracy"),
            "custom_metrics": {k: v for k, v in request.metrics.items() 
                              if k not in ["loss", "accuracy"]}
        },
        "training_metadata": {
            "samples_used": request.metrics.get("training_samples", 0),
            "epochs_completed": request.metrics.get("epochs", 1),
            "training_time_seconds": request.metrics.get("training_time", 0),
            "differential_privacy_applied": True
        },
        "cryptographic_proof": request.zkp,
        "timestamp": int(time.time() * 1000)
    }
    
    coordinator.event_publisher.publish(
        topic=f"persistent://platformq/{session_id}/federated-model-update",
        schema_path="federated_model_update.avsc",
        data=event_data
    )
    
    # Check if all participants have submitted
    await check_and_trigger_aggregation(session_id, request.round_number)
    
    return {
        "update_id": update_id,
        "status": "SUBMITTED",
        "round_number": request.round_number
    }


@app.get("/api/v1/sessions/{session_id}/status", response_model=SessionStatus)
async def get_session_status(
    session_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get the current status of a federated learning session"""
    # Get session from Ignite
    cache = coordinator.ignite_client.get_cache("fl_sessions")
    session_json = cache.get(session_id)
    
    if not session_json:
        raise HTTPException(status_code=404, detail="Session not found")
    
    session_data = json.loads(session_json)
    
    # Get participant details
    participants_cache = coordinator.ignite_client.get_cache("fl_participants")
    participant_keys = [k for k in participants_cache.keys() if k.startswith(f"{session_id}:")]
    
    participants = []
    for key in participant_keys:
        participant_data = json.loads(participants_cache.get(key))
        participants.append({
            "participant_id": participant_data["participant_id"],
            "dataset_samples": participant_data["dataset_stats"]["num_samples"],
            "status": participant_data["status"]
        })
    
    # Get convergence metrics if available
    convergence_metrics = None
    if session_data["current_round"] > 0:
        metrics_cache = coordinator.ignite_client.get_cache(f"fl_round_metrics_{session_id}")
        round_metrics_key = f"round_{session_data['current_round']}_metrics"
        metrics_json = metrics_cache.get(round_metrics_key)
        if metrics_json:
            metrics_data = json.loads(metrics_json)
            convergence_metrics = metrics_data.get("convergence_metrics")
    
    return SessionStatus(
        session_id=session_id,
        status=session_data["status"],
        current_round=session_data["current_round"],
        total_rounds=session_data["total_rounds"],
        participants=participants,
        convergence_metrics=convergence_metrics,
        last_updated=datetime.fromisoformat(session_data.get("updated_at", session_data["created_at"]))
    )


@app.get("/api/v1/sessions/{session_id}/model", response_model=Dict[str, Any])
async def get_aggregated_model(
    session_id: str,
    round_number: Optional[int] = None,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get the aggregated model for a specific round"""
    # Get session to verify access
    cache = coordinator.ignite_client.get_cache("fl_sessions")
    session_json = cache.get(session_id)
    
    if not session_json:
        raise HTTPException(status_code=404, detail="Session not found")
    
    session_data = json.loads(session_json)
    
    # Use latest round if not specified
    if round_number is None:
        round_number = max(0, session_data["current_round"] - 1)
    
    # Get aggregated model from Ignite
    model_cache = coordinator.ignite_client.get_cache(f"fl_aggregated_models_{session_id}")
    model_key = f"aggregated_model_round_{round_number}"
    model_json = model_cache.get(model_key)
    
    if not model_json:
        raise HTTPException(status_code=404, detail=f"No aggregated model for round {round_number}")
    
    model_data = json.loads(model_json)
    
    # Request VC for the aggregated model
    vc_id = await request_aggregated_model_vc(session_id, round_number, model_data)
    
    return {
        "session_id": session_id,
        "round_number": round_number,
        "model_uri": model_data["aggregated_model_uri"],
        "aggregation_method": model_data["aggregation_method"],
        "num_participants": model_data["aggregated_weights"]["num_participants"],
        "total_samples": model_data["aggregated_weights"]["total_samples"],
        "verifiable_credential_id": vc_id,
        "timestamp": model_data["aggregation_timestamp"]
    }


# Background tasks
async def monitor_and_start_session(session_id: str, min_participants: int, timeout_seconds: int):
    """Monitor session and start when enough participants join"""
    start_time = time.time()
    
    while time.time() - start_time < timeout_seconds:
        # Get session data
        cache = coordinator.ignite_client.get_cache("fl_sessions")
        session_json = cache.get(session_id)
        
        if session_json:
            session_data = json.loads(session_json)
            
            if len(session_data["participants"]) >= min_participants:
                # Start first round
                await coordinator.start_training_round(session_id, 1)
                logger.info(f"Started federated learning session {session_id} with {len(session_data['participants'])} participants")
                return
        
        # Wait before checking again
        await asyncio.sleep(10)
    
    # Timeout - cancel session
    coordinator.update_session_in_ignite(session_id, {
        "status": "CANCELLED",
        "cancel_reason": "Not enough participants joined within timeout"
    })
    logger.warning(f"Cancelled session {session_id} due to timeout")


async def check_and_trigger_aggregation(session_id: str, round_number: int):
    """Check if all participants submitted and trigger aggregation"""
    # Get session data
    cache = coordinator.ignite_client.get_cache("fl_sessions")
    session_data = json.loads(cache.get(session_id))
    
    # Get submitted updates
    updates_cache = coordinator.ignite_client.get_cache(f"fl_model_updates_{session_id}")
    update_keys = [k for k in updates_cache.keys() if f"round{round_number}" in k]
    
    if len(update_keys) >= len(session_data["participants"]):
        # All participants submitted - trigger aggregation
        update_uris = []
        for key in update_keys:
            update_data = json.loads(updates_cache.get(key))
            update_uris.append(update_data["update_uri"])
        
        await coordinator.aggregate_model_updates(session_id, round_number, update_uris)
        
        # Publish round completed event after aggregation
        # (In production, this would be triggered by aggregation job completion)
        asyncio.create_task(publish_round_completed(session_id, round_number))


async def publish_round_completed(session_id: str, round_number: int):
    """Publish round completed event"""
    # Wait for aggregation to complete (mock delay)
    await asyncio.sleep(30)
    
    # Get aggregated model info
    model_cache = coordinator.ignite_client.get_cache(f"fl_aggregated_models_{session_id}")
    model_data = json.loads(model_cache.get(f"aggregated_model_round_{round_number}"))
    
    # Get round metrics
    metrics_cache = coordinator.ignite_client.get_cache(f"fl_round_metrics_{session_id}")
    metrics_data = json.loads(metrics_cache.get(f"round_{round_number}_metrics"))
    
    # Publish event
    event_data = {
        "session_id": session_id,
        "round_number": round_number,
        "aggregated_model_uri": model_data["aggregated_model_uri"],
        "participants": [
            {
                "participant_id": p,
                "contribution_weight": 1.0 / metrics_data["num_participants"],
                "update_received": True
            }
            for p in json.loads(coordinator.ignite_client.get_cache("fl_sessions").get(session_id))["participants"]
        ],
        "aggregation_metrics": {
            "aggregation_method": "FedAvg",
            "total_samples": model_data["aggregated_weights"]["total_samples"],
            "avg_loss": metrics_data["participant_metrics"].get("avg_loss", 0),
            "model_divergence": metrics_data["convergence_metrics"].get("model_divergence"),
            "convergence_score": metrics_data["convergence_metrics"].get("convergence_score")
        },
        "verifiable_credential_id": None,  # Will be set after VC issuance
        "next_round_start": int((time.time() + 60) * 1000) if round_number < 10 else None,
        "timestamp": int(time.time() * 1000)
    }
    
    coordinator.event_publisher.publish(
        topic=f"persistent://platformq/{session_id}/federated-round-completed",
        schema_path="federated_round_completed.avsc",
        data=event_data
    )
    
    # Update session status
    session_data = json.loads(coordinator.ignite_client.get_cache("fl_sessions").get(session_id))
    
    if round_number < session_data["total_rounds"]:
        # Start next round
        await coordinator.start_training_round(session_id, round_number + 1)
    else:
        # Session completed
        coordinator.update_session_in_ignite(session_id, {
            "status": "COMPLETED",
            "completed_at": datetime.utcnow().isoformat()
        })


async def request_aggregated_model_vc(session_id: str, round_number: int, model_data: Dict[str, Any]) -> Optional[str]:
    """Request a verifiable credential for the aggregated model"""
    try:
        # Get session data for context
        cache = coordinator.ignite_client.get_cache("fl_sessions")
        session_data = json.loads(cache.get(session_id))
        
        # Prepare VC request
        vc_request = {
            "credentialSubject": {
                "session_id": session_id,
                "round_number": round_number,
                "model_uri": model_data["aggregated_model_uri"],
                "algorithm": session_data["algorithm"],
                "num_participants": model_data["aggregated_weights"]["num_participants"],
                "total_samples": model_data["aggregated_weights"]["total_samples"],
                "convergence_score": model_data.get("convergence_metrics", {}).get("convergence_score"),
                "privacy_preserving": True,
                "aggregation_method": model_data["aggregation_method"]
            },
            "credentialType": "FederatedLearningModelCredential"
        }
        
        # Call VC service
        response = await coordinator.http_client.post(
            f"{VC_SERVICE_URL}/api/v1/issue",
            json=vc_request,
            headers={"X-Tenant-ID": session_data["tenant_id"]}
        )
        
        if response.status_code == 200:
            vc_data = response.json()
            return vc_data.get("id")
            
    except Exception as e:
        logger.error(f"Failed to request VC for aggregated model: {e}")
    
    return None


# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "federated-learning-service",
        "timestamp": datetime.utcnow().isoformat()
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 