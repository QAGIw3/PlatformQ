from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from datetime import datetime

class DataQualityRequirements(BaseModel):
    """Data quality requirements for a federated learning session"""
    min_overall_score: float = Field(0.8, description="Minimum overall data quality score required")
    required_metrics: List[str] = Field(default_factory=list, description="List of specific quality metrics to enforce")

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
        "homomorphic_encryption": True,
        "encryption_scheme": "CKKS",  # CKKS, Paillier
        "aggregation_strategy": "SECURE_AGG",  # FED_AVG, FED_PROX, SECURE_AGG, DP_FED_AVG
        "adaptive_clipping": True,
        "noise_multiplier": 1.0,
        "byzantine_tolerance": 0.2
    })
    training_parameters: Dict[str, Any] = Field(..., description="Training configuration")
    participation_criteria: Dict[str, Any] = Field(default_factory=lambda: {
        "required_credentials": [],
        "min_reputation_score": None,
        "allowed_tenants": None
    })
    data_quality_requirements: DataQualityRequirements = Field(default_factory=DataQualityRequirements)

class ParticipantJoinRequest(BaseModel):
    """Request to join a federated learning session"""
    session_id: str
    dataset_stats: Dict[str, Any]
    compute_capabilities: Dict[str, Any]
    public_key: str = Field(..., description="Public key for secure aggregation")
    dataset_uri: str = Field(..., description="URI to the participant's dataset for quality profiling")

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