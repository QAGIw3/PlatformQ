"""
Domain models for ML models and serving
"""
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from enum import Enum
from pydantic import BaseModel, Field, ConfigDict
from uuid import UUID, uuid4


class ModelStage(str, Enum):
    """Model lifecycle stages"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"


class ModelFormat(str, Enum):
    """Model serialization formats"""
    PICKLE = "pickle"
    JOBLIB = "joblib"
    ONNX = "onnx"
    TENSORFLOW = "tensorflow"
    PYTORCH = "pytorch"
    PMML = "pmml"
    CUSTOM = "custom"


class ServingFramework(str, Enum):
    """Model serving frameworks"""
    TRITON = "triton"
    TORCHSERVE = "torchserve"
    TENSORFLOW_SERVING = "tensorflow_serving"
    BENTOML = "bentoml"
    KSERVE = "kserve"
    CUSTOM = "custom"


class DeploymentStrategy(str, Enum):
    """Deployment strategies"""
    BLUE_GREEN = "blue_green"
    CANARY = "canary"
    SHADOW = "shadow"
    AB_TEST = "ab_test"
    ROLLING = "rolling"


class Model(BaseModel):
    """ML model entity"""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID = Field(default_factory=uuid4)
    name: str
    version: str
    description: Optional[str] = None
    
    framework: str
    model_format: ModelFormat
    model_uri: str
    model_size_mb: float
    
    training_job_id: Optional[UUID] = None
    experiment_id: Optional[str] = None
    run_id: Optional[str] = None
    
    stage: ModelStage = ModelStage.DEVELOPMENT
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: str
    
    metrics: Dict[str, float] = Field(default_factory=dict)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    
    tags: Dict[str, str] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ModelVersion(BaseModel):
    """Model version tracking"""
    model_config = ConfigDict(from_attributes=True)
    
    model_id: UUID
    version: str
    stage: ModelStage
    
    promoted_from: Optional[ModelStage] = None
    promoted_at: Optional[datetime] = None
    promoted_by: Optional[str] = None
    
    deployment_count: int = 0
    inference_count: int = 0
    
    performance_metrics: Dict[str, float] = Field(default_factory=dict)
    drift_metrics: Dict[str, float] = Field(default_factory=dict)
    
    is_active: bool = True
    retired_at: Optional[datetime] = None
    retirement_reason: Optional[str] = None


class ModelDeployment(BaseModel):
    """Model deployment configuration"""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID = Field(default_factory=uuid4)
    model_id: UUID
    model_version: str
    deployment_name: str
    
    serving_framework: ServingFramework
    deployment_strategy: DeploymentStrategy
    
    replicas: int = 1
    min_replicas: int = 1
    max_replicas: int = 10
    
    cpu_request: str = "1"
    cpu_limit: str = "2"
    memory_request: str = "2Gi"
    memory_limit: str = "4Gi"
    gpu_request: int = 0
    
    endpoint_url: Optional[str] = None
    health_check_url: Optional[str] = None
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    deployed_by: str
    
    status: str = "pending"
    is_active: bool = True
    
    traffic_percentage: float = 100.0
    shadow_mode: bool = False
    
    environment_variables: Dict[str, str] = Field(default_factory=dict)
    labels: Dict[str, str] = Field(default_factory=dict)


class InferenceRequest(BaseModel):
    """Model inference request"""
    model_config = ConfigDict(from_attributes=True)
    
    request_id: UUID = Field(default_factory=uuid4)
    model_id: UUID
    model_version: str
    deployment_id: UUID
    
    input_data: Any
    input_format: str = "json"
    
    requested_at: datetime = Field(default_factory=datetime.utcnow)
    user_id: Optional[str] = None
    
    priority: int = 0
    timeout_seconds: int = 60
    
    metadata: Dict[str, Any] = Field(default_factory=dict)


class InferenceResponse(BaseModel):
    """Model inference response"""
    model_config = ConfigDict(from_attributes=True)
    
    request_id: UUID
    prediction: Any
    output_format: str = "json"
    
    model_id: UUID
    model_version: str
    
    inference_time_ms: float
    preprocessing_time_ms: Optional[float] = None
    postprocessing_time_ms: Optional[float] = None
    
    completed_at: datetime = Field(default_factory=datetime.utcnow)
    
    confidence_scores: Optional[Dict[str, float]] = None
    explanations: Optional[Dict[str, Any]] = None
    
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ModelMonitoringConfig(BaseModel):
    """Model monitoring configuration"""
    model_config = ConfigDict(from_attributes=True)
    
    model_id: UUID
    deployment_id: UUID
    
    drift_detection_enabled: bool = True
    drift_threshold: float = 0.1
    drift_window_size: int = 1000
    
    performance_monitoring_enabled: bool = True
    performance_metrics: List[str] = Field(default_factory=lambda: ["accuracy", "latency"])
    performance_threshold: Dict[str, float] = Field(default_factory=dict)
    
    data_quality_checks: List[str] = Field(default_factory=list)
    alert_channels: List[str] = Field(default_factory=list)
    
    sample_rate: float = 0.1
    log_predictions: bool = False
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class ModelDriftReport(BaseModel):
    """Model drift analysis report"""
    model_config = ConfigDict(from_attributes=True)
    
    report_id: UUID = Field(default_factory=uuid4)
    model_id: UUID
    deployment_id: UUID
    
    analysis_window_start: datetime
    analysis_window_end: datetime
    sample_count: int
    
    feature_drift_scores: Dict[str, float] = Field(default_factory=dict)
    prediction_drift_score: float
    
    drift_detected: bool
    drift_type: Optional[str] = None  # "data", "concept", "prediction"
    
    baseline_metrics: Dict[str, float] = Field(default_factory=dict)
    current_metrics: Dict[str, float] = Field(default_factory=dict)
    
    recommendations: List[str] = Field(default_factory=list)
    
    created_at: datetime = Field(default_factory=datetime.utcnow)


class ABTestConfig(BaseModel):
    """A/B test configuration for models"""
    model_config = ConfigDict(from_attributes=True)
    
    test_id: UUID = Field(default_factory=uuid4)
    test_name: str
    
    control_model_id: UUID
    control_version: str
    
    treatment_model_id: UUID
    treatment_version: str
    
    traffic_split: float = 0.5  # Percentage to treatment
    
    metrics_to_track: List[str]
    success_metric: str
    minimum_sample_size: int = 1000
    
    start_time: datetime
    end_time: Optional[datetime] = None
    
    status: str = "running"  # running, completed, cancelled
    winner: Optional[str] = None  # "control" or "treatment"
    
    results: Dict[str, Any] = Field(default_factory=dict) 