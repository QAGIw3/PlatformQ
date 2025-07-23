"""
Machine Learning data models.

Provides data models for ML operations, training, and inference.
"""

import uuid
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field

from .base_models import TimestampedModel, VersionedModel, AuditedModel


class ModelFramework(str, Enum):
    """ML frameworks"""
    TENSORFLOW = "tensorflow"
    PYTORCH = "pytorch"
    SCIKIT_LEARN = "scikit_learn"
    XGBOOST = "xgboost"
    LIGHTGBM = "lightgbm"
    CATBOOST = "catboost"
    SPARK_ML = "spark_ml"
    CUSTOM = "custom"


class ModelType(str, Enum):
    """Model types"""
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    CLUSTERING = "clustering"
    RECOMMENDATION = "recommendation"
    TIME_SERIES = "time_series"
    NLP = "nlp"
    COMPUTER_VISION = "computer_vision"
    REINFORCEMENT_LEARNING = "reinforcement_learning"
    GENERATIVE = "generative"
    ANOMALY_DETECTION = "anomaly_detection"


class ModelStage(str, Enum):
    """Model lifecycle stages"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"
    DEPRECATED = "deprecated"


class TrainingStatus(str, Enum):
    """Training job status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class DeploymentStatus(str, Enum):
    """Model deployment status"""
    NOT_DEPLOYED = "not_deployed"
    DEPLOYING = "deploying"
    DEPLOYED = "deployed"
    FAILED = "failed"
    UNDEPLOYING = "undeploying"


@dataclass
class ModelMetrics(TimestampedModel):
    """Model performance metrics"""
    model_id: str = ""
    version: str = ""
    metrics: Dict[str, float] = field(default_factory=dict)
    dataset_id: Optional[str] = None
    evaluation_type: str = "validation"  # training, validation, test, production
    
    # Common metrics
    accuracy: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    auc_roc: Optional[float] = None
    mse: Optional[float] = None
    mae: Optional[float] = None
    r2_score: Optional[float] = None
    
    # Additional info
    confusion_matrix: Optional[List[List[int]]] = None
    feature_importance: Optional[Dict[str, float]] = None
    sample_size: Optional[int] = None


@dataclass
class ModelArtifact(TimestampedModel):
    """Model artifact storage"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    model_id: str = ""
    version: str = ""
    artifact_type: str = "model"  # model, preprocessor, postprocessor, config
    storage_path: str = ""
    storage_backend: str = "minio"
    size_bytes: Optional[int] = None
    checksum: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_download_url(self, expiry: int = 3600) -> str:
        """Get presigned download URL"""
        # Implementation would generate presigned URL
        return f"{self.storage_backend}://{self.storage_path}?expires={expiry}"


@dataclass
class ModelVersion(VersionedModel):
    """Model version information"""
    model_id: str = ""
    version: str = ""
    description: Optional[str] = None
    stage: ModelStage = ModelStage.DEVELOPMENT
    
    # Training info
    training_job_id: Optional[str] = None
    training_dataset_id: Optional[str] = None
    training_config: Dict[str, Any] = field(default_factory=dict)
    
    # Model info
    framework: ModelFramework = ModelFramework.CUSTOM
    framework_version: Optional[str] = None
    algorithm: Optional[str] = None
    hyperparameters: Dict[str, Any] = field(default_factory=dict)
    
    # Performance
    metrics: Optional[ModelMetrics] = None
    
    # Artifacts
    artifacts: List[ModelArtifact] = field(default_factory=list)
    
    # Deployment
    deployment_status: DeploymentStatus = DeploymentStatus.NOT_DEPLOYED
    deployment_config: Dict[str, Any] = field(default_factory=dict)
    endpoints: List[str] = field(default_factory=list)
    
    tags: List[str] = field(default_factory=list)


@dataclass
class MLModel(AuditedModel):
    """Machine Learning model"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: Optional[str] = None
    model_type: ModelType = ModelType.CLASSIFICATION
    
    # Current version
    current_version: Optional[str] = None
    current_stage: ModelStage = ModelStage.DEVELOPMENT
    
    # All versions
    versions: List[ModelVersion] = field(default_factory=list)
    
    # Model metadata
    owner: Optional[str] = None
    team: Optional[str] = None
    project: Optional[str] = None
    use_case: Optional[str] = None
    
    # Features
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    feature_names: List[str] = field(default_factory=list)
    target_names: List[str] = field(default_factory=list)
    
    # Constraints
    max_batch_size: Optional[int] = None
    max_latency_ms: Optional[int] = None
    min_accuracy: Optional[float] = None
    
    # Monitoring
    monitoring_enabled: bool = True
    alert_thresholds: Dict[str, float] = field(default_factory=dict)
    
    tags: List[str] = field(default_factory=list)
    
    def get_version(self, version: str) -> Optional[ModelVersion]:
        """Get specific model version"""
        for v in self.versions:
            if v.version == version:
                return v
        return None
        
    def get_production_version(self) -> Optional[ModelVersion]:
        """Get current production version"""
        for v in self.versions:
            if v.stage == ModelStage.PRODUCTION:
                return v
        return None


@dataclass
class TrainingDataset:
    """Training dataset configuration"""
    dataset_id: str
    version: Optional[str] = None
    split_ratio: Dict[str, float] = field(default_factory=lambda: {
        "train": 0.7,
        "validation": 0.15,
        "test": 0.15
    })
    preprocessing_steps: List[Dict[str, Any]] = field(default_factory=list)
    sampling_config: Optional[Dict[str, Any]] = None
    features: List[str] = field(default_factory=list)
    target: Optional[str] = None


@dataclass
class TrainingConfig:
    """Training configuration"""
    framework: ModelFramework = ModelFramework.SCIKIT_LEARN
    algorithm: str = ""
    hyperparameters: Dict[str, Any] = field(default_factory=dict)
    
    # Training settings
    epochs: Optional[int] = None
    batch_size: Optional[int] = None
    learning_rate: Optional[float] = None
    optimizer: Optional[str] = None
    loss_function: Optional[str] = None
    
    # Resources
    compute_type: str = "cpu"  # cpu, gpu, tpu
    num_workers: int = 1
    memory_gb: Optional[int] = None
    gpu_type: Optional[str] = None
    
    # Advanced
    distributed: bool = False
    mixed_precision: bool = False
    gradient_checkpointing: bool = False
    early_stopping: Optional[Dict[str, Any]] = None
    callbacks: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class TrainingJob(TimestampedModel):
    """ML training job"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    model_id: str = ""
    version: str = ""
    
    # Status
    status: TrainingStatus = TrainingStatus.PENDING
    progress: float = 0.0
    
    # Configuration
    dataset: TrainingDataset = field(default_factory=TrainingDataset)
    config: TrainingConfig = field(default_factory=TrainingConfig)
    
    # Execution
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    
    # Results
    metrics: Dict[str, Any] = field(default_factory=dict)
    artifacts: List[str] = field(default_factory=list)
    logs_path: Optional[str] = None
    
    # Tracking
    experiment_id: Optional[str] = None
    run_id: Optional[str] = None
    
    # Error handling
    error_message: Optional[str] = None
    retry_count: int = 0
    
    def get_duration(self) -> Optional[float]:
        """Get job duration"""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return self.duration_seconds


@dataclass
class PredictionRequest(TimestampedModel):
    """Model prediction request"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    model_id: str = ""
    version: Optional[str] = None
    
    # Input data
    data: Union[Dict[str, Any], List[Dict[str, Any]]] = field(default_factory=dict)
    input_format: str = "json"  # json, csv, numpy, tensor
    
    # Options
    include_probabilities: bool = False
    include_explanations: bool = False
    timeout_ms: Optional[int] = None
    
    # Metadata
    client_id: Optional[str] = None
    request_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PredictionResult(TimestampedModel):
    """Model prediction result"""
    request_id: str = ""
    model_id: str = ""
    version: str = ""
    
    # Predictions
    predictions: Union[Any, List[Any]] = field(default_factory=list)
    probabilities: Optional[Union[List[float], List[List[float]]]] = None
    
    # Performance
    latency_ms: float = 0.0
    
    # Explanations
    explanations: Optional[Dict[str, Any]] = None
    feature_importance: Optional[Dict[str, float]] = None
    
    # Monitoring
    input_drift_score: Optional[float] = None
    prediction_drift_score: Optional[float] = None
    
    # Metadata
    model_stage: ModelStage = ModelStage.DEVELOPMENT
    served_by: Optional[str] = None
    
    def to_response(self) -> Dict[str, Any]:
        """Convert to API response format"""
        response = {
            "request_id": self.request_id,
            "predictions": self.predictions,
            "model_version": self.version,
            "latency_ms": self.latency_ms
        }
        
        if self.probabilities is not None:
            response["probabilities"] = self.probabilities
            
        if self.explanations:
            response["explanations"] = self.explanations
            
        return response


@dataclass
class ModelDeployment(AuditedModel):
    """Model deployment configuration"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    model_id: str = ""
    version: str = ""
    
    # Deployment target
    environment: str = "development"  # development, staging, production
    region: str = "default"
    
    # Configuration
    replicas: int = 1
    autoscaling: Optional[Dict[str, Any]] = None
    resources: Dict[str, Any] = field(default_factory=lambda: {
        "cpu": "1",
        "memory": "2Gi"
    })
    
    # Endpoint
    endpoint_url: Optional[str] = None
    endpoint_type: str = "rest"  # rest, grpc, websocket
    authentication: Optional[Dict[str, Any]] = None
    
    # Health
    health_check_path: str = "/health"
    readiness_check_path: str = "/ready"
    
    # Monitoring
    metrics_enabled: bool = True
    logging_enabled: bool = True
    tracing_enabled: bool = True
    
    # Status
    status: DeploymentStatus = DeploymentStatus.NOT_DEPLOYED
    last_deployed: Optional[datetime] = None
    deployment_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ModelMonitoring(TimestampedModel):
    """Model monitoring configuration and data"""
    model_id: str = ""
    version: str = ""
    
    # Drift detection
    input_drift_enabled: bool = True
    output_drift_enabled: bool = True
    drift_threshold: float = 0.1
    reference_dataset_id: Optional[str] = None
    
    # Performance monitoring
    accuracy_monitoring: bool = True
    latency_monitoring: bool = True
    throughput_monitoring: bool = True
    
    # Alerts
    alert_rules: List[Dict[str, Any]] = field(default_factory=list)
    notification_channels: List[str] = field(default_factory=list)
    
    # Current metrics
    current_accuracy: Optional[float] = None
    current_latency_p50: Optional[float] = None
    current_latency_p99: Optional[float] = None
    current_throughput: Optional[float] = None
    
    # Historical data
    metrics_retention_days: int = 30
    
    def add_alert_rule(
        self,
        metric: str,
        threshold: float,
        condition: str = "greater_than",
        duration_minutes: int = 5
    ):
        """Add monitoring alert rule"""
        self.alert_rules.append({
            "metric": metric,
            "threshold": threshold,
            "condition": condition,
            "duration_minutes": duration_minutes,
            "enabled": True
        }) 