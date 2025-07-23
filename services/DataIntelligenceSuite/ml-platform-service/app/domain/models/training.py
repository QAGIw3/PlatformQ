"""
Domain models for ML training
"""
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum
from pydantic import BaseModel, Field, ConfigDict
from uuid import UUID, uuid4


class TrainingStatus(str, Enum):
    """Training job status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Framework(str, Enum):
    """ML frameworks"""
    PYTORCH = "pytorch"
    TENSORFLOW = "tensorflow"
    SKLEARN = "sklearn"
    XGBOOST = "xgboost"
    LIGHTGBM = "lightgbm"
    CATBOOST = "catboost"
    CUSTOM = "custom"


class DistributedStrategy(str, Enum):
    """Distributed training strategies"""
    NONE = "none"
    DATA_PARALLEL = "data_parallel"
    MODEL_PARALLEL = "model_parallel"
    HOROVOD = "horovod"
    PARAMETER_SERVER = "parameter_server"


class TrainingConfig(BaseModel):
    """Training configuration"""
    model_config = ConfigDict(from_attributes=True)
    
    framework: Framework
    distributed_strategy: DistributedStrategy = DistributedStrategy.NONE
    hyperparameters: Dict[str, Any] = Field(default_factory=dict)
    epochs: Optional[int] = None
    batch_size: Optional[int] = None
    learning_rate: Optional[float] = None
    optimizer: Optional[str] = None
    loss_function: Optional[str] = None
    metrics: List[str] = Field(default_factory=list)
    early_stopping: bool = False
    early_stopping_patience: int = 5
    checkpoint_interval: int = 300
    gpu_enabled: bool = False
    gpu_count: int = 0
    cpu_count: int = 4
    memory_gb: int = 16


class DatasetConfig(BaseModel):
    """Dataset configuration for training"""
    model_config = ConfigDict(from_attributes=True)
    
    train_path: str
    validation_path: Optional[str] = None
    test_path: Optional[str] = None
    data_format: str = "parquet"
    features: List[str] = Field(default_factory=list)
    target: Optional[str] = None
    sample_fraction: float = 1.0
    stratify: bool = False
    preprocessing_steps: List[Dict[str, Any]] = Field(default_factory=list)


class TrainingJob(BaseModel):
    """Training job entity"""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID = Field(default_factory=uuid4)
    name: str
    description: Optional[str] = None
    experiment_id: str
    user_id: str
    project_id: str
    
    training_config: TrainingConfig
    dataset_config: DatasetConfig
    
    status: TrainingStatus = TrainingStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    model_uri: Optional[str] = None
    model_version: Optional[str] = None
    metrics: Dict[str, float] = Field(default_factory=dict)
    logs_uri: Optional[str] = None
    artifacts_uri: Optional[str] = None
    
    error_message: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    
    spark_job_id: Optional[str] = None
    container_id: Optional[str] = None
    
    tags: Dict[str, str] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class TrainingMetrics(BaseModel):
    """Training metrics during execution"""
    model_config = ConfigDict(from_attributes=True)
    
    job_id: UUID
    epoch: int
    step: int
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    loss: float
    metrics: Dict[str, float] = Field(default_factory=dict)
    learning_rate: Optional[float] = None
    
    gpu_utilization: Optional[float] = None
    gpu_memory_used_gb: Optional[float] = None
    cpu_utilization: Optional[float] = None
    memory_used_gb: Optional[float] = None
    
    examples_per_second: Optional[float] = None
    time_per_step: Optional[float] = None


class HyperparameterTuning(BaseModel):
    """Hyperparameter tuning configuration"""
    model_config = ConfigDict(from_attributes=True)
    
    search_space: Dict[str, Any]
    optimization_metric: str
    optimization_direction: str = "maximize"  # maximize or minimize
    n_trials: int = 20
    timeout_minutes: int = 60
    sampler: str = "TPE"  # TPE, Random, Grid
    pruner: Optional[str] = "MedianPruner"
    parallel_trials: int = 1


class ExperimentRun(BaseModel):
    """ML experiment run"""
    model_config = ConfigDict(from_attributes=True)
    
    run_id: str
    experiment_id: str
    run_name: str
    user_id: str
    
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    
    parameters: Dict[str, Any] = Field(default_factory=dict)
    metrics: Dict[str, float] = Field(default_factory=dict)
    tags: Dict[str, str] = Field(default_factory=dict)
    
    model_uri: Optional[str] = None
    artifacts: List[str] = Field(default_factory=list)
    
    source_type: str = "LOCAL"
    source_name: Optional[str] = None
    source_version: Optional[str] = None


class TrainingPipeline(BaseModel):
    """Training pipeline definition"""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID = Field(default_factory=uuid4)
    name: str
    description: Optional[str] = None
    
    steps: List[Dict[str, Any]]
    schedule: Optional[str] = None  # Cron expression
    
    active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    last_run_id: Optional[UUID] = None
    last_run_status: Optional[str] = None
    last_run_at: Optional[datetime] = None
    
    tags: Dict[str, str] = Field(default_factory=dict) 