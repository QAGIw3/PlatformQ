"""
Workflow domain models extending data-intelligence-common
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from enum import Enum

# Import from common library
from data_intelligence_common.models.base_models import BaseModel
from data_intelligence_common.models.processing_models import (
    ProcessingJob,
    ProcessingStatus,
    ProcessingResult
)
from data_intelligence_common.core.orchestration.workflow_orchestrator import (
    WorkflowDefinition,
    WorkflowInstance,
    WorkflowStatus,
    WorkflowStep
)


class WorkflowType(str, Enum):
    """Workflow types"""
    DATA_PIPELINE = "data_pipeline"
    ML_TRAINING = "ml_training"
    DATA_QUALITY = "data_quality"
    ETL = "etl"
    STREAMING = "streaming"
    HYBRID = "hybrid"
    CUSTOM = "custom"


class TriggerType(str, Enum):
    """Workflow trigger types"""
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    EVENT = "event"
    DEPENDENCY = "dependency"
    API = "api"
    WEBHOOK = "webhook"


class DagState(str, Enum):
    """DAG states (Airflow compatible)"""
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PAUSED = "paused"
    QUEUED = "queued"


@dataclass
class WorkflowTemplate(BaseModel):
    """Workflow template"""
    template_id: str
    name: str
    description: Optional[str] = None
    workflow_type: WorkflowType = WorkflowType.DATA_PIPELINE
    
    # Template definition
    steps: List[Dict[str, Any]] = field(default_factory=list)
    default_config: Dict[str, Any] = field(default_factory=dict)
    
    # Parameters
    required_params: List[str] = field(default_factory=list)
    optional_params: Dict[str, Any] = field(default_factory=dict)
    
    # Metadata
    category: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    version: str = "1.0.0"
    
    # Usage
    usage_count: int = 0
    last_used: Optional[datetime] = None
    created_by: Optional[str] = None


@dataclass
class EnhancedWorkflowDefinition(WorkflowDefinition):
    """Enhanced workflow definition with orchestration features"""
    workflow_type: WorkflowType = WorkflowType.DATA_PIPELINE
    
    # Scheduling
    schedule_cron: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    
    # Triggers
    trigger_type: TriggerType = TriggerType.MANUAL
    trigger_config: Dict[str, Any] = field(default_factory=dict)
    
    # Dependencies
    depends_on: List[str] = field(default_factory=list)
    
    # Resource requirements
    resource_requirements: Dict[str, Any] = field(default_factory=dict)
    
    # Quality gates
    quality_gates: List[Dict[str, Any]] = field(default_factory=list)
    
    # ML optimization
    ml_optimization_enabled: bool = False
    optimization_target: str = "balanced"  # cost, performance, balanced
    
    # Airflow integration
    dag_id: Optional[str] = None
    airflow_config: Dict[str, Any] = field(default_factory=dict)
    
    # SeaTunnel integration
    seatunnel_enabled: bool = False
    seatunnel_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkflowRun(WorkflowInstance):
    """Workflow execution instance"""
    # Execution details
    trigger_info: Dict[str, Any] = field(default_factory=dict)
    
    # Performance metrics
    total_duration_ms: Optional[int] = None
    resource_usage: Dict[str, Any] = field(default_factory=dict)
    cost_estimate: Optional[float] = None
    
    # Quality metrics
    quality_score: Optional[float] = None
    quality_issues: List[Dict[str, Any]] = field(default_factory=list)
    
    # ML insights
    ml_predictions: Dict[str, Any] = field(default_factory=dict)
    optimization_applied: bool = False
    
    # Airflow integration
    dag_run_id: Optional[str] = None
    task_instances: List[Dict[str, Any]] = field(default_factory=list)
    
    # Lineage
    input_datasets: List[str] = field(default_factory=list)
    output_datasets: List[str] = field(default_factory=list)
    
    # Attestation
    attestation_id: Optional[str] = None
    attestation_issued: bool = False


@dataclass
class PipelineDefinition(BaseModel):
    """Data pipeline definition"""
    pipeline_id: str
    name: str
    description: Optional[str] = None
    pipeline_type: str = "batch"  # batch, streaming, hybrid
    
    # Pipeline steps
    steps: List[Dict[str, Any]] = field(default_factory=list)
    
    # Configuration
    config: Dict[str, Any] = field(default_factory=dict)
    parallelism: int = 1
    
    # Schedule
    schedule: Optional[str] = None
    
    # Dependencies
    depends_on: List[str] = field(default_factory=list)
    
    # Resource allocation
    resources: Dict[str, Any] = field(default_factory=dict)
    
    # Retry policy
    retry_attempts: int = 3
    retry_delay: int = 60  # seconds
    
    # Timeout
    timeout_minutes: Optional[int] = None
    
    # Owner
    owner: str
    team: Optional[str] = None
    
    # Status
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class EventMapping(BaseModel):
    """Event to workflow mapping"""
    mapping_id: str
    event_type: str
    workflow_id: str
    
    # Mapping configuration
    mapping_type: str = "direct"  # direct, pattern, aggregated, conditional
    conditions: List[Dict[str, Any]] = field(default_factory=list)
    
    # Pattern matching
    event_pattern: Optional[str] = None
    correlation_window: Optional[timedelta] = None
    
    # Aggregation
    aggregation_config: Dict[str, Any] = field(default_factory=dict)
    
    # Transformation
    event_transformation: Optional[Dict[str, Any]] = None
    
    # Status
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    # Statistics
    trigger_count: int = 0
    last_triggered: Optional[datetime] = None


@dataclass
class WorkflowOptimization(BaseModel):
    """Workflow optimization recommendation"""
    optimization_id: str
    workflow_id: str
    
    # Optimization details
    optimization_type: str  # resource, cost, performance, quality
    current_value: float
    recommended_value: float
    improvement_percentage: float
    
    # Recommendations
    recommendations: List[Dict[str, Any]] = field(default_factory=list)
    
    # Configuration changes
    config_changes: Dict[str, Any] = field(default_factory=dict)
    
    # Impact analysis
    impact_analysis: Dict[str, Any] = field(default_factory=dict)
    
    # ML model info
    model_id: Optional[str] = None
    confidence_score: float = 0.0
    
    # Status
    status: str = "pending"  # pending, applied, rejected
    created_at: datetime = field(default_factory=datetime.utcnow)
    applied_at: Optional[datetime] = None


@dataclass
class K8sJobDefinition(BaseModel):
    """Kubernetes job definition"""
    job_name: str
    namespace: str = "default"
    
    # Container spec
    image: str
    command: Optional[List[str]] = None
    args: Optional[List[str]] = None
    env_vars: Dict[str, str] = field(default_factory=dict)
    
    # Resources
    resources: Dict[str, Dict[str, str]] = field(default_factory=dict)
    # Example: {"requests": {"cpu": "1", "memory": "2Gi"}, "limits": {"cpu": "2", "memory": "4Gi"}}
    
    # Job configuration
    parallelism: int = 1
    completions: int = 1
    backoff_limit: int = 3
    
    # Cleanup
    ttl_seconds_after_finished: Optional[int] = 3600
    
    # Node selection
    node_selector: Dict[str, str] = field(default_factory=dict)
    tolerations: List[Dict[str, Any]] = field(default_factory=list)
    
    # Status tracking
    status: str = "pending"
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


@dataclass
class WorkflowAttestation(BaseModel):
    """Workflow execution attestation (Verifiable Credential)"""
    attestation_id: str
    workflow_id: str
    workflow_run_id: str
    
    # Attestation details
    attestation_type: str = "workflow_completion"
    
    # Execution summary
    execution_summary: Dict[str, Any] = field(default_factory=dict)
    
    # Quality metrics
    quality_metrics: Dict[str, Any] = field(default_factory=dict)
    
    # Signatures
    issuer: str
    issued_at: datetime = field(default_factory=datetime.utcnow)
    signature: Optional[str] = None
    
    # Credential
    credential_id: Optional[str] = None
    credential_status: str = "pending"  # pending, issued, revoked
    
    # Verification
    verification_url: Optional[str] = None
    expires_at: Optional[datetime] = None 