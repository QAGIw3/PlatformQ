"""
Base classes for unified pipeline framework.

Consolidates common pipeline elements from processing and orchestration modules.
"""

from typing import Dict, List, Any, Optional, Union, Callable, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from abc import ABC, abstractmethod
import uuid

from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class StageType(str, Enum):
    """Unified stage types"""
    # Data operations
    SOURCE = "source"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    JOIN = "join"
    SINK = "sink"
    
    # Quality and validation
    QUALITY = "quality"
    VALIDATE = "validate"
    
    # Control flow
    BRANCH = "branch"
    MERGE = "merge"
    CONDITIONAL = "conditional"
    
    # ML operations
    FEATURE = "feature"
    TRAIN = "train"
    PREDICT = "predict"
    EVALUATE = "evaluate"
    
    # Custom
    CUSTOM = "custom"


class StageStatus(str, Enum):
    """Stage execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


class ExecutionMode(str, Enum):
    """Pipeline execution modes"""
    SEQUENTIAL = "sequential"      # Execute stages one after another
    PARALLEL = "parallel"          # Execute independent stages in parallel
    CONDITIONAL = "conditional"    # Execute based on conditions
    ITERATIVE = "iterative"       # Execute in loops
    STREAMING = "streaming"       # Continuous execution
    HYBRID = "hybrid"            # Mix of batch and stream


class ProcessingEngine(str, Enum):
    """Processing engines"""
    SPARK = "spark"
    FLINK = "flink"
    BEAM = "beam"
    NATIVE = "native"
    AUTO = "auto"


class TriggerType(str, Enum):
    """Pipeline trigger types"""
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    EVENT = "event"
    DATA_ARRIVAL = "data_arrival"
    DEPENDENCY = "dependency"
    API = "api"


@dataclass
class RetryConfig:
    """Retry configuration"""
    max_retries: int = 3
    retry_delay: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    exponential_backoff: bool = True
    backoff_factor: float = 2.0
    max_retry_delay: timedelta = field(default_factory=lambda: timedelta(minutes=30))
    retry_on_errors: List[type] = field(default_factory=list)
    skip_on_errors: List[type] = field(default_factory=list)


@dataclass
class ResourceConfig:
    """Resource configuration for stages"""
    cpu_cores: Optional[float] = None
    memory_mb: Optional[int] = None
    gpu_count: Optional[int] = None
    executor_instances: Optional[int] = None
    parallelism: Optional[int] = None
    
    # Spark specific
    spark_conf: Dict[str, str] = field(default_factory=dict)
    
    # Flink specific
    flink_conf: Dict[str, str] = field(default_factory=dict)
    
    # Resource limits
    max_execution_time: Optional[timedelta] = None
    max_memory_mb: Optional[int] = None


@dataclass
class StageConfig:
    """Configuration for a pipeline stage"""
    stage_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    stage_type: StageType = StageType.CUSTOM
    
    # Execution
    engine: ProcessingEngine = ProcessingEngine.AUTO
    function: Optional[Callable] = None
    processor: Optional[Any] = None  # Processor instance
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    # Dependencies
    depends_on: List[str] = field(default_factory=list)
    
    # Control flow
    condition: Optional[Callable[[Any], bool]] = None
    continue_on_error: bool = False
    timeout: Optional[timedelta] = None
    
    # Resources
    resources: ResourceConfig = field(default_factory=ResourceConfig)
    
    # Retry
    retry_config: RetryConfig = field(default_factory=RetryConfig)
    
    # Monitoring
    enable_metrics: bool = True
    enable_lineage: bool = True
    
    # Metadata
    description: str = ""
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StageResult:
    """Result of stage execution"""
    stage_id: str
    stage_name: str
    status: StageStatus
    
    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Data
    input_records: int = 0
    output_records: int = 0
    input_data: Optional[Any] = None
    output_data: Optional[Any] = None
    
    # Execution
    execution_time_ms: float = 0.0
    retry_count: int = 0
    
    # Error handling
    error: Optional[str] = None
    error_type: Optional[str] = None
    stack_trace: Optional[str] = None
    
    # Metrics
    metrics: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def success(self) -> bool:
        """Check if stage succeeded"""
        return self.status == StageStatus.COMPLETED
        
    @property
    def duration(self) -> Optional[timedelta]:
        """Get stage duration"""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None


@dataclass
class PipelineConfig:
    """Configuration for a pipeline"""
    pipeline_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    version: str = "1.0.0"
    
    # Execution
    execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL
    default_engine: ProcessingEngine = ProcessingEngine.AUTO
    
    # Resources
    max_parallelism: int = 10
    resource_limits: ResourceConfig = field(default_factory=ResourceConfig)
    
    # Error handling
    fail_fast: bool = True
    max_failures: int = 5
    
    # Monitoring
    enable_monitoring: bool = True
    enable_lineage: bool = True
    checkpoint_interval: Optional[timedelta] = None
    
    # Scheduling
    schedule: Optional[str] = None  # Cron expression
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    
    # Metadata
    description: str = ""
    owner: str = ""
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineResult:
    """Result of pipeline execution"""
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_id: str = ""
    pipeline_name: str = ""
    
    # Status
    status: StageStatus = StageStatus.PENDING
    
    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Execution
    execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL
    trigger_type: TriggerType = TriggerType.MANUAL
    triggered_by: Optional[str] = None
    
    # Parameters
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    # Results
    stage_results: Dict[str, StageResult] = field(default_factory=dict)
    final_output: Optional[Any] = None
    
    # Metrics
    total_records_processed: int = 0
    stages_completed: int = 0
    stages_failed: int = 0
    stages_skipped: int = 0
    
    # Error tracking
    errors: List[Dict[str, Any]] = field(default_factory=list)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def success(self) -> bool:
        """Check if pipeline succeeded"""
        return self.status == StageStatus.COMPLETED
        
    @property
    def duration(self) -> Optional[timedelta]:
        """Get pipeline duration"""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None
        
    def get_stage_order(self) -> List[str]:
        """Get execution order of stages"""
        return sorted(
            self.stage_results.keys(),
            key=lambda x: self.stage_results[x].started_at or datetime.min
        )


class PipelineStage(ABC):
    """Abstract base class for pipeline stages"""
    
    def __init__(self, config: StageConfig):
        self.config = config
        self.stage_id = config.stage_id
        self.name = config.name
        self.stage_type = config.stage_type
        
    @abstractmethod
    async def execute(
        self,
        input_data: Any,
        context: Dict[str, Any]
    ) -> StageResult:
        """Execute the stage"""
        pass
        
    async def validate_input(self, input_data: Any) -> bool:
        """Validate input data"""
        return True
        
    async def validate_output(self, output_data: Any) -> bool:
        """Validate output data"""
        return True
        
    def get_resource_requirements(self) -> ResourceConfig:
        """Get resource requirements"""
        return self.config.resources 