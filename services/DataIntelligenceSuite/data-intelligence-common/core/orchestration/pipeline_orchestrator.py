"""
Pipeline orchestration for data and ML workflows.

Provides pipeline definition, execution, and monitoring capabilities.
"""

import uuid
import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Callable, Set
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
from collections import defaultdict, deque
import json

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger
from ...models.processing_models import (
    Pipeline,
    PipelineStage,
    JobStatus,
    ProcessingEngine,
    TriggerType,
    StageType
)

logger = StructuredLogger.get_logger(__name__)


class ExecutionMode(str, Enum):
    """Pipeline execution modes"""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"
    ITERATIVE = "iterative"


class RetryStrategy(str, Enum):
    """Retry strategies for failed stages"""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_DELAY = "fixed_delay"
    NO_RETRY = "no_retry"


@dataclass
class StageResult:
    """Result of stage execution"""
    stage_id: str
    stage_name: str
    status: JobStatus
    
    # Execution details
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    
    # Output
    output_data: Optional[Dict[str, Any]] = None
    output_location: Optional[str] = None
    
    # Error info
    error_message: Optional[str] = None
    retry_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "stage_id": self.stage_id,
            "stage_name": self.stage_name,
            "status": self.status.value,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "output_data": self.output_data,
            "output_location": self.output_location,
            "error_message": self.error_message,
            "retry_count": self.retry_count
        }


@dataclass
class PipelineRun:
    """Pipeline execution run"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_id: str = ""
    pipeline_name: str = ""
    
    # Execution state
    status: JobStatus = JobStatus.PENDING
    execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL
    
    # Timing
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Configuration
    parameters: Dict[str, Any] = field(default_factory=dict)
    environment: Dict[str, str] = field(default_factory=dict)
    
    # Results
    stage_results: Dict[str, StageResult] = field(default_factory=dict)
    final_output: Optional[Dict[str, Any]] = None
    
    # Metadata
    triggered_by: Optional[str] = None
    trigger_type: TriggerType = TriggerType.MANUAL
    parent_run_id: Optional[str] = None
    
    def get_duration(self) -> Optional[float]:
        """Get total run duration"""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "status": self.status.value,
            "execution_mode": self.execution_mode.value,
            "scheduled_at": self.scheduled_at.isoformat() if self.scheduled_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "parameters": self.parameters,
            "environment": self.environment,
            "stage_results": {k: v.to_dict() for k, v in self.stage_results.items()},
            "final_output": self.final_output,
            "triggered_by": self.triggered_by,
            "trigger_type": self.trigger_type.value,
            "parent_run_id": self.parent_run_id
        }


@dataclass
class PipelineSchedule:
    """Pipeline scheduling configuration"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_id: str = ""
    
    # Schedule
    cron_expression: Optional[str] = None
    interval_seconds: Optional[int] = None
    
    # Configuration
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    # State
    is_active: bool = True
    last_run_at: Optional[datetime] = None
    next_run_at: Optional[datetime] = None
    
    # Metadata
    created_by: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)


class BaseStageExecutor(ABC):
    """Base class for stage executors"""
    
    @abstractmethod
    async def execute(
        self,
        stage: PipelineStage,
        input_data: Dict[str, Any],
        context: Dict[str, Any]
    ) -> StageResult:
        """Execute pipeline stage"""
        pass
        
    @abstractmethod
    def can_handle(self, stage: PipelineStage) -> bool:
        """Check if executor can handle stage"""
        pass


class SparkStageExecutor(BaseStageExecutor):
    """Executor for Spark-based stages"""
    
    async def execute(
        self,
        stage: PipelineStage,
        input_data: Dict[str, Any],
        context: Dict[str, Any]
    ) -> StageResult:
        """Execute Spark stage"""
        result = StageResult(
            stage_id=stage.id,
            stage_name=stage.name,
            status=JobStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        try:
            # Submit Spark job
            # This would integrate with Spark cluster
            logger.info(f"Executing Spark stage: {stage.name}")
            
            # Simulate execution
            await asyncio.sleep(1)
            
            # Mark as completed
            result.status = JobStatus.COMPLETED
            result.completed_at = datetime.utcnow()
            result.duration_seconds = (result.completed_at - result.started_at).total_seconds()
            result.output_data = {"processed_records": 1000}
            
        except Exception as e:
            result.status = JobStatus.FAILED
            result.error_message = str(e)
            result.completed_at = datetime.utcnow()
            
        return result
        
    def can_handle(self, stage: PipelineStage) -> bool:
        """Check if stage uses Spark"""
        return stage.engine == ProcessingEngine.SPARK


class FlinkStageExecutor(BaseStageExecutor):
    """Executor for Flink-based stages"""
    
    async def execute(
        self,
        stage: PipelineStage,
        input_data: Dict[str, Any],
        context: Dict[str, Any]
    ) -> StageResult:
        """Execute Flink stage"""
        result = StageResult(
            stage_id=stage.id,
            stage_name=stage.name,
            status=JobStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        try:
            # Submit Flink job
            # This would integrate with Flink cluster
            logger.info(f"Executing Flink stage: {stage.name}")
            
            # Simulate execution
            await asyncio.sleep(1)
            
            # Mark as completed
            result.status = JobStatus.COMPLETED
            result.completed_at = datetime.utcnow()
            result.duration_seconds = (result.completed_at - result.started_at).total_seconds()
            result.output_data = {"processed_events": 5000}
            
        except Exception as e:
            result.status = JobStatus.FAILED
            result.error_message = str(e)
            result.completed_at = datetime.utcnow()
            
        return result
        
    def can_handle(self, stage: PipelineStage) -> bool:
        """Check if stage uses Flink"""
        return stage.engine == ProcessingEngine.FLINK


class PipelineOrchestrator:
    """
    Orchestrates data and ML pipeline execution.
    
    Features:
    - Pipeline definition and validation
    - Stage dependency management
    - Parallel and sequential execution
    - Retry and error handling
    - Scheduling and triggers
    - Monitoring and metrics
    """
    
    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # Storage
        self._pipelines: Dict[str, Pipeline] = {}
        self._runs: Dict[str, PipelineRun] = {}
        self._schedules: Dict[str, PipelineSchedule] = {}
        
        # Executors
        self._executors: List[BaseStageExecutor] = [
            SparkStageExecutor(),
            FlinkStageExecutor()
        ]
        
        # Execution state
        self._active_runs: Set[str] = set()
        self._run_history: deque = deque(maxlen=1000)
        
        # Scheduler
        self._scheduler_task: Optional[asyncio.Task] = None
        
    def register_pipeline(
        self,
        pipeline: Pipeline
    ) -> str:
        """Register pipeline definition"""
        # Validate pipeline
        validation_errors = self._validate_pipeline(pipeline)
        if validation_errors:
            raise ValueError(f"Pipeline validation failed: {validation_errors}")
            
        # Store pipeline
        self._pipelines[pipeline.id] = pipeline
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="pipeline.registered",
                source="pipeline_orchestrator",
                data={
                    "pipeline_id": pipeline.id,
                    "pipeline_name": pipeline.name,
                    "stage_count": len(pipeline.stages)
                }
            ))
            
        logger.info(f"Registered pipeline: {pipeline.name}")
        return pipeline.id
        
    def _validate_pipeline(self, pipeline: Pipeline) -> List[str]:
        """Validate pipeline definition"""
        errors = []
        
        # Check for empty pipeline
        if not pipeline.stages:
            errors.append("Pipeline has no stages")
            
        # Check stage names are unique
        stage_names = [s.name for s in pipeline.stages]
        if len(stage_names) != len(set(stage_names)):
            errors.append("Duplicate stage names found")
            
        # Validate stage dependencies
        stage_ids = {s.id for s in pipeline.stages}
        for stage in pipeline.stages:
            for dep in stage.dependencies:
                if dep not in stage_ids:
                    errors.append(f"Stage {stage.name} depends on unknown stage: {dep}")
                    
        # Check for circular dependencies
        if self._has_circular_dependencies(pipeline):
            errors.append("Circular dependencies detected")
            
        return errors
        
    def _has_circular_dependencies(self, pipeline: Pipeline) -> bool:
        """Check for circular dependencies using DFS"""
        # Build adjacency list
        graph = defaultdict(list)
        for stage in pipeline.stages:
            for dep in stage.dependencies:
                graph[dep].append(stage.id)
                
        # DFS to detect cycles
        visited = set()
        rec_stack = set()
        
        def has_cycle(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in graph[node]:
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
                    
            rec_stack.remove(node)
            return False
            
        for stage in pipeline.stages:
            if stage.id not in visited:
                if has_cycle(stage.id):
                    return True
                    
        return False
        
    async def execute_pipeline(
        self,
        pipeline_id: str,
        parameters: Optional[Dict[str, Any]] = None,
        execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL,
        triggered_by: Optional[str] = None,
        trigger_type: TriggerType = TriggerType.MANUAL
    ) -> PipelineRun:
        """Execute pipeline"""
        pipeline = self._pipelines.get(pipeline_id)
        if not pipeline:
            raise ValueError(f"Pipeline not found: {pipeline_id}")
            
        # Create run
        run = PipelineRun(
            pipeline_id=pipeline_id,
            pipeline_name=pipeline.name,
            execution_mode=execution_mode,
            parameters=parameters or {},
            triggered_by=triggered_by,
            trigger_type=trigger_type
        )
        
        # Store run
        self._runs[run.id] = run
        self._active_runs.add(run.id)
        
        # Start execution
        asyncio.create_task(self._execute_run(pipeline, run))
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="pipeline.run.started",
                source="pipeline_orchestrator",
                data={
                    "run_id": run.id,
                    "pipeline_id": pipeline_id,
                    "execution_mode": execution_mode.value
                }
            ))
            
        logger.info(f"Started pipeline run: {run.id} for pipeline {pipeline.name}")
        return run
        
    async def _execute_run(
        self,
        pipeline: Pipeline,
        run: PipelineRun
    ):
        """Execute pipeline run"""
        run.status = JobStatus.RUNNING
        run.started_at = datetime.utcnow()
        
        try:
            # Build execution plan
            execution_plan = self._build_execution_plan(pipeline, run.execution_mode)
            
            # Execute stages
            stage_outputs = {}
            
            for stage_group in execution_plan:
                if run.execution_mode == ExecutionMode.PARALLEL:
                    # Execute stages in parallel
                    tasks = []
                    for stage_id in stage_group:
                        stage = self._get_stage_by_id(pipeline, stage_id)
                        if stage:
                            task = self._execute_stage(
                                stage,
                                run,
                                stage_outputs
                            )
                            tasks.append(task)
                            
                    # Wait for all stages in group
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Check for failures
                    for result in results:
                        if isinstance(result, Exception):
                            raise result
                            
                else:
                    # Execute stages sequentially
                    for stage_id in stage_group:
                        stage = self._get_stage_by_id(pipeline, stage_id)
                        if stage:
                            await self._execute_stage(
                                stage,
                                run,
                                stage_outputs
                            )
                            
                            # Check if stage failed
                            stage_result = run.stage_results.get(stage.id)
                            if stage_result and stage_result.status == JobStatus.FAILED:
                                raise Exception(f"Stage {stage.name} failed")
                                
            # Pipeline completed successfully
            run.status = JobStatus.COMPLETED
            run.final_output = stage_outputs
            
        except Exception as e:
            run.status = JobStatus.FAILED
            logger.error(f"Pipeline run {run.id} failed: {e}")
            
        finally:
            run.completed_at = datetime.utcnow()
            self._active_runs.discard(run.id)
            self._run_history.append(run.id)
            
            # Cache run result
            if self.cache:
                cache_key = f"pipeline_run:{run.id}"
                self.cache.set(cache_key, run.to_dict(), ttl=3600)
                
            # Publish completion event
            if self.event_bus:
                self.event_bus.publish(Event(
                    type="pipeline.run.completed",
                    source="pipeline_orchestrator",
                    data={
                        "run_id": run.id,
                        "status": run.status.value,
                        "duration_seconds": run.get_duration()
                    }
                ))
                
    def _build_execution_plan(
        self,
        pipeline: Pipeline,
        execution_mode: ExecutionMode
    ) -> List[List[str]]:
        """Build execution plan based on dependencies"""
        if execution_mode == ExecutionMode.SEQUENTIAL:
            # Simple sequential order
            return [[stage.id] for stage in pipeline.stages]
            
        elif execution_mode == ExecutionMode.PARALLEL:
            # Topological sort with level grouping
            # Stages at same level can run in parallel
            levels = []
            in_degree = defaultdict(int)
            graph = defaultdict(list)
            
            # Build graph
            for stage in pipeline.stages:
                for dep in stage.dependencies:
                    graph[dep].append(stage.id)
                    in_degree[stage.id] += 1
                    
            # Find stages with no dependencies
            queue = deque()
            for stage in pipeline.stages:
                if in_degree[stage.id] == 0:
                    queue.append(stage.id)
                    
            # Process levels
            while queue:
                level = []
                level_size = len(queue)
                
                for _ in range(level_size):
                    stage_id = queue.popleft()
                    level.append(stage_id)
                    
                    # Update dependencies
                    for dependent in graph[stage_id]:
                        in_degree[dependent] -= 1
                        if in_degree[dependent] == 0:
                            queue.append(dependent)
                            
                levels.append(level)
                
            return levels
            
        else:
            # Default to sequential
            return [[stage.id] for stage in pipeline.stages]
            
    def _get_stage_by_id(
        self,
        pipeline: Pipeline,
        stage_id: str
    ) -> Optional[PipelineStage]:
        """Get stage by ID"""
        for stage in pipeline.stages:
            if stage.id == stage_id:
                return stage
        return None
        
    async def _execute_stage(
        self,
        stage: PipelineStage,
        run: PipelineRun,
        stage_outputs: Dict[str, Any]
    ) -> StageResult:
        """Execute single stage"""
        # Get executor
        executor = self._get_executor_for_stage(stage)
        if not executor:
            raise ValueError(f"No executor found for stage {stage.name}")
            
        # Prepare input
        input_data = self._prepare_stage_input(stage, run, stage_outputs)
        
        # Create context
        context = {
            "run_id": run.id,
            "pipeline_id": run.pipeline_id,
            "parameters": run.parameters,
            "environment": run.environment
        }
        
        # Execute with retry
        max_retries = stage.retry_config.get("max_retries", 0) if stage.retry_config else 0
        retry_count = 0
        
        while retry_count <= max_retries:
            try:
                # Execute stage
                result = await executor.execute(stage, input_data, context)
                
                # Store result
                run.stage_results[stage.id] = result
                
                # Store output for downstream stages
                if result.output_data:
                    stage_outputs[stage.id] = result.output_data
                    
                # Publish event
                if self.event_bus:
                    self.event_bus.publish(Event(
                        type="pipeline.stage.completed",
                        source="pipeline_orchestrator",
                        data={
                            "run_id": run.id,
                            "stage_id": stage.id,
                            "status": result.status.value
                        }
                    ))
                    
                return result
                
            except Exception as e:
                retry_count += 1
                
                if retry_count > max_retries:
                    # Final failure
                    result = StageResult(
                        stage_id=stage.id,
                        stage_name=stage.name,
                        status=JobStatus.FAILED,
                        started_at=datetime.utcnow(),
                        completed_at=datetime.utcnow(),
                        error_message=str(e),
                        retry_count=retry_count - 1
                    )
                    run.stage_results[stage.id] = result
                    raise
                    
                # Wait before retry
                retry_delay = self._calculate_retry_delay(
                    retry_count,
                    stage.retry_config
                )
                await asyncio.sleep(retry_delay)
                
                logger.warning(f"Retrying stage {stage.name}, attempt {retry_count}")
                
    def _get_executor_for_stage(
        self,
        stage: PipelineStage
    ) -> Optional[BaseStageExecutor]:
        """Get appropriate executor for stage"""
        for executor in self._executors:
            if executor.can_handle(stage):
                return executor
        return None
        
    def _prepare_stage_input(
        self,
        stage: PipelineStage,
        run: PipelineRun,
        stage_outputs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Prepare input data for stage"""
        input_data = {}
        
        # Add outputs from dependencies
        for dep_id in stage.dependencies:
            if dep_id in stage_outputs:
                input_data[dep_id] = stage_outputs[dep_id]
                
        # Add stage configuration
        if stage.config:
            input_data["config"] = stage.config
            
        # Add run parameters
        input_data["parameters"] = run.parameters
        
        return input_data
        
    def _calculate_retry_delay(
        self,
        retry_count: int,
        retry_config: Optional[Dict[str, Any]]
    ) -> float:
        """Calculate retry delay based on strategy"""
        if not retry_config:
            return 1.0
            
        strategy = retry_config.get("strategy", "exponential_backoff")
        base_delay = retry_config.get("base_delay", 1.0)
        max_delay = retry_config.get("max_delay", 60.0)
        
        if strategy == "exponential_backoff":
            delay = base_delay * (2 ** (retry_count - 1))
        elif strategy == "linear_backoff":
            delay = base_delay * retry_count
        else:
            delay = base_delay
            
        return min(delay, max_delay)
        
    def schedule_pipeline(
        self,
        pipeline_id: str,
        cron_expression: Optional[str] = None,
        interval_seconds: Optional[int] = None,
        parameters: Optional[Dict[str, Any]] = None,
        created_by: Optional[str] = None
    ) -> PipelineSchedule:
        """Schedule pipeline execution"""
        if not cron_expression and not interval_seconds:
            raise ValueError("Either cron_expression or interval_seconds must be provided")
            
        schedule = PipelineSchedule(
            pipeline_id=pipeline_id,
            cron_expression=cron_expression,
            interval_seconds=interval_seconds,
            parameters=parameters or {},
            created_by=created_by
        )
        
        # Calculate next run time
        schedule.next_run_at = self._calculate_next_run(schedule)
        
        # Store schedule
        self._schedules[schedule.id] = schedule
        
        # Start scheduler if not running
        if not self._scheduler_task or self._scheduler_task.done():
            self._scheduler_task = asyncio.create_task(self._run_scheduler())
            
        logger.info(f"Scheduled pipeline {pipeline_id}")
        return schedule
        
    def _calculate_next_run(
        self,
        schedule: PipelineSchedule
    ) -> Optional[datetime]:
        """Calculate next run time for schedule"""
        now = datetime.utcnow()
        
        if schedule.interval_seconds:
            # Simple interval
            if schedule.last_run_at:
                return schedule.last_run_at + timedelta(seconds=schedule.interval_seconds)
            else:
                return now
                
        elif schedule.cron_expression:
            # Parse cron expression
            # This would use a cron parser library
            # For now, return next hour
            return now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            
        return None
        
    async def _run_scheduler(self):
        """Run pipeline scheduler"""
        logger.info("Pipeline scheduler started")
        
        while True:
            try:
                now = datetime.utcnow()
                
                # Check schedules
                for schedule in list(self._schedules.values()):
                    if not schedule.is_active:
                        continue
                        
                    if schedule.next_run_at and now >= schedule.next_run_at:
                        # Execute pipeline
                        try:
                            await self.execute_pipeline(
                                schedule.pipeline_id,
                                parameters=schedule.parameters,
                                triggered_by="scheduler",
                                trigger_type=TriggerType.SCHEDULED
                            )
                            
                            # Update schedule
                            schedule.last_run_at = now
                            schedule.next_run_at = self._calculate_next_run(schedule)
                            
                        except Exception as e:
                            logger.error(f"Failed to execute scheduled pipeline: {e}")
                            
                # Sleep until next check
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                await asyncio.sleep(60)
                
    def get_pipeline(self, pipeline_id: str) -> Optional[Pipeline]:
        """Get pipeline by ID"""
        return self._pipelines.get(pipeline_id)
        
    def list_pipelines(
        self,
        owner: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> List[Pipeline]:
        """List pipelines with filters"""
        pipelines = list(self._pipelines.values())
        
        if owner:
            pipelines = [p for p in pipelines if p.owner == owner]
            
        if tags:
            tag_set = set(tags)
            pipelines = [p for p in pipelines if tag_set.intersection(p.tags)]
            
        return pipelines
        
    def get_run(self, run_id: str) -> Optional[PipelineRun]:
        """Get pipeline run"""
        # Check cache first
        if self.cache:
            cache_key = f"pipeline_run:{run_id}"
            cached = self.cache.get(cache_key)
            if cached:
                return self._dict_to_run(cached)
                
        return self._runs.get(run_id)
        
    def list_runs(
        self,
        pipeline_id: Optional[str] = None,
        status: Optional[JobStatus] = None,
        limit: int = 100
    ) -> List[PipelineRun]:
        """List pipeline runs"""
        runs = list(self._runs.values())
        
        if pipeline_id:
            runs = [r for r in runs if r.pipeline_id == pipeline_id]
            
        if status:
            runs = [r for r in runs if r.status == status]
            
        # Sort by start time descending
        runs.sort(key=lambda r: r.started_at or datetime.min, reverse=True)
        
        return runs[:limit]
        
    def cancel_run(self, run_id: str):
        """Cancel pipeline run"""
        run = self._runs.get(run_id)
        if not run:
            raise ValueError(f"Run not found: {run_id}")
            
        if run.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            raise ValueError(f"Cannot cancel run in status: {run.status}")
            
        # Update status
        run.status = JobStatus.CANCELLED
        run.completed_at = datetime.utcnow()
        
        # Remove from active runs
        self._active_runs.discard(run_id)
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="pipeline.run.cancelled",
                source="pipeline_orchestrator",
                data={"run_id": run_id}
            ))
            
        logger.info(f"Cancelled pipeline run: {run_id}")
        
    def _dict_to_run(self, data: Dict[str, Any]) -> PipelineRun:
        """Convert dictionary to PipelineRun"""
        # Handle datetime fields
        for field in ["scheduled_at", "started_at", "completed_at"]:
            if data.get(field):
                data[field] = datetime.fromisoformat(data[field])
                
        # Handle enums
        data["status"] = JobStatus(data["status"])
        data["execution_mode"] = ExecutionMode(data["execution_mode"])
        data["trigger_type"] = TriggerType(data["trigger_type"])
        
        # Handle stage results
        stage_results = {}
        for stage_id, result_data in data.get("stage_results", {}).items():
            result_data["status"] = JobStatus(result_data["status"])
            result_data["started_at"] = datetime.fromisoformat(result_data["started_at"])
            if result_data.get("completed_at"):
                result_data["completed_at"] = datetime.fromisoformat(result_data["completed_at"])
            stage_results[stage_id] = StageResult(**result_data)
        data["stage_results"] = stage_results
        
        return PipelineRun(**data) 