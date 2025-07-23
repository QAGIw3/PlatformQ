"""
Pipeline Builder Implementation for DataIntelligenceSuite

Provides fluent API for building data processing pipelines.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Callable, Union, TypeVar, Generic
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import uuid

from .base_processor import ProcessingResult, ProcessingStatus
from .batch_processor import BatchProcessor
from .stream_processor import StreamProcessor
from .quality_processor import QualityProcessor

logger = logging.getLogger(__name__)

T = TypeVar('T')


class StageType(Enum):
    """Types of pipeline stages"""
    SOURCE = "source"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    JOIN = "join"
    SINK = "sink"
    QUALITY = "quality"
    BRANCH = "branch"
    CUSTOM = "custom"


class ExecutionMode(Enum):
    """Pipeline execution modes"""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"


@dataclass
class StageConfig:
    """Configuration for a pipeline stage"""
    stage_id: str
    name: str
    stage_type: StageType
    processor: Optional[Any] = None
    function: Optional[Callable] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    # Execution control
    retry_on_failure: bool = True
    max_retries: int = 3
    timeout: Optional[timedelta] = None
    
    # Dependencies
    depends_on: List[str] = field(default_factory=list)
    
    # Conditional execution
    condition: Optional[Callable] = None
    
    # Error handling
    on_error: Optional[Callable] = None
    continue_on_error: bool = False


@dataclass
class StageResult:
    """Result of a pipeline stage execution"""
    stage_id: str
    status: ProcessingStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    
    # Data
    input_records: int = 0
    output_records: int = 0
    output_data: Optional[Any] = None
    
    # Errors
    error: Optional[str] = None
    retries: int = 0
    
    # Metrics
    execution_time_ms: Optional[float] = None
    
    @property
    def success(self) -> bool:
        """Check if stage succeeded"""
        return self.status == ProcessingStatus.COMPLETED


class TransformFunction(ABC):
    """Base class for transform functions"""
    
    @abstractmethod
    async def apply(self, data: Any) -> Any:
        """Apply transformation to data"""
        pass
        
    def validate(self, data: Any) -> bool:
        """Validate input data"""
        return True


class Pipeline:
    """
    Data processing pipeline.
    
    Represents a configured pipeline ready for execution.
    """
    
    def __init__(
        self,
        pipeline_id: str,
        name: str,
        stages: List[StageConfig],
        execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL
    ):
        self.pipeline_id = pipeline_id
        self.name = name
        self.stages = stages
        self.execution_mode = execution_mode
        self._stage_map = {stage.stage_id: stage for stage in stages}
        self._results: Dict[str, StageResult] = {}
        
    async def execute(self, input_data: Any = None) -> ProcessingResult:
        """Execute the pipeline"""
        result = ProcessingResult(
            job_id=f"pipeline_{self.pipeline_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            status=ProcessingStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        try:
            # Execute stages based on execution mode
            if self.execution_mode == ExecutionMode.SEQUENTIAL:
                await self._execute_sequential(input_data, result)
            elif self.execution_mode == ExecutionMode.PARALLEL:
                await self._execute_parallel(input_data, result)
            elif self.execution_mode == ExecutionMode.CONDITIONAL:
                await self._execute_conditional(input_data, result)
                
            # Aggregate results
            result.status = ProcessingStatus.COMPLETED
            result.records_processed = sum(r.output_records for r in self._results.values())
            result.metadata = {
                "stages_executed": len(self._results),
                "stages_succeeded": sum(1 for r in self._results.values() if r.success),
                "stages_failed": sum(1 for r in self._results.values() if not r.success),
                "stage_results": {
                    stage_id: {
                        "status": r.status.value,
                        "input_records": r.input_records,
                        "output_records": r.output_records,
                        "execution_time_ms": r.execution_time_ms
                    }
                    for stage_id, r in self._results.items()
                }
            }
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            result.status = ProcessingStatus.FAILED
            result.errors.append({"error": str(e), "type": type(e).__name__})
            
        finally:
            result.completed_at = datetime.utcnow()
            result.processing_time_ms = (result.completed_at - result.started_at).total_seconds() * 1000
            
        return result
        
    async def _execute_sequential(self, input_data: Any, result: ProcessingResult):
        """Execute stages sequentially"""
        current_data = input_data
        
        for stage in self.stages:
            # Check dependencies
            if not self._check_dependencies(stage):
                continue
                
            # Check condition
            if stage.condition and not await self._evaluate_condition(stage.condition, current_data):
                logger.info(f"Skipping stage {stage.stage_id} due to condition")
                continue
                
            # Execute stage
            stage_result = await self._execute_stage(stage, current_data)
            self._results[stage.stage_id] = stage_result
            
            if stage_result.success:
                current_data = stage_result.output_data
            else:
                if not stage.continue_on_error:
                    raise Exception(f"Stage {stage.stage_id} failed: {stage_result.error}")
                    
    async def _execute_parallel(self, input_data: Any, result: ProcessingResult):
        """Execute independent stages in parallel"""
        # Group stages by dependency level
        levels = self._group_by_dependency_level()
        
        current_data = input_data
        
        for level in sorted(levels.keys()):
            # Execute all stages at this level in parallel
            tasks = []
            for stage in levels[level]:
                if stage.condition and not await self._evaluate_condition(stage.condition, current_data):
                    continue
                    
                task = asyncio.create_task(self._execute_stage(stage, current_data))
                tasks.append((stage.stage_id, task))
                
            # Wait for all tasks to complete
            for stage_id, task in tasks:
                stage_result = await task
                self._results[stage_id] = stage_result
                
                if not stage_result.success and not self._stage_map[stage_id].continue_on_error:
                    raise Exception(f"Stage {stage_id} failed: {stage_result.error}")
                    
            # Merge results for next level
            # This is simplified - actual implementation would handle data merging
            if tasks:
                current_data = self._results[tasks[-1][0]].output_data
                
    async def _execute_conditional(self, input_data: Any, result: ProcessingResult):
        """Execute stages based on conditions"""
        # Similar to sequential but with more complex branching logic
        await self._execute_sequential(input_data, result)
        
    async def _execute_stage(self, stage: StageConfig, input_data: Any) -> StageResult:
        """Execute a single stage"""
        stage_result = StageResult(
            stage_id=stage.stage_id,
            status=ProcessingStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        retries = 0
        while retries <= stage.max_retries:
            try:
                # Execute based on stage type
                if stage.processor:
                    # Use processor
                    output = await stage.processor.process(input_data)
                    stage_result.output_data = output
                elif stage.function:
                    # Use function
                    if asyncio.iscoroutinefunction(stage.function):
                        output = await stage.function(input_data, **stage.parameters)
                    else:
                        output = stage.function(input_data, **stage.parameters)
                    stage_result.output_data = output
                else:
                    # Built-in stage types
                    output = await self._execute_builtin_stage(stage, input_data)
                    stage_result.output_data = output
                    
                # Success
                stage_result.status = ProcessingStatus.COMPLETED
                stage_result.output_records = self._count_records(output)
                break
                
            except Exception as e:
                retries += 1
                stage_result.retries = retries
                
                if retries > stage.max_retries:
                    stage_result.status = ProcessingStatus.FAILED
                    stage_result.error = str(e)
                    
                    if stage.on_error:
                        await stage.on_error(e, input_data)
                        
                    break
                else:
                    logger.warning(f"Stage {stage.stage_id} failed, retrying ({retries}/{stage.max_retries})")
                    await asyncio.sleep(2 ** retries)  # Exponential backoff
                    
        stage_result.completed_at = datetime.utcnow()
        stage_result.execution_time_ms = (stage_result.completed_at - stage_result.started_at).total_seconds() * 1000
        stage_result.input_records = self._count_records(input_data)
        
        return stage_result
        
    async def _execute_builtin_stage(self, stage: StageConfig, input_data: Any) -> Any:
        """Execute built-in stage types"""
        if stage.stage_type == StageType.FILTER:
            # Apply filter
            filter_func = stage.parameters.get("filter")
            if filter_func:
                if isinstance(input_data, list):
                    return [item for item in input_data if filter_func(item)]
                else:
                    return input_data if filter_func(input_data) else None
                    
        elif stage.stage_type == StageType.TRANSFORM:
            # Apply transformation
            transform_func = stage.parameters.get("transform")
            if transform_func:
                if isinstance(input_data, list):
                    return [transform_func(item) for item in input_data]
                else:
                    return transform_func(input_data)
                    
        elif stage.stage_type == StageType.AGGREGATE:
            # Apply aggregation
            agg_func = stage.parameters.get("aggregate")
            if agg_func and isinstance(input_data, list):
                return agg_func(input_data)
                
        return input_data
        
    def _check_dependencies(self, stage: StageConfig) -> bool:
        """Check if stage dependencies are satisfied"""
        for dep_id in stage.depends_on:
            if dep_id not in self._results:
                return False
            if not self._results[dep_id].success:
                return False
        return True
        
    async def _evaluate_condition(self, condition: Callable, data: Any) -> bool:
        """Evaluate stage condition"""
        try:
            if asyncio.iscoroutinefunction(condition):
                return await condition(data)
            else:
                return condition(data)
        except Exception as e:
            logger.error(f"Error evaluating condition: {e}")
            return False
            
    def _group_by_dependency_level(self) -> Dict[int, List[StageConfig]]:
        """Group stages by dependency level for parallel execution"""
        levels = {}
        visited = set()
        
        def get_level(stage_id: str) -> int:
            if stage_id in visited:
                return 0
                
            visited.add(stage_id)
            stage = self._stage_map.get(stage_id)
            
            if not stage or not stage.depends_on:
                return 0
                
            max_dep_level = 0
            for dep_id in stage.depends_on:
                dep_level = get_level(dep_id)
                max_dep_level = max(max_dep_level, dep_level)
                
            return max_dep_level + 1
            
        for stage in self.stages:
            level = get_level(stage.stage_id)
            if level not in levels:
                levels[level] = []
            levels[level].append(stage)
            
        return levels
        
    def _count_records(self, data: Any) -> int:
        """Count records in data"""
        if data is None:
            return 0
        elif isinstance(data, list):
            return len(data)
        elif hasattr(data, '__len__'):
            return len(data)
        else:
            return 1


class PipelineBuilder:
    """
    Fluent API for building data processing pipelines.
    
    Example:
        pipeline = (PipelineBuilder("my_pipeline")
            .source(file_source("input.csv"))
            .transform(lambda x: x.upper())
            .filter(lambda x: len(x) > 5)
            .quality_check(rules=[...])
            .sink(file_sink("output.csv"))
            .build())
            
        result = await pipeline.execute()
    """
    
    def __init__(self, name: str):
        self.name = name
        self.pipeline_id = str(uuid.uuid4())
        self.stages: List[StageConfig] = []
        self.execution_mode = ExecutionMode.SEQUENTIAL
        self._stage_counter = 0
        
    def _next_stage_id(self) -> str:
        """Generate next stage ID"""
        self._stage_counter += 1
        return f"stage_{self._stage_counter}"
        
    def source(
        self,
        source_func: Callable,
        name: Optional[str] = None,
        **kwargs
    ) -> 'PipelineBuilder':
        """Add a source stage"""
        stage = StageConfig(
            stage_id=self._next_stage_id(),
            name=name or "source",
            stage_type=StageType.SOURCE,
            function=source_func,
            parameters=kwargs
        )
        self.stages.append(stage)
        return self
        
    def transform(
        self,
        transform_func: Union[Callable, TransformFunction],
        name: Optional[str] = None,
        **kwargs
    ) -> 'PipelineBuilder':
        """Add a transform stage"""
        if isinstance(transform_func, TransformFunction):
            func = transform_func.apply
        else:
            func = transform_func
            
        stage = StageConfig(
            stage_id=self._next_stage_id(),
            name=name or "transform",
            stage_type=StageType.TRANSFORM,
            function=func,
            parameters=kwargs
        )
        self.stages.append(stage)
        return self
        
    def filter(
        self,
        filter_func: Callable[[Any], bool],
        name: Optional[str] = None,
        **kwargs
    ) -> 'PipelineBuilder':
        """Add a filter stage"""
        stage = StageConfig(
            stage_id=self._next_stage_id(),
            name=name or "filter",
            stage_type=StageType.FILTER,
            parameters={"filter": filter_func, **kwargs}
        )
        self.stages.append(stage)
        return self
        
    def aggregate(
        self,
        agg_func: Callable[[List[Any]], Any],
        name: Optional[str] = None,
        **kwargs
    ) -> 'PipelineBuilder':
        """Add an aggregation stage"""
        stage = StageConfig(
            stage_id=self._next_stage_id(),
            name=name or "aggregate",
            stage_type=StageType.AGGREGATE,
            parameters={"aggregate": agg_func, **kwargs}
        )
        self.stages.append(stage)
        return self
        
    def batch_process(
        self,
        processor: BatchProcessor,
        name: Optional[str] = None,
        **kwargs
    ) -> 'PipelineBuilder':
        """Add a batch processing stage"""
        stage = StageConfig(
            stage_id=self._next_stage_id(),
            name=name or "batch_process",
            stage_type=StageType.TRANSFORM,
            processor=processor,
            parameters=kwargs
        )
        self.stages.append(stage)
        return self
        
    def stream_process(
        self,
        processor: StreamProcessor,
        name: Optional[str] = None,
        **kwargs
    ) -> 'PipelineBuilder':
        """Add a stream processing stage"""
        stage = StageConfig(
            stage_id=self._next_stage_id(),
            name=name or "stream_process",
            stage_type=StageType.TRANSFORM,
            processor=processor,
            parameters=kwargs
        )
        self.stages.append(stage)
        return self
        
    def quality_check(
        self,
        processor: Optional[QualityProcessor] = None,
        rules: Optional[List[Any]] = None,
        name: Optional[str] = None,
        fail_on_error: bool = False,
        **kwargs
    ) -> 'PipelineBuilder':
        """Add a quality check stage"""
        if not processor and rules:
            # Create quality processor with rules
            from .quality_processor import QualityConfig, QualityProcessor
            config = QualityConfig(
                name="quality_check",
                rules=rules,
                fail_on_critical=fail_on_error
            )
            processor = QualityProcessor(config)
            
        stage = StageConfig(
            stage_id=self._next_stage_id(),
            name=name or "quality_check",
            stage_type=StageType.QUALITY,
            processor=processor,
            parameters=kwargs,
            continue_on_error=not fail_on_error
        )
        self.stages.append(stage)
        return self
        
    def branch(
        self,
        condition: Callable[[Any], bool],
        true_branch: 'PipelineBuilder',
        false_branch: Optional['PipelineBuilder'] = None,
        name: Optional[str] = None
    ) -> 'PipelineBuilder':
        """Add a conditional branch"""
        # This is simplified - actual implementation would handle branching
        stage = StageConfig(
            stage_id=self._next_stage_id(),
            name=name or "branch",
            stage_type=StageType.BRANCH,
            condition=condition,
            parameters={
                "true_branch": true_branch,
                "false_branch": false_branch
            }
        )
        self.stages.append(stage)
        return self
        
    def sink(
        self,
        sink_func: Callable,
        name: Optional[str] = None,
        **kwargs
    ) -> 'PipelineBuilder':
        """Add a sink stage"""
        stage = StageConfig(
            stage_id=self._next_stage_id(),
            name=name or "sink",
            stage_type=StageType.SINK,
            function=sink_func,
            parameters=kwargs
        )
        self.stages.append(stage)
        return self
        
    def custom(
        self,
        stage_func: Callable,
        name: Optional[str] = None,
        stage_type: StageType = StageType.CUSTOM,
        **kwargs
    ) -> 'PipelineBuilder':
        """Add a custom stage"""
        stage = StageConfig(
            stage_id=self._next_stage_id(),
            name=name or "custom",
            stage_type=stage_type,
            function=stage_func,
            parameters=kwargs
        )
        self.stages.append(stage)
        return self
        
    def parallel(self) -> 'PipelineBuilder':
        """Set execution mode to parallel"""
        self.execution_mode = ExecutionMode.PARALLEL
        return self
        
    def sequential(self) -> 'PipelineBuilder':
        """Set execution mode to sequential"""
        self.execution_mode = ExecutionMode.SEQUENTIAL
        return self
        
    def conditional(self) -> 'PipelineBuilder':
        """Set execution mode to conditional"""
        self.execution_mode = ExecutionMode.CONDITIONAL
        return self
        
    def depends_on(self, *stage_names: str) -> 'PipelineBuilder':
        """Set dependencies for the last added stage"""
        if self.stages:
            last_stage = self.stages[-1]
            # Find stage IDs by name
            for stage in self.stages[:-1]:
                if stage.name in stage_names:
                    last_stage.depends_on.append(stage.stage_id)
        return self
        
    def on_error(self, handler: Callable) -> 'PipelineBuilder':
        """Set error handler for the last added stage"""
        if self.stages:
            self.stages[-1].on_error = handler
        return self
        
    def with_retries(self, max_retries: int) -> 'PipelineBuilder':
        """Set max retries for the last added stage"""
        if self.stages:
            self.stages[-1].max_retries = max_retries
        return self
        
    def with_timeout(self, timeout: timedelta) -> 'PipelineBuilder':
        """Set timeout for the last added stage"""
        if self.stages:
            self.stages[-1].timeout = timeout
        return self
        
    def build(self) -> Pipeline:
        """Build the pipeline"""
        return Pipeline(
            pipeline_id=self.pipeline_id,
            name=self.name,
            stages=self.stages,
            execution_mode=self.execution_mode
        )


# Helper functions for common sources and sinks

def file_source(path: str, format: str = "csv") -> Callable:
    """Create a file source function"""
    async def read_file(data: Any = None, **kwargs):
        import pandas as pd
        if format == "csv":
            return pd.read_csv(path)
        elif format == "json":
            return pd.read_json(path)
        elif format == "parquet":
            return pd.read_parquet(path)
        else:
            raise ValueError(f"Unsupported format: {format}")
    return read_file


def file_sink(path: str, format: str = "csv") -> Callable:
    """Create a file sink function"""
    async def write_file(data: Any, **kwargs):
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            if format == "csv":
                data.to_csv(path, index=False)
            elif format == "json":
                data.to_json(path)
            elif format == "parquet":
                data.to_parquet(path)
            else:
                raise ValueError(f"Unsupported format: {format}")
        return data
    return write_file 