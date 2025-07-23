"""
Unified pipeline builder with fluent API.

Combines functionality from processing.pipeline_builder and orchestration patterns.
"""

import asyncio
from typing import Any, Dict, List, Optional, Callable, Union, TypeVar, Generic
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import uuid

from .base import (
    PipelineConfig,
    PipelineResult,
    StageConfig,
    StageResult,
    StageType,
    StageStatus,
    ExecutionMode,
    ProcessingEngine,
    TriggerType,
    ResourceConfig,
    RetryConfig
)
from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')


@dataclass
class TransformFunction(Generic[T]):
    """Transform function wrapper"""
    name: str
    func: Callable[[T], T]
    input_type: Optional[type] = None
    output_type: Optional[type] = None
    
    async def apply(self, data: T) -> T:
        """Apply transformation"""
        if asyncio.iscoroutinefunction(self.func):
            return await self.func(data)
        return self.func(data)


@dataclass
class FilterFunction:
    """Filter function wrapper"""
    name: str
    predicate: Callable[[Any], bool]
    
    async def apply(self, data: Any) -> bool:
        """Apply filter"""
        if asyncio.iscoroutinefunction(self.predicate):
            return await self.predicate(data)
        return self.predicate(data)


@dataclass
class AggregateFunction:
    """Aggregate function wrapper"""
    name: str
    func: Callable[[List[Any]], Any]
    window_size: Optional[int] = None
    
    async def apply(self, data: List[Any]) -> Any:
        """Apply aggregation"""
        if asyncio.iscoroutinefunction(self.func):
            return await self.func(data)
        return self.func(data)


class Pipeline:
    """
    Executable pipeline with unified functionality.
    
    Combines features from both pipeline implementations.
    """
    
    def __init__(
        self,
        config: PipelineConfig,
        stages: List[StageConfig],
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.config = config
        self.stages = stages
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # Build stage map
        self._stage_map = {stage.stage_id: stage for stage in stages}
        
        # Execution state
        self._current_run: Optional[PipelineResult] = None
        self._stage_outputs: Dict[str, Any] = {}
        
    async def execute(
        self,
        input_data: Any = None,
        parameters: Optional[Dict[str, Any]] = None,
        trigger_type: TriggerType = TriggerType.MANUAL,
        triggered_by: Optional[str] = None
    ) -> PipelineResult:
        """Execute the pipeline"""
        # Create run result
        result = PipelineResult(
            pipeline_id=self.config.pipeline_id,
            pipeline_name=self.config.name,
            execution_mode=self.config.execution_mode,
            trigger_type=trigger_type,
            triggered_by=triggered_by,
            parameters=parameters or {},
            started_at=datetime.utcnow()
        )
        
        self._current_run = result
        self._stage_outputs.clear()
        
        try:
            # Publish start event
            if self.event_bus:
                await self.event_bus.publish(Event(
                    event_type="pipeline.started",
                    source=f"pipeline:{self.config.pipeline_id}",
                    data={
                        "run_id": result.run_id,
                        "pipeline_name": self.config.name,
                        "trigger_type": trigger_type.value
                    }
                ))
                
            # Execute based on mode
            if self.config.execution_mode == ExecutionMode.SEQUENTIAL:
                await self._execute_sequential(input_data, result)
            elif self.config.execution_mode == ExecutionMode.PARALLEL:
                await self._execute_parallel(input_data, result)
            elif self.config.execution_mode == ExecutionMode.CONDITIONAL:
                await self._execute_conditional(input_data, result)
            elif self.config.execution_mode == ExecutionMode.STREAMING:
                await self._execute_streaming(input_data, result)
            else:
                # Default to sequential
                await self._execute_sequential(input_data, result)
                
            # Update final status
            if result.stages_failed == 0:
                result.status = StageStatus.COMPLETED
            else:
                result.status = StageStatus.FAILED
                
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            result.status = StageStatus.FAILED
            result.errors.append({
                "error": str(e),
                "type": type(e).__name__,
                "timestamp": datetime.utcnow().isoformat()
            })
            
        finally:
            result.completed_at = datetime.utcnow()
            
            # Calculate metrics
            result.total_records_processed = sum(
                sr.output_records for sr in result.stage_results.values()
            )
            
            # Cache result
            if self.cache:
                cache_key = f"pipeline_run:{result.run_id}"
                await self.cache.put(
                    "pipeline_runs",
                    cache_key,
                    result,
                    ttl=timedelta(hours=24)
                )
                
            # Publish completion event
            if self.event_bus:
                await self.event_bus.publish(Event(
                    event_type="pipeline.completed",
                    source=f"pipeline:{self.config.pipeline_id}",
                    data={
                        "run_id": result.run_id,
                        "status": result.status.value,
                        "duration_seconds": result.duration.total_seconds() if result.duration else 0
                    }
                ))
                
        return result
        
    async def _execute_sequential(self, input_data: Any, result: PipelineResult):
        """Execute stages sequentially"""
        current_data = input_data
        
        for stage in self.stages:
            # Check dependencies
            if not self._check_dependencies(stage):
                stage_result = StageResult(
                    stage_id=stage.stage_id,
                    stage_name=stage.name,
                    status=StageStatus.SKIPPED,
                    error="Dependencies not met"
                )
                result.stage_results[stage.stage_id] = stage_result
                result.stages_skipped += 1
                continue
                
            # Execute stage
            stage_result = await self._execute_stage(stage, current_data)
            result.stage_results[stage.stage_id] = stage_result
            
            if stage_result.success:
                current_data = stage_result.output_data
                self._stage_outputs[stage.stage_id] = current_data
                result.stages_completed += 1
            else:
                result.stages_failed += 1
                if self.config.fail_fast and not stage.continue_on_error:
                    raise Exception(f"Stage {stage.name} failed: {stage_result.error}")
                    
        result.final_output = current_data
        
    async def _execute_parallel(self, input_data: Any, result: PipelineResult):
        """Execute independent stages in parallel"""
        # Group stages by dependency level
        levels = self._group_by_dependency_level()
        
        current_data = input_data
        
        for level in sorted(levels.keys()):
            # Execute all stages at this level in parallel
            tasks = []
            for stage in levels[level]:
                if self._check_dependencies(stage):
                    # Get input data from dependencies or use current
                    stage_input = self._get_stage_input(stage, current_data)
                    task = asyncio.create_task(
                        self._execute_stage(stage, stage_input)
                    )
                    tasks.append((stage.stage_id, task))
                else:
                    # Skip stage
                    stage_result = StageResult(
                        stage_id=stage.stage_id,
                        stage_name=stage.name,
                        status=StageStatus.SKIPPED,
                        error="Dependencies not met"
                    )
                    result.stage_results[stage.stage_id] = stage_result
                    result.stages_skipped += 1
                    
            # Wait for all tasks to complete
            if tasks:
                # Limit parallelism
                if len(tasks) > self.config.max_parallelism:
                    # Execute in batches
                    for i in range(0, len(tasks), self.config.max_parallelism):
                        batch = tasks[i:i + self.config.max_parallelism]
                        await self._execute_parallel_batch(batch, result)
                else:
                    await self._execute_parallel_batch(tasks, result)
                    
            # Update current data with last stage output
            if level in levels and levels[level]:
                last_stage = levels[level][-1]
                if last_stage.stage_id in self._stage_outputs:
                    current_data = self._stage_outputs[last_stage.stage_id]
                    
        result.final_output = current_data
        
    async def _execute_parallel_batch(
        self,
        tasks: List[tuple[str, asyncio.Task]],
        result: PipelineResult
    ):
        """Execute a batch of parallel tasks"""
        for stage_id, task in tasks:
            try:
                stage_result = await task
                result.stage_results[stage_id] = stage_result
                
                if stage_result.success:
                    self._stage_outputs[stage_id] = stage_result.output_data
                    result.stages_completed += 1
                else:
                    result.stages_failed += 1
                    stage = self._stage_map[stage_id]
                    if self.config.fail_fast and not stage.continue_on_error:
                        # Cancel remaining tasks
                        for _, t in tasks:
                            if not t.done():
                                t.cancel()
                        raise Exception(f"Stage {stage.name} failed: {stage_result.error}")
                        
            except Exception as e:
                logger.error(f"Error in parallel execution: {e}")
                raise
                
    async def _execute_conditional(self, input_data: Any, result: PipelineResult):
        """Execute stages based on conditions"""
        # Similar to sequential but evaluates conditions
        current_data = input_data
        
        for stage in self.stages:
            # Check condition
            if stage.condition:
                try:
                    should_execute = stage.condition(current_data)
                    if asyncio.iscoroutine(should_execute):
                        should_execute = await should_execute
                        
                    if not should_execute:
                        stage_result = StageResult(
                            stage_id=stage.stage_id,
                            stage_name=stage.name,
                            status=StageStatus.SKIPPED,
                            error="Condition not met"
                        )
                        result.stage_results[stage.stage_id] = stage_result
                        result.stages_skipped += 1
                        continue
                        
                except Exception as e:
                    logger.error(f"Error evaluating condition for stage {stage.name}: {e}")
                    stage_result = StageResult(
                        stage_id=stage.stage_id,
                        stage_name=stage.name,
                        status=StageStatus.FAILED,
                        error=f"Condition evaluation failed: {e}"
                    )
                    result.stage_results[stage.stage_id] = stage_result
                    result.stages_failed += 1
                    continue
                    
            # Execute stage
            stage_result = await self._execute_stage(stage, current_data)
            result.stage_results[stage.stage_id] = stage_result
            
            if stage_result.success:
                current_data = stage_result.output_data
                self._stage_outputs[stage.stage_id] = current_data
                result.stages_completed += 1
            else:
                result.stages_failed += 1
                if self.config.fail_fast and not stage.continue_on_error:
                    raise Exception(f"Stage {stage.name} failed: {stage_result.error}")
                    
        result.final_output = current_data
        
    async def _execute_streaming(self, input_data: Any, result: PipelineResult):
        """Execute pipeline in streaming mode"""
        # This would integrate with stream processing
        # For now, fallback to sequential
        await self._execute_sequential(input_data, result)
        
    async def _execute_stage(self, stage: StageConfig, input_data: Any) -> StageResult:
        """Execute a single stage with retry logic"""
        stage_result = StageResult(
            stage_id=stage.stage_id,
            stage_name=stage.name,
            status=StageStatus.RUNNING,
            started_at=datetime.utcnow(),
            input_data=input_data
        )
        
        retry_count = 0
        last_error = None
        
        while retry_count <= stage.retry_config.max_retries:
            try:
                # Record input records
                stage_result.input_records = self._count_records(input_data)
                
                # Execute based on processor or function
                if stage.processor:
                    # Use processor instance
                    output = await stage.processor.process(input_data)
                elif stage.function:
                    # Use function
                    if asyncio.iscoroutinefunction(stage.function):
                        output = await stage.function(input_data, **stage.parameters)
                    else:
                        output = stage.function(input_data, **stage.parameters)
                else:
                    # Built-in stage type
                    output = await self._execute_builtin_stage(stage, input_data)
                    
                # Success
                stage_result.output_data = output
                stage_result.output_records = self._count_records(output)
                stage_result.status = StageStatus.COMPLETED
                stage_result.completed_at = datetime.utcnow()
                stage_result.execution_time_ms = (
                    stage_result.completed_at - stage_result.started_at
                ).total_seconds() * 1000
                
                # Publish stage completion event
                if self.event_bus:
                    await self.event_bus.publish(Event(
                        event_type="pipeline.stage.completed",
                        source=f"pipeline:{self.config.pipeline_id}",
                        data={
                            "run_id": self._current_run.run_id if self._current_run else None,
                            "stage_id": stage.stage_id,
                            "stage_name": stage.name,
                            "status": "completed",
                            "records_processed": stage_result.output_records
                        }
                    ))
                    
                return stage_result
                
            except Exception as e:
                last_error = e
                retry_count += 1
                stage_result.retry_count = retry_count
                
                # Check if should retry
                should_retry = (
                    retry_count <= stage.retry_config.max_retries and
                    not any(isinstance(e, skip_type) for skip_type in stage.retry_config.skip_on_errors)
                )
                
                if should_retry:
                    # Calculate retry delay
                    delay = stage.retry_config.retry_delay.total_seconds()
                    if stage.retry_config.exponential_backoff:
                        delay *= (stage.retry_config.backoff_factor ** (retry_count - 1))
                        delay = min(delay, stage.retry_config.max_retry_delay.total_seconds())
                        
                    logger.warning(
                        f"Stage {stage.name} failed (attempt {retry_count}), "
                        f"retrying in {delay}s: {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    # Final failure
                    break
                    
        # Stage failed after all retries
        stage_result.status = StageStatus.FAILED
        stage_result.completed_at = datetime.utcnow()
        stage_result.execution_time_ms = (
            stage_result.completed_at - stage_result.started_at
        ).total_seconds() * 1000
        stage_result.error = str(last_error)
        stage_result.error_type = type(last_error).__name__ if last_error else None
        
        # Publish stage failure event
        if self.event_bus:
            await self.event_bus.publish(Event(
                event_type="pipeline.stage.failed",
                source=f"pipeline:{self.config.pipeline_id}",
                data={
                    "run_id": self._current_run.run_id if self._current_run else None,
                    "stage_id": stage.stage_id,
                    "stage_name": stage.name,
                    "error": str(last_error),
                    "retry_count": retry_count
                }
            ))
            
        return stage_result
        
    async def _execute_builtin_stage(self, stage: StageConfig, input_data: Any) -> Any:
        """Execute built-in stage types"""
        if stage.stage_type == StageType.TRANSFORM:
            # Simple pass-through for now
            return input_data
        elif stage.stage_type == StageType.FILTER:
            # Filter records
            if isinstance(input_data, list):
                return [item for item in input_data if stage.parameters.get("predicate", lambda x: True)(item)]
            return input_data
        elif stage.stage_type == StageType.AGGREGATE:
            # Simple aggregation
            if isinstance(input_data, list) and stage.parameters.get("func"):
                return stage.parameters["func"](input_data)
            return input_data
        else:
            # Default pass-through
            return input_data
            
    def _check_dependencies(self, stage: StageConfig) -> bool:
        """Check if stage dependencies are satisfied"""
        if not stage.depends_on:
            return True
            
        for dep_id in stage.depends_on:
            if dep_id not in self._stage_outputs:
                return False
                
            # Check if dependency succeeded
            if self._current_run and dep_id in self._current_run.stage_results:
                dep_result = self._current_run.stage_results[dep_id]
                if not dep_result.success:
                    return False
                    
        return True
        
    def _get_stage_input(self, stage: StageConfig, default_input: Any) -> Any:
        """Get input data for stage based on dependencies"""
        if not stage.depends_on:
            return default_input
            
        # If single dependency, use its output
        if len(stage.depends_on) == 1:
            dep_id = stage.depends_on[0]
            return self._stage_outputs.get(dep_id, default_input)
            
        # Multiple dependencies - return dict of outputs
        inputs = {}
        for dep_id in stage.depends_on:
            if dep_id in self._stage_outputs:
                inputs[dep_id] = self._stage_outputs[dep_id]
                
        return inputs if inputs else default_input
        
    def _group_by_dependency_level(self) -> Dict[int, List[StageConfig]]:
        """Group stages by dependency level for parallel execution"""
        levels = {}
        processed = set()
        
        # Find stages with no dependencies (level 0)
        level = 0
        for stage in self.stages:
            if not stage.depends_on:
                if level not in levels:
                    levels[level] = []
                levels[level].append(stage)
                processed.add(stage.stage_id)
                
        # Process remaining stages
        while len(processed) < len(self.stages):
            level += 1
            current_level = []
            
            for stage in self.stages:
                if stage.stage_id in processed:
                    continue
                    
                # Check if all dependencies are processed
                if all(dep_id in processed for dep_id in stage.depends_on):
                    current_level.append(stage)
                    processed.add(stage.stage_id)
                    
            if current_level:
                levels[level] = current_level
            else:
                # Circular dependency or unresolvable
                break
                
        return levels
        
    def _count_records(self, data: Any) -> int:
        """Count records in data"""
        if isinstance(data, list):
            return len(data)
        elif isinstance(data, dict):
            return len(data)
        elif hasattr(data, '__len__'):
            return len(data)
        elif data is None:
            return 0
        else:
            return 1


class StageBuilder:
    """Builder for individual stages"""
    
    def __init__(self, name: str, stage_type: StageType = StageType.CUSTOM):
        self.config = StageConfig(
            name=name,
            stage_type=stage_type
        )
        
    def with_function(self, func: Callable, **params) -> 'StageBuilder':
        """Set stage function"""
        self.config.function = func
        self.config.parameters.update(params)
        return self
        
    def with_processor(self, processor: Any) -> 'StageBuilder':
        """Set stage processor"""
        self.config.processor = processor
        return self
        
    def depends_on(self, *stage_names: str) -> 'StageBuilder':
        """Set dependencies"""
        self.config.depends_on.extend(stage_names)
        return self
        
    def with_condition(self, condition: Callable[[Any], bool]) -> 'StageBuilder':
        """Set execution condition"""
        self.config.condition = condition
        return self
        
    def with_resources(
        self,
        cpu_cores: Optional[float] = None,
        memory_mb: Optional[int] = None,
        **kwargs
    ) -> 'StageBuilder':
        """Set resource requirements"""
        if cpu_cores:
            self.config.resources.cpu_cores = cpu_cores
        if memory_mb:
            self.config.resources.memory_mb = memory_mb
        for key, value in kwargs.items():
            setattr(self.config.resources, key, value)
        return self
        
    def with_retry(
        self,
        max_retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=30),
        exponential_backoff: bool = True
    ) -> 'StageBuilder':
        """Configure retry behavior"""
        self.config.retry_config.max_retries = max_retries
        self.config.retry_config.retry_delay = retry_delay
        self.config.retry_config.exponential_backoff = exponential_backoff
        return self
        
    def continue_on_error(self) -> 'StageBuilder':
        """Continue pipeline on stage error"""
        self.config.continue_on_error = True
        return self
        
    def with_timeout(self, timeout: timedelta) -> 'StageBuilder':
        """Set stage timeout"""
        self.config.timeout = timeout
        return self
        
    def with_engine(self, engine: ProcessingEngine) -> 'StageBuilder':
        """Set processing engine"""
        self.config.engine = engine
        return self
        
    def build(self) -> StageConfig:
        """Build stage configuration"""
        return self.config


class PipelineBuilder:
    """
    Fluent API for building pipelines.
    
    Example:
        pipeline = (PipelineBuilder("my_pipeline")
            .source(file_source("input.csv"))
            .transform(lambda x: x.upper())
            .filter(lambda x: len(x) > 5)
            .quality_check(rules=[...])
            .sink(file_sink("output.csv"))
            .parallel()
            .build())
            
        result = await pipeline.execute()
    """
    
    def __init__(self, name: str, version: str = "1.0.0"):
        self.config = PipelineConfig(
            name=name,
            version=version
        )
        self.stages: List[StageConfig] = []
        self._stage_counter = 0
        
    def _next_stage_name(self, prefix: str) -> str:
        """Generate next stage name"""
        self._stage_counter += 1
        return f"{prefix}_{self._stage_counter}"
        
    # Data operations
    
    def source(
        self,
        source_func: Callable,
        name: Optional[str] = None,
        **kwargs
    ) -> 'PipelineBuilder':
        """Add a source stage"""
        stage = StageConfig(
            name=name or self._next_stage_name("source"),
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
            name=name or self._next_stage_name("transform"),
            stage_type=StageType.TRANSFORM,
            function=func,
            parameters=kwargs
        )
        self.stages.append(stage)
        return self
        
    def filter(
        self,
        predicate: Union[Callable[[Any], bool], FilterFunction],
        name: Optional[str] = None
    ) -> 'PipelineBuilder':
        """Add a filter stage"""
        if isinstance(predicate, FilterFunction):
            func = predicate.apply
        else:
            func = predicate
            
        stage = StageConfig(
            name=name or self._next_stage_name("filter"),
            stage_type=StageType.FILTER,
            function=lambda data: [item for item in data if func(item)] if isinstance(data, list) else data,
            parameters={"predicate": func}
        )
        self.stages.append(stage)
        return self
        
    def aggregate(
        self,
        agg_func: Union[Callable, AggregateFunction],
        name: Optional[str] = None,
        **kwargs
    ) -> 'PipelineBuilder':
        """Add an aggregate stage"""
        if isinstance(agg_func, AggregateFunction):
            func = agg_func.apply
        else:
            func = agg_func
            
        stage = StageConfig(
            name=name or self._next_stage_name("aggregate"),
            stage_type=StageType.AGGREGATE,
            function=func,
            parameters=kwargs
        )
        self.stages.append(stage)
        return self
        
    def join(
        self,
        right_source: Callable,
        join_func: Callable,
        name: Optional[str] = None,
        **kwargs
    ) -> 'PipelineBuilder':
        """Add a join stage"""
        stage = StageConfig(
            name=name or self._next_stage_name("join"),
            stage_type=StageType.JOIN,
            function=join_func,
            parameters={"right_source": right_source, **kwargs}
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
            name=name or self._next_stage_name("sink"),
            stage_type=StageType.SINK,
            function=sink_func,
            parameters=kwargs
        )
        self.stages.append(stage)
        return self
        
    # Quality operations
    
    def quality_check(
        self,
        quality_func: Callable,
        name: Optional[str] = None,
        fail_on_error: bool = True,
        **kwargs
    ) -> 'PipelineBuilder':
        """Add a quality check stage"""
        stage = StageConfig(
            name=name or self._next_stage_name("quality"),
            stage_type=StageType.QUALITY,
            function=quality_func,
            continue_on_error=not fail_on_error,
            parameters=kwargs
        )
        self.stages.append(stage)
        return self
        
    def validate(
        self,
        validation_func: Callable,
        name: Optional[str] = None,
        **kwargs
    ) -> 'PipelineBuilder':
        """Add a validation stage"""
        stage = StageConfig(
            name=name or self._next_stage_name("validate"),
            stage_type=StageType.VALIDATE,
            function=validation_func,
            parameters=kwargs
        )
        self.stages.append(stage)
        return self
        
    # Control flow
    
    def branch(
        self,
        condition: Callable[[Any], bool],
        true_branch: 'PipelineBuilder',
        false_branch: Optional['PipelineBuilder'] = None,
        name: Optional[str] = None
    ) -> 'PipelineBuilder':
        """Add conditional branching"""
        # This would require more complex implementation
        # For now, add as custom stage
        stage = StageConfig(
            name=name or self._next_stage_name("branch"),
            stage_type=StageType.BRANCH,
            parameters={
                "condition": condition,
                "true_branch": true_branch,
                "false_branch": false_branch
            }
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
            name=name or self._next_stage_name("custom"),
            stage_type=stage_type,
            function=stage_func,
            parameters=kwargs
        )
        self.stages.append(stage)
        return self
        
    def stage(self, stage_config: StageConfig) -> 'PipelineBuilder':
        """Add a pre-configured stage"""
        self.stages.append(stage_config)
        return self
        
    # Execution modes
    
    def sequential(self) -> 'PipelineBuilder':
        """Set execution mode to sequential"""
        self.config.execution_mode = ExecutionMode.SEQUENTIAL
        return self
        
    def parallel(self, max_parallelism: Optional[int] = None) -> 'PipelineBuilder':
        """Set execution mode to parallel"""
        self.config.execution_mode = ExecutionMode.PARALLEL
        if max_parallelism:
            self.config.max_parallelism = max_parallelism
        return self
        
    def conditional(self) -> 'PipelineBuilder':
        """Set execution mode to conditional"""
        self.config.execution_mode = ExecutionMode.CONDITIONAL
        return self
        
    def streaming(self) -> 'PipelineBuilder':
        """Set execution mode to streaming"""
        self.config.execution_mode = ExecutionMode.STREAMING
        return self
        
    # Dependencies
    
    def depends_on(self, *stage_names: str) -> 'PipelineBuilder':
        """Set dependencies for the last added stage"""
        if self.stages:
            last_stage = self.stages[-1]
            # Find stage IDs by name
            for stage in self.stages[:-1]:
                if stage.name in stage_names:
                    last_stage.depends_on.append(stage.stage_id)
        return self
        
    # Configuration
    
    def with_engine(self, engine: ProcessingEngine) -> 'PipelineBuilder':
        """Set default processing engine"""
        self.config.default_engine = engine
        return self
        
    def with_resources(
        self,
        cpu_cores: Optional[float] = None,
        memory_mb: Optional[int] = None,
        **kwargs
    ) -> 'PipelineBuilder':
        """Set resource limits"""
        if cpu_cores:
            self.config.resource_limits.cpu_cores = cpu_cores
        if memory_mb:
            self.config.resource_limits.memory_mb = memory_mb
        for key, value in kwargs.items():
            setattr(self.config.resource_limits, key, value)
        return self
        
    def with_schedule(
        self,
        cron: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> 'PipelineBuilder':
        """Set pipeline schedule"""
        self.config.schedule = cron
        self.config.start_date = start_date
        self.config.end_date = end_date
        return self
        
    def fail_fast(self, enabled: bool = True) -> 'PipelineBuilder':
        """Set fail-fast behavior"""
        self.config.fail_fast = enabled
        return self
        
    def with_checkpoint(self, interval: timedelta) -> 'PipelineBuilder':
        """Enable checkpointing"""
        self.config.checkpoint_interval = interval
        return self
        
    def with_metadata(self, **metadata) -> 'PipelineBuilder':
        """Add metadata"""
        self.config.metadata.update(metadata)
        return self
        
    def with_tags(self, *tags: str) -> 'PipelineBuilder':
        """Add tags"""
        self.config.tags.extend(tags)
        return self
        
    def owned_by(self, owner: str) -> 'PipelineBuilder':
        """Set pipeline owner"""
        self.config.owner = owner
        return self
        
    def describe(self, description: str) -> 'PipelineBuilder':
        """Set pipeline description"""
        self.config.description = description
        return self
        
    # Build
    
    def build(
        self,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ) -> Pipeline:
        """Build the pipeline"""
        # Validate pipeline
        self._validate()
        
        # Create pipeline
        return Pipeline(
            config=self.config,
            stages=self.stages,
            cache_manager=cache_manager,
            event_bus=event_bus
        )
        
    def _validate(self):
        """Validate pipeline configuration"""
        if not self.stages:
            raise ValueError("Pipeline has no stages")
            
        # Check for duplicate stage names
        stage_names = [s.name for s in self.stages]
        if len(stage_names) != len(set(stage_names)):
            raise ValueError("Duplicate stage names found")
            
        # Validate dependencies
        stage_ids = {s.stage_id for s in self.stages}
        stage_names_set = set(stage_names)
        
        for stage in self.stages:
            for dep in stage.depends_on:
                # Check if dependency is ID or name
                if dep not in stage_ids and dep not in stage_names_set:
                    raise ValueError(f"Stage {stage.name} depends on unknown stage: {dep}")
                    
        # Check for circular dependencies
        if self._has_circular_dependencies():
            raise ValueError("Circular dependencies detected in pipeline")
            
    def _has_circular_dependencies(self) -> bool:
        """Check for circular dependencies"""
        # Build adjacency list
        graph = {}
        for stage in self.stages:
            graph[stage.stage_id] = stage.depends_on
            
        # DFS to detect cycles
        visited = set()
        rec_stack = set()
        
        def has_cycle(node):
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
                    
            rec_stack.remove(node)
            return False
            
        for stage in self.stages:
            if stage.stage_id not in visited:
                if has_cycle(stage.stage_id):
                    return True
                    
        return False 