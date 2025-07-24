"""
Pipeline Orchestrator

Provides advanced orchestration capabilities for pipeline execution.
"""

import asyncio
from typing import Dict, List, Any, Optional, Callable, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import uuid
from collections import defaultdict
import json

from .base import Pipeline, PipelineStage, PipelineResult, StageResult, StageStatus
from .builder import PipelineBuilder
from ..events import EventBus, Event
from ..caching import CacheManager
from ...monitoring import StructuredLogger, MetricsCollector

logger = StructuredLogger.get_logger(__name__)


class OrchestrationStrategy(str, Enum):
    """Pipeline orchestration strategies"""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    DAG = "dag"
    CONDITIONAL = "conditional"
    DYNAMIC = "dynamic"


class ResourceAllocation(str, Enum):
    """Resource allocation strategies"""
    STATIC = "static"
    DYNAMIC = "dynamic"
    PRIORITY_BASED = "priority"
    FAIR_SHARE = "fair_share"


@dataclass
class OrchestrationConfig:
    """Configuration for pipeline orchestration"""
    strategy: OrchestrationStrategy = OrchestrationStrategy.DAG
    max_concurrent_stages: int = 10
    max_concurrent_pipelines: int = 5
    stage_timeout: timedelta = timedelta(minutes=30)
    pipeline_timeout: timedelta = timedelta(hours=2)
    
    # Resource management
    resource_allocation: ResourceAllocation = ResourceAllocation.DYNAMIC
    max_memory_gb: float = 16.0
    max_cpu_cores: int = 8
    
    # Retry configuration
    max_retries: int = 3
    retry_delay: timedelta = timedelta(seconds=30)
    exponential_backoff: bool = True
    
    # Monitoring
    enable_metrics: bool = True
    enable_tracing: bool = True
    checkpoint_interval: timedelta = timedelta(minutes=5)
    
    # Optimization
    enable_caching: bool = True
    enable_parallelization: bool = True
    optimize_execution_plan: bool = True


@dataclass
class ExecutionPlan:
    """Execution plan for pipeline"""
    pipeline_id: str
    stages: List[PipelineStage]
    dependencies: Dict[str, Set[str]]
    execution_order: List[List[str]]  # Stages that can run in parallel
    estimated_duration: timedelta
    resource_requirements: Dict[str, Any]
    optimization_notes: List[str] = field(default_factory=list)


class PipelineOrchestrator:
    """
    Advanced pipeline orchestrator with optimization and resource management.
    """
    
    def __init__(
        self,
        config: OrchestrationConfig,
        event_bus: Optional[EventBus] = None,
        cache_manager: Optional[CacheManager] = None,
        metrics: Optional[MetricsCollector] = None
    ):
        self.config = config
        self.event_bus = event_bus
        self.cache = cache_manager
        self.metrics = metrics or MetricsCollector()
        
        # Execution state
        self._running_pipelines: Dict[str, Pipeline] = {}
        self._execution_plans: Dict[str, ExecutionPlan] = {}
        self._stage_executors: Dict[str, asyncio.Task] = {}
        self._resource_usage: Dict[str, float] = {
            "memory_gb": 0.0,
            "cpu_cores": 0
        }
        
        # Performance tracking
        self._stage_metrics: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self._optimization_history: List[Dict[str, Any]] = []
        
    async def execute_pipeline(
        self,
        pipeline: Pipeline,
        input_data: Any = None,
        parameters: Optional[Dict[str, Any]] = None
    ) -> PipelineResult:
        """
        Execute pipeline with orchestration and optimization.
        """
        pipeline_id = str(uuid.uuid4())
        
        # Check resource availability
        if not await self._check_resources(pipeline):
            raise RuntimeError("Insufficient resources to execute pipeline")
            
        # Create execution plan
        plan = await self._create_execution_plan(pipeline)
        self._execution_plans[pipeline_id] = plan
        
        # Start execution
        self._running_pipelines[pipeline_id] = pipeline
        
        try:
            # Execute based on strategy
            if self.config.strategy == OrchestrationStrategy.SEQUENTIAL:
                result = await self._execute_sequential(
                    pipeline, plan, input_data, parameters
                )
            elif self.config.strategy == OrchestrationStrategy.PARALLEL:
                result = await self._execute_parallel(
                    pipeline, plan, input_data, parameters
                )
            elif self.config.strategy == OrchestrationStrategy.DAG:
                result = await self._execute_dag(
                    pipeline, plan, input_data, parameters
                )
            else:
                result = await pipeline.execute(input_data, parameters)
                
            # Record metrics
            await self._record_execution_metrics(pipeline_id, result)
            
            return result
            
        finally:
            # Cleanup
            self._running_pipelines.pop(pipeline_id, None)
            self._execution_plans.pop(pipeline_id, None)
            await self._release_resources(pipeline)
            
    async def _create_execution_plan(self, pipeline: Pipeline) -> ExecutionPlan:
        """Create optimized execution plan"""
        stages = pipeline.stages
        dependencies = await self._analyze_dependencies(stages)
        
        # Topological sort for execution order
        execution_order = self._topological_sort(stages, dependencies)
        
        # Estimate resource requirements
        resource_requirements = await self._estimate_resources(stages)
        
        # Optimization suggestions
        optimization_notes = []
        if self.config.optimize_execution_plan:
            optimization_notes = await self._optimize_plan(
                stages, dependencies, execution_order
            )
            
        plan = ExecutionPlan(
            pipeline_id=pipeline.config.name,
            stages=stages,
            dependencies=dependencies,
            execution_order=execution_order,
            estimated_duration=timedelta(minutes=len(stages) * 5),  # Simple estimate
            resource_requirements=resource_requirements,
            optimization_notes=optimization_notes
        )
        
        logger.info(
            "Created execution plan",
            pipeline=pipeline.config.name,
            stages=len(stages),
            parallel_groups=len(execution_order),
            optimizations=len(optimization_notes)
        )
        
        return plan
        
    async def _analyze_dependencies(
        self,
        stages: List[PipelineStage]
    ) -> Dict[str, Set[str]]:
        """Analyze stage dependencies"""
        dependencies = {}
        
        for stage in stages:
            stage_deps = set()
            
            # Explicit dependencies
            if hasattr(stage, 'depends_on'):
                stage_deps.update(stage.depends_on)
                
            # Implicit dependencies based on input/output
            for other_stage in stages:
                if other_stage.stage_id == stage.stage_id:
                    continue
                    
                # Check if this stage uses output from other stage
                if hasattr(stage, 'input_mapping') and hasattr(other_stage, 'output_schema'):
                    if self._check_data_dependency(
                        stage.input_mapping,
                        other_stage.output_schema
                    ):
                        stage_deps.add(other_stage.stage_id)
                        
            dependencies[stage.stage_id] = stage_deps
            
        return dependencies
        
    def _topological_sort(
        self,
        stages: List[PipelineStage],
        dependencies: Dict[str, Set[str]]
    ) -> List[List[str]]:
        """Perform topological sort to determine execution order"""
        # Build adjacency list
        graph = defaultdict(set)
        in_degree = defaultdict(int)
        
        stage_ids = [s.stage_id for s in stages]
        for stage_id in stage_ids:
            in_degree[stage_id] = len(dependencies.get(stage_id, set()))
            for dep in dependencies.get(stage_id, set()):
                graph[dep].add(stage_id)
                
        # Find stages that can run in parallel
        execution_order = []
        queue = [s for s in stage_ids if in_degree[s] == 0]
        
        while queue:
            # All stages in current queue can run in parallel
            parallel_group = queue[:]
            execution_order.append(parallel_group)
            
            # Process next level
            next_queue = []
            for stage_id in parallel_group:
                for dependent in graph[stage_id]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        next_queue.append(dependent)
                        
            queue = next_queue
            
        return execution_order
        
    async def _execute_dag(
        self,
        pipeline: Pipeline,
        plan: ExecutionPlan,
        input_data: Any,
        parameters: Optional[Dict[str, Any]]
    ) -> PipelineResult:
        """Execute pipeline as DAG with parallel stage execution"""
        result = PipelineResult(
            pipeline_id=pipeline.config.name,
            status=StageStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        stage_outputs = {}
        stage_map = {s.stage_id: s for s in plan.stages}
        
        try:
            for parallel_group in plan.execution_order:
                # Execute stages in parallel
                tasks = []
                
                for stage_id in parallel_group:
                    stage = stage_map[stage_id]
                    
                    # Prepare stage input
                    stage_input = await self._prepare_stage_input(
                        stage, input_data, stage_outputs, parameters
                    )
                    
                    # Create execution task
                    task = asyncio.create_task(
                        self._execute_stage_with_monitoring(
                            stage, stage_input, result
                        )
                    )
                    tasks.append((stage_id, task))
                    
                # Wait for all stages in group to complete
                for stage_id, task in tasks:
                    try:
                        stage_result = await task
                        stage_outputs[stage_id] = stage_result.output_data
                        result.stage_results[stage_id] = stage_result
                        
                    except Exception as e:
                        logger.error(
                            "Stage execution failed",
                            stage=stage_id,
                            error=str(e)
                        )
                        result.stage_results[stage_id] = StageResult(
                            stage_id=stage_id,
                            stage_name=stage_map[stage_id].name,
                            status=StageStatus.FAILED,
                            error=str(e),
                            started_at=datetime.utcnow(),
                            completed_at=datetime.utcnow()
                        )
                        
                        # Check if stage is critical
                        if stage_map[stage_id].config.get('critical', True):
                            raise
                            
            result.status = StageStatus.COMPLETED
            result.output_data = self._merge_outputs(stage_outputs)
            
        except Exception as e:
            result.status = StageStatus.FAILED
            result.error = str(e)
            raise
            
        finally:
            result.completed_at = datetime.utcnow()
            
        return result
        
    async def _execute_stage_with_monitoring(
        self,
        stage: PipelineStage,
        input_data: Any,
        pipeline_result: PipelineResult
    ) -> StageResult:
        """Execute stage with monitoring and optimization"""
        stage_id = stage.stage_id
        start_time = datetime.utcnow()
        
        # Record start metrics
        if self.metrics:
            self.metrics.increment(
                "pipeline_stage_started",
                labels={"stage": stage.name}
            )
            
        try:
            # Check cache
            if self.config.enable_caching and self.cache:
                cache_key = self._generate_cache_key(stage, input_data)
                cached_result = await self.cache.get(cache_key)
                if cached_result:
                    logger.info(f"Using cached result for stage {stage.name}")
                    return cached_result
                    
            # Execute stage
            if hasattr(stage, 'execute'):
                result = await stage.execute(input_data)
            else:
                # Use stage transform function
                output = await stage.transform(input_data)
                result = StageResult(
                    stage_id=stage_id,
                    stage_name=stage.name,
                    status=StageStatus.COMPLETED,
                    output_data=output,
                    started_at=start_time,
                    completed_at=datetime.utcnow()
                )
                
            # Cache result
            if self.config.enable_caching and self.cache and result.status == StageStatus.COMPLETED:
                await self.cache.set(
                    cache_key,
                    result,
                    ttl=3600  # 1 hour
                )
                
            # Record metrics
            duration = (datetime.utcnow() - start_time).total_seconds()
            self._stage_metrics[stage_id] = {
                "duration_seconds": duration,
                "status": result.status,
                "timestamp": datetime.utcnow()
            }
            
            if self.metrics:
                self.metrics.record_histogram(
                    "pipeline_stage_duration",
                    duration,
                    labels={"stage": stage.name}
                )
                
            return result
            
        except Exception as e:
            # Record failure metrics
            if self.metrics:
                self.metrics.increment(
                    "pipeline_stage_failed",
                    labels={"stage": stage.name, "error": type(e).__name__}
                )
            raise
            
    async def _prepare_stage_input(
        self,
        stage: PipelineStage,
        initial_input: Any,
        stage_outputs: Dict[str, Any],
        parameters: Optional[Dict[str, Any]]
    ) -> Any:
        """Prepare input for stage execution"""
        # Start with initial input
        stage_input = initial_input
        
        # Apply input mapping if defined
        if hasattr(stage, 'input_mapping'):
            mapped_input = {}
            
            for key, source in stage.input_mapping.items():
                if source.startswith('$'):
                    # Reference to another stage output
                    stage_ref = source[1:]
                    if stage_ref in stage_outputs:
                        mapped_input[key] = stage_outputs[stage_ref]
                elif source.startswith('@'):
                    # Reference to parameter
                    param_ref = source[1:]
                    if parameters and param_ref in parameters:
                        mapped_input[key] = parameters[param_ref]
                else:
                    # Direct value
                    mapped_input[key] = source
                    
            stage_input = mapped_input
            
        return stage_input
        
    def _merge_outputs(self, stage_outputs: Dict[str, Any]) -> Any:
        """Merge stage outputs into final result"""
        # Simple merge strategy - can be customized
        if len(stage_outputs) == 1:
            return list(stage_outputs.values())[0]
        return stage_outputs
        
    def _generate_cache_key(self, stage: PipelineStage, input_data: Any) -> str:
        """Generate cache key for stage result"""
        import hashlib
        
        # Create deterministic key
        key_parts = [
            stage.stage_id,
            stage.name,
            str(stage.config),
            json.dumps(input_data, sort_keys=True, default=str)
        ]
        
        key_string = "|".join(key_parts)
        return hashlib.sha256(key_string.encode()).hexdigest()
        
    async def _check_resources(self, pipeline: Pipeline) -> bool:
        """Check if resources are available for pipeline"""
        # Estimate required resources
        required_memory = sum(
            stage.config.get('memory_gb', 1.0)
            for stage in pipeline.stages
        )
        required_cores = sum(
            stage.config.get('cpu_cores', 1)
            for stage in pipeline.stages
        )
        
        # Check against limits
        available_memory = self.config.max_memory_gb - self._resource_usage['memory_gb']
        available_cores = self.config.max_cpu_cores - self._resource_usage['cpu_cores']
        
        return (
            required_memory <= available_memory and
            required_cores <= available_cores
        )
        
    async def _release_resources(self, pipeline: Pipeline):
        """Release resources used by pipeline"""
        # Update resource usage
        for stage in pipeline.stages:
            self._resource_usage['memory_gb'] -= stage.config.get('memory_gb', 1.0)
            self._resource_usage['cpu_cores'] -= stage.config.get('cpu_cores', 1)
            
        # Ensure non-negative
        self._resource_usage['memory_gb'] = max(0, self._resource_usage['memory_gb'])
        self._resource_usage['cpu_cores'] = max(0, self._resource_usage['cpu_cores'])
        
    async def _estimate_resources(
        self,
        stages: List[PipelineStage]
    ) -> Dict[str, Any]:
        """Estimate resource requirements for stages"""
        total_memory = 0
        total_cores = 0
        peak_memory = 0
        peak_cores = 0
        
        # Analyze each execution group
        for stage in stages:
            memory = stage.config.get('memory_gb', 1.0)
            cores = stage.config.get('cpu_cores', 1)
            
            total_memory += memory
            total_cores += cores
            peak_memory = max(peak_memory, memory)
            peak_cores = max(peak_cores, cores)
            
        return {
            "total_memory_gb": total_memory,
            "total_cpu_cores": total_cores,
            "peak_memory_gb": peak_memory,
            "peak_cpu_cores": peak_cores,
            "estimated_cost": total_memory * 0.05 + total_cores * 0.10  # Simple cost model
        }
        
    async def _optimize_plan(
        self,
        stages: List[PipelineStage],
        dependencies: Dict[str, Set[str]],
        execution_order: List[List[str]]
    ) -> List[str]:
        """Generate optimization suggestions"""
        suggestions = []
        
        # Check for parallelization opportunities
        max_parallel = max(len(group) for group in execution_order)
        if max_parallel < len(stages) / 2:
            suggestions.append(
                f"Consider refactoring to increase parallelization. "
                f"Current max parallel stages: {max_parallel}"
            )
            
        # Check for long-running stages
        for stage in stages:
            estimated_duration = stage.config.get('estimated_duration_minutes', 5)
            if estimated_duration > 30:
                suggestions.append(
                    f"Stage '{stage.name}' has long estimated duration ({estimated_duration}m). "
                    f"Consider splitting into smaller stages."
                )
                
        # Check for resource bottlenecks
        for group in execution_order:
            group_memory = sum(
                stages[i].config.get('memory_gb', 1.0)
                for i, s in enumerate(stages)
                if s.stage_id in group
            )
            if group_memory > self.config.max_memory_gb * 0.8:
                suggestions.append(
                    f"Execution group uses {group_memory}GB memory "
                    f"(80% of limit). Consider resource optimization."
                )
                
        return suggestions
        
    async def _record_execution_metrics(
        self,
        pipeline_id: str,
        result: PipelineResult
    ):
        """Record execution metrics for optimization"""
        metrics = {
            "pipeline_id": pipeline_id,
            "total_duration_seconds": (
                result.completed_at - result.started_at
            ).total_seconds() if result.completed_at else 0,
            "stage_count": len(result.stage_results),
            "failed_stages": sum(
                1 for r in result.stage_results.values()
                if r.status == StageStatus.FAILED
            ),
            "timestamp": datetime.utcnow()
        }
        
        # Add to optimization history
        self._optimization_history.append(metrics)
        
        # Trim history
        if len(self._optimization_history) > 1000:
            self._optimization_history = self._optimization_history[-1000:]
            
    def _check_data_dependency(
        self,
        input_mapping: Dict[str, str],
        output_schema: Dict[str, Any]
    ) -> bool:
        """Check if input mapping depends on output schema"""
        # Simple check - can be made more sophisticated
        for source in input_mapping.values():
            if source.startswith('$') and source[1:] in output_schema:
                return True
        return False
        
    async def _execute_sequential(
        self,
        pipeline: Pipeline,
        plan: ExecutionPlan,
        input_data: Any,
        parameters: Optional[Dict[str, Any]]
    ) -> PipelineResult:
        """Execute pipeline stages sequentially"""
        # Flatten execution order for sequential execution
        sequential_order = [
            stage_id
            for group in plan.execution_order
            for stage_id in group
        ]
        
        # Temporarily modify pipeline stages order
        stage_map = {s.stage_id: s for s in plan.stages}
        ordered_stages = [stage_map[sid] for sid in sequential_order]
        
        # Execute using pipeline's built-in sequential execution
        original_stages = pipeline.stages
        pipeline.stages = ordered_stages
        
        try:
            return await pipeline.execute(input_data, parameters)
        finally:
            pipeline.stages = original_stages
            
    async def _execute_parallel(
        self,
        pipeline: Pipeline,
        plan: ExecutionPlan,
        input_data: Any,
        parameters: Optional[Dict[str, Any]]
    ) -> PipelineResult:
        """Execute all independent stages in parallel"""
        # Find all stages with no dependencies
        independent_stages = [
            stage for stage in plan.stages
            if not plan.dependencies.get(stage.stage_id, set())
        ]
        
        if not independent_stages:
            # Fall back to DAG execution
            return await self._execute_dag(pipeline, plan, input_data, parameters)
            
        # Execute independent stages in parallel
        result = PipelineResult(
            pipeline_id=pipeline.config.name,
            status=StageStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        tasks = []
        for stage in independent_stages:
            task = asyncio.create_task(
                self._execute_stage_with_monitoring(
                    stage, input_data, result
                )
            )
            tasks.append((stage.stage_id, task))
            
        # Wait for completion
        for stage_id, task in tasks:
            try:
                stage_result = await task
                result.stage_results[stage_id] = stage_result
            except Exception as e:
                logger.error(f"Stage {stage_id} failed: {e}")
                result.status = StageStatus.FAILED
                
        result.completed_at = datetime.utcnow()
        result.status = StageStatus.COMPLETED if result.status != StageStatus.FAILED else StageStatus.FAILED
        
        return result 