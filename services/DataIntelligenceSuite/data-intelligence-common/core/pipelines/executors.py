"""
Pipeline Executors

Provides different execution strategies for pipeline stages.
"""

import asyncio
import concurrent.futures
from typing import Any, Dict, List, Optional, Callable, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from abc import ABC
import multiprocessing as mp
import threading
import queue
import ray
import dask
from dask import delayed
import numpy as np

from .base import PipelineStage, StageResult, StageStatus
from ...monitoring import StructuredLogger, MetricsCollector

logger = StructuredLogger.get_logger(__name__)


class ExecutorType(str, Enum):
    """Types of executors"""
    THREAD = "thread"
    PROCESS = "process"
    ASYNC = "async"
    RAY = "ray"
    DASK = "dask"
    KUBERNETES = "kubernetes"
    CELERY = "celery"


@dataclass
class ExecutorConfig:
    """Executor configuration"""
    executor_type: ExecutorType = ExecutorType.ASYNC
    max_workers: int = 4
    timeout: timedelta = timedelta(minutes=30)
    memory_limit_gb: float = 4.0
    cpu_limit: float = 1.0
    
    # Retry configuration
    max_retries: int = 3
    retry_delay: timedelta = timedelta(seconds=10)
    
    # Resource management
    enable_resource_monitoring: bool = True
    enable_auto_scaling: bool = False
    min_workers: int = 1
    
    # Performance
    enable_batching: bool = True
    batch_size: int = 100
    batch_timeout: timedelta = timedelta(seconds=5)


class BaseExecutor(ABC):
    """Base executor interface"""
    
    def __init__(self, config: ExecutorConfig, metrics: Optional[MetricsCollector] = None):
        self.config = config
        self.metrics = metrics or MetricsCollector()
        self._running_tasks: Dict[str, Any] = {}
        
    async def execute_stage(
        self,
        stage: PipelineStage,
        input_data: Any,
        context: Optional[Dict[str, Any]] = None
    ) -> StageResult:
        """Execute a pipeline stage"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement execute_stage method"
        )
        
    async def execute_batch(
        self,
        stages: List[PipelineStage],
        input_data: List[Any],
        context: Optional[Dict[str, Any]] = None
    ) -> List[StageResult]:
        """Execute multiple stages in batch"""
        results = []
        
        for stage, data in zip(stages, input_data):
            result = await self.execute_stage(stage, data, context)
            results.append(result)
            
        return results
        
    async def shutdown(self):
        """Shutdown executor and cleanup resources"""
        pass


class AsyncExecutor(BaseExecutor):
    """Asynchronous executor using asyncio"""
    
    def __init__(self, config: ExecutorConfig, metrics: Optional[MetricsCollector] = None):
        super().__init__(config, metrics)
        self._semaphore = asyncio.Semaphore(config.max_workers)
        
    async def execute_stage(
        self,
        stage: PipelineStage,
        input_data: Any,
        context: Optional[Dict[str, Any]] = None
    ) -> StageResult:
        """Execute stage asynchronously"""
        async with self._semaphore:
            start_time = datetime.utcnow()
            
            try:
                # Execute with timeout
                result = await asyncio.wait_for(
                    self._execute_stage_impl(stage, input_data, context),
                    timeout=self.config.timeout.total_seconds()
                )
                
                return result
                
            except asyncio.TimeoutError:
                return StageResult(
                    stage_id=stage.stage_id,
                    stage_name=stage.name,
                    status=StageStatus.FAILED,
                    error="Stage execution timed out",
                    started_at=start_time,
                    completed_at=datetime.utcnow()
                )
            except Exception as e:
                logger.error(f"Stage execution failed: {e}", exc_info=True)
                return StageResult(
                    stage_id=stage.stage_id,
                    stage_name=stage.name,
                    status=StageStatus.FAILED,
                    error=str(e),
                    started_at=start_time,
                    completed_at=datetime.utcnow()
                )
                
    async def _execute_stage_impl(
        self,
        stage: PipelineStage,
        input_data: Any,
        context: Optional[Dict[str, Any]]
    ) -> StageResult:
        """Implementation of stage execution"""
        start_time = datetime.utcnow()
        
        # Execute transform function
        if asyncio.iscoroutinefunction(stage.transform):
            output_data = await stage.transform(input_data)
        else:
            # Run sync function in thread pool
            loop = asyncio.get_event_loop()
            output_data = await loop.run_in_executor(
                None, stage.transform, input_data
            )
            
        return StageResult(
            stage_id=stage.stage_id,
            stage_name=stage.name,
            status=StageStatus.COMPLETED,
            output_data=output_data,
            started_at=start_time,
            completed_at=datetime.utcnow(),
            metrics={
                "input_size": self._get_data_size(input_data),
                "output_size": self._get_data_size(output_data)
            }
        )
        
    def _get_data_size(self, data: Any) -> int:
        """Get approximate size of data"""
        if hasattr(data, '__len__'):
            return len(data)
        elif hasattr(data, 'shape'):
            return int(np.prod(data.shape))
        else:
            return 1


class ThreadExecutor(BaseExecutor):
    """Thread-based executor"""
    
    def __init__(self, config: ExecutorConfig, metrics: Optional[MetricsCollector] = None):
        super().__init__(config, metrics)
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=config.max_workers
        )
        
    async def execute_stage(
        self,
        stage: PipelineStage,
        input_data: Any,
        context: Optional[Dict[str, Any]] = None
    ) -> StageResult:
        """Execute stage in thread pool"""
        loop = asyncio.get_event_loop()
        
        future = loop.run_in_executor(
            self._executor,
            self._execute_stage_sync,
            stage,
            input_data,
            context
        )
        
        try:
            result = await asyncio.wait_for(
                future,
                timeout=self.config.timeout.total_seconds()
            )
            return result
        except asyncio.TimeoutError:
            return StageResult(
                stage_id=stage.stage_id,
                stage_name=stage.name,
                status=StageStatus.FAILED,
                error="Stage execution timed out",
                started_at=datetime.utcnow(),
                completed_at=datetime.utcnow()
            )
            
    def _execute_stage_sync(
        self,
        stage: PipelineStage,
        input_data: Any,
        context: Optional[Dict[str, Any]]
    ) -> StageResult:
        """Synchronous stage execution"""
        start_time = datetime.utcnow()
        
        try:
            output_data = stage.transform(input_data)
            
            return StageResult(
                stage_id=stage.stage_id,
                stage_name=stage.name,
                status=StageStatus.COMPLETED,
                output_data=output_data,
                started_at=start_time,
                completed_at=datetime.utcnow()
            )
        except Exception as e:
            return StageResult(
                stage_id=stage.stage_id,
                stage_name=stage.name,
                status=StageStatus.FAILED,
                error=str(e),
                started_at=start_time,
                completed_at=datetime.utcnow()
            )
            
    async def shutdown(self):
        """Shutdown thread pool"""
        self._executor.shutdown(wait=True)


class ProcessExecutor(BaseExecutor):
    """Process-based executor for CPU-intensive tasks"""
    
    def __init__(self, config: ExecutorConfig, metrics: Optional[MetricsCollector] = None):
        super().__init__(config, metrics)
        self._executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=config.max_workers
        )
        
    async def execute_stage(
        self,
        stage: PipelineStage,
        input_data: Any,
        context: Optional[Dict[str, Any]] = None
    ) -> StageResult:
        """Execute stage in separate process"""
        loop = asyncio.get_event_loop()
        
        # Note: stage.transform must be picklable for process execution
        future = loop.run_in_executor(
            self._executor,
            _execute_stage_in_process,
            stage,
            input_data,
            context
        )
        
        try:
            result = await asyncio.wait_for(
                future,
                timeout=self.config.timeout.total_seconds()
            )
            return result
        except asyncio.TimeoutError:
            return StageResult(
                stage_id=stage.stage_id,
                stage_name=stage.name,
                status=StageStatus.FAILED,
                error="Stage execution timed out",
                started_at=datetime.utcnow(),
                completed_at=datetime.utcnow()
            )
            
    async def shutdown(self):
        """Shutdown process pool"""
        self._executor.shutdown(wait=True)


def _execute_stage_in_process(
    stage: PipelineStage,
    input_data: Any,
    context: Optional[Dict[str, Any]]
) -> StageResult:
    """Execute stage in separate process (must be picklable)"""
    start_time = datetime.utcnow()
    
    try:
        output_data = stage.transform(input_data)
        
        return StageResult(
            stage_id=stage.stage_id,
            stage_name=stage.name,
            status=StageStatus.COMPLETED,
            output_data=output_data,
            started_at=start_time,
            completed_at=datetime.utcnow()
        )
    except Exception as e:
        return StageResult(
            stage_id=stage.stage_id,
            stage_name=stage.name,
            status=StageStatus.FAILED,
            error=str(e),
            started_at=start_time,
            completed_at=datetime.utcnow()
        )


class RayExecutor(BaseExecutor):
    """Ray-based distributed executor"""
    
    def __init__(self, config: ExecutorConfig, metrics: Optional[MetricsCollector] = None):
        super().__init__(config, metrics)
        
        # Initialize Ray if not already initialized
        if not ray.is_initialized():
            ray.init(
                num_cpus=config.max_workers,
                memory=config.memory_limit_gb * 1024 * 1024 * 1024
            )
            
    async def execute_stage(
        self,
        stage: PipelineStage,
        input_data: Any,
        context: Optional[Dict[str, Any]] = None
    ) -> StageResult:
        """Execute stage using Ray"""
        # Create Ray remote function
        remote_execute = ray.remote(_execute_stage_ray)
        
        # Submit task
        future = remote_execute.remote(stage, input_data, context)
        
        try:
            # Wait for result with timeout
            result = await asyncio.wait_for(
                asyncio.create_task(self._wait_for_ray_result(future)),
                timeout=self.config.timeout.total_seconds()
            )
            return result
        except asyncio.TimeoutError:
            ray.cancel(future)
            return StageResult(
                stage_id=stage.stage_id,
                stage_name=stage.name,
                status=StageStatus.FAILED,
                error="Stage execution timed out",
                started_at=datetime.utcnow(),
                completed_at=datetime.utcnow()
            )
            
    async def _wait_for_ray_result(self, future):
        """Wait for Ray result asynchronously"""
        while not ray.wait([future], timeout=0.1)[0]:
            await asyncio.sleep(0.1)
        return ray.get(future)
        
    async def execute_batch(
        self,
        stages: List[PipelineStage],
        input_data: List[Any],
        context: Optional[Dict[str, Any]] = None
    ) -> List[StageResult]:
        """Execute multiple stages in parallel using Ray"""
        remote_execute = ray.remote(_execute_stage_ray)
        
        # Submit all tasks
        futures = [
            remote_execute.remote(stage, data, context)
            for stage, data in zip(stages, input_data)
        ]
        
        # Wait for all results
        results = ray.get(futures)
        return results
        
    async def shutdown(self):
        """Shutdown Ray"""
        if ray.is_initialized():
            ray.shutdown()


def _execute_stage_ray(
    stage: PipelineStage,
    input_data: Any,
    context: Optional[Dict[str, Any]]
) -> StageResult:
    """Execute stage in Ray worker"""
    start_time = datetime.utcnow()
    
    try:
        output_data = stage.transform(input_data)
        
        return StageResult(
            stage_id=stage.stage_id,
            stage_name=stage.name,
            status=StageStatus.COMPLETED,
            output_data=output_data,
            started_at=start_time,
            completed_at=datetime.utcnow()
        )
    except Exception as e:
        return StageResult(
            stage_id=stage.stage_id,
            stage_name=stage.name,
            status=StageStatus.FAILED,
            error=str(e),
            started_at=start_time,
            completed_at=datetime.utcnow()
        )


class DaskExecutor(BaseExecutor):
    """Dask-based distributed executor"""
    
    def __init__(self, config: ExecutorConfig, metrics: Optional[MetricsCollector] = None):
        super().__init__(config, metrics)
        
        # Create Dask client
        from dask.distributed import Client
        self._client = Client(
            n_workers=config.max_workers,
            threads_per_worker=1,
            memory_limit=f"{config.memory_limit_gb}GB"
        )
        
    async def execute_stage(
        self,
        stage: PipelineStage,
        input_data: Any,
        context: Optional[Dict[str, Any]] = None
    ) -> StageResult:
        """Execute stage using Dask"""
        # Create delayed task
        task = delayed(_execute_stage_dask)(stage, input_data, context)
        
        # Submit and compute
        future = self._client.compute(task, asynchronous=True)
        
        try:
            result = await asyncio.wait_for(
                future,
                timeout=self.config.timeout.total_seconds()
            )
            return result
        except asyncio.TimeoutError:
            future.cancel()
            return StageResult(
                stage_id=stage.stage_id,
                stage_name=stage.name,
                status=StageStatus.FAILED,
                error="Stage execution timed out",
                started_at=datetime.utcnow(),
                completed_at=datetime.utcnow()
            )
            
    async def execute_batch(
        self,
        stages: List[PipelineStage],
        input_data: List[Any],
        context: Optional[Dict[str, Any]] = None
    ) -> List[StageResult]:
        """Execute multiple stages using Dask"""
        # Create delayed tasks
        tasks = [
            delayed(_execute_stage_dask)(stage, data, context)
            for stage, data in zip(stages, input_data)
        ]
        
        # Compute all tasks
        futures = self._client.compute(tasks, asynchronous=True)
        results = await futures
        
        return results
        
    async def shutdown(self):
        """Shutdown Dask client"""
        await self._client.close()


def _execute_stage_dask(
    stage: PipelineStage,
    input_data: Any,
    context: Optional[Dict[str, Any]]
) -> StageResult:
    """Execute stage in Dask worker"""
    start_time = datetime.utcnow()
    
    try:
        output_data = stage.transform(input_data)
        
        return StageResult(
            stage_id=stage.stage_id,
            stage_name=stage.name,
            status=StageStatus.COMPLETED,
            output_data=output_data,
            started_at=start_time,
            completed_at=datetime.utcnow()
        )
    except Exception as e:
        return StageResult(
            stage_id=stage.stage_id,
            stage_name=stage.name,
            status=StageStatus.FAILED,
            error=str(e),
            started_at=start_time,
            completed_at=datetime.utcnow()
        )


class ExecutorFactory:
    """Factory for creating executors"""
    
    _executors = {
        ExecutorType.ASYNC: AsyncExecutor,
        ExecutorType.THREAD: ThreadExecutor,
        ExecutorType.PROCESS: ProcessExecutor,
        ExecutorType.RAY: RayExecutor,
        ExecutorType.DASK: DaskExecutor
    }
    
    @classmethod
    def create_executor(
        cls,
        config: ExecutorConfig,
        metrics: Optional[MetricsCollector] = None
    ) -> BaseExecutor:
        """Create executor based on configuration"""
        executor_class = cls._executors.get(config.executor_type)
        
        if not executor_class:
            raise ValueError(f"Unknown executor type: {config.executor_type}")
            
        return executor_class(config, metrics)
        
    @classmethod
    def register_executor(
        cls,
        executor_type: ExecutorType,
        executor_class: type
    ):
        """Register custom executor"""
        cls._executors[executor_type] = executor_class 