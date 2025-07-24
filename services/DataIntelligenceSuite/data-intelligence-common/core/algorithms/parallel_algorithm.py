"""
Parallel Algorithm Implementation

Provides base class for algorithms that can be parallelized across multiple workers.
"""

from abc import abstractmethod
from typing import Any, Dict, List, Optional, Union, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
import asyncio
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import multiprocessing

from .base_algorithm import BaseAlgorithm, AlgorithmConfig, AlgorithmResult, AlgorithmType
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')
R = TypeVar('R')
P = TypeVar('P')  # Partition type


class PartitionStrategy(str, Enum):
    """Strategy for partitioning data"""
    ROUND_ROBIN = "round_robin"
    HASH_BASED = "hash_based"
    RANGE_BASED = "range_based"
    SIZE_BASED = "size_based"
    CUSTOM = "custom"


class ExecutorType(str, Enum):
    """Type of executor for parallel processing"""
    THREAD = "thread"
    PROCESS = "process"
    ASYNCIO = "asyncio"


@dataclass
class ParallelConfig(AlgorithmConfig):
    """Configuration for parallel algorithms"""
    # Parallelism settings
    num_workers: Optional[int] = None  # None means use CPU count
    executor_type: ExecutorType = ExecutorType.THREAD
    partition_strategy: PartitionStrategy = PartitionStrategy.ROUND_ROBIN
    
    # Chunk settings
    chunk_size: Optional[int] = None
    min_chunk_size: int = 1
    
    # Coordination
    enable_progress_tracking: bool = True
    enable_partial_results: bool = False
    
    def __post_init__(self):
        if self.num_workers is None:
            self.num_workers = multiprocessing.cpu_count()
        self.type = AlgorithmType.CUSTOM  # Can be overridden


@dataclass
class PartitionResult(Generic[P, R]):
    """Result from processing a single partition"""
    partition_id: int
    partition_data: P
    result: Optional[R] = None
    error: Optional[str] = None
    processing_time: float = 0.0


@dataclass
class ParallelResult(AlgorithmResult[R]):
    """Result from parallel algorithm execution"""
    partition_results: List[PartitionResult] = field(default_factory=list)
    num_partitions: int = 0
    successful_partitions: int = 0
    failed_partitions: int = 0
    
    # Aggregated result
    aggregated_result: Optional[R] = None


class ParallelAlgorithm(BaseAlgorithm[T, R], Generic[T, R, P]):
    """
    Base class for parallel algorithms.
    
    Provides:
    - Data partitioning strategies
    - Parallel execution with different executors
    - Result aggregation
    - Progress tracking
    - Fault tolerance
    """
    
    def __init__(self, config: ParallelConfig, **kwargs):
        super().__init__(config, **kwargs)
        self.config: ParallelConfig = config
        self._executor = None
        self._progress = 0
        
    async def _execute_algorithm(self, input_data: T, **kwargs) -> R:
        """Execute algorithm in parallel"""
        # Partition data
        partitions = await self.partition_data(input_data)
        num_partitions = len(partitions)
        
        logger.info(f"Partitioned data into {num_partitions} chunks for parallel processing")
        
        # Create result container
        result = ParallelResult[R](
            algorithm_name=self.config.name,
            status=self.config.type,
            num_partitions=num_partitions
        )
        
        # Execute based on executor type
        if self.config.executor_type == ExecutorType.ASYNCIO:
            partition_results = await self._execute_async(partitions, **kwargs)
        elif self.config.executor_type == ExecutorType.THREAD:
            partition_results = await self._execute_threaded(partitions, **kwargs)
        else:  # PROCESS
            partition_results = await self._execute_multiprocess(partitions, **kwargs)
        
        # Store partition results
        result.partition_results = partition_results
        result.successful_partitions = sum(1 for pr in partition_results if pr.result is not None)
        result.failed_partitions = sum(1 for pr in partition_results if pr.error is not None)
        
        # Aggregate results
        successful_results = [pr for pr in partition_results if pr.result is not None]
        if successful_results:
            aggregated = await self.aggregate_results([pr.result for pr in successful_results])
            result.aggregated_result = aggregated
            return aggregated
        else:
            raise RuntimeError("All partitions failed to process")
    
    async def _execute_async(self, partitions: List[P], **kwargs) -> List[PartitionResult[P, R]]:
        """Execute partitions using asyncio"""
        tasks = []
        for i, partition in enumerate(partitions):
            task = self._process_partition_async(i, partition, **kwargs)
            tasks.append(task)
        
        # Execute with progress tracking
        if self.config.enable_progress_tracking:
            return await self._execute_with_progress(tasks)
        else:
            return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _execute_threaded(self, partitions: List[P], **kwargs) -> List[PartitionResult[P, R]]:
        """Execute partitions using thread pool"""
        with ThreadPoolExecutor(max_workers=self.config.num_workers) as executor:
            loop = asyncio.get_event_loop()
            tasks = []
            
            for i, partition in enumerate(partitions):
                task = loop.run_in_executor(
                    executor,
                    self._process_partition_sync,
                    i, partition, kwargs
                )
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return self._handle_results(partitions, results)
    
    async def _execute_multiprocess(self, partitions: List[P], **kwargs) -> List[PartitionResult[P, R]]:
        """Execute partitions using process pool"""
        with ProcessPoolExecutor(max_workers=self.config.num_workers) as executor:
            loop = asyncio.get_event_loop()
            tasks = []
            
            for i, partition in enumerate(partitions):
                task = loop.run_in_executor(
                    executor,
                    self._process_partition_sync,
                    i, partition, kwargs
                )
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return self._handle_results(partitions, results)
    
    async def _process_partition_async(self, partition_id: int, partition: P, **kwargs) -> PartitionResult[P, R]:
        """Process a single partition asynchronously"""
        import time
        start_time = time.time()
        
        result = PartitionResult[P, R](
            partition_id=partition_id,
            partition_data=partition
        )
        
        try:
            processed = await self.process_partition(partition_id, partition, **kwargs)
            result.result = processed
        except Exception as e:
            logger.error(f"Failed to process partition {partition_id}: {str(e)}")
            result.error = str(e)
        finally:
            result.processing_time = time.time() - start_time
            self._update_progress()
            
        return result
    
    def _process_partition_sync(self, partition_id: int, partition: P, kwargs: Dict[str, Any]) -> PartitionResult[P, R]:
        """Process a single partition synchronously (for thread/process pools)"""
        import time
        start_time = time.time()
        
        result = PartitionResult[P, R](
            partition_id=partition_id,
            partition_data=partition
        )
        
        try:
            # Note: This is a sync wrapper - actual implementation should handle async properly
            processed = asyncio.run(self.process_partition(partition_id, partition, **kwargs))
            result.result = processed
        except Exception as e:
            logger.error(f"Failed to process partition {partition_id}: {str(e)}")
            result.error = str(e)
        finally:
            result.processing_time = time.time() - start_time
            
        return result
    
    def _handle_results(self, partitions: List[P], results: List[Any]) -> List[PartitionResult[P, R]]:
        """Handle results from parallel execution"""
        partition_results = []
        
        for i, (partition, result) in enumerate(zip(partitions, results)):
            if isinstance(result, PartitionResult):
                partition_results.append(result)
            elif isinstance(result, Exception):
                partition_results.append(PartitionResult[P, R](
                    partition_id=i,
                    partition_data=partition,
                    error=str(result)
                ))
            else:
                partition_results.append(PartitionResult[P, R](
                    partition_id=i,
                    partition_data=partition,
                    result=result
                ))
                
        return partition_results
    
    async def _execute_with_progress(self, tasks: List[asyncio.Task]) -> List[PartitionResult[P, R]]:
        """Execute tasks with progress tracking"""
        results = []
        total = len(tasks)
        
        for i, task in enumerate(asyncio.as_completed(tasks)):
            result = await task
            results.append(result)
            
            # Update progress
            self._progress = (i + 1) / total * 100
            
            # Publish progress event
            if self.event_bus:
                await self._publish_event("algorithm.progress", {
                    "algorithm": self.config.name,
                    "execution_id": self._execution_id,
                    "progress": self._progress
                })
                
        return results
    
    def _update_progress(self):
        """Update progress counter"""
        # This is simplified - in practice would need thread-safe counter
        pass
    
    @abstractmethod
    async def partition_data(self, input_data: T) -> List[P]:
        """
        Partition input data for parallel processing.
        
        Args:
            input_data: Input data to partition
            
        Returns:
            List of partitions
        """
        pass
    
    @abstractmethod
    async def process_partition(self, partition_id: int, partition: P, **kwargs) -> R:
        """
        Process a single partition.
        
        Args:
            partition_id: ID of the partition
            partition: Partition data
            **kwargs: Additional parameters
            
        Returns:
            Processed result for this partition
        """
        pass
    
    @abstractmethod
    async def aggregate_results(self, results: List[R]) -> R:
        """
        Aggregate results from all partitions.
        
        Args:
            results: Results from each partition
            
        Returns:
            Aggregated final result
        """
        pass
    
    # Built-in partitioning strategies
    
    def partition_round_robin(self, data: List[Any], num_partitions: Optional[int] = None) -> List[List[Any]]:
        """Partition data using round-robin strategy"""
        num_partitions = num_partitions or self.config.num_workers
        partitions = [[] for _ in range(num_partitions)]
        
        for i, item in enumerate(data):
            partition_idx = i % num_partitions
            partitions[partition_idx].append(item)
            
        return [p for p in partitions if p]  # Remove empty partitions
    
    def partition_by_size(self, data: List[Any], chunk_size: Optional[int] = None) -> List[List[Any]]:
        """Partition data into fixed-size chunks"""
        chunk_size = chunk_size or self.config.chunk_size or len(data) // self.config.num_workers
        chunk_size = max(chunk_size, self.config.min_chunk_size)
        
        partitions = []
        for i in range(0, len(data), chunk_size):
            partitions.append(data[i:i + chunk_size])
            
        return partitions
    
    def partition_by_hash(self, data: List[Any], key_func: Callable[[Any], Any], 
                         num_partitions: Optional[int] = None) -> List[List[Any]]:
        """Partition data based on hash of key"""
        num_partitions = num_partitions or self.config.num_workers
        partitions = [[] for _ in range(num_partitions)]
        
        for item in data:
            key = key_func(item)
            partition_idx = hash(key) % num_partitions
            partitions[partition_idx].append(item)
            
        return [p for p in partitions if p]  # Remove empty partitions


__all__ = [
    "ParallelAlgorithm",
    "ParallelConfig",
    "PartitionStrategy",
    "ParallelResult",
    "PartitionResult",
    "ExecutorType"
] 