"""
Base Algorithm Class

Provides base class and common patterns for all algorithms.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Callable, TypeVar, Generic
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import time
import uuid

from ...monitoring import StructuredLogger, MetricsCollector
from ..caching import CacheManager
from ..events import EventBus

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')
R = TypeVar('R')


class AlgorithmStatus(str, Enum):
    """Algorithm execution status"""
    PENDING = "pending"
    INITIALIZING = "initializing"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class AlgorithmType(str, Enum):
    """Types of algorithms"""
    OPTIMIZATION = "optimization"
    MACHINE_LEARNING = "machine_learning"
    GRAPH = "graph"
    NUMERICAL = "numerical"
    CRYPTOGRAPHIC = "cryptographic"
    COMPRESSION = "compression"
    SEARCH = "search"
    SORT = "sort"
    CUSTOM = "custom"


@dataclass
class AlgorithmConfig:
    """Base configuration for algorithms"""
    name: str
    type: AlgorithmType
    version: str = "1.0.0"
    description: str = ""
    
    # Execution settings
    timeout_seconds: Optional[int] = None
    max_iterations: Optional[int] = None
    convergence_threshold: Optional[float] = None
    
    # Resource limits
    max_memory_mb: Optional[int] = None
    max_cpu_percent: Optional[float] = None
    
    # Caching
    enable_caching: bool = True
    cache_ttl_seconds: int = 3600
    
    # Monitoring
    enable_metrics: bool = True
    enable_profiling: bool = False
    
    # Custom parameters
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AlgorithmResult(Generic[R]):
    """Base result class for algorithms"""
    algorithm_name: str
    status: AlgorithmStatus
    result: Optional[R] = None
    error: Optional[str] = None
    
    # Timing
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    
    # Performance metrics
    iterations: Optional[int] = None
    convergence_value: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    
    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def complete(self, result: R):
        """Mark algorithm as completed with result"""
        self.status = AlgorithmStatus.COMPLETED
        self.result = result
        self.end_time = datetime.utcnow()
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()
        
    def fail(self, error: str):
        """Mark algorithm as failed with error"""
        self.status = AlgorithmStatus.FAILED
        self.error = error
        self.end_time = datetime.utcnow()
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()


class BaseAlgorithm(ABC, Generic[T, R]):
    """
    Base class for all algorithms.
    
    Provides:
    - Standardized execution flow
    - Parameter validation
    - Result tracking
    - Caching support
    - Metrics collection
    - Event publishing
    """
    
    def __init__(
        self,
        config: AlgorithmConfig,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.config = config
        self.cache = cache_manager
        self.event_bus = event_bus
        self.metrics = metrics_collector or MetricsCollector(f"algorithm_{config.name}")
        
        # Execution state
        self._execution_id: Optional[str] = None
        self._is_running = False
        self._start_time: Optional[datetime] = None
        self._iterations = 0
        
    async def execute(self, input_data: T, **kwargs) -> AlgorithmResult[R]:
        """
        Execute the algorithm with given input.
        
        Args:
            input_data: Input data for the algorithm
            **kwargs: Additional parameters
            
        Returns:
            AlgorithmResult with execution details
        """
        self._execution_id = str(uuid.uuid4())
        result = AlgorithmResult[R](
            algorithm_name=self.config.name,
            status=AlgorithmStatus.PENDING
        )
        
        try:
            # Check cache if enabled
            if self.config.enable_caching and self.cache:
                cache_key = self._generate_cache_key(input_data, kwargs)
                cached_result = await self._get_cached_result(cache_key)
                if cached_result:
                    logger.info(f"Using cached result for {self.config.name}")
                    return cached_result
            
            # Initialize
            result.status = AlgorithmStatus.INITIALIZING
            await self._publish_event("algorithm.started", {
                "algorithm": self.config.name,
                "execution_id": self._execution_id
            })
            
            # Validate input
            self.validate_input(input_data)
            
            # Pre-process
            processed_input = await self.preprocess(input_data)
            
            # Run algorithm
            result.status = AlgorithmStatus.RUNNING
            self._is_running = True
            self._start_time = datetime.utcnow()
            result.start_time = self._start_time
            
            # Execute with timeout if configured
            if self.config.timeout_seconds:
                algorithm_result = await asyncio.wait_for(
                    self._execute_algorithm(processed_input, **kwargs),
                    timeout=self.config.timeout_seconds
                )
            else:
                algorithm_result = await self._execute_algorithm(processed_input, **kwargs)
            
            # Post-process
            final_result = await self.postprocess(algorithm_result)
            
            # Complete
            result.complete(final_result)
            result.iterations = self._iterations
            
            # Cache result if enabled
            if self.config.enable_caching and self.cache:
                await self._cache_result(cache_key, result)
            
            # Publish completion event
            await self._publish_event("algorithm.completed", {
                "algorithm": self.config.name,
                "execution_id": self._execution_id,
                "duration_seconds": result.duration_seconds
            })
            
            # Record metrics
            if self.config.enable_metrics:
                self.metrics.record_histogram(
                    "algorithm_duration_seconds",
                    result.duration_seconds,
                    {"algorithm": self.config.name}
                )
                
        except asyncio.TimeoutError:
            result.status = AlgorithmStatus.TIMEOUT
            result.fail(f"Algorithm timed out after {self.config.timeout_seconds} seconds")
            await self._publish_event("algorithm.timeout", {
                "algorithm": self.config.name,
                "execution_id": self._execution_id
            })
            
        except Exception as e:
            logger.error(f"Algorithm {self.config.name} failed: {str(e)}")
            result.fail(str(e))
            await self._publish_event("algorithm.failed", {
                "algorithm": self.config.name,
                "execution_id": self._execution_id,
                "error": str(e)
            })
            
        finally:
            self._is_running = False
            
        return result
    
    @abstractmethod
    def validate_input(self, input_data: T) -> None:
        """
        Validate input data.
        
        Args:
            input_data: Input to validate
            
        Raises:
            ValueError: If input is invalid
        """
        pass
    
    @abstractmethod
    async def _execute_algorithm(self, input_data: T, **kwargs) -> R:
        """
        Execute the core algorithm logic.
        
        Args:
            input_data: Preprocessed input data
            **kwargs: Additional parameters
            
        Returns:
            Algorithm result
        """
        pass
    
    async def preprocess(self, input_data: T) -> T:
        """
        Preprocess input data before algorithm execution.
        
        Args:
            input_data: Raw input data
            
        Returns:
            Preprocessed data
        """
        return input_data
    
    async def postprocess(self, result: R) -> R:
        """
        Postprocess algorithm result.
        
        Args:
            result: Raw algorithm result
            
        Returns:
            Processed result
        """
        return result
    
    def _generate_cache_key(self, input_data: T, kwargs: Dict[str, Any]) -> str:
        """Generate cache key for input data and parameters"""
        import hashlib
        import json
        
        # Create a deterministic string representation
        key_data = {
            "algorithm": self.config.name,
            "version": self.config.version,
            "input": str(input_data),
            "kwargs": kwargs
        }
        
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.sha256(key_str.encode()).hexdigest()
    
    async def _get_cached_result(self, cache_key: str) -> Optional[AlgorithmResult[R]]:
        """Get cached result if available"""
        if not self.cache:
            return None
            
        try:
            cached = await self.cache.get(f"algorithm:{self.config.name}:{cache_key}")
            if cached:
                return AlgorithmResult[R](**cached)
        except Exception as e:
            logger.warning(f"Failed to get cached result: {e}")
            
        return None
    
    async def _cache_result(self, cache_key: str, result: AlgorithmResult[R]):
        """Cache algorithm result"""
        if not self.cache:
            return
            
        try:
            await self.cache.set(
                f"algorithm:{self.config.name}:{cache_key}",
                result.__dict__,
                ttl=self.config.cache_ttl_seconds
            )
        except Exception as e:
            logger.warning(f"Failed to cache result: {e}")
    
    async def _publish_event(self, event_type: str, data: Dict[str, Any]):
        """Publish algorithm event"""
        if not self.event_bus:
            return
            
        try:
            await self.event_bus.publish(event_type, {
                **data,
                "timestamp": datetime.utcnow().isoformat()
            })
        except Exception as e:
            logger.warning(f"Failed to publish event: {e}")
    
    def _increment_iterations(self):
        """Increment iteration counter"""
        self._iterations += 1
        
        # Check max iterations
        if self.config.max_iterations and self._iterations >= self.config.max_iterations:
            raise RuntimeError(f"Maximum iterations ({self.config.max_iterations}) reached")
    
    def _check_convergence(self, current_value: float, previous_value: float) -> bool:
        """
        Check if algorithm has converged.
        
        Args:
            current_value: Current convergence metric
            previous_value: Previous convergence metric
            
        Returns:
            True if converged
        """
        if not self.config.convergence_threshold:
            return False
            
        return abs(current_value - previous_value) < self.config.convergence_threshold


__all__ = [
    "BaseAlgorithm",
    "AlgorithmConfig",
    "AlgorithmResult",
    "AlgorithmStatus",
    "AlgorithmType"
] 