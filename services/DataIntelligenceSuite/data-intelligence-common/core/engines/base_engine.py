"""
Base Engine Class

Provides base class for all processing engines.
"""

from abc import ABC
from typing import Any, Dict, List, Optional, Callable, TypeVar, Generic, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import uuid

from ...monitoring import StructuredLogger, MetricsCollector
from ..caching import CacheManager
from ..events import EventBus, Event

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')  # Input type
R = TypeVar('R')  # Result type


class EngineStatus(str, Enum):
    """Engine lifecycle status"""
    IDLE = "idle"
    INITIALIZING = "initializing"
    READY = "ready"
    PROCESSING = "processing"
    PAUSED = "paused"
    ERROR = "error"
    SHUTTING_DOWN = "shutting_down"
    SHUTDOWN = "shutdown"


class EngineType(str, Enum):
    """Types of processing engines"""
    BATCH = "batch"
    STREAM = "stream"
    REALTIME = "realtime"
    ASYNC = "async"
    DISTRIBUTED = "distributed"
    HYBRID = "hybrid"


@dataclass
class EngineConfig:
    """Base configuration for engines"""
    name: str
    type: EngineType
    version: str = "1.0.0"
    description: str = ""
    
    # Resource settings
    max_workers: int = 4
    max_memory_mb: Optional[int] = None
    max_queue_size: int = 1000
    
    # Behavior settings
    auto_start: bool = True
    enable_monitoring: bool = True
    enable_caching: bool = True
    enable_checkpointing: bool = False
    
    # Timeouts
    initialization_timeout: int = 60
    shutdown_timeout: int = 30
    task_timeout: Optional[int] = None
    
    # Custom configuration
    custom_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EngineResult(Generic[R]):
    """Result from engine processing"""
    engine_name: str
    task_id: str
    status: str
    result: Optional[R] = None
    error: Optional[str] = None
    
    # Timing
    submitted_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Metrics
    processing_time_seconds: Optional[float] = None
    queue_time_seconds: Optional[float] = None
    
    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseEngine(ABC, Generic[T, R]):
    """
    Base class for all processing engines.
    
    Provides:
    - Lifecycle management
    - Task queuing and processing
    - Resource management
    - Monitoring and metrics
    - Event publishing
    - Error handling
    """
    
    def __init__(
        self,
        config: EngineConfig,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.config = config
        self.cache = cache_manager
        self.event_bus = event_bus
        self.metrics = metrics_collector or MetricsCollector(f"engine_{config.name}")
        
        # Engine state
        self._status = EngineStatus.IDLE
        self._initialized = False
        self._running = False
        
        # Task management
        self._task_queue: asyncio.Queue = asyncio.Queue(maxsize=config.max_queue_size)
        self._active_tasks: Dict[str, asyncio.Task] = {}
        self._completed_tasks: Dict[str, EngineResult] = {}
        
        # Workers
        self._workers: List[asyncio.Task] = []
        self._worker_semaphore = asyncio.Semaphore(config.max_workers)
        
        # Monitoring
        self._start_time: Optional[datetime] = None
        self._processed_count = 0
        self._error_count = 0
        
    async def initialize(self) -> None:
        """Initialize the engine"""
        if self._initialized:
            logger.warning(f"Engine {self.config.name} already initialized")
            return
            
        logger.info(f"Initializing engine {self.config.name}")
        self._status = EngineStatus.INITIALIZING
        
        try:
            # Initialize components
            await asyncio.wait_for(
                self._initialize_components(),
                timeout=self.config.initialization_timeout
            )
            
            # Start workers if auto-start enabled
            if self.config.auto_start:
                await self.start()
            else:
                self._status = EngineStatus.READY
                
            self._initialized = True
            self._start_time = datetime.utcnow()
            
            # Publish initialization event
            await self._publish_event("engine.initialized", {
                "engine": self.config.name,
                "type": self.config.type.value
            })
            
            logger.info(f"Engine {self.config.name} initialized successfully")
            
        except asyncio.TimeoutError:
            self._status = EngineStatus.ERROR
            raise RuntimeError(f"Engine initialization timed out after {self.config.initialization_timeout}s")
        except Exception as e:
            self._status = EngineStatus.ERROR
            logger.error(f"Failed to initialize engine: {e}")
            raise
    
    async def start(self) -> None:
        """Start the engine"""
        if not self._initialized:
            await self.initialize()
            
        if self._running:
            logger.warning(f"Engine {self.config.name} already running")
            return
            
        logger.info(f"Starting engine {self.config.name}")
        self._running = True
        self._status = EngineStatus.READY
        
        # Start worker tasks
        for i in range(self.config.max_workers):
            worker = asyncio.create_task(self._worker_loop(i))
            self._workers.append(worker)
        
        # Start monitoring if enabled
        if self.config.enable_monitoring:
            asyncio.create_task(self._monitor_loop())
        
        await self._publish_event("engine.started", {
            "engine": self.config.name,
            "workers": self.config.max_workers
        })
    
    async def stop(self) -> None:
        """Stop the engine"""
        if not self._running:
            return
            
        logger.info(f"Stopping engine {self.config.name}")
        self._running = False
        self._status = EngineStatus.SHUTTING_DOWN
        
        # Cancel all workers
        for worker in self._workers:
            worker.cancel()
            
        # Wait for workers to finish
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        
        # Process remaining tasks
        await self._drain_queue()
        
        self._status = EngineStatus.SHUTDOWN
        
        await self._publish_event("engine.stopped", {
            "engine": self.config.name,
            "processed_count": self._processed_count,
            "error_count": self._error_count
        })
    
    async def submit(self, task_data: T, priority: int = 0, **kwargs) -> str:
        """
        Submit a task to the engine.
        
        Args:
            task_data: Task input data
            priority: Task priority (higher = more important)
            **kwargs: Additional task parameters
            
        Returns:
            Task ID
        """
        task_id = str(uuid.uuid4())
        
        task = {
            "id": task_id,
            "data": task_data,
            "priority": priority,
            "kwargs": kwargs,
            "submitted_at": datetime.utcnow()
        }
        
        await self._task_queue.put(task)
        
        # Record metric
        self.metrics.increment("tasks_submitted", {"engine": self.config.name})
        
        return task_id
    
    async def get_result(self, task_id: str, wait: bool = False) -> Optional[EngineResult[R]]:
        """
        Get result for a task.
        
        Args:
            task_id: Task ID
            wait: Whether to wait for completion
            
        Returns:
            Task result or None if not ready
        """
        # Check completed tasks
        if task_id in self._completed_tasks:
            return self._completed_tasks[task_id]
        
        # Check active tasks
        if task_id in self._active_tasks and wait:
            await self._active_tasks[task_id]
            return self._completed_tasks.get(task_id)
        
        return None
    
    async def _initialize_components(self) -> None:
        """Initialize engine-specific components"""
        # Base initialization - set up common components
        logger.info(f"Initializing base components for engine {self.config.name}")
        
        # Initialize metrics if enabled
        if self.config.enable_monitoring and self.metrics:
            self.metrics.register_counter(f"{self.config.name}_tasks_submitted")
            self.metrics.register_counter(f"{self.config.name}_tasks_completed")
            self.metrics.register_counter(f"{self.config.name}_tasks_failed")
            self.metrics.register_histogram(f"{self.config.name}_task_duration")
            self.metrics.register_gauge(f"{self.config.name}_active_tasks")
            
        # Initialize health checks
        if hasattr(self, 'health_manager') and self.health_manager:
            await self.health_manager.register_check(
                f"{self.config.name}_engine",
                self._check_engine_health
            )
            
        # Derived classes should override this to add engine-specific initialization
        logger.info(f"Base initialization completed for engine {self.config.name}")
    
    async def _process_task(self, task_data: T, **kwargs) -> R:
        """
        Process a single task.
        
        This base implementation should be overridden by derived classes.
        
        Args:
            task_data: Task input data
            **kwargs: Additional parameters
            
        Returns:
            Task result
        """
        # Base implementation - raise NotImplementedError
        # Derived classes MUST override this method
        raise NotImplementedError(
            f"Engine {self.config.name} must implement _process_task method"
        )
    
    async def _worker_loop(self, worker_id: int) -> None:
        """Worker loop for processing tasks"""
        logger.info(f"Worker {worker_id} started for engine {self.config.name}")
        
        while self._running:
            try:
                # Get task from queue
                task = await asyncio.wait_for(
                    self._task_queue.get(),
                    timeout=1.0
                )
                
                # Process task
                async with self._worker_semaphore:
                    await self._process_task_wrapper(task)
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                
        logger.info(f"Worker {worker_id} stopped")
    
    async def _process_task_wrapper(self, task: Dict[str, Any]) -> None:
        """Wrapper for task processing with error handling"""
        task_id = task["id"]
        self._status = EngineStatus.PROCESSING
        
        result = EngineResult(
            engine_name=self.config.name,
            task_id=task_id,
            status="processing",
            submitted_at=task["submitted_at"]
        )
        
        # Track active task
        task_future = asyncio.create_task(self._process_single_task(task, result))
        self._active_tasks[task_id] = task_future
        
        try:
            await task_future
        finally:
            # Remove from active tasks
            self._active_tasks.pop(task_id, None)
            
            # Update status if no more tasks
            if not self._active_tasks and self._task_queue.empty():
                self._status = EngineStatus.READY
    
    async def _process_single_task(self, task: Dict[str, Any], result: EngineResult) -> None:
        """Process a single task"""
        try:
            # Record start time
            result.started_at = datetime.utcnow()
            result.queue_time_seconds = (result.started_at - task["submitted_at"]).total_seconds()
            
            # Process with timeout if configured
            if self.config.task_timeout:
                processed_result = await asyncio.wait_for(
                    self._process_task(task["data"], **task["kwargs"]),
                    timeout=self.config.task_timeout
                )
            else:
                processed_result = await self._process_task(task["data"], **task["kwargs"])
            
            # Update result
            result.status = "completed"
            result.result = processed_result
            result.completed_at = datetime.utcnow()
            result.processing_time_seconds = (result.completed_at - result.started_at).total_seconds()
            
            self._processed_count += 1
            
            # Record metrics
            self.metrics.record_histogram(
                "task_processing_time",
                result.processing_time_seconds,
                {"engine": self.config.name}
            )
            
        except Exception as e:
            # Handle error
            result.status = "error"
            result.error = str(e)
            result.completed_at = datetime.utcnow()
            
            self._error_count += 1
            
            logger.error(f"Task {task['id']} failed: {e}")
            
            # Record error metric
            self.metrics.increment("task_errors", {"engine": self.config.name})
        
        finally:
            # Store result
            self._completed_tasks[task["id"]] = result
            
            # Publish completion event
            await self._publish_event("task.completed", {
                "engine": self.config.name,
                "task_id": task["id"],
                "status": result.status,
                "duration": result.processing_time_seconds
            })
    
    async def _monitor_loop(self) -> None:
        """Monitoring loop for engine metrics"""
        while self._running:
            try:
                # Record queue size
                self.metrics.record_gauge(
                    "queue_size",
                    self._task_queue.qsize(),
                    {"engine": self.config.name}
                )
                
                # Record active tasks
                self.metrics.record_gauge(
                    "active_tasks",
                    len(self._active_tasks),
                    {"engine": self.config.name}
                )
                
                await asyncio.sleep(10)  # Monitor every 10 seconds
                
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
    
    async def _drain_queue(self) -> None:
        """Process remaining tasks in queue"""
        remaining = self._task_queue.qsize()
        if remaining > 0:
            logger.info(f"Draining {remaining} remaining tasks")
            
            while not self._task_queue.empty():
                try:
                    task = self._task_queue.get_nowait()
                    await self._process_task_wrapper(task)
                except asyncio.QueueEmpty:
                    break
    
    async def _publish_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """Publish engine event"""
        if not self.event_bus:
            return
            
        try:
            event = Event(
                type=event_type,
                source=f"engine.{self.config.name}",
                data=data,
                timestamp=datetime.utcnow()
            )
            await self.event_bus.publish(event)
        except Exception as e:
            logger.warning(f"Failed to publish event: {e}")
    
    async def _check_engine_health(self) -> Tuple[bool, str]:
        """Check engine health status"""
        try:
            # Check if engine is running
            if not self._running:
                return False, "Engine is not running"
                
            # Check worker threads
            active_workers = sum(1 for w in self._workers if not w.done())
            if active_workers < self.config.max_workers * 0.5:
                return False, f"Only {active_workers}/{self.config.max_workers} workers active"
                
            # Check error rate
            total = self._processed_count + self._error_count
            if total > 100:  # Only check after processing some tasks
                error_rate = self._error_count / total
                if error_rate > 0.1:  # More than 10% errors
                    return False, f"High error rate: {error_rate:.2%}"
                    
            # Check queue size
            queue_size = self._task_queue.qsize()
            if queue_size > self.config.max_queue_size * 0.9:
                return False, f"Queue nearly full: {queue_size}/{self.config.max_queue_size}"
                
            return True, "Engine is healthy"
            
        except Exception as e:
            return False, f"Health check failed: {str(e)}"
    
    @property
    def status(self) -> EngineStatus:
        """Get current engine status"""
        return self._status
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Get engine statistics"""
        uptime = None
        if self._start_time:
            uptime = (datetime.utcnow() - self._start_time).total_seconds()
            
        return {
            "status": self._status.value,
            "uptime_seconds": uptime,
            "processed_count": self._processed_count,
            "error_count": self._error_count,
            "queue_size": self._task_queue.qsize(),
            "active_tasks": len(self._active_tasks),
            "completed_tasks": len(self._completed_tasks)
        }


__all__ = ["BaseEngine", "EngineConfig", "EngineStatus", "EngineResult", "EngineType"] 