"""
Base orchestrator providing common orchestration patterns.

Consolidates common functionality from various orchestrators across services.
"""

import asyncio
import uuid
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Callable, TypeVar, Generic
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
import logging

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import MetricsCollector, StructuredLogger

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')


class OrchestrationStatus(str, Enum):
    """Common orchestration statuses"""
    PENDING = "pending"
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"


@dataclass
class OrchestrationError:
    """Orchestration error details"""
    error_type: str
    message: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    details: Dict[str, Any] = field(default_factory=dict)
    recoverable: bool = True


@dataclass
class OrchestrationConfig:
    """Base orchestration configuration"""
    name: str
    timeout: Optional[timedelta] = None
    max_retries: int = 3
    retry_delay: timedelta = timedelta(seconds=5)
    retry_backoff: float = 2.0
    enable_checkpointing: bool = True
    checkpoint_interval: timedelta = timedelta(minutes=5)
    enable_monitoring: bool = True
    enable_tracing: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OrchestrationResult(Generic[T]):
    """Result of orchestration execution"""
    orchestration_id: str
    status: OrchestrationStatus
    result: Optional[T] = None
    error: Optional[OrchestrationError] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration: Optional[timedelta] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_successful(self) -> bool:
        """Check if orchestration was successful"""
        return self.status == OrchestrationStatus.COMPLETED
    
    @property
    def is_failed(self) -> bool:
        """Check if orchestration failed"""
        return self.status in [OrchestrationStatus.FAILED, OrchestrationStatus.CANCELLED]


class BaseOrchestrator(ABC, Generic[T]):
    """
    Base orchestrator with common patterns.
    
    Provides:
    - Lifecycle management
    - Error handling and retry logic
    - Checkpointing and recovery
    - Event publishing
    - Metrics collection
    - Distributed coordination
    """
    
    def __init__(
        self,
        config: OrchestrationConfig,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.config = config
        self.cache = cache_manager
        self.event_bus = event_bus
        self.metrics = metrics_collector or MetricsCollector()
        
        # Execution tracking
        self._executions: Dict[str, OrchestrationResult] = {}
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._checkpoints: Dict[str, Dict[str, Any]] = {}
        
        # State
        self._initialized = False
        self._running = False
        
    async def initialize(self):
        """Initialize orchestrator"""
        if self._initialized:
            return
            
        logger.info(f"Initializing {self.__class__.__name__}")
        
        try:
            await self._initialize_components()
            self._initialized = True
            
            # Start background tasks
            if self.config.enable_checkpointing:
                asyncio.create_task(self._checkpoint_loop())
                
            if self.config.enable_monitoring:
                asyncio.create_task(self._monitoring_loop())
                
            logger.info(f"{self.__class__.__name__} initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize {self.__class__.__name__}: {e}")
            raise
            
    @abstractmethod
    async def _initialize_components(self):
        """Initialize specific components"""
        pass
        
    async def start(
        self,
        input_data: Dict[str, Any],
        correlation_id: Optional[str] = None
    ) -> str:
        """Start new orchestration"""
        orchestration_id = str(uuid.uuid4())
        correlation_id = correlation_id or orchestration_id
        
        # Create result
        result = OrchestrationResult[T](
            orchestration_id=orchestration_id,
            status=OrchestrationStatus.PENDING,
            started_at=datetime.utcnow()
        )
        
        self._executions[orchestration_id] = result
        
        # Start execution task
        task = asyncio.create_task(
            self._execute_with_lifecycle(orchestration_id, input_data, correlation_id)
        )
        self._running_tasks[orchestration_id] = task
        
        # Publish start event
        await self._publish_event("orchestration.started", {
            "orchestration_id": orchestration_id,
            "type": self.config.name,
            "correlation_id": correlation_id
        })
        
        logger.info(f"Started orchestration {orchestration_id}")
        return orchestration_id
        
    async def get_status(self, orchestration_id: str) -> Optional[OrchestrationResult[T]]:
        """Get orchestration status"""
        return self._executions.get(orchestration_id)
        
    async def cancel(self, orchestration_id: str) -> bool:
        """Cancel running orchestration"""
        if orchestration_id not in self._running_tasks:
            return False
            
        task = self._running_tasks[orchestration_id]
        task.cancel()
        
        if orchestration_id in self._executions:
            self._executions[orchestration_id].status = OrchestrationStatus.CANCELLED
            
        await self._publish_event("orchestration.cancelled", {
            "orchestration_id": orchestration_id
        })
        
        logger.info(f"Cancelled orchestration {orchestration_id}")
        return True
        
    async def pause(self, orchestration_id: str) -> bool:
        """Pause orchestration"""
        if orchestration_id in self._executions:
            self._executions[orchestration_id].status = OrchestrationStatus.PAUSED
            await self._save_checkpoint(orchestration_id)
            return True
        return False
        
    async def resume(self, orchestration_id: str) -> bool:
        """Resume paused orchestration"""
        if orchestration_id not in self._executions:
            return False
            
        execution = self._executions[orchestration_id]
        if execution.status != OrchestrationStatus.PAUSED:
            return False
            
        # Load checkpoint and resume
        checkpoint = await self._load_checkpoint(orchestration_id)
        if checkpoint:
            task = asyncio.create_task(
                self._resume_from_checkpoint(orchestration_id, checkpoint)
            )
            self._running_tasks[orchestration_id] = task
            return True
            
        return False
        
    async def _execute_with_lifecycle(
        self,
        orchestration_id: str,
        input_data: Dict[str, Any],
        correlation_id: str
    ):
        """Execute orchestration with full lifecycle management"""
        execution = self._executions[orchestration_id]
        execution.status = OrchestrationStatus.INITIALIZING
        
        try:
            # Apply timeout if configured
            if self.config.timeout:
                await asyncio.wait_for(
                    self._execute_with_retry(orchestration_id, input_data, correlation_id),
                    self.config.timeout.total_seconds()
                )
            else:
                await self._execute_with_retry(orchestration_id, input_data, correlation_id)
                
            # Mark as completed
            execution.status = OrchestrationStatus.COMPLETED
            execution.completed_at = datetime.utcnow()
            execution.duration = execution.completed_at - execution.started_at
            
            await self._publish_event("orchestration.completed", {
                "orchestration_id": orchestration_id,
                "duration_seconds": execution.duration.total_seconds()
            })
            
            logger.info(f"Orchestration {orchestration_id} completed successfully")
            
        except asyncio.TimeoutError:
            await self._handle_timeout(orchestration_id)
            
        except asyncio.CancelledError:
            logger.info(f"Orchestration {orchestration_id} was cancelled")
            raise
            
        except Exception as e:
            await self._handle_error(orchestration_id, e)
            
        finally:
            # Cleanup
            self._running_tasks.pop(orchestration_id, None)
            await self._cleanup_orchestration(orchestration_id)
            
    async def _execute_with_retry(
        self,
        orchestration_id: str,
        input_data: Dict[str, Any],
        correlation_id: str
    ):
        """Execute with retry logic"""
        execution = self._executions[orchestration_id]
        retry_count = 0
        last_error = None
        
        while retry_count <= self.config.max_retries:
            try:
                execution.status = OrchestrationStatus.RUNNING
                
                # Execute the actual orchestration
                result = await self._execute(orchestration_id, input_data, correlation_id)
                execution.result = result
                
                # Track success metric
                self.metrics.increment(
                    f"{self.config.name}_success_total",
                    labels={"retry_count": str(retry_count)}
                )
                
                return result
                
            except Exception as e:
                last_error = e
                retry_count += 1
                
                if retry_count > self.config.max_retries:
                    raise
                    
                # Calculate retry delay with backoff
                delay = self.config.retry_delay.total_seconds() * (
                    self.config.retry_backoff ** (retry_count - 1)
                )
                
                logger.warning(
                    f"Orchestration {orchestration_id} failed (attempt {retry_count}), "
                    f"retrying in {delay}s: {e}"
                )
                
                # Track retry metric
                self.metrics.increment(
                    f"{self.config.name}_retry_total",
                    labels={"attempt": str(retry_count)}
                )
                
                await asyncio.sleep(delay)
                
        # All retries exhausted
        raise last_error or Exception("Max retries exceeded")
        
    @abstractmethod
    async def _execute(
        self,
        orchestration_id: str,
        input_data: Dict[str, Any],
        correlation_id: str
    ) -> T:
        """Execute the actual orchestration logic"""
        pass
        
    async def _handle_timeout(self, orchestration_id: str):
        """Handle orchestration timeout"""
        execution = self._executions[orchestration_id]
        execution.status = OrchestrationStatus.FAILED
        execution.error = OrchestrationError(
            error_type="timeout",
            message=f"Orchestration timed out after {self.config.timeout}",
            recoverable=False
        )
        
        await self._publish_event("orchestration.timeout", {
            "orchestration_id": orchestration_id
        })
        
        logger.error(f"Orchestration {orchestration_id} timed out")
        
    async def _handle_error(self, orchestration_id: str, error: Exception):
        """Handle orchestration error"""
        execution = self._executions[orchestration_id]
        execution.status = OrchestrationStatus.FAILED
        execution.error = OrchestrationError(
            error_type=type(error).__name__,
            message=str(error),
            details={"traceback": str(error.__traceback__)}
        )
        
        await self._publish_event("orchestration.failed", {
            "orchestration_id": orchestration_id,
            "error": str(error)
        })
        
        logger.error(f"Orchestration {orchestration_id} failed: {error}")
        
        # Attempt compensation if supported
        if hasattr(self, "_compensate"):
            await self._attempt_compensation(orchestration_id)
            
    async def _attempt_compensation(self, orchestration_id: str):
        """Attempt to compensate failed orchestration"""
        execution = self._executions[orchestration_id]
        execution.status = OrchestrationStatus.COMPENSATING
        
        try:
            await self._compensate(orchestration_id)
            execution.status = OrchestrationStatus.COMPENSATED
            
            await self._publish_event("orchestration.compensated", {
                "orchestration_id": orchestration_id
            })
            
        except Exception as e:
            logger.error(f"Compensation failed for {orchestration_id}: {e}")
            
    async def _save_checkpoint(self, orchestration_id: str):
        """Save orchestration checkpoint"""
        if not self.config.enable_checkpointing or not self.cache:
            return
            
        checkpoint = await self._create_checkpoint(orchestration_id)
        if checkpoint:
            cache_key = f"orchestration:checkpoint:{orchestration_id}"
            await self.cache.set(cache_key, checkpoint, ttl=86400)  # 24 hours
            
            logger.debug(f"Saved checkpoint for {orchestration_id}")
            
    async def _load_checkpoint(self, orchestration_id: str) -> Optional[Dict[str, Any]]:
        """Load orchestration checkpoint"""
        if not self.cache:
            return None
            
        cache_key = f"orchestration:checkpoint:{orchestration_id}"
        return await self.cache.get(cache_key)
        
    @abstractmethod
    async def _create_checkpoint(self, orchestration_id: str) -> Optional[Dict[str, Any]]:
        """Create checkpoint data"""
        pass
        
    @abstractmethod
    async def _resume_from_checkpoint(
        self,
        orchestration_id: str,
        checkpoint: Dict[str, Any]
    ):
        """Resume from checkpoint"""
        pass
        
    async def _cleanup_orchestration(self, orchestration_id: str):
        """Cleanup orchestration resources"""
        # Remove checkpoints
        if self.cache:
            cache_key = f"orchestration:checkpoint:{orchestration_id}"
            await self.cache.delete(cache_key)
            
        # Additional cleanup
        await self._cleanup_specific(orchestration_id)
        
    async def _cleanup_specific(self, orchestration_id: str):
        """Specific cleanup logic"""
        pass
        
    async def _publish_event(self, event_type: str, data: Dict[str, Any]):
        """Publish orchestration event"""
        if not self.event_bus:
            return
            
        event = Event(
            type=event_type,
            source=f"{self.config.name}:{self.__class__.__name__}",
            data=data,
            timestamp=datetime.utcnow()
        )
        
        await self.event_bus.publish(event)
        
    async def _checkpoint_loop(self):
        """Background checkpoint saving"""
        while self._running:
            try:
                for orchestration_id in list(self._running_tasks.keys()):
                    if orchestration_id in self._executions:
                        execution = self._executions[orchestration_id]
                        if execution.status == OrchestrationStatus.RUNNING:
                            await self._save_checkpoint(orchestration_id)
                            
                await asyncio.sleep(self.config.checkpoint_interval.total_seconds())
                
            except Exception as e:
                logger.error(f"Checkpoint loop error: {e}")
                
    async def _monitoring_loop(self):
        """Background monitoring"""
        while self._running:
            try:
                # Collect metrics
                active_count = len(self._running_tasks)
                self.metrics.gauge(
                    f"{self.config.name}_active_orchestrations",
                    active_count
                )
                
                # Check for stuck orchestrations
                for orchestration_id, execution in self._executions.items():
                    if execution.status == OrchestrationStatus.RUNNING:
                        if execution.started_at:
                            duration = datetime.utcnow() - execution.started_at
                            if self.config.timeout and duration > self.config.timeout:
                                logger.warning(
                                    f"Orchestration {orchestration_id} "
                                    f"exceeding timeout: {duration}"
                                )
                                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                
    async def shutdown(self):
        """Shutdown orchestrator"""
        logger.info(f"Shutting down {self.__class__.__name__}")
        
        self._running = False
        
        # Cancel all running tasks
        for task in self._running_tasks.values():
            task.cancel()
            
        # Wait for tasks to complete
        if self._running_tasks:
            await asyncio.gather(
                *self._running_tasks.values(),
                return_exceptions=True
            )
            
        # Shutdown specific components
        await self._shutdown_components()
        
        logger.info(f"{self.__class__.__name__} shutdown complete")
        
    @abstractmethod
    async def _shutdown_components(self):
        """Shutdown specific components"""
        pass 