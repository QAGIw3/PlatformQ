"""
Saga pattern implementation for distributed transactions.

Provides orchestration and choreography for long-running transactions
with compensation support.
"""

import asyncio
import uuid
from typing import Any, Dict, List, Optional, Callable, Union, TypeVar
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import logging

from ...monitoring import StructuredLogger
from ..events import EventBus, Event

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')


class SagaState(Enum):
    """Saga execution states"""
    PENDING = "pending"
    RUNNING = "running"
    COMPENSATING = "compensating"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATED = "compensated"


class CompensationStrategy(Enum):
    """Compensation strategies"""
    BACKWARD = "backward"  # Compensate in reverse order
    FORWARD = "forward"    # Compensate in forward order
    PARALLEL = "parallel"  # Compensate all in parallel
    CUSTOM = "custom"      # Custom compensation order


@dataclass
class SagaContext:
    """Context for saga execution"""
    saga_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    transaction_id: Optional[str] = None
    state: SagaState = SagaState.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    
    # Step execution tracking
    completed_steps: List[str] = field(default_factory=list)
    compensated_steps: List[str] = field(default_factory=list)
    failed_step: Optional[str] = None
    
    # Data storage
    step_data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Error tracking
    error: Optional[Exception] = None
    compensation_errors: List[Dict[str, Any]] = field(default_factory=list)
    
    def add_step_data(self, step_name: str, data: Any):
        """Add data from step execution"""
        self.step_data[step_name] = data
        
    def get_step_data(self, step_name: str) -> Any:
        """Get data from previous step"""
        return self.step_data.get(step_name)
        
    def mark_step_completed(self, step_name: str):
        """Mark step as completed"""
        if step_name not in self.completed_steps:
            self.completed_steps.append(step_name)
            
    def mark_step_compensated(self, step_name: str):
        """Mark step as compensated"""
        if step_name not in self.compensated_steps:
            self.compensated_steps.append(step_name)
            
    def set_failed(self, step_name: str, error: Exception):
        """Set saga as failed"""
        self.state = SagaState.FAILED
        self.failed_step = step_name
        self.error = error
        
    def add_compensation_error(self, step_name: str, error: Exception):
        """Add compensation error"""
        self.compensation_errors.append({
            "step": step_name,
            "error": str(error),
            "timestamp": datetime.utcnow()
        })


class SagaStep(ABC):
    """
    Abstract base class for saga steps.
    
    Each step must implement execute and compensate methods.
    """
    
    def __init__(self, name: str):
        self.name = name
        self.retryable = True
        self.timeout = 30.0  # seconds
        
    @abstractmethod
    async def execute(self, context: SagaContext) -> Any:
        """Execute the step forward action"""
        pass
        
    @abstractmethod
    async def compensate(self, context: SagaContext) -> None:
        """Execute the step compensation action"""
        pass
        
    async def can_execute(self, context: SagaContext) -> bool:
        """Check if step can be executed"""
        return True
        
    async def can_compensate(self, context: SagaContext) -> bool:
        """Check if step can be compensated"""
        return self.name in context.completed_steps


class LambdaSagaStep(SagaStep):
    """Saga step using lambda functions"""
    
    def __init__(
        self,
        name: str,
        execute_func: Callable[[SagaContext], Any],
        compensate_func: Callable[[SagaContext], None]
    ):
        super().__init__(name)
        self._execute_func = execute_func
        self._compensate_func = compensate_func
        
    async def execute(self, context: SagaContext) -> Any:
        """Execute the step"""
        if asyncio.iscoroutinefunction(self._execute_func):
            return await self._execute_func(context)
        else:
            return self._execute_func(context)
            
    async def compensate(self, context: SagaContext) -> None:
        """Compensate the step"""
        if asyncio.iscoroutinefunction(self._compensate_func):
            await self._compensate_func(context)
        else:
            self._compensate_func(context)


@dataclass
class SagaDefinition:
    """Saga definition with steps and configuration"""
    name: str
    steps: List[SagaStep]
    compensation_strategy: CompensationStrategy = CompensationStrategy.BACKWARD
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def validate(self) -> bool:
        """Validate saga definition"""
        if not self.steps:
            raise ValueError("Saga must have at least one step")
            
        step_names = [step.name for step in self.steps]
        if len(step_names) != len(set(step_names)):
            raise ValueError("Saga steps must have unique names")
            
        return True


class SagaTransaction:
    """
    Represents a single saga transaction execution.
    """
    
    def __init__(
        self,
        definition: SagaDefinition,
        context: Optional[SagaContext] = None
    ):
        self.definition = definition
        self.context = context or SagaContext()
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
    @property
    def duration(self) -> Optional[float]:
        """Get transaction duration in seconds"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
        
    @property
    def is_completed(self) -> bool:
        """Check if transaction is completed"""
        return self.context.state in [
            SagaState.COMPLETED,
            SagaState.FAILED,
            SagaState.COMPENSATED
        ]


class SagaOrchestrator:
    """
    Orchestrates saga execution with compensation support.
    
    Features:
    - Step-by-step execution
    - Automatic compensation on failure
    - Multiple compensation strategies
    - Event publishing
    - Metrics collection
    """
    
    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        self.event_bus = event_bus
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        self._active_transactions: Dict[str, SagaTransaction] = {}
        self._completed_transactions: List[SagaTransaction] = []
        
        self._metrics = {
            "started": 0,
            "completed": 0,
            "failed": 0,
            "compensated": 0,
            "active": 0
        }
        
    async def execute(
        self,
        definition: SagaDefinition,
        initial_data: Optional[Dict[str, Any]] = None
    ) -> SagaContext:
        """
        Execute a saga transaction.
        
        Args:
            definition: Saga definition
            initial_data: Initial context data
            
        Returns:
            Saga execution context
        """
        # Validate definition
        definition.validate()
        
        # Create transaction
        context = SagaContext(metadata=initial_data or {})
        transaction = SagaTransaction(definition, context)
        
        # Track transaction
        self._active_transactions[context.saga_id] = transaction
        self._metrics["started"] += 1
        self._metrics["active"] += 1
        
        # Start execution
        transaction.start_time = datetime.utcnow()
        context.state = SagaState.RUNNING
        
        # Publish start event
        await self._publish_event("saga.started", context)
        
        try:
            # Execute steps
            await self._execute_steps(transaction)
            
            # Mark as completed
            context.state = SagaState.COMPLETED
            context.completed_at = datetime.utcnow()
            self._metrics["completed"] += 1
            
            # Publish completion event
            await self._publish_event("saga.completed", context)
            
        except Exception as e:
            logger.error(f"Saga {context.saga_id} failed: {e}")
            
            # Start compensation
            await self._compensate(transaction)
            
        finally:
            # Clean up
            transaction.end_time = datetime.utcnow()
            del self._active_transactions[context.saga_id]
            self._completed_transactions.append(transaction)
            self._metrics["active"] -= 1
            
        return context
        
    async def _execute_steps(self, transaction: SagaTransaction):
        """Execute saga steps"""
        context = transaction.context
        
        for step in transaction.definition.steps:
            try:
                # Check if can execute
                if not await step.can_execute(context):
                    logger.info(f"Skipping step {step.name} - cannot execute")
                    continue
                    
                logger.info(f"Executing step {step.name}")
                
                # Execute with timeout
                result = await asyncio.wait_for(
                    step.execute(context),
                    timeout=step.timeout
                )
                
                # Store result
                context.add_step_data(step.name, result)
                context.mark_step_completed(step.name)
                
                # Publish step event
                await self._publish_event("saga.step.completed", {
                    "saga_id": context.saga_id,
                    "step": step.name,
                    "result": result
                })
                
            except Exception as e:
                # Mark as failed
                context.set_failed(step.name, e)
                
                # Publish failure event
                await self._publish_event("saga.step.failed", {
                    "saga_id": context.saga_id,
                    "step": step.name,
                    "error": str(e)
                })
                
                raise
                
    async def _compensate(self, transaction: SagaTransaction):
        """Compensate failed saga"""
        context = transaction.context
        context.state = SagaState.COMPENSATING
        
        logger.info(f"Starting compensation for saga {context.saga_id}")
        
        # Publish compensation start event
        await self._publish_event("saga.compensation.started", context)
        
        # Get steps to compensate
        steps_to_compensate = self._get_compensation_steps(transaction)
        
        # Execute compensation
        compensation_success = True
        
        for step in steps_to_compensate:
            try:
                if not await step.can_compensate(context):
                    logger.info(f"Skipping compensation for {step.name}")
                    continue
                    
                logger.info(f"Compensating step {step.name}")
                
                # Compensate with timeout
                await asyncio.wait_for(
                    step.compensate(context),
                    timeout=step.timeout
                )
                
                context.mark_step_compensated(step.name)
                
                # Publish step compensation event
                await self._publish_event("saga.step.compensated", {
                    "saga_id": context.saga_id,
                    "step": step.name
                })
                
            except Exception as e:
                logger.error(f"Compensation failed for step {step.name}: {e}")
                context.add_compensation_error(step.name, e)
                compensation_success = False
                
                # Continue with other compensations
                
        # Update final state
        if compensation_success:
            context.state = SagaState.COMPENSATED
            self._metrics["compensated"] += 1
        else:
            context.state = SagaState.FAILED
            self._metrics["failed"] += 1
            
        # Publish compensation result
        await self._publish_event("saga.compensation.completed", {
            "saga_id": context.saga_id,
            "success": compensation_success,
            "errors": context.compensation_errors
        })
        
    def _get_compensation_steps(self, transaction: SagaTransaction) -> List[SagaStep]:
        """Get steps for compensation based on strategy"""
        completed_step_names = transaction.context.completed_steps
        all_steps = transaction.definition.steps
        
        # Get completed steps
        completed_steps = [
            step for step in all_steps
            if step.name in completed_step_names
        ]
        
        strategy = transaction.definition.compensation_strategy
        
        if strategy == CompensationStrategy.BACKWARD:
            # Reverse order
            return list(reversed(completed_steps))
            
        elif strategy == CompensationStrategy.FORWARD:
            # Same order
            return completed_steps
            
        elif strategy == CompensationStrategy.PARALLEL:
            # Return all for parallel execution
            return completed_steps
            
        else:
            # Custom strategy - implement as needed
            return completed_steps
            
    async def _publish_event(self, event_type: str, data: Any):
        """Publish saga event"""
        if self.event_bus:
            event = Event(
                type=event_type,
                data=data,
                timestamp=datetime.utcnow()
            )
            await self.event_bus.publish(event)
            
    def get_transaction(self, saga_id: str) -> Optional[SagaTransaction]:
        """Get active transaction by ID"""
        return self._active_transactions.get(saga_id)
        
    def get_metrics(self) -> Dict[str, int]:
        """Get orchestrator metrics"""
        return self._metrics.copy()
        
    async def cancel(self, saga_id: str) -> bool:
        """Cancel running saga"""
        transaction = self._active_transactions.get(saga_id)
        if not transaction:
            return False
            
        # Trigger compensation
        await self._compensate(transaction)
        return True


class SagaBuilder:
    """
    Builder for creating saga definitions.
    
    Example:
        saga = (SagaBuilder("order-processing")
            .add_step("validate", validate_order, compensate_validation)
            .add_step("payment", process_payment, refund_payment)
            .add_step("shipping", create_shipment, cancel_shipment)
            .with_compensation_strategy(CompensationStrategy.BACKWARD)
            .build())
    """
    
    def __init__(self, name: str):
        self.name = name
        self.steps: List[SagaStep] = []
        self.compensation_strategy = CompensationStrategy.BACKWARD
        self.metadata: Dict[str, Any] = {}
        
    def add_step(
        self,
        name: str,
        execute_func: Callable,
        compensate_func: Callable
    ) -> 'SagaBuilder':
        """Add a step to the saga"""
        step = LambdaSagaStep(name, execute_func, compensate_func)
        self.steps.append(step)
        return self
        
    def add_custom_step(self, step: SagaStep) -> 'SagaBuilder':
        """Add a custom step implementation"""
        self.steps.append(step)
        return self
        
    def with_compensation_strategy(
        self,
        strategy: CompensationStrategy
    ) -> 'SagaBuilder':
        """Set compensation strategy"""
        self.compensation_strategy = strategy
        return self
        
    def with_metadata(self, metadata: Dict[str, Any]) -> 'SagaBuilder':
        """Add metadata"""
        self.metadata.update(metadata)
        return self
        
    def build(self) -> SagaDefinition:
        """Build saga definition"""
        return SagaDefinition(
            name=self.name,
            steps=self.steps,
            compensation_strategy=self.compensation_strategy,
            metadata=self.metadata
        ) 