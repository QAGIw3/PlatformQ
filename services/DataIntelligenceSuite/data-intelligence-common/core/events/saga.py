"""
Saga Orchestrator for DataIntelligenceSuite

Provides distributed transaction management using the Saga pattern.
"""

import logging
from typing import Any, Dict, Optional, List, Callable, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import uuid

from .event_bus import Event, EventBus

logger = logging.getLogger(__name__)


class SagaStatus(Enum):
    """Saga execution status"""
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


@dataclass
class SagaStep:
    """Definition of a saga step"""
    step_id: str
    name: str
    action: Callable[[Dict[str, Any]], Any]
    compensation: Optional[Callable[[Dict[str, Any]], Any]] = None
    timeout: Optional[timedelta] = None
    retry_count: int = 0
    retry_delay: timedelta = timedelta(seconds=1)
    
    # Step dependencies
    depends_on: List[str] = field(default_factory=list)
    
    # Step configuration
    is_critical: bool = True  # If false, failure doesn't trigger compensation
    can_retry: bool = True
    
    # Runtime state
    status: str = "pending"
    result: Optional[Any] = None
    error: Optional[str] = None
    attempts: int = 0


@dataclass
class SagaContext:
    """Context passed between saga steps"""
    saga_id: str
    correlation_id: str
    data: Dict[str, Any] = field(default_factory=dict)
    step_results: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get value from context"""
        return self.data.get(key, default)
        
    def set(self, key: str, value: Any):
        """Set value in context"""
        self.data[key] = value
        
    def get_step_result(self, step_id: str) -> Any:
        """Get result from previous step"""
        return self.step_results.get(step_id)


@dataclass
class SagaDefinition:
    """Saga definition"""
    saga_type: str
    name: str
    steps: List[SagaStep]
    compensation_strategy: CompensationStrategy = CompensationStrategy.BACKWARD
    timeout: Optional[timedelta] = None
    
    def get_step(self, step_id: str) -> Optional[SagaStep]:
        """Get step by ID"""
        for step in self.steps:
            if step.step_id == step_id:
                return step
        return None


@dataclass
class SagaExecution:
    """Saga execution state"""
    saga_id: str
    saga_type: str
    status: SagaStatus
    context: SagaContext
    started_at: datetime
    completed_at: Optional[datetime] = None
    
    # Execution state
    current_step: Optional[str] = None
    executed_steps: List[str] = field(default_factory=list)
    compensated_steps: List[str] = field(default_factory=list)
    
    # Error information
    error: Optional[str] = None
    failed_step: Optional[str] = None


class SagaOrchestrator:
    """
    Orchestrates saga execution for distributed transactions.
    
    Features:
    - Step orchestration with dependencies
    - Automatic compensation on failure
    - Retry logic with backoff
    - Timeout handling
    - Event-driven coordination
    """
    
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self._definitions: Dict[str, SagaDefinition] = {}
        self._executions: Dict[str, SagaExecution] = {}
        self._running_tasks: Dict[str, asyncio.Task] = {}
        
    def register_saga(self, definition: SagaDefinition):
        """Register saga definition"""
        self._definitions[definition.saga_type] = definition
        logger.info(f"Registered saga: {definition.saga_type}")
        
    async def start_saga(
        self,
        saga_type: str,
        initial_data: Dict[str, Any],
        correlation_id: Optional[str] = None
    ) -> str:
        """Start new saga execution"""
        if saga_type not in self._definitions:
            raise ValueError(f"Unknown saga type: {saga_type}")
            
        # Create execution
        saga_id = str(uuid.uuid4())
        correlation_id = correlation_id or saga_id
        
        context = SagaContext(
            saga_id=saga_id,
            correlation_id=correlation_id,
            data=initial_data.copy()
        )
        
        execution = SagaExecution(
            saga_id=saga_id,
            saga_type=saga_type,
            status=SagaStatus.PENDING,
            context=context,
            started_at=datetime.utcnow()
        )
        
        self._executions[saga_id] = execution
        
        # Start execution task
        task = asyncio.create_task(self._execute_saga(execution))
        self._running_tasks[saga_id] = task
        
        # Publish saga started event
        await self._publish_event("saga.started", execution)
        
        logger.info(f"Started saga {saga_id} of type {saga_type}")
        return saga_id
        
    async def get_saga_status(self, saga_id: str) -> Optional[SagaExecution]:
        """Get saga execution status"""
        return self._executions.get(saga_id)
        
    async def _execute_saga(self, execution: SagaExecution):
        """Execute saga steps"""
        definition = self._definitions[execution.saga_type]
        execution.status = SagaStatus.RUNNING
        
        try:
            # Apply timeout if configured
            if definition.timeout:
                await asyncio.wait_for(
                    self._execute_steps(execution, definition),
                    definition.timeout.total_seconds()
                )
            else:
                await self._execute_steps(execution, definition)
                
            # Mark as completed
            execution.status = SagaStatus.COMPLETED
            execution.completed_at = datetime.utcnow()
            
            await self._publish_event("saga.completed", execution)
            logger.info(f"Saga {execution.saga_id} completed successfully")
            
        except asyncio.TimeoutError:
            logger.error(f"Saga {execution.saga_id} timed out")
            execution.error = "Saga execution timed out"
            await self._compensate_saga(execution, definition)
            
        except Exception as e:
            logger.error(f"Saga {execution.saga_id} failed: {e}")
            execution.error = str(e)
            await self._compensate_saga(execution, definition)
            
        finally:
            # Clean up
            self._running_tasks.pop(execution.saga_id, None)
            
    async def _execute_steps(self, execution: SagaExecution, definition: SagaDefinition):
        """Execute saga steps in order"""
        # Get execution order respecting dependencies
        execution_order = self._get_execution_order(definition.steps)
        
        for step_id in execution_order:
            step = definition.get_step(step_id)
            if not step:
                continue
                
            execution.current_step = step_id
            
            try:
                # Execute step with retry
                result = await self._execute_step_with_retry(step, execution.context)
                
                # Store result
                step.status = "completed"
                step.result = result
                execution.context.step_results[step_id] = result
                execution.executed_steps.append(step_id)
                
                # Publish step completed event
                await self._publish_event("saga.step.completed", {
                    "saga_id": execution.saga_id,
                    "step_id": step_id,
                    "result": result
                })
                
            except Exception as e:
                logger.error(f"Step {step_id} failed: {e}")
                step.status = "failed"
                step.error = str(e)
                execution.failed_step = step_id
                
                # Check if step is critical
                if step.is_critical:
                    raise  # Trigger compensation
                else:
                    # Log and continue
                    logger.warning(f"Non-critical step {step_id} failed, continuing")
                    
    async def _execute_step_with_retry(self, step: SagaStep, context: SagaContext) -> Any:
        """Execute step with retry logic"""
        last_error = None
        
        for attempt in range(step.retry_count + 1):
            step.attempts = attempt + 1
            
            try:
                # Apply timeout if configured
                if step.timeout:
                    result = await asyncio.wait_for(
                        self._execute_step_action(step.action, context),
                        step.timeout.total_seconds()
                    )
                else:
                    result = await self._execute_step_action(step.action, context)
                    
                return result
                
            except Exception as e:
                last_error = e
                
                if attempt < step.retry_count and step.can_retry:
                    logger.warning(f"Step {step.step_id} attempt {attempt + 1} failed, retrying")
                    await asyncio.sleep(step.retry_delay.total_seconds())
                else:
                    break
                    
        # All retries failed
        raise last_error
        
    async def _execute_step_action(self, action: Callable, context: SagaContext) -> Any:
        """Execute step action"""
        if asyncio.iscoroutinefunction(action):
            return await action(context)
        else:
            return action(context)
            
    async def _compensate_saga(self, execution: SagaExecution, definition: SagaDefinition):
        """Compensate saga by running compensation actions"""
        execution.status = SagaStatus.COMPENSATING
        
        await self._publish_event("saga.compensating", execution)
        
        try:
            # Get steps to compensate based on strategy
            steps_to_compensate = self._get_compensation_order(
                execution.executed_steps,
                definition
            )
            
            # Execute compensations
            if definition.compensation_strategy == CompensationStrategy.PARALLEL:
                # Compensate all in parallel
                tasks = []
                for step_id in steps_to_compensate:
                    step = definition.get_step(step_id)
                    if step and step.compensation:
                        task = self._compensate_step(step, execution)
                        tasks.append(task)
                        
                await asyncio.gather(*tasks, return_exceptions=True)
                
            else:
                # Compensate sequentially
                for step_id in steps_to_compensate:
                    step = definition.get_step(step_id)
                    if step and step.compensation:
                        await self._compensate_step(step, execution)
                        
            execution.status = SagaStatus.COMPENSATED
            execution.completed_at = datetime.utcnow()
            
            await self._publish_event("saga.compensated", execution)
            logger.info(f"Saga {execution.saga_id} compensated successfully")
            
        except Exception as e:
            logger.error(f"Compensation failed for saga {execution.saga_id}: {e}")
            execution.status = SagaStatus.FAILED
            execution.completed_at = datetime.utcnow()
            
            await self._publish_event("saga.failed", execution)
            
    async def _compensate_step(self, step: SagaStep, execution: SagaExecution):
        """Compensate single step"""
        try:
            logger.info(f"Compensating step {step.step_id}")
            
            await self._execute_step_action(step.compensation, execution.context)
            
            execution.compensated_steps.append(step.step_id)
            
            await self._publish_event("saga.step.compensated", {
                "saga_id": execution.saga_id,
                "step_id": step.step_id
            })
            
        except Exception as e:
            logger.error(f"Failed to compensate step {step.step_id}: {e}")
            # Continue with other compensations
            
    def _get_execution_order(self, steps: List[SagaStep]) -> List[str]:
        """Get step execution order respecting dependencies"""
        # Simple topological sort
        visited = set()
        order = []
        
        def visit(step: SagaStep):
            if step.step_id in visited:
                return
                
            visited.add(step.step_id)
            
            # Visit dependencies first
            for dep_id in step.depends_on:
                dep_step = next((s for s in steps if s.step_id == dep_id), None)
                if dep_step:
                    visit(dep_step)
                    
            order.append(step.step_id)
            
        for step in steps:
            visit(step)
            
        return order
        
    def _get_compensation_order(
        self,
        executed_steps: List[str],
        definition: SagaDefinition
    ) -> List[str]:
        """Get compensation order based on strategy"""
        if definition.compensation_strategy == CompensationStrategy.BACKWARD:
            return list(reversed(executed_steps))
        elif definition.compensation_strategy == CompensationStrategy.FORWARD:
            return executed_steps
        else:  # PARALLEL
            return executed_steps
            
    async def _publish_event(self, event_type: str, data: Any):
        """Publish saga event"""
        event_data = data
        if isinstance(data, SagaExecution):
            event_data = {
                "saga_id": data.saga_id,
                "saga_type": data.saga_type,
                "status": data.status.value,
                "error": data.error
            }
            
        event = Event(
            event_type=event_type,
            source="saga_orchestrator",
            correlation_id=data.get("saga_id") if isinstance(data, dict) else data.saga_id,
            payload=event_data
        )
        
        await self.event_bus.publish(f"saga.{event_type}", event) 