"""
Workflow orchestrator for DAG-based workflow execution.

Provides workflow management with dependencies, parallel execution, and state tracking.
"""

import asyncio
from typing import Any, Dict, List, Optional, Set, Callable, Union
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
import networkx as nx

from .base_orchestrator import (
    BaseOrchestrator,
    OrchestrationConfig,
    OrchestrationResult,
    OrchestrationStatus
)
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class WorkflowStatus(str, Enum):
    """Workflow step status"""
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


@dataclass
class StepDependency:
    """Step dependency definition"""
    step_id: str
    condition: Optional[Callable[[Dict[str, Any]], bool]] = None
    required: bool = True


@dataclass
class WorkflowStep:
    """Workflow step definition"""
    step_id: str
    name: str
    handler: Union[Callable, str]  # Callable or handler name
    dependencies: List[StepDependency] = field(default_factory=list)
    timeout: Optional[timedelta] = None
    retries: int = 3
    parallel: bool = True
    config: Dict[str, Any] = field(default_factory=dict)
    
    # Runtime state
    status: WorkflowStatus = WorkflowStatus.PENDING
    result: Optional[Any] = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    attempts: int = 0


@dataclass
class WorkflowDefinition:
    """Workflow definition"""
    workflow_id: str
    name: str
    description: str
    steps: List[WorkflowStep]
    timeout: Optional[timedelta] = None
    max_parallel: int = 10
    on_failure: str = "fail"  # fail, continue, compensate
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        # Build step index
        self.step_index = {step.step_id: step for step in self.steps}
        
        # Build dependency graph
        self.graph = nx.DiGraph()
        for step in self.steps:
            self.graph.add_node(step.step_id)
            for dep in step.dependencies:
                self.graph.add_edge(dep.step_id, step.step_id)
                
        # Validate graph
        if not nx.is_directed_acyclic_graph(self.graph):
            raise ValueError("Workflow contains cycles")


class WorkflowOrchestrator(BaseOrchestrator[Dict[str, Any]]):
    """
    Orchestrates DAG-based workflows.
    
    Features:
    - Dependency resolution
    - Parallel step execution
    - Conditional branching
    - Step retry logic
    - Workflow state tracking
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._definitions: Dict[str, WorkflowDefinition] = {}
        self._handlers: Dict[str, Callable] = {}
        self._workflow_states: Dict[str, Dict[str, WorkflowStep]] = {}
        
    async def _initialize_components(self):
        """Initialize workflow components"""
        # Register built-in handlers
        self._register_builtin_handlers()
        
    def register_workflow(self, definition: WorkflowDefinition):
        """Register workflow definition"""
        self._definitions[definition.workflow_id] = definition
        logger.info(f"Registered workflow: {definition.workflow_id}")
        
    def register_handler(self, name: str, handler: Callable):
        """Register step handler"""
        self._handlers[name] = handler
        
    async def _execute(
        self,
        orchestration_id: str,
        input_data: Dict[str, Any],
        correlation_id: str
    ) -> Dict[str, Any]:
        """Execute workflow"""
        workflow_id = input_data.get("workflow_id")
        if not workflow_id or workflow_id not in self._definitions:
            raise ValueError(f"Unknown workflow: {workflow_id}")
            
        definition = self._definitions[workflow_id]
        
        # Initialize workflow state
        workflow_state = self._initialize_workflow_state(definition)
        self._workflow_states[orchestration_id] = workflow_state
        
        # Create execution context
        context = {
            "orchestration_id": orchestration_id,
            "correlation_id": correlation_id,
            "workflow_id": workflow_id,
            "input": input_data.get("parameters", {}),
            "results": {},
            "metadata": {}
        }
        
        # Execute workflow
        try:
            await self._execute_workflow(
                orchestration_id,
                definition,
                workflow_state,
                context
            )
            
            # Return results
            return {
                "workflow_id": workflow_id,
                "status": "completed",
                "results": context["results"],
                "metadata": context["metadata"]
            }
            
        except Exception as e:
            # Handle failure based on strategy
            if definition.on_failure == "compensate":
                await self._compensate_workflow(
                    orchestration_id,
                    workflow_state,
                    context
                )
            raise
            
    def _initialize_workflow_state(
        self,
        definition: WorkflowDefinition
    ) -> Dict[str, WorkflowStep]:
        """Initialize workflow state"""
        # Create deep copy of steps
        state = {}
        for step in definition.steps:
            state[step.step_id] = WorkflowStep(
                step_id=step.step_id,
                name=step.name,
                handler=step.handler,
                dependencies=step.dependencies.copy(),
                timeout=step.timeout,
                retries=step.retries,
                parallel=step.parallel,
                config=step.config.copy()
            )
        return state
        
    async def _execute_workflow(
        self,
        orchestration_id: str,
        definition: WorkflowDefinition,
        workflow_state: Dict[str, WorkflowStep],
        context: Dict[str, Any]
    ):
        """Execute workflow steps"""
        completed_steps = set()
        running_tasks = {}
        semaphore = asyncio.Semaphore(definition.max_parallel)
        
        while len(completed_steps) < len(workflow_state):
            # Find ready steps
            ready_steps = self._find_ready_steps(
                workflow_state,
                completed_steps,
                context
            )
            
            if not ready_steps and not running_tasks:
                # No steps ready and nothing running - deadlock
                raise RuntimeError("Workflow deadlock detected")
                
            # Start ready steps
            for step_id in ready_steps:
                if len(running_tasks) >= definition.max_parallel:
                    break
                    
                step = workflow_state[step_id]
                task = asyncio.create_task(
                    self._execute_step_with_semaphore(
                        step,
                        context,
                        semaphore
                    )
                )
                running_tasks[step_id] = task
                
            # Wait for at least one task to complete
            if running_tasks:
                done, pending = await asyncio.wait(
                    running_tasks.values(),
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Process completed tasks
                for task in done:
                    # Find which step completed
                    for step_id, step_task in running_tasks.items():
                        if step_task == task:
                            del running_tasks[step_id]
                            
                            try:
                                await task
                                completed_steps.add(step_id)
                                
                                # Publish step completed event
                                await self._publish_event("workflow.step.completed", {
                                    "orchestration_id": orchestration_id,
                                    "workflow_id": definition.workflow_id,
                                    "step_id": step_id
                                })
                                
                            except Exception as e:
                                step = workflow_state[step_id]
                                step.status = WorkflowStatus.FAILED
                                step.error = str(e)
                                
                                # Handle step failure
                                if definition.on_failure == "fail":
                                    # Cancel remaining tasks
                                    for t in running_tasks.values():
                                        t.cancel()
                                    raise
                                elif definition.on_failure == "continue":
                                    completed_steps.add(step_id)
                                    
                            break
                            
    def _find_ready_steps(
        self,
        workflow_state: Dict[str, WorkflowStep],
        completed_steps: Set[str],
        context: Dict[str, Any]
    ) -> List[str]:
        """Find steps ready to execute"""
        ready = []
        
        for step_id, step in workflow_state.items():
            if step_id in completed_steps:
                continue
                
            if step.status != WorkflowStatus.PENDING:
                continue
                
            # Check dependencies
            dependencies_met = True
            for dep in step.dependencies:
                if dep.step_id not in completed_steps:
                    dependencies_met = False
                    break
                    
                # Check condition if specified
                if dep.condition:
                    dep_result = context["results"].get(dep.step_id)
                    if not dep.condition(dep_result):
                        if dep.required:
                            dependencies_met = False
                            break
                        else:
                            # Optional dependency failed - skip step
                            step.status = WorkflowStatus.SKIPPED
                            completed_steps.add(step_id)
                            dependencies_met = False
                            break
                            
            if dependencies_met:
                ready.append(step_id)
                step.status = WorkflowStatus.READY
                
        return ready
        
    async def _execute_step_with_semaphore(
        self,
        step: WorkflowStep,
        context: Dict[str, Any],
        semaphore: asyncio.Semaphore
    ):
        """Execute step with semaphore control"""
        async with semaphore:
            await self._execute_step(step, context)
            
    async def _execute_step(
        self,
        step: WorkflowStep,
        context: Dict[str, Any]
    ):
        """Execute individual step"""
        step.status = WorkflowStatus.RUNNING
        step.started_at = datetime.utcnow()
        
        # Get handler
        if isinstance(step.handler, str):
            handler = self._handlers.get(step.handler)
            if not handler:
                raise ValueError(f"Unknown handler: {step.handler}")
        else:
            handler = step.handler
            
        # Execute with retry
        last_error = None
        for attempt in range(step.retries + 1):
            step.attempts = attempt + 1
            
            try:
                # Apply timeout if configured
                if step.timeout:
                    result = await asyncio.wait_for(
                        handler(context, step.config),
                        step.timeout.total_seconds()
                    )
                else:
                    result = await handler(context, step.config)
                    
                # Store result
                step.result = result
                step.status = WorkflowStatus.COMPLETED
                step.completed_at = datetime.utcnow()
                context["results"][step.step_id] = result
                
                logger.info(f"Step {step.step_id} completed successfully")
                return
                
            except asyncio.TimeoutError:
                last_error = f"Step timed out after {step.timeout}"
                logger.warning(f"Step {step.step_id} timed out (attempt {attempt + 1})")
                
            except Exception as e:
                last_error = str(e)
                logger.warning(
                    f"Step {step.step_id} failed (attempt {attempt + 1}): {e}"
                )
                
            if attempt < step.retries:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
                
        # All retries exhausted
        step.status = WorkflowStatus.FAILED
        step.error = last_error
        raise RuntimeError(f"Step {step.step_id} failed: {last_error}")
        
    async def _compensate_workflow(
        self,
        orchestration_id: str,
        workflow_state: Dict[str, WorkflowStep],
        context: Dict[str, Any]
    ):
        """Compensate workflow by running compensation handlers"""
        # Run compensation in reverse order of completion
        completed_steps = [
            step for step in workflow_state.values()
            if step.status == WorkflowStatus.COMPLETED
        ]
        completed_steps.sort(key=lambda s: s.completed_at, reverse=True)
        
        for step in completed_steps:
            compensation_handler = self._handlers.get(f"{step.handler}_compensate")
            if compensation_handler:
                try:
                    await compensation_handler(context, step.config)
                    logger.info(f"Compensated step {step.step_id}")
                except Exception as e:
                    logger.error(f"Failed to compensate step {step.step_id}: {e}")
                    
    def _register_builtin_handlers(self):
        """Register built-in step handlers"""
        
        async def log_handler(context: Dict[str, Any], config: Dict[str, Any]):
            """Log message handler"""
            message = config.get("message", "Step executed")
            level = config.get("level", "info")
            getattr(logger, level)(message, **context)
            
        async def delay_handler(context: Dict[str, Any], config: Dict[str, Any]):
            """Delay handler"""
            seconds = config.get("seconds", 1)
            await asyncio.sleep(seconds)
            
        async def condition_handler(context: Dict[str, Any], config: Dict[str, Any]):
            """Conditional handler"""
            condition = config.get("condition")
            if condition:
                return eval(condition, {"context": context})
            return True
            
        self.register_handler("log", log_handler)
        self.register_handler("delay", delay_handler)
        self.register_handler("condition", condition_handler)
        
    async def _create_checkpoint(self, orchestration_id: str) -> Optional[Dict[str, Any]]:
        """Create workflow checkpoint"""
        if orchestration_id not in self._workflow_states:
            return None
            
        workflow_state = self._workflow_states[orchestration_id]
        
        return {
            "workflow_state": {
                step_id: {
                    "status": step.status,
                    "result": step.result,
                    "error": step.error,
                    "attempts": step.attempts
                }
                for step_id, step in workflow_state.items()
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    async def _resume_from_checkpoint(
        self,
        orchestration_id: str,
        checkpoint: Dict[str, Any]
    ):
        """Resume workflow from checkpoint"""
        # Restore workflow state
        if "workflow_state" in checkpoint:
            for step_id, state_data in checkpoint["workflow_state"].items():
                if orchestration_id in self._workflow_states:
                    if step_id in self._workflow_states[orchestration_id]:
                        step = self._workflow_states[orchestration_id][step_id]
                        step.status = WorkflowStatus(state_data["status"])
                        step.result = state_data.get("result")
                        step.error = state_data.get("error")
                        step.attempts = state_data.get("attempts", 0)
                        
    async def _shutdown_components(self):
        """Shutdown workflow components"""
        # Clear definitions and handlers
        self._definitions.clear()
        self._handlers.clear()
        self._workflow_states.clear() 