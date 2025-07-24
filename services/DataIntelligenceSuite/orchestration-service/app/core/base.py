"""
Orchestration Service Base Classes

Migrated to use the unified data-intelligence-common library.
"""

from typing import Dict, Any, List, Optional, Union, Callable, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import uuid
from collections import defaultdict

from data_intelligence_common.base_service import DataIntelligenceBaseService
from data_intelligence_common.core.config.unified import UnifiedServiceConfig
from data_intelligence_common.core.events import (
    Event, EventType, EventRouter, BaseEventProcessor,
    EventProcessingConfig, EventProcessingMode
)
from data_intelligence_common.core.processing import ProcessingContext
from data_intelligence_common.core.patterns.factory import FactoryRegistry
from data_intelligence_common.core.mixins import StateMixin
from data_intelligence_common.monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class WorkflowStatus(str, Enum):
    """Workflow execution status"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class TaskStatus(str, Enum):
    """Task execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class TriggerType(str, Enum):
    """Workflow trigger types"""
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    EVENT = "event"
    API = "api"
    DEPENDENCY = "dependency"


@dataclass
class OrchestrationConfig(UnifiedServiceConfig):
    """Configuration for orchestration service"""
    # Workflow settings
    max_concurrent_workflows: int = 100
    max_concurrent_tasks: int = 1000
    default_task_timeout: timedelta = field(default_factory=lambda: timedelta(hours=1))
    
    # Scheduling
    enable_scheduling: bool = True
    scheduler_interval: timedelta = field(default_factory=lambda: timedelta(seconds=60))
    
    # Event-driven settings
    enable_event_triggers: bool = True
    event_processing_mode: EventProcessingMode = EventProcessingMode.PARALLEL
    max_event_retries: int = 3
    
    # State management
    state_backend: str = "ignite"
    enable_checkpointing: bool = True
    checkpoint_interval: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    
    # Monitoring
    enable_workflow_monitoring: bool = True
    enable_sla_monitoring: bool = True
    
    # Recovery
    enable_auto_recovery: bool = True
    recovery_window: timedelta = field(default_factory=lambda: timedelta(hours=24))


@dataclass
class WorkflowDefinition:
    """Workflow definition"""
    workflow_id: str
    name: str
    description: str
    tasks: List['TaskDefinition']
    triggers: List[Dict[str, Any]]
    config: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_task(self, task_id: str) -> Optional['TaskDefinition']:
        """Get task by ID"""
        for task in self.tasks:
            if task.task_id == task_id:
                return task
        return None


@dataclass
class TaskDefinition:
    """Task definition"""
    task_id: str
    name: str
    task_type: str
    config: Dict[str, Any]
    dependencies: List[str] = field(default_factory=list)
    retry_policy: Dict[str, Any] = field(default_factory=dict)
    timeout: Optional[timedelta] = None
    
    def can_execute(self, completed_tasks: Set[str]) -> bool:
        """Check if task can be executed"""
        return all(dep in completed_tasks for dep in self.dependencies)


class OrchestrationService(DataIntelligenceBaseService, StateMixin):
    """
    Orchestration service for workflow management.
    
    Provides workflow execution, scheduling, and event-driven orchestration.
    """
    
    def __init__(self, config: OrchestrationConfig):
        super().__init__(config)
        self.config = config
        
        # Workflow registry
        self._workflows: Dict[str, WorkflowDefinition] = {}
        self._workflow_instances: Dict[str, 'WorkflowInstance'] = {}
        
        # Event routing
        self._event_router = EventRouter()
        self._event_processor = None
        
        # Task executors
        self._task_executors: FactoryRegistry = FactoryRegistry()
        
        # Scheduling
        self._scheduled_workflows: Dict[str, asyncio.Task] = {}
        
        # Concurrency control
        self._workflow_semaphore = asyncio.Semaphore(config.max_concurrent_workflows)
        self._task_semaphore = asyncio.Semaphore(config.max_concurrent_tasks)
        
    async def _initialize_internal(self):
        """Initialize orchestration components"""
        await super()._initialize_internal()
        
        # Initialize event processor
        if self.config.enable_event_triggers:
            await self._initialize_event_processor()
            
        # Initialize task executors
        self._initialize_task_executors()
        
        # Register health checks
        self.register_health_check(
            "workflow_engine",
            self._check_workflow_engine_health,
            critical=True
        )
        
        # Start background tasks
        if self.config.enable_scheduling:
            self._start_background_task(self._scheduler_loop())
            
        if self.config.enable_workflow_monitoring:
            self._start_background_task(self._monitor_workflows_loop())
            
        logger.info("Orchestration service initialized")
        
    async def _initialize_event_processor(self):
        """Initialize event-driven workflow processing"""
        # Create event processing configuration
        event_config = EventProcessingConfig(
            name="workflow_event_processor",
            processing_mode=self.config.event_processing_mode,
            max_concurrent=self.config.max_concurrent_workflows,
            max_retries=self.config.event_max_retries,
            enable_deduplication=True
        )
        
        # Create custom event processor
        self._event_processor = WorkflowEventProcessor(
            config=event_config,
            orchestration_service=self,
            event_bus=self.event_bus
        )
        
        await self._event_processor.start()
        
    def _initialize_task_executors(self):
        """Initialize task executor registry"""
        # Register built-in executors
        from ..executors.http_executor import HttpTaskExecutor
        from ..executors.database_executor import DatabaseTaskExecutor
        from ..executors.script_executor import ScriptTaskExecutor
        from ..executors.service_executor import ServiceTaskExecutor
        
        self._task_executors.register("http", HttpTaskExecutor)
        self._task_executors.register("database", DatabaseTaskExecutor)
        self._task_executors.register("script", ScriptTaskExecutor)
        self._task_executors.register("service", ServiceTaskExecutor)
        
    async def register_workflow(
        self,
        workflow_def: WorkflowDefinition
    ) -> Dict[str, Any]:
        """Register a workflow definition"""
        try:
            # Validate workflow
            self._validate_workflow(workflow_def)
            
            # Store workflow
            self._workflows[workflow_def.workflow_id] = workflow_def
            
            # Set up triggers
            for trigger in workflow_def.triggers:
                await self._setup_trigger(workflow_def.workflow_id, trigger)
                
            # Emit registration event
            await self.publish_event(
                event_type="workflow.registered",
                data={
                    "workflow_id": workflow_def.workflow_id,
                    "name": workflow_def.name,
                    "triggers": len(workflow_def.triggers)
                }
            )
            
            # Record metrics
            self.record_operation("workflow_registered", {
                "workflow_id": workflow_def.workflow_id
            })
            
            return {
                "workflow_id": workflow_def.workflow_id,
                "status": "registered",
                "triggers_configured": len(workflow_def.triggers)
            }
            
        except Exception as e:
            self.record_error("workflow_registration_failed", e)
            raise
            
    def _validate_workflow(self, workflow_def: WorkflowDefinition):
        """Validate workflow definition"""
        # Check for circular dependencies
        task_ids = {task.task_id for task in workflow_def.tasks}
        
        for task in workflow_def.tasks:
            # Check dependencies exist
            for dep in task.dependencies:
                if dep not in task_ids:
                    raise ValueError(f"Task {task.task_id} has unknown dependency: {dep}")
                    
        # Check for cycles using DFS
        visited = set()
        rec_stack = set()
        
        def has_cycle(task_id: str) -> bool:
            visited.add(task_id)
            rec_stack.add(task_id)
            
            task = workflow_def.get_task(task_id)
            for dep in task.dependencies:
                if dep not in visited:
                    if has_cycle(dep):
                        return True
                elif dep in rec_stack:
                    return True
                    
            rec_stack.remove(task_id)
            return False
            
        for task in workflow_def.tasks:
            if task.task_id not in visited:
                if has_cycle(task.task_id):
                    raise ValueError("Workflow contains circular dependencies")
                    
    async def _setup_trigger(self, workflow_id: str, trigger: Dict[str, Any]):
        """Set up workflow trigger"""
        trigger_type = TriggerType(trigger["type"])
        
        if trigger_type == TriggerType.SCHEDULED:
            # Set up scheduled trigger
            schedule = trigger["schedule"]
            task = asyncio.create_task(
                self._scheduled_trigger_loop(workflow_id, schedule)
            )
            self._scheduled_workflows[f"{workflow_id}:{schedule}"] = task
            
        elif trigger_type == TriggerType.EVENT:
            # Set up event trigger
            event_pattern = trigger["event_pattern"]
            
            # Add route to event router
            self._event_router.add_route(
                event_pattern,
                lambda event: asyncio.create_task(
                    self._handle_event_trigger(workflow_id, event)
                )
            )
            
            # Subscribe to events
            await self._event_processor.subscribe(
                topic_pattern=trigger.get("topic", "workflow.events"),
                event_types=[event_pattern]
            )
            
    async def execute_workflow(
        self,
        workflow_id: str,
        input_data: Optional[Dict[str, Any]] = None,
        trigger_info: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute a workflow"""
        workflow_def = self._workflows.get(workflow_id)
        if not workflow_def:
            raise ValueError(f"Workflow {workflow_id} not found")
            
        # Create workflow instance
        instance_id = str(uuid.uuid4())
        instance = WorkflowInstance(
            instance_id=instance_id,
            workflow_def=workflow_def,
            input_data=input_data or {},
            trigger_info=trigger_info or {"type": TriggerType.MANUAL}
        )
        
        # Store instance
        self._workflow_instances[instance_id] = instance
        
        # Update state
        await self.set_state(f"workflow:{instance_id}:status", WorkflowStatus.PENDING)
        
        # Emit start event
        await self.publish_event(
            event_type="workflow.started",
            data={
                "instance_id": instance_id,
                "workflow_id": workflow_id,
                "trigger": trigger_info
            }
        )
        
        # Execute workflow with concurrency control
        async with self._workflow_semaphore:
            try:
                result = await self._execute_workflow_instance(instance)
                
                # Record metrics
                self.record_operation("workflow_executed", {
                    "workflow_id": workflow_id,
                    "status": result["status"],
                    "duration": result.get("duration", 0)
                })
                
                return result
                
            except Exception as e:
                # Update state
                await self.set_state(f"workflow:{instance_id}:status", WorkflowStatus.FAILED)
                
                # Emit failure event
                await self.publish_event(
                    event_type="workflow.failed",
                    data={
                        "instance_id": instance_id,
                        "error": str(e)
                    }
                )
                
                self.record_error("workflow_execution_failed", e)
                raise
                
    async def _execute_workflow_instance(
        self,
        instance: 'WorkflowInstance'
    ) -> Dict[str, Any]:
        """Execute a workflow instance"""
        start_time = datetime.utcnow()
        instance.status = WorkflowStatus.RUNNING
        
        # Update state
        await self.set_state(
            f"workflow:{instance.instance_id}:status",
            WorkflowStatus.RUNNING
        )
        
        try:
            # Create execution context
            context = ProcessingContext(
                job_id=instance.instance_id,
                config=None,  # Would use workflow config
                metadata={
                    "workflow_id": instance.workflow_def.workflow_id,
                    "input_data": instance.input_data
                }
            )
            
            # Execute tasks in dependency order
            completed_tasks = set()
            task_results = {}
            
            while len(completed_tasks) < len(instance.workflow_def.tasks):
                # Find executable tasks
                executable_tasks = [
                    task for task in instance.workflow_def.tasks
                    if task.task_id not in completed_tasks
                    and task.can_execute(completed_tasks)
                ]
                
                if not executable_tasks:
                    # No tasks can execute - deadlock
                    raise RuntimeError("Workflow deadlock detected")
                    
                # Execute tasks in parallel
                task_futures = []
                for task in executable_tasks:
                    future = asyncio.create_task(
                        self._execute_task(instance, task, context, task_results)
                    )
                    task_futures.append((task.task_id, future))
                    
                # Wait for tasks to complete
                for task_id, future in task_futures:
                    try:
                        result = await future
                        task_results[task_id] = result
                        completed_tasks.add(task_id)
                        instance.completed_tasks.add(task_id)
                        
                    except Exception as e:
                        # Task failed
                        instance.failed_tasks.add(task_id)
                        
                        # Check if workflow should fail
                        if not task.retry_policy.get("continue_on_failure", False):
                            raise
                            
            # Workflow completed successfully
            instance.status = WorkflowStatus.COMPLETED
            await self.set_state(
                f"workflow:{instance.instance_id}:status",
                WorkflowStatus.COMPLETED
            )
            
            # Emit completion event
            await self.publish_event(
                event_type="workflow.completed",
                data={
                    "instance_id": instance.instance_id,
                    "duration": (datetime.utcnow() - start_time).total_seconds(),
                    "task_results": task_results
                }
            )
            
            return {
                "instance_id": instance.instance_id,
                "status": "completed",
                "duration": (datetime.utcnow() - start_time).total_seconds(),
                "results": task_results
            }
            
        except Exception as e:
            instance.status = WorkflowStatus.FAILED
            raise
            
    async def _execute_task(
        self,
        instance: 'WorkflowInstance',
        task: TaskDefinition,
        context: ProcessingContext,
        task_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute a single task"""
        async with self._task_semaphore:
            start_time = datetime.utcnow()
            
            # Update task status
            await self.set_state(
                f"task:{instance.instance_id}:{task.task_id}:status",
                TaskStatus.RUNNING
            )
            
            try:
                # Get task executor
                executor = self._task_executors.create(task.task_type)
                
                # Prepare task input
                task_input = {
                    "config": task.config,
                    "workflow_input": instance.input_data,
                    "dependencies": {
                        dep: task_results.get(dep, {})
                        for dep in task.dependencies
                    }
                }
                
                # Execute task with timeout
                timeout = task.timeout or self.config.default_task_timeout
                result = await asyncio.wait_for(
                    executor.execute(task_input, context),
                    timeout=timeout.total_seconds()
                )
                
                # Update task status
                await self.set_state(
                    f"task:{instance.instance_id}:{task.task_id}:status",
                    TaskStatus.COMPLETED
                )
                
                # Emit task completion event
                await self.publish_event(
                    event_type="task.completed",
                    data={
                        "instance_id": instance.instance_id,
                        "task_id": task.task_id,
                        "duration": (datetime.utcnow() - start_time).total_seconds()
                    }
                )
                
                return result
                
            except asyncio.TimeoutError:
                # Task timed out
                await self.set_state(
                    f"task:{instance.instance_id}:{task.task_id}:status",
                    TaskStatus.FAILED
                )
                raise RuntimeError(f"Task {task.task_id} timed out")
                
            except Exception as e:
                # Task failed
                await self.set_state(
                    f"task:{instance.instance_id}:{task.task_id}:status",
                    TaskStatus.FAILED
                )
                
                # Check retry policy
                if task.retry_policy.get("max_retries", 0) > 0:
                    # Implement retry logic
                    pass
                    
                raise
                
    async def _handle_event_trigger(self, workflow_id: str, event: Event):
        """Handle event-triggered workflow execution"""
        try:
            # Extract input data from event
            input_data = event.payload
            
            # Create trigger info
            trigger_info = {
                "type": TriggerType.EVENT,
                "event_type": event.event_type,
                "event_id": event.event_id
            }
            
            # Execute workflow
            await self.execute_workflow(
                workflow_id=workflow_id,
                input_data=input_data,
                trigger_info=trigger_info
            )
            
        except Exception as e:
            logger.error(f"Failed to handle event trigger for workflow {workflow_id}: {e}")
            
    async def _scheduled_trigger_loop(self, workflow_id: str, schedule: str):
        """Handle scheduled workflow execution"""
        # This would implement cron-like scheduling
        # For now, simple interval-based
        interval = int(schedule)  # seconds
        
        while True:
            try:
                await asyncio.sleep(interval)
                
                # Create trigger info
                trigger_info = {
                    "type": TriggerType.SCHEDULED,
                    "schedule": schedule,
                    "triggered_at": datetime.utcnow().isoformat()
                }
                
                # Execute workflow
                await self.execute_workflow(
                    workflow_id=workflow_id,
                    trigger_info=trigger_info
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Failed to execute scheduled workflow {workflow_id}: {e}")
                
    async def _scheduler_loop(self):
        """Main scheduler loop"""
        while True:
            try:
                await asyncio.sleep(self.config.scheduler_interval.total_seconds())
                
                # Check for workflows to schedule
                # This would implement more sophisticated scheduling logic
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                
    async def _monitor_workflows_loop(self):
        """Monitor running workflows"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                # Check for stuck workflows
                for instance_id, instance in self._workflow_instances.items():
                    if instance.status == WorkflowStatus.RUNNING:
                        # Check if workflow is stuck
                        if datetime.utcnow() - instance.started_at > timedelta(hours=24):
                            logger.warning(f"Workflow {instance_id} appears stuck")
                            
                            # Emit monitoring event
                            await self.publish_event(
                                event_type="workflow.stuck",
                                data={
                                    "instance_id": instance_id,
                                    "duration": (datetime.utcnow() - instance.started_at).total_seconds()
                                }
                            )
                            
                # Clean up old instances
                cutoff_time = datetime.utcnow() - self.config.recovery_window
                old_instances = [
                    instance_id for instance_id, instance in self._workflow_instances.items()
                    if instance.completed_at and instance.completed_at < cutoff_time
                ]
                
                for instance_id in old_instances:
                    del self._workflow_instances[instance_id]
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in workflow monitoring: {e}")
                
    async def _check_workflow_engine_health(self) -> Dict[str, Any]:
        """Check workflow engine health"""
        active_workflows = sum(
            1 for instance in self._workflow_instances.values()
            if instance.status == WorkflowStatus.RUNNING
        )
        
        return {
            "healthy": True,
            "registered_workflows": len(self._workflows),
            "active_workflows": active_workflows,
            "total_instances": len(self._workflow_instances)
        }
        
    async def _stop_internal(self):
        """Stop orchestration components"""
        # Cancel scheduled workflows
        for task in self._scheduled_workflows.values():
            if not task.done():
                task.cancel()
                
        # Stop event processor
        if self._event_processor:
            await self._event_processor.stop()
            
        await super()._stop_internal()
        
        logger.info("Orchestration service stopped")


@dataclass
class WorkflowInstance:
    """Runtime workflow instance"""
    instance_id: str
    workflow_def: WorkflowDefinition
    input_data: Dict[str, Any]
    trigger_info: Dict[str, Any]
    status: WorkflowStatus = WorkflowStatus.PENDING
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    completed_tasks: Set[str] = field(default_factory=set)
    failed_tasks: Set[str] = field(default_factory=set)
    context: Dict[str, Any] = field(default_factory=dict)


class WorkflowEventProcessor(BaseEventProcessor):
    """Event processor for workflow triggers"""
    
    def __init__(
        self,
        config: EventProcessingConfig,
        orchestration_service: OrchestrationService,
        **kwargs
    ):
        super().__init__(event_bus=kwargs.get('event_bus'), config=config)
        self.orchestration_service = orchestration_service
        
    async def process_event(self, event: Event):
        """Process event and trigger workflows"""
        # Route event through event router
        await self.orchestration_service._event_router.route(event)


# Export main components
__all__ = [
    'WorkflowStatus',
    'TaskStatus',
    'TriggerType',
    'OrchestrationConfig',
    'WorkflowDefinition',
    'TaskDefinition',
    'OrchestrationService'
] 