"""
Pipeline Scheduler

Manages pipeline scheduling, triggers, and execution timing.
"""

from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
import asyncio
from croniter import croniter
import uuid

from data_intelligence_common import StructuredLogger
from data_intelligence_common.vault_consul import VaultConsulIntegration
from .pipeline_repository import PipelineRepository, PipelineStatus

logger = StructuredLogger.get_logger(__name__)


class ScheduleType:
    """Schedule types"""
    ONCE = "once"
    INTERVAL = "interval"
    CRON = "cron"
    EVENT = "event"
    MANUAL = "manual"


class ScheduledTask:
    """Scheduled task representation"""
    
    def __init__(
        self,
        task_id: str,
        pipeline_id: str,
        schedule_type: str,
        schedule_config: Dict[str, Any],
        next_run: Optional[datetime] = None,
        last_run: Optional[datetime] = None,
        enabled: bool = True
    ):
        self.task_id = task_id
        self.pipeline_id = pipeline_id
        self.schedule_type = schedule_type
        self.schedule_config = schedule_config
        self.next_run = next_run
        self.last_run = last_run
        self.enabled = enabled
        self.task_handle: Optional[asyncio.Task] = None


class PipelineScheduler:
    """
    Manages pipeline scheduling and execution triggers
    """
    
    def __init__(
        self,
        repository: PipelineRepository,
        coordinator,
        vault_consul: VaultConsulIntegration
    ):
        self.repository = repository
        self.coordinator = coordinator
        self.vault_consul = vault_consul
        
        # Scheduled tasks
        self.scheduled_tasks: Dict[str, ScheduledTask] = {}
        
        # Background tasks
        self.scheduler_task: Optional[asyncio.Task] = None
        self.is_running = False
        
        # Execution callback
        self.execution_callback: Optional[Callable] = None
    
    async def start(self):
        """Start the scheduler"""
        logger.info("starting_pipeline_scheduler")
        
        self.is_running = True
        
        # Load scheduled pipelines
        await self._load_scheduled_pipelines()
        
        # Start scheduler loop
        self.scheduler_task = asyncio.create_task(self._scheduler_loop())
        
        logger.info("pipeline_scheduler_started", 
                   scheduled_count=len(self.scheduled_tasks))
    
    async def stop(self):
        """Stop the scheduler"""
        logger.info("stopping_pipeline_scheduler")
        
        self.is_running = False
        
        # Cancel scheduler task
        if self.scheduler_task:
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass
        
        # Cancel all scheduled tasks
        for task in self.scheduled_tasks.values():
            if task.task_handle and not task.task_handle.done():
                task.task_handle.cancel()
        
        logger.info("pipeline_scheduler_stopped")
    
    async def _load_scheduled_pipelines(self):
        """Load pipelines with schedules"""
        pipelines = await self.repository.list_pipelines(status=PipelineStatus.ACTIVE)
        
        for pipeline in pipelines:
            if pipeline.schedule:
                await self.schedule_pipeline(
                    pipeline.id,
                    pipeline.schedule
                )
    
    async def schedule_pipeline(
        self,
        pipeline_id: str,
        schedule_config: Dict[str, Any]
    ) -> str:
        """Schedule a pipeline for execution"""
        schedule_type = schedule_config.get("type", ScheduleType.MANUAL)
        
        # Create scheduled task
        task_id = str(uuid.uuid4())
        task = ScheduledTask(
            task_id=task_id,
            pipeline_id=pipeline_id,
            schedule_type=schedule_type,
            schedule_config=schedule_config
        )
        
        # Calculate next run time
        task.next_run = self._calculate_next_run(task)
        
        # Add to scheduled tasks
        self.scheduled_tasks[task_id] = task
        
        logger.info("pipeline_scheduled",
                   task_id=task_id,
                   pipeline_id=pipeline_id,
                   schedule_type=schedule_type,
                   next_run=task.next_run)
        
        return task_id
    
    async def unschedule_pipeline(self, task_id: str) -> bool:
        """Unschedule a pipeline"""
        task = self.scheduled_tasks.get(task_id)
        if not task:
            return False
        
        # Cancel task if running
        if task.task_handle and not task.task_handle.done():
            task.task_handle.cancel()
        
        # Remove from scheduled tasks
        del self.scheduled_tasks[task_id]
        
        logger.info("pipeline_unscheduled", task_id=task_id)
        return True
    
    async def trigger_pipeline(
        self,
        pipeline_id: str,
        trigger_type: str = "manual",
        parameters: Optional[Dict[str, Any]] = None
    ) -> str:
        """Manually trigger a pipeline execution"""
        execution_id = str(uuid.uuid4())
        
        logger.info("pipeline_triggered",
                   pipeline_id=pipeline_id,
                   execution_id=execution_id,
                   trigger_type=trigger_type)
        
        # Execute pipeline
        asyncio.create_task(
            self._execute_pipeline(
                pipeline_id,
                execution_id,
                trigger_type,
                parameters
            )
        )
        
        return execution_id
    
    async def _scheduler_loop(self):
        """Main scheduler loop"""
        while self.is_running:
            try:
                now = datetime.utcnow()
                
                # Check scheduled tasks
                for task_id, task in list(self.scheduled_tasks.items()):
                    if not task.enabled:
                        continue
                    
                    if task.next_run and task.next_run <= now:
                        # Execute pipeline
                        execution_id = str(uuid.uuid4())
                        task.task_handle = asyncio.create_task(
                            self._execute_pipeline(
                                task.pipeline_id,
                                execution_id,
                                "scheduled",
                                {"schedule_task_id": task_id}
                            )
                        )
                        
                        # Update task
                        task.last_run = now
                        task.next_run = self._calculate_next_run(task)
                        
                        logger.info("scheduled_execution_started",
                                   task_id=task_id,
                                   pipeline_id=task.pipeline_id,
                                   execution_id=execution_id)
                
                # Sleep for a short interval
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("scheduler_loop_error", error=str(e))
                await asyncio.sleep(60)  # Wait longer on error
    
    def _calculate_next_run(self, task: ScheduledTask) -> Optional[datetime]:
        """Calculate next run time for a task"""
        now = datetime.utcnow()
        schedule_type = task.schedule_type
        config = task.schedule_config
        
        if schedule_type == ScheduleType.ONCE:
            # Run once at specified time
            run_at = config.get("run_at")
            if run_at:
                return datetime.fromisoformat(run_at)
            return None
            
        elif schedule_type == ScheduleType.INTERVAL:
            # Run at regular intervals
            interval = config.get("interval_seconds", 3600)
            if task.last_run:
                return task.last_run + timedelta(seconds=interval)
            return now + timedelta(seconds=interval)
            
        elif schedule_type == ScheduleType.CRON:
            # Run based on cron expression
            cron_expr = config.get("cron_expression")
            if cron_expr:
                try:
                    cron = croniter(cron_expr, now)
                    return cron.get_next(datetime)
                except Exception as e:
                    logger.error("invalid_cron_expression",
                               expression=cron_expr,
                               error=str(e))
            return None
            
        elif schedule_type == ScheduleType.EVENT:
            # Event-driven, no scheduled time
            return None
            
        else:
            # Manual or unknown
            return None
    
    async def _execute_pipeline(
        self,
        pipeline_id: str,
        execution_id: str,
        trigger_type: str,
        parameters: Optional[Dict[str, Any]] = None
    ):
        """Execute a pipeline"""
        try:
            # Get pipeline definition
            pipeline = await self.repository.get_pipeline(pipeline_id)
            if not pipeline:
                logger.error("pipeline_not_found", pipeline_id=pipeline_id)
                return
            
            # Check if pipeline is active
            if pipeline.status != PipelineStatus.ACTIVE:
                logger.warning("pipeline_not_active",
                             pipeline_id=pipeline_id,
                             status=pipeline.status.value)
                return
            
            # Prepare execution context
            context = {
                "execution_id": execution_id,
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline.name,
                "trigger_type": trigger_type,
                "trigger_time": datetime.utcnow().isoformat(),
                "parameters": parameters or {}
            }
            
            # Execute through coordinator
            await self.coordinator.execute_pipeline(
                pipeline_id=pipeline_id,
                execution_id=execution_id,
                context=context
            )
            
        except Exception as e:
            logger.error("pipeline_execution_error",
                        pipeline_id=pipeline_id,
                        execution_id=execution_id,
                        error=str(e))
    
    async def get_scheduled_tasks(
        self,
        pipeline_id: Optional[str] = None,
        enabled_only: bool = True
    ) -> List[Dict[str, Any]]:
        """Get scheduled tasks"""
        tasks = []
        
        for task in self.scheduled_tasks.values():
            if pipeline_id and task.pipeline_id != pipeline_id:
                continue
            
            if enabled_only and not task.enabled:
                continue
            
            tasks.append({
                "task_id": task.task_id,
                "pipeline_id": task.pipeline_id,
                "schedule_type": task.schedule_type,
                "schedule_config": task.schedule_config,
                "next_run": task.next_run.isoformat() if task.next_run else None,
                "last_run": task.last_run.isoformat() if task.last_run else None,
                "enabled": task.enabled,
                "is_running": task.task_handle is not None and not task.task_handle.done()
            })
        
        return tasks
    
    async def update_schedule(
        self,
        task_id: str,
        updates: Dict[str, Any]
    ) -> bool:
        """Update a scheduled task"""
        task = self.scheduled_tasks.get(task_id)
        if not task:
            return False
        
        # Update schedule config
        if "schedule_config" in updates:
            task.schedule_config.update(updates["schedule_config"])
            # Recalculate next run
            task.next_run = self._calculate_next_run(task)
        
        # Update enabled status
        if "enabled" in updates:
            task.enabled = updates["enabled"]
        
        logger.info("schedule_updated",
                   task_id=task_id,
                   updates=list(updates.keys()))
        
        return True
    
    async def get_execution_history(
        self,
        pipeline_id: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get execution history"""
        # This would integrate with the monitor/executor for history
        # For now, return placeholder
        return [] 