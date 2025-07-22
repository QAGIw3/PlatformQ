"""
Pipeline Event Processor

Processes events related to pipeline orchestration.
"""

from typing import Dict, Any
import asyncio

from data_intelligence_common import BaseEventProcessor, StructuredLogger
from platformq_shared.event_subscriber import EventSubscriber

logger = StructuredLogger.get_logger(__name__)


class PipelineEventProcessor(BaseEventProcessor):
    """
    Processes pipeline orchestration events
    """
    
    def __init__(
        self,
        event_subscriber: EventSubscriber,
        coordinator,
        executor,
        monitor
    ):
        super().__init__(event_subscriber)
        self.coordinator = coordinator
        self.executor = executor
        self.monitor = monitor
        
        # Configure event routing
        self._configure_routes()
    
    def _configure_routes(self):
        """Configure event routing"""
        # Pipeline management events
        self.router.add_route(
            "pipeline.create.requested",
            self._handle_create_pipeline
        )
        self.router.add_route(
            "pipeline.update.requested",
            self._handle_update_pipeline
        )
        self.router.add_route(
            "pipeline.delete.requested",
            self._handle_delete_pipeline
        )
        
        # Execution events
        self.router.add_route(
            "pipeline.execute.requested",
            self._handle_execute_pipeline
        )
        self.router.add_route(
            "pipeline.cancel.requested",
            self._handle_cancel_pipeline
        )
        
        # Step completion events from other services
        self.router.add_route(
            "pipeline.extract.completed",
            self._handle_step_completed
        )
        self.router.add_route(
            "pipeline.transform.completed",
            self._handle_step_completed
        )
        self.router.add_route(
            "pipeline.load.completed",
            self._handle_step_completed
        )
        self.router.add_route(
            "data.quality.check.completed",
            self._handle_step_completed
        )
        
        # Monitoring events
        self.router.add_route(
            "pipeline.execution.started",
            self._handle_execution_started
        )
        self.router.add_route(
            "pipeline.execution.completed",
            self._handle_execution_completed
        )
        self.router.add_route(
            "pipeline.step.started",
            self._handle_step_started
        )
        self.router.add_route(
            "pipeline.step.completed",
            self._handle_step_completed_monitoring
        )
        
        # Trigger events
        self.router.add_route(
            "dataset.updated",
            self._handle_dataset_event
        )
        self.router.add_route(
            "schedule.trigger",
            self._handle_schedule_trigger
        )
    
    async def _handle_create_pipeline(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle pipeline creation request"""
        try:
            pipeline_config = event.get("config", {})
            
            pipeline = await self.coordinator.repository.create_pipeline(
                name=pipeline_config.get("name"),
                type=pipeline_config.get("type"),
                config=pipeline_config.get("config", {}),
                description=pipeline_config.get("description", ""),
                schedule=pipeline_config.get("schedule"),
                dependencies=pipeline_config.get("dependencies"),
                tags=pipeline_config.get("tags"),
                owner=event.get("owner")
            )
            
            logger.info("pipeline_created_via_event",
                       pipeline_id=pipeline.id,
                       name=pipeline.name)
            
            return {
                "success": True,
                "pipeline_id": pipeline.id
            }
            
        except Exception as e:
            logger.error("create_pipeline_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_update_pipeline(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle pipeline update request"""
        try:
            pipeline_id = event.get("pipeline_id")
            updates = event.get("updates", {})
            
            pipeline = await self.coordinator.repository.update_pipeline(
                pipeline_id,
                updates
            )
            
            if pipeline:
                logger.info("pipeline_updated_via_event",
                           pipeline_id=pipeline_id)
                return {
                    "success": True,
                    "pipeline_id": pipeline_id
                }
            else:
                return {
                    "success": False,
                    "error": "Pipeline not found"
                }
                
        except Exception as e:
            logger.error("update_pipeline_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_delete_pipeline(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle pipeline deletion request"""
        try:
            pipeline_id = event.get("pipeline_id")
            
            success = await self.coordinator.repository.delete_pipeline(pipeline_id)
            
            if success:
                logger.info("pipeline_deleted_via_event",
                           pipeline_id=pipeline_id)
            
            return {
                "success": success,
                "pipeline_id": pipeline_id
            }
            
        except Exception as e:
            logger.error("delete_pipeline_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_execute_pipeline(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle pipeline execution request"""
        try:
            pipeline_id = event.get("pipeline_id")
            context = event.get("context", {})
            
            # Get pipeline definition
            pipeline = await self.coordinator.repository.get_pipeline(pipeline_id)
            if not pipeline:
                return {
                    "success": False,
                    "error": "Pipeline not found"
                }
            
            # Execute pipeline
            execution_id = await self.executor.execute_pipeline(
                pipeline_id=pipeline_id,
                pipeline_config=pipeline.config,
                context=context
            )
            
            logger.info("pipeline_execution_started_via_event",
                       pipeline_id=pipeline_id,
                       execution_id=execution_id)
            
            return {
                "success": True,
                "execution_id": execution_id
            }
            
        except Exception as e:
            logger.error("execute_pipeline_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_cancel_pipeline(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle pipeline cancellation request"""
        try:
            execution_id = event.get("execution_id")
            
            success = await self.executor.cancel_execution(execution_id)
            
            return {
                "success": success,
                "execution_id": execution_id
            }
            
        except Exception as e:
            logger.error("cancel_pipeline_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_step_completed(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle step completion from other services"""
        try:
            execution_id = event.get("execution_id")
            step_name = event.get("step_name")
            result = event.get("result", {})
            
            logger.info("external_step_completed",
                       execution_id=execution_id,
                       step_name=step_name)
            
            # Update execution state
            # This would coordinate with the executor to continue pipeline
            
            return {"success": True}
            
        except Exception as e:
            logger.error("step_completed_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_execution_started(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle execution started event for monitoring"""
        try:
            execution_id = event.get("execution_id")
            pipeline_id = event.get("pipeline_id")
            
            await self.monitor.record_execution_start(
                execution_id,
                pipeline_id
            )
            
            return {"success": True}
            
        except Exception as e:
            logger.error("execution_started_monitoring_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_execution_completed(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle execution completed event for monitoring"""
        try:
            execution_id = event.get("execution_id")
            status = event.get("status")
            error_count = event.get("error_count", 0)
            
            await self.monitor.record_execution_completion(
                execution_id,
                status,
                error_count
            )
            
            return {"success": True}
            
        except Exception as e:
            logger.error("execution_completed_monitoring_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_step_started(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle step started event"""
        try:
            # Log step start
            logger.info("step_started_event",
                       execution_id=event.get("execution_id"),
                       step_name=event.get("step_name"))
            
            return {"success": True}
            
        except Exception as e:
            logger.error("step_started_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_step_completed_monitoring(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle step completed event for monitoring"""
        try:
            execution_id = event.get("execution_id")
            step_name = event.get("step_name")
            status = event.get("status", "completed")
            duration_seconds = event.get("duration_seconds", 0)
            
            await self.monitor.record_step_completion(
                execution_id,
                step_name,
                status,
                duration_seconds
            )
            
            return {"success": True}
            
        except Exception as e:
            logger.error("step_completed_monitoring_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_dataset_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle dataset events that might trigger pipelines"""
        try:
            dataset = event.get("dataset")
            event_type = event.get("type")
            
            logger.info("dataset_event_received",
                       dataset=dataset,
                       event_type=event_type)
            
            # Check for pipelines triggered by this dataset
            # This would integrate with the scheduler
            
            return {"success": True}
            
        except Exception as e:
            logger.error("dataset_event_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_schedule_trigger(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle scheduled trigger event"""
        try:
            pipeline_id = event.get("pipeline_id")
            schedule_id = event.get("schedule_id")
            
            logger.info("schedule_trigger_received",
                       pipeline_id=pipeline_id,
                       schedule_id=schedule_id)
            
            # Trigger pipeline execution
            execution_id = await self.coordinator.scheduler.trigger_pipeline(
                pipeline_id,
                trigger_type="scheduled",
                parameters={"schedule_id": schedule_id}
            )
            
            return {
                "success": True,
                "execution_id": execution_id
            }
            
        except Exception as e:
            logger.error("schedule_trigger_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            } 