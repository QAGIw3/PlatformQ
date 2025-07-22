"""
Pipeline Executor

Manages pipeline execution, step orchestration, and error handling.
"""

from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import asyncio
import json
from enum import Enum
import traceback
import uuid

from data_intelligence_common import StructuredLogger
from data_intelligence_common.vault_consul import VaultConsulIntegration
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.errors import ValidationError, ServiceError

logger = StructuredLogger.get_logger(__name__)


class ExecutionStatus(Enum):
    """Pipeline execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class StepStatus(Enum):
    """Pipeline step status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRY = "retry"


class PipelineExecution:
    """Pipeline execution instance"""
    
    def __init__(
        self,
        execution_id: str,
        pipeline_id: str,
        pipeline_name: str,
        context: Dict[str, Any]
    ):
        self.execution_id = execution_id
        self.pipeline_id = pipeline_id
        self.pipeline_name = pipeline_name
        self.context = context
        self.status = ExecutionStatus.PENDING
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.current_step: Optional[str] = None
        self.steps: Dict[str, Dict[str, Any]] = {}
        self.results: Dict[str, Any] = {}
        self.errors: List[Dict[str, Any]] = []
        self.metadata: Dict[str, Any] = {}


class PipelineExecutor:
    """
    Executes pipelines and manages their lifecycle
    """
    
    def __init__(
        self,
        coordinator,
        monitor,
        vault_consul: VaultConsulIntegration,
        event_publisher: Optional[EventPublisher] = None
    ):
        self.coordinator = coordinator
        self.monitor = monitor
        self.vault_consul = vault_consul
        self.event_publisher = event_publisher
        
        # Active executions
        self.executions: Dict[str, PipelineExecution] = {}
        
        # Execution history (limited)
        self.execution_history: List[PipelineExecution] = []
        self.max_history_size = 1000
        
        # Step executors (pluggable)
        self.step_executors: Dict[str, Any] = {}
        self._register_default_executors()
    
    async def initialize(self):
        """Initialize executor"""
        logger.info("initializing_pipeline_executor")
        
        # Load any persisted executions
        await self._load_persisted_executions()
        
        logger.info("pipeline_executor_initialized")
    
    async def cleanup(self):
        """Cleanup executor"""
        logger.info("cleaning_up_pipeline_executor")
        
        # Cancel any running executions
        for execution in list(self.executions.values()):
            if execution.status == ExecutionStatus.RUNNING:
                await self.cancel_execution(execution.execution_id)
        
        logger.info("pipeline_executor_cleaned_up")
    
    async def is_healthy(self) -> bool:
        """Check executor health"""
        # Basic health check
        return True
    
    def _register_default_executors(self):
        """Register default step executors"""
        self.step_executors = {
            "extract": self._execute_extract_step,
            "transform": self._execute_transform_step,
            "load": self._execute_load_step,
            "validate": self._execute_validate_step,
            "quality_check": self._execute_quality_check_step,
            "aggregate": self._execute_aggregate_step,
            "custom": self._execute_custom_step
        }
    
    async def execute_pipeline(
        self,
        pipeline_id: str,
        pipeline_config: Dict[str, Any],
        context: Dict[str, Any]
    ) -> str:
        """Execute a pipeline"""
        execution_id = context.get("execution_id", str(uuid.uuid4()))
        pipeline_name = context.get("pipeline_name", pipeline_id)
        
        # Create execution instance
        execution = PipelineExecution(
            execution_id=execution_id,
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
            context=context
        )
        
        # Add to active executions
        self.executions[execution_id] = execution
        
        # Start execution
        asyncio.create_task(
            self._run_pipeline(execution, pipeline_config)
        )
        
        return execution_id
    
    async def _run_pipeline(
        self,
        execution: PipelineExecution,
        pipeline_config: Dict[str, Any]
    ):
        """Run pipeline execution"""
        try:
            # Update status
            execution.status = ExecutionStatus.RUNNING
            execution.started_at = datetime.utcnow()
            
            # Publish start event
            await self._publish_event(
                "pipeline.execution.started",
                {
                    "execution_id": execution.execution_id,
                    "pipeline_id": execution.pipeline_id,
                    "pipeline_name": execution.pipeline_name,
                    "context": execution.context
                }
            )
            
            # Execute steps
            steps = pipeline_config.get("steps", [])
            for i, step_config in enumerate(steps):
                step_name = step_config.get("name", f"step_{i}")
                
                # Check if execution was cancelled
                if execution.status == ExecutionStatus.CANCELLED:
                    break
                
                # Execute step
                await self._execute_step(
                    execution,
                    step_name,
                    step_config
                )
                
                # Check if step failed
                step_result = execution.steps.get(step_name, {})
                if step_result.get("status") == StepStatus.FAILED.value:
                    execution.status = ExecutionStatus.FAILED
                    break
            
            # Complete execution
            if execution.status == ExecutionStatus.RUNNING:
                execution.status = ExecutionStatus.COMPLETED
            
            execution.completed_at = datetime.utcnow()
            
            # Publish completion event
            await self._publish_event(
                "pipeline.execution.completed",
                {
                    "execution_id": execution.execution_id,
                    "pipeline_id": execution.pipeline_id,
                    "status": execution.status.value,
                    "duration_seconds": (
                        execution.completed_at - execution.started_at
                    ).total_seconds()
                }
            )
            
        except Exception as e:
            logger.error("pipeline_execution_error",
                        execution_id=execution.execution_id,
                        error=str(e),
                        traceback=traceback.format_exc())
            
            execution.status = ExecutionStatus.FAILED
            execution.errors.append({
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e),
                "traceback": traceback.format_exc()
            })
            
            # Publish failure event
            await self._publish_event(
                "pipeline.execution.failed",
                {
                    "execution_id": execution.execution_id,
                    "pipeline_id": execution.pipeline_id,
                    "error": str(e)
                }
            )
            
        finally:
            # Move to history
            self._move_to_history(execution)
    
    async def _execute_step(
        self,
        execution: PipelineExecution,
        step_name: str,
        step_config: Dict[str, Any]
    ):
        """Execute a pipeline step"""
        step_type = step_config.get("type", "custom")
        
        logger.info("executing_step",
                   execution_id=execution.execution_id,
                   step_name=step_name,
                   step_type=step_type)
        
        # Initialize step tracking
        execution.current_step = step_name
        execution.steps[step_name] = {
            "type": step_type,
            "status": StepStatus.RUNNING.value,
            "started_at": datetime.utcnow().isoformat(),
            "config": step_config
        }
        
        # Publish step start event
        await self._publish_event(
            "pipeline.step.started",
            {
                "execution_id": execution.execution_id,
                "pipeline_id": execution.pipeline_id,
                "step_name": step_name,
                "step_type": step_type
            }
        )
        
        try:
            # Get step executor
            executor = self.step_executors.get(step_type, self._execute_custom_step)
            
            # Execute step
            result = await executor(
                execution,
                step_name,
                step_config
            )
            
            # Update step result
            execution.steps[step_name].update({
                "status": StepStatus.COMPLETED.value,
                "completed_at": datetime.utcnow().isoformat(),
                "result": result
            })
            
            # Store result
            execution.results[step_name] = result
            
            # Publish step completion event
            await self._publish_event(
                "pipeline.step.completed",
                {
                    "execution_id": execution.execution_id,
                    "pipeline_id": execution.pipeline_id,
                    "step_name": step_name,
                    "duration_seconds": self._calculate_step_duration(
                        execution.steps[step_name]
                    )
                }
            )
            
        except Exception as e:
            logger.error("step_execution_error",
                        execution_id=execution.execution_id,
                        step_name=step_name,
                        error=str(e))
            
            # Update step status
            execution.steps[step_name].update({
                "status": StepStatus.FAILED.value,
                "completed_at": datetime.utcnow().isoformat(),
                "error": str(e)
            })
            
            # Add to execution errors
            execution.errors.append({
                "step": step_name,
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e)
            })
            
            # Publish step failure event
            await self._publish_event(
                "pipeline.step.failed",
                {
                    "execution_id": execution.execution_id,
                    "pipeline_id": execution.pipeline_id,
                    "step_name": step_name,
                    "error": str(e)
                }
            )
            
            # Re-raise to fail pipeline
            raise
    
    async def _execute_extract_step(
        self,
        execution: PipelineExecution,
        step_name: str,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute extract step"""
        # Publish event for data platform service to handle
        await self._publish_event(
            "pipeline.extract.requested",
            {
                "execution_id": execution.execution_id,
                "step_name": step_name,
                "config": config
            }
        )
        
        # Wait for completion (simplified - would use proper async coordination)
        await asyncio.sleep(1)
        
        return {"status": "extracted", "records": 1000}
    
    async def _execute_transform_step(
        self,
        execution: PipelineExecution,
        step_name: str,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute transform step"""
        # Publish event for data platform service to handle
        await self._publish_event(
            "pipeline.transform.requested",
            {
                "execution_id": execution.execution_id,
                "step_name": step_name,
                "config": config
            }
        )
        
        # Wait for completion
        await asyncio.sleep(1)
        
        return {"status": "transformed", "records": 950}
    
    async def _execute_load_step(
        self,
        execution: PipelineExecution,
        step_name: str,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute load step"""
        # Publish event for data platform service to handle
        await self._publish_event(
            "pipeline.load.requested",
            {
                "execution_id": execution.execution_id,
                "step_name": step_name,
                "config": config
            }
        )
        
        # Wait for completion
        await asyncio.sleep(1)
        
        return {"status": "loaded", "records": 950}
    
    async def _execute_validate_step(
        self,
        execution: PipelineExecution,
        step_name: str,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute validation step"""
        # Publish event for data quality service to handle
        await self._publish_event(
            "pipeline.validate.requested",
            {
                "execution_id": execution.execution_id,
                "step_name": step_name,
                "config": config
            }
        )
        
        # Wait for completion
        await asyncio.sleep(1)
        
        return {"status": "validated", "valid": True}
    
    async def _execute_quality_check_step(
        self,
        execution: PipelineExecution,
        step_name: str,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute quality check step"""
        # Publish event for data quality service to handle
        await self._publish_event(
            "data.quality.check.requested",
            {
                "execution_id": execution.execution_id,
                "step_name": step_name,
                "dataset": config.get("dataset"),
                "checks": config.get("checks", [])
            }
        )
        
        # Wait for completion
        await asyncio.sleep(1)
        
        return {"status": "checked", "quality_score": 0.95}
    
    async def _execute_aggregate_step(
        self,
        execution: PipelineExecution,
        step_name: str,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute aggregation step"""
        # Publish event for analytics service to handle
        await self._publish_event(
            "pipeline.aggregate.requested",
            {
                "execution_id": execution.execution_id,
                "step_name": step_name,
                "config": config
            }
        )
        
        # Wait for completion
        await asyncio.sleep(1)
        
        return {"status": "aggregated", "metrics": 10}
    
    async def _execute_custom_step(
        self,
        execution: PipelineExecution,
        step_name: str,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute custom step"""
        # Generic custom step execution
        custom_type = config.get("custom_type", "unknown")
        
        await self._publish_event(
            f"pipeline.custom.{custom_type}.requested",
            {
                "execution_id": execution.execution_id,
                "step_name": step_name,
                "config": config
            }
        )
        
        # Wait for completion
        await asyncio.sleep(1)
        
        return {"status": "completed", "custom_type": custom_type}
    
    async def cancel_execution(self, execution_id: str) -> bool:
        """Cancel a running execution"""
        execution = self.executions.get(execution_id)
        if not execution:
            return False
        
        if execution.status != ExecutionStatus.RUNNING:
            return False
        
        execution.status = ExecutionStatus.CANCELLED
        execution.completed_at = datetime.utcnow()
        
        await self._publish_event(
            "pipeline.execution.cancelled",
            {
                "execution_id": execution_id,
                "pipeline_id": execution.pipeline_id
            }
        )
        
        logger.info("execution_cancelled", execution_id=execution_id)
        return True
    
    async def get_execution_status(
        self,
        execution_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get execution status"""
        execution = self.executions.get(execution_id)
        if not execution:
            # Check history
            for hist_exec in self.execution_history:
                if hist_exec.execution_id == execution_id:
                    execution = hist_exec
                    break
        
        if not execution:
            return None
        
        return {
            "execution_id": execution.execution_id,
            "pipeline_id": execution.pipeline_id,
            "pipeline_name": execution.pipeline_name,
            "status": execution.status.value,
            "started_at": execution.started_at.isoformat() if execution.started_at else None,
            "completed_at": execution.completed_at.isoformat() if execution.completed_at else None,
            "current_step": execution.current_step,
            "steps": execution.steps,
            "results": execution.results,
            "errors": execution.errors,
            "context": execution.context
        }
    
    async def list_executions(
        self,
        pipeline_id: Optional[str] = None,
        status: Optional[ExecutionStatus] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List executions"""
        executions = []
        
        # Add active executions
        for execution in self.executions.values():
            if pipeline_id and execution.pipeline_id != pipeline_id:
                continue
            if status and execution.status != status:
                continue
            
            executions.append({
                "execution_id": execution.execution_id,
                "pipeline_id": execution.pipeline_id,
                "pipeline_name": execution.pipeline_name,
                "status": execution.status.value,
                "started_at": execution.started_at.isoformat() if execution.started_at else None,
                "current_step": execution.current_step
            })
        
        # Add from history
        for execution in self.execution_history[:limit - len(executions)]:
            if pipeline_id and execution.pipeline_id != pipeline_id:
                continue
            if status and execution.status != status:
                continue
            
            executions.append({
                "execution_id": execution.execution_id,
                "pipeline_id": execution.pipeline_id,
                "pipeline_name": execution.pipeline_name,
                "status": execution.status.value,
                "started_at": execution.started_at.isoformat() if execution.started_at else None,
                "completed_at": execution.completed_at.isoformat() if execution.completed_at else None
            })
        
        return executions[:limit]
    
    def _move_to_history(self, execution: PipelineExecution):
        """Move execution to history"""
        # Remove from active
        if execution.execution_id in self.executions:
            del self.executions[execution.execution_id]
        
        # Add to history
        self.execution_history.insert(0, execution)
        
        # Trim history
        if len(self.execution_history) > self.max_history_size:
            self.execution_history = self.execution_history[:self.max_history_size]
    
    def _calculate_step_duration(self, step_info: Dict[str, Any]) -> float:
        """Calculate step duration in seconds"""
        started_at = datetime.fromisoformat(step_info["started_at"])
        completed_at = datetime.fromisoformat(step_info.get("completed_at", datetime.utcnow().isoformat()))
        return (completed_at - started_at).total_seconds()
    
    async def _publish_event(self, event_type: str, data: Dict[str, Any]):
        """Publish event"""
        if self.event_publisher:
            try:
                await self.event_publisher.publish(event_type, data)
            except Exception as e:
                logger.error("event_publish_error",
                           event_type=event_type,
                           error=str(e))
    
    async def _load_persisted_executions(self):
        """Load persisted executions from storage"""
        # TODO: Implement loading from storage
        pass 