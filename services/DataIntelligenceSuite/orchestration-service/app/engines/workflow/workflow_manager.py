"""
Workflow Manager

Manages workflow creation, execution, and monitoring.
"""

import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
from enum import Enum
import uuid

from data_intelligence_common import StructuredLogger, EventBus
from data_intelligence_common.vault_consul import VaultConsulIntegration

logger = StructuredLogger.get_logger(__name__)


class WorkflowStatus(Enum):
    """Workflow execution status"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class WorkflowType(Enum):
    """Workflow types"""
    DATA_PIPELINE = "data_pipeline"
    ML_TRAINING = "ml_training"
    DATA_QUALITY = "data_quality"
    ETL = "etl"
    STREAMING = "streaming"
    CUSTOM = "custom"


class WorkflowManager:
    """
    Manages workflow lifecycle and execution
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration, event_bus: EventBus,
                 airflow_bridge: Any, dag_generator: Any):
        self.vault_consul = vault_consul
        self.event_bus = event_bus
        self.airflow_bridge = airflow_bridge
        self.dag_generator = dag_generator
        
        # Workflow tracking
        self.workflows: Dict[str, Dict[str, Any]] = {}
        self.active_runs: Dict[str, Dict[str, Any]] = {}
        
        # Configuration
        self.config = {
            "max_concurrent_workflows": 100,
            "default_timeout": 3600,
            "retry_policy": {
                "max_retries": 3,
                "retry_delay": 300,
                "exponential_backoff": True
            },
            "templates_path": "/config/workflow-templates"
        }
        
        # Metrics
        self.metrics = {
            "workflows_created": 0,
            "workflows_executed": 0,
            "workflows_succeeded": 0,
            "workflows_failed": 0,
            "avg_execution_time": 0
        }
    
    async def initialize(self):
        """Initialize workflow manager"""
        logger.info("initializing_workflow_manager")
        
        # Load configuration from Consul
        await self._load_configuration()
        
        # Initialize Airflow bridge
        await self.airflow_bridge.initialize()
        
        # Load workflow templates
        await self._load_templates()
        
        # Start monitoring
        asyncio.create_task(self._monitor_workflows())
        
        logger.info("workflow_manager_initialized")
    
    async def cleanup(self):
        """Cleanup resources"""
        # Cancel all active workflows
        for run_id in list(self.active_runs.keys()):
            await self.cancel_workflow(run_id)
        
        await self.airflow_bridge.cleanup()
    
    async def _load_configuration(self):
        """Load configuration from Consul"""
        try:
            config = await self.vault_consul.consul.kv.get("config/workflow-manager")
            if config:
                import json
                self.config.update(json.loads(config["Value"].decode()))
        except Exception as e:
            logger.warning(f"Failed to load config from Consul: {e}")
    
    async def create_workflow(self, workflow_config: Dict[str, Any]) -> str:
        """
        Create a new workflow
        
        Args:
            workflow_config: Workflow configuration including:
                - name: Workflow name
                - type: Workflow type
                - description: Workflow description
                - steps: List of workflow steps
                - schedule: Schedule configuration (optional)
                - retry_policy: Retry configuration (optional)
                
        Returns:
            Workflow ID
        """
        workflow_id = str(uuid.uuid4())
        
        # Validate workflow configuration
        self._validate_workflow_config(workflow_config)
        
        # Create workflow record
        workflow = {
            "id": workflow_id,
            "config": workflow_config,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "version": 1,
            "status": "active",
            "dag_id": None,
            "runs": []
        }
        
        # Generate DAG from workflow config
        dag_config = await self.dag_generator.generate_dag(workflow_config)
        
        # Create DAG in Airflow
        dag_id = await self.airflow_bridge.create_dag(dag_config)
        workflow["dag_id"] = dag_id
        
        # Store workflow
        self.workflows[workflow_id] = workflow
        
        # Update metrics
        self.metrics["workflows_created"] += 1
        
        # Emit event
        await self.event_bus.publish(
            "orchestration.workflow.created",
            {
                "workflow_id": workflow_id,
                "name": workflow_config.get("name"),
                "type": workflow_config.get("type"),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Workflow created: {workflow_id}")
        return workflow_id
    
    async def trigger_workflow(self, workflow_id: str, context: Dict[str, Any] = None) -> str:
        """
        Trigger workflow execution
        
        Args:
            workflow_id: Workflow ID
            context: Execution context
            
        Returns:
            Run ID
        """
        workflow = self.workflows.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow not found: {workflow_id}")
        
        if workflow["status"] != "active":
            raise RuntimeError(f"Workflow not active: {workflow['status']}")
        
        # Check concurrent execution limit
        active_count = sum(1 for run in self.active_runs.values() 
                          if run["status"] == WorkflowStatus.RUNNING)
        
        if active_count >= self.config["max_concurrent_workflows"]:
            raise RuntimeError("Maximum concurrent workflows reached")
        
        run_id = str(uuid.uuid4())
        
        # Create run record
        run = {
            "id": run_id,
            "workflow_id": workflow_id,
            "status": WorkflowStatus.PENDING,
            "context": context or {},
            "started_at": datetime.utcnow(),
            "completed_at": None,
            "airflow_run_id": None,
            "tasks": {},
            "error": None
        }
        
        # Store run
        self.active_runs[run_id] = run
        workflow["runs"].append(run_id)
        
        # Trigger DAG in Airflow
        airflow_run_id = await self.airflow_bridge.trigger_dag(
            workflow["dag_id"],
            conf=context
        )
        
        run["airflow_run_id"] = airflow_run_id
        run["status"] = WorkflowStatus.RUNNING
        
        # Update metrics
        self.metrics["workflows_executed"] += 1
        
        # Start monitoring run
        asyncio.create_task(self._monitor_run(run_id))
        
        # Emit event
        await self.event_bus.publish(
            "orchestration.workflow.triggered",
            {
                "workflow_id": workflow_id,
                "run_id": run_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Workflow triggered: {workflow_id}, run: {run_id}")
        return run_id
    
    async def get_workflow_status(self, workflow_id: str) -> Dict[str, Any]:
        """Get workflow status"""
        workflow = self.workflows.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow not found: {workflow_id}")
        
        # Get latest run status if exists
        latest_run = None
        if workflow["runs"]:
            latest_run_id = workflow["runs"][-1]
            latest_run = self.active_runs.get(latest_run_id)
        
        return {
            "id": workflow_id,
            "name": workflow["config"]["name"],
            "type": workflow["config"]["type"],
            "status": workflow["status"],
            "created_at": workflow["created_at"].isoformat(),
            "updated_at": workflow["updated_at"].isoformat(),
            "version": workflow["version"],
            "total_runs": len(workflow["runs"]),
            "latest_run": {
                "id": latest_run["id"],
                "status": latest_run["status"].value,
                "started_at": latest_run["started_at"].isoformat(),
                "completed_at": latest_run["completed_at"].isoformat() if latest_run["completed_at"] else None
            } if latest_run else None
        }
    
    async def get_run_status(self, run_id: str) -> Dict[str, Any]:
        """Get workflow run status"""
        run = self.active_runs.get(run_id)
        if not run:
            raise ValueError(f"Run not found: {run_id}")
        
        # Get task status from Airflow
        if run["airflow_run_id"]:
            workflow = self.workflows.get(run["workflow_id"])
            task_status = await self.airflow_bridge.get_dag_run_status(
                workflow["dag_id"],
                run["airflow_run_id"]
            )
            run["tasks"] = task_status.get("tasks", {})
        
        return {
            "id": run_id,
            "workflow_id": run["workflow_id"],
            "status": run["status"].value,
            "started_at": run["started_at"].isoformat(),
            "completed_at": run["completed_at"].isoformat() if run["completed_at"] else None,
            "duration": (run["completed_at"] - run["started_at"]).total_seconds() if run["completed_at"] else None,
            "tasks": run["tasks"],
            "error": run["error"]
        }
    
    async def cancel_workflow(self, run_id: str) -> bool:
        """Cancel workflow execution"""
        run = self.active_runs.get(run_id)
        if not run:
            raise ValueError(f"Run not found: {run_id}")
        
        if run["status"] not in [WorkflowStatus.PENDING, WorkflowStatus.RUNNING]:
            return False
        
        # Cancel in Airflow
        if run["airflow_run_id"]:
            workflow = self.workflows.get(run["workflow_id"])
            await self.airflow_bridge.cancel_dag_run(
                workflow["dag_id"],
                run["airflow_run_id"]
            )
        
        # Update run status
        run["status"] = WorkflowStatus.CANCELLED
        run["completed_at"] = datetime.utcnow()
        
        # Emit event
        await self.event_bus.publish(
            "orchestration.workflow.cancelled",
            {
                "workflow_id": run["workflow_id"],
                "run_id": run_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Workflow run cancelled: {run_id}")
        return True
    
    async def update_workflow(self, workflow_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update workflow configuration"""
        workflow = self.workflows.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow not found: {workflow_id}")
        
        # Validate updates
        if "steps" in updates:
            self._validate_workflow_steps(updates["steps"])
        
        # Update configuration
        workflow["config"].update(updates)
        workflow["updated_at"] = datetime.utcnow()
        workflow["version"] += 1
        
        # Regenerate DAG if needed
        if any(key in updates for key in ["steps", "schedule", "retry_policy"]):
            dag_config = await self.dag_generator.generate_dag(workflow["config"])
            await self.airflow_bridge.update_dag(workflow["dag_id"], dag_config)
        
        # Emit event
        await self.event_bus.publish(
            "orchestration.workflow.updated",
            {
                "workflow_id": workflow_id,
                "version": workflow["version"],
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Workflow updated: {workflow_id}")
        return await self.get_workflow_status(workflow_id)
    
    async def pause_workflow(self, workflow_id: str) -> bool:
        """Pause workflow (disable scheduling)"""
        workflow = self.workflows.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow not found: {workflow_id}")
        
        # Pause in Airflow
        await self.airflow_bridge.pause_dag(workflow["dag_id"])
        
        # Update status
        workflow["status"] = "paused"
        workflow["updated_at"] = datetime.utcnow()
        
        logger.info(f"Workflow paused: {workflow_id}")
        return True
    
    async def resume_workflow(self, workflow_id: str) -> bool:
        """Resume workflow (enable scheduling)"""
        workflow = self.workflows.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow not found: {workflow_id}")
        
        # Resume in Airflow
        await self.airflow_bridge.unpause_dag(workflow["dag_id"])
        
        # Update status
        workflow["status"] = "active"
        workflow["updated_at"] = datetime.utcnow()
        
        logger.info(f"Workflow resumed: {workflow_id}")
        return True
    
    async def _monitor_run(self, run_id: str):
        """Monitor workflow run execution"""
        run = self.active_runs.get(run_id)
        if not run:
            return
        
        workflow = self.workflows.get(run["workflow_id"])
        
        while run["status"] == WorkflowStatus.RUNNING:
            try:
                # Get status from Airflow
                status = await self.airflow_bridge.get_dag_run_status(
                    workflow["dag_id"],
                    run["airflow_run_id"]
                )
                
                # Update run status
                airflow_state = status.get("state")
                
                if airflow_state == "success":
                    run["status"] = WorkflowStatus.SUCCESS
                    run["completed_at"] = datetime.utcnow()
                    self.metrics["workflows_succeeded"] += 1
                    
                elif airflow_state == "failed":
                    run["status"] = WorkflowStatus.FAILED
                    run["completed_at"] = datetime.utcnow()
                    run["error"] = status.get("error", "Unknown error")
                    self.metrics["workflows_failed"] += 1
                    
                elif airflow_state == "running":
                    # Update task progress
                    run["tasks"] = status.get("tasks", {})
                
                # Emit progress event
                await self.event_bus.publish(
                    "orchestration.workflow.progress",
                    {
                        "workflow_id": run["workflow_id"],
                        "run_id": run_id,
                        "status": run["status"].value,
                        "tasks": run["tasks"],
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
                
                if run["status"] != WorkflowStatus.RUNNING:
                    break
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Error monitoring run {run_id}: {e}")
                run["status"] = WorkflowStatus.FAILED
                run["error"] = str(e)
                break
        
        # Update execution time metric
        if run["completed_at"]:
            duration = (run["completed_at"] - run["started_at"]).total_seconds()
            self._update_avg_execution_time(duration)
        
        # Emit completion event
        await self.event_bus.publish(
            "orchestration.workflow.completed",
            {
                "workflow_id": run["workflow_id"],
                "run_id": run_id,
                "status": run["status"].value,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
    
    async def _monitor_workflows(self):
        """Monitor all workflows"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                # Clean up old completed runs
                current_time = datetime.utcnow()
                
                for run_id, run in list(self.active_runs.items()):
                    if run["status"] in [WorkflowStatus.SUCCESS, WorkflowStatus.FAILED, WorkflowStatus.CANCELLED]:
                        if run["completed_at"]:
                            age = (current_time - run["completed_at"]).days
                            if age > 7:  # Remove runs older than 7 days
                                del self.active_runs[run_id]
                
            except Exception as e:
                logger.error(f"Error monitoring workflows: {e}")
    
    async def _load_templates(self):
        """Load workflow templates"""
        # This would load predefined workflow templates
        pass
    
    def _validate_workflow_config(self, config: Dict[str, Any]):
        """Validate workflow configuration"""
        required_fields = ["name", "type", "steps"]
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate workflow type
        workflow_type = config["type"]
        if workflow_type not in [t.value for t in WorkflowType]:
            raise ValueError(f"Invalid workflow type: {workflow_type}")
        
        # Validate steps
        self._validate_workflow_steps(config["steps"])
    
    def _validate_workflow_steps(self, steps: List[Dict[str, Any]]):
        """Validate workflow steps"""
        if not steps:
            raise ValueError("Workflow must have at least one step")
        
        for i, step in enumerate(steps):
            if "name" not in step:
                raise ValueError(f"Step {i} missing name")
            if "type" not in step:
                raise ValueError(f"Step {i} missing type")
    
    def _update_avg_execution_time(self, duration: float):
        """Update average execution time metric"""
        total_completed = self.metrics["workflows_succeeded"] + self.metrics["workflows_failed"]
        
        if total_completed == 1:
            self.metrics["avg_execution_time"] = duration
        else:
            current_avg = self.metrics["avg_execution_time"]
            self.metrics["avg_execution_time"] = (
                (current_avg * (total_completed - 1) + duration) / total_completed
            )
    
    async def get_workflow_metrics(self) -> Dict[str, Any]:
        """Get workflow manager metrics"""
        return {
            **self.metrics,
            "active_workflows": len([w for w in self.workflows.values() if w["status"] == "active"]),
            "total_workflows": len(self.workflows),
            "running_workflows": sum(1 for run in self.active_runs.values() 
                                   if run["status"] == WorkflowStatus.RUNNING)
        } 