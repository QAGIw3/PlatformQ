"""
Enhanced Airflow Bridge extending common library
"""
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime
import json

# Import from common library
from data_intelligence_common.integrations.airflow_client import AirflowClient
from data_intelligence_common.core.orchestration.workflow_orchestrator import WorkflowOrchestrator
from data_intelligence_common.core.events.event_bus import EventBus
from data_intelligence_common.core.caching.cache_manager import CacheManager
from data_intelligence_common.monitoring.metrics import MetricsCollector

# Import domain models
from ..domain.models.workflow import (
    EnhancedWorkflowDefinition,
    WorkflowRun,
    DagState,
    WorkflowType
)


class EnhancedAirflowBridge:
    """Enhanced Airflow integration with advanced features"""
    
    def __init__(
        self,
        config,
        airflow_client: AirflowClient,
        workflow_orchestrator: WorkflowOrchestrator,
        cache_manager: CacheManager,
        event_bus: EventBus,
        metrics_collector: MetricsCollector
    ):
        self.config = config
        self.airflow_client = airflow_client
        self.workflow_orchestrator = workflow_orchestrator
        self.cache_manager = cache_manager
        self.event_bus = event_bus
        self.metrics_collector = metrics_collector
        
        # DAG registry
        self.dag_registry: Dict[str, EnhancedWorkflowDefinition] = {}
        
        # Active runs
        self.active_runs: Dict[str, WorkflowRun] = {}
    
    async def initialize(self):
        """Initialize Airflow bridge"""
        # Verify Airflow connectivity
        await self.airflow_client.health_check()
        
        # Load existing DAGs
        await self._sync_dags()
        
        # Start monitoring
        asyncio.create_task(self._monitor_dag_runs())
    
    async def create_workflow(
        self,
        workflow: EnhancedWorkflowDefinition
    ) -> str:
        """Create workflow in Airflow"""
        # Generate DAG ID if not provided
        if not workflow.dag_id:
            workflow.dag_id = f"{workflow.name}_{workflow.workflow_id}"
        
        # Create DAG definition
        dag_definition = self._generate_dag_code(workflow)
        
        # Deploy to Airflow
        await self.airflow_client.create_dag(
            dag_id=workflow.dag_id,
            dag_code=dag_definition
        )
        
        # Register in local registry
        self.dag_registry[workflow.workflow_id] = workflow
        
        # Cache workflow
        await self.cache_manager.put(
            "workflows",
            workflow.workflow_id,
            workflow.dict()
        )
        
        # Emit event
        await self.event_bus.publish(
            "workflow.created",
            {
                "workflow_id": workflow.workflow_id,
                "dag_id": workflow.dag_id,
                "type": workflow.workflow_type.value
            }
        )
        
        return workflow.dag_id
    
    async def trigger_workflow(
        self,
        workflow_id: str,
        context: Optional[Dict[str, Any]] = None
    ) -> WorkflowRun:
        """Trigger workflow execution"""
        workflow = self.dag_registry.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow {workflow_id} not found")
        
        # Create run instance
        run = WorkflowRun(
            workflow_id=workflow_id,
            instance_id=f"run_{datetime.utcnow().timestamp()}",
            status="running",
            started_at=datetime.utcnow(),
            context=context or {},
            trigger_info={
                "trigger_type": "manual",
                "triggered_by": context.get("user") if context else "system"
            }
        )
        
        # Trigger in Airflow
        dag_run = await self.airflow_client.trigger_dag(
            dag_id=workflow.dag_id,
            conf=context
        )
        
        run.dag_run_id = dag_run["dag_run_id"]
        
        # Track active run
        self.active_runs[run.instance_id] = run
        
        # Emit event
        await self.event_bus.publish(
            "workflow.triggered",
            {
                "workflow_id": workflow_id,
                "run_id": run.instance_id,
                "dag_run_id": run.dag_run_id
            }
        )
        
        return run
    
    async def get_workflow_status(
        self,
        run_id: str
    ) -> Dict[str, Any]:
        """Get workflow execution status"""
        run = self.active_runs.get(run_id)
        if not run:
            # Try loading from cache
            cached = await self.cache_manager.get("workflow_runs", run_id)
            if cached:
                run = WorkflowRun(**cached)
            else:
                raise ValueError(f"Workflow run {run_id} not found")
        
        # Get status from Airflow
        if run.dag_run_id:
            dag_run = await self.airflow_client.get_dag_run(
                dag_id=run.dag_id,
                dag_run_id=run.dag_run_id
            )
            
            # Update status
            run.status = self._map_airflow_state(dag_run["state"])
            
            # Get task instances
            task_instances = await self.airflow_client.get_task_instances(
                dag_id=run.dag_id,
                dag_run_id=run.dag_run_id
            )
            run.task_instances = task_instances
        
        return run.dict()
    
    async def pause_workflow(self, workflow_id: str):
        """Pause workflow"""
        workflow = self.dag_registry.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow {workflow_id} not found")
        
        await self.airflow_client.pause_dag(workflow.dag_id)
        
        await self.event_bus.publish(
            "workflow.paused",
            {"workflow_id": workflow_id}
        )
    
    async def resume_workflow(self, workflow_id: str):
        """Resume workflow"""
        workflow = self.dag_registry.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow {workflow_id} not found")
        
        await self.airflow_client.unpause_dag(workflow.dag_id)
        
        await self.event_bus.publish(
            "workflow.resumed",
            {"workflow_id": workflow_id}
        )
    
    async def list_workflows(
        self,
        workflow_type: Optional[WorkflowType] = None,
        is_active: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        """List workflows"""
        workflows = []
        
        for workflow in self.dag_registry.values():
            if workflow_type and workflow.workflow_type != workflow_type:
                continue
            
            if is_active is not None:
                # Check if DAG is paused in Airflow
                dag_info = await self.airflow_client.get_dag(workflow.dag_id)
                if dag_info["is_paused"] == is_active:
                    continue
            
            workflows.append(workflow.dict())
        
        return workflows
    
    def _generate_dag_code(
        self,
        workflow: EnhancedWorkflowDefinition
    ) -> str:
        """Generate Airflow DAG code"""
        # Template for DAG generation
        dag_template = f"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json

default_args = {{
    'owner': '{workflow.owner or "orchestration-service"}',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': {workflow.retry_attempts},
    'retry_delay': timedelta(minutes={workflow.retry_delay})
}}

dag = DAG(
    '{workflow.dag_id}',
    default_args=default_args,
    description='{workflow.description or workflow.name}',
    schedule_interval={'"{}"'.format(workflow.schedule_cron) if workflow.schedule_cron else 'None'},
    catchup=False,
    tags={json.dumps(workflow.tags)}
)

# Workflow context
workflow_config = {json.dumps(workflow.config)}

"""
        
        # Generate tasks for each step
        for i, step in enumerate(workflow.steps):
            task_code = self._generate_task_code(step, i)
            dag_template += task_code + "\n"
        
        # Set dependencies
        if len(workflow.steps) > 1:
            dag_template += "\n# Set task dependencies\n"
            for i in range(len(workflow.steps) - 1):
                dag_template += f"task_{i} >> task_{i+1}\n"
        
        return dag_template
    
    def _generate_task_code(
        self,
        step: Dict[str, Any],
        index: int
    ) -> str:
        """Generate task code for a workflow step"""
        step_type = step.get("type", "python")
        
        if step_type == "bash":
            return f"""
task_{index} = BashOperator(
    task_id='{step.get("name", f"step_{index}")}',
    bash_command='{step.get("command", "echo 'No command'")}',
    dag=dag
)
"""
        else:
            # Python operator
            return f"""
def execute_step_{index}(**context):
    import requests
    # Execute step via orchestration service
    response = requests.post(
        'http://orchestration-service:8000/api/v1/steps/execute',
        json={{
            'step': {json.dumps(step)},
            'context': context
        }}
    )
    return response.json()

task_{index} = PythonOperator(
    task_id='{step.get("name", f"step_{index}")}',
    python_callable=execute_step_{index},
    dag=dag
)
"""
    
    def _map_airflow_state(self, airflow_state: str) -> str:
        """Map Airflow state to workflow status"""
        state_mapping = {
            "running": "running",
            "success": "completed",
            "failed": "failed",
            "queued": "pending",
            "skipped": "skipped"
        }
        return state_mapping.get(airflow_state.lower(), "unknown")
    
    async def _sync_dags(self):
        """Sync DAGs from Airflow"""
        try:
            dags = await self.airflow_client.list_dags()
            
            for dag in dags:
                # Check if DAG is managed by orchestration service
                if dag.get("tags") and "orchestration-service" in dag["tags"]:
                    # Load workflow definition from cache
                    workflow_id = dag["dag_id"].rsplit("_", 1)[0]
                    cached = await self.cache_manager.get("workflows", workflow_id)
                    if cached:
                        workflow = EnhancedWorkflowDefinition(**cached)
                        self.dag_registry[workflow_id] = workflow
        
        except Exception as e:
            self.logger.error(f"Failed to sync DAGs: {e}")
    
    async def _monitor_dag_runs(self):
        """Monitor active DAG runs"""
        while True:
            try:
                for run_id, run in list(self.active_runs.items()):
                    if run.status in ["completed", "failed", "cancelled"]:
                        continue
                    
                    # Update status
                    try:
                        status = await self.get_workflow_status(run_id)
                        
                        # Check if completed
                        if status["status"] in ["completed", "failed"]:
                            # Calculate duration
                            run.completed_at = datetime.utcnow()
                            run.total_duration_ms = int(
                                (run.completed_at - run.started_at).total_seconds() * 1000
                            )
                            
                            # Emit completion event
                            await self.event_bus.publish(
                                f"workflow.{status['status']}",
                                {
                                    "workflow_id": run.workflow_id,
                                    "run_id": run_id,
                                    "duration_ms": run.total_duration_ms
                                }
                            )
                            
                            # Move to completed cache
                            await self.cache_manager.put(
                                "workflow_runs",
                                run_id,
                                run.dict(),
                                ttl=86400  # 24 hours
                            )
                            
                            # Remove from active
                            del self.active_runs[run_id]
                    
                    except Exception as e:
                        self.logger.error(f"Error monitoring run {run_id}: {e}")
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(60) 