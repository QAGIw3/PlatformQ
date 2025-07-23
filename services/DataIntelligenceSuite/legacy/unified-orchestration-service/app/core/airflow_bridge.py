"""
Apache Airflow integration bridge for workflow orchestration
"""

import asyncio
import json
import os
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
import aiofiles
import httpx
from jinja2 import Template

from platformq_shared.logging import get_logger
from ..core.config import settings

logger = get_logger(__name__)


class DagState(str, Enum):
    """DAG states"""
    ENABLED = "enabled"
    PAUSED = "paused"
    DISABLED = "disabled"


class TaskState(str, Enum):
    """Task states"""
    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"
    QUEUED = "queued"
    SKIPPED = "skipped"
    UP_FOR_RETRY = "up_for_retry"


class AirflowBridge:
    """Bridge for interacting with Apache Airflow"""
    
    def __init__(self):
        self.base_url = settings.airflow_api_url
        self.username = settings.airflow_username
        self.password = settings.airflow_password
        self.dags_folder = settings.airflow_dags_folder
        
        # HTTP client with auth
        self.client = httpx.AsyncClient(
            auth=(self.username, self.password),
            timeout=30.0,
            headers={"Content-Type": "application/json"}
        )
        
        # DAG templates
        self.dag_templates = self._load_dag_templates()
        
        # Cache for DAG metadata
        self.dag_cache: Dict[str, Any] = {}
        self._cache_lock = asyncio.Lock()
    
    async def initialize(self):
        """Initialize Airflow bridge"""
        logger.info("Initializing Airflow bridge")
        
        # Verify Airflow connection
        await self._verify_connection()
        
        # Load existing DAGs
        await self.refresh_dag_cache()
        
        logger.info("Airflow bridge initialized")
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.client:
            await self.client.aclose()
    
    async def _verify_connection(self):
        """Verify connection to Airflow"""
        try:
            response = await self.client.get(f"{self.base_url}/api/v1/health")
            response.raise_for_status()
            logger.info("Airflow connection verified")
        except Exception as e:
            logger.error(f"Failed to connect to Airflow: {str(e)}")
            raise
    
    def _load_dag_templates(self) -> Dict[str, Template]:
        """Load DAG templates"""
        templates = {
            "data_pipeline": Template("""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor

default_args = {
    'owner': '{{ owner }}',
    'depends_on_past': {{ depends_on_past | default(false) }},
    'start_date': datetime({{ start_date.year }}, {{ start_date.month }}, {{ start_date.day }}),
    'email_on_failure': {{ email_on_failure | default(true) }},
    'email_on_retry': {{ email_on_retry | default(false) }},
    'retries': {{ retries | default(3) }},
    'retry_delay': timedelta(minutes={{ retry_delay | default(5) }})
}

dag = DAG(
    '{{ dag_id }}',
    default_args=default_args,
    description='{{ description }}',
    schedule_interval='{{ schedule_interval }}',
    catchup={{ catchup | default(false) }},
    tags={{ tags | default([]) }}
)

{% for task in tasks %}
{{ task.name }} = {{ task.operator }}(
    task_id='{{ task.task_id }}',
    {{ task.params | join(',\n    ') }},
    dag=dag
)
{% endfor %}

# Define dependencies
{% for dep in dependencies %}
{{ dep.upstream }} >> {{ dep.downstream }}
{% endfor %}
"""),
            
            "ml_pipeline": Template("""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

default_args = {
    'owner': '{{ owner }}',
    'start_date': datetime({{ start_date.year }}, {{ start_date.month }}, {{ start_date.day }}),
    'retries': {{ retries | default(2) }},
    'retry_delay': timedelta(minutes={{ retry_delay | default(10) }})
}

dag = DAG(
    '{{ dag_id }}',
    default_args=default_args,
    description='{{ description }}',
    schedule_interval='{{ schedule_interval }}',
    tags=['ml', '{{ model_type }}']
)

# ML Pipeline tasks
data_prep = KubernetesPodOperator(
    task_id='data_preparation',
    name='data-prep-{{ dag_id }}',
    namespace='{{ namespace | default("ml") }}',
    image='{{ data_prep_image }}',
    cmds=['python'],
    arguments=['/app/prepare_data.py', '--dataset', '{{ dataset_id }}'],
    dag=dag
)

model_training = KubernetesPodOperator(
    task_id='model_training',
    name='train-{{ dag_id }}',
    namespace='{{ namespace | default("ml") }}',
    image='{{ training_image }}',
    cmds=['python'],
    arguments=['/app/train.py', '--config', '{{ model_config }}'],
    resources={
        'request_memory': '{{ memory_request | default("4Gi") }}',
        'request_cpu': '{{ cpu_request | default("2") }}',
        'limit_gpu': '{{ gpu_limit | default("0") }}'
    },
    dag=dag
)

model_evaluation = KubernetesPodOperator(
    task_id='model_evaluation',
    name='eval-{{ dag_id }}',
    namespace='{{ namespace | default("ml") }}',
    image='{{ evaluation_image }}',
    cmds=['python'],
    arguments=['/app/evaluate.py', '--model', '{{ "{{ ti.xcom_pull(task_ids=\"model_training\") }}" }}'],
    dag=dag
)

data_prep >> model_training >> model_evaluation
""")
        }
        
        return templates
    
    # DAG Management
    
    async def list_dags(self, limit: int = 100, offset: int = 0,
                       tags: Optional[List[str]] = None) -> Dict[str, Any]:
        """List all DAGs"""
        params = {
            "limit": limit,
            "offset": offset
        }
        
        if tags:
            params["tags"] = ",".join(tags)
        
        response = await self.client.get(
            f"{self.base_url}/api/v1/dags",
            params=params
        )
        response.raise_for_status()
        
        return response.json()
    
    async def get_dag(self, dag_id: str) -> Dict[str, Any]:
        """Get DAG details"""
        # Check cache first
        if dag_id in self.dag_cache:
            return self.dag_cache[dag_id]
        
        response = await self.client.get(f"{self.base_url}/api/v1/dags/{dag_id}")
        response.raise_for_status()
        
        dag_info = response.json()
        
        # Update cache
        async with self._cache_lock:
            self.dag_cache[dag_id] = dag_info
        
        return dag_info
    
    async def create_dag(self, dag_config: Dict[str, Any]) -> str:
        """Create a new DAG"""
        dag_id = dag_config["dag_id"]
        template_name = dag_config.get("template", "data_pipeline")
        
        if template_name not in self.dag_templates:
            raise ValueError(f"Unknown DAG template: {template_name}")
        
        # Render DAG from template
        template = self.dag_templates[template_name]
        
        # Add default values
        dag_config.setdefault("start_date", datetime.utcnow() - timedelta(days=1))
        dag_config.setdefault("owner", "orchestration-service")
        dag_config.setdefault("schedule_interval", "@daily")
        
        dag_content = template.render(**dag_config)
        
        # Write DAG file
        dag_path = os.path.join(self.dags_folder, f"{dag_id}.py")
        
        async with aiofiles.open(dag_path, 'w') as f:
            await f.write(dag_content)
        
        # Wait for Airflow to pick up the DAG
        await asyncio.sleep(5)
        
        # Refresh cache
        await self.refresh_dag_cache()
        
        logger.info(f"Created DAG: {dag_id}")
        return dag_id
    
    async def update_dag_state(self, dag_id: str, is_paused: bool) -> Dict[str, Any]:
        """Update DAG state (pause/unpause)"""
        response = await self.client.patch(
            f"{self.base_url}/api/v1/dags/{dag_id}",
            json={"is_paused": is_paused}
        )
        response.raise_for_status()
        
        # Update cache
        result = response.json()
        if dag_id in self.dag_cache:
            self.dag_cache[dag_id]["is_paused"] = is_paused
        
        return result
    
    async def delete_dag(self, dag_id: str) -> bool:
        """Delete a DAG"""
        # First pause the DAG
        await self.update_dag_state(dag_id, True)
        
        # Remove DAG file
        dag_path = os.path.join(self.dags_folder, f"{dag_id}.py")
        if os.path.exists(dag_path):
            os.remove(dag_path)
        
        # Remove from cache
        async with self._cache_lock:
            self.dag_cache.pop(dag_id, None)
        
        logger.info(f"Deleted DAG: {dag_id}")
        return True
    
    # DAG Execution
    
    async def trigger_dag(self, dag_id: str, conf: Optional[Dict[str, Any]] = None,
                         execution_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Trigger a DAG run"""
        payload = {
            "conf": conf or {},
            "execution_date": (execution_date or datetime.utcnow()).isoformat()
        }
        
        response = await self.client.post(
            f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
            json=payload
        )
        response.raise_for_status()
        
        return response.json()
    
    async def get_dag_runs(self, dag_id: str, limit: int = 100,
                          state: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get DAG runs"""
        params = {"limit": limit}
        if state:
            params["state"] = state
        
        response = await self.client.get(
            f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns",
            params=params
        )
        response.raise_for_status()
        
        return response.json().get("dag_runs", [])
    
    async def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """Get specific DAG run details"""
        response = await self.client.get(
            f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
        )
        response.raise_for_status()
        
        return response.json()
    
    async def get_task_instances(self, dag_id: str, dag_run_id: str) -> List[Dict[str, Any]]:
        """Get task instances for a DAG run"""
        response = await self.client.get(
            f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        )
        response.raise_for_status()
        
        return response.json().get("task_instances", [])
    
    # Monitoring
    
    async def get_dag_stats(self, dag_id: str) -> Dict[str, Any]:
        """Get DAG statistics"""
        # Get recent runs
        runs = await self.get_dag_runs(dag_id, limit=100)
        
        # Calculate stats
        stats = {
            "total_runs": len(runs),
            "success_rate": 0,
            "average_duration": 0,
            "last_run": None,
            "state_distribution": {}
        }
        
        if runs:
            success_count = sum(1 for r in runs if r["state"] == "success")
            stats["success_rate"] = success_count / len(runs)
            
            durations = []
            for run in runs:
                if run["end_date"] and run["start_date"]:
                    start = datetime.fromisoformat(run["start_date"].replace("Z", "+00:00"))
                    end = datetime.fromisoformat(run["end_date"].replace("Z", "+00:00"))
                    durations.append((end - start).total_seconds())
            
            if durations:
                stats["average_duration"] = sum(durations) / len(durations)
            
            stats["last_run"] = runs[0]
            
            # State distribution
            for run in runs:
                state = run["state"]
                stats["state_distribution"][state] = stats["state_distribution"].get(state, 0) + 1
        
        return stats
    
    async def get_active_dag_runs(self) -> List[Dict[str, Any]]:
        """Get all active DAG runs"""
        response = await self.client.get(
            f"{self.base_url}/api/v1/dags/~/dagRuns",
            params={"state": "running"}
        )
        response.raise_for_status()
        
        return response.json().get("dag_runs", [])
    
    # Cache Management
    
    async def refresh_dag_cache(self):
        """Refresh DAG cache"""
        logger.info("Refreshing DAG cache")
        
        dags = await self.list_dags(limit=1000)
        
        async with self._cache_lock:
            self.dag_cache.clear()
            for dag in dags.get("dags", []):
                self.dag_cache[dag["dag_id"]] = dag
        
        logger.info(f"Cached {len(self.dag_cache)} DAGs")
    
    # Dynamic DAG Generation
    
    async def generate_dag_from_pipeline(self, pipeline_config: Dict[str, Any]) -> str:
        """Generate DAG from pipeline configuration"""
        dag_id = f"pipeline_{pipeline_config['id']}"
        
        # Convert pipeline steps to DAG tasks
        tasks = []
        dependencies = []
        
        for i, step in enumerate(pipeline_config["steps"]):
            task = {
                "name": f"task_{i}",
                "task_id": step["name"],
                "operator": self._get_operator_for_step(step),
                "params": self._get_operator_params(step)
            }
            tasks.append(task)
            
            # Add dependency if not first task
            if i > 0:
                dependencies.append({
                    "upstream": f"task_{i-1}",
                    "downstream": f"task_{i}"
                })
        
        dag_config = {
            "dag_id": dag_id,
            "description": pipeline_config.get("description", ""),
            "schedule_interval": pipeline_config.get("schedule", "@once"),
            "tasks": tasks,
            "dependencies": dependencies,
            "tags": ["pipeline", "generated"]
        }
        
        return await self.create_dag(dag_config)
    
    def _get_operator_for_step(self, step: Dict[str, Any]) -> str:
        """Get Airflow operator for pipeline step"""
        step_type = step.get("type", "python")
        
        operator_mapping = {
            "python": "PythonOperator",
            "bash": "BashOperator",
            "http": "SimpleHttpOperator",
            "spark": "SparkSubmitOperator",
            "kubernetes": "KubernetesPodOperator"
        }
        
        return operator_mapping.get(step_type, "PythonOperator")
    
    def _get_operator_params(self, step: Dict[str, Any]) -> List[str]:
        """Get operator parameters for step"""
        params = []
        
        if step["type"] == "python":
            params.append(f"python_callable={step.get('callable', 'lambda: None')}")
        elif step["type"] == "bash":
            params.append(f"bash_command='{step.get('command', 'echo')}'")
        elif step["type"] == "http":
            params.append(f"endpoint='{step.get('endpoint', '')}'")
            params.append(f"method='{step.get('method', 'GET')}'")
        
        return params
    
    # Health Check
    
    async def is_healthy(self) -> bool:
        """Check if Airflow is healthy"""
        try:
            response = await self.client.get(f"{self.base_url}/api/v1/health")
            return response.status_code == 200
        except:
            return False 