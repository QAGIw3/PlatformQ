"""
Apache Airflow Client Integration

Provides high-level client for Apache Airflow workflow orchestration.
"""

import logging
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


@dataclass
class AirflowConfig:
    """Configuration for Airflow client"""
    api_endpoint: str = "http://localhost:8080/api/v1"
    
    # Authentication
    username: Optional[str] = None
    password: Optional[str] = None
    auth_token: Optional[str] = None
    
    # Timeouts
    request_timeout: int = 30
    dag_timeout: int = 3600
    
    # Defaults
    default_pool: str = "default_pool"
    default_queue: str = "default"


@dataclass
class DAGInfo:
    """DAG information"""
    dag_id: str
    description: Optional[str] = None
    file_token: Optional[str] = None
    fileloc: Optional[str] = None
    is_paused: bool = False
    is_active: bool = True
    is_subdag: bool = False
    owners: List[str] = field(default_factory=list)
    root_dag_id: Optional[str] = None
    schedule_interval: Optional[str] = None
    tags: List[str] = field(default_factory=list)


@dataclass
class DAGRun:
    """DAG run information"""
    dag_run_id: str
    dag_id: str
    execution_date: datetime
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    state: str = "queued"
    external_trigger: bool = False
    conf: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskInstance:
    """Task instance information"""
    task_id: str
    dag_id: str
    execution_date: datetime
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    duration: Optional[float] = None
    state: Optional[str] = None
    try_number: int = 0
    max_tries: int = 0
    hostname: Optional[str] = None
    pool: Optional[str] = None
    queue: Optional[str] = None
    priority_weight: Optional[int] = None
    operator: Optional[str] = None


class AirflowClient:
    """
    High-level client for Apache Airflow operations.
    
    Features:
    - DAG management
    - DAG run triggering and monitoring
    - Task instance management
    - Pool and variable management
    - Connection management
    """
    
    def __init__(self, config: AirflowConfig):
        self.config = config
        self._session = requests.Session()
        
        # Set up authentication
        if config.auth_token:
            self._session.headers.update({
                "Authorization": f"Bearer {config.auth_token}"
            })
        elif config.username and config.password:
            self._session.auth = HTTPBasicAuth(
                config.username,
                config.password
            )
            
    def _request(
        self,
        method: str,
        endpoint: str,
        **kwargs
    ) -> requests.Response:
        """Make HTTP request to Airflow API"""
        url = f"{self.config.api_endpoint}{endpoint}"
        
        kwargs.setdefault("timeout", self.config.request_timeout)
        
        response = self._session.request(method, url, **kwargs)
        response.raise_for_status()
        
        return response
        
    def _get(self, endpoint: str, **kwargs) -> Union[Dict, List]:
        """GET request"""
        response = self._request("GET", endpoint, **kwargs)
        return response.json()
        
    def _post(self, endpoint: str, **kwargs) -> Union[Dict, List]:
        """POST request"""
        response = self._request("POST", endpoint, **kwargs)
        return response.json() if response.text else {}
        
    def _patch(self, endpoint: str, **kwargs) -> Union[Dict, List]:
        """PATCH request"""
        response = self._request("PATCH", endpoint, **kwargs)
        return response.json() if response.text else {}
        
    def _delete(self, endpoint: str, **kwargs) -> Union[Dict, List]:
        """DELETE request"""
        response = self._request("DELETE", endpoint, **kwargs)
        return response.json() if response.text else {}
        
    # DAG operations
    
    def list_dags(
        self,
        limit: int = 100,
        offset: int = 0,
        tags: Optional[List[str]] = None,
        only_active: bool = True
    ) -> List[DAGInfo]:
        """List all DAGs"""
        params = {
            "limit": limit,
            "offset": offset,
            "only_active": only_active
        }
        
        if tags:
            params["tags"] = ",".join(tags)
            
        result = self._get("/dags", params=params)
        
        dags = []
        for dag_data in result.get("dags", []):
            dags.append(DAGInfo(
                dag_id=dag_data["dag_id"],
                description=dag_data.get("description"),
                file_token=dag_data.get("file_token"),
                fileloc=dag_data.get("fileloc"),
                is_paused=dag_data.get("is_paused", False),
                is_active=dag_data.get("is_active", True),
                is_subdag=dag_data.get("is_subdag", False),
                owners=dag_data.get("owners", []),
                root_dag_id=dag_data.get("root_dag_id"),
                schedule_interval=dag_data.get("schedule_interval"),
                tags=dag_data.get("tags", [])
            ))
            
        return dags
        
    def get_dag(self, dag_id: str) -> DAGInfo:
        """Get DAG details"""
        result = self._get(f"/dags/{dag_id}")
        
        return DAGInfo(
            dag_id=result["dag_id"],
            description=result.get("description"),
            file_token=result.get("file_token"),
            fileloc=result.get("fileloc"),
            is_paused=result.get("is_paused", False),
            is_active=result.get("is_active", True),
            is_subdag=result.get("is_subdag", False),
            owners=result.get("owners", []),
            root_dag_id=result.get("root_dag_id"),
            schedule_interval=result.get("schedule_interval"),
            tags=result.get("tags", [])
        )
        
    def pause_dag(self, dag_id: str) -> bool:
        """Pause a DAG"""
        result = self._patch(
            f"/dags/{dag_id}",
            json={"is_paused": True}
        )
        return result.get("is_paused", False)
        
    def unpause_dag(self, dag_id: str) -> bool:
        """Unpause a DAG"""
        result = self._patch(
            f"/dags/{dag_id}",
            json={"is_paused": False}
        )
        return not result.get("is_paused", True)
        
    # DAG run operations
    
    def trigger_dag(
        self,
        dag_id: str,
        dag_run_id: Optional[str] = None,
        execution_date: Optional[datetime] = None,
        conf: Optional[Dict[str, Any]] = None,
        replace_microseconds: bool = True
    ) -> DAGRun:
        """Trigger a DAG run"""
        import uuid
        
        if not dag_run_id:
            dag_run_id = f"manual__{datetime.utcnow().isoformat()}_{uuid.uuid4().hex[:8]}"
            
        data = {
            "dag_run_id": dag_run_id,
            "execution_date": (execution_date or datetime.utcnow()).isoformat(),
            "conf": conf or {}
        }
        
        if replace_microseconds:
            data["replace_microseconds"] = "true"
            
        result = self._post(f"/dags/{dag_id}/dagRuns", json=data)
        
        return DAGRun(
            dag_run_id=result["dag_run_id"],
            dag_id=result["dag_id"],
            execution_date=datetime.fromisoformat(result["execution_date"].replace("Z", "+00:00")),
            start_date=datetime.fromisoformat(result["start_date"].replace("Z", "+00:00")) if result.get("start_date") else None,
            end_date=datetime.fromisoformat(result["end_date"].replace("Z", "+00:00")) if result.get("end_date") else None,
            state=result.get("state", "queued"),
            external_trigger=result.get("external_trigger", True),
            conf=result.get("conf", {})
        )
        
    def get_dag_run(
        self,
        dag_id: str,
        dag_run_id: str
    ) -> DAGRun:
        """Get DAG run details"""
        result = self._get(f"/dags/{dag_id}/dagRuns/{dag_run_id}")
        
        return DAGRun(
            dag_run_id=result["dag_run_id"],
            dag_id=result["dag_id"],
            execution_date=datetime.fromisoformat(result["execution_date"].replace("Z", "+00:00")),
            start_date=datetime.fromisoformat(result["start_date"].replace("Z", "+00:00")) if result.get("start_date") else None,
            end_date=datetime.fromisoformat(result["end_date"].replace("Z", "+00:00")) if result.get("end_date") else None,
            state=result.get("state"),
            external_trigger=result.get("external_trigger", False),
            conf=result.get("conf", {})
        )
        
    def list_dag_runs(
        self,
        dag_id: str,
        limit: int = 100,
        offset: int = 0,
        execution_date_gte: Optional[datetime] = None,
        execution_date_lte: Optional[datetime] = None,
        start_date_gte: Optional[datetime] = None,
        start_date_lte: Optional[datetime] = None,
        end_date_gte: Optional[datetime] = None,
        end_date_lte: Optional[datetime] = None,
        state: Optional[List[str]] = None
    ) -> List[DAGRun]:
        """List DAG runs"""
        params = {
            "limit": limit,
            "offset": offset
        }
        
        if execution_date_gte:
            params["execution_date_gte"] = execution_date_gte.isoformat()
        if execution_date_lte:
            params["execution_date_lte"] = execution_date_lte.isoformat()
        if start_date_gte:
            params["start_date_gte"] = start_date_gte.isoformat()
        if start_date_lte:
            params["start_date_lte"] = start_date_lte.isoformat()
        if end_date_gte:
            params["end_date_gte"] = end_date_gte.isoformat()
        if end_date_lte:
            params["end_date_lte"] = end_date_lte.isoformat()
        if state:
            params["state"] = state
            
        result = self._get(f"/dags/{dag_id}/dagRuns", params=params)
        
        runs = []
        for run_data in result.get("dag_runs", []):
            runs.append(DAGRun(
                dag_run_id=run_data["dag_run_id"],
                dag_id=run_data["dag_id"],
                execution_date=datetime.fromisoformat(run_data["execution_date"].replace("Z", "+00:00")),
                start_date=datetime.fromisoformat(run_data["start_date"].replace("Z", "+00:00")) if run_data.get("start_date") else None,
                end_date=datetime.fromisoformat(run_data["end_date"].replace("Z", "+00:00")) if run_data.get("end_date") else None,
                state=run_data.get("state"),
                external_trigger=run_data.get("external_trigger", False),
                conf=run_data.get("conf", {})
            ))
            
        return runs
        
    def clear_dag_run(
        self,
        dag_id: str,
        dag_run_id: str,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """Clear a DAG run"""
        data = {"dry_run": dry_run}
        
        return self._post(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/clear",
            json=data
        )
        
    # Task instance operations
    
    def get_task_instance(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str
    ) -> TaskInstance:
        """Get task instance details"""
        result = self._get(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
        )
        
        return TaskInstance(
            task_id=result["task_id"],
            dag_id=result["dag_id"],
            execution_date=datetime.fromisoformat(result["execution_date"].replace("Z", "+00:00")),
            start_date=datetime.fromisoformat(result["start_date"].replace("Z", "+00:00")) if result.get("start_date") else None,
            end_date=datetime.fromisoformat(result["end_date"].replace("Z", "+00:00")) if result.get("end_date") else None,
            duration=result.get("duration"),
            state=result.get("state"),
            try_number=result.get("try_number", 0),
            max_tries=result.get("max_tries", 0),
            hostname=result.get("hostname"),
            pool=result.get("pool"),
            queue=result.get("queue"),
            priority_weight=result.get("priority_weight"),
            operator=result.get("operator")
        )
        
    def list_task_instances(
        self,
        dag_id: str,
        dag_run_id: str,
        limit: int = 100,
        offset: int = 0,
        state: Optional[List[str]] = None
    ) -> List[TaskInstance]:
        """List task instances for a DAG run"""
        params = {
            "limit": limit,
            "offset": offset
        }
        
        if state:
            params["state"] = state
            
        result = self._get(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
            params=params
        )
        
        tasks = []
        for task_data in result.get("task_instances", []):
            tasks.append(TaskInstance(
                task_id=task_data["task_id"],
                dag_id=task_data["dag_id"],
                execution_date=datetime.fromisoformat(task_data["execution_date"].replace("Z", "+00:00")),
                start_date=datetime.fromisoformat(task_data["start_date"].replace("Z", "+00:00")) if task_data.get("start_date") else None,
                end_date=datetime.fromisoformat(task_data["end_date"].replace("Z", "+00:00")) if task_data.get("end_date") else None,
                duration=task_data.get("duration"),
                state=task_data.get("state"),
                try_number=task_data.get("try_number", 0),
                max_tries=task_data.get("max_tries", 0),
                hostname=task_data.get("hostname"),
                pool=task_data.get("pool"),
                queue=task_data.get("queue"),
                priority_weight=task_data.get("priority_weight"),
                operator=task_data.get("operator")
            ))
            
        return tasks
        
    def get_task_logs(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        task_try_number: int = 1,
        full_content: bool = True
    ) -> str:
        """Get task logs"""
        params = {
            "task_try_number": task_try_number,
            "full_content": full_content
        }
        
        result = self._get(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs",
            params=params
        )
        
        return result.get("content", "")
        
    # Variable operations
    
    def get_variable(self, key: str) -> str:
        """Get Airflow variable"""
        result = self._get(f"/variables/{key}")
        return result.get("value", "")
        
    def set_variable(
        self,
        key: str,
        value: str,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """Set Airflow variable"""
        data = {
            "key": key,
            "value": value
        }
        
        if description:
            data["description"] = description
            
        return self._post("/variables", json=data)
        
    def delete_variable(self, key: str) -> bool:
        """Delete Airflow variable"""
        try:
            self._delete(f"/variables/{key}")
            return True
        except Exception:
            return False
            
    # Connection operations
    
    def get_connection(self, conn_id: str) -> Dict[str, Any]:
        """Get Airflow connection"""
        return self._get(f"/connections/{conn_id}")
        
    def create_connection(
        self,
        conn_id: str,
        conn_type: str,
        host: Optional[str] = None,
        login: Optional[str] = None,
        password: Optional[str] = None,
        schema: Optional[str] = None,
        port: Optional[int] = None,
        extra: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create Airflow connection"""
        data = {
            "connection_id": conn_id,
            "conn_type": conn_type
        }
        
        if host:
            data["host"] = host
        if login:
            data["login"] = login
        if password:
            data["password"] = password
        if schema:
            data["schema"] = schema
        if port:
            data["port"] = port
        if extra:
            import json
            data["extra"] = json.dumps(extra)
            
        return self._post("/connections", json=data)
        
    def delete_connection(self, conn_id: str) -> bool:
        """Delete Airflow connection"""
        try:
            self._delete(f"/connections/{conn_id}")
            return True
        except Exception:
            return False
            
    # Pool operations
    
    def get_pool(self, pool_name: str) -> Dict[str, Any]:
        """Get pool details"""
        return self._get(f"/pools/{pool_name}")
        
    def create_pool(
        self,
        name: str,
        slots: int,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a pool"""
        data = {
            "name": name,
            "slots": slots
        }
        
        if description:
            data["description"] = description
            
        return self._post("/pools", json=data)
        
    def update_pool(
        self,
        name: str,
        slots: int,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update a pool"""
        data = {"slots": slots}
        
        if description:
            data["description"] = description
            
        return self._patch(f"/pools/{name}", json=data)
        
    def delete_pool(self, pool_name: str) -> bool:
        """Delete a pool"""
        try:
            self._delete(f"/pools/{pool_name}")
            return True
        except Exception:
            return False
            
    # Utility methods
    
    def wait_for_dag_run(
        self,
        dag_id: str,
        dag_run_id: str,
        timeout: Optional[int] = None,
        poll_interval: int = 30
    ) -> DAGRun:
        """Wait for DAG run to complete"""
        import time
        
        start_time = time.time()
        timeout = timeout or self.config.dag_timeout
        
        while True:
            dag_run = self.get_dag_run(dag_id, dag_run_id)
            
            if dag_run.state in ["success", "failed", "skipped"]:
                return dag_run
                
            if time.time() - start_time > timeout:
                raise TimeoutError(
                    f"DAG run {dag_run_id} did not complete within {timeout}s"
                )
                
            time.sleep(poll_interval)
            
    def get_dag_state(
        self,
        dag_id: str,
        execution_date: datetime
    ) -> str:
        """Get DAG state for specific execution date"""
        dag_runs = self.list_dag_runs(
            dag_id,
            execution_date_gte=execution_date,
            execution_date_lte=execution_date
        )
        
        if dag_runs:
            return dag_runs[0].state
        return "not_found" 