"""
Apache Flink Client Integration

Provides high-level client for Apache Flink operations.
"""

import logging
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, field
from datetime import datetime
import requests
import json

logger = logging.getLogger(__name__)


@dataclass
class FlinkConfig:
    """Configuration for Flink client"""
    rest_endpoint: str = "http://localhost:8081"
    
    # Job submission
    jar_path: Optional[str] = None
    entry_class: Optional[str] = None
    parallelism: int = 1
    
    # Savepoint/Checkpoint
    savepoint_dir: Optional[str] = None
    checkpoint_dir: Optional[str] = None
    checkpoint_interval: int = 60000  # milliseconds
    
    # Timeouts
    request_timeout: int = 30
    submission_timeout: int = 300
    
    # Authentication
    auth_token: Optional[str] = None


@dataclass
class JobInfo:
    """Flink job information"""
    job_id: str
    name: str
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[int] = None
    tasks: Dict[str, Any] = field(default_factory=dict)
    vertices: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class JobSubmissionResult:
    """Result of job submission"""
    job_id: str
    status: str
    message: Optional[str] = None
    web_url: Optional[str] = None


class FlinkClient:
    """
    High-level client for Apache Flink operations.
    
    Features:
    - Job submission and management
    - Job monitoring
    - Savepoint management
    - Metrics collection
    - REST API operations
    """
    
    def __init__(self, config: FlinkConfig):
        self.config = config
        self._session = requests.Session()
        
        if config.auth_token:
            self._session.headers.update({
                "Authorization": f"Bearer {config.auth_token}"
            })
            
    def _request(
        self,
        method: str,
        endpoint: str,
        **kwargs
    ) -> requests.Response:
        """Make HTTP request to Flink REST API"""
        url = f"{self.config.rest_endpoint}{endpoint}"
        
        kwargs.setdefault("timeout", self.config.request_timeout)
        
        response = self._session.request(method, url, **kwargs)
        response.raise_for_status()
        
        return response
        
    def _get(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """GET request"""
        response = self._request("GET", endpoint, **kwargs)
        return response.json() if response.text else {}
        
    def _post(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """POST request"""
        response = self._request("POST", endpoint, **kwargs)
        return response.json() if response.text else {}
        
    def _delete(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """DELETE request"""
        response = self._request("DELETE", endpoint, **kwargs)
        return response.json() if response.text else {}
        
    # Cluster operations
    
    def get_cluster_overview(self) -> Dict[str, Any]:
        """Get cluster overview"""
        return self._get("/overview")
        
    def get_config(self) -> Dict[str, Any]:
        """Get cluster configuration"""
        return self._get("/config")
        
    def get_task_managers(self) -> List[Dict[str, Any]]:
        """Get task managers"""
        result = self._get("/taskmanagers")
        return result.get("taskmanagers", [])
        
    def get_job_manager_config(self) -> Dict[str, Any]:
        """Get JobManager configuration"""
        return self._get("/jobmanager/config")
        
    # Job operations
    
    def submit_job(
        self,
        jar_path: Optional[str] = None,
        entry_class: Optional[str] = None,
        program_args: Optional[List[str]] = None,
        parallelism: Optional[int] = None,
        savepoint_path: Optional[str] = None
    ) -> JobSubmissionResult:
        """Submit a job to Flink cluster"""
        # Use config defaults if not provided
        jar_path = jar_path or self.config.jar_path
        entry_class = entry_class or self.config.entry_class
        parallelism = parallelism or self.config.parallelism
        
        if not jar_path:
            raise ValueError("JAR path must be provided")
            
        # First, upload the JAR
        jar_id = self._upload_jar(jar_path)
        
        # Prepare job submission request
        data = {
            "entryClass": entry_class,
            "programArgs": " ".join(program_args) if program_args else "",
            "parallelism": parallelism
        }
        
        if savepoint_path:
            data["savepointPath"] = savepoint_path
            
        # Submit job
        try:
            result = self._post(
                f"/jars/{jar_id}/run",
                json=data
            )
            
            return JobSubmissionResult(
                job_id=result["jobid"],
                status="submitted",
                web_url=f"{self.config.rest_endpoint}/#/jobs/{result['jobid']}"
            )
            
        except Exception as e:
            return JobSubmissionResult(
                job_id="",
                status="failed",
                message=str(e)
            )
            
    def _upload_jar(self, jar_path: str) -> str:
        """Upload JAR file to cluster"""
        with open(jar_path, "rb") as f:
            files = {"jarfile": (jar_path.split("/")[-1], f)}
            result = self._post("/jars/upload", files=files)
            
        # Extract JAR ID from filename
        filename = result["filename"]
        jar_id = filename.split("/")[-1]
        
        return jar_id
        
    def list_jobs(
        self,
        status: Optional[str] = None
    ) -> List[JobInfo]:
        """List all jobs"""
        result = self._get("/jobs")
        
        jobs = []
        for job_data in result.get("jobs", []):
            # Filter by status if specified
            if status and job_data["status"] != status.upper():
                continue
                
            jobs.append(JobInfo(
                job_id=job_data["id"],
                name=job_data.get("name", ""),
                status=job_data["status"],
                start_time=datetime.fromtimestamp(
                    job_data["start-time"] / 1000
                )
            ))
            
        return jobs
        
    def get_job(self, job_id: str) -> JobInfo:
        """Get job details"""
        result = self._get(f"/jobs/{job_id}")
        
        return JobInfo(
            job_id=result["jid"],
            name=result.get("name", ""),
            status=result["state"],
            start_time=datetime.fromtimestamp(
                result["start-time"] / 1000
            ),
            end_time=datetime.fromtimestamp(
                result["end-time"] / 1000
            ) if result.get("end-time", -1) > 0 else None,
            duration=result.get("duration", 0),
            vertices=result.get("vertices", [])
        )
        
    def cancel_job(
        self,
        job_id: str,
        savepoint: bool = True,
        savepoint_dir: Optional[str] = None
    ) -> Dict[str, Any]:
        """Cancel a running job"""
        params = {}
        
        if savepoint:
            params["mode"] = "cancel"
            params["targetDirectory"] = savepoint_dir or self.config.savepoint_dir
        else:
            params["mode"] = "cancel"
            
        return self._post(f"/jobs/{job_id}/stop", params=params)
        
    def get_job_exceptions(self, job_id: str) -> List[Dict[str, Any]]:
        """Get job exceptions"""
        result = self._get(f"/jobs/{job_id}/exceptions")
        return result.get("all-exceptions", [])
        
    # Savepoint operations
    
    def trigger_savepoint(
        self,
        job_id: str,
        savepoint_dir: Optional[str] = None,
        cancel_job: bool = False
    ) -> Dict[str, Any]:
        """Trigger a savepoint"""
        data = {
            "target-directory": savepoint_dir or self.config.savepoint_dir,
            "cancel-job": cancel_job
        }
        
        return self._post(f"/jobs/{job_id}/savepoints", json=data)
        
    def get_savepoint_status(
        self,
        job_id: str,
        trigger_id: str
    ) -> Dict[str, Any]:
        """Get savepoint operation status"""
        return self._get(f"/jobs/{job_id}/savepoints/{trigger_id}")
        
    def dispose_savepoint(self, savepoint_path: str) -> Dict[str, Any]:
        """Dispose a savepoint"""
        data = {"savepoint-path": savepoint_path}
        return self._post("/savepoints/disposal", json=data)
        
    # Metrics operations
    
    def get_job_metrics(self, job_id: str) -> Dict[str, Any]:
        """Get job metrics"""
        return self._get(f"/jobs/{job_id}/metrics")
        
    def get_task_metrics(
        self,
        job_id: str,
        vertex_id: str
    ) -> Dict[str, Any]:
        """Get task metrics"""
        return self._get(f"/jobs/{job_id}/vertices/{vertex_id}/metrics")
        
    def get_job_manager_metrics(self) -> List[Dict[str, Any]]:
        """Get JobManager metrics"""
        result = self._get("/jobmanager/metrics")
        return result if isinstance(result, list) else []
        
    def get_task_manager_metrics(
        self,
        tm_id: str
    ) -> List[Dict[str, Any]]:
        """Get TaskManager metrics"""
        result = self._get(f"/taskmanagers/{tm_id}/metrics")
        return result if isinstance(result, list) else []
        
    # Checkpoint operations
    
    def get_checkpoints(self, job_id: str) -> Dict[str, Any]:
        """Get checkpoint statistics"""
        return self._get(f"/jobs/{job_id}/checkpoints")
        
    def get_checkpoint_details(
        self,
        job_id: str,
        checkpoint_id: int
    ) -> Dict[str, Any]:
        """Get checkpoint details"""
        return self._get(f"/jobs/{job_id}/checkpoints/details/{checkpoint_id}")
        
    # SQL operations (if SQL is enabled)
    
    def submit_sql_job(
        self,
        sql: str,
        session_id: Optional[str] = None
    ) -> JobSubmissionResult:
        """Submit SQL job (requires SQL gateway)"""
        # This is a simplified version - actual implementation
        # would depend on Flink SQL gateway configuration
        data = {
            "statement": sql,
            "session_id": session_id
        }
        
        try:
            result = self._post("/sql/submit", json=data)
            
            return JobSubmissionResult(
                job_id=result.get("job_id", ""),
                status="submitted",
                message=result.get("message")
            )
        except Exception as e:
            return JobSubmissionResult(
                job_id="",
                status="failed",
                message=str(e)
            )
            
    # Utilities
    
    def wait_for_job_completion(
        self,
        job_id: str,
        timeout: Optional[int] = None,
        poll_interval: int = 5
    ) -> JobInfo:
        """Wait for job to complete"""
        import time
        
        start_time = time.time()
        timeout = timeout or self.config.submission_timeout
        
        while True:
            job = self.get_job(job_id)
            
            if job.status in ["FINISHED", "FAILED", "CANCELED"]:
                return job
                
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Job {job_id} did not complete within {timeout}s")
                
            time.sleep(poll_interval)
            
    def get_job_plan(self, job_id: str) -> Dict[str, Any]:
        """Get job execution plan"""
        return self._get(f"/jobs/{job_id}/plan")
        
    def rescale_job(
        self,
        job_id: str,
        parallelism: int
    ) -> Dict[str, Any]:
        """Rescale a job (requires savepoint)"""
        # First trigger savepoint
        savepoint_result = self.trigger_savepoint(job_id, cancel_job=True)
        trigger_id = savepoint_result["trigger-id"]
        
        # Wait for savepoint completion
        import time
        while True:
            status = self.get_savepoint_status(job_id, trigger_id)
            if status["status"]["id"] == "COMPLETED":
                savepoint_path = status["operation"]["location"]
                break
            elif status["status"]["id"] == "FAILED":
                raise Exception("Savepoint failed")
            time.sleep(2)
            
        # Resubmit job with new parallelism
        return self.submit_job(
            parallelism=parallelism,
            savepoint_path=savepoint_path
        ) 