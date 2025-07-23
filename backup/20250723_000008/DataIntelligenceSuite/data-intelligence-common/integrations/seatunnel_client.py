"""
Apache SeaTunnel Client Integration

Provides high-level client for Apache SeaTunnel data integration operations.
"""

import logging
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
import requests
import json
import yaml

logger = logging.getLogger(__name__)


@dataclass
class SeaTunnelConfig:
    """Configuration for SeaTunnel client"""
    api_endpoint: str = "http://localhost:8080"
    engine: str = "spark"  # spark, flink, or seatunnel-engine
    
    # Job submission
    config_file_path: Optional[str] = None
    master: Optional[str] = None
    deploy_mode: str = "client"
    
    # Engine specific
    spark_home: Optional[str] = None
    flink_home: Optional[str] = None
    
    # Timeouts
    request_timeout: int = 30
    job_timeout: int = 3600
    
    # Authentication
    auth_token: Optional[str] = None


@dataclass
class ConnectorConfig:
    """Configuration for a SeaTunnel connector"""
    type: str  # source or sink
    plugin_name: str
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TransformConfig:
    """Configuration for a SeaTunnel transform"""
    plugin_name: str
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class JobConfig:
    """SeaTunnel job configuration"""
    name: str
    env: Dict[str, Any] = field(default_factory=dict)
    source: List[ConnectorConfig] = field(default_factory=list)
    transform: List[TransformConfig] = field(default_factory=list)
    sink: List[ConnectorConfig] = field(default_factory=list)


@dataclass
class JobStatus:
    """SeaTunnel job status"""
    job_id: str
    name: str
    status: str
    engine: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    error: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)


class SeaTunnelClient:
    """
    High-level client for Apache SeaTunnel operations.
    
    Features:
    - Job configuration and submission
    - Connector management
    - Job monitoring
    - Multi-engine support (Spark, Flink, SeaTunnel Engine)
    - Configuration validation
    """
    
    def __init__(self, config: SeaTunnelConfig):
        self.config = config
        self._session = requests.Session()
        
        if config.auth_token:
            self._session.headers.update({
                "Authorization": f"Bearer {config.auth_token}"
            })
            
    # Configuration builders
    
    def create_job_config(
        self,
        name: str,
        env: Optional[Dict[str, Any]] = None
    ) -> JobConfig:
        """Create a new job configuration"""
        default_env = {
            "execution.parallelism": 1,
            "execution.checkpoint.interval": 10000,
            "execution.checkpoint.data-uri": "hdfs://localhost:9000/seatunnel/checkpoint"
        }
        
        if self.config.engine == "spark":
            default_env.update({
                "spark.app.name": name,
                "spark.executor.instances": 2,
                "spark.executor.cores": 1,
                "spark.executor.memory": "1g"
            })
        elif self.config.engine == "flink":
            default_env.update({
                "execution.runtime-mode": "STREAMING",
                "execution.time-characteristic": "EventTime"
            })
            
        if env:
            default_env.update(env)
            
        return JobConfig(name=name, env=default_env)
        
    def add_source(
        self,
        job_config: JobConfig,
        plugin_name: str,
        config: Dict[str, Any]
    ) -> JobConfig:
        """Add source connector to job"""
        source = ConnectorConfig(
            type="source",
            plugin_name=plugin_name,
            config=config
        )
        job_config.source.append(source)
        return job_config
        
    def add_transform(
        self,
        job_config: JobConfig,
        plugin_name: str,
        config: Dict[str, Any]
    ) -> JobConfig:
        """Add transform to job"""
        transform = TransformConfig(
            plugin_name=plugin_name,
            config=config
        )
        job_config.transform.append(transform)
        return job_config
        
    def add_sink(
        self,
        job_config: JobConfig,
        plugin_name: str,
        config: Dict[str, Any]
    ) -> JobConfig:
        """Add sink connector to job"""
        sink = ConnectorConfig(
            type="sink",
            plugin_name=plugin_name,
            config=config
        )
        job_config.sink.append(sink)
        return job_config
        
    # Common source configurations
    
    def create_jdbc_source(
        self,
        url: str,
        driver: str,
        query: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Create JDBC source configuration"""
        config = {
            "url": url,
            "driver": driver,
            "query": query
        }
        
        if username:
            config["user"] = username
        if password:
            config["password"] = password
            
        config.update(kwargs)
        return config
        
    def create_kafka_source(
        self,
        bootstrap_servers: str,
        topics: Union[str, List[str]],
        consumer_group: str,
        format: str = "json",
        **kwargs
    ) -> Dict[str, Any]:
        """Create Kafka source configuration"""
        config = {
            "bootstrap.servers": bootstrap_servers,
            "topics": topics if isinstance(topics, str) else ",".join(topics),
            "consumer.group": consumer_group,
            "format": format,
            "consumer.auto.offset.reset": "latest"
        }
        
        config.update(kwargs)
        return config
        
    def create_file_source(
        self,
        path: str,
        format: str = "json",
        schema: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Create file source configuration"""
        config = {
            "path": path,
            "format": format
        }
        
        if schema:
            config["schema"] = schema
            
        config.update(kwargs)
        return config
        
    # Common sink configurations
    
    def create_jdbc_sink(
        self,
        url: str,
        driver: str,
        table: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        save_mode: str = "append",
        **kwargs
    ) -> Dict[str, Any]:
        """Create JDBC sink configuration"""
        config = {
            "url": url,
            "driver": driver,
            "table": table,
            "save_mode": save_mode
        }
        
        if username:
            config["user"] = username
        if password:
            config["password"] = password
            
        config.update(kwargs)
        return config
        
    def create_elasticsearch_sink(
        self,
        hosts: List[str],
        index: str,
        index_type: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Create Elasticsearch sink configuration"""
        config = {
            "hosts": hosts,
            "index": index
        }
        
        if index_type:
            config["index_type"] = index_type
            
        config.update(kwargs)
        return config
        
    def create_console_sink(self) -> Dict[str, Any]:
        """Create console sink configuration"""
        return {"limit": 100}
        
    # Job operations
    
    def submit_job(
        self,
        job_config: JobConfig,
        wait_for_completion: bool = False
    ) -> JobStatus:
        """Submit a SeaTunnel job"""
        # Convert job config to SeaTunnel format
        config_dict = self._job_config_to_dict(job_config)
        
        # Save config to temporary file or submit via API
        if self.config.api_endpoint:
            return self._submit_via_api(config_dict, wait_for_completion)
        else:
            return self._submit_via_cli(config_dict, wait_for_completion)
            
    def _job_config_to_dict(self, job_config: JobConfig) -> Dict[str, Any]:
        """Convert JobConfig to SeaTunnel configuration format"""
        config = {
            "env": job_config.env,
            "source": [
                {
                    s.plugin_name: s.config
                }
                for s in job_config.source
            ],
            "sink": [
                {
                    s.plugin_name: s.config
                }
                for s in job_config.sink
            ]
        }
        
        if job_config.transform:
            config["transform"] = [
                {
                    t.plugin_name: t.config
                }
                for t in job_config.transform
            ]
            
        return config
        
    def _submit_via_api(
        self,
        config: Dict[str, Any],
        wait_for_completion: bool
    ) -> JobStatus:
        """Submit job via REST API"""
        import uuid
        
        job_id = str(uuid.uuid4())
        
        # Submit job
        response = self._session.post(
            f"{self.config.api_endpoint}/api/v1/jobs",
            json={
                "job_id": job_id,
                "config": config,
                "engine": self.config.engine
            },
            timeout=self.config.request_timeout
        )
        
        response.raise_for_status()
        result = response.json()
        
        status = JobStatus(
            job_id=result["job_id"],
            name=config.get("env", {}).get("job.name", "unnamed"),
            status="RUNNING",
            engine=self.config.engine,
            start_time=datetime.utcnow()
        )
        
        if wait_for_completion:
            status = self.wait_for_job(status.job_id)
            
        return status
        
    def _submit_via_cli(
        self,
        config: Dict[str, Any],
        wait_for_completion: bool
    ) -> JobStatus:
        """Submit job via CLI (local execution)"""
        import subprocess
        import tempfile
        import uuid
        
        job_id = str(uuid.uuid4())
        
        # Write config to temporary file
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.conf',
            delete=False
        ) as f:
            # SeaTunnel uses HOCON format
            f.write(self._dict_to_hocon(config))
            config_file = f.name
            
        # Build command
        cmd = ["seatunnel"]
        
        if self.config.engine == "spark":
            cmd.extend(["--master", self.config.master or "local[*]"])
            cmd.extend(["--deploy-mode", self.config.deploy_mode])
        elif self.config.engine == "flink":
            cmd.extend(["--target", "local"])
            
        cmd.extend(["--config", config_file])
        
        # Execute
        start_time = datetime.utcnow()
        
        try:
            if wait_for_completion:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=self.config.job_timeout
                )
                
                status = JobStatus(
                    job_id=job_id,
                    name=config.get("env", {}).get("job.name", "unnamed"),
                    status="FINISHED" if result.returncode == 0 else "FAILED",
                    engine=self.config.engine,
                    start_time=start_time,
                    end_time=datetime.utcnow(),
                    error=result.stderr if result.returncode != 0 else None
                )
            else:
                # Run in background
                subprocess.Popen(cmd)
                
                status = JobStatus(
                    job_id=job_id,
                    name=config.get("env", {}).get("job.name", "unnamed"),
                    status="RUNNING",
                    engine=self.config.engine,
                    start_time=start_time
                )
                
        except subprocess.TimeoutExpired:
            status = JobStatus(
                job_id=job_id,
                name=config.get("env", {}).get("job.name", "unnamed"),
                status="TIMEOUT",
                engine=self.config.engine,
                start_time=start_time,
                end_time=datetime.utcnow(),
                error="Job execution timeout"
            )
            
        finally:
            # Clean up temp file
            import os
            os.unlink(config_file)
            
        return status
        
    def _dict_to_hocon(self, d: Dict[str, Any], indent: int = 0) -> str:
        """Convert dictionary to HOCON format"""
        lines = []
        indent_str = "  " * indent
        
        for key, value in d.items():
            if isinstance(value, dict):
                lines.append(f"{indent_str}{key} {{")
                lines.append(self._dict_to_hocon(value, indent + 1))
                lines.append(f"{indent_str}}}")
            elif isinstance(value, list):
                if all(isinstance(item, dict) for item in value):
                    lines.append(f"{indent_str}{key} = [")
                    for item in value:
                        lines.append(f"{indent_str}  {{")
                        lines.append(self._dict_to_hocon(item, indent + 2))
                        lines.append(f"{indent_str}  }}")
                    lines.append(f"{indent_str}]")
                else:
                    lines.append(f"{indent_str}{key} = {json.dumps(value)}")
            else:
                lines.append(f"{indent_str}{key} = {json.dumps(value)}")
                
        return "\n".join(lines)
        
    def get_job_status(self, job_id: str) -> JobStatus:
        """Get job status"""
        response = self._session.get(
            f"{self.config.api_endpoint}/api/v1/jobs/{job_id}",
            timeout=self.config.request_timeout
        )
        
        response.raise_for_status()
        result = response.json()
        
        return JobStatus(
            job_id=result["job_id"],
            name=result["name"],
            status=result["status"],
            engine=result["engine"],
            start_time=datetime.fromisoformat(result["start_time"]),
            end_time=datetime.fromisoformat(result["end_time"]) if result.get("end_time") else None,
            duration_seconds=result.get("duration_seconds"),
            error=result.get("error"),
            metrics=result.get("metrics", {})
        )
        
    def wait_for_job(
        self,
        job_id: str,
        timeout: Optional[int] = None,
        poll_interval: int = 5
    ) -> JobStatus:
        """Wait for job completion"""
        import time
        
        start_time = time.time()
        timeout = timeout or self.config.job_timeout
        
        while True:
            status = self.get_job_status(job_id)
            
            if status.status in ["FINISHED", "FAILED", "CANCELLED"]:
                return status
                
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Job {job_id} did not complete within {timeout}s")
                
            time.sleep(poll_interval)
            
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job"""
        response = self._session.post(
            f"{self.config.api_endpoint}/api/v1/jobs/{job_id}/cancel",
            timeout=self.config.request_timeout
        )
        
        response.raise_for_status()
        result = response.json()
        
        return result.get("success", False)
        
    def list_jobs(
        self,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[JobStatus]:
        """List jobs"""
        params = {"limit": limit}
        if status:
            params["status"] = status
            
        response = self._session.get(
            f"{self.config.api_endpoint}/api/v1/jobs",
            params=params,
            timeout=self.config.request_timeout
        )
        
        response.raise_for_status()
        result = response.json()
        
        return [
            JobStatus(
                job_id=job["job_id"],
                name=job["name"],
                status=job["status"],
                engine=job["engine"],
                start_time=datetime.fromisoformat(job["start_time"]),
                end_time=datetime.fromisoformat(job["end_time"]) if job.get("end_time") else None,
                duration_seconds=job.get("duration_seconds"),
                error=job.get("error"),
                metrics=job.get("metrics", {})
            )
            for job in result.get("jobs", [])
        ]
        
    # Connector management
    
    def list_connectors(
        self,
        connector_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List available connectors"""
        params = {}
        if connector_type:
            params["type"] = connector_type
            
        response = self._session.get(
            f"{self.config.api_endpoint}/api/v1/connectors",
            params=params,
            timeout=self.config.request_timeout
        )
        
        response.raise_for_status()
        return response.json().get("connectors", [])
        
    def validate_config(self, job_config: JobConfig) -> Dict[str, Any]:
        """Validate job configuration"""
        config_dict = self._job_config_to_dict(job_config)
        
        response = self._session.post(
            f"{self.config.api_endpoint}/api/v1/validate",
            json={"config": config_dict},
            timeout=self.config.request_timeout
        )
        
        response.raise_for_status()
        return response.json() 