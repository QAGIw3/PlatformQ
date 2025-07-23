"""
Processing Service Client

Client for data processing operations.
"""

import logging
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

from .base_client import BaseServiceClient, ClientConfig

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """Processing job status"""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class ProcessingType(Enum):
    """Types of processing"""
    BATCH = "batch"
    STREAM = "stream"
    REAL_TIME = "real_time"
    MICRO_BATCH = "micro_batch"


class ComputeEngine(Enum):
    """Compute engines"""
    SPARK = "spark"
    FLINK = "flink"
    PYTHON = "python"
    SQL = "sql"
    CUSTOM = "custom"


@dataclass
class ProcessingJob:
    """Processing job"""
    id: str
    name: str
    status: JobStatus
    type: ProcessingType
    engine: ComputeEngine
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration: Optional[float] = None
    input_datasets: List[str] = field(default_factory=list)
    output_datasets: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    retry_count: int = 0


@dataclass
class Pipeline:
    """Processing pipeline"""
    id: str
    name: str
    description: Optional[str] = None
    stages: List[Dict[str, Any]] = field(default_factory=list)
    schedule: Optional[str] = None
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    tags: List[str] = field(default_factory=list)


@dataclass
class Transform:
    """Data transformation"""
    id: str
    name: str
    type: str
    description: Optional[str] = None
    input_schema: Dict[str, Any] = field(default_factory=dict)
    output_schema: Dict[str, Any] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)
    code: Optional[str] = None


@dataclass
class DataQualityCheck:
    """Data quality check"""
    id: str
    name: str
    type: str
    rules: List[Dict[str, Any]] = field(default_factory=list)
    thresholds: Dict[str, float] = field(default_factory=dict)
    actions: Dict[str, str] = field(default_factory=dict)


class ProcessingServiceClient(BaseServiceClient):
    """
    Client for processing service operations.
    
    Features:
    - Batch processing
    - Stream processing
    - Pipeline management
    - Transform operations
    - Job monitoring
    """
    
    def __init__(self, config: Optional[ClientConfig] = None, **kwargs):
        if not config:
            config = ClientConfig(service_name="batch-processing-service")
        super().__init__(config, **kwargs)
        
    # Job Operations
    
    async def submit_job(
        self,
        name: str,
        processing_type: ProcessingType,
        engine: ComputeEngine,
        config: Dict[str, Any],
        input_datasets: Optional[List[str]] = None,
        output_datasets: Optional[List[str]] = None,
        priority: int = 5,
        tags: Optional[List[str]] = None
    ) -> ProcessingJob:
        """
        Submit a processing job.
        
        Args:
            name: Job name
            processing_type: Type of processing
            engine: Compute engine to use
            config: Job configuration
            input_datasets: Input dataset IDs
            output_datasets: Output dataset IDs
            priority: Job priority (1-10)
            tags: Job tags
            
        Returns:
            Submitted job
        """
        data = {
            "name": name,
            "type": processing_type.value,
            "engine": engine.value,
            "config": config,
            "input_datasets": input_datasets or [],
            "output_datasets": output_datasets or [],
            "priority": priority,
            "tags": tags or []
        }
        
        response = await self.post("/jobs", json_data=data)
        
        return ProcessingJob(
            id=response["id"],
            name=response["name"],
            status=JobStatus(response["status"]),
            type=ProcessingType(response["type"]),
            engine=ComputeEngine(response["engine"]),
            started_at=response.get("started_at"),
            completed_at=response.get("completed_at"),
            duration=response.get("duration"),
            input_datasets=response.get("input_datasets", []),
            output_datasets=response.get("output_datasets", []),
            config=response.get("config", {}),
            metrics=response.get("metrics", {}),
            error=response.get("error"),
            retry_count=response.get("retry_count", 0)
        )
        
    async def get_job(self, job_id: str) -> Optional[ProcessingJob]:
        """
        Get job by ID.
        
        Args:
            job_id: Job ID
            
        Returns:
            Job if found
        """
        try:
            response = await self.get(f"/jobs/{job_id}")
            
            return ProcessingJob(
                id=response["id"],
                name=response["name"],
                status=JobStatus(response["status"]),
                type=ProcessingType(response["type"]),
                engine=ComputeEngine(response["engine"]),
                started_at=response.get("started_at"),
                completed_at=response.get("completed_at"),
                duration=response.get("duration"),
                input_datasets=response.get("input_datasets", []),
                output_datasets=response.get("output_datasets", []),
                config=response.get("config", {}),
                metrics=response.get("metrics", {}),
                error=response.get("error"),
                retry_count=response.get("retry_count", 0)
            )
        except Exception as e:
            logger.error(f"Failed to get job {job_id}: {e}")
            return None
            
    async def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running job.
        
        Args:
            job_id: Job ID
            
        Returns:
            Success status
        """
        response = await self.post(f"/jobs/{job_id}/cancel")
        return response.get("success", False)
        
    async def retry_job(self, job_id: str) -> ProcessingJob:
        """
        Retry a failed job.
        
        Args:
            job_id: Job ID
            
        Returns:
            New job instance
        """
        response = await self.post(f"/jobs/{job_id}/retry")
        
        return ProcessingJob(
            id=response["id"],
            name=response["name"],
            status=JobStatus(response["status"]),
            type=ProcessingType(response["type"]),
            engine=ComputeEngine(response["engine"]),
            started_at=response.get("started_at"),
            completed_at=response.get("completed_at"),
            duration=response.get("duration"),
            input_datasets=response.get("input_datasets", []),
            output_datasets=response.get("output_datasets", []),
            config=response.get("config", {}),
            metrics=response.get("metrics", {}),
            error=response.get("error"),
            retry_count=response.get("retry_count", 0)
        )
        
    async def list_jobs(
        self,
        status: Optional[JobStatus] = None,
        processing_type: Optional[ProcessingType] = None,
        engine: Optional[ComputeEngine] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[ProcessingJob]:
        """
        List processing jobs.
        
        Args:
            status: Filter by status
            processing_type: Filter by type
            engine: Filter by engine
            start_date: Filter by start date
            end_date: Filter by end date
            limit: Maximum results
            offset: Pagination offset
            
        Returns:
            List of jobs
        """
        params = {
            "limit": limit,
            "offset": offset
        }
        
        if status:
            params["status"] = status.value
        if processing_type:
            params["type"] = processing_type.value
        if engine:
            params["engine"] = engine.value
        if start_date:
            params["start_date"] = start_date.isoformat()
        if end_date:
            params["end_date"] = end_date.isoformat()
            
        response = await self.get("/jobs", params=params)
        
        return [
            ProcessingJob(
                id=j["id"],
                name=j["name"],
                status=JobStatus(j["status"]),
                type=ProcessingType(j["type"]),
                engine=ComputeEngine(j["engine"]),
                started_at=j.get("started_at"),
                completed_at=j.get("completed_at"),
                duration=j.get("duration"),
                input_datasets=j.get("input_datasets", []),
                output_datasets=j.get("output_datasets", []),
                config=j.get("config", {}),
                metrics=j.get("metrics", {}),
                error=j.get("error"),
                retry_count=j.get("retry_count", 0)
            )
            for j in response.get("jobs", [])
        ]
        
    async def get_job_logs(
        self,
        job_id: str,
        limit: int = 1000,
        offset: int = 0,
        level: Optional[str] = None
    ) -> List[str]:
        """
        Get job logs.
        
        Args:
            job_id: Job ID
            limit: Maximum log lines
            offset: Log offset
            level: Log level filter
            
        Returns:
            Log lines
        """
        params = {
            "limit": limit,
            "offset": offset
        }
        
        if level:
            params["level"] = level
            
        response = await self.get(f"/jobs/{job_id}/logs", params=params)
        return response.get("logs", [])
        
    # Pipeline Operations
    
    async def create_pipeline(
        self,
        name: str,
        stages: List[Dict[str, Any]],
        description: Optional[str] = None,
        schedule: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> Pipeline:
        """
        Create a processing pipeline.
        
        Args:
            name: Pipeline name
            stages: Pipeline stages
            description: Pipeline description
            schedule: Cron schedule
            tags: Pipeline tags
            
        Returns:
            Created pipeline
        """
        data = {
            "name": name,
            "stages": stages,
            "description": description,
            "schedule": schedule,
            "tags": tags or []
        }
        
        response = await self.post("/pipelines", json_data=data)
        
        return Pipeline(
            id=response["id"],
            name=response["name"],
            description=response.get("description"),
            stages=response.get("stages", []),
            schedule=response.get("schedule"),
            is_active=response.get("is_active", True),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at"),
            tags=response.get("tags", [])
        )
        
    async def get_pipeline(self, pipeline_id: str) -> Optional[Pipeline]:
        """
        Get pipeline by ID.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Pipeline if found
        """
        try:
            response = await self.get(f"/pipelines/{pipeline_id}")
            
            return Pipeline(
                id=response["id"],
                name=response["name"],
                description=response.get("description"),
                stages=response.get("stages", []),
                schedule=response.get("schedule"),
                is_active=response.get("is_active", True),
                created_at=response.get("created_at"),
                updated_at=response.get("updated_at"),
                tags=response.get("tags", [])
            )
        except Exception as e:
            logger.error(f"Failed to get pipeline {pipeline_id}: {e}")
            return None
            
    async def update_pipeline(
        self,
        pipeline_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        stages: Optional[List[Dict[str, Any]]] = None,
        schedule: Optional[str] = None,
        is_active: Optional[bool] = None
    ) -> Pipeline:
        """
        Update pipeline.
        
        Args:
            pipeline_id: Pipeline ID
            name: New name
            description: New description
            stages: New stages
            schedule: New schedule
            is_active: Active status
            
        Returns:
            Updated pipeline
        """
        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if stages is not None:
            data["stages"] = stages
        if schedule is not None:
            data["schedule"] = schedule
        if is_active is not None:
            data["is_active"] = is_active
            
        response = await self.patch(f"/pipelines/{pipeline_id}", json_data=data)
        
        return Pipeline(
            id=response["id"],
            name=response["name"],
            description=response.get("description"),
            stages=response.get("stages", []),
            schedule=response.get("schedule"),
            is_active=response.get("is_active", True),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at"),
            tags=response.get("tags", [])
        )
        
    async def trigger_pipeline(
        self,
        pipeline_id: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Trigger pipeline execution.
        
        Args:
            pipeline_id: Pipeline ID
            parameters: Runtime parameters
            
        Returns:
            Execution ID
        """
        data = {
            "parameters": parameters or {}
        }
        
        response = await self.post(f"/pipelines/{pipeline_id}/trigger", json_data=data)
        return response["execution_id"]
        
    async def delete_pipeline(self, pipeline_id: str) -> bool:
        """
        Delete pipeline.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Success status
        """
        response = await self.delete(f"/pipelines/{pipeline_id}")
        return response.get("success", False)
        
    # Transform Operations
    
    async def create_transform(
        self,
        name: str,
        transform_type: str,
        config: Dict[str, Any],
        description: Optional[str] = None,
        input_schema: Optional[Dict[str, Any]] = None,
        output_schema: Optional[Dict[str, Any]] = None,
        code: Optional[str] = None
    ) -> Transform:
        """
        Create a data transform.
        
        Args:
            name: Transform name
            transform_type: Type of transform
            config: Transform configuration
            description: Transform description
            input_schema: Expected input schema
            output_schema: Expected output schema
            code: Transform code (if custom)
            
        Returns:
            Created transform
        """
        data = {
            "name": name,
            "type": transform_type,
            "config": config,
            "description": description,
            "input_schema": input_schema or {},
            "output_schema": output_schema or {},
            "code": code
        }
        
        response = await self.post("/transforms", json_data=data)
        
        return Transform(
            id=response["id"],
            name=response["name"],
            type=response["type"],
            description=response.get("description"),
            input_schema=response.get("input_schema", {}),
            output_schema=response.get("output_schema", {}),
            config=response.get("config", {}),
            code=response.get("code")
        )
        
    async def test_transform(
        self,
        transform_id: str,
        sample_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Test transform with sample data.
        
        Args:
            transform_id: Transform ID
            sample_data: Sample input data
            
        Returns:
            Test results including output data
        """
        data = {
            "sample_data": sample_data
        }
        
        return await self.post(f"/transforms/{transform_id}/test", json_data=data)
        
    # Data Quality Operations
    
    async def create_quality_check(
        self,
        name: str,
        check_type: str,
        rules: List[Dict[str, Any]],
        thresholds: Optional[Dict[str, float]] = None,
        actions: Optional[Dict[str, str]] = None
    ) -> DataQualityCheck:
        """
        Create data quality check.
        
        Args:
            name: Check name
            check_type: Type of check
            rules: Quality rules
            thresholds: Quality thresholds
            actions: Actions on failure
            
        Returns:
            Created quality check
        """
        data = {
            "name": name,
            "type": check_type,
            "rules": rules,
            "thresholds": thresholds or {},
            "actions": actions or {}
        }
        
        response = await self.post("/quality-checks", json_data=data)
        
        return DataQualityCheck(
            id=response["id"],
            name=response["name"],
            type=response["type"],
            rules=response.get("rules", []),
            thresholds=response.get("thresholds", {}),
            actions=response.get("actions", {})
        )
        
    async def run_quality_check(
        self,
        check_id: str,
        dataset_id: str,
        sample_size: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Run quality check on dataset.
        
        Args:
            check_id: Quality check ID
            dataset_id: Dataset to check
            sample_size: Sample size (full dataset if not specified)
            
        Returns:
            Quality check results
        """
        data = {
            "dataset_id": dataset_id,
            "sample_size": sample_size
        }
        
        return await self.post(f"/quality-checks/{check_id}/run", json_data=data)
        
    # Monitoring Operations
    
    async def get_job_metrics(
        self,
        job_id: str
    ) -> Dict[str, Any]:
        """
        Get job execution metrics.
        
        Args:
            job_id: Job ID
            
        Returns:
            Job metrics
        """
        return await self.get(f"/jobs/{job_id}/metrics")
        
    async def get_resource_usage(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        aggregation: str = "hour"
    ) -> Dict[str, Any]:
        """
        Get resource usage statistics.
        
        Args:
            start_time: Start time
            end_time: End time
            aggregation: Aggregation level (minute, hour, day)
            
        Returns:
            Resource usage data
        """
        params = {
            "aggregation": aggregation
        }
        
        if start_time:
            params["start_time"] = start_time.isoformat()
        if end_time:
            params["end_time"] = end_time.isoformat()
            
        return await self.get("/monitoring/resource-usage", params=params)
        
    async def get_processing_stats(
        self,
        period: str = "day"
    ) -> Dict[str, Any]:
        """
        Get processing statistics.
        
        Args:
            period: Time period (hour, day, week, month)
            
        Returns:
            Processing statistics
        """
        params = {"period": period}
        return await self.get("/monitoring/stats", params=params)
        
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get processing-specific configuration from Consul"""
        if self.consul_client:
            config = await self.consul_client.get_key(
                f"config/{self.config.service_name}/client"
            )
            return config or {}
        return {} 