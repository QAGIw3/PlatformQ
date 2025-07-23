"""Job Manager for Stream Processing Service

Manages submission, monitoring, and lifecycle of Flink streaming jobs.
"""

import asyncio
import logging
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum
import json

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration
from pyflink.table import StreamTableEnvironment
import aiohttp

from app.core.config import Settings
from app.core.state_manager import StateManager


logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """Job status enumeration"""
    INITIALIZING = "initializing"
    RUNNING = "running"
    FINISHED = "finished"
    CANCELING = "canceling"
    CANCELED = "canceled"
    FAILED = "failed"
    SUSPENDED = "suspended"
    RECONCILING = "reconciling"


class JobType(Enum):
    """Supported job types"""
    STREAMING_SQL = "streaming_sql"
    CEP_PATTERN = "cep_pattern"
    STATEFUL_PROCESSING = "stateful_processing"
    WINDOW_AGGREGATION = "window_aggregation"
    ASYNC_IO = "async_io"


class Job:
    """Represents a streaming job"""
    def __init__(self, job_id: str, name: str, job_type: str, config: Dict[str, Any]):
        self.id = job_id
        self.name = name
        self.type = job_type
        self.config = config
        self.status = JobStatus.INITIALIZING
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.flink_job_id: Optional[str] = None
        self.error: Optional[str] = None
        self.metrics: Dict[str, Any] = {}
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "config": self.config,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "flink_job_id": self.flink_job_id,
            "error": self.error,
            "metrics": self.metrics
        }


class JobManager:
    """Manages Flink streaming jobs"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.jobs: Dict[str, Job] = {}
        self.flink_client: Optional[aiohttp.ClientSession] = None
        self.state_manager: Optional[StateManager] = None
        self._monitor_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start the job manager"""
        logger.info("Starting JobManager")
        
        # Initialize Flink client
        self.flink_client = aiohttp.ClientSession()
        
        # Start monitoring task
        self._monitor_task = asyncio.create_task(self._monitor_jobs())
        
        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._cleanup_jobs())
        
        logger.info("JobManager started")
        
    async def stop(self):
        """Stop the job manager"""
        logger.info("Stopping JobManager")
        
        # Cancel tasks
        if self._monitor_task:
            self._monitor_task.cancel()
        if self._cleanup_task:
            self._cleanup_task.cancel()
            
        # Close client
        if self.flink_client:
            await self.flink_client.close()
            
        logger.info("JobManager stopped")
        
    async def submit_job(self, name: str, job_type: str, config: Dict[str, Any],
                        parallelism: Optional[int] = None,
                        checkpoint_interval: Optional[int] = None,
                        restart_strategy: Optional[str] = None) -> str:
        """Submit a new streaming job"""
        job_id = str(uuid.uuid4())
        logger.info(f"Submitting job {name} ({job_id}) of type {job_type}")
        
        # Create job instance
        job = Job(job_id, name, job_type, config)
        self.jobs[job_id] = job
        
        try:
            # Configure job based on type
            if job_type == JobType.STREAMING_SQL.value:
                flink_job_id = await self._submit_sql_job(job, parallelism, checkpoint_interval)
            elif job_type == JobType.CEP_PATTERN.value:
                flink_job_id = await self._submit_cep_job(job, parallelism, checkpoint_interval)
            elif job_type == JobType.STATEFUL_PROCESSING.value:
                flink_job_id = await self._submit_stateful_job(job, parallelism, checkpoint_interval)
            elif job_type == JobType.WINDOW_AGGREGATION.value:
                flink_job_id = await self._submit_window_job(job, parallelism, checkpoint_interval)
            elif job_type == JobType.ASYNC_IO.value:
                flink_job_id = await self._submit_async_job(job, parallelism, checkpoint_interval)
            else:
                raise ValueError(f"Unknown job type: {job_type}")
                
            job.flink_job_id = flink_job_id
            job.status = JobStatus.RUNNING
            job.updated_at = datetime.utcnow()
            
            logger.info(f"Job {job_id} submitted successfully with Flink ID {flink_job_id}")
            return job_id
            
        except Exception as e:
            logger.error(f"Failed to submit job {job_id}: {e}")
            job.status = JobStatus.FAILED
            job.error = str(e)
            job.updated_at = datetime.utcnow()
            raise
            
    async def _submit_sql_job(self, job: Job, parallelism: Optional[int],
                             checkpoint_interval: Optional[int]) -> str:
        """Submit a streaming SQL job"""
        config = Configuration()
        config.set_string("parallelism.default", str(parallelism or self.settings.flink_parallelism))
        
        # Create execution environment
        env = StreamExecutionEnvironment.get_execution_environment(config)
        table_env = StreamTableEnvironment.create(env)
        
        # Configure checkpointing
        if checkpoint_interval:
            env.enable_checkpointing(checkpoint_interval)
            
        # Execute SQL statements
        sql_statements = job.config.get("sql_statements", [])
        for sql in sql_statements:
            table_env.execute_sql(sql)
            
        # Submit job to Flink
        job_client = env.execute_async(job.name)
        return str(job_client.get_job_id())
        
    async def _submit_cep_job(self, job: Job, parallelism: Optional[int],
                             checkpoint_interval: Optional[int]) -> str:
        """Submit a CEP pattern job"""
        # Implementation would create CEP patterns and submit
        # For now, returning a mock ID
        return f"cep-{job.id}"
        
    async def _submit_stateful_job(self, job: Job, parallelism: Optional[int],
                                  checkpoint_interval: Optional[int]) -> str:
        """Submit a stateful processing job"""
        # Implementation would create stateful operators
        # For now, returning a mock ID
        return f"stateful-{job.id}"
        
    async def _submit_window_job(self, job: Job, parallelism: Optional[int],
                                checkpoint_interval: Optional[int]) -> str:
        """Submit a window aggregation job"""
        # Implementation would create windowed operations
        # For now, returning a mock ID
        return f"window-{job.id}"
        
    async def _submit_async_job(self, job: Job, parallelism: Optional[int],
                               checkpoint_interval: Optional[int]) -> str:
        """Submit an async I/O job"""
        # Implementation would create async operators
        # For now, returning a mock ID
        return f"async-{job.id}"
        
    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job status"""
        job = self.jobs.get(job_id)
        if not job:
            return None
            
        # Update status from Flink if running
        if job.status == JobStatus.RUNNING and job.flink_job_id:
            await self._update_job_status(job)
            
        return job.to_dict()
        
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job"""
        job = self.jobs.get(job_id)
        if not job:
            return False
            
        if job.status != JobStatus.RUNNING:
            return False
            
        logger.info(f"Canceling job {job_id}")
        job.status = JobStatus.CANCELING
        
        try:
            # Cancel in Flink
            if job.flink_job_id:
                url = f"http://{self.settings.flink_master}/jobs/{job.flink_job_id}"
                async with self.flink_client.patch(url) as resp:
                    if resp.status == 202:
                        job.status = JobStatus.CANCELED
                        job.updated_at = datetime.utcnow()
                        return True
                        
        except Exception as e:
            logger.error(f"Failed to cancel job {job_id}: {e}")
            job.status = JobStatus.FAILED
            job.error = str(e)
            
        return False
        
    async def create_savepoint(self, job_id: str) -> Optional[str]:
        """Create a savepoint for the job"""
        job = self.jobs.get(job_id)
        if not job or job.status != JobStatus.RUNNING:
            return None
            
        try:
            # Trigger savepoint
            url = f"http://{self.settings.flink_master}/jobs/{job.flink_job_id}/savepoints"
            data = {"target-directory": f"{self.settings.flink_checkpoint_dir}/savepoints"}
            
            async with self.flink_client.post(url, json=data) as resp:
                if resp.status == 202:
                    result = await resp.json()
                    return result.get("location")
                    
        except Exception as e:
            logger.error(f"Failed to create savepoint for job {job_id}: {e}")
            
        return None
        
    async def list_jobs(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all jobs"""
        jobs = []
        for job in self.jobs.values():
            if status and job.status.value != status:
                continue
            jobs.append(job.to_dict())
        return jobs
        
    async def _monitor_jobs(self):
        """Monitor running jobs"""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                for job in self.jobs.values():
                    if job.status == JobStatus.RUNNING:
                        await self._update_job_status(job)
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring jobs: {e}")
                
    async def _update_job_status(self, job: Job):
        """Update job status from Flink"""
        if not job.flink_job_id:
            return
            
        try:
            url = f"http://{self.settings.flink_master}/jobs/{job.flink_job_id}"
            async with self.flink_client.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    flink_state = data.get("state", "").upper()
                    
                    # Map Flink state to our status
                    if flink_state == "RUNNING":
                        job.status = JobStatus.RUNNING
                    elif flink_state == "FINISHED":
                        job.status = JobStatus.FINISHED
                    elif flink_state == "CANCELED":
                        job.status = JobStatus.CANCELED
                    elif flink_state == "FAILED":
                        job.status = JobStatus.FAILED
                        
                    job.updated_at = datetime.utcnow()
                    
                    # Update metrics
                    job.metrics = {
                        "start_time": data.get("start-time"),
                        "duration": data.get("duration"),
                        "vertices": len(data.get("vertices", []))
                    }
                    
        except Exception as e:
            logger.error(f"Failed to update job status for {job.id}: {e}")
            
    async def _cleanup_jobs(self):
        """Clean up old finished jobs"""
        while True:
            try:
                await asyncio.sleep(self.settings.job_cleanup_interval)
                
                # Remove finished jobs older than 1 day
                cutoff = datetime.utcnow().timestamp() - 86400
                to_remove = []
                
                for job_id, job in self.jobs.items():
                    if job.status in [JobStatus.FINISHED, JobStatus.CANCELED, JobStatus.FAILED]:
                        if job.updated_at.timestamp() < cutoff:
                            to_remove.append(job_id)
                            
                for job_id in to_remove:
                    del self.jobs[job_id]
                    logger.info(f"Cleaned up job {job_id}")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error cleaning up jobs: {e}") 