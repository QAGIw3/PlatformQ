"""Job Scheduler for Batch Processing Service

Manages job submission, scheduling, monitoring, and lifecycle.
"""

import logging
import asyncio
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from enum import Enum
import json
import os
from croniter import croniter

from app.core.config import Settings
from app.core.spark_manager import SparkManager
from app.jobs import get_job_handler


logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """Job status enumeration"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SCHEDULED = "scheduled"


class JobType(Enum):
    """Supported job types"""
    SPARK_SQL = "spark_sql"
    ML_TRAINING = "ml_training"
    ETL_PIPELINE = "etl_pipeline"
    FEATURE_ENGINEERING = "feature_engineering"
    GRAPH_PROCESSING = "graph_processing"


class BatchJob:
    """Represents a batch processing job"""
    
    def __init__(self, job_id: str, name: str, job_type: str, config: Dict[str, Any],
                 resource_profile: str = "medium", priority: int = 5, 
                 schedule: Optional[str] = None):
        self.id = job_id
        self.name = name
        self.type = job_type
        self.config = config
        self.resource_profile = resource_profile
        self.priority = priority
        self.schedule = schedule
        self.status = JobStatus.PENDING if not schedule else JobStatus.SCHEDULED
        self.created_at = datetime.utcnow()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.error: Optional[str] = None
        self.result: Optional[Dict[str, Any]] = None
        self.spark_app_id: Optional[str] = None
        self.log_path: Optional[str] = None
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "config": self.config,
            "resource_profile": self.resource_profile,
            "priority": self.priority,
            "schedule": self.schedule,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": (self.completed_at - self.started_at).total_seconds() 
                              if self.completed_at and self.started_at else None,
            "error": self.error,
            "result": self.result,
            "spark_app_id": self.spark_app_id,
            "log_path": self.log_path
        }


class JobScheduler:
    """Manages batch job scheduling and execution"""
    
    def __init__(self, settings: Settings, spark_manager: SparkManager):
        self.settings = settings
        self.spark_manager = spark_manager
        self.jobs: Dict[str, BatchJob] = {}
        self.job_queue: asyncio.Queue = asyncio.Queue()
        self.scheduled_jobs: List[BatchJob] = []
        self._executor_task: Optional[asyncio.Task] = None
        self._scheduler_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running_jobs: Dict[str, asyncio.Task] = {}
        
    async def start(self):
        """Start the job scheduler"""
        logger.info("Starting JobScheduler")
        
        # Start executor task
        self._executor_task = asyncio.create_task(self._job_executor())
        
        # Start scheduler task for cron jobs
        self._scheduler_task = asyncio.create_task(self._cron_scheduler())
        
        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._cleanup_old_jobs())
        
        logger.info("JobScheduler started")
        
    async def stop(self):
        """Stop the job scheduler"""
        logger.info("Stopping JobScheduler")
        
        # Cancel all running jobs
        for job_id, task in self._running_jobs.items():
            task.cancel()
            
        # Cancel background tasks
        if self._executor_task:
            self._executor_task.cancel()
        if self._scheduler_task:
            self._scheduler_task.cancel()
        if self._cleanup_task:
            self._cleanup_task.cancel()
            
        logger.info("JobScheduler stopped")
        
    async def submit_job(self, name: str, job_type: str, config: Dict[str, Any],
                        resource_profile: str = "medium", priority: int = 5,
                        schedule: Optional[str] = None) -> str:
        """Submit a new batch job"""
        job_id = str(uuid.uuid4())
        logger.info(f"Submitting job {name} ({job_id}) of type {job_type}")
        
        # Validate job type
        if job_type not in [jt.value for jt in JobType]:
            raise ValueError(f"Invalid job type: {job_type}")
            
        # Validate resource profile
        if resource_profile not in self.settings.resource_profiles:
            raise ValueError(f"Invalid resource profile: {resource_profile}")
            
        # Validate schedule if provided
        if schedule:
            try:
                croniter(schedule)
            except:
                raise ValueError(f"Invalid cron schedule: {schedule}")
                
        # Create job
        job = BatchJob(
            job_id=job_id,
            name=name,
            job_type=job_type,
            config=config,
            resource_profile=resource_profile,
            priority=priority,
            schedule=schedule
        )
        
        self.jobs[job_id] = job
        
        # Handle scheduled vs immediate jobs
        if schedule:
            self.scheduled_jobs.append(job)
            logger.info(f"Job {job_id} scheduled with cron: {schedule}")
        else:
            await self.job_queue.put(job)
            logger.info(f"Job {job_id} added to queue")
            
        return job_id
        
    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job status"""
        job = self.jobs.get(job_id)
        if not job:
            return None
        return job.to_dict()
        
    async def list_jobs(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all jobs"""
        jobs = []
        for job in self.jobs.values():
            if status and job.status.value != status:
                continue
            jobs.append(job.to_dict())
        return sorted(jobs, key=lambda x: x["created_at"], reverse=True)
        
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a job"""
        job = self.jobs.get(job_id)
        if not job:
            return False
            
        if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            return False
            
        logger.info(f"Cancelling job {job_id}")
        
        # Cancel running task if exists
        if job_id in self._running_jobs:
            self._running_jobs[job_id].cancel()
            
        job.status = JobStatus.CANCELLED
        job.completed_at = datetime.utcnow()
        
        return True
        
    async def get_job_logs(self, job_id: str, lines: int = 100) -> Optional[List[str]]:
        """Get job logs"""
        job = self.jobs.get(job_id)
        if not job or not job.log_path:
            return None
            
        try:
            # Read logs from file/S3
            # For now, returning mock logs
            return [
                f"[INFO] Job {job_id} started at {job.started_at}",
                f"[INFO] Using resource profile: {job.resource_profile}",
                f"[INFO] Executing {job.type} job",
                f"[INFO] Job completed with status: {job.status.value}"
            ]
        except Exception as e:
            logger.error(f"Failed to read logs for job {job_id}: {e}")
            return None
            
    async def _job_executor(self):
        """Execute jobs from the queue"""
        while True:
            try:
                # Get job from queue
                job = await self.job_queue.get()
                
                # Check if we can run more jobs
                if len(self._running_jobs) >= self.settings.max_concurrent_jobs:
                    # Put job back in queue and wait
                    await self.job_queue.put(job)
                    await asyncio.sleep(5)
                    continue
                    
                # Start job execution
                task = asyncio.create_task(self._execute_job(job))
                self._running_jobs[job.id] = task
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in job executor: {e}")
                
    async def _execute_job(self, job: BatchJob):
        """Execute a single job"""
        logger.info(f"Executing job {job.id} ({job.name})")
        
        try:
            # Update job status
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            
            # Get job handler based on type
            handler = get_job_handler(job.type)
            if not handler:
                raise ValueError(f"No handler found for job type: {job.type}")
                
            # Configure Spark for this job
            await self._configure_spark_for_job(job)
            
            # Execute job
            result = await handler.execute(
                spark_manager=self.spark_manager,
                config=job.config,
                job_id=job.id
            )
            
            # Update job with results
            job.status = JobStatus.COMPLETED
            job.result = result
            logger.info(f"Job {job.id} completed successfully")
            
        except Exception as e:
            logger.error(f"Job {job.id} failed: {e}")
            job.status = JobStatus.FAILED
            job.error = str(e)
            
        finally:
            job.completed_at = datetime.utcnow()
            
            # Remove from running jobs
            if job.id in self._running_jobs:
                del self._running_jobs[job.id]
                
    async def _configure_spark_for_job(self, job: BatchJob):
        """Configure Spark session for specific job"""
        spark = self.spark_manager.get_spark()
        profile = self.settings.resource_profiles[job.resource_profile]
        
        # Set dynamic configurations
        spark.conf.set("spark.executor.memory", profile["executor_memory"])
        spark.conf.set("spark.executor.cores", str(profile["executor_cores"]))
        spark.conf.set("spark.dynamicAllocation.maxExecutors", str(profile["max_executors"]))
        
        # Set job description
        spark.sparkContext.setJobDescription(f"{job.name} ({job.id})")
        
    async def _cron_scheduler(self):
        """Check and execute scheduled jobs"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                now = datetime.utcnow()
                for job in self.scheduled_jobs[:]:
                    if job.schedule:
                        cron = croniter(job.schedule, now)
                        next_run = cron.get_next(datetime)
                        
                        # If it's time to run
                        if next_run <= now:
                            # Create a new instance of the job
                            new_job = BatchJob(
                                job_id=str(uuid.uuid4()),
                                name=f"{job.name}_scheduled_{now.strftime('%Y%m%d_%H%M%S')}",
                                job_type=job.type,
                                config=job.config,
                                resource_profile=job.resource_profile,
                                priority=job.priority,
                                schedule=None  # Don't schedule the instance
                            )
                            
                            self.jobs[new_job.id] = new_job
                            await self.job_queue.put(new_job)
                            logger.info(f"Scheduled job {job.id} triggered as {new_job.id}")
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cron scheduler: {e}")
                
    async def _cleanup_old_jobs(self):
        """Cleanup old completed jobs"""
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour
                
                cutoff = datetime.utcnow() - timedelta(days=self.settings.job_log_retention_days)
                to_remove = []
                
                for job_id, job in self.jobs.items():
                    if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                        if job.completed_at and job.completed_at < cutoff:
                            to_remove.append(job_id)
                            
                for job_id in to_remove:
                    # Clean up logs if stored
                    job = self.jobs[job_id]
                    if job.log_path:
                        # Delete log file
                        pass
                        
                    del self.jobs[job_id]
                    logger.info(f"Cleaned up old job {job_id}")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in job cleanup: {e}") 