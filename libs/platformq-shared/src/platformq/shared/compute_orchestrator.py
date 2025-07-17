"""Compute orchestration library for distributed processing."""
import os
import json
import uuid
import time
import logging
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, asdict
from enum import Enum

import pulsar
from minio import Minio
from pyspark.sql import SparkSession
import apache_ignite
from knative import serving

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """Job status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ComputeJob:
    """Represents a distributed compute job."""
    job_id: str
    job_type: str
    input_data: Dict[str, Any]
    output_location: str
    parameters: Dict[str, Any]
    status: JobStatus = JobStatus.PENDING
    created_at: float = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()
        if not self.job_id:
            self.job_id = str(uuid.uuid4())


class ComputeOrchestrator:
    """Orchestrates distributed compute jobs."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        
        # Configuration
        self.pulsar_url = os.getenv('PULSAR_URL', 'pulsar://localhost:6650')
        self.minio_url = os.getenv('MINIO_URL', 'localhost:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.ignite_host = os.getenv('IGNITE_HOST', 'localhost')
        self.ignite_port = int(os.getenv('IGNITE_PORT', '10800'))
        
        # Initialize clients
        self._initialize_clients()
        
        # Job handlers
        self.job_handlers: Dict[str, Callable] = {}
        
    def _initialize_clients(self):
        """Initialize all required clients."""
        # Pulsar
        self.pulsar_client = pulsar.Client(self.pulsar_url)
        
        # MinIO
        self.minio_client = Minio(
            self.minio_url,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=False
        )
        
        # Ignite
        self.ignite_client = apache_ignite.Client()
        self.ignite_client.connect(self.ignite_host, self.ignite_port)
        
        # Create or get cache for job tracking
        self.job_cache = self.ignite_client.get_or_create_cache(
            f"{self.service_name}_jobs"
        )
        
        logger.info(f"ComputeOrchestrator initialized for {self.service_name}")
    
    def register_job_handler(self, job_type: str, handler: Callable):
        """Register a handler for a specific job type."""
        self.job_handlers[job_type] = handler
        logger.info(f"Registered handler for job type: {job_type}")
    
    def submit_job(self, job: ComputeJob) -> str:
        """Submit a job for distributed processing."""
        try:
            # Store job in Ignite cache
            self.job_cache.put(job.job_id, asdict(job))
            
            # Get producer for job type
            producer = self.pulsar_client.create_producer(
                topic=f'persistent://public/default/{job.job_type}-jobs'
            )
            
            # Send job message
            job_message = {
                'job_id': job.job_id,
                'job_type': job.job_type,
                'input_data': job.input_data,
                'output_location': job.output_location,
                'parameters': job.parameters
            }
            
            producer.send(json.dumps(job_message).encode('utf-8'))
            producer.close()
            
            logger.info(f"Submitted job {job.job_id} of type {job.job_type}")
            return job.job_id
            
        except Exception as e:
            logger.error(f"Failed to submit job: {e}")
            raise
    
    def submit_spark_job(self, job: ComputeJob, 
                        spark_app_path: str,
                        main_class: Optional[str] = None) -> str:
        """Submit a Spark job for distributed processing."""
        try:
            # Create Spark session
            spark = SparkSession.builder \
                .appName(f"{self.service_name}_{job.job_id}") \
                .config("spark.executor.instances", "4") \
                .config("spark.executor.memory", "4g") \
                .config("spark.executor.cores", "2") \
                .getOrCreate()
            
            # Prepare job arguments
            args = [
                "--job-id", job.job_id,
                "--input", json.dumps(job.input_data),
                "--output", job.output_location,
                "--params", json.dumps(job.parameters)
            ]
            
            # Submit job
            if main_class:
                spark.sparkContext.addPyFile(spark_app_path)
                # For Java/Scala apps
                spark._jvm.org.apache.spark.deploy.SparkSubmit.main(
                    ["--class", main_class, spark_app_path] + args
                )
            else:
                # For Python apps
                spark.sparkContext.addPyFile(spark_app_path)
                exec(open(spark_app_path).read())
            
            # Update job status
            job.status = JobStatus.RUNNING
            job.started_at = time.time()
            self.job_cache.put(job.job_id, asdict(job))
            
            spark.stop()
            
            logger.info(f"Submitted Spark job {job.job_id}")
            return job.job_id
            
        except Exception as e:
            logger.error(f"Failed to submit Spark job: {e}")
            job.status = JobStatus.FAILED
            job.error = str(e)
            self.job_cache.put(job.job_id, asdict(job))
            raise
    
    def scale_knative_service(self, service_name: str, 
                             min_replicas: int = 1,
                             max_replicas: int = 100) -> bool:
        """Scale a Knative service for distributed processing."""
        try:
            # Update Knative service configuration
            serving_client = serving.ServingV1Api()
            
            # Get current service
            service = serving_client.get_namespaced_service(
                name=service_name,
                namespace="default"
            )
            
            # Update scaling annotations
            service.spec.template.metadata.annotations.update({
                "autoscaling.knative.dev/minScale": str(min_replicas),
                "autoscaling.knative.dev/maxScale": str(max_replicas),
                "autoscaling.knative.dev/target": "100"  # Target concurrency
            })
            
            # Apply update
            serving_client.replace_namespaced_service(
                name=service_name,
                namespace="default",
                body=service
            )
            
            logger.info(f"Scaled Knative service {service_name} to {min_replicas}-{max_replicas} replicas")
            return True
            
        except Exception as e:
            logger.error(f"Failed to scale Knative service: {e}")
            return False
    
    def get_job_status(self, job_id: str) -> Optional[ComputeJob]:
        """Get the status of a job."""
        try:
            job_data = self.job_cache.get(job_id)
            if job_data:
                return ComputeJob(**job_data)
            return None
        except Exception as e:
            logger.error(f"Failed to get job status: {e}")
            return None
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job."""
        try:
            job_data = self.job_cache.get(job_id)
            if job_data:
                job = ComputeJob(**job_data)
                if job.status in [JobStatus.PENDING, JobStatus.RUNNING]:
                    job.status = JobStatus.CANCELLED
                    job.completed_at = time.time()
                    self.job_cache.put(job_id, asdict(job))
                    
                    # Send cancellation message
                    producer = self.pulsar_client.create_producer(
                        topic=f'persistent://public/default/{job.job_type}-cancel'
                    )
                    producer.send(json.dumps({'job_id': job_id}).encode('utf-8'))
                    producer.close()
                    
                    logger.info(f"Cancelled job {job_id}")
                    return True
            return False
        except Exception as e:
            logger.error(f"Failed to cancel job: {e}")
            return False
    
    def list_jobs(self, status: Optional[JobStatus] = None, 
                  limit: int = 100) -> List[ComputeJob]:
        """List jobs with optional status filter."""
        try:
            jobs = []
            query = self.ignite_client.sql(
                f"SELECT * FROM {self.service_name}_jobs.CACHE"
            )
            
            for row in query:
                job_data = row[1]  # Value is in second column
                job = ComputeJob(**job_data)
                if status is None or job.status == status:
                    jobs.append(job)
                if len(jobs) >= limit:
                    break
            
            return jobs
        except Exception as e:
            logger.error(f"Failed to list jobs: {e}")
            return []
    
    def cleanup_old_jobs(self, days: int = 7):
        """Clean up old completed/failed jobs."""
        try:
            cutoff_time = time.time() - (days * 24 * 60 * 60)
            
            query = self.ignite_client.sql(
                f"SELECT * FROM {self.service_name}_jobs.CACHE"
            )
            
            for row in query:
                job_id = row[0]
                job_data = row[1]
                job = ComputeJob(**job_data)
                
                if (job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED] 
                    and job.completed_at and job.completed_at < cutoff_time):
                    self.job_cache.remove(job_id)
                    logger.info(f"Cleaned up old job {job_id}")
                    
        except Exception as e:
            logger.error(f"Failed to cleanup old jobs: {e}")
    
    def close(self):
        """Close all client connections."""
        try:
            self.pulsar_client.close()
            self.ignite_client.close()
            logger.info("ComputeOrchestrator closed")
        except Exception as e:
            logger.error(f"Error closing ComputeOrchestrator: {e}")


class JobResultProcessor:
    """Processes job results from workers."""
    
    def __init__(self, orchestrator: ComputeOrchestrator, job_type: str):
        self.orchestrator = orchestrator
        self.job_type = job_type
        self.consumer = None
        self._setup_consumer()
    
    def _setup_consumer(self):
        """Set up Pulsar consumer for job results."""
        self.consumer = self.orchestrator.pulsar_client.subscribe(
            topic=f'persistent://public/default/{self.job_type}-results',
            subscription_name=f'{self.orchestrator.service_name}-results-sub'
        )
    
    def process_results(self):
        """Process job results continuously."""
        while True:
            try:
                msg = self.consumer.receive(timeout_millis=1000)
                result_data = json.loads(msg.data().decode('utf-8'))
                
                job_id = result_data['job_id']
                status = result_data['status']
                
                # Update job in cache
                job_data = self.orchestrator.job_cache.get(job_id)
                if job_data:
                    job = ComputeJob(**job_data)
                    
                    if status == 'completed':
                        job.status = JobStatus.COMPLETED
                        job.result = result_data.get('result', {})
                    else:
                        job.status = JobStatus.FAILED
                        job.error = result_data.get('error', 'Unknown error')
                    
                    job.completed_at = time.time()
                    self.orchestrator.job_cache.put(job_id, asdict(job))
                    
                    # Call registered handler if exists
                    if self.job_type in self.orchestrator.job_handlers:
                        handler = self.orchestrator.job_handlers[self.job_type]
                        handler(job)
                    
                    logger.info(f"Processed result for job {job_id}: {status}")
                
                self.consumer.acknowledge(msg)
                
            except pulsar.Timeout:
                continue
            except Exception as e:
                logger.error(f"Error processing results: {e}")
    
    def close(self):
        """Close the consumer."""
        if self.consumer:
            self.consumer.close() 