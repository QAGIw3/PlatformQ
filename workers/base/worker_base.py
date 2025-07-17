"""Base worker class for distributed compute tasks."""
import os
import json
import time
import logging
import traceback
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import pulsar
from minio import Minio
from prometheus_client import Counter, Histogram, Gauge, push_to_gateway, CollectorRegistry
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
registry = CollectorRegistry()
jobs_processed = Counter('worker_jobs_processed_total', 'Total number of jobs processed', 
                        ['worker_type', 'status'], registry=registry)
job_duration = Histogram('worker_job_duration_seconds', 'Job processing duration',
                        ['worker_type'], registry=registry)
active_jobs = Gauge('worker_active_jobs', 'Number of currently active jobs',
                   ['worker_type'], registry=registry)

# OpenTelemetry setup
resource = Resource.create({"service.name": "compute-worker"})
provider = TracerProvider(resource=resource)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)


class ComputeWorker(ABC):
    """Base class for all distributed compute workers."""
    
    def __init__(self, worker_type: str):
        self.worker_type = worker_type
        self.pulsar_url = os.getenv('PULSAR_URL', 'pulsar://localhost:6650')
        self.minio_url = os.getenv('MINIO_URL', 'localhost:9000')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.pushgateway_url = os.getenv('PROMETHEUS_PUSHGATEWAY_URL', 'localhost:9091')
        
        # Initialize clients
        self.pulsar_client = None
        self.consumer = None
        self.producer = None
        self.minio_client = None
        
        self._initialize_clients()
    
    def _initialize_clients(self):
        """Initialize Pulsar and MinIO clients."""
        try:
            # Pulsar setup
            self.pulsar_client = pulsar.Client(self.pulsar_url)
            
            # Consumer for job requests
            self.consumer = self.pulsar_client.subscribe(
                topic=f'persistent://public/default/{self.worker_type}-jobs',
                subscription_name=f'{self.worker_type}-worker-sub',
                consumer_type=pulsar.ConsumerType.Shared
            )
            
            # Producer for job results
            self.producer = self.pulsar_client.create_producer(
                topic=f'persistent://public/default/{self.worker_type}-results'
            )
            
            # MinIO setup
            self.minio_client = Minio(
                self.minio_url,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=False
            )
            
            # Ensure buckets exist
            for bucket in ['input-data', 'output-data', 'checkpoints']:
                if not self.minio_client.bucket_exists(bucket):
                    self.minio_client.make_bucket(bucket)
            
            logger.info(f"{self.worker_type} worker initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize worker: {e}")
            raise
    
    @abstractmethod
    def process_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single job. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def validate_job(self, job_data: Dict[str, Any]) -> bool:
        """Validate job data. Must be implemented by subclasses."""
        pass
    
    def download_input_data(self, input_path: str, local_path: str) -> bool:
        """Download input data from MinIO."""
        try:
            bucket, object_name = input_path.split('/', 1)
            self.minio_client.fget_object(bucket, object_name, local_path)
            logger.info(f"Downloaded input data from {input_path} to {local_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to download input data: {e}")
            return False
    
    def upload_output_data(self, local_path: str, output_path: str) -> bool:
        """Upload output data to MinIO."""
        try:
            bucket, object_name = output_path.split('/', 1)
            self.minio_client.fput_object(bucket, object_name, local_path)
            logger.info(f"Uploaded output data from {local_path} to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload output data: {e}")
            return False
    
    def run(self):
        """Main worker loop."""
        logger.info(f"Starting {self.worker_type} worker...")
        
        while True:
            try:
                # Receive job message
                msg = self.consumer.receive(timeout_millis=1000)
                
                with tracer.start_as_current_span(f"{self.worker_type}_job") as span:
                    job_data = json.loads(msg.data().decode('utf-8'))
                    job_id = job_data.get('job_id', 'unknown')
                    
                    span.set_attribute("job.id", job_id)
                    span.set_attribute("job.type", self.worker_type)
                    
                    logger.info(f"Received job {job_id}")
                    
                    # Update metrics
                    active_jobs.labels(worker_type=self.worker_type).inc()
                    
                    start_time = time.time()
                    
                    try:
                        # Validate job
                        if not self.validate_job(job_data):
                            raise ValueError("Invalid job data")
                        
                        # Process job
                        result = self.process_job(job_data)
                        
                        # Send result
                        result_msg = {
                            'job_id': job_id,
                            'worker_type': self.worker_type,
                            'status': 'completed',
                            'result': result,
                            'processing_time': time.time() - start_time
                        }
                        
                        self.producer.send(json.dumps(result_msg).encode('utf-8'))
                        
                        # Update metrics
                        jobs_processed.labels(
                            worker_type=self.worker_type, 
                            status='success'
                        ).inc()
                        
                        logger.info(f"Job {job_id} completed successfully")
                        
                    except Exception as e:
                        logger.error(f"Job {job_id} failed: {e}")
                        
                        # Send error result
                        error_msg = {
                            'job_id': job_id,
                            'worker_type': self.worker_type,
                            'status': 'failed',
                            'error': str(e),
                            'traceback': traceback.format_exc()
                        }
                        
                        self.producer.send(json.dumps(error_msg).encode('utf-8'))
                        
                        # Update metrics
                        jobs_processed.labels(
                            worker_type=self.worker_type,
                            status='failed'
                        ).inc()
                        
                        span.record_exception(e)
                        span.set_status(trace.Status(trace.StatusCode.ERROR))
                    
                    finally:
                        # Update metrics
                        active_jobs.labels(worker_type=self.worker_type).dec()
                        job_duration.labels(worker_type=self.worker_type).observe(
                            time.time() - start_time
                        )
                        
                        # Push metrics
                        push_to_gateway(
                            self.pushgateway_url, 
                            job=f'{self.worker_type}_worker',
                            registry=registry
                        )
                        
                        # Acknowledge message
                        self.consumer.acknowledge(msg)
            
            except pulsar.Timeout:
                # No message received, continue
                continue
            
            except Exception as e:
                logger.error(f"Worker error: {e}")
                time.sleep(5)  # Wait before retrying
    
    def cleanup(self):
        """Clean up resources."""
        try:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.close()
            if self.pulsar_client:
                self.pulsar_client.close()
            logger.info(f"{self.worker_type} worker cleaned up")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")


if __name__ == "__main__":
    # This is just the base class, actual workers will inherit from this
    pass 