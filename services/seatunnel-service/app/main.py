from platformq_shared.base_service import create_base_app
from fastapi import Depends, HTTPException, BackgroundTasks, UploadFile, File, Request, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List, Union
from enum import Enum
import logging
import os
import yaml
import json
import asyncio
import httpx
from datetime import datetime, timedelta
import subprocess
import threading
import time
from pathlib import Path
import uuid
from sqlalchemy import Column, String, DateTime, JSON, Enum as SQLEnum, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.config import ConfigLoader
from platformq_shared.security import get_current_tenant_and_user
import pulsar
from pulsar.schema import AvroSchema
from kubernetes import client, config as k8s_config
from kubernetes.client.rest import ApiException
from .pipeline_generator import PipelineGenerator, PipelineOptimizer, PipelinePattern
import re
from .monitoring import PipelineMonitor, AlertSeverity, IgniteMetricsStore
from seatunnel import SeaTunnelPipeline

# Add resource data sync pipeline
resource_pipeline = SeaTunnelPipeline(
    name="resource_data_sync",
    sources=[{"type": "cassandra", "keyspace": "resources"}],
    sinks=[{"type": "minio", "bucket": "historical_resources"}],
    transforms=[{"type": "filter", "condition": "timestamp > now() - 1 day"}]
)
resource_pipeline.schedule("daily")

graph_sync_pipeline = SeaTunnelPipeline(
    name="graph_data_sync",
    sources=[{"type": "janusgraph", "host": "janusgraph:8182"}],
    sinks=[{"type": "minio", "bucket": "graph_data"}, {"type": "ignite", "host": "ignite:10800"}],
    transforms=[{"type": "filter", "condition": "updated > now() - 1 hour"}]
)
graph_sync_pipeline.schedule("hourly")

simulation_sync = SeaTunnelPipeline(
    name="simulation_output_sync",
    sources=[{"type": "pulsar", "topic": "simulation-events"}],
    sinks=[{"type": "minio", "bucket": "simulation_outputs"}],
    transforms=[{"type": "json_parse"}]
)
simulation_sync.schedule("continuous")

opt_pipeline = SeaTunnelPipeline(
    name="optimized_pipeline",
    sources=[{}],
    sinks=[{}],
    transforms=[{}]
)

federated_workflow = SeaTunnelPipeline(
    name="federated_workflow",
    sources=[{}],
    sinks=[{}]
)

# Database models
Base = declarative_base()

class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class ConnectorType(str, Enum):
    JDBC = "jdbc"
    KAFKA = "kafka"
    PULSAR = "pulsar"
    S3 = "s3"
    MINIO = "minio"
    HTTP = "http"
    MONGODB = "mongodb"
    ELASTICSEARCH = "elasticsearch"
    CASSANDRA = "cassandra"
    IGNITE = "ignite"
    HIVE = "hive"
    MYSQL_CDC = "mysql-cdc"
    POSTGRES_CDC = "postgres-cdc"
    MONGODB_CDC = "mongodb-cdc"

class SyncMode(str, Enum):
    BATCH = "batch"
    STREAMING = "streaming"
    CDC = "cdc"

class SeaTunnelJob(Base):
    __tablename__ = "seatunnel_jobs"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    tenant_id = Column(String, nullable=False)
    name = Column(String, nullable=False)
    description = Column(String)
    config = Column(JSON, nullable=False)
    status = Column(SQLEnum(JobStatus), default=JobStatus.PENDING)
    sync_mode = Column(SQLEnum(SyncMode), nullable=False)
    schedule = Column(String)  # Cron expression for scheduled jobs
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_run = Column(DateTime)
    error_message = Column(String)
    metrics = Column(JSON)  # Store job metrics
    k8s_job_name = Column(String)
    is_active = Column(Boolean, default=True)

# Initialize database
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./seatunnel_jobs.db")
engine = create_engine(DATABASE_URL)
Base.metadata.create_all(bind=engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

logger = logging.getLogger(__name__)

# Pydantic models
class ConnectorConfig(BaseModel):
    connector_type: ConnectorType
    connection_params: Dict[str, Any]
    table_or_topic: Optional[str] = None
    query: Optional[str] = None
    options: Optional[Dict[str, Any]] = None

class TransformConfig(BaseModel):
    type: str  # sql, field_mapper, filter, etc.
    config: Dict[str, Any]

class SeaTunnelJobCreate(BaseModel):
    name: str = Field(..., description="Job name")
    description: Optional[str] = Field(None, description="Job description")
    source: ConnectorConfig = Field(..., description="Source connector configuration")
    sink: ConnectorConfig = Field(..., description="Sink connector configuration")
    transforms: Optional[List[TransformConfig]] = Field(None, description="Transformation pipeline")
    sync_mode: SyncMode = Field(..., description="Synchronization mode")
    schedule: Optional[str] = Field(None, description="Cron schedule for batch jobs")
    parallelism: Optional[int] = Field(4, description="Job parallelism")
    checkpoint_interval: Optional[int] = Field(10000, description="Checkpoint interval in ms")

class SeaTunnelJobResponse(BaseModel):
    id: str
    name: str
    description: Optional[str]
    status: JobStatus
    sync_mode: SyncMode
    schedule: Optional[str]
    created_at: datetime
    last_run: Optional[datetime]
    metrics: Optional[Dict[str, Any]]

# SeaTunnel configuration generator
class SeaTunnelConfigGenerator:
    """Generates SeaTunnel configuration files from job specifications"""
    
    @staticmethod
    def generate_config(job: SeaTunnelJobCreate, tenant_id: str) -> Dict[str, Any]:
        """Generate SeaTunnel configuration from job specification"""
        config = {
            "env": {
                "execution.parallelism": job.parallelism,
                "execution.checkpoint.interval": job.checkpoint_interval,
                "execution.checkpoint.data-uri": f"s3://platformq-checkpoints/{tenant_id}/seatunnel/"
            },
            "source": [
                SeaTunnelConfigGenerator._generate_source_config(job.source)
            ],
            "sink": [
                SeaTunnelConfigGenerator._generate_sink_config(job.sink, tenant_id)
            ]
        }
        
        if job.transforms:
            config["transform"] = [
                SeaTunnelConfigGenerator._generate_transform_config(t) 
                for t in job.transforms
            ]
        
        return config
    
    @staticmethod
    def _generate_source_config(source: ConnectorConfig) -> Dict[str, Any]:
        """Generate source connector configuration"""
        config = {
            "plugin_name": source.connector_type.value
        }
        
        # Add connection parameters
        config.update(source.connection_params)
        
        # Add specific configurations based on connector type
        if source.connector_type in [ConnectorType.JDBC, ConnectorType.MYSQL_CDC, ConnectorType.POSTGRES_CDC]:
            if source.query:
                config["query"] = source.query
            elif source.table_or_topic:
                config["table_name"] = source.table_or_topic
                
        elif source.connector_type in [ConnectorType.KAFKA, ConnectorType.PULSAR]:
            config["topic"] = source.table_or_topic
            if source.connector_type == ConnectorType.PULSAR:
                config["subscription_name"] = f"seatunnel-{source.table_or_topic}"
                
        elif source.connector_type == ConnectorType.S3:
            config["path"] = source.table_or_topic
            config["file_format"] = source.options.get("format", "parquet")
            
        if source.options:
            config.update(source.options)
            
        return config
    
    @staticmethod
    def _generate_sink_config(sink: ConnectorConfig, tenant_id: str) -> Dict[str, Any]:
        """Generate sink connector configuration"""
        config = {
            "plugin_name": sink.connector_type.value
        }
        
        # Add connection parameters
        config.update(sink.connection_params)
        
        # Add tenant isolation for sinks
        if sink.connector_type == ConnectorType.PULSAR:
            topic = sink.table_or_topic
            if not topic.startswith(f"persistent://platformq/{tenant_id}/"):
                topic = f"persistent://platformq/{tenant_id}/{topic}"
            config["topic"] = topic
            
        elif sink.connector_type in [ConnectorType.S3, ConnectorType.MINIO]:
            path = sink.table_or_topic
            if not path.startswith(f"{tenant_id}/"):
                path = f"{tenant_id}/{path}"
            config["path"] = f"s3://platformq-datalake/{path}"
            config["file_format"] = sink.options.get("format", "parquet")
            
        elif sink.connector_type == ConnectorType.CASSANDRA:
            config["keyspace"] = f"platformq_{tenant_id}"
            config["table"] = sink.table_or_topic
            
        elif sink.connector_type == ConnectorType.ELASTICSEARCH:
            config["index"] = f"{tenant_id}_{sink.table_or_topic}"
            
        if sink.options:
            config.update(sink.options)
            
        return config
    
    @staticmethod
    def _generate_transform_config(transform: TransformConfig) -> Dict[str, Any]:
        """Generate transform configuration"""
        config = {
            "plugin_name": transform.type
        }
        config.update(transform.config)
        return config

# Kubernetes job manager
class KubernetesJobManager:
    """Manages SeaTunnel jobs on Kubernetes"""
    
    def __init__(self):
        try:
            k8s_config.load_incluster_config()
        except:
            k8s_config.load_kube_config()
        
        self.batch_api = client.BatchV1Api()
        self.core_api = client.CoreV1Api()
        
    def create_job(self, job_id: str, tenant_id: str, config: Dict[str, Any], 
                   sync_mode: SyncMode) -> str:
        """Create a Kubernetes job to run SeaTunnel"""
        job_name = f"seatunnel-{job_id[:8]}-{int(time.time())}"
        
        # Create ConfigMap with SeaTunnel configuration
        config_map = client.V1ConfigMap(
            metadata=client.V1ObjectMeta(
                name=job_name,
                namespace=tenant_id,
                labels={"app": "seatunnel", "job_id": job_id}
            ),
            data={"seatunnel.conf": yaml.dump(config)}
        )
        
        try:
            self.core_api.create_namespaced_config_map(
                namespace=tenant_id,
                body=config_map
            )
        except ApiException as e:
            if e.status != 409:  # Ignore if already exists
                raise
        
        # Determine SeaTunnel image based on sync mode
        if sync_mode == SyncMode.CDC:
            image = "apache/seatunnel:2.3.3-connector-cdc"
        elif sync_mode == SyncMode.STREAMING:
            image = "apache/seatunnel:2.3.3-flink"
        else:
            image = "apache/seatunnel:2.3.3-spark"
        
        # Create Kubernetes Job
        job = client.V1Job(
            metadata=client.V1ObjectMeta(
                name=job_name,
                namespace=tenant_id,
                labels={"app": "seatunnel", "job_id": job_id}
            ),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels={"app": "seatunnel", "job_id": job_id}
                    ),
                    spec=client.V1PodSpec(
                        restart_policy="OnFailure",
                        containers=[
                            client.V1Container(
                                name="seatunnel",
                                image=image,
                                command=["/opt/seatunnel/bin/seatunnel.sh"],
                                args=["--config", "/config/seatunnel.conf"],
                                volume_mounts=[
                                    client.V1VolumeMount(
                                        name="config",
                                        mount_path="/config"
                                    )
                                ],
                                resources=client.V1ResourceRequirements(
                                    requests={
                                        "memory": "2Gi",
                                        "cpu": "1"
                                    },
                                    limits={
                                        "memory": "4Gi",
                                        "cpu": "2"
                                    }
                                )
                            )
                        ],
                        volumes=[
                            client.V1Volume(
                                name="config",
                                config_map=client.V1ConfigMapVolumeSource(
                                    name=job_name
                                )
                            )
                        ]
                    )
                )
            )
        )
        
        self.batch_api.create_namespaced_job(
            namespace=tenant_id,
            body=job
        )
        
        return job_name
    
    def get_job_status(self, job_name: str, namespace: str) -> Dict[str, Any]:
        """Get the status of a Kubernetes job"""
        try:
            job = self.batch_api.read_namespaced_job(
                name=job_name,
                namespace=namespace
            )
            
            status = {
                "active": job.status.active or 0,
                "succeeded": job.status.succeeded or 0,
                "failed": job.status.failed or 0
            }
            
            # Get pod logs if job completed or failed
            if status["succeeded"] > 0 or status["failed"] > 0:
                pods = self.core_api.list_namespaced_pod(
                    namespace=namespace,
                    label_selector=f"job-name={job_name}"
                )
                
                if pods.items:
                    pod = pods.items[0]
                    try:
                        logs = self.core_api.read_namespaced_pod_log(
                            name=pod.metadata.name,
                            namespace=namespace,
                            tail_lines=100
                        )
                        status["logs"] = logs
                    except:
                        pass
            
            return status
            
        except ApiException as e:
            if e.status == 404:
                return {"error": "Job not found"}
            raise

# Background job monitor
class JobMonitor(threading.Thread):
    """Background thread to monitor running SeaTunnel jobs"""
    
    def __init__(self, app_state):
        super().__init__(daemon=True)
        self.app_state = app_state
        self.k8s_manager = KubernetesJobManager()
        self.running = True
        self.monitors = {}  # Store active pipeline monitors
        
    def run(self):
        """Monitor running jobs and update their status"""
        while self.running:
            try:
                db = SessionLocal()
                
                # Get all running jobs
                running_jobs = db.query(SeaTunnelJob).filter(
                    SeaTunnelJob.status == JobStatus.RUNNING
                ).all()
                
                for job in running_jobs:
                    if job.k8s_job_name:
                        status = self.k8s_manager.get_job_status(
                            job.k8s_job_name,
                            job.tenant_id
                        )
                        
                        # Initialize monitoring if not exists
                        if job.id not in self.monitors:
                            self._init_job_monitoring(job)
                        
                        # Update metrics
                        if job.id in self.monitors:
                            self._update_job_metrics(job, status)
                        
                        # Update job status based on K8s job status
                        if status.get("succeeded", 0) > 0:
                            job.status = JobStatus.COMPLETED
                            job.metrics = self._extract_metrics_from_logs(
                                status.get("logs", "")
                            )
                            
                            # Stop monitoring
                            if job.id in self.monitors:
                                self.monitors[job.id].stop_monitoring()
                                del self.monitors[job.id]
                            
                            # Publish completion event
                            self._publish_job_completed(job)
                            
                        elif status.get("failed", 0) > 0:
                            job.status = JobStatus.FAILED
                            job.error_message = self._extract_error_from_logs(
                                status.get("logs", "")
                            )
                            
                            # Record error in monitoring
                            if job.id in self.monitors:
                                self.monitors[job.id].record_metric(
                                    "errors", 1, 
                                    {"error_type": "job_failed"}
                                )
                                self.monitors[job.id].stop_monitoring()
                                del self.monitors[job.id]
                            
                        job.updated_at = datetime.utcnow()
                        db.commit()
                
                # Cleanup monitors for non-running jobs
                self._cleanup_monitors(running_jobs)
                
                db.close()
                
            except Exception as e:
                logger.error(f"Error in job monitor: {e}")
            
            time.sleep(30)  # Check every 30 seconds
    
    def _init_job_monitoring(self, job: SeaTunnelJob):
        """Initialize monitoring for a job"""
        try:
            # Determine pipeline pattern from job config
            pattern = job.config.get("metadata", {}).get("pattern", "etl")
            
            # Setup alert configuration
            alert_config = {
                "webhook_url": os.getenv("ALERT_WEBHOOK_URL"),
                "rules": job.config.get("monitoring", {}).get("alert_rules", [
                    {
                        "metric": "error_rate",
                        "threshold": 0.05,
                        "window": "5m",
                        "severity": "warning"
                    },
                    {
                        "metric": "processing_lag",
                        "threshold": 300,  # 5 minutes
                        "window": "10m",
                        "severity": "critical"
                    }
                ])
            }
            
            # Create monitor
            monitor = PipelineMonitor(
                job_id=job.id,
                pattern=pattern,
                alert_config=alert_config
            )
            
            # Start monitoring
            asyncio.create_task(monitor.start_monitoring(interval=60))
            
            self.monitors[job.id] = monitor
            logger.info(f"Initialized monitoring for job {job.id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize monitoring for job {job.id}: {e}")
    
    def _update_job_metrics(self, job: SeaTunnelJob, status: Dict[str, Any]):
        """Update job metrics from status"""
        monitor = self.monitors.get(job.id)
        if not monitor:
            return
        
        try:
            logs = status.get("logs", "")
            
            # Extract metrics from logs (patterns depend on SeaTunnel output)
            import re
            
            # Rows processed
            rows_match = re.search(r"rows processed: (\d+)", logs, re.IGNORECASE)
            if rows_match:
                rows = int(rows_match.group(1))
                monitor.record_metric("rows_processed", rows)
            
            # Processing time
            time_match = re.search(r"processing time: ([\d.]+)s", logs, re.IGNORECASE)
            if time_match:
                proc_time = float(time_match.group(1))
                monitor.record_metric("processing_time", proc_time)
            
            # Throughput calculation
            if rows_match and time_match and float(time_match.group(1)) > 0:
                throughput = int(rows_match.group(1)) / float(time_match.group(1))
                monitor.record_metric("throughput", throughput)
            
            # Error count
            error_matches = re.findall(r"(error|exception|failed)", logs, re.IGNORECASE)
            if error_matches:
                monitor.record_metric("errors", len(error_matches), {"error_type": "runtime"})
            
        except Exception as e:
            logger.error(f"Failed to update metrics for job {job.id}: {e}")
    
    def _cleanup_monitors(self, running_jobs: List[SeaTunnelJob]):
        """Cleanup monitors for jobs that are no longer running"""
        running_job_ids = {job.id for job in running_jobs}
        
        monitors_to_remove = []
        for job_id, monitor in self.monitors.items():
            if job_id not in running_job_ids:
                monitor.stop_monitoring()
                monitors_to_remove.append(job_id)
        
        for job_id in monitors_to_remove:
            del self.monitors[job_id]
            logger.info(f"Cleaned up monitor for job {job_id}")
    
    def _extract_metrics_from_logs(self, logs: str) -> Dict[str, Any]:
        """Extract job metrics from SeaTunnel logs"""
        metrics = {
            "rows_read": 0,
            "rows_written": 0,
            "duration_ms": 0,
            "throughput_rows_per_sec": 0
        }
        
        # Parse SeaTunnel logs for metrics
        for line in logs.split("\n"):
            if "Total Read Count" in line:
                try:
                    metrics["rows_read"] = int(line.split(":")[-1].strip())
                except:
                    pass
            elif "Total Write Count" in line:
                try:
                    metrics["rows_written"] = int(line.split(":")[-1].strip())
                except:
                    pass
                    
        return metrics
    
    def _extract_error_from_logs(self, logs: str) -> str:
        """Extract error message from SeaTunnel logs"""
        error_lines = []
        in_error = False
        
        for line in logs.split("\n"):
            if "ERROR" in line or "Exception" in line:
                in_error = True
            if in_error:
                error_lines.append(line)
                if len(error_lines) > 10:  # Limit error message length
                    break
                    
        return "\n".join(error_lines) or "Job failed without clear error message"
    
    def _publish_job_completed(self, job: SeaTunnelJob):
        """Publish job completion event"""
        try:
            event_data = {
                "job_id": job.id,
                "job_name": job.name,
                "tenant_id": job.tenant_id,
                "status": job.status.value,
                "metrics": job.metrics,
                "completed_at": datetime.utcnow().isoformat()
            }
            
            publisher = self.app_state.event_publisher
            publisher.publish(
                topic_base="seatunnel-job-completed",
                tenant_id=job.tenant_id,
                schema_class=None,  # Would define proper schema
                data=event_data
            )
        except Exception as e:
            logger.error(f"Failed to publish job completion event: {e}")

# Create the FastAPI app
app = create_base_app(
    service_name="seatunnel-service",
    db_session_dependency=get_db,
    api_key_crud_dependency=lambda: None,  # Placeholder
    user_crud_dependency=lambda: None,  # Placeholder
    password_verifier_dependency=lambda: None  # Placeholder
)

# API Endpoints
@app.post("/api/v1/jobs", response_model=SeaTunnelJobResponse)
async def create_job(
    job_spec: SeaTunnelJobCreate,
    background_tasks: BackgroundTasks,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db)
):
    """Create a new SeaTunnel job"""
    tenant_id = context["tenant_id"]
    
    # Generate SeaTunnel configuration
    config_generator = SeaTunnelConfigGenerator()
    seatunnel_config = config_generator.generate_config(job_spec, tenant_id)
    
    # Create job record
    job = SeaTunnelJob(
        tenant_id=tenant_id,
        name=job_spec.name,
        description=job_spec.description,
        config=seatunnel_config,
        sync_mode=job_spec.sync_mode,
        schedule=job_spec.schedule,
        status=JobStatus.PENDING
    )
    
    db.add(job)
    db.commit()
    db.refresh(job)
    
    # Launch job in background
    background_tasks.add_task(launch_job, job.id, tenant_id, seatunnel_config, job_spec.sync_mode)
    
    return SeaTunnelJobResponse(
        id=job.id,
        name=job.name,
        description=job.description,
        status=job.status,
        sync_mode=job.sync_mode,
        schedule=job.schedule,
        created_at=job.created_at,
        last_run=job.last_run,
        metrics=job.metrics
    )

@app.get("/api/v1/jobs", response_model=List[SeaTunnelJobResponse])
async def list_jobs(
    skip: int = 0,
    limit: int = 100,
    status: Optional[JobStatus] = None,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db)
):
    """List SeaTunnel jobs for the current tenant"""
    tenant_id = context["tenant_id"]
    
    query = db.query(SeaTunnelJob).filter(
        SeaTunnelJob.tenant_id == tenant_id,
        SeaTunnelJob.is_active == True
    )
    
    if status:
        query = query.filter(SeaTunnelJob.status == status)
    
    jobs = query.offset(skip).limit(limit).all()
    
    return [
        SeaTunnelJobResponse(
            id=job.id,
            name=job.name,
            description=job.description,
            status=job.status,
            sync_mode=job.sync_mode,
            schedule=job.schedule,
            created_at=job.created_at,
            last_run=job.last_run,
            metrics=job.metrics
        )
        for job in jobs
    ]

@app.get("/api/v1/jobs/{job_id}", response_model=SeaTunnelJobResponse)
async def get_job(
    job_id: str,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db)
):
    """Get details of a specific SeaTunnel job"""
    tenant_id = context["tenant_id"]
    
    job = db.query(SeaTunnelJob).filter(
        SeaTunnelJob.id == job_id,
        SeaTunnelJob.tenant_id == tenant_id
    ).first()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return SeaTunnelJobResponse(
        id=job.id,
        name=job.name,
        description=job.description,
        status=job.status,
        sync_mode=job.sync_mode,
        schedule=job.schedule,
        created_at=job.created_at,
        last_run=job.last_run,
        metrics=job.metrics
    )

@app.delete("/api/v1/jobs/{job_id}")
async def delete_job(
    job_id: str,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db)
):
    """Delete a SeaTunnel job"""
    tenant_id = context["tenant_id"]
    
    job = db.query(SeaTunnelJob).filter(
        SeaTunnelJob.id == job_id,
        SeaTunnelJob.tenant_id == tenant_id
    ).first()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Cancel if running
    if job.status == JobStatus.RUNNING and job.k8s_job_name:
        k8s_manager = KubernetesJobManager()
        try:
            k8s_manager.batch_api.delete_namespaced_job(
                name=job.k8s_job_name,
                namespace=tenant_id,
                propagation_policy="Background"
            )
        except:
            pass
    
    # Soft delete
    job.is_active = False
    job.status = JobStatus.CANCELLED
    db.commit()
    
    return {"message": "Job deleted successfully"}

@app.post("/api/v1/jobs/{job_id}/run")
async def run_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db)
):
    """Manually trigger a SeaTunnel job"""
    tenant_id = context["tenant_id"]
    
    job = db.query(SeaTunnelJob).filter(
        SeaTunnelJob.id == job_id,
        SeaTunnelJob.tenant_id == tenant_id
    ).first()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.status == JobStatus.RUNNING:
        raise HTTPException(status_code=400, detail="Job is already running")
    
    # Launch job in background
    background_tasks.add_task(
        launch_job, 
        job.id, 
        tenant_id, 
        job.config, 
        job.sync_mode
    )
    
    return {"message": "Job triggered successfully", "job_id": job.id}

# Helper function to launch jobs
async def launch_job(job_id: str, tenant_id: str, config: Dict[str, Any], 
                    sync_mode: SyncMode):
    """Launch a SeaTunnel job on Kubernetes"""
    db = SessionLocal()
    
    try:
        job = db.query(SeaTunnelJob).filter(SeaTunnelJob.id == job_id).first()
        if not job:
            return
        
        # Update job status
        job.status = JobStatus.RUNNING
        job.last_run = datetime.utcnow()
        db.commit()
        
        # Launch Kubernetes job
        k8s_manager = KubernetesJobManager()
        k8s_job_name = k8s_manager.create_job(job_id, tenant_id, config, sync_mode)
        
        # Update job with K8s job name
        job.k8s_job_name = k8s_job_name
        db.commit()
        
    except Exception as e:
        logger.error(f"Failed to launch job {job_id}: {e}")
        job = db.query(SeaTunnelJob).filter(SeaTunnelJob.id == job_id).first()
        if job:
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            db.commit()
    finally:
        db.close()

# Template endpoints for common integration patterns
@app.get("/api/v1/templates")
async def get_job_templates():
    """Get pre-configured job templates"""
    templates = {
        "mysql_to_datalake": {
            "name": "MySQL to Data Lake",
            "description": "Sync MySQL tables to data lake",
            "source": {
                "connector_type": "jdbc",
                "connection_params": {
                    "url": "jdbc:mysql://hostname:3306/database",
                    "driver": "com.mysql.cj.jdbc.Driver",
                    "user": "username",
                    "password": "password"
                },
                "query": "SELECT * FROM table_name"
            },
            "sink": {
                "connector_type": "minio",
                "connection_params": {
                    "endpoint": "http://minio:9000",
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin"
                },
                "table_or_topic": "raw/mysql/table_name",
                "options": {
                    "format": "parquet",
                    "partition_by": ["date"]
                }
            },
            "sync_mode": "batch"
        },
        "postgres_cdc_to_pulsar": {
            "name": "PostgreSQL CDC to Pulsar",
            "description": "Stream PostgreSQL changes to Pulsar",
            "source": {
                "connector_type": "postgres-cdc",
                "connection_params": {
                    "hostname": "postgres",
                    "port": 5432,
                    "database": "database",
                    "username": "username",
                    "password": "password",
                    "schema": "public",
                    "table.whitelist": "table1,table2"
                }
            },
            "sink": {
                "connector_type": "pulsar",
                "connection_params": {
                    "service-url": "pulsar://pulsar:6650",
                    "admin-url": "http://pulsar:8080"
                },
                "table_or_topic": "postgres-cdc-events"
            },
            "sync_mode": "cdc"
        },
        "pulsar_to_elasticsearch": {
            "name": "Pulsar to Elasticsearch",
            "description": "Index Pulsar events in Elasticsearch",
            "source": {
                "connector_type": "pulsar",
                "connection_params": {
                    "service-url": "pulsar://pulsar:6650",
                    "admin-url": "http://pulsar:8080",
                    "subscription-name": "es-indexer"
                },
                "table_or_topic": "events-to-index"
            },
            "sink": {
                "connector_type": "elasticsearch",
                "connection_params": {
                    "hosts": ["http://elasticsearch:9200"],
                    "index": "events"
                }
            },
            "sync_mode": "streaming"
        }
    }
    
    return templates

# Pipeline auto-generation endpoints
@app.post("/api/v1/pipelines/generate")
async def generate_pipeline_from_metadata(
    request: Request,
    asset_metadata: Dict[str, Any],
    auto_deploy: bool = False,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db)
):
    """
    Generate a SeaTunnel pipeline configuration from asset metadata
    
    This endpoint analyzes asset metadata and automatically generates
    an optimized pipeline configuration based on asset type, data
    characteristics, and best practices.
    """
    tenant_id = context["tenant_id"]
    
    try:
        # Initialize pipeline generator
        generator = PipelineGenerator()
        optimizer = PipelineOptimizer()
        
        # Generate pipeline configuration
        pipeline_config, pattern = generator.generate_pipeline(
            asset_metadata=asset_metadata,
            tenant_id=tenant_id
        )
        
        # Optimize the pipeline
        optimized_config = optimizer.optimize_pipeline(pipeline_config)
        
        # Convert to SeaTunnel format
        # For multi-sink support, we'll use the first sink as primary
        primary_sink = optimized_config["sinks"][0] if optimized_config["sinks"] else None
        
        seatunnel_job = SeaTunnelJobCreate(
            name=optimized_config["name"],
            description=optimized_config["description"],
            source=ConnectorConfig(
                connector_type=optimized_config["source"]["connector_type"],
                connection_params=optimized_config["source"]["connection_params"],
                table_or_topic=optimized_config["source"].get("table_or_topic", ""),
                options=optimized_config["source"].get("options", {})
            ),
            sink=ConnectorConfig(
                connector_type=primary_sink["connector_type"],
                connection_params=primary_sink["connection_params"],
                table_or_topic=primary_sink.get("table_or_topic", ""),
                options=primary_sink.get("options", {})
            ) if primary_sink else None,
            transforms=[
                TransformConfig(type=t["type"], config=t["config"])
                for t in optimized_config.get("transforms", [])
            ],
            sync_mode=SyncMode(optimized_config["sync_mode"]),
            schedule=optimized_config.get("schedule"),
            parallelism=optimized_config.get("parallelism", 4),
            checkpoint_interval=optimized_config.get("checkpoint_interval", 10000)
        )
        
        response = {
            "pipeline_config": optimized_config,
            "pattern": pattern.value,
            "estimated_cost": _estimate_pipeline_cost(optimized_config),
            "recommendations": _get_pipeline_recommendations(pattern, optimized_config)
        }
        
        # Auto-deploy if requested
        if auto_deploy:
            # Create and deploy the job
            job_config = SeaTunnelConfigGenerator.generate_config(seatunnel_job, tenant_id)
            
            job = SeaTunnelJob(
                tenant_id=tenant_id,
                name=seatunnel_job.name,
                description=seatunnel_job.description,
                config=job_config,
                sync_mode=seatunnel_job.sync_mode,
                schedule=seatunnel_job.schedule,
                status=JobStatus.PENDING
            )
            
            db.add(job)
            db.commit()
            db.refresh(job)
            
            # Launch job
            await launch_job(job.id, tenant_id, job_config, seatunnel_job.sync_mode)
            
            response["job_id"] = job.id
            response["status"] = "deployed"
        
        return response
        
    except Exception as e:
        logger.error(f"Failed to generate pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/pipelines/analyze")
async def analyze_data_source(
    source_uri: str,
    source_type: str = Query(..., regex="^(file|database|streaming)$"),
    sample_size: int = Query(1000, ge=100, le=10000),
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Analyze a data source and provide pipeline recommendations
    
    This endpoint samples data from the source and provides
    recommendations for pipeline configuration.
    """
    try:
        analysis = await _analyze_source(source_uri, source_type, sample_size)
        
        # Generate recommendations based on analysis
        recommendations = {
            "suggested_transformations": _suggest_transformations(analysis),
            "optimal_sinks": _suggest_sinks(analysis),
            "performance_settings": _suggest_performance_settings(analysis),
            "monitoring_metrics": _suggest_metrics(analysis)
        }
        
        return {
            "source_analysis": analysis,
            "recommendations": recommendations
        }
        
    except Exception as e:
        logger.error(f"Failed to analyze source: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/pipelines/patterns")
async def get_pipeline_patterns():
    """Get available pipeline patterns with descriptions and use cases"""
    patterns = {
        pattern.value: {
            "name": pattern.name,
            "description": _get_pattern_description(pattern),
            "use_cases": _get_pattern_use_cases(pattern),
            "typical_sources": _get_pattern_sources(pattern),
            "typical_sinks": _get_pattern_sinks(pattern)
        }
        for pattern in PipelinePattern
    }
    
    return {"patterns": patterns}

@app.post("/api/v1/pipelines/batch/generate")
async def batch_generate_pipelines(
    assets: List[Dict[str, Any]],
    pipeline_template: Optional[Dict[str, Any]] = None,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Generate multiple pipelines in batch for a list of assets
    
    Useful for bulk processing of similar assets with consistent
    pipeline configurations.
    """
    tenant_id = context["tenant_id"]
    generator = PipelineGenerator()
    optimizer = PipelineOptimizer()
    
    results = []
    
    for asset in assets:
        try:
            # Apply template overrides if provided
            if pipeline_template:
                asset_metadata = {**asset, **pipeline_template}
            else:
                asset_metadata = asset
            
            # Generate and optimize pipeline
            pipeline_config, pattern = generator.generate_pipeline(
                asset_metadata=asset_metadata,
                tenant_id=tenant_id
            )
            optimized = optimizer.optimize_pipeline(pipeline_config)
            
            results.append({
                "asset_id": asset.get("asset_id"),
                "pipeline_name": optimized["name"],
                "pattern": pattern.value,
                "status": "generated"
            })
            
        except Exception as e:
            results.append({
                "asset_id": asset.get("asset_id"),
                "status": "failed",
                "error": str(e)
            })
    
    return {
        "total": len(assets),
        "successful": len([r for r in results if r["status"] == "generated"]),
        "failed": len([r for r in results if r["status"] == "failed"]),
        "results": results
    }

# Helper functions for pipeline generation
def _estimate_pipeline_cost(config: Dict[str, Any]) -> Dict[str, Any]:
    """Estimate resource costs for pipeline execution"""
    base_cost = 0.1  # Base cost per hour
    
    # Factor in parallelism
    parallelism = config.get("parallelism", 4)
    cost_multiplier = 1 + (parallelism - 1) * 0.3
    
    # Factor in sync mode
    if config.get("sync_mode") == "streaming":
        cost_multiplier *= 2.5  # Streaming is more expensive
    
    # Factor in number of sinks
    sink_count = len(config.get("sinks", []))
    cost_multiplier *= (1 + (sink_count - 1) * 0.2)
    
    hourly_cost = base_cost * cost_multiplier
    
    return {
        "estimated_hourly_cost": round(hourly_cost, 2),
        "estimated_monthly_cost": round(hourly_cost * 24 * 30, 2),
        "cost_factors": {
            "parallelism": parallelism,
            "sync_mode": config.get("sync_mode"),
            "sink_count": sink_count
        }
    }

def _get_pipeline_recommendations(pattern: PipelinePattern, config: Dict[str, Any]) -> List[str]:
    """Get recommendations for pipeline optimization"""
    recommendations = []
    
    # Pattern-specific recommendations
    if pattern == PipelinePattern.STREAMING:
        recommendations.append("Consider implementing backpressure handling for streaming sources")
        recommendations.append("Enable checkpointing with appropriate intervals for fault tolerance")
    
    elif pattern == PipelinePattern.CDC:
        recommendations.append("Ensure CDC slots are properly managed to avoid storage issues")
        recommendations.append("Consider implementing snapshot isolation for initial loads")
    
    elif pattern == PipelinePattern.AGGREGATION:
        recommendations.append("Use incremental aggregation for better performance")
        recommendations.append("Consider pre-aggregation at ingestion time if possible")
    
    # General recommendations
    if config.get("parallelism", 0) > 8:
        recommendations.append("High parallelism detected - ensure cluster has sufficient resources")
    
    transforms = config.get("transforms", [])
    if len(transforms) > 5:
        recommendations.append("Complex transformation chain - consider breaking into multiple pipelines")
    
    return recommendations

async def _analyze_source(source_uri: str, source_type: str, sample_size: int) -> Dict[str, Any]:
    """Analyze data source characteristics"""
    # This is a simplified implementation
    # In production, this would actually sample and analyze the data
    
    analysis = {
        "source_uri": source_uri,
        "source_type": source_type,
        "sample_size": sample_size,
        "detected_schema": {},
        "data_characteristics": {
            "estimated_volume": "medium",
            "update_frequency": "daily",
            "data_quality_score": 0.85
        }
    }
    
    if source_type == "file":
        # Analyze file-based sources
        if source_uri.endswith(".csv"):
            analysis["detected_format"] = "csv"
            analysis["detected_schema"] = {
                "fields": ["id", "name", "value", "timestamp"],
                "types": ["integer", "string", "float", "timestamp"]
            }
        elif source_uri.endswith(".json"):
            analysis["detected_format"] = "json"
    
    elif source_type == "database":
        # Analyze database sources
        analysis["detected_format"] = "relational"
        analysis["table_count"] = 5
        analysis["relationship_count"] = 3
    
    return analysis

def _suggest_transformations(analysis: Dict[str, Any]) -> List[str]:
    """Suggest transformations based on data analysis"""
    suggestions = []
    
    if analysis.get("data_characteristics", {}).get("data_quality_score", 1.0) < 0.9:
        suggestions.append("data_quality_validation")
        suggestions.append("data_cleansing")
    
    if "timestamp" in str(analysis.get("detected_schema", {})):
        suggestions.append("time_based_partitioning")
        suggestions.append("watermarking")
    
    return suggestions

def _suggest_sinks(analysis: Dict[str, Any]) -> List[str]:
    """Suggest optimal sinks based on data characteristics"""
    sinks = []
    
    volume = analysis.get("data_characteristics", {}).get("estimated_volume", "medium")
    
    if volume in ["large", "xlarge"]:
        sinks.append("minio")  # Object storage for large volumes
    
    if "timestamp" in str(analysis.get("detected_schema", {})):
        sinks.append("ignite")  # For time-series caching
    
    if analysis.get("source_type") == "streaming":
        sinks.append("pulsar")  # Stream to stream
    
    # Always include data lake
    if "minio" not in sinks:
        sinks.append("minio")
    
    return sinks

def _suggest_performance_settings(analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Suggest performance settings based on data characteristics"""
    volume = analysis.get("data_characteristics", {}).get("estimated_volume", "medium")
    
    volume_settings = {
        "small": {"parallelism": 2, "memory": "1Gi"},
        "medium": {"parallelism": 4, "memory": "2Gi"},
        "large": {"parallelism": 8, "memory": "4Gi"},
        "xlarge": {"parallelism": 16, "memory": "8Gi"}
    }
    
    return volume_settings.get(volume, volume_settings["medium"])

def _suggest_metrics(analysis: Dict[str, Any]) -> List[str]:
    """Suggest monitoring metrics based on data characteristics"""
    metrics = ["rows_processed", "processing_time", "error_rate"]
    
    if analysis.get("source_type") == "streaming":
        metrics.extend(["lag", "throughput", "backpressure"])
    
    if analysis.get("data_characteristics", {}).get("data_quality_score", 1.0) < 0.9:
        metrics.extend(["validation_failures", "data_quality_score"])
    
    return metrics

def _get_pattern_description(pattern: PipelinePattern) -> str:
    """Get description for pipeline pattern"""
    descriptions = {
        PipelinePattern.INGESTION: "Basic data ingestion from source to data lake",
        PipelinePattern.ETL: "Extract, transform, and load data between systems",
        PipelinePattern.STREAMING: "Real-time stream processing pipeline",
        PipelinePattern.CDC: "Change data capture for database replication",
        PipelinePattern.ENRICHMENT: "Enrich data with additional context or reference data",
        PipelinePattern.AGGREGATION: "Aggregate and summarize data for analytics",
        PipelinePattern.REPLICATION: "Replicate data to multiple target systems",
        PipelinePattern.FEDERATION: "Federate data from multiple sources into unified view"
    }
    return descriptions.get(pattern, "")

def _get_pattern_use_cases(pattern: PipelinePattern) -> List[str]:
    """Get use cases for pipeline pattern"""
    use_cases = {
        PipelinePattern.INGESTION: [
            "Initial data lake population",
            "Batch file uploads",
            "API data extraction"
        ],
        PipelinePattern.STREAMING: [
            "Real-time IoT data processing",
            "Live event stream analytics",
            "Real-time dashboards"
        ],
        PipelinePattern.CDC: [
            "Database synchronization",
            "Real-time data warehouse updates",
            "Audit trail capture"
        ]
    }
    return use_cases.get(pattern, [])

def _get_pattern_sources(pattern: PipelinePattern) -> List[str]:
    """Get typical sources for pipeline pattern"""
    sources = {
        PipelinePattern.INGESTION: ["S3/MinIO", "FTP", "REST APIs"],
        PipelinePattern.STREAMING: ["Pulsar", "Kafka", "IoT devices"],
        PipelinePattern.CDC: ["PostgreSQL", "MySQL", "MongoDB"]
    }
    return sources.get(pattern, [])

def _get_pattern_sinks(pattern: PipelinePattern) -> List[str]:
    """Get typical sinks for pipeline pattern"""
    sinks = {
        PipelinePattern.INGESTION: ["Data Lake (MinIO)", "Cassandra"],
        PipelinePattern.STREAMING: ["Ignite", "Elasticsearch", "Pulsar"],
        PipelinePattern.CDC: ["Data Warehouse", "Elasticsearch", "Data Lake"]
    }
    return sinks.get(pattern, [])

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize the service"""
    # Start job monitor
    monitor = JobMonitor(app.state)
    monitor.start()
    app.state.job_monitor = monitor
    
    # Start asset event consumer for auto-pipeline generation
    consumer_thread = threading.Thread(
        target=asset_event_consumer,
        args=(app.state,),
        daemon=True
    )
    consumer_thread.start()
    app.state.asset_consumer_thread = consumer_thread
    
    logger.info("SeaTunnel service started")

# Asset event consumer for auto-pipeline generation
def asset_event_consumer(app_state):
    """Consume DigitalAssetCreated events and auto-generate pipelines"""
    logger.info("Starting asset event consumer for pipeline auto-generation...")
    
    config_loader = ConfigLoader()
    settings = config_loader.load_settings()
    pulsar_url = settings.get("PULSAR_URL", "pulsar://pulsar:6650")
    
    client = pulsar.Client(pulsar_url)
    
    # Import the event schema
    from platformq_shared.events import DigitalAssetCreated
    
    try:
        consumer = client.subscribe(
            topic_pattern="persistent://platformq/.*/digital-asset-created-events",
            subscription_name="seatunnel-pipeline-generator",
            schema=AvroSchema(DigitalAssetCreated),
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        generator = PipelineGenerator()
        optimizer = PipelineOptimizer()
        db = SessionLocal()
        
        while True:
            try:
                msg = consumer.receive(timeout_millis=1000)
                if msg is None:
                    continue
                
                event = msg.value()
                logger.info(f"Received DigitalAssetCreated event for asset: {event.asset_id}")
                
                # Extract tenant ID from topic
                topic = msg.topic_name()
                tenant_match = re.search(r'platformq/([a-f0-9-]+)/', topic)
                tenant_id = tenant_match.group(1) if tenant_match else 'default'
                
                # Check if auto-generation is enabled for this asset type
                if should_auto_generate_pipeline(event.asset_type):
                    try:
                        # Prepare asset metadata
                        asset_metadata = {
                            "asset_id": event.asset_id,
                            "asset_type": event.asset_type,
                            "asset_name": event.asset_name,
                            "raw_data_uri": event.raw_data_uri,
                            "owner_id": event.owner_id,
                            "created_at": event.created_at
                        }
                        
                        # Generate pipeline
                        pipeline_config, pattern = generator.generate_pipeline(
                            asset_metadata=asset_metadata,
                            tenant_id=tenant_id
                        )
                        
                        # Optimize pipeline
                        optimized_config = optimizer.optimize_pipeline(pipeline_config)
                        
                        # Create SeaTunnel job from generated config
                        job_name = f"auto_{event.asset_type.lower()}_{event.asset_id[:8]}"
                        primary_sink = optimized_config["sinks"][0] if optimized_config["sinks"] else None
                        
                        if primary_sink:
                            # Generate SeaTunnel configuration
                            seatunnel_config = {
                                "env": {
                                    "execution.parallelism": optimized_config.get("parallelism", 4),
                                    "execution.checkpoint.interval": optimized_config.get("checkpoint_interval", 60000)
                                },
                                "source": [optimized_config["source"]],
                                "transform": optimized_config.get("transforms", []),
                                "sink": optimized_config["sinks"]
                            }
                            
                            # Create job record
                            job = SeaTunnelJob(
                                tenant_id=tenant_id,
                                name=job_name,
                                description=f"Auto-generated {pattern.value} pipeline for {event.asset_type}",
                                config=seatunnel_config,
                                sync_mode=SyncMode(optimized_config["sync_mode"]),
                                schedule=optimized_config.get("schedule"),
                                status=JobStatus.PENDING
                            )
                            
                            db.add(job)
                            db.commit()
                            
                            # Launch job asynchronously
                            asyncio.create_task(
                                launch_job(
                                    job.id,
                                    tenant_id,
                                    seatunnel_config,
                                    SyncMode(optimized_config["sync_mode"])
                                )
                            )
                            
                            logger.info(f"Auto-generated and launched pipeline {job_name} for asset {event.asset_id}")
                            
                            # Publish pipeline creation event
                            publish_pipeline_created_event(
                                tenant_id=tenant_id,
                                asset_id=event.asset_id,
                                pipeline_id=job.id,
                                pattern=pattern.value
                            )
                        
                    except Exception as e:
                        logger.error(f"Failed to auto-generate pipeline for asset {event.asset_id}: {e}")
                
                # Acknowledge message
                consumer.acknowledge(msg)
                
            except Exception as e:
                if "Timeout" not in str(e):
                    logger.error(f"Error processing asset event: {e}")
                    
    except Exception as e:
        logger.error(f"Failed to start asset event consumer: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
        if 'client' in locals():
            client.close()
        if 'db' in locals():
            db.close()

def should_auto_generate_pipeline(asset_type: str) -> bool:
    """Determine if pipeline should be auto-generated for asset type"""
    # Auto-generate for known structured data types
    auto_generate_types = [
        "CSV", "EXCEL", "PARQUET", "AVRO", "JSON",
        "CRM_CONTACT", "CRM_ACCOUNT", "CRM_OPPORTUNITY",
        "IOT_SENSOR", "TELEMETRY", "LOG", "EVENT",
        "DATABASE_EXPORT", "SQL_DUMP"
    ]
    
    return any(asset_type.upper().startswith(t) for t in auto_generate_types)

def publish_pipeline_created_event(tenant_id: str, asset_id: str, pipeline_id: str, pattern: str):
    """Publish event when pipeline is created"""
    try:
        # Define event schema if not exists
        from pulsar.schema import Record, String, Long
        
        class PipelineCreatedEvent(Record):
            tenant_id = String()
            asset_id = String()
            pipeline_id = String()
            pattern = String()
            created_at = Long()
        
        config_loader = ConfigLoader()
        publisher = EventPublisher(
            pulsar_url=config_loader.load_settings().get("PULSAR_URL", "pulsar://pulsar:6650")
        )
        publisher.connect()
        
        event = PipelineCreatedEvent(
            tenant_id=tenant_id,
            asset_id=asset_id,
            pipeline_id=pipeline_id,
            pattern=pattern,
            created_at=int(datetime.utcnow().timestamp() * 1000)
        )
        
        publisher.publish(
            topic_base='seatunnel-pipeline-created-events',
            tenant_id=tenant_id,
            schema_class=PipelineCreatedEvent,
            data=event
        )
        
        publisher.close()
        
    except Exception as e:
        logger.error(f"Failed to publish pipeline created event: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    if hasattr(app.state, "job_monitor"):
        app.state.job_monitor.running = False
        app.state.job_monitor.join(timeout=5)

# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "seatunnel-service"}

# Monitoring endpoints
@app.get("/api/v1/jobs/{job_id}/metrics")
async def get_job_metrics(
    job_id: str,
    time_range_hours: int = Query(1, ge=1, le=24),
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db)
):
    """Get metrics for a specific job"""
    tenant_id = context["tenant_id"]
    
    # Verify job exists and belongs to tenant
    job = db.query(SeaTunnelJob).filter(
        SeaTunnelJob.id == job_id,
        SeaTunnelJob.tenant_id == tenant_id
    ).first()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Get monitor from job monitor thread
    monitor = None
    if hasattr(app.state, 'job_monitor') and hasattr(app.state.job_monitor, 'monitors'):
        monitor = app.state.job_monitor.monitors.get(job_id)
    
    if monitor:
        # Get dashboard data from active monitor
        dashboard_data = monitor.get_dashboard_data(
            time_range=timedelta(hours=time_range_hours)
        )
        return dashboard_data
    else:
        # Job not currently running, get historical data
        pattern = job.config.get("metadata", {}).get("pattern", "etl")
        
        # Create temporary monitor to retrieve historical data
        metrics_store = IgniteMetricsStore()
        
        cache_name = f"pipeline_metrics_{job_id}"
        metrics = metrics_store.get_metrics(cache_name)
        
        metrics_store.close()
        
        return {
            "job_id": job_id,
            "pattern": pattern,
            "status": job.status.value,
            "historical_metrics": metrics
        }

@app.get("/api/v1/monitoring/dashboard")
async def get_monitoring_dashboard(
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db)
):
    """Get overall monitoring dashboard for tenant"""
    tenant_id = context["tenant_id"]
    
    # Get all jobs for tenant
    jobs = db.query(SeaTunnelJob).filter(
        SeaTunnelJob.tenant_id == tenant_id,
        SeaTunnelJob.is_active == True
    ).all()
    
    dashboard = {
        "tenant_id": tenant_id,
        "timestamp": datetime.utcnow().isoformat(),
        "summary": {
            "total_jobs": len(jobs),
            "running_jobs": len([j for j in jobs if j.status == JobStatus.RUNNING]),
            "completed_jobs": len([j for j in jobs if j.status == JobStatus.COMPLETED]),
            "failed_jobs": len([j for j in jobs if j.status == JobStatus.FAILED])
        },
        "jobs": []
    }
    
    # Get monitors from job monitor thread
    monitors = {}
    if hasattr(app.state, 'job_monitor') and hasattr(app.state.job_monitor, 'monitors'):
        monitors = app.state.job_monitor.monitors
    
    for job in jobs:
        job_info = {
            "job_id": job.id,
            "name": job.name,
            "status": job.status.value,
            "sync_mode": job.sync_mode.value,
            "last_run": job.last_run.isoformat() if job.last_run else None,
            "metrics": {}
        }
        
        # Get current metrics if job is monitored
        if job.id in monitors:
            monitor = monitors[job.id]
            try:
                current_metrics = monitor.get_dashboard_data(timedelta(minutes=5))
                job_info["metrics"] = current_metrics.get("metrics", {})
            except Exception as e:
                logger.error(f"Failed to get metrics for job {job.id}: {e}")
        
        dashboard["jobs"].append(job_info)
    
    return dashboard

@app.post("/api/v1/jobs/{job_id}/alerts")
async def configure_job_alerts(
    job_id: str,
    alert_rules: List[Dict[str, Any]],
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db)
):
    """Configure alert rules for a job"""
    tenant_id = context["tenant_id"]
    
    # Verify job exists and belongs to tenant
    job = db.query(SeaTunnelJob).filter(
        SeaTunnelJob.id == job_id,
        SeaTunnelJob.tenant_id == tenant_id
    ).first()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Update job configuration with alert rules
    config = job.config
    if "monitoring" not in config:
        config["monitoring"] = {}
    
    config["monitoring"]["alert_rules"] = alert_rules
    
    job.config = config
    db.commit()
    
    # Update monitor if job is running
    if hasattr(app.state, 'job_monitor') and hasattr(app.state.job_monitor, 'monitors'):
        monitor = app.state.job_monitor.monitors.get(job_id)
        if monitor:
            monitor.alert_rules = alert_rules
    
    return {"message": "Alert rules updated successfully", "job_id": job_id}

@app.get("/api/v1/monitoring/metrics/export")
async def export_metrics_prometheus(
    context: dict = Depends(get_current_tenant_and_user)
):
    """Export metrics in Prometheus format"""
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    from fastapi.responses import Response
    
    # Collect metrics from all monitors
    metrics_data = generate_latest()
    
    return Response(
        content=metrics_data,
        media_type=CONTENT_TYPE_LATEST
    ) 

# Import core pipelines
from .pipelines.core_pipelines import (
    list_core_pipelines, 
    get_core_pipeline, 
    generate_pipeline_config,
    CorePipelineType
)

# Core Pipeline Management Endpoints
@app.get("/api/v1/pipelines/core")
async def list_core_data_mesh_pipelines():
    """List all available core data mesh pipelines"""
    return {
        "pipelines": list_core_pipelines(),
        "description": "Core pipelines for unified data mesh synchronization"
    }

@app.get("/api/v1/pipelines/core/{pipeline_name}")
async def get_core_pipeline_details(pipeline_name: str):
    """Get details of a specific core pipeline"""
    try:
        pipeline = get_core_pipeline(pipeline_name)
        return pipeline
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.post("/api/v1/pipelines/core/{pipeline_name}/deploy")
async def deploy_core_pipeline(
    pipeline_name: str,
    background_tasks: BackgroundTasks,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db),
    override_config: Optional[Dict[str, Any]] = None
):
    """Deploy a core data mesh pipeline for a tenant"""
    tenant_id = context["tenant_id"]
    
    try:
        # Generate tenant-specific configuration
        pipeline_config = generate_pipeline_config(pipeline_name, tenant_id)
        
        # Apply any overrides
        if override_config:
            pipeline_config["config"].update(override_config)
        
        # Create job record
        job = SeaTunnelJob(
            tenant_id=tenant_id,
            name=pipeline_config["name"],
            description=pipeline_config["description"],
            config=pipeline_config["config"],
            sync_mode=SyncMode(pipeline_config["sync_mode"]),
            schedule=pipeline_config.get("schedule"),
            status=JobStatus.PENDING,
            is_active=True
        )
        
        db.add(job)
        db.commit()
        db.refresh(job)
        
        # Launch job in background
        background_tasks.add_task(
            launch_job, 
            job.id, 
            tenant_id, 
            pipeline_config["config"], 
            SyncMode(pipeline_config["sync_mode"])
        )
        
        # Publish pipeline deployment event
        event_publisher = EventPublisher(settings.get("PULSAR_URL"))
        event_publisher.connect()
        event_publisher.publish_with_schema_registry(
            "data-mesh-pipeline-deployed",
            tenant_id,
            "pipeline-deployment",
            {
                "type": "record",
                "name": "PipelineDeployment",
                "namespace": "com.platformq.datamesh",
                "fields": [
                    {"name": "job_id", "type": "string"},
                    {"name": "pipeline_name", "type": "string"},
                    {"name": "pipeline_type", "type": "string"},
                    {"name": "tenant_id", "type": "string"},
                    {"name": "deployed_at", "type": "long", "logicalType": "timestamp-millis"}
                ]
            },
            {
                "job_id": job.id,
                "pipeline_name": pipeline_name,
                "pipeline_type": "core",
                "tenant_id": tenant_id,
                "deployed_at": int(datetime.now().timestamp() * 1000)
            }
        )
        event_publisher.close()
        
        return {
            "job_id": job.id,
            "status": "deploying",
            "pipeline_name": pipeline_name,
            "message": f"Core pipeline '{pipeline_name}' is being deployed"
        }
        
    except Exception as e:
        logger.error(f"Failed to deploy core pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/pipelines/core/deploy-all")
async def deploy_all_core_pipelines(
    background_tasks: BackgroundTasks,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db),
    pipeline_types: Optional[List[str]] = None
):
    """Deploy all core pipelines for a tenant"""
    tenant_id = context["tenant_id"]
    deployed_pipelines = []
    
    # Get all core pipelines
    all_pipelines = list_core_pipelines()
    
    # Filter by type if specified
    if pipeline_types:
        all_pipelines = [
            p for p in all_pipelines 
            if p["type"] in pipeline_types
        ]
    
    for pipeline_info in all_pipelines:
        try:
            pipeline_name = pipeline_info["name"]
            pipeline_config = generate_pipeline_config(pipeline_name, tenant_id)
            
            # Check if already deployed
            existing_job = db.query(SeaTunnelJob).filter(
                SeaTunnelJob.tenant_id == tenant_id,
                SeaTunnelJob.name == pipeline_config["name"],
                SeaTunnelJob.is_active == True
            ).first()
            
            if existing_job:
                logger.info(f"Pipeline '{pipeline_name}' already deployed for tenant {tenant_id}")
                continue
            
            # Create job
            job = SeaTunnelJob(
                tenant_id=tenant_id,
                name=pipeline_config["name"],
                description=pipeline_config["description"],
                config=pipeline_config["config"],
                sync_mode=SyncMode(pipeline_config["sync_mode"]),
                schedule=pipeline_config.get("schedule"),
                status=JobStatus.PENDING,
                is_active=True
            )
            
            db.add(job)
            db.commit()
            db.refresh(job)
            
            # Launch job
            background_tasks.add_task(
                launch_job,
                job.id,
                tenant_id,
                pipeline_config["config"],
                SyncMode(pipeline_config["sync_mode"])
            )
            
            deployed_pipelines.append({
                "job_id": job.id,
                "pipeline_name": pipeline_name,
                "status": "deploying"
            })
            
        except Exception as e:
            logger.error(f"Failed to deploy pipeline '{pipeline_name}': {e}")
            deployed_pipelines.append({
                "pipeline_name": pipeline_name,
                "status": "failed",
                "error": str(e)
            })
    
    return {
        "tenant_id": tenant_id,
        "deployed_count": len([p for p in deployed_pipelines if p.get("status") == "deploying"]),
        "pipelines": deployed_pipelines
    }

@app.get("/api/v1/pipelines/health")
async def check_pipeline_health(
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db)
):
    """Check health of all data mesh pipelines"""
    tenant_id = context["tenant_id"]
    
    # Get all active pipelines
    active_pipelines = db.query(SeaTunnelJob).filter(
        SeaTunnelJob.tenant_id == tenant_id,
        SeaTunnelJob.is_active == True
    ).all()
    
    health_status = []
    
    for pipeline in active_pipelines:
        # Check last run status
        last_run_healthy = True
        if pipeline.last_run:
            time_since_last_run = datetime.now() - pipeline.last_run
            # Check if pipeline should have run based on schedule
            if pipeline.schedule:
                # Simple check - should be enhanced with cron parsing
                if "hourly" in pipeline.schedule and time_since_last_run.hours > 2:
                    last_run_healthy = False
                elif "daily" in pipeline.schedule and time_since_last_run.days > 1:
                    last_run_healthy = False
        
        health_status.append({
            "job_id": pipeline.id,
            "name": pipeline.name,
            "status": pipeline.status.value,
            "last_run": pipeline.last_run.isoformat() if pipeline.last_run else None,
            "healthy": pipeline.status == JobStatus.COMPLETED and last_run_healthy,
            "error_message": pipeline.error_message
        })
    
    # Overall health
    healthy_count = len([p for p in health_status if p["healthy"]])
    total_count = len(health_status)
    
    return {
        "overall_health": "healthy" if healthy_count == total_count else "degraded" if healthy_count > 0 else "unhealthy",
        "healthy_pipelines": healthy_count,
        "total_pipelines": total_count,
        "pipelines": health_status
    } 