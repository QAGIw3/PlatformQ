from platformq_shared.base_service import create_base_app
from fastapi import Depends, HTTPException, BackgroundTasks, UploadFile, File
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
from datetime import datetime
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
                        
                        # Update job status based on K8s job status
                        if status.get("succeeded", 0) > 0:
                            job.status = JobStatus.COMPLETED
                            job.metrics = self._extract_metrics_from_logs(
                                status.get("logs", "")
                            )
                            
                            # Publish completion event
                            self._publish_job_completed(job)
                            
                        elif status.get("failed", 0) > 0:
                            job.status = JobStatus.FAILED
                            job.error_message = self._extract_error_from_logs(
                                status.get("logs", "")
                            )
                            
                        job.updated_at = datetime.utcnow()
                        db.commit()
                
                db.close()
                
            except Exception as e:
                logger.error(f"Error in job monitor: {e}")
                
            time.sleep(10)  # Check every 10 seconds
    
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

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize the service"""
    # Start job monitor
    monitor = JobMonitor(app.state)
    monitor.start()
    app.state.job_monitor = monitor
    
    logger.info("SeaTunnel service started")

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