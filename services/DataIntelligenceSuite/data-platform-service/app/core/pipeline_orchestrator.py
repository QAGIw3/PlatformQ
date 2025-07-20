"""
Pipeline Orchestrator for managing data pipelines
"""
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import httpx
from platformq_shared.utils.logger import get_logger
from platformq_shared.cache.ignite_client import IgniteClient
from platformq_shared.events import EventPublisher
from platformq_shared.errors import ValidationError, ServiceError

logger = get_logger(__name__)


class PipelineStatus(Enum):
    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ConnectorType(Enum):
    JDBC = "jdbc"
    KAFKA = "kafka"
    PULSAR = "pulsar"
    S3 = "s3"
    MINIO = "minio"
    ELASTICSEARCH = "elasticsearch"
    CASSANDRA = "cassandra"
    IGNITE = "ignite"
    HIVE = "hive"
    ICEBERG = "iceberg"


class PipelineOrchestrator:
    """Orchestrator for data pipeline management using SeaTunnel"""
    
    def __init__(self):
        self.cache = IgniteClient()
        self.event_publisher = EventPublisher()
        self.seatunnel_api = "http://seatunnel-api:8080"
        self.pipelines: Dict[str, Dict] = {}
        self.connectors: Dict[str, Dict] = {}
        
    async def create_pipeline(self, pipeline_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new data pipeline"""
        try:
            # Validate pipeline definition
            required_fields = ["name", "source", "sink", "transformations"]
            for field in required_fields:
                if field not in pipeline_data:
                    raise ValidationError(f"Missing required field: {field}")
            
            # Generate pipeline ID
            pipeline_id = f"pipeline_{pipeline_data['name']}_{datetime.utcnow().timestamp()}"
            
            # Create pipeline configuration
            pipeline = {
                "pipeline_id": pipeline_id,
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
                "status": PipelineStatus.CREATED.value,
                "version": 1,
                **pipeline_data
            }
            
            # Generate SeaTunnel config
            seatunnel_config = await self._generate_seatunnel_config(pipeline)
            pipeline["seatunnel_config"] = seatunnel_config
            
            # Store pipeline
            self.pipelines[pipeline_id] = pipeline
            await self.cache.put(f"pipeline:{pipeline_id}", pipeline)
            
            # Publish event
            await self.event_publisher.publish("pipeline.created", {
                "pipeline_id": pipeline_id,
                "name": pipeline_data["name"]
            })
            
            logger.info(f"Created pipeline: {pipeline_id}")
            return pipeline
            
        except Exception as e:
            logger.error(f"Failed to create pipeline: {str(e)}")
            raise
    
    async def run_pipeline(self, pipeline_id: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Run a pipeline"""
        try:
            # Get pipeline
            pipeline = await self._get_pipeline(pipeline_id)
            
            if pipeline["status"] == PipelineStatus.RUNNING.value:
                raise ValidationError(f"Pipeline {pipeline_id} is already running")
            
            # Create run ID
            run_id = f"run_{pipeline_id}_{datetime.utcnow().timestamp()}"
            
            # Update pipeline status
            pipeline["status"] = PipelineStatus.RUNNING.value
            pipeline["current_run_id"] = run_id
            await self._update_pipeline(pipeline_id, pipeline)
            
            # Submit to SeaTunnel
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.seatunnel_api}/jobs/submit",
                    json={
                        "config": pipeline["seatunnel_config"],
                        "params": params or {}
                    }
                )
                
                if response.status_code != 200:
                    raise ServiceError(f"Failed to submit job: {response.text}")
                
                job_data = response.json()
            
            # Create run record
            run = {
                "run_id": run_id,
                "pipeline_id": pipeline_id,
                "job_id": job_data["job_id"],
                "started_at": datetime.utcnow().isoformat(),
                "status": "running",
                "params": params
            }
            
            await self.cache.put(f"pipeline:run:{run_id}", run)
            
            # Start monitoring
            asyncio.create_task(self._monitor_run(run_id))
            
            # Publish event
            await self.event_publisher.publish("pipeline.started", {
                "pipeline_id": pipeline_id,
                "run_id": run_id
            })
            
            return run
            
        except Exception as e:
            logger.error(f"Failed to run pipeline: {str(e)}")
            raise
    
    async def pause_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Pause a running pipeline"""
        try:
            pipeline = await self._get_pipeline(pipeline_id)
            
            if pipeline["status"] != PipelineStatus.RUNNING.value:
                raise ValidationError(f"Pipeline {pipeline_id} is not running")
            
            # Pause via SeaTunnel API
            run_id = pipeline.get("current_run_id")
            if run_id:
                run = await self.cache.get(f"pipeline:run:{run_id}")
                if run and "job_id" in run:
                    async with httpx.AsyncClient() as client:
                        await client.post(
                            f"{self.seatunnel_api}/jobs/{run['job_id']}/pause"
                        )
            
            # Update status
            pipeline["status"] = PipelineStatus.PAUSED.value
            await self._update_pipeline(pipeline_id, pipeline)
            
            # Publish event
            await self.event_publisher.publish("pipeline.paused", {
                "pipeline_id": pipeline_id
            })
            
            return {"pipeline_id": pipeline_id, "status": "paused"}
            
        except Exception as e:
            logger.error(f"Failed to pause pipeline: {str(e)}")
            raise
    
    async def get_pipeline_runs(self, 
                               pipeline_id: str,
                               limit: int = 100) -> List[Dict[str, Any]]:
        """Get pipeline run history"""
        # In production, query from storage
        # For now, return mock data
        runs = []
        for i in range(min(limit, 5)):
            runs.append({
                "run_id": f"run_{pipeline_id}_{i}",
                "pipeline_id": pipeline_id,
                "started_at": (datetime.utcnow() - timedelta(hours=i)).isoformat(),
                "completed_at": (datetime.utcnow() - timedelta(hours=i-1)).isoformat() if i > 0 else None,
                "status": "completed" if i > 0 else "running",
                "records_processed": 1000000 * (i + 1),
                "duration_seconds": 3600 if i > 0 else None
            })
        
        return runs
    
    async def create_connector(self, connector_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a reusable connector configuration"""
        try:
            # Validate connector
            required_fields = ["name", "type", "config"]
            for field in required_fields:
                if field not in connector_data:
                    raise ValidationError(f"Missing required field: {field}")
            
            # Validate connector type
            if connector_data["type"] not in [t.value for t in ConnectorType]:
                raise ValidationError(f"Invalid connector type: {connector_data['type']}")
            
            # Generate connector ID
            connector_id = f"connector_{connector_data['name']}_{datetime.utcnow().timestamp()}"
            
            # Create connector
            connector = {
                "connector_id": connector_id,
                "created_at": datetime.utcnow().isoformat(),
                **connector_data
            }
            
            # Test connection
            test_result = await self._test_connector(connector)
            connector["last_test"] = test_result
            
            # Store connector
            self.connectors[connector_id] = connector
            await self.cache.put(f"connector:{connector_id}", connector)
            
            return connector
            
        except Exception as e:
            logger.error(f"Failed to create connector: {str(e)}")
            raise
    
    async def get_pipeline_metrics(self, pipeline_id: str) -> Dict[str, Any]:
        """Get pipeline metrics"""
        pipeline = await self._get_pipeline(pipeline_id)
        
        # Get current run metrics if running
        metrics = {
            "pipeline_id": pipeline_id,
            "status": pipeline["status"],
            "total_runs": 10,  # Mock data
            "successful_runs": 8,
            "failed_runs": 2,
            "average_duration_seconds": 3600,
            "total_records_processed": 10000000
        }
        
        if pipeline["status"] == PipelineStatus.RUNNING.value:
            run_id = pipeline.get("current_run_id")
            if run_id:
                # Get real-time metrics from SeaTunnel
                metrics["current_run"] = {
                    "run_id": run_id,
                    "records_processed": 500000,
                    "records_per_second": 1000,
                    "elapsed_seconds": 500
                }
        
        return metrics
    
    async def _generate_seatunnel_config(self, pipeline: Dict) -> Dict[str, Any]:
        """Generate SeaTunnel configuration from pipeline definition"""
        config = {
            "env": {
                "execution.parallelism": pipeline.get("parallelism", 2),
                "job.mode": "BATCH",
                "checkpoint.interval": 10000
            },
            "source": []
        }
        
        # Add source configuration
        source = pipeline["source"]
        source_config = {
            "plugin_name": source["type"],
            **source.get("config", {})
        }
        
        # Handle connector reference
        if "connector_id" in source:
            connector = self.connectors.get(source["connector_id"])
            if connector:
                source_config.update(connector["config"])
        
        config["source"].append(source_config)
        
        # Add transformations
        if pipeline.get("transformations"):
            config["transform"] = []
            for transform in pipeline["transformations"]:
                transform_config = {
                    "plugin_name": transform["type"],
                    **transform.get("config", {})
                }
                config["transform"].append(transform_config)
        
        # Add sink configuration
        sink = pipeline["sink"]
        sink_config = {
            "plugin_name": sink["type"],
            **sink.get("config", {})
        }
        
        # Handle connector reference
        if "connector_id" in sink:
            connector = self.connectors.get(sink["connector_id"])
            if connector:
                sink_config.update(connector["config"])
        
        config["sink"] = [sink_config]
        
        return config
    
    async def _monitor_run(self, run_id: str):
        """Monitor pipeline run status"""
        try:
            while True:
                # Get run info
                run = await self.cache.get(f"pipeline:run:{run_id}")
                if not run:
                    break
                
                # Check job status via SeaTunnel API
                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        f"{self.seatunnel_api}/jobs/{run['job_id']}/status"
                    )
                    
                    if response.status_code == 200:
                        job_status = response.json()
                        
                        # Update run status
                        run["status"] = job_status["status"]
                        if job_status["status"] in ["completed", "failed", "cancelled"]:
                            run["completed_at"] = datetime.utcnow().isoformat()
                            run["metrics"] = job_status.get("metrics", {})
                            
                            # Update pipeline status
                            pipeline_id = run["pipeline_id"]
                            pipeline = await self._get_pipeline(pipeline_id)
                            pipeline["status"] = PipelineStatus.COMPLETED.value if job_status["status"] == "completed" else PipelineStatus.FAILED.value
                            await self._update_pipeline(pipeline_id, pipeline)
                            
                            # Publish event
                            await self.event_publisher.publish(f"pipeline.{job_status['status']}", {
                                "pipeline_id": pipeline_id,
                                "run_id": run_id,
                                "metrics": job_status.get("metrics", {})
                            })
                            
                            break
                        
                        await self.cache.put(f"pipeline:run:{run_id}", run)
                
                # Wait before next check
                await asyncio.sleep(10)
                
        except Exception as e:
            logger.error(f"Error monitoring run {run_id}: {str(e)}")
    
    async def _test_connector(self, connector: Dict) -> Dict[str, Any]:
        """Test connector configuration"""
        # In production, actually test the connection
        # For now, return mock result
        return {
            "tested_at": datetime.utcnow().isoformat(),
            "status": "success",
            "latency_ms": 50
        }
    
    async def _get_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Get pipeline by ID"""
        pipeline = self.pipelines.get(pipeline_id)
        if not pipeline:
            cached = await self.cache.get(f"pipeline:{pipeline_id}")
            if not cached:
                raise ValidationError(f"Pipeline {pipeline_id} not found")
            pipeline = cached
            self.pipelines[pipeline_id] = pipeline
        
        return pipeline
    
    async def _update_pipeline(self, pipeline_id: str, pipeline: Dict):
        """Update pipeline"""
        pipeline["updated_at"] = datetime.utcnow().isoformat()
        self.pipelines[pipeline_id] = pipeline
        await self.cache.put(f"pipeline:{pipeline_id}", pipeline) 