"""
SeaTunnel-based Pipeline Orchestration Manager
"""
import asyncio
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from enum import Enum
import json
import yaml
import uuid

import httpx
from pydantic import BaseModel, Field

from platformq_shared.utils.logger import get_logger
from platformq_shared.errors import ValidationError, ServiceError
from ..core.connection_manager import UnifiedConnectionManager
from ..lineage.lineage_tracker import DataLineageTracker, LineageNodeType, LineageEdgeType

logger = get_logger(__name__)


class PipelineStatus(str, Enum):
    """Pipeline execution status"""
    CREATED = "created"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class ConnectorType(str, Enum):
    """SeaTunnel connector types"""
    # Sources
    JDBC = "jdbc"
    KAFKA = "kafka"
    PULSAR = "pulsar"
    FILE = "file"
    ELASTICSEARCH = "elasticsearch"
    MONGODB = "mongodb"
    CASSANDRA = "cassandra"
    S3 = "s3"
    HTTP = "http"
    
    # Sinks
    CONSOLE = "console"
    HIVE = "hive"
    CLICKHOUSE = "clickhouse"
    DORIS = "doris"
    ICEBERG = "iceberg"
    HUDI = "hudi"


class TransformType(str, Enum):
    """SeaTunnel transform types"""
    SQL = "sql"
    FIELD_MAPPER = "field_mapper"
    FILTER = "filter"
    REPLACE = "replace"
    SPLIT = "split"
    JOIN = "join"
    AGGREGATE = "aggregate"
    WATERMARK = "watermark"


class PipelineConfig(BaseModel):
    """Pipeline configuration model"""
    name: str
    description: Optional[str] = None
    source: Dict[str, Any]
    transforms: Optional[List[Dict[str, Any]]] = []
    sink: Dict[str, Any]
    env: Optional[Dict[str, Any]] = {}
    
    class Config:
        extra = "allow"


class SeaTunnelPipelineManager:
    """
    SeaTunnel-based pipeline orchestration manager.
    
    Features:
    - Pipeline creation and management
    - Source/sink connector configuration
    - Data transformation chains
    - Real-time and batch processing
    - Pipeline monitoring
    - Automatic lineage tracking
    - Error handling and recovery
    """
    
    def __init__(self,
                 seatunnel_api_url: str = "http://seatunnel-api:8080",
                 connection_manager: Optional[UnifiedConnectionManager] = None,
                 lineage_tracker: Optional[DataLineageTracker] = None):
        self.seatunnel_api_url = seatunnel_api_url
        self.connection_manager = connection_manager
        self.lineage_tracker = lineage_tracker
        
        # HTTP client for SeaTunnel API
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Pipeline registry
        self.pipelines: Dict[str, Dict[str, Any]] = {}
        
        # Connector templates
        self._load_connector_templates()
        
        # Statistics
        self.stats = {
            "total_pipelines": 0,
            "active_pipelines": 0,
            "successful_runs": 0,
            "failed_runs": 0,
            "total_records_processed": 0
        }
    
    def _load_connector_templates(self) -> None:
        """Load connector configuration templates"""
        self.connector_templates = {
            # Source templates
            ConnectorType.JDBC: {
                "plugin_name": "Jdbc",
                "required": ["url", "driver", "query"],
                "optional": ["user", "password", "partition_column", "partition_num"]
            },
            ConnectorType.KAFKA: {
                "plugin_name": "Kafka",
                "required": ["topics", "bootstrap.servers"],
                "optional": ["group.id", "format", "schema"]
            },
            ConnectorType.PULSAR: {
                "plugin_name": "Pulsar",
                "required": ["topic", "service-url"],
                "optional": ["subscription-name", "subscription-type"]
            },
            ConnectorType.ELASTICSEARCH: {
                "plugin_name": "Elasticsearch",
                "required": ["hosts", "index"],
                "optional": ["query", "scroll_size", "scroll_time"]
            },
            ConnectorType.MONGODB: {
                "plugin_name": "MongoDB",
                "required": ["uri", "database", "collection"],
                "optional": ["projection", "filter"]
            },
            ConnectorType.S3: {
                "plugin_name": "S3File",
                "required": ["path", "format"],
                "optional": ["access_key", "secret_key", "endpoint"]
            },
            
            # Sink templates
            ConnectorType.CONSOLE: {
                "plugin_name": "Console",
                "required": [],
                "optional": ["limit"]
            },
            ConnectorType.ICEBERG: {
                "plugin_name": "Iceberg",
                "required": ["catalog_name", "namespace", "table"],
                "optional": ["catalog_type", "warehouse"]
            },
            ConnectorType.HUDI: {
                "plugin_name": "Hudi",
                "required": ["table.name", "hoodie.datasource.write.recordkey.field"],
                "optional": ["hoodie.datasource.write.precombine.field"]
            }
        }
    
    async def create_pipeline(self,
                            config: PipelineConfig,
                            tenant_id: str) -> Dict[str, Any]:
        """Create a new data pipeline"""
        try:
            pipeline_id = f"pipeline_{tenant_id}_{uuid.uuid4().hex[:8]}"
            
            # Validate configuration
            await self._validate_pipeline_config(config)
            
            # Build SeaTunnel job configuration
            job_config = await self._build_job_config(config, pipeline_id)
            
            # Create pipeline record
            pipeline = {
                "pipeline_id": pipeline_id,
                "tenant_id": tenant_id,
                "name": config.name,
                "description": config.description,
                "config": config.dict(),
                "job_config": job_config,
                "status": PipelineStatus.CREATED,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
            
            # Store pipeline
            self.pipelines[pipeline_id] = pipeline
            
            # Register in lineage if available
            if self.lineage_tracker:
                await self._register_pipeline_lineage(pipeline_id, config, tenant_id)
            
            # Update statistics
            self.stats["total_pipelines"] += 1
            
            logger.info(f"Created pipeline: {pipeline_id}")
            
            return pipeline
            
        except Exception as e:
            logger.error(f"Failed to create pipeline: {e}")
            raise ServiceError(f"Pipeline creation failed: {str(e)}")
    
    async def _validate_pipeline_config(self, config: PipelineConfig) -> None:
        """Validate pipeline configuration"""
        # Validate source
        source_type = config.source.get("plugin_name", "").lower()
        if source_type not in [ct.value for ct in ConnectorType]:
            raise ValidationError(f"Invalid source type: {source_type}")
        
        # Validate sink
        sink_type = config.sink.get("plugin_name", "").lower()
        if sink_type not in [ct.value for ct in ConnectorType]:
            raise ValidationError(f"Invalid sink type: {sink_type}")
        
        # Validate transforms
        for transform in config.transforms:
            transform_type = transform.get("plugin_name", "").lower()
            if transform_type not in [tt.value for tt in TransformType]:
                raise ValidationError(f"Invalid transform type: {transform_type}")
    
    async def _build_job_config(self,
                              config: PipelineConfig,
                              pipeline_id: str) -> Dict[str, Any]:
        """Build SeaTunnel job configuration"""
        job_config = {
            "env": {
                "job.name": pipeline_id,
                "job.mode": config.env.get("job.mode", "BATCH"),
                "checkpoint.interval": config.env.get("checkpoint.interval", 10000),
                **config.env
            },
            "source": [await self._build_connector_config(config.source, "source")],
            "transform": [
                await self._build_transform_config(t) 
                for t in config.transforms
            ],
            "sink": [await self._build_connector_config(config.sink, "sink")]
        }
        
        return job_config
    
    async def _build_connector_config(self,
                                    config: Dict[str, Any],
                                    connector_role: str) -> Dict[str, Any]:
        """Build connector configuration"""
        plugin_name = config.get("plugin_name")
        
        # Get template if available
        template = None
        for conn_type in ConnectorType:
            if conn_type.value == plugin_name.lower():
                template = self.connector_templates.get(conn_type)
                break
        
        if template:
            # Validate required fields
            for field in template["required"]:
                if field not in config:
                    raise ValidationError(f"Missing required field '{field}' for {plugin_name}")
        
        # Build final config
        connector_config = {
            "plugin_name": plugin_name,
            **{k: v for k, v in config.items() if k != "plugin_name"}
        }
        
        # Add connection details from connection manager if available
        if self.connection_manager and connector_role == "source":
            connector_config = await self._enrich_with_connection_details(
                connector_config, plugin_name
            )
        
        return connector_config
    
    async def _build_transform_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Build transform configuration"""
        plugin_name = config.get("plugin_name")
        
        transform_config = {
            "plugin_name": plugin_name,
            **{k: v for k, v in config.items() if k != "plugin_name"}
        }
        
        # Add default configurations based on transform type
        if plugin_name.lower() == TransformType.SQL.value:
            if "result_table_name" not in transform_config:
                transform_config["result_table_name"] = "transformed_data"
        
        return transform_config
    
    async def _enrich_with_connection_details(self,
                                            config: Dict[str, Any],
                                            plugin_name: str) -> Dict[str, Any]:
        """Enrich configuration with connection details"""
        if not self.connection_manager:
            return config
        
        plugin_lower = plugin_name.lower()
        
        # JDBC connections
        if plugin_lower == "jdbc":
            # Try to get PostgreSQL connection details
            if "postgresql" in config.get("url", ""):
                conn_config = self.connection_manager.config
                config.setdefault("user", conn_config.get("postgresql_user"))
                config.setdefault("password", conn_config.get("postgresql_password"))
        
        # Elasticsearch
        elif plugin_lower == "elasticsearch":
            es_config = self.connection_manager.config
            config.setdefault("hosts", [f"http://{es_config.get('elasticsearch_host')}:9200"])
        
        # MongoDB
        elif plugin_lower == "mongodb":
            mongo_config = self.connection_manager.config
            config.setdefault("uri", mongo_config.get("mongodb_uri"))
        
        return config
    
    async def run_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Run a pipeline"""
        try:
            if pipeline_id not in self.pipelines:
                raise NotFoundError(f"Pipeline not found: {pipeline_id}")
            
            pipeline = self.pipelines[pipeline_id]
            
            # Update status
            pipeline["status"] = PipelineStatus.RUNNING
            pipeline["started_at"] = datetime.utcnow()
            
            # Submit job to SeaTunnel
            job_config = pipeline["job_config"]
            
            response = await self.http_client.post(
                f"{self.seatunnel_api_url}/hazelcast/rest/maps/submit-job",
                json=job_config
            )
            
            if response.status_code == 200:
                result = response.json()
                job_id = result.get("jobId")
                
                pipeline["job_id"] = job_id
                pipeline["status"] = PipelineStatus.RUNNING
                
                # Start monitoring task
                asyncio.create_task(self._monitor_pipeline(pipeline_id, job_id))
                
                # Update statistics
                self.stats["active_pipelines"] += 1
                
                logger.info(f"Started pipeline {pipeline_id} with job ID {job_id}")
                
                return {
                    "pipeline_id": pipeline_id,
                    "job_id": job_id,
                    "status": "running",
                    "message": "Pipeline started successfully"
                }
            else:
                raise ServiceError(f"Failed to submit job: {response.text}")
                
        except Exception as e:
            logger.error(f"Failed to run pipeline: {e}")
            
            # Update pipeline status
            if pipeline_id in self.pipelines:
                self.pipelines[pipeline_id]["status"] = PipelineStatus.FAILED
                self.pipelines[pipeline_id]["error"] = str(e)
                self.stats["failed_runs"] += 1
            
            raise
    
    async def _monitor_pipeline(self, pipeline_id: str, job_id: str) -> None:
        """Monitor pipeline execution"""
        try:
            while True:
                # Check job status
                response = await self.http_client.get(
                    f"{self.seatunnel_api_url}/hazelcast/rest/maps/job-info/{job_id}"
                )
                
                if response.status_code == 200:
                    job_info = response.json()
                    status = job_info.get("status", "").upper()
                    
                    # Update pipeline status
                    pipeline = self.pipelines[pipeline_id]
                    
                    if status == "FINISHED":
                        pipeline["status"] = PipelineStatus.SUCCEEDED
                        pipeline["completed_at"] = datetime.utcnow()
                        pipeline["metrics"] = job_info.get("metrics", {})
                        
                        # Update statistics
                        self.stats["successful_runs"] += 1
                        self.stats["active_pipelines"] -= 1
                        
                        records_processed = job_info.get("metrics", {}).get("readRecords", 0)
                        self.stats["total_records_processed"] += records_processed
                        
                        # Track lineage completion
                        if self.lineage_tracker:
                            await self._update_lineage_completion(pipeline_id, job_info)
                        
                        logger.info(f"Pipeline {pipeline_id} completed successfully")
                        break
                        
                    elif status == "FAILED":
                        pipeline["status"] = PipelineStatus.FAILED
                        pipeline["completed_at"] = datetime.utcnow()
                        pipeline["error"] = job_info.get("errorMsg", "Unknown error")
                        
                        # Update statistics
                        self.stats["failed_runs"] += 1
                        self.stats["active_pipelines"] -= 1
                        
                        logger.error(f"Pipeline {pipeline_id} failed: {pipeline['error']}")
                        break
                    
                    elif status == "CANCELED":
                        pipeline["status"] = PipelineStatus.CANCELLED
                        pipeline["completed_at"] = datetime.utcnow()
                        
                        self.stats["active_pipelines"] -= 1
                        
                        logger.info(f"Pipeline {pipeline_id} was cancelled")
                        break
                
                # Wait before next check
                await asyncio.sleep(5)
                
        except Exception as e:
            logger.error(f"Error monitoring pipeline {pipeline_id}: {e}")
            
            # Mark pipeline as failed
            if pipeline_id in self.pipelines:
                self.pipelines[pipeline_id]["status"] = PipelineStatus.FAILED
                self.pipelines[pipeline_id]["error"] = f"Monitoring error: {str(e)}"
    
    async def stop_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Stop a running pipeline"""
        try:
            if pipeline_id not in self.pipelines:
                raise NotFoundError(f"Pipeline not found: {pipeline_id}")
            
            pipeline = self.pipelines[pipeline_id]
            job_id = pipeline.get("job_id")
            
            if not job_id:
                raise ValidationError("Pipeline has no associated job ID")
            
            # Cancel job in SeaTunnel
            response = await self.http_client.post(
                f"{self.seatunnel_api_url}/hazelcast/rest/maps/cancel-job/{job_id}"
            )
            
            if response.status_code == 200:
                pipeline["status"] = PipelineStatus.CANCELLED
                pipeline["stopped_at"] = datetime.utcnow()
                
                logger.info(f"Stopped pipeline {pipeline_id}")
                
                return {
                    "pipeline_id": pipeline_id,
                    "status": "cancelled",
                    "message": "Pipeline stopped successfully"
                }
            else:
                raise ServiceError(f"Failed to stop pipeline: {response.text}")
                
        except Exception as e:
            logger.error(f"Failed to stop pipeline: {e}")
            raise
    
    async def get_pipeline_status(self, pipeline_id: str) -> Dict[str, Any]:
        """Get pipeline status"""
        if pipeline_id not in self.pipelines:
            raise NotFoundError(f"Pipeline not found: {pipeline_id}")
        
        pipeline = self.pipelines[pipeline_id]
        
        return {
            "pipeline_id": pipeline_id,
            "name": pipeline["name"],
            "status": pipeline["status"],
            "created_at": pipeline["created_at"],
            "started_at": pipeline.get("started_at"),
            "completed_at": pipeline.get("completed_at"),
            "error": pipeline.get("error"),
            "metrics": pipeline.get("metrics", {})
        }
    
    async def list_pipelines(self, 
                           tenant_id: str,
                           status: Optional[PipelineStatus] = None) -> List[Dict[str, Any]]:
        """List pipelines for a tenant"""
        pipelines = []
        
        for pipeline_id, pipeline in self.pipelines.items():
            if pipeline["tenant_id"] == tenant_id:
                if status is None or pipeline["status"] == status:
                    pipelines.append({
                        "pipeline_id": pipeline_id,
                        "name": pipeline["name"],
                        "description": pipeline["description"],
                        "status": pipeline["status"],
                        "created_at": pipeline["created_at"],
                        "updated_at": pipeline["updated_at"]
                    })
        
        return pipelines
    
    async def update_pipeline(self,
                            pipeline_id: str,
                            updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update pipeline configuration"""
        if pipeline_id not in self.pipelines:
            raise NotFoundError(f"Pipeline not found: {pipeline_id}")
        
        pipeline = self.pipelines[pipeline_id]
        
        # Only allow updates when pipeline is not running
        if pipeline["status"] == PipelineStatus.RUNNING:
            raise ValidationError("Cannot update running pipeline")
        
        # Update allowed fields
        allowed_fields = ["name", "description", "config"]
        for field in allowed_fields:
            if field in updates:
                if field == "config":
                    # Validate new config
                    new_config = PipelineConfig(**updates[field])
                    await self._validate_pipeline_config(new_config)
                    pipeline["config"] = new_config.dict()
                    # Rebuild job config
                    pipeline["job_config"] = await self._build_job_config(
                        new_config, pipeline_id
                    )
                else:
                    pipeline[field] = updates[field]
        
        pipeline["updated_at"] = datetime.utcnow()
        
        logger.info(f"Updated pipeline {pipeline_id}")
        
        return pipeline
    
    async def delete_pipeline(self, pipeline_id: str) -> None:
        """Delete a pipeline"""
        if pipeline_id not in self.pipelines:
            raise NotFoundError(f"Pipeline not found: {pipeline_id}")
        
        pipeline = self.pipelines[pipeline_id]
        
        # Don't allow deletion of running pipelines
        if pipeline["status"] == PipelineStatus.RUNNING:
            raise ValidationError("Cannot delete running pipeline")
        
        # Remove from registry
        del self.pipelines[pipeline_id]
        
        # Update statistics
        self.stats["total_pipelines"] -= 1
        
        logger.info(f"Deleted pipeline {pipeline_id}")
    
    async def _register_pipeline_lineage(self,
                                       pipeline_id: str,
                                       config: PipelineConfig,
                                       tenant_id: str) -> None:
        """Register pipeline in lineage system"""
        if not self.lineage_tracker:
            return
        
        try:
            # Create pipeline node
            await self.lineage_tracker.add_node(
                node_id=pipeline_id,
                node_type=LineageNodeType.PROCESS,
                name=config.name,
                tenant_id=tenant_id,
                metadata={
                    "type": "seatunnel_pipeline",
                    "source": config.source.get("plugin_name"),
                    "sink": config.sink.get("plugin_name"),
                    "transforms": [t.get("plugin_name") for t in config.transforms]
                }
            )
            
            # Create source dataset node if identifiable
            source_id = self._extract_dataset_id(config.source, "source")
            if source_id:
                await self.lineage_tracker.add_node(
                    node_id=source_id,
                    node_type=LineageNodeType.DATASET,
                    name=source_id,
                    tenant_id=tenant_id,
                    metadata={"source_type": config.source.get("plugin_name")}
                )
                
                # Link source to pipeline
                await self.lineage_tracker.add_edge(
                    source_id=source_id,
                    target_id=pipeline_id,
                    edge_type=LineageEdgeType.READS
                )
            
            # Create sink dataset node if identifiable
            sink_id = self._extract_dataset_id(config.sink, "sink")
            if sink_id:
                await self.lineage_tracker.add_node(
                    node_id=sink_id,
                    node_type=LineageNodeType.DATASET,
                    name=sink_id,
                    tenant_id=tenant_id,
                    metadata={"sink_type": config.sink.get("plugin_name")}
                )
                
                # Link pipeline to sink
                await self.lineage_tracker.add_edge(
                    source_id=pipeline_id,
                    target_id=sink_id,
                    edge_type=LineageEdgeType.WRITES
                )
                
        except Exception as e:
            logger.error(f"Failed to register pipeline lineage: {e}")
    
    def _extract_dataset_id(self, 
                          connector_config: Dict[str, Any],
                          role: str) -> Optional[str]:
        """Extract dataset identifier from connector config"""
        plugin_name = connector_config.get("plugin_name", "").lower()
        
        if plugin_name == "jdbc":
            # Use table name or query
            if "table" in connector_config:
                return connector_config["table"]
            elif "query" in connector_config:
                # Extract table from query (simplified)
                query = connector_config["query"].lower()
                if "from" in query:
                    parts = query.split("from")[1].strip().split()
                    if parts:
                        return parts[0]
        
        elif plugin_name == "elasticsearch":
            return connector_config.get("index")
        
        elif plugin_name == "mongodb":
            database = connector_config.get("database")
            collection = connector_config.get("collection")
            if database and collection:
                return f"{database}.{collection}"
        
        elif plugin_name == "kafka":
            topics = connector_config.get("topics")
            if topics:
                return f"kafka:{topics[0] if isinstance(topics, list) else topics}"
        
        elif plugin_name == "iceberg":
            catalog = connector_config.get("catalog_name")
            namespace = connector_config.get("namespace")
            table = connector_config.get("table")
            if all([catalog, namespace, table]):
                return f"{catalog}.{namespace}.{table}"
        
        return None
    
    async def _update_lineage_completion(self,
                                       pipeline_id: str,
                                       job_info: Dict[str, Any]) -> None:
        """Update lineage with pipeline completion info"""
        if not self.lineage_tracker:
            return
        
        try:
            # Update pipeline node with metrics
            pipeline = self.pipelines[pipeline_id]
            
            await self.lineage_tracker.add_node(
                node_id=f"{pipeline_id}_run_{datetime.utcnow().timestamp()}",
                node_type=LineageNodeType.JOB,
                name=f"{pipeline['name']}_run",
                tenant_id=pipeline["tenant_id"],
                metadata={
                    "pipeline_id": pipeline_id,
                    "status": "completed",
                    "started_at": pipeline.get("started_at"),
                    "completed_at": pipeline.get("completed_at"),
                    "records_read": job_info.get("metrics", {}).get("readRecords", 0),
                    "records_written": job_info.get("metrics", {}).get("writeRecords", 0)
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to update lineage completion: {e}")
    
    async def export_pipeline_config(self, 
                                   pipeline_id: str,
                                   format: str = "yaml") -> str:
        """Export pipeline configuration"""
        if pipeline_id not in self.pipelines:
            raise NotFoundError(f"Pipeline not found: {pipeline_id}")
        
        pipeline = self.pipelines[pipeline_id]
        config = pipeline["config"]
        
        if format == "yaml":
            return yaml.dump(config, default_flow_style=False)
        elif format == "json":
            return json.dumps(config, indent=2)
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    async def import_pipeline_config(self,
                                   config_str: str,
                                   format: str = "yaml",
                                   tenant_id: str) -> Dict[str, Any]:
        """Import pipeline configuration"""
        try:
            if format == "yaml":
                config_dict = yaml.safe_load(config_str)
            elif format == "json":
                config_dict = json.loads(config_str)
            else:
                raise ValueError(f"Unsupported import format: {format}")
            
            # Create pipeline from imported config
            config = PipelineConfig(**config_dict)
            return await self.create_pipeline(config, tenant_id)
            
        except Exception as e:
            logger.error(f"Failed to import pipeline config: {e}")
            raise ValidationError(f"Invalid pipeline configuration: {str(e)}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get pipeline manager statistics"""
        return self.stats
    
    async def shutdown(self) -> None:
        """Shutdown pipeline manager"""
        # Stop all running pipelines
        for pipeline_id, pipeline in self.pipelines.items():
            if pipeline["status"] == PipelineStatus.RUNNING:
                try:
                    await self.stop_pipeline(pipeline_id)
                except Exception as e:
                    logger.error(f"Error stopping pipeline {pipeline_id}: {e}")
        
        # Close HTTP client
        await self.http_client.aclose()
        
        logger.info("Pipeline manager shut down") 