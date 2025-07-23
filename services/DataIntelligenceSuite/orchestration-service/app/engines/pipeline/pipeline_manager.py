"""
Pipeline Manager

Manages data pipeline creation, execution, and monitoring.
"""

import asyncio
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
from enum import Enum
import uuid

from data_intelligence_common import StructuredLogger, EventBus
from data_intelligence_common.vault_consul import VaultConsulIntegration
from pyignite import AsyncClient

logger = StructuredLogger.get_logger(__name__)


class PipelineType(Enum):
    """Pipeline types"""
    ETL = "etl"
    TRANSFORMATION = "transformation"
    STREAMING = "streaming"
    ML_TRAINING = "ml_training"
    DATA_QUALITY = "data_quality"
    HYBRID = "hybrid"


class PipelineStatus(Enum):
    """Pipeline execution status"""
    CREATED = "created"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepType(Enum):
    """Pipeline step types"""
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    VALIDATE = "validate"
    ENRICH = "enrich"
    AGGREGATE = "aggregate"
    CUSTOM = "custom"


class PipelineManager:
    """
    Manages data pipeline lifecycle
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration, event_bus: EventBus,
                 executor: Any, dependency_resolver: Any):
        self.vault_consul = vault_consul
        self.event_bus = event_bus
        self.executor = executor
        self.dependency_resolver = dependency_resolver
        
        # Pipeline storage
        self.pipelines: Dict[str, Dict[str, Any]] = {}
        self.executions: Dict[str, Dict[str, Any]] = {}
        self.templates: Dict[str, Dict[str, Any]] = {}
        
        # Ignite client for distributed caching
        self.ignite_client: Optional[AsyncClient] = None
        
        # Configuration
        self.config = {
            "max_concurrent_pipelines": 50,
            "max_retries": 3,
            "retry_delay": 300,
            "checkpoint_interval": 100,
            "batch_size": 1000,
            "storage_backend": "ignite"
        }
        
        # Metrics
        self.metrics = {
            "pipelines_created": 0,
            "pipelines_executed": 0,
            "pipelines_succeeded": 0,
            "pipelines_failed": 0,
            "total_records_processed": 0,
            "avg_processing_time": 0
        }
    
    async def initialize(self):
        """Initialize pipeline manager"""
        logger.info("initializing_pipeline_manager")
        
        # Load configuration
        await self._load_configuration()
        
        # Initialize Ignite client
        self.ignite_client = AsyncClient()
        await self.ignite_client.connect("ignite", 10800)
        
        # Initialize executor
        await self.executor.initialize()
        
        # Load pipeline templates
        await self._load_templates()
        
        # Start background tasks
        asyncio.create_task(self._process_pipeline_queue())
        asyncio.create_task(self._monitor_pipelines())
        
        logger.info("pipeline_manager_initialized")
    
    async def cleanup(self):
        """Cleanup resources"""
        # Cancel all running pipelines
        for exec_id in list(self.executions.keys()):
            if self.executions[exec_id]["status"] == PipelineStatus.RUNNING:
                await self.cancel_pipeline(exec_id)
        
        if self.ignite_client:
            await self.ignite_client.close()
        
        await self.executor.cleanup()
    
    async def _load_configuration(self):
        """Load configuration from Consul"""
        try:
            config = await self.vault_consul.consul.kv.get("config/pipeline-manager")
            if config:
                import json
                self.config.update(json.loads(config["Value"].decode()))
        except Exception as e:
            logger.warning(f"Failed to load config from Consul: {e}")
    
    async def create_pipeline(self, pipeline_config: Dict[str, Any]) -> str:
        """
        Create a new pipeline
        
        Args:
            pipeline_config: Pipeline configuration including:
                - name: Pipeline name
                - type: Pipeline type
                - description: Pipeline description
                - steps: List of pipeline steps
                - dependencies: Step dependencies
                - config: Additional configuration
                
        Returns:
            Pipeline ID
        """
        pipeline_id = str(uuid.uuid4())
        
        # Validate pipeline configuration
        self._validate_pipeline_config(pipeline_config)
        
        # Resolve step dependencies
        dependency_graph = await self.dependency_resolver.resolve_dependencies(
            pipeline_config["steps"],
            pipeline_config.get("dependencies", {})
        )
        
        # Create pipeline record
        pipeline = {
            "id": pipeline_id,
            "config": pipeline_config,
            "dependency_graph": dependency_graph,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "version": 1,
            "status": "active",
            "executions": []
        }
        
        # Store pipeline
        self.pipelines[pipeline_id] = pipeline
        
        # Cache in Ignite
        if self.ignite_client:
            cache = await self.ignite_client.get_or_create_cache(f"pipeline_{pipeline_id}")
            await cache.put("config", pipeline_config)
            await cache.put("dependency_graph", dependency_graph)
        
        # Update metrics
        self.metrics["pipelines_created"] += 1
        
        # Emit event
        await self.event_bus.publish(
            "orchestration.pipeline.created",
            {
                "pipeline_id": pipeline_id,
                "name": pipeline_config.get("name"),
                "type": pipeline_config.get("type"),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Pipeline created: {pipeline_id}")
        return pipeline_id
    
    async def execute_pipeline(self, pipeline_id: str, input_data: Dict[str, Any] = None) -> str:
        """
        Execute a pipeline
        
        Args:
            pipeline_id: Pipeline ID
            input_data: Input data for pipeline
            
        Returns:
            Execution ID
        """
        pipeline = self.pipelines.get(pipeline_id)
        if not pipeline:
            raise ValueError(f"Pipeline not found: {pipeline_id}")
        
        if pipeline["status"] != "active":
            raise RuntimeError(f"Pipeline not active: {pipeline['status']}")
        
        # Check concurrent execution limit
        running_count = sum(1 for exec in self.executions.values() 
                          if exec["status"] == PipelineStatus.RUNNING)
        
        if running_count >= self.config["max_concurrent_pipelines"]:
            raise RuntimeError("Maximum concurrent pipelines reached")
        
        execution_id = str(uuid.uuid4())
        
        # Create execution record
        execution = {
            "id": execution_id,
            "pipeline_id": pipeline_id,
            "status": PipelineStatus.QUEUED,
            "input_data": input_data or {},
            "output_data": None,
            "started_at": None,
            "completed_at": None,
            "steps_completed": 0,
            "total_steps": len(pipeline["config"]["steps"]),
            "current_step": None,
            "error": None,
            "metrics": {
                "records_processed": 0,
                "processing_time": 0,
                "retries": 0
            }
        }
        
        # Store execution
        self.executions[execution_id] = execution
        pipeline["executions"].append(execution_id)
        
        # Queue for execution
        await self._queue_pipeline_execution(execution_id)
        
        # Update metrics
        self.metrics["pipelines_executed"] += 1
        
        # Emit event
        await self.event_bus.publish(
            "orchestration.pipeline.queued",
            {
                "pipeline_id": pipeline_id,
                "execution_id": execution_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Pipeline execution queued: {execution_id}")
        return execution_id
    
    async def get_pipeline_status(self, pipeline_id: str) -> Dict[str, Any]:
        """Get pipeline status"""
        pipeline = self.pipelines.get(pipeline_id)
        if not pipeline:
            raise ValueError(f"Pipeline not found: {pipeline_id}")
        
        # Get latest execution status
        latest_execution = None
        if pipeline["executions"]:
            latest_exec_id = pipeline["executions"][-1]
            latest_execution = self.executions.get(latest_exec_id)
        
        return {
            "id": pipeline_id,
            "name": pipeline["config"]["name"],
            "type": pipeline["config"]["type"],
            "status": pipeline["status"],
            "created_at": pipeline["created_at"].isoformat(),
            "updated_at": pipeline["updated_at"].isoformat(),
            "version": pipeline["version"],
            "total_executions": len(pipeline["executions"]),
            "latest_execution": {
                "id": latest_execution["id"],
                "status": latest_execution["status"].value,
                "started_at": latest_execution["started_at"].isoformat() if latest_execution["started_at"] else None,
                "progress": f"{latest_execution['steps_completed']}/{latest_execution['total_steps']}"
            } if latest_execution else None
        }
    
    async def get_execution_status(self, execution_id: str) -> Dict[str, Any]:
        """Get pipeline execution status"""
        execution = self.executions.get(execution_id)
        if not execution:
            raise ValueError(f"Execution not found: {execution_id}")
        
        return {
            "id": execution_id,
            "pipeline_id": execution["pipeline_id"],
            "status": execution["status"].value,
            "started_at": execution["started_at"].isoformat() if execution["started_at"] else None,
            "completed_at": execution["completed_at"].isoformat() if execution["completed_at"] else None,
            "progress": {
                "steps_completed": execution["steps_completed"],
                "total_steps": execution["total_steps"],
                "current_step": execution["current_step"],
                "percentage": (execution["steps_completed"] / execution["total_steps"] * 100) 
                            if execution["total_steps"] > 0 else 0
            },
            "metrics": execution["metrics"],
            "error": execution["error"]
        }
    
    async def cancel_pipeline(self, execution_id: str) -> bool:
        """Cancel pipeline execution"""
        execution = self.executions.get(execution_id)
        if not execution:
            raise ValueError(f"Execution not found: {execution_id}")
        
        if execution["status"] not in [PipelineStatus.QUEUED, PipelineStatus.RUNNING]:
            return False
        
        # Cancel execution
        await self.executor.cancel_execution(execution_id)
        
        # Update status
        execution["status"] = PipelineStatus.CANCELLED
        execution["completed_at"] = datetime.utcnow()
        
        # Emit event
        await self.event_bus.publish(
            "orchestration.pipeline.cancelled",
            {
                "pipeline_id": execution["pipeline_id"],
                "execution_id": execution_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Pipeline execution cancelled: {execution_id}")
        return True
    
    async def optimize_pipeline(self, pipeline_id: str, target: str = "balanced") -> Dict[str, Any]:
        """
        Optimize pipeline configuration
        
        Args:
            pipeline_id: Pipeline ID
            target: Optimization target (cost, performance, balanced)
            
        Returns:
            Optimization recommendations
        """
        pipeline = self.pipelines.get(pipeline_id)
        if not pipeline:
            raise ValueError(f"Pipeline not found: {pipeline_id}")
        
        # Analyze pipeline execution history
        execution_stats = await self._analyze_execution_history(pipeline_id)
        
        # Generate optimization recommendations
        recommendations = {
            "target": target,
            "current_performance": execution_stats,
            "recommendations": []
        }
        
        # Batch size optimization
        if execution_stats["avg_records_per_second"] < 1000:
            recommendations["recommendations"].append({
                "type": "batch_size",
                "current": self.config["batch_size"],
                "recommended": min(self.config["batch_size"] * 2, 10000),
                "impact": "Increase throughput by 50-100%"
            })
        
        # Parallelization recommendations
        if execution_stats["avg_processing_time"] > 300:  # 5 minutes
            recommendations["recommendations"].append({
                "type": "parallelization",
                "current": 1,
                "recommended": 4,
                "impact": "Reduce processing time by 60-75%"
            })
        
        # Resource allocation
        if target in ["performance", "balanced"]:
            recommendations["recommendations"].append({
                "type": "resources",
                "current": {"cpu": 2, "memory": "4Gi"},
                "recommended": {"cpu": 4, "memory": "8Gi"},
                "impact": "Improve processing speed by 40-60%"
            })
        
        return recommendations
    
    async def _queue_pipeline_execution(self, execution_id: str):
        """Queue pipeline for execution"""
        # In production, this would use a proper queue (Redis, RabbitMQ, etc.)
        # For now, we'll use an in-memory queue
        await asyncio.sleep(0)  # Yield control
    
    async def _process_pipeline_queue(self):
        """Process queued pipeline executions"""
        while True:
            try:
                # Find queued executions
                queued = [
                    exec_id for exec_id, exec in self.executions.items()
                    if exec["status"] == PipelineStatus.QUEUED
                ]
                
                for execution_id in queued:
                    # Check if we can run more pipelines
                    running_count = sum(1 for exec in self.executions.values() 
                                      if exec["status"] == PipelineStatus.RUNNING)
                    
                    if running_count >= self.config["max_concurrent_pipelines"]:
                        break
                    
                    # Start execution
                    asyncio.create_task(self._execute_pipeline(execution_id))
                
                await asyncio.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.error(f"Error processing pipeline queue: {e}")
                await asyncio.sleep(10)
    
    async def _execute_pipeline(self, execution_id: str):
        """Execute a pipeline"""
        execution = self.executions.get(execution_id)
        if not execution:
            return
        
        pipeline = self.pipelines.get(execution["pipeline_id"])
        
        try:
            # Update status
            execution["status"] = PipelineStatus.RUNNING
            execution["started_at"] = datetime.utcnow()
            
            # Emit start event
            await self.event_bus.publish(
                "orchestration.pipeline.started",
                {
                    "pipeline_id": execution["pipeline_id"],
                    "execution_id": execution_id,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            # Execute pipeline using executor
            result = await self.executor.execute(
                pipeline_config=pipeline["config"],
                dependency_graph=pipeline["dependency_graph"],
                input_data=execution["input_data"],
                execution_id=execution_id,
                callbacks={
                    "on_step_start": lambda step: self._on_step_start(execution_id, step),
                    "on_step_complete": lambda step, result: self._on_step_complete(execution_id, step, result),
                    "on_progress": lambda metrics: self._on_progress(execution_id, metrics)
                }
            )
            
            # Update execution with results
            execution["status"] = PipelineStatus.COMPLETED
            execution["completed_at"] = datetime.utcnow()
            execution["output_data"] = result.get("output")
            execution["metrics"]["records_processed"] = result.get("records_processed", 0)
            
            # Update global metrics
            self.metrics["pipelines_succeeded"] += 1
            self.metrics["total_records_processed"] += execution["metrics"]["records_processed"]
            
            # Update average processing time
            processing_time = (execution["completed_at"] - execution["started_at"]).total_seconds()
            execution["metrics"]["processing_time"] = processing_time
            self._update_avg_processing_time(processing_time)
            
            # Emit completion event
            await self.event_bus.publish(
                "orchestration.pipeline.completed",
                {
                    "pipeline_id": execution["pipeline_id"],
                    "execution_id": execution_id,
                    "metrics": execution["metrics"],
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            logger.info(f"Pipeline execution completed: {execution_id}")
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {execution_id}, error: {e}")
            
            execution["status"] = PipelineStatus.FAILED
            execution["completed_at"] = datetime.utcnow()
            execution["error"] = str(e)
            
            self.metrics["pipelines_failed"] += 1
            
            # Emit failure event
            await self.event_bus.publish(
                "orchestration.pipeline.failed",
                {
                    "pipeline_id": execution["pipeline_id"],
                    "execution_id": execution_id,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
    
    async def _on_step_start(self, execution_id: str, step: Dict[str, Any]):
        """Handle step start"""
        execution = self.executions.get(execution_id)
        if execution:
            execution["current_step"] = step["name"]
    
    async def _on_step_complete(self, execution_id: str, step: Dict[str, Any], result: Any):
        """Handle step completion"""
        execution = self.executions.get(execution_id)
        if execution:
            execution["steps_completed"] += 1
    
    async def _on_progress(self, execution_id: str, metrics: Dict[str, Any]):
        """Handle progress update"""
        execution = self.executions.get(execution_id)
        if execution:
            execution["metrics"].update(metrics)
    
    async def _monitor_pipelines(self):
        """Monitor pipeline executions"""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                # Check for stuck pipelines
                current_time = datetime.utcnow()
                
                for exec_id, execution in self.executions.items():
                    if execution["status"] == PipelineStatus.RUNNING:
                        if execution["started_at"]:
                            elapsed = (current_time - execution["started_at"]).seconds
                            timeout = self.config.get("default_timeout", 3600)
                            
                            if elapsed > timeout:
                                logger.warning(f"Pipeline execution timeout: {exec_id}")
                                await self.cancel_pipeline(exec_id)
                
            except Exception as e:
                logger.error(f"Error monitoring pipelines: {e}")
    
    async def _load_templates(self):
        """Load pipeline templates"""
        # This would load predefined pipeline templates
        # For now, create some example templates
        
        self.templates["etl_template"] = {
            "type": "etl",
            "steps": [
                {"name": "extract", "type": "extract"},
                {"name": "validate", "type": "validate"},
                {"name": "transform", "type": "transform"},
                {"name": "load", "type": "load"}
            ],
            "dependencies": {
                "validate": ["extract"],
                "transform": ["validate"],
                "load": ["transform"]
            }
        }
        
        self.templates["quality_template"] = {
            "type": "data_quality",
            "steps": [
                {"name": "profile", "type": "custom", "config": {"operation": "profile"}},
                {"name": "validate", "type": "validate"},
                {"name": "remediate", "type": "custom", "config": {"operation": "remediate"}},
                {"name": "report", "type": "custom", "config": {"operation": "report"}}
            ],
            "dependencies": {
                "validate": ["profile"],
                "remediate": ["validate"],
                "report": ["remediate"]
            }
        }
    
    async def _analyze_execution_history(self, pipeline_id: str) -> Dict[str, Any]:
        """Analyze pipeline execution history"""
        pipeline = self.pipelines.get(pipeline_id)
        if not pipeline:
            return {}
        
        executions = [
            self.executions.get(exec_id) 
            for exec_id in pipeline["executions"][-10:]  # Last 10 executions
            if self.executions.get(exec_id)
        ]
        
        if not executions:
            return {
                "avg_processing_time": 0,
                "avg_records_per_second": 0,
                "success_rate": 0,
                "avg_retries": 0
            }
        
        # Calculate statistics
        completed_executions = [
            e for e in executions 
            if e["status"] in [PipelineStatus.COMPLETED, PipelineStatus.FAILED]
        ]
        
        if not completed_executions:
            return {
                "avg_processing_time": 0,
                "avg_records_per_second": 0,
                "success_rate": 0,
                "avg_retries": 0
            }
        
        total_time = sum(e["metrics"].get("processing_time", 0) for e in completed_executions)
        total_records = sum(e["metrics"].get("records_processed", 0) for e in completed_executions)
        successful = sum(1 for e in completed_executions if e["status"] == PipelineStatus.COMPLETED)
        total_retries = sum(e["metrics"].get("retries", 0) for e in completed_executions)
        
        avg_time = total_time / len(completed_executions) if completed_executions else 0
        avg_records_per_second = total_records / total_time if total_time > 0 else 0
        
        return {
            "avg_processing_time": avg_time,
            "avg_records_per_second": avg_records_per_second,
            "success_rate": successful / len(completed_executions) if completed_executions else 0,
            "avg_retries": total_retries / len(completed_executions) if completed_executions else 0
        }
    
    def _validate_pipeline_config(self, config: Dict[str, Any]):
        """Validate pipeline configuration"""
        required_fields = ["name", "type", "steps"]
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate pipeline type
        pipeline_type = config["type"]
        if pipeline_type not in [t.value for t in PipelineType]:
            raise ValueError(f"Invalid pipeline type: {pipeline_type}")
        
        # Validate steps
        if not config["steps"]:
            raise ValueError("Pipeline must have at least one step")
        
        for i, step in enumerate(config["steps"]):
            if "name" not in step:
                raise ValueError(f"Step {i} missing name")
            if "type" not in step:
                raise ValueError(f"Step {i} missing type")
    
    def _update_avg_processing_time(self, processing_time: float):
        """Update average processing time metric"""
        total_completed = self.metrics["pipelines_succeeded"] + self.metrics["pipelines_failed"]
        
        if total_completed == 1:
            self.metrics["avg_processing_time"] = processing_time
        else:
            current_avg = self.metrics["avg_processing_time"]
            self.metrics["avg_processing_time"] = (
                (current_avg * (total_completed - 1) + processing_time) / total_completed
            )
    
    async def get_pipeline_metrics(self) -> Dict[str, Any]:
        """Get pipeline manager metrics"""
        return {
            **self.metrics,
            "active_pipelines": len([p for p in self.pipelines.values() if p["status"] == "active"]),
            "total_pipelines": len(self.pipelines),
            "running_executions": sum(1 for exec in self.executions.values() 
                                    if exec["status"] == PipelineStatus.RUNNING),
            "queued_executions": sum(1 for exec in self.executions.values() 
                                   if exec["status"] == PipelineStatus.QUEUED)
        } 