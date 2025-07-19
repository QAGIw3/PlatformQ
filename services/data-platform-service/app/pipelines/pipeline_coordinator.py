"""
Comprehensive Pipeline Coordinator for data platform
"""
import asyncio
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from enum import Enum
import json

from platformq_shared.utils.logger import get_logger
from platformq_shared.errors import ValidationError, ServiceError, NotFoundError
from ..lake.ingestion_engine import DataIngestionEngine, IngestionMode
from ..lake.transformation_engine import TransformationEngine
from ..quality.profiler import DataQualityProfiler
from ..lineage.lineage_tracker import DataLineageTracker, LineageNodeType
from .seatunnel_manager import SeaTunnelPipelineManager, PipelineConfig
from ..core.cache_manager import DataCacheManager

logger = get_logger(__name__)


class PipelineType(str, Enum):
    """Types of data pipelines"""
    INGESTION = "ingestion"
    TRANSFORMATION = "transformation"
    QUALITY = "quality"
    SEATUNNEL = "seatunnel"
    COMPOSITE = "composite"


class PipelineSchedule(str, Enum):
    """Pipeline scheduling options"""
    ONCE = "once"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    CRON = "cron"


class PipelineCoordinator:
    """
    Coordinates all pipeline activities in the data platform.
    
    Features:
    - Unified pipeline management
    - Pipeline orchestration
    - Dependency management
    - Scheduling and triggers
    - Pipeline monitoring
    - Error handling and recovery
    - Pipeline templates
    """
    
    def __init__(self,
                 ingestion_engine: DataIngestionEngine,
                 transformation_engine: TransformationEngine,
                 quality_profiler: DataQualityProfiler,
                 seatunnel_manager: SeaTunnelPipelineManager,
                 lineage_tracker: DataLineageTracker,
                 cache_manager: DataCacheManager):
        self.ingestion_engine = ingestion_engine
        self.transformation_engine = transformation_engine
        self.quality_profiler = quality_profiler
        self.seatunnel_manager = seatunnel_manager
        self.lineage_tracker = lineage_tracker
        self.cache = cache_manager
        
        # Pipeline registry
        self.pipelines: Dict[str, Dict[str, Any]] = {}
        
        # Pipeline dependencies
        self.dependencies: Dict[str, Set[str]] = {}
        
        # Scheduled pipelines
        self.scheduled_tasks: Dict[str, asyncio.Task] = {}
        
        # Pipeline templates
        self._load_pipeline_templates()
        
        # Statistics
        self.stats = {
            "total_pipelines": 0,
            "active_pipelines": 0,
            "scheduled_pipelines": 0,
            "executed_pipelines": 0,
            "failed_pipelines": 0
        }
    
    def _load_pipeline_templates(self) -> None:
        """Load pipeline templates"""
        self.templates = {
            "bronze_to_silver": {
                "type": PipelineType.COMPOSITE,
                "description": "Standard Bronze to Silver transformation",
                "steps": [
                    {
                        "name": "quality_check",
                        "type": PipelineType.QUALITY,
                        "config": {
                            "sample_size": 10000
                        }
                    },
                    {
                        "name": "transform",
                        "type": PipelineType.TRANSFORMATION,
                        "config": {
                            "target_zone": "silver",
                            "quality_rules": [
                                {"type": "not_null", "columns": ["id"]},
                                {"type": "dedup", "columns": ["id"]}
                            ],
                            "transformations": [
                                {"type": "trim_strings"},
                                {"type": "standardize_dates"}
                            ]
                        }
                    }
                ]
            },
            "silver_to_gold": {
                "type": PipelineType.COMPOSITE,
                "description": "Standard Silver to Gold aggregation",
                "steps": [
                    {
                        "name": "aggregate",
                        "type": PipelineType.TRANSFORMATION,
                        "config": {
                            "target_zone": "gold",
                            "aggregations": [
                                {
                                    "type": "time_series_aggregation",
                                    "window": "1 day",
                                    "group_by": ["category"],
                                    "aggregations": {
                                        "amount": ["sum", "avg"],
                                        "count": ["count"]
                                    }
                                }
                            ]
                        }
                    }
                ]
            },
            "cdc_pipeline": {
                "type": PipelineType.INGESTION,
                "description": "Change Data Capture pipeline",
                "config": {
                    "mode": "cdc",
                    "poll_interval": 60
                }
            }
        }
    
    async def create_pipeline(self,
                            name: str,
                            pipeline_type: PipelineType,
                            config: Dict[str, Any],
                            tenant_id: str,
                            schedule: Optional[PipelineSchedule] = None,
                            dependencies: Optional[List[str]] = None) -> Dict[str, Any]:
        """Create a new pipeline"""
        try:
            pipeline_id = f"{pipeline_type.value}_{name}_{datetime.utcnow().timestamp()}"
            
            # Validate dependencies
            if dependencies:
                for dep_id in dependencies:
                    if dep_id not in self.pipelines:
                        raise ValidationError(f"Dependency not found: {dep_id}")
            
            # Create pipeline record
            pipeline = {
                "pipeline_id": pipeline_id,
                "name": name,
                "type": pipeline_type,
                "config": config,
                "tenant_id": tenant_id,
                "schedule": schedule,
                "dependencies": dependencies or [],
                "status": "created",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
                "execution_history": []
            }
            
            # Store pipeline
            self.pipelines[pipeline_id] = pipeline
            
            # Store dependencies
            if dependencies:
                self.dependencies[pipeline_id] = set(dependencies)
            
            # Register in lineage
            await self._register_pipeline_lineage(pipeline_id, pipeline_type, name, tenant_id)
            
            # Schedule if needed
            if schedule and schedule != PipelineSchedule.ONCE:
                await self._schedule_pipeline(pipeline_id, schedule, config.get("schedule_config"))
            
            # Update statistics
            self.stats["total_pipelines"] += 1
            
            logger.info(f"Created pipeline: {pipeline_id}")
            
            return pipeline
            
        except Exception as e:
            logger.error(f"Failed to create pipeline: {e}")
            raise
    
    async def create_from_template(self,
                                 name: str,
                                 template_name: str,
                                 tenant_id: str,
                                 overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create pipeline from template"""
        if template_name not in self.templates:
            raise ValidationError(f"Template not found: {template_name}")
        
        template = self.templates[template_name].copy()
        
        # Apply overrides
        if overrides:
            self._deep_merge(template, overrides)
        
        # Extract type and config
        pipeline_type = PipelineType(template["type"])
        config = template.get("config", template)
        
        return await self.create_pipeline(
            name=name,
            pipeline_type=pipeline_type,
            config=config,
            tenant_id=tenant_id,
            schedule=template.get("schedule"),
            dependencies=template.get("dependencies")
        )
    
    async def execute_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Execute a pipeline"""
        try:
            if pipeline_id not in self.pipelines:
                raise NotFoundError(f"Pipeline not found: {pipeline_id}")
            
            pipeline = self.pipelines[pipeline_id]
            
            # Check dependencies
            if not await self._check_dependencies(pipeline_id):
                raise ValidationError("Pipeline dependencies not satisfied")
            
            # Update status
            pipeline["status"] = "running"
            pipeline["last_run_at"] = datetime.utcnow()
            
            # Execute based on type
            result = await self._execute_by_type(pipeline)
            
            # Update execution history
            execution_record = {
                "execution_id": f"exec_{datetime.utcnow().timestamp()}",
                "started_at": pipeline["last_run_at"],
                "completed_at": datetime.utcnow(),
                "status": result.get("status", "completed"),
                "result": result
            }
            
            pipeline["execution_history"].append(execution_record)
            
            # Keep only last 100 executions
            if len(pipeline["execution_history"]) > 100:
                pipeline["execution_history"] = pipeline["execution_history"][-100:]
            
            # Update status
            pipeline["status"] = result.get("status", "completed")
            
            # Update statistics
            self.stats["executed_pipelines"] += 1
            if result.get("status") == "failed":
                self.stats["failed_pipelines"] += 1
            
            logger.info(f"Executed pipeline {pipeline_id} with status {pipeline['status']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to execute pipeline {pipeline_id}: {e}")
            
            # Update pipeline status
            if pipeline_id in self.pipelines:
                self.pipelines[pipeline_id]["status"] = "failed"
                self.pipelines[pipeline_id]["last_error"] = str(e)
                self.stats["failed_pipelines"] += 1
            
            raise
    
    async def _execute_by_type(self, pipeline: Dict[str, Any]) -> Dict[str, Any]:
        """Execute pipeline based on its type"""
        pipeline_type = pipeline["type"]
        config = pipeline["config"]
        tenant_id = pipeline["tenant_id"]
        
        if pipeline_type == PipelineType.INGESTION:
            # Execute ingestion pipeline
            result = await self.ingestion_engine.ingest_data(
                dataset_id=config.get("dataset_id", pipeline["name"]),
                source_config=config.get("source_config", config),
                tenant_id=tenant_id,
                mode=IngestionMode(config.get("mode", "batch"))
            )
            
        elif pipeline_type == PipelineType.TRANSFORMATION:
            # Execute transformation pipeline
            if config.get("target_zone") == "silver":
                result = await self.transformation_engine.transform_bronze_to_silver(
                    bronze_dataset=config["source_dataset"],
                    silver_dataset=config.get("target_dataset", config["source_dataset"]),
                    tenant_id=tenant_id,
                    transformation_config=config
                )
            else:  # Gold
                result = await self.transformation_engine.transform_silver_to_gold(
                    silver_datasets=config.get("source_datasets", [config["source_dataset"]]),
                    gold_dataset=config["target_dataset"],
                    tenant_id=tenant_id,
                    aggregation_config=config
                )
                
        elif pipeline_type == PipelineType.QUALITY:
            # Execute quality pipeline
            # This would need access to the actual DataFrame
            # For now, return placeholder
            result = {
                "status": "completed",
                "message": "Quality profiling requires DataFrame access"
            }
            
        elif pipeline_type == PipelineType.SEATUNNEL:
            # Execute SeaTunnel pipeline
            seatunnel_config = PipelineConfig(**config)
            created_pipeline = await self.seatunnel_manager.create_pipeline(
                config=seatunnel_config,
                tenant_id=tenant_id
            )
            result = await self.seatunnel_manager.run_pipeline(
                created_pipeline["pipeline_id"]
            )
            
        elif pipeline_type == PipelineType.COMPOSITE:
            # Execute composite pipeline (multiple steps)
            result = await self._execute_composite_pipeline(pipeline)
            
        else:
            raise ValueError(f"Unknown pipeline type: {pipeline_type}")
        
        return result
    
    async def _execute_composite_pipeline(self, pipeline: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a composite pipeline with multiple steps"""
        steps = pipeline["config"].get("steps", [])
        results = []
        
        for i, step in enumerate(steps):
            try:
                logger.info(f"Executing step {i+1}/{len(steps)}: {step['name']}")
                
                # Create temporary pipeline for step
                step_pipeline = {
                    "name": f"{pipeline['name']}_{step['name']}",
                    "type": step["type"],
                    "config": step["config"],
                    "tenant_id": pipeline["tenant_id"]
                }
                
                # Execute step
                step_result = await self._execute_by_type(step_pipeline)
                results.append({
                    "step": step["name"],
                    "status": "completed",
                    "result": step_result
                })
                
            except Exception as e:
                logger.error(f"Step {step['name']} failed: {e}")
                results.append({
                    "step": step["name"],
                    "status": "failed",
                    "error": str(e)
                })
                
                # Stop on failure unless configured otherwise
                if not step.get("continue_on_failure", False):
                    break
        
        # Determine overall status
        failed_steps = [r for r in results if r["status"] == "failed"]
        
        return {
            "status": "failed" if failed_steps else "completed",
            "steps_executed": len(results),
            "steps_failed": len(failed_steps),
            "results": results
        }
    
    async def _check_dependencies(self, pipeline_id: str) -> bool:
        """Check if pipeline dependencies are satisfied"""
        if pipeline_id not in self.dependencies:
            return True
        
        deps = self.dependencies[pipeline_id]
        
        for dep_id in deps:
            if dep_id not in self.pipelines:
                return False
            
            dep_pipeline = self.pipelines[dep_id]
            
            # Check if dependency was executed successfully
            if not dep_pipeline.get("execution_history"):
                return False
            
            last_execution = dep_pipeline["execution_history"][-1]
            if last_execution.get("status") != "completed":
                return False
            
            # Check if execution is recent enough (within 24 hours)
            last_run = datetime.fromisoformat(last_execution["completed_at"])
            if datetime.utcnow() - last_run > timedelta(hours=24):
                return False
        
        return True
    
    async def _schedule_pipeline(self,
                               pipeline_id: str,
                               schedule: PipelineSchedule,
                               schedule_config: Optional[Dict[str, Any]] = None) -> None:
        """Schedule pipeline execution"""
        try:
            # Cancel existing schedule if any
            if pipeline_id in self.scheduled_tasks:
                self.scheduled_tasks[pipeline_id].cancel()
            
            # Create scheduled task
            if schedule == PipelineSchedule.HOURLY:
                interval = 3600  # 1 hour
            elif schedule == PipelineSchedule.DAILY:
                interval = 86400  # 24 hours
            elif schedule == PipelineSchedule.WEEKLY:
                interval = 604800  # 7 days
            elif schedule == PipelineSchedule.MONTHLY:
                interval = 2592000  # 30 days
            elif schedule == PipelineSchedule.CRON:
                # Would implement cron scheduling
                # For now, use daily as default
                interval = 86400
            else:
                return
            
            # Create background task
            task = asyncio.create_task(
                self._scheduled_execution_loop(pipeline_id, interval)
            )
            
            self.scheduled_tasks[pipeline_id] = task
            self.stats["scheduled_pipelines"] += 1
            
            logger.info(f"Scheduled pipeline {pipeline_id} with {schedule} schedule")
            
        except Exception as e:
            logger.error(f"Failed to schedule pipeline: {e}")
            raise
    
    async def _scheduled_execution_loop(self, pipeline_id: str, interval: int) -> None:
        """Background task for scheduled pipeline execution"""
        while True:
            try:
                # Wait for next execution
                await asyncio.sleep(interval)
                
                # Check if pipeline still exists
                if pipeline_id not in self.pipelines:
                    break
                
                # Execute pipeline
                logger.info(f"Executing scheduled pipeline {pipeline_id}")
                await self.execute_pipeline(pipeline_id)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Scheduled execution failed for {pipeline_id}: {e}")
                # Continue with next scheduled execution
    
    async def pause_pipeline(self, pipeline_id: str) -> None:
        """Pause a scheduled pipeline"""
        if pipeline_id in self.scheduled_tasks:
            self.scheduled_tasks[pipeline_id].cancel()
            del self.scheduled_tasks[pipeline_id]
            
            if pipeline_id in self.pipelines:
                self.pipelines[pipeline_id]["status"] = "paused"
            
            self.stats["scheduled_pipelines"] -= 1
            
            logger.info(f"Paused pipeline {pipeline_id}")
    
    async def resume_pipeline(self, pipeline_id: str) -> None:
        """Resume a paused pipeline"""
        if pipeline_id not in self.pipelines:
            raise NotFoundError(f"Pipeline not found: {pipeline_id}")
        
        pipeline = self.pipelines[pipeline_id]
        
        if pipeline.get("schedule"):
            await self._schedule_pipeline(
                pipeline_id,
                pipeline["schedule"],
                pipeline["config"].get("schedule_config")
            )
            
            pipeline["status"] = "scheduled"
            
            logger.info(f"Resumed pipeline {pipeline_id}")
    
    async def get_pipeline_status(self, pipeline_id: str) -> Dict[str, Any]:
        """Get pipeline status and details"""
        if pipeline_id not in self.pipelines:
            raise NotFoundError(f"Pipeline not found: {pipeline_id}")
        
        pipeline = self.pipelines[pipeline_id]
        
        # Get latest execution info
        latest_execution = None
        if pipeline["execution_history"]:
            latest_execution = pipeline["execution_history"][-1]
        
        return {
            "pipeline_id": pipeline_id,
            "name": pipeline["name"],
            "type": pipeline["type"],
            "status": pipeline["status"],
            "created_at": pipeline["created_at"],
            "updated_at": pipeline["updated_at"],
            "last_run_at": pipeline.get("last_run_at"),
            "schedule": pipeline.get("schedule"),
            "dependencies": pipeline.get("dependencies", []),
            "latest_execution": latest_execution,
            "execution_count": len(pipeline["execution_history"]),
            "is_scheduled": pipeline_id in self.scheduled_tasks
        }
    
    async def list_pipelines(self,
                           tenant_id: str,
                           pipeline_type: Optional[PipelineType] = None,
                           status: Optional[str] = None) -> List[Dict[str, Any]]:
        """List pipelines with filters"""
        pipelines = []
        
        for pipeline_id, pipeline in self.pipelines.items():
            if pipeline["tenant_id"] != tenant_id:
                continue
            
            if pipeline_type and pipeline["type"] != pipeline_type:
                continue
            
            if status and pipeline["status"] != status:
                continue
            
            pipelines.append({
                "pipeline_id": pipeline_id,
                "name": pipeline["name"],
                "type": pipeline["type"],
                "status": pipeline["status"],
                "created_at": pipeline["created_at"],
                "last_run_at": pipeline.get("last_run_at"),
                "schedule": pipeline.get("schedule"),
                "is_scheduled": pipeline_id in self.scheduled_tasks
            })
        
        return pipelines
    
    async def get_pipeline_lineage(self, pipeline_id: str) -> Dict[str, Any]:
        """Get pipeline lineage information"""
        if pipeline_id not in self.pipelines:
            raise NotFoundError(f"Pipeline not found: {pipeline_id}")
        
        # Get upstream and downstream lineage
        upstream = await self.lineage_tracker.get_upstream_lineage(pipeline_id)
        downstream = await self.lineage_tracker.get_downstream_lineage(pipeline_id)
        
        return {
            "pipeline_id": pipeline_id,
            "upstream": upstream,
            "downstream": downstream
        }
    
    async def delete_pipeline(self, pipeline_id: str) -> None:
        """Delete a pipeline"""
        if pipeline_id not in self.pipelines:
            raise NotFoundError(f"Pipeline not found: {pipeline_id}")
        
        # Cancel scheduled task if any
        if pipeline_id in self.scheduled_tasks:
            self.scheduled_tasks[pipeline_id].cancel()
            del self.scheduled_tasks[pipeline_id]
            self.stats["scheduled_pipelines"] -= 1
        
        # Remove from dependencies
        self.dependencies.pop(pipeline_id, None)
        
        # Remove pipeline
        del self.pipelines[pipeline_id]
        
        # Update statistics
        self.stats["total_pipelines"] -= 1
        
        logger.info(f"Deleted pipeline {pipeline_id}")
    
    async def _register_pipeline_lineage(self,
                                       pipeline_id: str,
                                       pipeline_type: PipelineType,
                                       name: str,
                                       tenant_id: str) -> None:
        """Register pipeline in lineage system"""
        try:
            await self.lineage_tracker.add_node(
                node_id=pipeline_id,
                node_type=LineageNodeType.PROCESS,
                name=name,
                tenant_id=tenant_id,
                metadata={
                    "pipeline_type": pipeline_type.value,
                    "created_at": datetime.utcnow().isoformat()
                }
            )
        except Exception as e:
            logger.error(f"Failed to register pipeline lineage: {e}")
    
    def _deep_merge(self, base: Dict, override: Dict) -> None:
        """Deep merge override into base dictionary"""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get coordinator statistics"""
        return {
            **self.stats,
            "active_scheduled_tasks": len(self.scheduled_tasks)
        }
    
    async def shutdown(self) -> None:
        """Shutdown pipeline coordinator"""
        # Cancel all scheduled tasks
        for task in self.scheduled_tasks.values():
            task.cancel()
        
        # Wait for tasks to complete
        if self.scheduled_tasks:
            await asyncio.gather(
                *self.scheduled_tasks.values(),
                return_exceptions=True
            )
        
        logger.info("Pipeline coordinator shut down") 