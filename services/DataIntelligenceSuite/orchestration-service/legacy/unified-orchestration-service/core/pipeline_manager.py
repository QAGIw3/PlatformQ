"""
Pipeline Manager for orchestrating data pipelines
"""

import asyncio
import json
import uuid
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path

from platformq_shared.logging import get_logger
from pyignite import AsyncClient
from ..core.config import settings

logger = get_logger(__name__)


class PipelineType(str, Enum):
    """Pipeline types"""
    ETL = "etl"
    TRANSFORMATION = "transformation"
    STREAMING = "streaming"
    ML_TRAINING = "ml_training"
    DATA_QUALITY = "data_quality"
    HYBRID = "hybrid"


class PipelineStatus(str, Enum):
    """Pipeline execution status"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class StepType(str, Enum):
    """Pipeline step types"""
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    QUALITY_CHECK = "quality_check"
    ENRICHMENT = "enrichment"
    VALIDATION = "validation"
    NOTIFICATION = "notification"
    CUSTOM = "custom"


class PipelineManager:
    """Manages pipeline creation, execution, and monitoring"""
    
    def __init__(self):
        self.ignite_client: Optional[AsyncClient] = None
        self.pipelines: Dict[str, Dict[str, Any]] = {}
        self.templates: Dict[str, Dict[str, Any]] = {}
        self.executions: Dict[str, Dict[str, Any]] = {}
        self.resource_pool: Dict[str, Any] = {
            "cpu": settings.pipeline_cpu_limit,
            "memory": settings.pipeline_memory_limit,
            "concurrent": settings.max_concurrent_pipelines
        }
        
    async def initialize(self):
        """Initialize the pipeline manager"""
        logger.info("Initializing pipeline manager")
        
        # Initialize Ignite client
        self.ignite_client = AsyncClient()
        await self.ignite_client.connect(settings.ignite_host, settings.ignite_port)
        
        # Load pipeline templates
        await self._load_templates()
        
        # Start monitoring task
        asyncio.create_task(self._monitor_pipelines())
        
        logger.info("Pipeline manager initialized")
        
    async def cleanup(self):
        """Cleanup resources"""
        if self.ignite_client:
            await self.ignite_client.close()
            
    async def _load_templates(self):
        """Load pipeline templates from disk"""
        template_dir = Path("/config/pipeline-templates")
        if not template_dir.exists():
            logger.warning(f"Template directory {template_dir} not found")
            return
            
        for template_file in template_dir.glob("*.json"):
            try:
                with open(template_file, 'r') as f:
                    template = json.load(f)
                    self.templates[template['name']] = template
                    logger.info(f"Loaded pipeline template: {template['name']}")
            except Exception as e:
                logger.error(f"Failed to load template {template_file}: {e}")
                
    async def create_pipeline(self, 
                            name: str,
                            type: PipelineType,
                            steps: List[Dict[str, Any]],
                            config: Optional[Dict[str, Any]] = None,
                            template: Optional[str] = None,
                            optimization: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a new pipeline"""
        logger.info(f"Creating pipeline: {name}")
        
        pipeline_id = str(uuid.uuid4())
        
        # If using template, merge with provided config
        if template and template in self.templates:
            template_data = self.templates[template].copy()
            if steps:
                template_data['steps'] = steps
            if config:
                template_data['config'] = {**template_data.get('config', {}), **config}
            steps = template_data.get('steps', steps)
            config = template_data.get('config', config)
        
        # Validate pipeline steps
        validated_steps = await self._validate_steps(steps)
        
        # Create dependency graph
        dependency_graph = await self._build_dependency_graph(validated_steps)
        
        # Create pipeline object
        pipeline = {
            "id": pipeline_id,
            "name": name,
            "type": type,
            "steps": validated_steps,
            "config": config or {},
            "optimization": optimization or {},
            "dependency_graph": dependency_graph,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "version": 1,
            "status": PipelineStatus.PENDING
        }
        
        # Store pipeline
        self.pipelines[pipeline_id] = pipeline
        
        # Cache in Ignite
        if self.ignite_client:
            cache = await self.ignite_client.get_or_create_cache(f"pipelines")
            await cache.put(pipeline_id, json.dumps(pipeline))
        
        logger.info(f"Pipeline created: {pipeline_id}")
        return pipeline
        
    async def _validate_steps(self, steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate pipeline steps"""
        validated = []
        
        for i, step in enumerate(steps):
            # Ensure required fields
            if 'type' not in step:
                raise ValueError(f"Step {i} missing 'type' field")
                
            # Add defaults
            validated_step = {
                "id": step.get('id', f"step_{i}"),
                "type": step['type'],
                "name": step.get('name', f"{step['type']}_{i}"),
                "config": step.get('config', {}),
                "dependencies": step.get('dependencies', []),
                "retry": step.get('retry', {"count": 3, "delay": 60}),
                "timeout": step.get('timeout', 3600),
                "resources": step.get('resources', {"cpu": 1, "memory": "1Gi"})
            }
            
            validated.append(validated_step)
            
        return validated
        
    async def _build_dependency_graph(self, steps: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
        """Build dependency graph for pipeline steps"""
        graph = {}
        step_ids = {step['id'] for step in steps}
        
        for step in steps:
            dependencies = set()
            for dep in step.get('dependencies', []):
                if dep in step_ids:
                    dependencies.add(dep)
                else:
                    logger.warning(f"Unknown dependency {dep} for step {step['id']}")
                    
            graph[step['id']] = dependencies
            
        # Check for cycles
        if self._has_cycle(graph):
            raise ValueError("Pipeline contains circular dependencies")
            
        return graph
        
    def _has_cycle(self, graph: Dict[str, Set[str]]) -> bool:
        """Check if dependency graph has cycles"""
        visited = set()
        rec_stack = set()
        
        def has_cycle_util(node):
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in graph.get(node, set()):
                if neighbor not in visited:
                    if has_cycle_util(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
                    
            rec_stack.remove(node)
            return False
            
        for node in graph:
            if node not in visited:
                if has_cycle_util(node):
                    return True
                    
        return False
        
    async def execute_pipeline(self, 
                             pipeline_id: str,
                             context: Optional[Dict[str, Any]] = None,
                             async_execution: bool = True) -> Dict[str, Any]:
        """Execute a pipeline"""
        logger.info(f"Executing pipeline: {pipeline_id}")
        
        if pipeline_id not in self.pipelines:
            raise ValueError(f"Pipeline {pipeline_id} not found")
            
        pipeline = self.pipelines[pipeline_id]
        
        # Check resource availability
        if not await self._check_resources(pipeline):
            raise RuntimeError("Insufficient resources to execute pipeline")
            
        # Create execution record
        execution_id = str(uuid.uuid4())
        execution = {
            "id": execution_id,
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline['name'],
            "status": PipelineStatus.RUNNING,
            "context": context or {},
            "started_at": datetime.utcnow().isoformat(),
            "steps_completed": [],
            "steps_failed": [],
            "current_step": None,
            "logs": []
        }
        
        self.executions[execution_id] = execution
        
        # Execute pipeline
        if async_execution:
            asyncio.create_task(self._execute_pipeline_async(execution_id, pipeline, context))
        else:
            await self._execute_pipeline_async(execution_id, pipeline, context)
            
        return execution
        
    async def _check_resources(self, pipeline: Dict[str, Any]) -> bool:
        """Check if resources are available for pipeline"""
        # Simple resource check - can be enhanced
        active_pipelines = sum(1 for e in self.executions.values() 
                              if e['status'] == PipelineStatus.RUNNING)
        
        return active_pipelines < self.resource_pool['concurrent']
        
    async def _execute_pipeline_async(self, 
                                    execution_id: str,
                                    pipeline: Dict[str, Any],
                                    context: Optional[Dict[str, Any]]):
        """Execute pipeline asynchronously"""
        execution = self.executions[execution_id]
        
        try:
            # Get execution order based on dependencies
            execution_order = self._topological_sort(pipeline['dependency_graph'])
            
            # Execute steps in order
            for step_id in execution_order:
                step = next(s for s in pipeline['steps'] if s['id'] == step_id)
                
                execution['current_step'] = step_id
                
                # Execute step
                success = await self._execute_step(step, context, execution)
                
                if success:
                    execution['steps_completed'].append(step_id)
                else:
                    execution['steps_failed'].append(step_id)
                    
                    # Check if should continue on failure
                    if not pipeline.get('config', {}).get('continue_on_failure', False):
                        raise RuntimeError(f"Step {step_id} failed")
                        
            # Pipeline completed successfully
            execution['status'] = PipelineStatus.SUCCESS
            execution['completed_at'] = datetime.utcnow().isoformat()
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            execution['status'] = PipelineStatus.FAILED
            execution['error'] = str(e)
            execution['completed_at'] = datetime.utcnow().isoformat()
            
        finally:
            # Update cache
            if self.ignite_client:
                cache = await self.ignite_client.get_or_create_cache("executions")
                await cache.put(execution_id, json.dumps(execution))
                
    def _topological_sort(self, graph: Dict[str, Set[str]]) -> List[str]:
        """Topological sort of dependency graph"""
        in_degree = {node: 0 for node in graph}
        
        for node in graph:
            for dep in graph[node]:
                in_degree[dep] = in_degree.get(dep, 0) + 1
                
        queue = [node for node, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            node = queue.pop(0)
            result.append(node)
            
            for neighbor in [n for n, deps in graph.items() if node in deps]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
                    
        return result
        
    async def _execute_step(self, 
                          step: Dict[str, Any],
                          context: Optional[Dict[str, Any]],
                          execution: Dict[str, Any]) -> bool:
        """Execute a single pipeline step"""
        logger.info(f"Executing step: {step['id']}")
        
        try:
            # Log step start
            execution['logs'].append({
                "timestamp": datetime.utcnow().isoformat(),
                "step": step['id'],
                "message": f"Starting step {step['name']}",
                "level": "INFO"
            })
            
            # Execute based on step type
            if step['type'] == StepType.EXTRACT:
                await self._execute_extract_step(step, context)
            elif step['type'] == StepType.TRANSFORM:
                await self._execute_transform_step(step, context)
            elif step['type'] == StepType.LOAD:
                await self._execute_load_step(step, context)
            elif step['type'] == StepType.QUALITY_CHECK:
                await self._execute_quality_step(step, context)
            else:
                # Custom step execution
                logger.info(f"Executing custom step: {step['type']}")
                
            # Log step completion
            execution['logs'].append({
                "timestamp": datetime.utcnow().isoformat(),
                "step": step['id'],
                "message": f"Completed step {step['name']}",
                "level": "INFO"
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Step {step['id']} failed: {e}")
            
            # Log failure
            execution['logs'].append({
                "timestamp": datetime.utcnow().isoformat(),
                "step": step['id'],
                "message": f"Step {step['name']} failed: {str(e)}",
                "level": "ERROR"
            })
            
            # Handle retries
            retry_config = step.get('retry', {})
            if retry_config.get('count', 0) > 0:
                # Implement retry logic
                logger.info(f"Retrying step {step['id']}")
                # Simplified - real implementation would track retry attempts
                
            return False
            
    async def _execute_extract_step(self, step: Dict[str, Any], context: Optional[Dict[str, Any]]):
        """Execute data extraction step"""
        config = step['config']
        source_type = config.get('source_type')
        
        logger.info(f"Extracting data from {source_type}")
        # Placeholder - integrate with actual data sources
        await asyncio.sleep(1)  # Simulate extraction
        
    async def _execute_transform_step(self, step: Dict[str, Any], context: Optional[Dict[str, Any]]):
        """Execute data transformation step"""
        config = step['config']
        operations = config.get('operations', [])
        
        logger.info(f"Applying transformations: {operations}")
        # Placeholder - integrate with actual transformation logic
        await asyncio.sleep(1)  # Simulate transformation
        
    async def _execute_load_step(self, step: Dict[str, Any], context: Optional[Dict[str, Any]]):
        """Execute data loading step"""
        config = step['config']
        target_type = config.get('target_type')
        
        logger.info(f"Loading data to {target_type}")
        # Placeholder - integrate with actual data targets
        await asyncio.sleep(1)  # Simulate loading
        
    async def _execute_quality_step(self, step: Dict[str, Any], context: Optional[Dict[str, Any]]):
        """Execute data quality check step"""
        config = step['config']
        rules = config.get('rules', [])
        
        logger.info(f"Running quality checks: {rules}")
        # Placeholder - integrate with quality service
        await asyncio.sleep(1)  # Simulate quality check
        
    async def get_pipeline(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """Get pipeline by ID"""
        return self.pipelines.get(pipeline_id)
        
    async def list_pipelines(self, 
                           type: Optional[PipelineType] = None,
                           status: Optional[PipelineStatus] = None) -> List[Dict[str, Any]]:
        """List pipelines with optional filtering"""
        pipelines = list(self.pipelines.values())
        
        if type:
            pipelines = [p for p in pipelines if p['type'] == type]
            
        if status:
            pipelines = [p for p in pipelines if p.get('status') == status]
            
        return pipelines
        
    async def get_execution(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Get execution by ID"""
        return self.executions.get(execution_id)
        
    async def cancel_execution(self, execution_id: str) -> bool:
        """Cancel a running execution"""
        if execution_id not in self.executions:
            return False
            
        execution = self.executions[execution_id]
        if execution['status'] == PipelineStatus.RUNNING:
            execution['status'] = PipelineStatus.CANCELLED
            execution['completed_at'] = datetime.utcnow().isoformat()
            return True
            
        return False
        
    async def _monitor_pipelines(self):
        """Monitor pipeline executions"""
        while True:
            try:
                # Check for timed out executions
                for execution in self.executions.values():
                    if execution['status'] == PipelineStatus.RUNNING:
                        started = datetime.fromisoformat(execution['started_at'])
                        if (datetime.utcnow() - started).total_seconds() > settings.pipeline_timeout:
                            logger.warning(f"Pipeline execution {execution['id']} timed out")
                            execution['status'] = PipelineStatus.FAILED
                            execution['error'] = "Execution timed out"
                            execution['completed_at'] = datetime.utcnow().isoformat()
                            
                # Clean up old executions
                cutoff = datetime.utcnow() - timedelta(days=7)
                to_remove = []
                for exec_id, execution in self.executions.items():
                    if 'completed_at' in execution:
                        completed = datetime.fromisoformat(execution['completed_at'])
                        if completed < cutoff:
                            to_remove.append(exec_id)
                            
                for exec_id in to_remove:
                    del self.executions[exec_id]
                    
            except Exception as e:
                logger.error(f"Pipeline monitoring error: {e}")
                
            await asyncio.sleep(60)  # Check every minute 