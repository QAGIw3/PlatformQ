"""
Cognitive Orchestrator

Main orchestration engine that learns from system behavior and optimizes workflows
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from dataclasses import dataclass
from enum import Enum
import structlog

from app.core.ml_optimizer import MLOptimizer, OptimizationResult
from app.core.system_monitor import SystemMonitor, SystemMetrics
from app.integrations.data_platform import DataPlatformClient
from app.integrations.ml_platform import MLPlatformClient
from app.core.config import Settings

logger = structlog.get_logger()


class WorkflowStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    OPTIMIZING = "optimizing"


@dataclass
class WorkflowConfig:
    """Workflow configuration"""
    workflow_id: str
    name: str
    steps: List[Dict[str, Any]]
    constraints: Dict[str, Any]
    priority: int = 1
    retry_policy: Optional[Dict[str, Any]] = None
    
    
@dataclass
class WorkflowExecution:
    """Workflow execution record"""
    execution_id: str
    workflow_id: str
    status: WorkflowStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    metrics: Optional[Dict[str, float]] = None
    optimizations_applied: Optional[List[str]] = None
    error: Optional[str] = None


class CognitiveOrchestrator:
    """
    Self-learning orchestrator that optimizes workflows based on:
    - Historical performance patterns
    - Resource utilization
    - Business objectives
    - Real-time system state
    """
    
    def __init__(self,
                 ml_optimizer: MLOptimizer,
                 system_monitor: SystemMonitor,
                 data_platform: DataPlatformClient,
                 ml_platform: MLPlatformClient,
                 settings: Settings):
        self.ml_optimizer = ml_optimizer
        self.system_monitor = system_monitor
        self.data_platform = data_platform
        self.ml_platform = ml_platform
        self.settings = settings
        
        # Workflow management
        self.active_workflows: Dict[str, WorkflowExecution] = {}
        self.workflow_history: List[WorkflowExecution] = []
        self.optimization_cache: Dict[str, OptimizationResult] = {}
        
        # Learning state
        self.performance_model = None
        self.resource_predictor = None
        self.anomaly_detector = None
        
        # Background tasks
        self._tasks: List[asyncio.Task] = []
        self._running = False
        
    async def start(self):
        """Start the orchestrator"""
        self._running = True
        
        # Initialize ML models
        await self._initialize_models()
        
        # Start background tasks
        self._tasks.append(
            asyncio.create_task(self._optimization_loop())
        )
        self._tasks.append(
            asyncio.create_task(self._learning_loop())
        )
        self._tasks.append(
            asyncio.create_task(self._monitoring_loop())
        )
        
        logger.info("Cognitive orchestrator started")
        
    async def stop(self):
        """Stop the orchestrator"""
        self._running = False
        
        # Cancel background tasks
        for task in self._tasks:
            task.cancel()
            
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        logger.info("Cognitive orchestrator stopped")
        
    async def auto_optimize_pipeline(self, 
                                   pipeline_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Automatically optimize a pipeline configuration based on learned patterns
        
        Args:
            pipeline_config: Original pipeline configuration
            
        Returns:
            Optimized pipeline configuration
        """
        try:
            # Analyze historical executions
            performance_history = await self._analyze_pipeline_history(
                pipeline_config.get("name", "unknown")
            )
            
            # Get current system state
            system_state = await self.system_monitor.get_current_metrics()
            
            # Use ML to predict optimal configuration
            optimal_config = await self.ml_optimizer.predict_optimal_config(
                pipeline_config=pipeline_config,
                historical_performance=performance_history,
                current_system_state=system_state,
                business_constraints=self._get_business_rules()
            )
            
            # Apply optimizations
            optimized_pipeline = await self._apply_optimizations(
                pipeline_config,
                optimal_config
            )
            
            # Cache optimization result
            self.optimization_cache[pipeline_config.get("name")] = optimal_config
            
            logger.info(
                "Pipeline optimized",
                pipeline_name=pipeline_config.get("name"),
                optimizations=optimal_config.optimizations
            )
            
            return optimized_pipeline
            
        except Exception as e:
            logger.error(f"Failed to optimize pipeline: {e}")
            return pipeline_config  # Return original on failure
            
    async def execute_workflow(self,
                             workflow_config: WorkflowConfig) -> WorkflowExecution:
        """
        Execute a workflow with cognitive optimization
        
        Args:
            workflow_config: Workflow configuration
            
        Returns:
            Workflow execution result
        """
        execution_id = f"{workflow_config.workflow_id}_{datetime.utcnow().timestamp()}"
        
        execution = WorkflowExecution(
            execution_id=execution_id,
            workflow_id=workflow_config.workflow_id,
            status=WorkflowStatus.PENDING,
            start_time=datetime.utcnow()
        )
        
        self.active_workflows[execution_id] = execution
        
        try:
            # Optimize workflow before execution
            execution.status = WorkflowStatus.OPTIMIZING
            optimized_config = await self._optimize_workflow(workflow_config)
            
            # Execute workflow
            execution.status = WorkflowStatus.RUNNING
            result = await self._execute_workflow_steps(
                optimized_config,
                execution
            )
            
            # Record completion
            execution.status = WorkflowStatus.COMPLETED
            execution.end_time = datetime.utcnow()
            execution.metrics = result.get("metrics", {})
            
        except Exception as e:
            logger.error(f"Workflow execution failed: {e}")
            execution.status = WorkflowStatus.FAILED
            execution.error = str(e)
            execution.end_time = datetime.utcnow()
            
        finally:
            # Move to history
            self.workflow_history.append(execution)
            del self.active_workflows[execution_id]
            
            # Learn from execution
            await self._learn_from_execution(execution)
            
        return execution
        
    async def predict_resource_needs(self,
                                   workflow_config: WorkflowConfig,
                                   time_horizon: int = 3600) -> Dict[str, Any]:
        """
        Predict resource needs for a workflow
        
        Args:
            workflow_config: Workflow configuration
            time_horizon: Prediction horizon in seconds
            
        Returns:
            Resource predictions
        """
        try:
            # Get historical resource usage
            history = await self._get_workflow_resource_history(
                workflow_config.workflow_id
            )
            
            # Use ML to predict resources
            predictions = await self.ml_optimizer.predict_resources(
                workflow_config=workflow_config,
                historical_usage=history,
                time_horizon=time_horizon
            )
            
            # Add buffer based on uncertainty
            buffered_predictions = self._add_resource_buffer(predictions)
            
            return {
                "predictions": buffered_predictions,
                "confidence": predictions.confidence,
                "time_horizon": time_horizon
            }
            
        except Exception as e:
            logger.error(f"Failed to predict resources: {e}")
            return self._get_default_resources(workflow_config)
            
    async def get_optimization_recommendations(self,
                                             workflow_id: str) -> List[Dict[str, Any]]:
        """
        Get optimization recommendations for a workflow
        
        Args:
            workflow_id: Workflow identifier
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        try:
            # Analyze workflow patterns
            patterns = await self._analyze_workflow_patterns(workflow_id)
            
            # Check for optimization opportunities
            if patterns.get("parallelization_opportunity", 0) > 0.3:
                recommendations.append({
                    "type": "parallelization",
                    "description": "Increase parallelization for better performance",
                    "expected_improvement": patterns["parallelization_opportunity"],
                    "config_changes": {
                        "parallelism": patterns["optimal_parallelism"]
                    }
                })
                
            if patterns.get("caching_opportunity", 0) > 0.2:
                recommendations.append({
                    "type": "caching",
                    "description": "Add caching to reduce redundant computations",
                    "expected_improvement": patterns["caching_opportunity"],
                    "config_changes": {
                        "enable_caching": True,
                        "cache_ttl": patterns["optimal_cache_ttl"]
                    }
                })
                
            if patterns.get("resource_waste", 0) > 0.15:
                recommendations.append({
                    "type": "resource_optimization",
                    "description": "Reduce resource allocation to save costs",
                    "expected_savings": patterns["resource_waste"],
                    "config_changes": {
                        "cpu": patterns["optimal_cpu"],
                        "memory": patterns["optimal_memory"]
                    }
                })
                
        except Exception as e:
            logger.error(f"Failed to generate recommendations: {e}")
            
        return recommendations
        
    # Private methods
    
    async def _initialize_models(self):
        """Initialize ML models for optimization"""
        # Load or train performance prediction model
        self.performance_model = await self.ml_optimizer.load_performance_model()
        
        # Load or train resource prediction model
        self.resource_predictor = await self.ml_optimizer.load_resource_predictor()
        
        # Initialize anomaly detector
        self.anomaly_detector = await self.ml_optimizer.initialize_anomaly_detector()
        
    async def _optimization_loop(self):
        """Background loop for continuous optimization"""
        while self._running:
            try:
                # Check active workflows for optimization opportunities
                for execution_id, execution in self.active_workflows.items():
                    if execution.status == WorkflowStatus.RUNNING:
                        # Check if optimization is needed
                        if await self._needs_optimization(execution):
                            await self._apply_runtime_optimization(execution)
                            
                await asyncio.sleep(self.settings.optimization_interval_seconds)
                
            except Exception as e:
                logger.error(f"Optimization loop error: {e}")
                await asyncio.sleep(60)  # Back off on error
                
    async def _learning_loop(self):
        """Background loop for continuous learning"""
        while self._running:
            try:
                # Update models based on recent executions
                if len(self.workflow_history) >= 10:  # Batch learning
                    await self._update_models()
                    
                # Analyze patterns
                await self._analyze_system_patterns()
                
                await asyncio.sleep(self.settings.optimization_interval_seconds * 2)
                
            except Exception as e:
                logger.error(f"Learning loop error: {e}")
                await asyncio.sleep(300)  # Longer back off for learning
                
    async def _monitoring_loop(self):
        """Background loop for system monitoring"""
        while self._running:
            try:
                # Collect system metrics
                metrics = await self.system_monitor.collect_metrics()
                
                # Check for anomalies
                anomalies = await self.anomaly_detector.detect_anomalies(metrics)
                
                if anomalies:
                    await self._handle_anomalies(anomalies)
                    
                await asyncio.sleep(self.settings.metrics_collection_interval)
                
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(60)
                
    async def _optimize_workflow(self,
                               workflow_config: WorkflowConfig) -> WorkflowConfig:
        """Apply optimizations to workflow configuration"""
        # Check cache first
        cached = self.optimization_cache.get(workflow_config.name)
        if cached and self._is_cache_valid(cached):
            return self._apply_cached_optimizations(workflow_config, cached)
            
        # Perform new optimization
        optimization_result = await self.ml_optimizer.optimize_workflow(
            workflow_config=workflow_config,
            system_state=await self.system_monitor.get_current_metrics(),
            constraints=workflow_config.constraints
        )
        
        # Apply optimizations
        optimized = workflow_config
        
        # Optimize parallelism
        if "parallelism" in optimization_result.optimizations:
            optimized = self._optimize_parallelism(
                optimized,
                optimization_result.optimizations["parallelism"]
            )
            
        # Optimize resources
        if "resources" in optimization_result.optimizations:
            optimized = self._optimize_resources(
                optimized,
                optimization_result.optimizations["resources"]
            )
            
        # Optimize caching
        if "caching" in optimization_result.optimizations:
            optimized = self._optimize_caching(
                optimized,
                optimization_result.optimizations["caching"]
            )
            
        return optimized
        
    async def _execute_workflow_steps(self,
                                    workflow_config: WorkflowConfig,
                                    execution: WorkflowExecution) -> Dict[str, Any]:
        """Execute workflow steps with monitoring"""
        results = {}
        step_metrics = []
        
        for step in workflow_config.steps:
            step_start = datetime.utcnow()
            
            try:
                # Execute step
                step_result = await self._execute_step(step, execution)
                results[step["name"]] = step_result
                
                # Collect metrics
                step_end = datetime.utcnow()
                step_metrics.append({
                    "step": step["name"],
                    "duration": (step_end - step_start).total_seconds(),
                    "success": True,
                    "resources": await self.system_monitor.get_step_resources(step["name"])
                })
                
            except Exception as e:
                logger.error(f"Step {step['name']} failed: {e}")
                step_metrics.append({
                    "step": step["name"],
                    "duration": (datetime.utcnow() - step_start).total_seconds(),
                    "success": False,
                    "error": str(e)
                })
                
                # Apply retry policy if configured
                if workflow_config.retry_policy:
                    # Implement retry logic
                    pass
                else:
                    raise
                    
        return {
            "results": results,
            "metrics": self._aggregate_metrics(step_metrics)
        }
        
    async def _execute_step(self,
                          step: Dict[str, Any],
                          execution: WorkflowExecution) -> Any:
        """Execute a single workflow step"""
        step_type = step.get("type")
        
        if step_type == "data_query":
            return await self.data_platform.execute_query(step["config"])
        elif step_type == "ml_training":
            return await self.ml_platform.submit_training(step["config"])
        elif step_type == "transform":
            return await self.data_platform.apply_transformation(step["config"])
        else:
            raise ValueError(f"Unknown step type: {step_type}")
            
    async def _analyze_pipeline_history(self, pipeline_name: str) -> pd.DataFrame:
        """Analyze historical pipeline performance"""
        # Query historical executions
        history = [
            ex for ex in self.workflow_history
            if ex.workflow_id == pipeline_name
        ]
        
        if not history:
            return pd.DataFrame()
            
        # Convert to DataFrame for analysis
        df = pd.DataFrame([
            {
                "execution_id": ex.execution_id,
                "duration": (ex.end_time - ex.start_time).total_seconds() if ex.end_time else None,
                "status": ex.status.value,
                "cpu_usage": ex.metrics.get("cpu_usage") if ex.metrics else None,
                "memory_usage": ex.metrics.get("memory_usage") if ex.metrics else None,
                "cost": ex.metrics.get("cost") if ex.metrics else None,
                "timestamp": ex.start_time
            }
            for ex in history
        ])
        
        return df
        
    def _get_business_rules(self) -> Dict[str, Any]:
        """Get current business rules and constraints"""
        return {
            "cost_weight": self.settings.cost_weight,
            "performance_weight": self.settings.performance_weight,
            "reliability_weight": self.settings.reliability_weight,
            "max_cost_per_hour": 100.0,  # Example constraint
            "min_success_rate": 0.95,
            "max_latency_ms": 5000
        }
        
    async def _apply_optimizations(self,
                                 pipeline_config: Dict[str, Any],
                                 optimal_config: OptimizationResult) -> Dict[str, Any]:
        """Apply optimization recommendations to pipeline"""
        optimized = pipeline_config.copy()
        
        # Apply each optimization
        for opt_type, opt_value in optimal_config.optimizations.items():
            if opt_type == "parallelism":
                optimized["parallelism"] = opt_value
            elif opt_type == "batch_size":
                optimized["batch_size"] = opt_value
            elif opt_type == "cache_strategy":
                optimized["cache"] = opt_value
            elif opt_type == "resource_allocation":
                optimized["resources"] = opt_value
                
        # Add optimization metadata
        optimized["_optimizations"] = {
            "timestamp": datetime.utcnow().isoformat(),
            "expected_improvement": optimal_config.expected_improvement,
            "confidence": optimal_config.confidence
        }
        
        return optimized
        
    async def _learn_from_execution(self, execution: WorkflowExecution):
        """Learn from completed execution"""
        if execution.status == WorkflowStatus.COMPLETED:
            # Update performance model
            await self.ml_optimizer.update_performance_model(
                workflow_id=execution.workflow_id,
                execution_metrics=execution.metrics,
                optimizations=execution.optimizations_applied
            )
            
    async def _update_models(self):
        """Update ML models based on recent history"""
        # Prepare training data
        recent_executions = self.workflow_history[-100:]  # Last 100 executions
        
        if recent_executions:
            # Update performance model
            await self.ml_optimizer.retrain_performance_model(recent_executions)
            
            # Update resource predictor
            await self.ml_optimizer.retrain_resource_predictor(recent_executions)
            
            # Clear old history to save memory
            self.workflow_history = self.workflow_history[-1000:]
            
    async def _analyze_system_patterns(self):
        """Analyze system-wide patterns"""
        # Analyze workflow patterns
        patterns = await self.ml_optimizer.analyze_patterns(
            executions=self.workflow_history[-500:],
            metrics=await self.system_monitor.get_historical_metrics()
        )
        
        # Store insights for future use
        if patterns:
            logger.info("System patterns analyzed", patterns=patterns)
            
    def _aggregate_metrics(self, step_metrics: List[Dict[str, Any]]) -> Dict[str, float]:
        """Aggregate step metrics into workflow metrics"""
        total_duration = sum(m["duration"] for m in step_metrics)
        success_rate = sum(1 for m in step_metrics if m.get("success", False)) / len(step_metrics)
        
        # Calculate resource usage
        cpu_usage = []
        memory_usage = []
        
        for metric in step_metrics:
            if "resources" in metric and metric["resources"]:
                cpu_usage.append(metric["resources"].get("cpu", 0))
                memory_usage.append(metric["resources"].get("memory", 0))
                
        return {
            "total_duration": total_duration,
            "success_rate": success_rate,
            "avg_cpu": np.mean(cpu_usage) if cpu_usage else 0,
            "max_cpu": np.max(cpu_usage) if cpu_usage else 0,
            "avg_memory": np.mean(memory_usage) if memory_usage else 0,
            "max_memory": np.max(memory_usage) if memory_usage else 0,
            "step_count": len(step_metrics)
        } 