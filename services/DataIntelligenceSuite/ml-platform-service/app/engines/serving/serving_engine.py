"""
Serving Engine

Manages ML model deployment and real-time inference.
"""

import asyncio
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from enum import Enum
import uuid
import numpy as np

from data_intelligence_common import StructuredLogger, EventBus
from data_intelligence_common.vault_consul import VaultConsulIntegration

logger = StructuredLogger.get_logger(__name__)


class DeploymentStatus(Enum):
    """Model deployment status"""
    PENDING = "pending"
    DEPLOYING = "deploying"
    ACTIVE = "active"
    FAILED = "failed"
    STOPPING = "stopping"
    STOPPED = "stopped"


class ServingFramework(Enum):
    """Supported serving frameworks"""
    TRITON = "triton"
    TORCHSERVE = "torchserve"
    TENSORFLOW_SERVING = "tensorflow-serving"
    KSERVE = "kserve"
    CUSTOM = "custom"


class ServingEngine:
    """
    Manages ML model serving and inference
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration, event_bus: EventBus,
                 model_registry: Any):
        self.vault_consul = vault_consul
        self.event_bus = event_bus
        self.model_registry = model_registry
        
        # Deployment tracking
        self.deployments: Dict[str, Dict[str, Any]] = {}
        self.model_cache: Dict[str, Any] = {}
        
        # Configuration
        self.config = {
            "max_models_per_server": 10,
            "inference_timeout": 60,
            "batch_size": 32,
            "auto_scaling": {
                "enabled": True,
                "min_replicas": 1,
                "max_replicas": 10,
                "target_cpu": 70,
                "target_memory": 80
            },
            "frameworks": {
                "triton": {
                    "url": "http://triton:8001",
                    "grpc_url": "triton:8001"
                },
                "torchserve": {
                    "url": "http://torchserve:8080",
                    "management_url": "http://torchserve:8081"
                }
            }
        }
        
        # Metrics
        self.metrics = {
            "deployments_total": 0,
            "active_deployments": 0,
            "inference_requests": 0,
            "inference_errors": 0,
            "avg_latency_ms": 0
        }
    
    async def initialize(self):
        """Initialize serving engine"""
        logger.info("initializing_serving_engine")
        
        # Load configuration from Consul
        await self._load_configuration()
        
        # Initialize serving frameworks
        await self._initialize_frameworks()
        
        # Start monitoring
        asyncio.create_task(self._monitor_deployments())
        
        logger.info("serving_engine_initialized")
    
    async def cleanup(self):
        """Cleanup resources"""
        # Stop all deployments
        for deployment_id in list(self.deployments.keys()):
            await self.undeploy_model(deployment_id)
    
    async def _load_configuration(self):
        """Load configuration from Consul"""
        try:
            config = await self.vault_consul.consul.kv.get("config/serving-engine")
            if config:
                import json
                self.config.update(json.loads(config["Value"].decode()))
        except Exception as e:
            logger.warning(f"Failed to load config from Consul: {e}")
    
    async def _initialize_frameworks(self):
        """Initialize serving frameworks"""
        # This would initialize connections to various serving frameworks
        pass
    
    async def deploy_model(self, deployment_config: Dict[str, Any]) -> str:
        """
        Deploy a model for serving
        
        Args:
            deployment_config: Deployment configuration including:
                - model_id: Model ID from registry
                - model_version: Model version
                - name: Deployment name
                - framework: Serving framework
                - resources: Resource requirements
                - scaling: Auto-scaling configuration
                - endpoints: API endpoints configuration
                
        Returns:
            Deployment ID
        """
        deployment_id = str(uuid.uuid4())
        
        # Validate deployment configuration
        self._validate_deployment_config(deployment_config)
        
        # Get model from registry
        model = await self.model_registry.get_model(
            deployment_config["model_id"],
            deployment_config.get("model_version")
        )
        
        if not model:
            raise ValueError(f"Model not found: {deployment_config['model_id']}")
        
        # Create deployment record
        deployment = {
            "id": deployment_id,
            "config": deployment_config,
            "model": model,
            "status": DeploymentStatus.PENDING,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "endpoints": [],
            "metrics": {
                "requests": 0,
                "errors": 0,
                "avg_latency": 0
            }
        }
        
        # Store deployment
        self.deployments[deployment_id] = deployment
        
        # Start deployment
        asyncio.create_task(self._deploy_model(deployment_id))
        
        # Update metrics
        self.metrics["deployments_total"] += 1
        
        # Emit event
        await self.event_bus.publish(
            "ml.model.deployment.started",
            {
                "deployment_id": deployment_id,
                "model_id": deployment_config["model_id"],
                "framework": deployment_config["framework"],
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Model deployment started: {deployment_id}")
        return deployment_id
    
    async def get_deployment_status(self, deployment_id: str) -> Dict[str, Any]:
        """Get deployment status"""
        deployment = self.deployments.get(deployment_id)
        if not deployment:
            raise ValueError(f"Deployment not found: {deployment_id}")
        
        return {
            "id": deployment_id,
            "status": deployment["status"].value,
            "model_id": deployment["config"]["model_id"],
            "endpoints": deployment["endpoints"],
            "metrics": deployment["metrics"],
            "created_at": deployment["created_at"].isoformat(),
            "updated_at": deployment["updated_at"].isoformat()
        }
    
    async def predict(self, deployment_id: str, input_data: Union[Dict, List]) -> Dict[str, Any]:
        """
        Make prediction using deployed model
        
        Args:
            deployment_id: Deployment ID
            input_data: Input data for prediction
            
        Returns:
            Prediction results
        """
        start_time = datetime.utcnow()
        
        deployment = self.deployments.get(deployment_id)
        if not deployment:
            raise ValueError(f"Deployment not found: {deployment_id}")
        
        if deployment["status"] != DeploymentStatus.ACTIVE:
            raise RuntimeError(f"Deployment not active: {deployment['status'].value}")
        
        try:
            # Get serving framework
            framework = deployment["config"]["framework"]
            
            # Prepare input
            prepared_input = await self._prepare_input(deployment, input_data)
            
            # Make prediction based on framework
            if framework == ServingFramework.TRITON.value:
                result = await self._predict_triton(deployment, prepared_input)
            elif framework == ServingFramework.TORCHSERVE.value:
                result = await self._predict_torchserve(deployment, prepared_input)
            elif framework == ServingFramework.TENSORFLOW_SERVING.value:
                result = await self._predict_tensorflow(deployment, prepared_input)
            else:
                raise ValueError(f"Unsupported framework: {framework}")
            
            # Post-process output
            processed_result = await self._process_output(deployment, result)
            
            # Update metrics
            latency = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_deployment_metrics(deployment_id, latency, success=True)
            
            # Emit event
            await self.event_bus.publish(
                "ml.inference.completed",
                {
                    "deployment_id": deployment_id,
                    "latency_ms": latency,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            return {
                "predictions": processed_result,
                "model_id": deployment["config"]["model_id"],
                "deployment_id": deployment_id,
                "latency_ms": latency
            }
            
        except Exception as e:
            logger.error(f"Prediction failed for deployment {deployment_id}: {e}")
            
            # Update metrics
            self._update_deployment_metrics(deployment_id, 0, success=False)
            
            # Emit event
            await self.event_bus.publish(
                "ml.inference.failed",
                {
                    "deployment_id": deployment_id,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            raise
    
    async def undeploy_model(self, deployment_id: str) -> bool:
        """Undeploy a model"""
        deployment = self.deployments.get(deployment_id)
        if not deployment:
            raise ValueError(f"Deployment not found: {deployment_id}")
        
        if deployment["status"] in [DeploymentStatus.STOPPING, DeploymentStatus.STOPPED]:
            return False
        
        # Update status
        deployment["status"] = DeploymentStatus.STOPPING
        deployment["updated_at"] = datetime.utcnow()
        
        try:
            # Stop model based on framework
            framework = deployment["config"]["framework"]
            
            if framework == ServingFramework.TRITON.value:
                await self._undeploy_triton(deployment)
            elif framework == ServingFramework.TORCHSERVE.value:
                await self._undeploy_torchserve(deployment)
            elif framework == ServingFramework.TENSORFLOW_SERVING.value:
                await self._undeploy_tensorflow(deployment)
            
            # Update status
            deployment["status"] = DeploymentStatus.STOPPED
            deployment["updated_at"] = datetime.utcnow()
            
            # Update metrics
            self.metrics["active_deployments"] -= 1
            
            # Emit event
            await self.event_bus.publish(
                "ml.model.deployment.stopped",
                {
                    "deployment_id": deployment_id,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            logger.info(f"Model deployment stopped: {deployment_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to undeploy model {deployment_id}: {e}")
            deployment["status"] = DeploymentStatus.FAILED
            raise
    
    async def _deploy_model(self, deployment_id: str):
        """Deploy model to serving framework"""
        deployment = self.deployments.get(deployment_id)
        if not deployment:
            return
        
        try:
            # Update status
            deployment["status"] = DeploymentStatus.DEPLOYING
            deployment["updated_at"] = datetime.utcnow()
            
            # Get framework
            framework = deployment["config"]["framework"]
            
            # Deploy based on framework
            if framework == ServingFramework.TRITON.value:
                endpoints = await self._deploy_triton(deployment)
            elif framework == ServingFramework.TORCHSERVE.value:
                endpoints = await self._deploy_torchserve(deployment)
            elif framework == ServingFramework.TENSORFLOW_SERVING.value:
                endpoints = await self._deploy_tensorflow(deployment)
            else:
                raise ValueError(f"Unsupported framework: {framework}")
            
            # Update deployment
            deployment["status"] = DeploymentStatus.ACTIVE
            deployment["endpoints"] = endpoints
            deployment["updated_at"] = datetime.utcnow()
            
            # Update metrics
            self.metrics["active_deployments"] += 1
            
            # Emit event
            await self.event_bus.publish(
                "ml.model.deployment.completed",
                {
                    "deployment_id": deployment_id,
                    "endpoints": endpoints,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            logger.info(f"Model deployment completed: {deployment_id}")
            
        except Exception as e:
            logger.error(f"Model deployment failed: {deployment_id}, error: {e}")
            
            deployment["status"] = DeploymentStatus.FAILED
            deployment["updated_at"] = datetime.utcnow()
            
            # Emit event
            await self.event_bus.publish(
                "ml.model.deployment.failed",
                {
                    "deployment_id": deployment_id,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
    
    def _validate_deployment_config(self, config: Dict[str, Any]):
        """Validate deployment configuration"""
        required_fields = ["model_id", "name", "framework"]
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate framework
        framework = config["framework"]
        if framework not in [f.value for f in ServingFramework]:
            raise ValueError(f"Unsupported framework: {framework}")
    
    async def _prepare_input(self, deployment: Dict[str, Any], 
                           input_data: Union[Dict, List]) -> Any:
        """Prepare input for model inference"""
        # This would handle input preprocessing based on model requirements
        return input_data
    
    async def _process_output(self, deployment: Dict[str, Any], 
                            output: Any) -> Any:
        """Process model output"""
        # This would handle output post-processing
        return output
    
    def _update_deployment_metrics(self, deployment_id: str, latency: float, 
                                 success: bool):
        """Update deployment metrics"""
        deployment = self.deployments.get(deployment_id)
        if not deployment:
            return
        
        metrics = deployment["metrics"]
        
        if success:
            metrics["requests"] += 1
            self.metrics["inference_requests"] += 1
            
            # Update average latency
            if metrics["requests"] == 1:
                metrics["avg_latency"] = latency
            else:
                current_avg = metrics["avg_latency"]
                metrics["avg_latency"] = (
                    (current_avg * (metrics["requests"] - 1) + latency) / metrics["requests"]
                )
            
            # Update global average latency
            self._update_global_avg_latency(latency)
        else:
            metrics["errors"] += 1
            self.metrics["inference_errors"] += 1
    
    def _update_global_avg_latency(self, latency: float):
        """Update global average latency metric"""
        total_requests = self.metrics["inference_requests"]
        
        if total_requests == 1:
            self.metrics["avg_latency_ms"] = latency
        else:
            current_avg = self.metrics["avg_latency_ms"]
            self.metrics["avg_latency_ms"] = (
                (current_avg * (total_requests - 1) + latency) / total_requests
            )
    
    async def _monitor_deployments(self):
        """Monitor active deployments"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                for deployment_id, deployment in list(self.deployments.items()):
                    if deployment["status"] != DeploymentStatus.ACTIVE:
                        continue
                    
                    # Check deployment health
                    # This would check the actual serving framework
                    
                    # Handle auto-scaling if enabled
                    if deployment["config"].get("scaling", {}).get("enabled", False):
                        await self._handle_auto_scaling(deployment)
                
            except Exception as e:
                logger.error(f"Error monitoring deployments: {e}")
    
    async def _handle_auto_scaling(self, deployment: Dict[str, Any]):
        """Handle auto-scaling for deployment"""
        # This would implement auto-scaling logic based on metrics
        pass
    
    # Framework-specific methods (placeholders)
    async def _deploy_triton(self, deployment: Dict[str, Any]) -> List[str]:
        """Deploy model to NVIDIA Triton"""
        # Placeholder implementation
        return [f"{self.config['frameworks']['triton']['url']}/v2/models/{deployment['config']['name']}"]
    
    async def _deploy_torchserve(self, deployment: Dict[str, Any]) -> List[str]:
        """Deploy model to TorchServe"""
        # Placeholder implementation
        return [f"{self.config['frameworks']['torchserve']['url']}/predictions/{deployment['config']['name']}"]
    
    async def _deploy_tensorflow(self, deployment: Dict[str, Any]) -> List[str]:
        """Deploy model to TensorFlow Serving"""
        # Placeholder implementation
        return [f"http://tensorflow-serving:8501/v1/models/{deployment['config']['name']}"]
    
    async def _predict_triton(self, deployment: Dict[str, Any], input_data: Any) -> Any:
        """Make prediction using Triton"""
        # Placeholder implementation
        return {"predictions": [0.8, 0.2]}
    
    async def _predict_torchserve(self, deployment: Dict[str, Any], input_data: Any) -> Any:
        """Make prediction using TorchServe"""
        # Placeholder implementation
        return {"predictions": [0.8, 0.2]}
    
    async def _predict_tensorflow(self, deployment: Dict[str, Any], input_data: Any) -> Any:
        """Make prediction using TensorFlow Serving"""
        # Placeholder implementation
        return {"predictions": [0.8, 0.2]}
    
    async def _undeploy_triton(self, deployment: Dict[str, Any]):
        """Undeploy model from Triton"""
        # Placeholder implementation
        pass
    
    async def _undeploy_torchserve(self, deployment: Dict[str, Any]):
        """Undeploy model from TorchServe"""
        # Placeholder implementation
        pass
    
    async def _undeploy_tensorflow(self, deployment: Dict[str, Any]):
        """Undeploy model from TensorFlow Serving"""
        # Placeholder implementation
        pass
    
    async def get_serving_metrics(self) -> Dict[str, Any]:
        """Get serving engine metrics"""
        return {
            **self.metrics,
            "deployments": len(self.deployments),
            "model_cache_size": len(self.model_cache)
        } 