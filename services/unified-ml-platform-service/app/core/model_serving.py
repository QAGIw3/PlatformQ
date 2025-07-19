"""
Model Serving for deploying and serving ML models
"""
from typing import Dict, List, Any, Optional
from datetime import datetime
import asyncio
import httpx
import numpy as np
from platformq_shared.utils.logger import get_logger
from platformq_shared.cache.ignite_client import IgniteClient
from platformq_shared.events import EventPublisher
from platformq_shared.errors import ValidationError, NotFoundError

logger = get_logger(__name__)


class ModelServer:
    """Model serving engine for deployment and inference"""
    
    def __init__(self):
        self.cache = IgniteClient()
        self.event_publisher = EventPublisher()
        self.deployments: Dict[str, Dict] = {}
        self.knative_api = "http://knative-serving"
        self.seldon_api = "http://seldon-core:8000"
        
    async def deploy_model(self, deployment_data: Dict[str, Any]) -> Dict[str, Any]:
        """Deploy a model for serving"""
        try:
            # Validate deployment
            required_fields = ["model_id", "name", "version", "resources"]
            for field in required_fields:
                if field not in deployment_data:
                    raise ValidationError(f"Missing required field: {field}")
            
            # Generate deployment ID
            deployment_id = f"dep_{deployment_data['name']}_{datetime.utcnow().timestamp()}"
            
            # Create deployment configuration
            deployment = {
                "deployment_id": deployment_id,
                "created_at": datetime.utcnow().isoformat(),
                "status": "deploying",
                "endpoint": f"https://models.platform.io/{deployment_data['name']}/v{deployment_data['version']}",
                **deployment_data
            }
            
            # Deploy to Knative/Seldon
            if deployment_data.get("serving_framework") == "seldon":
                await self._deploy_seldon(deployment)
            else:
                await self._deploy_knative(deployment)
            
            # Store deployment
            self.deployments[deployment_id] = deployment
            await self.cache.put(f"serving:deployment:{deployment_id}", deployment)
            
            # Publish event
            await self.event_publisher.publish("model.deployed", {
                "deployment_id": deployment_id,
                "model_id": deployment_data["model_id"]
            })
            
            return deployment
            
        except Exception as e:
            logger.error(f"Failed to deploy model: {str(e)}")
            raise
    
    async def predict(self, 
                     deployment_id: str,
                     input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Run inference on deployed model"""
        try:
            # Get deployment
            deployment = await self._get_deployment(deployment_id)
            
            if deployment["status"] != "active":
                raise ValidationError(f"Deployment {deployment_id} is not active")
            
            # Transform input if needed
            processed_input = await self._preprocess_input(input_data, deployment)
            
            # Run inference
            if deployment.get("serving_framework") == "seldon":
                prediction = await self._predict_seldon(deployment, processed_input)
            else:
                prediction = await self._predict_knative(deployment, processed_input)
            
            # Post-process output
            result = await self._postprocess_output(prediction, deployment)
            
            # Log prediction
            await self._log_prediction(deployment_id, input_data, result)
            
            return {
                "deployment_id": deployment_id,
                "prediction": result,
                "model_version": deployment["version"],
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Prediction failed: {str(e)}")
            raise
    
    async def batch_predict(self,
                          deployment_id: str,
                          batch_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Run batch inference"""
        try:
            deployment = await self._get_deployment(deployment_id)
            
            # Create batch job
            job_id = f"batch_{deployment_id}_{datetime.utcnow().timestamp()}"
            
            job = {
                "job_id": job_id,
                "deployment_id": deployment_id,
                "started_at": datetime.utcnow().isoformat(),
                "status": "running",
                "total_items": len(batch_data),
                "processed_items": 0
            }
            
            await self.cache.put(f"serving:batch:{job_id}", job)
            
            # Process batch asynchronously
            asyncio.create_task(self._process_batch(job_id, deployment_id, batch_data))
            
            return {
                "job_id": job_id,
                "status": "submitted",
                "total_items": len(batch_data)
            }
            
        except Exception as e:
            logger.error(f"Batch prediction failed: {str(e)}")
            raise
    
    async def scale_deployment(self,
                             deployment_id: str,
                             replicas: int) -> Dict[str, Any]:
        """Scale deployment replicas"""
        try:
            deployment = await self._get_deployment(deployment_id)
            
            # Update replica count
            if deployment.get("serving_framework") == "seldon":
                await self._scale_seldon(deployment, replicas)
            else:
                await self._scale_knative(deployment, replicas)
            
            # Update deployment
            deployment["replicas"] = replicas
            deployment["scaled_at"] = datetime.utcnow().isoformat()
            
            await self._update_deployment(deployment_id, deployment)
            
            # Publish event
            await self.event_publisher.publish("deployment.scaled", {
                "deployment_id": deployment_id,
                "replicas": replicas
            })
            
            return {
                "deployment_id": deployment_id,
                "replicas": replicas,
                "status": "scaling"
            }
            
        except Exception as e:
            logger.error(f"Failed to scale deployment: {str(e)}")
            raise
    
    async def create_ab_test(self, test_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create A/B test between deployments"""
        try:
            # Validate test configuration
            required_fields = ["name", "deployment_a", "deployment_b", "traffic_split"]
            for field in required_fields:
                if field not in test_data:
                    raise ValidationError(f"Missing required field: {field}")
            
            # Generate test ID
            test_id = f"abtest_{test_data['name']}_{datetime.utcnow().timestamp()}"
            
            # Create test
            ab_test = {
                "test_id": test_id,
                "created_at": datetime.utcnow().isoformat(),
                "status": "active",
                "metrics": {
                    "deployment_a": {"requests": 0, "latency_ms": [], "errors": 0},
                    "deployment_b": {"requests": 0, "latency_ms": [], "errors": 0}
                },
                **test_data
            }
            
            # Configure traffic routing
            await self._configure_traffic_split(ab_test)
            
            # Store test
            await self.cache.put(f"serving:abtest:{test_id}", ab_test)
            
            return ab_test
            
        except Exception as e:
            logger.error(f"Failed to create A/B test: {str(e)}")
            raise
    
    async def _deploy_knative(self, deployment: Dict):
        """Deploy model using Knative"""
        # Create Knative service configuration
        knative_config = {
            "apiVersion": "serving.knative.dev/v1",
            "kind": "Service",
            "metadata": {
                "name": deployment["name"],
                "namespace": "ml-models"
            },
            "spec": {
                "template": {
                    "spec": {
                        "containers": [{
                            "image": f"mlflow-model:{deployment['model_id']}",
                            "ports": [{"containerPort": 8080}],
                            "resources": deployment["resources"]
                        }]
                    }
                }
            }
        }
        
        # Deploy via Knative API
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.knative_api}/apis/serving.knative.dev/v1/namespaces/ml-models/services",
                json=knative_config
            )
            
            if response.status_code not in [200, 201]:
                raise ValidationError(f"Knative deployment failed: {response.text}")
        
        deployment["status"] = "active"
    
    async def _deploy_seldon(self, deployment: Dict):
        """Deploy model using Seldon Core"""
        # Create Seldon deployment configuration
        seldon_config = {
            "apiVersion": "machinelearning.seldon.io/v1",
            "kind": "SeldonDeployment",
            "metadata": {
                "name": deployment["name"]
            },
            "spec": {
                "predictors": [{
                    "name": "default",
                    "graph": {
                        "name": "model",
                        "type": "MODEL",
                        "implementation": "MLFLOW_SERVER",
                        "modelUri": f"s3://models/{deployment['model_id']}"
                    },
                    "replicas": deployment.get("replicas", 1)
                }]
            }
        }
        
        # Deploy via Seldon API
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.seldon_api}/seldon-deployments",
                json=seldon_config
            )
            
            if response.status_code not in [200, 201]:
                raise ValidationError(f"Seldon deployment failed: {response.text}")
        
        deployment["status"] = "active"
    
    async def _predict_knative(self, deployment: Dict, input_data: Any) -> Any:
        """Run prediction via Knative endpoint"""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                deployment["endpoint"],
                json={"instances": [input_data]}
            )
            
            if response.status_code != 200:
                raise ValidationError(f"Prediction failed: {response.text}")
            
            return response.json()["predictions"][0]
    
    async def _predict_seldon(self, deployment: Dict, input_data: Any) -> Any:
        """Run prediction via Seldon endpoint"""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.seldon_api}/seldon/{deployment['name']}/api/v1.0/predictions",
                json={"data": {"ndarray": input_data}}
            )
            
            if response.status_code != 200:
                raise ValidationError(f"Prediction failed: {response.text}")
            
            return response.json()["data"]["ndarray"][0]
    
    async def _scale_knative(self, deployment: Dict, replicas: int):
        """Scale Knative deployment"""
        # Update Knative service with new replica count
        async with httpx.AsyncClient() as client:
            await client.patch(
                f"{self.knative_api}/apis/serving.knative.dev/v1/namespaces/ml-models/services/{deployment['name']}",
                json={
                    "spec": {
                        "template": {
                            "metadata": {
                                "annotations": {
                                    "autoscaling.knative.dev/minScale": str(replicas),
                                    "autoscaling.knative.dev/maxScale": str(replicas * 2)
                                }
                            }
                        }
                    }
                }
            )
    
    async def _scale_seldon(self, deployment: Dict, replicas: int):
        """Scale Seldon deployment"""
        async with httpx.AsyncClient() as client:
            await client.patch(
                f"{self.seldon_api}/seldon-deployments/{deployment['name']}",
                json={
                    "spec": {
                        "predictors": [{
                            "name": "default",
                            "replicas": replicas
                        }]
                    }
                }
            )
    
    async def _process_batch(self, job_id: str, deployment_id: str, batch_data: List[Dict]):
        """Process batch predictions"""
        try:
            results = []
            job = await self.cache.get(f"serving:batch:{job_id}")
            
            for i, item in enumerate(batch_data):
                try:
                    result = await self.predict(deployment_id, item)
                    results.append(result)
                    
                    # Update progress
                    job["processed_items"] = i + 1
                    if i % 100 == 0:  # Update every 100 items
                        await self.cache.put(f"serving:batch:{job_id}", job)
                        
                except Exception as e:
                    results.append({"error": str(e)})
            
            # Complete job
            job["status"] = "completed"
            job["completed_at"] = datetime.utcnow().isoformat()
            job["results"] = results
            
            await self.cache.put(f"serving:batch:{job_id}", job)
            
            # Publish event
            await self.event_publisher.publish("batch.completed", {
                "job_id": job_id,
                "total_items": len(batch_data),
                "successful": sum(1 for r in results if "error" not in r)
            })
            
        except Exception as e:
            logger.error(f"Batch processing failed: {str(e)}")
            job["status"] = "failed"
            job["error"] = str(e)
            await self.cache.put(f"serving:batch:{job_id}", job)
    
    async def _preprocess_input(self, input_data: Dict, deployment: Dict) -> Any:
        """Preprocess input data"""
        # Apply any preprocessing defined in deployment
        if "preprocessing" in deployment:
            # Apply transformations
            pass
        
        return input_data
    
    async def _postprocess_output(self, prediction: Any, deployment: Dict) -> Any:
        """Post-process model output"""
        # Apply any post-processing defined in deployment
        if "postprocessing" in deployment:
            # Apply transformations
            pass
        
        return prediction
    
    async def _log_prediction(self, deployment_id: str, input_data: Dict, output: Any):
        """Log prediction for monitoring"""
        await self.event_publisher.publish("prediction.logged", {
            "deployment_id": deployment_id,
            "timestamp": datetime.utcnow().isoformat(),
            "input_size": len(str(input_data)),
            "output_type": type(output).__name__
        })
    
    async def _configure_traffic_split(self, ab_test: Dict):
        """Configure traffic split for A/B test"""
        # Configure Kong or service mesh for traffic routing
        pass
    
    async def _get_deployment(self, deployment_id: str) -> Dict[str, Any]:
        """Get deployment by ID"""
        deployment = self.deployments.get(deployment_id)
        if not deployment:
            cached = await self.cache.get(f"serving:deployment:{deployment_id}")
            if not cached:
                raise NotFoundError(f"Deployment {deployment_id} not found")
            deployment = cached
            self.deployments[deployment_id] = deployment
        
        return deployment
    
    async def _update_deployment(self, deployment_id: str, deployment: Dict):
        """Update deployment"""
        self.deployments[deployment_id] = deployment
        await self.cache.put(f"serving:deployment:{deployment_id}", deployment) 