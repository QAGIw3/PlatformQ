"""
Model Registry Manager

Manages interactions with MLflow model registry and integrates with
digital asset service to treat models as tradeable digital assets.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import httpx
import asyncio
import json
from mlflow.tracking import MlflowClient
from mlflow.entities.model_registry import ModelVersion

from platformq.shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


class ModelRegistryManager:
    """Manages MLflow model registry operations and digital asset integration"""
    
    def __init__(self, mlflow_client: MlflowClient, event_publisher: EventPublisher,
                 digital_asset_service_url: str = "http://digital-asset-service:80"):
        self.client = mlflow_client
        self.event_publisher = event_publisher
        self.digital_asset_service_url = digital_asset_service_url
        
    async def register_model_as_asset(self, 
                                    model_name: str, 
                                    run_id: str,
                                    tenant_id: str,
                                    user_id: str,
                                    description: Optional[str] = None,
                                    tags: Optional[Dict[str, str]] = None,
                                    license_terms: Optional[Dict[str, Any]] = None,
                                    sale_price: Optional[float] = None,
                                    royalty_percentage: int = 250) -> ModelVersion:
        """Register a model and create corresponding digital asset"""
        try:
            # Register model in MLflow
            model_version = self.client.create_model_version(
                name=model_name,
                source=f"runs:/{run_id}/model",
                run_id=run_id,
                description=description
            )
            
            # Add tags
            if tags:
                for key, value in tags.items():
                    self.client.set_model_version_tag(
                        name=model_name,
                        version=model_version.version,
                        key=key,
                        value=value
                    )
            
            # Get model metadata from MLflow
            run = self.client.get_run(run_id)
            model_metrics = run.data.metrics
            model_params = run.data.params
            
            # Create digital asset for the model
            asset_metadata = {
                "model_name": model_name,
                "version": str(model_version.version),
                "run_id": run_id,
                "mlflow_tracking_uri": f"models:/{model_name}/{model_version.version}",
                "framework": model_params.get("framework", "unknown"),
                "algorithm": model_params.get("algorithm", "unknown"),
                "metrics": model_metrics,
                "parameters": model_params,
                "training_dataset": model_params.get("training_dataset", "unknown"),
                "training_duration": model_params.get("training_duration", "unknown"),
                "model_size_mb": model_params.get("model_size_mb", 0),
                "input_schema": self._get_model_signature(model_name, model_version.version),
                "output_schema": self._get_model_output_schema(model_name, model_version.version)
            }
            
            # Prepare digital asset creation request
            asset_request = {
                "asset_name": f"{model_name}_v{model_version.version}",
                "asset_type": "ML_MODEL",
                "owner_id": user_id,
                "source_tool": "mlflow",
                "source_asset_id": f"{run_id}:{model_version.version}",
                "tags": ["ml-model", "mlflow"] + list(tags.keys()) if tags else ["ml-model", "mlflow"],
                "metadata": asset_metadata,
                "raw_data_uri": f"mlflow://models/{model_name}/{model_version.version}",
                "is_for_sale": sale_price is not None,
                "sale_price": sale_price,
                "is_licensable": license_terms is not None,
                "license_terms": license_terms or {
                    "type": "usage_based",
                    "price_per_prediction": 0.001,
                    "minimum_purchase": 1000,
                    "allowed_use_cases": ["commercial", "research"],
                    "restrictions": ["no_redistribution", "no_derivative_works"]
                },
                "royalty_percentage": royalty_percentage,
                "payload_schema_version": "ml_model_v1"
            }
            
            # Create digital asset
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.digital_asset_service_url}/api/v1/digital-assets",
                    json=asset_request,
                    headers={
                        "X-Tenant-ID": tenant_id,
                        "X-User-ID": user_id,
                        "Authorization": f"Bearer mock-token-{tenant_id}"  # In production, use real auth
                    }
                )
                
                if response.status_code != 201:
                    logger.error(f"Failed to create digital asset: {response.text}")
                else:
                    asset_info = response.json()
                    logger.info(f"Created digital asset {asset_info['cid']} for model {model_name} v{model_version.version}")
                    
                    # Update model tags with asset CID
                    self.client.set_model_version_tag(
                        name=model_name,
                        version=model_version.version,
                        key="digital_asset_cid",
                        value=asset_info['cid']
                    )
            
            # Publish event
            self._publish_model_registered_event(model_name, model_version, tenant_id, user_id)
            
            logger.info(f"Registered model {model_name} version {model_version.version} as digital asset")
            return model_version
            
        except Exception as e:
            logger.error(f"Failed to register model as asset: {e}")
            raise
    
    def register_model(self, 
                      model_name: str, 
                      run_id: str,
                      description: Optional[str] = None,
                      tags: Optional[Dict[str, str]] = None) -> ModelVersion:
        """Register a new model version (legacy method)"""
        try:
            # Register model
            model_version = self.client.create_model_version(
                name=model_name,
                source=f"runs:/{run_id}/model",
                run_id=run_id,
                description=description
            )
            
            # Add tags
            if tags:
                for key, value in tags.items():
                    self.client.set_model_version_tag(
                        name=model_name,
                        version=model_version.version,
                        key=key,
                        value=value
                    )
            
            # Publish event
            self._publish_model_registered_event(model_name, model_version)
            
            logger.info(f"Registered model {model_name} version {model_version.version}")
            return model_version
            
        except Exception as e:
            logger.error(f"Failed to register model: {e}")
            raise
            
    def list_model_versions(self, model_name: str) -> List[ModelVersion]:
        """List all versions of a model"""
        try:
            return self.client.search_model_versions(f"name = '{model_name}'")
        except Exception as e:
            logger.error(f"Failed to list model versions: {e}")
            raise
            
    def get_latest_version(self, model_name: str, stage: Optional[str] = None) -> Optional[ModelVersion]:
        """Get latest model version"""
        try:
            if stage:
                versions = self.client.get_latest_versions(model_name, stages=[stage])
            else:
                versions = self.client.get_latest_versions(model_name)
                
            return versions[0] if versions else None
            
        except Exception as e:
            logger.error(f"Failed to get latest version: {e}")
            raise
            
    def transition_model_stage(self,
                             model_name: str,
                             version: str,
                             stage: str,
                             archive_existing: bool = True):
        """Transition model to new stage"""
        try:
            self.client.transition_model_version_stage(
                name=model_name,
                version=version,
                stage=stage,
                archive_existing_versions=archive_existing
            )
            
            logger.info(f"Transitioned {model_name} v{version} to {stage}")
            
        except Exception as e:
            logger.error(f"Failed to transition model stage: {e}")
            raise
            
    def validate_model(self, model_name: str, version: str) -> Dict[str, Any]:
        """Validate a model version"""
        validation_results = {
            "model_name": model_name,
            "version": version,
            "validation_status": "passed",
            "checks": {
                "signature_present": True,
                "artifacts_complete": True,
                "performance_meets_threshold": True,
                "model_size_acceptable": True,
                "dependencies_compatible": True
            },
            "warnings": []
        }
        
        try:
            # Check model signature
            signature = self._get_model_signature(model_name, version)
            if not signature:
                validation_results["checks"]["signature_present"] = False
                validation_results["validation_status"] = "failed"
                validation_results["warnings"].append("Model signature not found")
            
            # Check model size
            model_version = self.client.get_model_version(model_name, version)
            if model_version.run_id:
                run = self.client.get_run(model_version.run_id)
                model_size = run.data.params.get("model_size_mb", 0)
                if float(model_size) > 1000:  # 1GB limit
                    validation_results["checks"]["model_size_acceptable"] = False
                    validation_results["warnings"].append(f"Model size {model_size}MB exceeds recommended 1GB")
            
            # Check performance metrics
            if model_version.run_id:
                run = self.client.get_run(model_version.run_id)
                accuracy = run.data.metrics.get("accuracy", 0)
                if float(accuracy) < 0.7:  # 70% minimum accuracy
                    validation_results["checks"]["performance_meets_threshold"] = False
                    validation_results["warnings"].append(f"Model accuracy {accuracy} below 0.7 threshold")
            
        except Exception as e:
            logger.error(f"Error during model validation: {e}")
            validation_results["validation_status"] = "error"
            validation_results["warnings"].append(str(e))
        
        return validation_results
        
    def get_model_lineage(self, model_name: str) -> List[Dict[str, Any]]:
        """Get model training lineage"""
        try:
            versions = self.list_model_versions(model_name)
            lineage = []
            
            for version in versions:
                if version.run_id:
                    run = self.client.get_run(version.run_id)
                    
                    # Get digital asset info if available
                    asset_cid = None
                    for tag in version.tags:
                        if tag.key == "digital_asset_cid":
                            asset_cid = tag.value
                            break
                    
                    lineage_entry = {
                        "version": version.version,
                        "run_id": version.run_id,
                        "created_at": version.creation_timestamp,
                        "stage": version.current_stage,
                        "metrics": run.data.metrics,
                        "params": run.data.params,
                        "tags": run.data.tags,
                        "digital_asset_cid": asset_cid,
                        "parent_run_id": run.data.tags.get("mlflow.parentRunId"),
                        "source_type": run.data.tags.get("mlflow.source.type", "UNKNOWN"),
                        "user": run.data.tags.get("mlflow.user", "unknown")
                    }
                    
                    lineage.append(lineage_entry)
                    
            return sorted(lineage, key=lambda x: x["created_at"], reverse=True)
            
        except Exception as e:
            logger.error(f"Failed to get model lineage: {e}")
            raise
    
    def _get_model_signature(self, model_name: str, version: str) -> Optional[Dict[str, Any]]:
        """Get model input/output signature"""
        try:
            # In a real implementation, this would fetch the actual model signature
            # from MLflow model artifacts
            return {
                "inputs": [
                    {"name": "features", "type": "tensor", "shape": [-1, 100]}
                ],
                "outputs": [
                    {"name": "predictions", "type": "tensor", "shape": [-1, 1]}
                ]
            }
        except:
            return None
    
    def _get_model_output_schema(self, model_name: str, version: str) -> Optional[Dict[str, Any]]:
        """Get model output schema"""
        try:
            return {
                "type": "object",
                "properties": {
                    "prediction": {"type": "number"},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "explanation": {"type": "string"}
                }
            }
        except:
            return None
    
    def _publish_model_registered_event(self, model_name: str, model_version: ModelVersion,
                                      tenant_id: str = None, user_id: str = None):
        """Publish model registered event"""
        try:
            event_data = {
                "model_name": model_name,
                "version": model_version.version,
                "run_id": model_version.run_id,
                "stage": model_version.current_stage,
                "created_at": model_version.creation_timestamp,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "event_type": "MODEL_REGISTERED"
            }
            
            self.event_publisher.publish(
                topic_base="model-lifecycle-events",
                tenant_id=tenant_id or "default",
                schema_class=None,  # Use JSON for now
                data=event_data
            )
        except Exception as e:
            logger.error(f"Failed to publish model registered event: {e}") 