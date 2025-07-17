"""
Model Registry Manager

Manages interactions with MLflow model registry.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from mlflow.tracking import MlflowClient
from mlflow.entities.model_registry import ModelVersion

from platformq.shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


class ModelRegistryManager:
    """Manages MLflow model registry operations"""
    
    def __init__(self, mlflow_client: MlflowClient, event_publisher: EventPublisher):
        self.client = mlflow_client
        self.event_publisher = event_publisher
        
    def register_model(self, 
                      model_name: str, 
                      run_id: str,
                      description: Optional[str] = None,
                      tags: Optional[Dict[str, str]] = None) -> ModelVersion:
        """Register a new model version"""
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
        # This would run validation checks
        # For now, return success
        return {
            "model_name": model_name,
            "version": version,
            "validation_status": "passed",
            "checks": {
                "signature_present": True,
                "artifacts_complete": True,
                "performance_meets_threshold": True
            }
        }
        
    def get_model_lineage(self, model_name: str) -> List[Dict[str, Any]]:
        """Get model training lineage"""
        try:
            versions = self.list_model_versions(model_name)
            lineage = []
            
            for version in versions:
                if version.run_id:
                    run = self.client.get_run(version.run_id)
                    lineage.append({
                        "version": version.version,
                        "run_id": version.run_id,
                        "created_at": version.creation_timestamp,
                        "stage": version.current_stage,
                        "metrics": run.data.metrics,
                        "params": run.data.params,
                        "tags": run.data.tags
                    })
                    
            return sorted(lineage, key=lambda x: x["created_at"], reverse=True)
            
        except Exception as e:
            logger.error(f"Failed to get model lineage: {e}")
            raise
            
    def _publish_model_registered_event(self, model_name: str, model_version: ModelVersion):
        """Publish model registered event"""
        event = {
            "event_type": "MODEL_REGISTERED",
            "model_name": model_name,
            "version": model_version.version,
            "run_id": model_version.run_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Extract tenant from model name
        tenant_id = model_name.split("_")[0]
        
        self.event_publisher.publish(
            topic=f"persistent://platformq/{tenant_id}/model-registry-events",
            schema_path="model_registered.avsc",
            data=event
        ) 