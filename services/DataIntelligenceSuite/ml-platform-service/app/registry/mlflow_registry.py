"""
MLflow Model Registry Adapter

Provides integration with MLflow for model lifecycle management.
"""

from typing import Dict, Any, List, Optional, Union
import asyncio
from datetime import datetime
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.models import Model
from mlflow.pyfunc import PythonModel
import pandas as pd
import numpy as np
from pathlib import Path
import json

from data_intelligence_common.monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class MLFlowRegistry:
    """
    MLflow model registry adapter for ML Platform.
    
    Features:
    - Model registration and versioning
    - Model stage transitions (staging, production, archived)
    - Model metadata and tags management
    - Model artifact storage
    - Model serving preparation
    """
    
    def __init__(self, tracking_uri: str):
        self.tracking_uri = tracking_uri
        self._client: Optional[MlflowClient] = None
        self._initialized = False
        
    async def initialize(self):
        """Initialize MLflow connection"""
        try:
            # Set tracking URI
            mlflow.set_tracking_uri(self.tracking_uri)
            
            # Create client
            self._client = MlflowClient(self.tracking_uri)
            
            # Test connection
            self._client.list_experiments()
            
            self._initialized = True
            logger.info(f"MLflow registry initialized at {self.tracking_uri}")
            
        except Exception as e:
            logger.error(f"Failed to initialize MLflow registry: {e}")
            raise
            
    async def register_model(
        self,
        model_id: str,
        model_name: str,
        model_object: Optional[Any] = None,
        model_path: Optional[str] = None,
        metrics: Optional[Dict[str, float]] = None,
        params: Optional[Dict[str, Any]] = None,
        artifacts: Optional[Dict[str, str]] = None,
        tags: Optional[Dict[str, str]] = None,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """Register a model in MLflow"""
        if not self._initialized:
            await self.initialize()
            
        try:
            # Create or get experiment
            experiment_name = f"ml-platform/{model_name}"
            experiment = self._get_or_create_experiment(experiment_name)
            
            # Start MLflow run
            with mlflow.start_run(experiment_id=experiment.experiment_id) as run:
                run_id = run.info.run_id
                
                # Log parameters
                if params:
                    for key, value in params.items():
                        mlflow.log_param(key, value)
                        
                # Log metrics
                if metrics:
                    for key, value in metrics.items():
                        mlflow.log_metric(key, value)
                        
                # Log artifacts
                if artifacts:
                    for name, path in artifacts.items():
                        mlflow.log_artifact(path, artifact_path=name)
                        
                # Log model
                if model_object is not None:
                    # Determine model flavor
                    model_info = self._log_model(
                        model_object,
                        artifact_path="model",
                        registered_model_name=model_name
                    )
                elif model_path:
                    # Load and log existing model
                    mlflow.log_artifacts(model_path, artifact_path="model")
                    model_info = mlflow.register_model(
                        f"runs:/{run_id}/model",
                        model_name
                    )
                else:
                    raise ValueError("Either model_object or model_path must be provided")
                    
                # Set tags
                if tags:
                    for key, value in tags.items():
                        mlflow.set_tag(key, value)
                        
                # Add platform-specific tags
                mlflow.set_tag("platform", "ml-platform-service")
                mlflow.set_tag("model_id", model_id)
                mlflow.set_tag("registered_at", datetime.utcnow().isoformat())
                
            # Get model version
            model_version = self._client.get_latest_versions(
                model_name,
                stages=["None"]
            )[0]
            
            # Update model version description and tags
            if description:
                self._client.update_model_version(
                    name=model_name,
                    version=model_version.version,
                    description=description
                )
                
            # Set model version tags
            if tags:
                for key, value in tags.items():
                    self._client.set_model_version_tag(
                        model_name,
                        model_version.version,
                        key,
                        value
                    )
                    
            result = {
                "model_name": model_name,
                "model_version": model_version.version,
                "run_id": run_id,
                "model_uri": f"models:/{model_name}/{model_version.version}",
                "status": "registered",
                "stage": model_version.current_stage,
                "created_at": model_version.creation_timestamp
            }
            
            logger.info(f"Model registered: {model_name} v{model_version.version}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to register model: {e}")
            raise
            
    def _log_model(self, model_object: Any, artifact_path: str, registered_model_name: str):
        """Log model based on its type"""
        # Detect model type and log appropriately
        module_name = type(model_object).__module__
        
        if "sklearn" in module_name:
            return mlflow.sklearn.log_model(
                model_object,
                artifact_path,
                registered_model_name=registered_model_name
            )
        elif "xgboost" in module_name:
            return mlflow.xgboost.log_model(
                model_object,
                artifact_path,
                registered_model_name=registered_model_name
            )
        elif "lightgbm" in module_name:
            return mlflow.lightgbm.log_model(
                model_object,
                artifact_path,
                registered_model_name=registered_model_name
            )
        elif "tensorflow" in module_name or "keras" in module_name:
            return mlflow.tensorflow.log_model(
                model_object,
                artifact_path,
                registered_model_name=registered_model_name
            )
        elif "torch" in module_name:
            return mlflow.pytorch.log_model(
                model_object,
                artifact_path,
                registered_model_name=registered_model_name
            )
        else:
            # Use generic Python function model
            return mlflow.pyfunc.log_model(
                artifact_path,
                python_model=GenericModelWrapper(model_object),
                registered_model_name=registered_model_name
            )
            
    async def get_model(
        self,
        model_name: str,
        version: Optional[str] = None,
        stage: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get model information"""
        if not self._initialized:
            await self.initialize()
            
        try:
            if version:
                # Get specific version
                model_version = self._client.get_model_version(
                    model_name,
                    version
                )
            elif stage:
                # Get latest version in stage
                versions = self._client.get_latest_versions(
                    model_name,
                    stages=[stage]
                )
                if not versions:
                    raise ValueError(f"No model found in stage {stage}")
                model_version = versions[0]
            else:
                # Get latest version
                versions = self._client.get_latest_versions(model_name)
                if not versions:
                    raise ValueError(f"No versions found for model {model_name}")
                model_version = versions[0]
                
            # Get run info
            run = self._client.get_run(model_version.run_id)
            
            return {
                "model_name": model_name,
                "version": model_version.version,
                "stage": model_version.current_stage,
                "status": model_version.status,
                "created_at": model_version.creation_timestamp,
                "updated_at": model_version.last_updated_timestamp,
                "description": model_version.description,
                "tags": model_version.tags,
                "run_id": model_version.run_id,
                "model_uri": f"models:/{model_name}/{model_version.version}",
                "metrics": run.data.metrics,
                "params": run.data.params
            }
            
        except Exception as e:
            logger.error(f"Failed to get model: {e}")
            raise
            
    async def load_model(
        self,
        model_name: str,
        version: Optional[str] = None,
        stage: Optional[str] = None
    ) -> Any:
        """Load a model from registry"""
        if not self._initialized:
            await self.initialize()
            
        try:
            # Build model URI
            if version:
                model_uri = f"models:/{model_name}/{version}"
            elif stage:
                model_uri = f"models:/{model_name}/{stage}"
            else:
                model_uri = f"models:/{model_name}/latest"
                
            # Load model
            model = mlflow.pyfunc.load_model(model_uri)
            
            logger.info(f"Model loaded: {model_uri}")
            return model
            
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise
            
    async def transition_model_stage(
        self,
        model_name: str,
        version: str,
        stage: str,
        archive_existing: bool = True
    ) -> Dict[str, Any]:
        """Transition model to a different stage"""
        if not self._initialized:
            await self.initialize()
            
        valid_stages = ["None", "Staging", "Production", "Archived"]
        if stage not in valid_stages:
            raise ValueError(f"Invalid stage. Must be one of {valid_stages}")
            
        try:
            # Transition model
            self._client.transition_model_version_stage(
                name=model_name,
                version=version,
                stage=stage,
                archive_existing_versions=archive_existing
            )
            
            # Get updated model info
            model_version = self._client.get_model_version(model_name, version)
            
            result = {
                "model_name": model_name,
                "version": version,
                "previous_stage": model_version.current_stage,
                "new_stage": stage,
                "transitioned_at": datetime.utcnow().isoformat()
            }
            
            logger.info(f"Model transitioned: {model_name} v{version} -> {stage}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to transition model stage: {e}")
            raise
            
    async def list_models(
        self,
        filter_string: Optional[str] = None,
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        """List registered models"""
        if not self._initialized:
            await self.initialize()
            
        try:
            models = []
            
            # Search models
            results = self._client.search_registered_models(
                filter_string=filter_string,
                max_results=max_results
            )
            
            for model in results:
                model_info = {
                    "name": model.name,
                    "creation_timestamp": model.creation_timestamp,
                    "last_updated_timestamp": model.last_updated_timestamp,
                    "description": model.description,
                    "latest_versions": []
                }
                
                # Get latest versions
                for version in model.latest_versions:
                    model_info["latest_versions"].append({
                        "version": version.version,
                        "stage": version.current_stage,
                        "status": version.status
                    })
                    
                models.append(model_info)
                
            return models
            
        except Exception as e:
            logger.error(f"Failed to list models: {e}")
            raise
            
    async def delete_model(
        self,
        model_name: str,
        version: Optional[str] = None
    ):
        """Delete a model or specific version"""
        if not self._initialized:
            await self.initialize()
            
        try:
            if version:
                # Delete specific version
                self._client.delete_model_version(
                    name=model_name,
                    version=version
                )
                logger.info(f"Deleted model version: {model_name} v{version}")
            else:
                # Delete entire model
                self._client.delete_registered_model(name=model_name)
                logger.info(f"Deleted model: {model_name}")
                
        except Exception as e:
            logger.error(f"Failed to delete model: {e}")
            raise
            
    async def search_models(
        self,
        filter_string: str,
        order_by: Optional[List[str]] = None,
        max_results: int = 100
    ) -> List[Dict[str, Any]]:
        """Search for models with advanced filtering"""
        if not self._initialized:
            await self.initialize()
            
        try:
            models = []
            
            # Search model versions
            results = self._client.search_model_versions(
                filter_string=filter_string,
                order_by=order_by or ["version_number DESC"],
                max_results=max_results
            )
            
            for version in results:
                # Get run info
                run = self._client.get_run(version.run_id)
                
                models.append({
                    "model_name": version.name,
                    "version": version.version,
                    "stage": version.current_stage,
                    "status": version.status,
                    "created_at": version.creation_timestamp,
                    "metrics": run.data.metrics,
                    "tags": version.tags
                })
                
            return models
            
        except Exception as e:
            logger.error(f"Failed to search models: {e}")
            raise
            
    async def get_model_metrics(
        self,
        model_name: str,
        version: str
    ) -> Dict[str, Any]:
        """Get detailed metrics for a model version"""
        if not self._initialized:
            await self.initialize()
            
        try:
            # Get model version
            model_version = self._client.get_model_version(model_name, version)
            
            # Get run
            run = self._client.get_run(model_version.run_id)
            
            # Get metrics history
            metrics_history = {}
            for metric_key in run.data.metrics.keys():
                history = self._client.get_metric_history(
                    run.info.run_id,
                    metric_key
                )
                metrics_history[metric_key] = [
                    {"timestamp": m.timestamp, "value": m.value, "step": m.step}
                    for m in history
                ]
                
            return {
                "model_name": model_name,
                "version": version,
                "metrics": run.data.metrics,
                "metrics_history": metrics_history,
                "params": run.data.params,
                "tags": run.data.tags
            }
            
        except Exception as e:
            logger.error(f"Failed to get model metrics: {e}")
            raise
            
    def _get_or_create_experiment(self, name: str):
        """Get or create an MLflow experiment"""
        try:
            return self._client.get_experiment_by_name(name)
        except:
            experiment_id = self._client.create_experiment(
                name,
                tags={"created_by": "ml-platform-service"}
            )
            return self._client.get_experiment(experiment_id)
            
    async def close(self):
        """Close registry connection"""
        # MLflow client doesn't need explicit closing
        self._initialized = False
        logger.info("MLflow registry closed")


class GenericModelWrapper(PythonModel):
    """Generic wrapper for non-standard models"""
    
    def __init__(self, model):
        self.model = model
        
    def predict(self, context, model_input):
        """Make predictions using the wrapped model"""
        if hasattr(self.model, 'predict'):
            return self.model.predict(model_input)
        elif hasattr(self.model, '__call__'):
            return self.model(model_input)
        else:
            raise ValueError("Model must have predict method or be callable") 