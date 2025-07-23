"""
MLflow client for model registry and experiment tracking
"""
import logging
from typing import Dict, List, Optional, Any, Tuple
import asyncio
from datetime import datetime
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.entities import ViewType
import pandas as pd

logger = logging.getLogger(__name__)


class MLflowClient:
    """
    Async wrapper for MLflow operations
    """
    
    def __init__(self, 
                 tracking_uri: str,
                 backend_store_uri: str,
                 artifact_location: str,
                 experiment_name: str = "default"):
        self.tracking_uri = tracking_uri
        self.backend_store_uri = backend_store_uri
        self.artifact_location = artifact_location
        self.experiment_name = experiment_name
        self.client: Optional[MlflowClient] = None
        self.experiment_id: Optional[str] = None
        
    async def initialize(self):
        """Initialize MLflow client"""
        try:
            # Set tracking URI
            mlflow.set_tracking_uri(self.tracking_uri)
            
            # Initialize client
            self.client = MlflowClient()
            
            # Create or get experiment
            experiment = self.client.get_experiment_by_name(self.experiment_name)
            if experiment is None:
                self.experiment_id = self.client.create_experiment(
                    self.experiment_name,
                    artifact_location=self.artifact_location
                )
            else:
                self.experiment_id = experiment.experiment_id
                
            logger.info(f"MLflow client initialized with experiment: {self.experiment_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize MLflow client: {str(e)}")
            raise
    
    async def create_run(self, 
                        run_name: str,
                        tags: Optional[Dict[str, str]] = None) -> str:
        """Create a new MLflow run"""
        loop = asyncio.get_event_loop()
        
        def _create_run():
            run = self.client.create_run(
                experiment_id=self.experiment_id,
                run_name=run_name,
                tags=tags or {}
            )
            return run.info.run_id
            
        return await loop.run_in_executor(None, _create_run)
    
    async def log_params(self, run_id: str, params: Dict[str, Any]):
        """Log parameters to a run"""
        loop = asyncio.get_event_loop()
        
        def _log_params():
            for key, value in params.items():
                self.client.log_param(run_id, key, value)
                
        await loop.run_in_executor(None, _log_params)
    
    async def log_metrics(self, run_id: str, metrics: Dict[str, float], step: int = 0):
        """Log metrics to a run"""
        loop = asyncio.get_event_loop()
        
        def _log_metrics():
            for key, value in metrics.items():
                self.client.log_metric(run_id, key, value, step=step)
                
        await loop.run_in_executor(None, _log_metrics)
    
    async def log_model(self,
                       run_id: str,
                       model: Any,
                       artifact_path: str,
                       model_format: str = "sklearn",
                       **kwargs) -> str:
        """Log a model to a run"""
        loop = asyncio.get_event_loop()
        
        def _log_model():
            with mlflow.start_run(run_id=run_id):
                if model_format == "sklearn":
                    mlflow.sklearn.log_model(model, artifact_path, **kwargs)
                elif model_format == "pytorch":
                    mlflow.pytorch.log_model(model, artifact_path, **kwargs)
                elif model_format == "tensorflow":
                    mlflow.tensorflow.log_model(model, artifact_path, **kwargs)
                elif model_format == "xgboost":
                    mlflow.xgboost.log_model(model, artifact_path, **kwargs)
                else:
                    mlflow.pyfunc.log_model(artifact_path, python_model=model, **kwargs)
                    
                return f"{self.tracking_uri}/runs/{run_id}/artifacts/{artifact_path}"
                
        return await loop.run_in_executor(None, _log_model)
    
    async def register_model(self,
                           model_uri: str,
                           name: str,
                           tags: Optional[Dict[str, str]] = None) -> Tuple[str, int]:
        """Register a model in the model registry"""
        loop = asyncio.get_event_loop()
        
        def _register_model():
            result = self.client.create_model_version(
                name=name,
                source=model_uri,
                tags=tags or {}
            )
            return result.name, result.version
            
        return await loop.run_in_executor(None, _register_model)
    
    async def transition_model_stage(self,
                                   name: str,
                                   version: int,
                                   stage: str,
                                   archive_existing: bool = True):
        """Transition a model version to a new stage"""
        loop = asyncio.get_event_loop()
        
        def _transition_stage():
            self.client.transition_model_version_stage(
                name=name,
                version=version,
                stage=stage,
                archive_existing_versions=archive_existing
            )
            
        await loop.run_in_executor(None, _transition_stage)
    
    async def get_model_version(self, name: str, version: int) -> Dict[str, Any]:
        """Get model version details"""
        loop = asyncio.get_event_loop()
        
        def _get_version():
            mv = self.client.get_model_version(name, version)
            return {
                "name": mv.name,
                "version": mv.version,
                "stage": mv.current_stage,
                "source": mv.source,
                "run_id": mv.run_id,
                "status": mv.status,
                "creation_timestamp": mv.creation_timestamp,
                "last_updated_timestamp": mv.last_updated_timestamp,
                "tags": mv.tags
            }
            
        return await loop.run_in_executor(None, _get_version)
    
    async def search_models(self,
                          filter_string: Optional[str] = None,
                          max_results: int = 100) -> List[Dict[str, Any]]:
        """Search for registered models"""
        loop = asyncio.get_event_loop()
        
        def _search_models():
            models = self.client.search_registered_models(
                filter_string=filter_string,
                max_results=max_results
            )
            
            return [{
                "name": model.name,
                "creation_timestamp": model.creation_timestamp,
                "last_updated_timestamp": model.last_updated_timestamp,
                "description": model.description,
                "latest_versions": [
                    {
                        "version": v.version,
                        "stage": v.current_stage,
                        "status": v.status
                    }
                    for v in model.latest_versions
                ],
                "tags": model.tags
            } for model in models]
            
        return await loop.run_in_executor(None, _search_models)
    
    async def search_runs(self,
                         experiment_ids: Optional[List[str]] = None,
                         filter_string: Optional[str] = None,
                         run_view_type: ViewType = ViewType.ACTIVE_ONLY,
                         max_results: int = 100) -> pd.DataFrame:
        """Search for runs"""
        loop = asyncio.get_event_loop()
        
        def _search_runs():
            runs = mlflow.search_runs(
                experiment_ids=experiment_ids or [self.experiment_id],
                filter_string=filter_string,
                run_view_type=run_view_type,
                max_results=max_results
            )
            return runs
            
        return await loop.run_in_executor(None, _search_runs)
    
    async def load_model(self, model_uri: str, model_format: str = "sklearn") -> Any:
        """Load a model from MLflow"""
        loop = asyncio.get_event_loop()
        
        def _load_model():
            if model_format == "sklearn":
                return mlflow.sklearn.load_model(model_uri)
            elif model_format == "pytorch":
                return mlflow.pytorch.load_model(model_uri)
            elif model_format == "tensorflow":
                return mlflow.tensorflow.load_model(model_uri)
            elif model_format == "xgboost":
                return mlflow.xgboost.load_model(model_uri)
            else:
                return mlflow.pyfunc.load_model(model_uri)
                
        return await loop.run_in_executor(None, _load_model)
    
    async def delete_model_version(self, name: str, version: int):
        """Delete a model version"""
        loop = asyncio.get_event_loop()
        
        def _delete_version():
            self.client.delete_model_version(name=name, version=version)
            
        await loop.run_in_executor(None, _delete_version)
    
    async def set_model_version_tag(self,
                                  name: str,
                                  version: int,
                                  key: str,
                                  value: str):
        """Set a tag on a model version"""
        loop = asyncio.get_event_loop()
        
        def _set_tag():
            self.client.set_model_version_tag(name, version, key, value)
            
        await loop.run_in_executor(None, _set_tag)
    
    async def close(self):
        """Close MLflow client"""
        # MLflow client doesn't need explicit closing
        logger.info("MLflow client closed") 