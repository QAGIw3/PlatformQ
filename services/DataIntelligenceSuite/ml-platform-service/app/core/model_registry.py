"""
Model Registry Manager for ML Platform
"""
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from uuid import UUID
import json

from ..domain.models.model import (
    Model, ModelStage, ModelFormat, ModelVersion,
    ModelDeployment, ServingFramework, DeploymentStrategy
)
from ..infrastructure.mlflow import MLflowClient
from ..infrastructure.minio import MinIOClient
from ..infrastructure.ignite import IgniteClient

logger = logging.getLogger(__name__)


class ModelRegistryManager:
    """
    Manages ML model registry and lifecycle
    """
    
    def __init__(self,
                 mlflow_client: MLflowClient,
                 minio_client: MinIOClient,
                 ignite_client: IgniteClient,
                 model_bucket: str,
                 artifact_bucket: str):
        self.mlflow = mlflow_client
        self.minio = minio_client
        self.ignite = ignite_client
        self.model_bucket = model_bucket
        self.artifact_bucket = artifact_bucket
        
    async def initialize(self):
        """Initialize model registry"""
        logger.info("Initializing model registry manager")
        
        # Create buckets if they don't exist
        await self.minio.create_bucket(self.model_bucket)
        await self.minio.create_bucket(self.artifact_bucket)
        
        # Initialize cache
        await self.ignite.create_cache("model_metadata", {
            "cache_mode": "REPLICATED",
            "atomicity_mode": "TRANSACTIONAL"
        })
        
        logger.info("Model registry manager initialized")
        
    async def register_model(self,
                           name: str,
                           model_path: str,
                           framework: str,
                           model_format: ModelFormat,
                           training_job_id: Optional[UUID] = None,
                           experiment_id: Optional[str] = None,
                           run_id: Optional[str] = None,
                           metrics: Optional[Dict[str, float]] = None,
                           parameters: Optional[Dict[str, Any]] = None,
                           tags: Optional[Dict[str, str]] = None,
                           created_by: str = "system") -> Model:
        """Register a new model"""
        try:
            # Upload model to MinIO
            model_key = f"models/{name}/{datetime.utcnow().isoformat()}"
            model_uri = await self.minio.upload_file(
                self.model_bucket,
                model_key,
                model_path
            )
            
            # Get model size
            stat = await self.minio.stat_object(self.model_bucket, model_key)
            model_size_mb = stat.size / (1024 * 1024)
            
            # Register in MLflow
            mlflow_name, version = await self.mlflow.register_model(
                model_uri=f"s3://{self.model_bucket}/{model_key}",
                name=name,
                tags=tags
            )
            
            # Create model entity
            model = Model(
                name=name,
                version=str(version),
                framework=framework,
                model_format=model_format,
                model_uri=model_uri,
                model_size_mb=model_size_mb,
                training_job_id=training_job_id,
                experiment_id=experiment_id,
                run_id=run_id,
                metrics=metrics or {},
                parameters=parameters or {},
                tags=tags or {},
                created_by=created_by
            )
            
            # Cache model metadata
            await self.ignite.put(
                "model_metadata",
                f"{model.name}:{model.version}",
                model.model_dump()
            )
            
            logger.info(f"Model registered: {model.name} v{model.version}")
            return model
            
        except Exception as e:
            logger.error(f"Failed to register model: {str(e)}")
            raise
        
    async def get_model(self, name: str, version: str) -> Optional[Model]:
        """Get model by name and version"""
        # Check cache first
        cached = await self.ignite.get("model_metadata", f"{name}:{version}")
        if cached:
            return Model(**cached)
            
        # Get from MLflow
        try:
            model_data = await self.mlflow.get_model_version(name, int(version))
            
            # Reconstruct model entity
            model = Model(
                name=model_data["name"],
                version=model_data["version"],
                framework="unknown",  # MLflow doesn't store this
                model_format=ModelFormat.CUSTOM,
                model_uri=model_data["source"],
                model_size_mb=0,  # Would need to query MinIO
                stage=ModelStage(model_data["stage"].lower()),
                created_at=datetime.fromtimestamp(model_data["creation_timestamp"] / 1000),
                updated_at=datetime.fromtimestamp(model_data["last_updated_timestamp"] / 1000),
                created_by="system",
                tags=model_data.get("tags", {})
            )
            
            # Cache it
            await self.ignite.put(
                "model_metadata",
                f"{model.name}:{model.version}",
                model.model_dump()
            )
            
            return model
            
        except Exception as e:
            logger.error(f"Failed to get model {name}:{version}: {str(e)}")
            return None
    
    async def list_models(self,
                         filter_string: Optional[str] = None,
                         stage: Optional[ModelStage] = None,
                         limit: int = 100) -> List[Model]:
        """List models with optional filtering"""
        models = []
        
        # Get from MLflow
        mlflow_models = await self.mlflow.search_models(
            filter_string=filter_string,
            max_results=limit
        )
        
        for mlflow_model in mlflow_models:
            for version in mlflow_model["latest_versions"]:
                if stage and version["stage"].lower() != stage.value:
                    continue
                    
                model = Model(
                    name=mlflow_model["name"],
                    version=str(version["version"]),
                    framework="unknown",
                    model_format=ModelFormat.CUSTOM,
                    model_uri="",
                    model_size_mb=0,
                    stage=ModelStage(version["stage"].lower()),
                    created_at=datetime.fromtimestamp(mlflow_model["creation_timestamp"] / 1000),
                    updated_at=datetime.fromtimestamp(mlflow_model["last_updated_timestamp"] / 1000),
                    created_by="system",
                    tags=mlflow_model.get("tags", {})
                )
                models.append(model)
                
        return models
    
    async def promote_model(self,
                          name: str,
                          version: str,
                          target_stage: ModelStage,
                          archive_existing: bool = True) -> ModelVersion:
        """Promote model to a new stage"""
        try:
            # Transition in MLflow
            await self.mlflow.transition_model_stage(
                name=name,
                version=int(version),
                stage=target_stage.value.upper(),
                archive_existing=archive_existing
            )
            
            # Create version record
            model_version = ModelVersion(
                model_id=UUID(int=0),  # Would need proper ID management
                version=version,
                stage=target_stage,
                promoted_from=ModelStage.DEVELOPMENT,  # Would need to track
                promoted_at=datetime.utcnow(),
                promoted_by="system"
            )
            
            # Update cache
            model = await self.get_model(name, version)
            if model:
                model.stage = target_stage
                model.updated_at = datetime.utcnow()
                await self.ignite.put(
                    "model_metadata",
                    f"{model.name}:{model.version}",
                    model.model_dump()
                )
            
            logger.info(f"Model promoted: {name} v{version} to {target_stage.value}")
            return model_version
            
        except Exception as e:
            logger.error(f"Failed to promote model: {str(e)}")
            raise
    
    async def create_deployment(self,
                              model_name: str,
                              model_version: str,
                              deployment_name: str,
                              serving_framework: ServingFramework,
                              deployment_strategy: DeploymentStrategy,
                              deployed_by: str,
                              replicas: int = 1,
                              cpu_request: str = "1",
                              memory_request: str = "2Gi",
                              gpu_request: int = 0,
                              environment_variables: Optional[Dict[str, str]] = None) -> ModelDeployment:
        """Create a model deployment configuration"""
        # Get model
        model = await self.get_model(model_name, model_version)
        if not model:
            raise ValueError(f"Model {model_name}:{model_version} not found")
            
        # Create deployment
        deployment = ModelDeployment(
            model_id=model.id,
            model_version=model_version,
            deployment_name=deployment_name,
            serving_framework=serving_framework,
            deployment_strategy=deployment_strategy,
            replicas=replicas,
            cpu_request=cpu_request,
            memory_request=memory_request,
            gpu_request=gpu_request,
            deployed_by=deployed_by,
            environment_variables=environment_variables or {}
        )
        
        # Store deployment config
        await self.ignite.put(
            "model_deployments",
            str(deployment.id),
            deployment.model_dump()
        )
        
        logger.info(f"Deployment created: {deployment_name} for {model_name}:{model_version}")
        return deployment
    
    async def get_model_lineage(self, name: str, version: str) -> Dict[str, Any]:
        """Get model lineage information"""
        model = await self.get_model(name, version)
        if not model:
            return {}
            
        lineage = {
            "model": model.model_dump(),
            "training_job": None,
            "experiment": None,
            "parent_models": [],
            "derived_models": []
        }
        
        # Get training job info if available
        if model.training_job_id:
            job_data = await self.ignite.get("training_jobs", str(model.training_job_id))
            if job_data:
                lineage["training_job"] = job_data
                
        # Get experiment info from MLflow
        if model.experiment_id:
            runs = await self.mlflow.search_runs(
                experiment_ids=[model.experiment_id],
                filter_string=f"run_id = '{model.run_id}'"
            )
            if not runs.empty:
                lineage["experiment"] = runs.iloc[0].to_dict()
                
        return lineage
    
    async def delete_model_version(self, name: str, version: str):
        """Delete a model version"""
        try:
            # Delete from MLflow
            await self.mlflow.delete_model_version(name, int(version))
            
            # Remove from cache
            await self.ignite.remove("model_metadata", f"{name}:{version}")
            
            # TODO: Clean up MinIO artifacts
            
            logger.info(f"Model version deleted: {name}:{version}")
            
        except Exception as e:
            logger.error(f"Failed to delete model version: {str(e)}")
            raise
        
    async def shutdown(self):
        """Shutdown model registry manager"""
        logger.info("Shutting down model registry manager") 