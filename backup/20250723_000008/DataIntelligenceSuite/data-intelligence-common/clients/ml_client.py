"""
ML Service Client

Client for machine learning operations.
"""

import logging
from typing import Any, Dict, List, Optional, Union, BinaryIO
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import json

from .base_client import BaseServiceClient, ClientConfig

logger = logging.getLogger(__name__)


class ModelStage(Enum):
    """Model lifecycle stages"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"


class TrainingStatus(Enum):
    """Training job status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ExperimentStatus(Enum):
    """Experiment status"""
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Model:
    """ML model"""
    id: str
    name: str
    version: str
    stage: ModelStage
    description: Optional[str] = None
    algorithm: Optional[str] = None
    framework: Optional[str] = None
    metrics: Dict[str, float] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    created_by: Optional[str] = None


@dataclass
class TrainingJob:
    """Training job"""
    id: str
    model_name: str
    status: TrainingStatus
    experiment_id: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration: Optional[float] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, float] = field(default_factory=dict)
    artifacts: Dict[str, str] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class Experiment:
    """ML experiment"""
    id: str
    name: str
    description: Optional[str] = None
    status: ExperimentStatus
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    tags: List[str] = field(default_factory=list)
    best_run_id: Optional[str] = None
    metrics_summary: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PredictionRequest:
    """Prediction request"""
    model_id: str
    model_version: Optional[str] = None
    data: Union[Dict[str, Any], List[Dict[str, Any]]]
    features: Optional[List[str]] = None
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PredictionResult:
    """Prediction result"""
    request_id: str
    model_id: str
    model_version: str
    predictions: Union[Any, List[Any]]
    probabilities: Optional[Union[List[float], List[List[float]]]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    latency_ms: Optional[float] = None


@dataclass
class ModelMetrics:
    """Model performance metrics"""
    model_id: str
    model_version: str
    accuracy: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    auc_roc: Optional[float] = None
    mse: Optional[float] = None
    rmse: Optional[float] = None
    mae: Optional[float] = None
    custom_metrics: Dict[str, float] = field(default_factory=dict)


class MLServiceClient(BaseServiceClient):
    """
    Client for ML service operations.
    
    Features:
    - Model management
    - Training operations
    - Inference/prediction
    - Experiment tracking
    - Model monitoring
    """
    
    def __init__(self, config: Optional[ClientConfig] = None, **kwargs):
        if not config:
            config = ClientConfig(service_name="ml-service")
        super().__init__(config, **kwargs)
        
    # Model Management
    
    async def create_model(
        self,
        name: str,
        algorithm: str,
        framework: str,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Model:
        """
        Create a new model.
        
        Args:
            name: Model name
            algorithm: Algorithm type
            framework: ML framework
            description: Model description
            tags: Model tags
            metadata: Additional metadata
            
        Returns:
            Created model
        """
        data = {
            "name": name,
            "algorithm": algorithm,
            "framework": framework,
            "description": description,
            "tags": tags or [],
            "metadata": metadata or {}
        }
        
        response = await self.post("/models", json_data=data)
        
        return Model(
            id=response["id"],
            name=response["name"],
            version=response["version"],
            stage=ModelStage(response["stage"]),
            description=response.get("description"),
            algorithm=response.get("algorithm"),
            framework=response.get("framework"),
            metrics=response.get("metrics", {}),
            parameters=response.get("parameters", {}),
            tags=response.get("tags", []),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at"),
            created_by=response.get("created_by")
        )
        
    async def get_model(
        self,
        model_id: str,
        version: Optional[str] = None
    ) -> Optional[Model]:
        """
        Get model by ID and version.
        
        Args:
            model_id: Model ID
            version: Model version (latest if not specified)
            
        Returns:
            Model if found
        """
        try:
            path = f"/models/{model_id}"
            if version:
                path += f"/versions/{version}"
                
            response = await self.get(path)
            
            return Model(
                id=response["id"],
                name=response["name"],
                version=response["version"],
                stage=ModelStage(response["stage"]),
                description=response.get("description"),
                algorithm=response.get("algorithm"),
                framework=response.get("framework"),
                metrics=response.get("metrics", {}),
                parameters=response.get("parameters", {}),
                tags=response.get("tags", []),
                created_at=response.get("created_at"),
                updated_at=response.get("updated_at"),
                created_by=response.get("created_by")
            )
        except Exception as e:
            logger.error(f"Failed to get model {model_id}: {e}")
            return None
            
    async def update_model_stage(
        self,
        model_id: str,
        version: str,
        stage: ModelStage
    ) -> bool:
        """
        Update model stage.
        
        Args:
            model_id: Model ID
            version: Model version
            stage: New stage
            
        Returns:
            Success status
        """
        data = {"stage": stage.value}
        response = await self.patch(
            f"/models/{model_id}/versions/{version}/stage",
            json_data=data
        )
        return response.get("success", False)
        
    async def list_models(
        self,
        name_filter: Optional[str] = None,
        stage: Optional[ModelStage] = None,
        tags: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Model]:
        """
        List models.
        
        Args:
            name_filter: Filter by name pattern
            stage: Filter by stage
            tags: Filter by tags
            limit: Maximum results
            offset: Pagination offset
            
        Returns:
            List of models
        """
        params = {
            "limit": limit,
            "offset": offset
        }
        
        if name_filter:
            params["name"] = name_filter
        if stage:
            params["stage"] = stage.value
        if tags:
            params["tags"] = ",".join(tags)
            
        response = await self.get("/models", params=params)
        
        return [
            Model(
                id=m["id"],
                name=m["name"],
                version=m["version"],
                stage=ModelStage(m["stage"]),
                description=m.get("description"),
                algorithm=m.get("algorithm"),
                framework=m.get("framework"),
                metrics=m.get("metrics", {}),
                parameters=m.get("parameters", {}),
                tags=m.get("tags", []),
                created_at=m.get("created_at"),
                updated_at=m.get("updated_at"),
                created_by=m.get("created_by")
            )
            for m in response.get("models", [])
        ]
        
    async def delete_model(
        self,
        model_id: str,
        version: Optional[str] = None
    ) -> bool:
        """
        Delete model or specific version.
        
        Args:
            model_id: Model ID
            version: Model version (all versions if not specified)
            
        Returns:
            Success status
        """
        path = f"/models/{model_id}"
        if version:
            path += f"/versions/{version}"
            
        response = await self.delete(path)
        return response.get("success", False)
        
    # Training Operations
    
    async def start_training(
        self,
        model_name: str,
        algorithm: str,
        dataset_id: str,
        parameters: Dict[str, Any],
        experiment_id: Optional[str] = None,
        validation_split: float = 0.2,
        compute_config: Optional[Dict[str, Any]] = None
    ) -> TrainingJob:
        """
        Start model training job.
        
        Args:
            model_name: Model name
            algorithm: Algorithm to use
            dataset_id: Training dataset ID
            parameters: Training parameters
            experiment_id: Experiment ID
            validation_split: Validation data split
            compute_config: Compute resource config
            
        Returns:
            Training job
        """
        data = {
            "model_name": model_name,
            "algorithm": algorithm,
            "dataset_id": dataset_id,
            "parameters": parameters,
            "experiment_id": experiment_id,
            "validation_split": validation_split,
            "compute_config": compute_config or {}
        }
        
        response = await self.post("/training/jobs", json_data=data)
        
        return TrainingJob(
            id=response["id"],
            model_name=response["model_name"],
            status=TrainingStatus(response["status"]),
            experiment_id=response.get("experiment_id"),
            started_at=response.get("started_at"),
            completed_at=response.get("completed_at"),
            duration=response.get("duration"),
            parameters=response.get("parameters", {}),
            metrics=response.get("metrics", {}),
            artifacts=response.get("artifacts", {}),
            error=response.get("error")
        )
        
    async def get_training_job(self, job_id: str) -> Optional[TrainingJob]:
        """
        Get training job status.
        
        Args:
            job_id: Training job ID
            
        Returns:
            Training job if found
        """
        try:
            response = await self.get(f"/training/jobs/{job_id}")
            
            return TrainingJob(
                id=response["id"],
                model_name=response["model_name"],
                status=TrainingStatus(response["status"]),
                experiment_id=response.get("experiment_id"),
                started_at=response.get("started_at"),
                completed_at=response.get("completed_at"),
                duration=response.get("duration"),
                parameters=response.get("parameters", {}),
                metrics=response.get("metrics", {}),
                artifacts=response.get("artifacts", {}),
                error=response.get("error")
            )
        except Exception as e:
            logger.error(f"Failed to get training job {job_id}: {e}")
            return None
            
    async def stop_training(self, job_id: str) -> bool:
        """
        Stop training job.
        
        Args:
            job_id: Training job ID
            
        Returns:
            Success status
        """
        response = await self.post(f"/training/jobs/{job_id}/stop")
        return response.get("success", False)
        
    async def get_training_logs(
        self,
        job_id: str,
        limit: int = 1000,
        offset: int = 0
    ) -> List[str]:
        """
        Get training job logs.
        
        Args:
            job_id: Training job ID
            limit: Maximum log lines
            offset: Log offset
            
        Returns:
            Log lines
        """
        params = {
            "limit": limit,
            "offset": offset
        }
        
        response = await self.get(f"/training/jobs/{job_id}/logs", params=params)
        return response.get("logs", [])
        
    # Inference Operations
    
    async def predict(
        self,
        model_id: str,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        model_version: Optional[str] = None,
        features: Optional[List[str]] = None,
        options: Optional[Dict[str, Any]] = None
    ) -> PredictionResult:
        """
        Make predictions using model.
        
        Args:
            model_id: Model ID
            data: Input data
            model_version: Model version (latest if not specified)
            features: Feature names
            options: Prediction options
            
        Returns:
            Prediction result
        """
        request = PredictionRequest(
            model_id=model_id,
            model_version=model_version,
            data=data,
            features=features,
            options=options or {}
        )
        
        response = await self.post(
            "/predictions",
            json_data={
                "model_id": request.model_id,
                "model_version": request.model_version,
                "data": request.data,
                "features": request.features,
                "options": request.options
            }
        )
        
        return PredictionResult(
            request_id=response["request_id"],
            model_id=response["model_id"],
            model_version=response["model_version"],
            predictions=response["predictions"],
            probabilities=response.get("probabilities"),
            metadata=response.get("metadata", {}),
            latency_ms=response.get("latency_ms")
        )
        
    async def batch_predict(
        self,
        model_id: str,
        dataset_id: str,
        model_version: Optional[str] = None,
        output_dataset_id: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Start batch prediction job.
        
        Args:
            model_id: Model ID
            dataset_id: Input dataset ID
            model_version: Model version
            output_dataset_id: Output dataset ID
            options: Prediction options
            
        Returns:
            Batch job ID
        """
        data = {
            "model_id": model_id,
            "dataset_id": dataset_id,
            "model_version": model_version,
            "output_dataset_id": output_dataset_id,
            "options": options or {}
        }
        
        response = await self.post("/predictions/batch", json_data=data)
        return response["job_id"]
        
    # Experiment Tracking
    
    async def create_experiment(
        self,
        name: str,
        description: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> Experiment:
        """
        Create a new experiment.
        
        Args:
            name: Experiment name
            description: Experiment description
            tags: Experiment tags
            
        Returns:
            Created experiment
        """
        data = {
            "name": name,
            "description": description,
            "tags": tags or []
        }
        
        response = await self.post("/experiments", json_data=data)
        
        return Experiment(
            id=response["id"],
            name=response["name"],
            description=response.get("description"),
            status=ExperimentStatus(response["status"]),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at"),
            tags=response.get("tags", []),
            best_run_id=response.get("best_run_id"),
            metrics_summary=response.get("metrics_summary", {})
        )
        
    async def get_experiment(self, experiment_id: str) -> Optional[Experiment]:
        """
        Get experiment by ID.
        
        Args:
            experiment_id: Experiment ID
            
        Returns:
            Experiment if found
        """
        try:
            response = await self.get(f"/experiments/{experiment_id}")
            
            return Experiment(
                id=response["id"],
                name=response["name"],
                description=response.get("description"),
                status=ExperimentStatus(response["status"]),
                created_at=response.get("created_at"),
                updated_at=response.get("updated_at"),
                tags=response.get("tags", []),
                best_run_id=response.get("best_run_id"),
                metrics_summary=response.get("metrics_summary", {})
            )
        except Exception as e:
            logger.error(f"Failed to get experiment {experiment_id}: {e}")
            return None
            
    async def list_experiment_runs(
        self,
        experiment_id: str,
        limit: int = 100,
        offset: int = 0
    ) -> List[TrainingJob]:
        """
        List experiment runs.
        
        Args:
            experiment_id: Experiment ID
            limit: Maximum results
            offset: Pagination offset
            
        Returns:
            List of training jobs
        """
        params = {
            "limit": limit,
            "offset": offset
        }
        
        response = await self.get(f"/experiments/{experiment_id}/runs", params=params)
        
        return [
            TrainingJob(
                id=r["id"],
                model_name=r["model_name"],
                status=TrainingStatus(r["status"]),
                experiment_id=r.get("experiment_id"),
                started_at=r.get("started_at"),
                completed_at=r.get("completed_at"),
                duration=r.get("duration"),
                parameters=r.get("parameters", {}),
                metrics=r.get("metrics", {}),
                artifacts=r.get("artifacts", {}),
                error=r.get("error")
            )
            for r in response.get("runs", [])
        ]
        
    # Model Monitoring
    
    async def get_model_metrics(
        self,
        model_id: str,
        model_version: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> ModelMetrics:
        """
        Get model performance metrics.
        
        Args:
            model_id: Model ID
            model_version: Model version
            start_date: Start date for metrics
            end_date: End date for metrics
            
        Returns:
            Model metrics
        """
        params = {}
        if start_date:
            params["start_date"] = start_date.isoformat()
        if end_date:
            params["end_date"] = end_date.isoformat()
            
        response = await self.get(
            f"/models/{model_id}/versions/{model_version}/metrics",
            params=params
        )
        
        return ModelMetrics(
            model_id=model_id,
            model_version=model_version,
            accuracy=response.get("accuracy"),
            precision=response.get("precision"),
            recall=response.get("recall"),
            f1_score=response.get("f1_score"),
            auc_roc=response.get("auc_roc"),
            mse=response.get("mse"),
            rmse=response.get("rmse"),
            mae=response.get("mae"),
            custom_metrics=response.get("custom_metrics", {})
        )
        
    async def log_prediction_feedback(
        self,
        request_id: str,
        actual_value: Any,
        feedback: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Log prediction feedback for monitoring.
        
        Args:
            request_id: Prediction request ID
            actual_value: Actual/true value
            feedback: Additional feedback
            
        Returns:
            Success status
        """
        data = {
            "request_id": request_id,
            "actual_value": actual_value,
            "feedback": feedback or {}
        }
        
        response = await self.post("/predictions/feedback", json_data=data)
        return response.get("success", False)
        
    async def check_model_drift(
        self,
        model_id: str,
        model_version: str,
        reference_dataset_id: str,
        comparison_dataset_id: str,
        drift_type: str = "feature"
    ) -> Dict[str, Any]:
        """
        Check for model/data drift.
        
        Args:
            model_id: Model ID
            model_version: Model version
            reference_dataset_id: Reference dataset
            comparison_dataset_id: Comparison dataset
            drift_type: Type of drift (feature, prediction, concept)
            
        Returns:
            Drift analysis results
        """
        data = {
            "model_id": model_id,
            "model_version": model_version,
            "reference_dataset_id": reference_dataset_id,
            "comparison_dataset_id": comparison_dataset_id,
            "drift_type": drift_type
        }
        
        return await self.post("/models/drift-analysis", json_data=data)
        
    # Model Artifacts
    
    async def upload_model_artifact(
        self,
        model_id: str,
        model_version: str,
        artifact_name: str,
        artifact_data: BinaryIO,
        artifact_type: str = "model"
    ) -> str:
        """
        Upload model artifact.
        
        Args:
            model_id: Model ID
            model_version: Model version
            artifact_name: Artifact name
            artifact_data: Artifact binary data
            artifact_type: Type of artifact
            
        Returns:
            Artifact ID
        """
        # This would typically use multipart form upload
        # Simplified for demonstration
        files = {
            'artifact': (artifact_name, artifact_data, 'application/octet-stream')
        }
        
        data = {
            'artifact_type': artifact_type
        }
        
        response = await self.post(
            f"/models/{model_id}/versions/{model_version}/artifacts",
            data=data,
            files=files
        )
        
        return response["artifact_id"]
        
    async def download_model_artifact(
        self,
        model_id: str,
        model_version: str,
        artifact_id: str
    ) -> bytes:
        """
        Download model artifact.
        
        Args:
            model_id: Model ID
            model_version: Model version
            artifact_id: Artifact ID
            
        Returns:
            Artifact binary data
        """
        response = await self.get(
            f"/models/{model_id}/versions/{model_version}/artifacts/{artifact_id}",
            raw_response=True
        )
        return response
        
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get ML-specific configuration from Consul"""
        if self.consul_client:
            config = await self.consul_client.get_key(
                f"config/{self.config.service_name}/client"
            )
            return config or {}
        return {} 