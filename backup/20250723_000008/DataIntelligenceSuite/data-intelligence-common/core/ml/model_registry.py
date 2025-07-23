"""
Model registry for ML lifecycle management.

Provides centralized model registration, versioning, and lifecycle management.
"""

import uuid
import os
import shutil
from typing import Any, Dict, List, Optional, Union, Tuple, Set, Callable
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
from collections import defaultdict
import json
import hashlib

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger
from ...models.ml_models import (
    MLModel,
    ModelVersion,
    ModelStage,
    ModelType,
    ModelArtifact,
    ModelMetrics
)

logger = StructuredLogger.get_logger(__name__)


class TransitionRule(str, Enum):
    """Model stage transition rules"""
    NONE_TO_STAGING = "none_to_staging"
    STAGING_TO_PRODUCTION = "staging_to_production"
    PRODUCTION_TO_ARCHIVED = "production_to_archived"
    ANY_TO_ARCHIVED = "any_to_archived"
    ROLLBACK = "rollback"


@dataclass
class ModelSignature:
    """Model input/output signature"""
    inputs: Dict[str, Any] = field(default_factory=dict)
    outputs: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "inputs": self.inputs,
            "outputs": self.outputs
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ModelSignature":
        """Create from dictionary"""
        return cls(
            inputs=data.get("inputs", {}),
            outputs=data.get("outputs", {})
        )


@dataclass
class ModelLineage:
    """Model lineage information"""
    parent_model_id: Optional[str] = None
    parent_version: Optional[str] = None
    training_dataset_id: Optional[str] = None
    training_job_id: Optional[str] = None
    experiment_id: Optional[str] = None
    run_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "parent_model_id": self.parent_model_id,
            "parent_version": self.parent_version,
            "training_dataset_id": self.training_dataset_id,
            "training_job_id": self.training_job_id,
            "experiment_id": self.experiment_id,
            "run_id": self.run_id
        }


@dataclass
class StageTransition:
    """Model stage transition record"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    model_id: str = ""
    version: str = ""
    from_stage: ModelStage = ModelStage.DEVELOPMENT
    to_stage: ModelStage = ModelStage.STAGING
    transition_type: TransitionRule = TransitionRule.NONE_TO_STAGING
    
    # Approval
    requested_by: str = ""
    requested_at: datetime = field(default_factory=datetime.utcnow)
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    
    # Validation
    validation_results: Dict[str, Any] = field(default_factory=dict)
    is_approved: bool = False
    
    # Metadata
    reason: Optional[str] = None
    notes: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "model_id": self.model_id,
            "version": self.version,
            "from_stage": self.from_stage.value,
            "to_stage": self.to_stage.value,
            "transition_type": self.transition_type.value,
            "requested_by": self.requested_by,
            "requested_at": self.requested_at.isoformat(),
            "approved_by": self.approved_by,
            "approved_at": self.approved_at.isoformat() if self.approved_at else None,
            "validation_results": self.validation_results,
            "is_approved": self.is_approved,
            "reason": self.reason,
            "notes": self.notes
        }


@dataclass
class ModelComparison:
    """Model comparison result"""
    model_a_id: str
    model_a_version: str
    model_b_id: str
    model_b_version: str
    
    # Metric comparisons
    metric_deltas: Dict[str, float] = field(default_factory=dict)
    
    # Performance comparison
    latency_delta_ms: Optional[float] = None
    throughput_delta: Optional[float] = None
    
    # Resource comparison
    memory_delta_mb: Optional[float] = None
    model_size_delta_mb: Optional[float] = None
    
    # Summary
    better_model: Optional[str] = None
    comparison_summary: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "model_a": f"{self.model_a_id}:{self.model_a_version}",
            "model_b": f"{self.model_b_id}:{self.model_b_version}",
            "metric_deltas": self.metric_deltas,
            "latency_delta_ms": self.latency_delta_ms,
            "throughput_delta": self.throughput_delta,
            "memory_delta_mb": self.memory_delta_mb,
            "model_size_delta_mb": self.model_size_delta_mb,
            "better_model": self.better_model,
            "comparison_summary": self.comparison_summary
        }


class ModelRegistry:
    """
    Centralized model registry for ML lifecycle management.
    
    Features:
    - Model registration and versioning
    - Stage transitions and approvals
    - Model lineage tracking
    - Artifact management
    - Model comparison
    - Search and discovery
    """
    
    def __init__(
        self,
        storage_path: str = "/tmp/model_registry",
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.storage_path = storage_path
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # Create storage directory
        os.makedirs(storage_path, exist_ok=True)
        
        # Storage
        self._models: Dict[str, MLModel] = {}
        self._versions: Dict[str, Dict[str, ModelVersion]] = defaultdict(dict)
        self._transitions: List[StageTransition] = []
        
        # Indexes
        self._name_index: Dict[str, str] = {}  # name -> model_id
        self._stage_index: Dict[ModelStage, Set[str]] = defaultdict(set)
        self._tag_index: Dict[str, Set[str]] = defaultdict(set)
        
        # Validation rules
        self._transition_validators: Dict[TransitionRule, List[Callable]] = defaultdict(list)
        
        # Initialize default validators
        self._initialize_validators()
        
    def _initialize_validators(self):
        """Initialize default transition validators"""
        # Staging validators
        self._transition_validators[TransitionRule.NONE_TO_STAGING].extend([
            self._validate_model_artifacts,
            self._validate_model_metrics,
            self._validate_model_signature
        ])
        
        # Production validators
        self._transition_validators[TransitionRule.STAGING_TO_PRODUCTION].extend([
            self._validate_model_artifacts,
            self._validate_model_metrics,
            self._validate_model_signature,
            self._validate_performance_threshold,
            self._validate_staging_duration
        ])
        
    def register_model(
        self,
        name: str,
        model_type: ModelType,
        description: Optional[str] = None,
        owner: Optional[str] = None,
        tags: Optional[List[str]] = None,
        **kwargs
    ) -> MLModel:
        """Register new model"""
        # Check if model name already exists
        if name in self._name_index:
            raise ValueError(f"Model with name '{name}' already exists")
            
        # Create model
        model = MLModel(
            name=name,
            description=description,
            model_type=model_type,
            owner=owner,
            tags=tags or [],
            **kwargs
        )
        
        # Store model
        self._models[model.id] = model
        self._name_index[name] = model.id
        
        # Update indexes
        for tag in model.tags:
            self._tag_index[tag].add(model.id)
            
        # Create storage directory
        model_dir = os.path.join(self.storage_path, model.id)
        os.makedirs(model_dir, exist_ok=True)
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="model.registered",
                source="model_registry",
                data={
                    "model_id": model.id,
                    "model_name": name,
                    "model_type": model_type.value
                }
            ))
            
        logger.info(f"Registered model: {name} (ID: {model.id})")
        return model
        
    def create_model_version(
        self,
        model_id: str,
        version: str,
        model_path: str,
        signature: Optional[ModelSignature] = None,
        metrics: Optional[ModelMetrics] = None,
        lineage: Optional[ModelLineage] = None,
        **kwargs
    ) -> ModelVersion:
        """Create new model version"""
        model = self._models.get(model_id)
        if not model:
            raise ValueError(f"Model not found: {model_id}")
            
        # Check if version already exists
        if version in self._versions[model_id]:
            raise ValueError(f"Version {version} already exists for model {model_id}")
            
        # Create version
        model_version = ModelVersion(
            model_id=model_id,
            version=version,
            **kwargs
        )
        
        # Store artifacts
        artifacts = self._store_model_artifacts(model_id, version, model_path)
        model_version.artifacts = artifacts
        
        # Set signature
        if signature:
            model.input_schema = signature.inputs
            model.output_schema = signature.outputs
            
        # Set metrics
        if metrics:
            model_version.metrics = metrics
            
        # Store version
        self._versions[model_id][version] = model_version
        model.versions.append(model_version)
        
        # Update current version
        model.current_version = version
        
        # Cache version
        if self.cache:
            cache_key = f"model_version:{model_id}:{version}"
            self.cache.set(cache_key, model_version.to_dict(), ttl=3600)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="model.version.created",
                source="model_registry",
                data={
                    "model_id": model_id,
                    "version": version,
                    "lineage": lineage.to_dict() if lineage else None
                }
            ))
            
        logger.info(f"Created model version: {model.name} v{version}")
        return model_version
        
    def _store_model_artifacts(
        self,
        model_id: str,
        version: str,
        model_path: str
    ) -> List[ModelArtifact]:
        """Store model artifacts"""
        artifacts = []
        
        # Create version directory
        version_dir = os.path.join(self.storage_path, model_id, version)
        os.makedirs(version_dir, exist_ok=True)
        
        # Copy model file
        if os.path.isfile(model_path):
            # Single file
            filename = os.path.basename(model_path)
            dest_path = os.path.join(version_dir, filename)
            shutil.copy2(model_path, dest_path)
            
            # Create artifact record
            artifact = ModelArtifact(
                model_id=model_id,
                version=version,
                artifact_type="model",
                storage_path=dest_path,
                size_bytes=os.path.getsize(dest_path),
                checksum=self._calculate_checksum(dest_path)
            )
            artifacts.append(artifact)
            
        elif os.path.isdir(model_path):
            # Directory of files
            shutil.copytree(model_path, version_dir, dirs_exist_ok=True)
            
            # Create artifact for each file
            for root, _, files in os.walk(version_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    artifact = ModelArtifact(
                        model_id=model_id,
                        version=version,
                        artifact_type=self._infer_artifact_type(file),
                        storage_path=file_path,
                        size_bytes=os.path.getsize(file_path),
                        checksum=self._calculate_checksum(file_path)
                    )
                    artifacts.append(artifact)
                    
        return artifacts
        
    def _calculate_checksum(self, file_path: str) -> str:
        """Calculate file checksum"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
        
    def _infer_artifact_type(self, filename: str) -> str:
        """Infer artifact type from filename"""
        if filename.endswith((".pkl", ".joblib", ".h5", ".pt", ".pth", ".onnx")):
            return "model"
        elif filename.endswith((".json", ".yaml", ".yml")):
            return "config"
        elif "preprocessor" in filename.lower():
            return "preprocessor"
        else:
            return "artifact"
            
    def transition_model_stage(
        self,
        model_id: str,
        version: str,
        target_stage: ModelStage,
        requested_by: str,
        reason: Optional[str] = None
    ) -> StageTransition:
        """Request model stage transition"""
        model = self._models.get(model_id)
        if not model:
            raise ValueError(f"Model not found: {model_id}")
            
        model_version = self._versions[model_id].get(version)
        if not model_version:
            raise ValueError(f"Version not found: {version}")
            
        # Determine transition type
        current_stage = model_version.stage
        transition_type = self._get_transition_type(current_stage, target_stage)
        
        # Create transition request
        transition = StageTransition(
            model_id=model_id,
            version=version,
            from_stage=current_stage,
            to_stage=target_stage,
            transition_type=transition_type,
            requested_by=requested_by,
            reason=reason
        )
        
        # Validate transition
        validation_results = self._validate_transition(model, model_version, transition)
        transition.validation_results = validation_results
        
        # Auto-approve if all validations pass
        if all(v.get("passed", False) for v in validation_results.values()):
            transition.is_approved = True
            transition.approved_by = "system"
            transition.approved_at = datetime.utcnow()
            
            # Apply transition
            self._apply_transition(model, model_version, transition)
            
        # Store transition
        self._transitions.append(transition)
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="model.transition.requested",
                source="model_registry",
                data={
                    "transition_id": transition.id,
                    "model_id": model_id,
                    "version": version,
                    "from_stage": current_stage.value,
                    "to_stage": target_stage.value,
                    "approved": transition.is_approved
                }
            ))
            
        return transition
        
    def _get_transition_type(
        self,
        from_stage: ModelStage,
        to_stage: ModelStage
    ) -> TransitionRule:
        """Get transition type from stages"""
        if from_stage == ModelStage.DEVELOPMENT and to_stage == ModelStage.STAGING:
            return TransitionRule.NONE_TO_STAGING
        elif from_stage == ModelStage.STAGING and to_stage == ModelStage.PRODUCTION:
            return TransitionRule.STAGING_TO_PRODUCTION
        elif from_stage == ModelStage.PRODUCTION and to_stage == ModelStage.ARCHIVED:
            return TransitionRule.PRODUCTION_TO_ARCHIVED
        elif to_stage == ModelStage.ARCHIVED:
            return TransitionRule.ANY_TO_ARCHIVED
        else:
            return TransitionRule.ROLLBACK
            
    def _validate_transition(
        self,
        model: MLModel,
        version: ModelVersion,
        transition: StageTransition
    ) -> Dict[str, Any]:
        """Validate stage transition"""
        results = {}
        
        # Get validators for transition type
        validators = self._transition_validators.get(transition.transition_type, [])
        
        for validator in validators:
            try:
                result = validator(model, version, transition)
                results[validator.__name__] = result
            except Exception as e:
                results[validator.__name__] = {
                    "passed": False,
                    "error": str(e)
                }
                
        return results
        
    def _validate_model_artifacts(
        self,
        model: MLModel,
        version: ModelVersion,
        transition: StageTransition
    ) -> Dict[str, Any]:
        """Validate model has required artifacts"""
        if not version.artifacts:
            return {"passed": False, "message": "No artifacts found"}
            
        # Check for model artifact
        model_artifacts = [a for a in version.artifacts if a.artifact_type == "model"]
        if not model_artifacts:
            return {"passed": False, "message": "No model artifact found"}
            
        return {"passed": True, "artifact_count": len(version.artifacts)}
        
    def _validate_model_metrics(
        self,
        model: MLModel,
        version: ModelVersion,
        transition: StageTransition
    ) -> Dict[str, Any]:
        """Validate model has metrics"""
        if not version.metrics:
            return {"passed": False, "message": "No metrics found"}
            
        # Check minimum accuracy if specified
        if model.min_accuracy and version.metrics.accuracy:
            if version.metrics.accuracy < model.min_accuracy:
                return {
                    "passed": False,
                    "message": f"Accuracy {version.metrics.accuracy} below minimum {model.min_accuracy}"
                }
                
        return {"passed": True, "metrics": version.metrics.to_dict()}
        
    def _validate_model_signature(
        self,
        model: MLModel,
        version: ModelVersion,
        transition: StageTransition
    ) -> Dict[str, Any]:
        """Validate model has signature"""
        if not model.input_schema or not model.output_schema:
            return {"passed": False, "message": "Model signature not defined"}
            
        return {"passed": True}
        
    def _validate_performance_threshold(
        self,
        model: MLModel,
        version: ModelVersion,
        transition: StageTransition
    ) -> Dict[str, Any]:
        """Validate performance meets production thresholds"""
        # Would check against production SLAs
        return {"passed": True}
        
    def _validate_staging_duration(
        self,
        model: MLModel,
        version: ModelVersion,
        transition: StageTransition
    ) -> Dict[str, Any]:
        """Validate model has been in staging long enough"""
        # Check if model has been in staging for minimum duration
        min_staging_days = 7  # Configurable
        
        # Find when model entered staging
        staging_transitions = [
            t for t in self._transitions
            if t.model_id == model.id and 
            t.version == version.version and
            t.to_stage == ModelStage.STAGING and
            t.is_approved
        ]
        
        if not staging_transitions:
            return {"passed": False, "message": "Model never entered staging"}
            
        staging_date = staging_transitions[-1].approved_at
        days_in_staging = (datetime.utcnow() - staging_date).days
        
        if days_in_staging < min_staging_days:
            return {
                "passed": False,
                "message": f"Model has been in staging for {days_in_staging} days, minimum is {min_staging_days}"
            }
            
        return {"passed": True, "days_in_staging": days_in_staging}
        
    def _apply_transition(
        self,
        model: MLModel,
        version: ModelVersion,
        transition: StageTransition
    ):
        """Apply approved transition"""
        # Update version stage
        version.stage = transition.to_stage
        
        # Update model current stage
        model.current_stage = transition.to_stage
        
        # Update stage index
        self._stage_index[transition.from_stage].discard(f"{model.id}:{version.version}")
        self._stage_index[transition.to_stage].add(f"{model.id}:{version.version}")
        
        # Clear cache
        if self.cache:
            cache_key = f"model_version:{model.id}:{version.version}"
            self.cache.delete(cache_key)
            
        logger.info(f"Transitioned {model.name} v{version.version} from {transition.from_stage.value} to {transition.to_stage.value}")
        
    def get_model(self, model_id: str) -> Optional[MLModel]:
        """Get model by ID"""
        return self._models.get(model_id)
        
    def get_model_by_name(self, name: str) -> Optional[MLModel]:
        """Get model by name"""
        model_id = self._name_index.get(name)
        if model_id:
            return self._models.get(model_id)
        return None
        
    def get_model_version(
        self,
        model_id: str,
        version: Optional[str] = None
    ) -> Optional[ModelVersion]:
        """Get specific model version"""
        if version:
            return self._versions[model_id].get(version)
        else:
            # Get latest version
            model = self._models.get(model_id)
            if model and model.current_version:
                return self._versions[model_id].get(model.current_version)
        return None
        
    def list_models(
        self,
        model_type: Optional[ModelType] = None,
        tags: Optional[List[str]] = None,
        owner: Optional[str] = None,
        stage: Optional[ModelStage] = None
    ) -> List[MLModel]:
        """List models with filters"""
        models = list(self._models.values())
        
        # Apply filters
        if model_type:
            models = [m for m in models if m.model_type == model_type]
            
        if tags:
            tag_set = set(tags)
            models = [m for m in models if tag_set.intersection(m.tags)]
            
        if owner:
            models = [m for m in models if m.owner == owner]
            
        if stage:
            models = [m for m in models if m.current_stage == stage]
            
        return models
        
    def search_models(
        self,
        query: str,
        limit: int = 50
    ) -> List[MLModel]:
        """Search models by name or description"""
        results = []
        query_lower = query.lower()
        
        for model in self._models.values():
            if (query_lower in model.name.lower() or
                (model.description and query_lower in model.description.lower())):
                results.append(model)
                
        return results[:limit]
        
    def compare_models(
        self,
        model_a_id: str,
        version_a: str,
        model_b_id: str,
        version_b: str
    ) -> ModelComparison:
        """Compare two model versions"""
        # Get versions
        version_a_obj = self._versions[model_a_id].get(version_a)
        version_b_obj = self._versions[model_b_id].get(version_b)
        
        if not version_a_obj or not version_b_obj:
            raise ValueError("Model version not found")
            
        comparison = ModelComparison(
            model_a_id=model_a_id,
            model_a_version=version_a,
            model_b_id=model_b_id,
            model_b_version=version_b
        )
        
        # Compare metrics
        if version_a_obj.metrics and version_b_obj.metrics:
            metrics_a = version_a_obj.metrics
            metrics_b = version_b_obj.metrics
            
            # Calculate deltas
            if metrics_a.accuracy and metrics_b.accuracy:
                comparison.metric_deltas["accuracy"] = metrics_b.accuracy - metrics_a.accuracy
                
            if metrics_a.precision and metrics_b.precision:
                comparison.metric_deltas["precision"] = metrics_b.precision - metrics_a.precision
                
            if metrics_a.recall and metrics_b.recall:
                comparison.metric_deltas["recall"] = metrics_b.recall - metrics_a.recall
                
            if metrics_a.f1_score and metrics_b.f1_score:
                comparison.metric_deltas["f1_score"] = metrics_b.f1_score - metrics_a.f1_score
                
        # Determine better model
        if comparison.metric_deltas:
            positive_deltas = sum(1 for d in comparison.metric_deltas.values() if d > 0)
            negative_deltas = sum(1 for d in comparison.metric_deltas.values() if d < 0)
            
            if positive_deltas > negative_deltas:
                comparison.better_model = f"{model_b_id}:{version_b}"
            elif negative_deltas > positive_deltas:
                comparison.better_model = f"{model_a_id}:{version_a}"
            else:
                comparison.better_model = "tie"
                
        return comparison
        
    def get_model_lineage(
        self,
        model_id: str,
        version: str
    ) -> Dict[str, Any]:
        """Get model lineage information"""
        lineage = {
            "model_id": model_id,
            "version": version,
            "parents": [],
            "children": [],
            "training_data": [],
            "experiments": []
        }
        
        # Get version lineage
        version_obj = self._versions[model_id].get(version)
        if not version_obj:
            return lineage
            
        # Add training info
        if version_obj.training_dataset_id:
            lineage["training_data"].append(version_obj.training_dataset_id)
            
        if version_obj.training_job_id:
            lineage["training_job"] = version_obj.training_job_id
            
        # Find parent models
        for mid, versions in self._versions.items():
            for v, vobj in versions.items():
                # Check if this version references our model as parent
                # This would be tracked in lineage metadata
                pass
                
        return lineage
        
    def delete_model_version(
        self,
        model_id: str,
        version: str,
        force: bool = False
    ):
        """Delete model version"""
        model = self._models.get(model_id)
        if not model:
            raise ValueError(f"Model not found: {model_id}")
            
        version_obj = self._versions[model_id].get(version)
        if not version_obj:
            raise ValueError(f"Version not found: {version}")
            
        # Check if version is in production
        if version_obj.stage == ModelStage.PRODUCTION and not force:
            raise ValueError("Cannot delete production model version without force=True")
            
        # Remove artifacts
        version_dir = os.path.join(self.storage_path, model_id, version)
        if os.path.exists(version_dir):
            shutil.rmtree(version_dir)
            
        # Remove from storage
        del self._versions[model_id][version]
        model.versions = [v for v in model.versions if v.version != version]
        
        # Update current version if needed
        if model.current_version == version:
            if model.versions:
                model.current_version = model.versions[-1].version
            else:
                model.current_version = None
                
        # Clear cache
        if self.cache:
            cache_key = f"model_version:{model_id}:{version}"
            self.cache.delete(cache_key)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="model.version.deleted",
                source="model_registry",
                data={
                    "model_id": model_id,
                    "version": version
                }
            ))
            
        logger.info(f"Deleted model version: {model.name} v{version}")
        
    def export_model(
        self,
        model_id: str,
        version: str,
        export_path: str,
        include_metadata: bool = True
    ):
        """Export model version with artifacts"""
        model = self._models.get(model_id)
        version_obj = self._versions[model_id].get(version)
        
        if not model or not version_obj:
            raise ValueError("Model or version not found")
            
        # Create export directory
        os.makedirs(export_path, exist_ok=True)
        
        # Copy artifacts
        version_dir = os.path.join(self.storage_path, model_id, version)
        if os.path.exists(version_dir):
            shutil.copytree(version_dir, os.path.join(export_path, "artifacts"), dirs_exist_ok=True)
            
        # Export metadata
        if include_metadata:
            metadata = {
                "model": model.to_dict(),
                "version": version_obj.to_dict(),
                "exported_at": datetime.utcnow().isoformat()
            }
            
            with open(os.path.join(export_path, "metadata.json"), "w") as f:
                json.dump(metadata, f, indent=2)
                
        logger.info(f"Exported model: {model.name} v{version} to {export_path}") 