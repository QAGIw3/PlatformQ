"""
MLflow Integration for Federated Learning

This module provides MLflow tracking capabilities for federated learning sessions,
including privacy-preserving model versioning and aggregated model management.
"""

import logging
import os
from typing import Dict, Any, List, Optional
from datetime import datetime
import json
import hashlib

import mlflow
import mlflow.pyfunc
from mlflow.tracking import MlflowClient
from mlflow.models.signature import ModelSignature, infer_signature
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Configure MLflow
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


class FederatedMLflowTracker:
    """Manages MLflow tracking for federated learning sessions"""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.client = MlflowClient()
        self.active_runs = {}
        
    def create_federated_experiment(self, session_id: str, session_config: Dict[str, Any]) -> str:
        """
        Create or get MLflow experiment for federated session
        
        Args:
            session_id: Federated learning session ID
            session_config: Session configuration
            
        Returns:
            Experiment ID
        """
        experiment_name = f"/Tenants/{self.tenant_id}/federated_learning/{session_id}"
        
        try:
            experiment = self.client.get_experiment_by_name(experiment_name)
            if experiment:
                return experiment.experiment_id
        except Exception:
            pass
            
        # Create new experiment with tags
        tags = {
            "tenant_id": self.tenant_id,
            "session_id": session_id,
            "model_type": session_config.get("model_type"),
            "algorithm": session_config.get("algorithm"),
            "federated": "true",
            "privacy_enabled": str(session_config.get("privacy_parameters", {}).get("differential_privacy_enabled", True))
        }
        
        experiment_id = self.client.create_experiment(
            experiment_name,
            tags=tags
        )
        
        return experiment_id
        
    def start_federated_round(self, 
                            session_id: str, 
                            round_number: int,
                            num_participants: int,
                            round_config: Dict[str, Any]) -> str:
        """
        Start MLflow run for a federated learning round
        
        Args:
            session_id: Session ID
            round_number: Current round number
            num_participants: Number of participants
            round_config: Round configuration
            
        Returns:
            MLflow run ID
        """
        experiment_name = f"/Tenants/{self.tenant_id}/federated_learning/{session_id}"
        mlflow.set_experiment(experiment_name)
        
        # Tags for the round
        tags = {
            "round_number": str(round_number),
            "num_participants": str(num_participants),
            "aggregation_strategy": round_config.get("aggregation_strategy", "FedAvg"),
            "tenant_id": self.tenant_id
        }
        
        # Start parent run for the round
        run = mlflow.start_run(
            run_name=f"round_{round_number}",
            tags=tags
        )
        
        # Log round configuration
        mlflow.log_params({
            "round_number": round_number,
            "num_participants": num_participants,
            **round_config
        })
        
        # Store active run
        self.active_runs[f"{session_id}_round_{round_number}"] = run.info.run_id
        
        return run.info.run_id
        
    def log_participant_update(self,
                             session_id: str,
                             round_number: int,
                             participant_id: str,
                             metrics: Dict[str, float],
                             model_metadata: Dict[str, Any]):
        """
        Log participant model update as nested run
        
        Args:
            session_id: Session ID
            round_number: Round number
            participant_id: Participant ID
            metrics: Participant metrics
            model_metadata: Model metadata
        """
        parent_run_id = self.active_runs.get(f"{session_id}_round_{round_number}")
        
        if not parent_run_id:
            logger.warning(f"No active run found for session {session_id} round {round_number}")
            return
            
        # Create nested run for participant
        with mlflow.start_run(run_id=parent_run_id):
            with mlflow.start_run(
                run_name=f"participant_{participant_id}",
                nested=True
            ) as nested_run:
                # Log participant info
                mlflow.log_param("participant_id", participant_id)
                mlflow.log_param("num_samples", model_metadata.get("num_samples", 0))
                
                # Log metrics
                for metric_name, value in metrics.items():
                    mlflow.log_metric(f"participant_{metric_name}", value)
                    
                # Log model quality indicators
                if "data_quality_score" in model_metadata:
                    mlflow.log_metric("data_quality_score", model_metadata["data_quality_score"])
                    
    def log_aggregated_model(self,
                           session_id: str,
                           round_number: int,
                           aggregated_model: Any,
                           global_metrics: Dict[str, float],
                           model_metadata: Dict[str, Any]):
        """
        Log aggregated model for the round
        
        Args:
            session_id: Session ID
            round_number: Round number
            aggregated_model: Aggregated model object
            global_metrics: Global model metrics
            model_metadata: Model metadata
        """
        parent_run_id = self.active_runs.get(f"{session_id}_round_{round_number}")
        
        if not parent_run_id:
            logger.warning(f"No active run found for session {session_id} round {round_number}")
            return
            
        with mlflow.start_run(run_id=parent_run_id):
            # Log global metrics
            for metric_name, value in global_metrics.items():
                mlflow.log_metric(f"global_{metric_name}", value)
                
            # Log convergence info
            if "convergence_delta" in model_metadata:
                mlflow.log_metric("convergence_delta", model_metadata["convergence_delta"])
                
            # Log aggregated model
            if aggregated_model:
                # Create model signature if possible
                signature = None
                if "input_schema" in model_metadata:
                    try:
                        input_example = pd.DataFrame(model_metadata["input_schema"])
                        signature = infer_signature(input_example, None)
                    except Exception as e:
                        logger.warning(f"Could not infer signature: {e}")
                
                # Save model
                mlflow.pyfunc.log_model(
                    artifact_path="aggregated_model",
                    python_model=FederatedModel(aggregated_model, model_metadata),
                    signature=signature,
                    registered_model_name=None  # Don't auto-register
                )
                
    def complete_round(self, session_id: str, round_number: int):
        """Complete MLflow run for a round"""
        run_key = f"{session_id}_round_{round_number}"
        if run_key in self.active_runs:
            mlflow.end_run()
            del self.active_runs[run_key]
            
    def register_final_model(self,
                           session_id: str,
                           model_uri: str,
                           model_name: str,
                           model_metrics: Dict[str, float]) -> str:
        """
        Register final federated model in MLflow registry
        
        Args:
            session_id: Session ID
            model_uri: URI of the model to register
            model_name: Name for registered model
            model_metrics: Final model metrics
            
        Returns:
            Model version
        """
        # Create model name with tenant prefix
        full_model_name = f"{self.tenant_id}_{model_name}"
        
        # Register model
        model_version = mlflow.register_model(
            model_uri,
            full_model_name
        )
        
        # Add tags and description
        self.client.update_model_version(
            name=full_model_name,
            version=model_version.version,
            description=f"Federated learning model from session {session_id}"
        )
        
        # Add metrics as tags
        for metric_name, value in model_metrics.items():
            self.client.set_model_version_tag(
                name=full_model_name,
                version=model_version.version,
                key=metric_name,
                value=str(value)
            )
            
        # Add federated learning tags
        self.client.set_model_version_tag(
            name=full_model_name,
            version=model_version.version,
            key="federated_session_id",
            value=session_id
        )
        
        self.client.set_model_version_tag(
            name=full_model_name,
            version=model_version.version,
            key="privacy_preserving",
            value="true"
        )
        
        return model_version.version
        
    def get_model_lineage(self, session_id: str) -> List[Dict[str, Any]]:
        """
        Get model lineage for a federated session
        
        Args:
            session_id: Session ID
            
        Returns:
            List of runs with metadata
        """
        experiment_name = f"/Tenants/{self.tenant_id}/federated_learning/{session_id}"
        experiment = self.client.get_experiment_by_name(experiment_name)
        
        if not experiment:
            return []
            
        # Get all runs for the experiment
        runs = self.client.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["attribute.start_time DESC"]
        )
        
        lineage = []
        for run in runs:
            lineage.append({
                "run_id": run.info.run_id,
                "round_number": run.data.tags.get("round_number"),
                "num_participants": run.data.tags.get("num_participants"),
                "start_time": run.info.start_time,
                "end_time": run.info.end_time,
                "metrics": run.data.metrics,
                "status": run.info.status
            })
            
        return lineage


class FederatedModel(mlflow.pyfunc.PythonModel):
    """Custom MLflow model wrapper for federated models"""
    
    def __init__(self, model: Any, metadata: Dict[str, Any]):
        self.model = model
        self.metadata = metadata
        
    def predict(self, context, model_input):
        """Predict using the federated model"""
        # This would be implemented based on the actual model type
        # For now, it's a placeholder
        return self.model.predict(model_input)
        
    def get_model_metadata(self) -> Dict[str, Any]:
        """Get model metadata"""
        return self.metadata 