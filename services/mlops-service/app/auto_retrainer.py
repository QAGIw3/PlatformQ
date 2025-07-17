"""
Automated Model Retrainer

Triggers model retraining when performance degrades
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import asyncio
import httpx
from dataclasses import dataclass
import json
from enum import Enum

logger = logging.getLogger(__name__)


class RetrainingTrigger(Enum):
    PERFORMANCE_DEGRADATION = "performance_degradation"
    DRIFT_DETECTED = "drift_detected"
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    DATA_VOLUME = "data_volume"


@dataclass
class RetrainingJob:
    job_id: str
    model_name: str
    version: str
    tenant_id: str
    trigger: RetrainingTrigger
    trigger_metrics: Dict[str, Any]
    status: str
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    new_version: Optional[str]
    improvement_metrics: Optional[Dict[str, Any]]


class AutomatedRetrainer:
    """Manages automated model retraining based on performance metrics"""
    
    def __init__(self,
                 workflow_service_url: str = "http://workflow-service:8000",
                 mlflow_url: str = "http://mlflow:5000",
                 data_lake_url: str = "http://data-lake-service:8000",
                 spark_master_url: str = "spark://spark-master:7077"):
        self.workflow_service_url = workflow_service_url
        self.mlflow_url = mlflow_url
        self.data_lake_url = data_lake_url
        self.spark_master_url = spark_master_url
        self.retraining_jobs: Dict[str, RetrainingJob] = {}
        self.retraining_policies: Dict[str, Dict[str, Any]] = {}
        
    async def register_retraining_policy(self,
                                       model_name: str,
                                       tenant_id: str,
                                       policy: Dict[str, Any]):
        """Register a retraining policy for a model"""
        key = f"{tenant_id}:{model_name}"
        
        default_policy = {
            "accuracy_threshold": 0.85,
            "drift_threshold": 0.7,
            "min_data_points": 1000,
            "retraining_interval_days": 30,
            "auto_deploy": False,
            "improvement_threshold": 0.02,
            "max_retries": 3,
            "data_window_days": 90,
            "hyperparameter_tuning": True,
            "test_split": 0.2
        }
        
        self.retraining_policies[key] = {**default_policy, **policy}
        logger.info(f"Registered retraining policy for {key}")
        
    async def evaluate_retraining_need(self,
                                     model_name: str,
                                     version: str,
                                     tenant_id: str,
                                     performance_metrics: Dict[str, Any]) -> Optional[RetrainingTrigger]:
        """Evaluate if model needs retraining"""
        key = f"{tenant_id}:{model_name}"
        policy = self.retraining_policies.get(key)
        
        if not policy:
            logger.warning(f"No retraining policy for {key}")
            return None
            
        # Check accuracy threshold
        if performance_metrics.get("accuracy", 1.0) < policy["accuracy_threshold"]:
            return RetrainingTrigger.PERFORMANCE_DEGRADATION
            
        # Check drift threshold
        if performance_metrics.get("drift_score", 0.0) > policy["drift_threshold"]:
            return RetrainingTrigger.DRIFT_DETECTED
            
        # Check scheduled retraining
        last_training = await self._get_last_training_time(model_name, tenant_id)
        if last_training:
            days_since = (datetime.utcnow() - last_training).days
            if days_since >= policy["retraining_interval_days"]:
                return RetrainingTrigger.SCHEDULED
                
        return None
        
    async def trigger_retraining(self,
                               model_name: str,
                               version: str,
                               tenant_id: str,
                               trigger: RetrainingTrigger,
                               trigger_metrics: Dict[str, Any]) -> str:
        """Trigger model retraining"""
        job_id = f"retrain_{model_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        job = RetrainingJob(
            job_id=job_id,
            model_name=model_name,
            version=version,
            tenant_id=tenant_id,
            trigger=trigger,
            trigger_metrics=trigger_metrics,
            status="pending",
            created_at=datetime.utcnow(),
            started_at=None,
            completed_at=None,
            new_version=None,
            improvement_metrics=None
        )
        
        self.retraining_jobs[job_id] = job
        
        # Create retraining workflow
        workflow_config = await self._create_retraining_workflow(job)
        
        # Submit to workflow service
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.workflow_service_url}/api/v1/workflows",
                json=workflow_config,
                headers={"X-Tenant-ID": tenant_id}
            )
            
            if response.status_code == 200:
                workflow_id = response.json()["workflow_id"]
                job.status = "submitted"
                logger.info(f"Submitted retraining job {job_id} as workflow {workflow_id}")
                
                # Start monitoring job
                asyncio.create_task(self._monitor_retraining_job(job_id, workflow_id))
            else:
                job.status = "failed"
                logger.error(f"Failed to submit retraining job: {response.text}")
                
        return job_id
        
    async def _create_retraining_workflow(self, job: RetrainingJob) -> Dict[str, Any]:
        """Create workflow configuration for retraining"""
        policy = self.retraining_policies.get(f"{job.tenant_id}:{job.model_name}", {})
        
        return {
            "name": f"retrain_{job.model_name}",
            "description": f"Automated retraining triggered by {job.trigger.value}",
            "steps": [
                {
                    "name": "prepare_training_data",
                    "type": "spark_job",
                    "config": {
                        "main_class": "com.platformq.ml.DataPreparation",
                        "spark_config": {
                            "spark.master": self.spark_master_url,
                            "spark.executor.memory": "4g",
                            "spark.executor.cores": "4"
                        },
                        "arguments": [
                            "--model-name", job.model_name,
                            "--tenant-id", job.tenant_id,
                            "--data-window-days", str(policy.get("data_window_days", 90)),
                            "--min-samples", str(policy.get("min_data_points", 1000)),
                            "--output-path", f"s3://platformq-ml/retraining/{job.job_id}/data"
                        ]
                    }
                },
                {
                    "name": "feature_engineering",
                    "type": "spark_job",
                    "config": {
                        "main_class": "com.platformq.ml.FeatureEngineering",
                        "spark_config": {
                            "spark.master": self.spark_master_url,
                            "spark.executor.memory": "4g"
                        },
                        "arguments": [
                            "--input-path", "${prepare_training_data.output.data_path}",
                            "--output-path", f"s3://platformq-ml/retraining/{job.job_id}/features",
                            "--feature-config", json.dumps(await self._get_feature_config(job.model_name))
                        ]
                    }
                },
                {
                    "name": "train_model",
                    "type": "mlflow_training",
                    "config": {
                        "experiment_name": f"{job.tenant_id}/{job.model_name}",
                        "run_name": f"retrain_{job.job_id}",
                        "entry_point": "train",
                        "parameters": {
                            "training_data": "${feature_engineering.output.feature_path}",
                            "test_split": policy.get("test_split", 0.2),
                            "hyperparameter_tuning": policy.get("hyperparameter_tuning", True),
                            "base_model_uri": f"models:/{job.model_name}/{job.version}",
                            "transfer_learning": True
                        },
                        "backend": "kubernetes",
                        "backend_config": {
                            "kube_context": "platformq-ml",
                            "resources": {
                                "requests": {"memory": "8Gi", "cpu": "4"},
                                "limits": {"memory": "16Gi", "cpu": "8", "nvidia.com/gpu": "1"}
                            }
                        }
                    }
                },
                {
                    "name": "evaluate_model",
                    "type": "python_function",
                    "config": {
                        "function": "evaluate_retrained_model",
                        "module": "platformq.ml.evaluation",
                        "arguments": {
                            "new_model_uri": "${train_model.output.model_uri}",
                            "old_model_uri": f"models:/{job.model_name}/{job.version}",
                            "test_data": "${feature_engineering.output.test_data_path}",
                            "metrics": ["accuracy", "precision", "recall", "f1", "auc"]
                        }
                    }
                },
                {
                    "name": "register_model",
                    "type": "conditional",
                    "condition": "${evaluate_model.output.improvement} > " + str(policy.get("improvement_threshold", 0.02)),
                    "if_true": {
                        "type": "mlflow_operation",
                        "config": {
                            "operation": "register_model",
                            "model_uri": "${train_model.output.model_uri}",
                            "model_name": job.model_name,
                            "tags": {
                                "retraining_trigger": job.trigger.value,
                                "parent_version": job.version,
                                "improvement": "${evaluate_model.output.improvement}",
                                "auto_retrained": "true"
                            }
                        }
                    },
                    "if_false": {
                        "type": "notification",
                        "config": {
                            "message": "Retrained model did not meet improvement threshold",
                            "severity": "warning"
                        }
                    }
                },
                {
                    "name": "auto_deploy",
                    "type": "conditional",
                    "condition": str(policy.get("auto_deploy", False)).lower() + " and ${evaluate_model.output.improvement} > " + str(policy.get("improvement_threshold", 0.02)),
                    "if_true": {
                        "type": "deployment",
                        "config": {
                            "model_name": job.model_name,
                            "model_version": "${register_model.output.version}",
                            "deployment_config": {
                                "canary_percentage": 10,
                                "monitoring_duration": "2h",
                                "rollback_on_error": True
                            }
                        }
                    }
                }
            ],
            "error_handling": {
                "max_retries": policy.get("max_retries", 3),
                "retry_delay": 300,
                "on_failure": "notify"
            }
        }
        
    async def _monitor_retraining_job(self, job_id: str, workflow_id: str):
        """Monitor retraining job progress"""
        job = self.retraining_jobs[job_id]
        
        while job.status not in ["completed", "failed"]:
            await asyncio.sleep(60)  # Check every minute
            
            # Get workflow status
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.workflow_service_url}/api/v1/workflows/{workflow_id}",
                    headers={"X-Tenant-ID": job.tenant_id}
                )
                
                if response.status_code == 200:
                    workflow_status = response.json()
                    
                    if workflow_status["status"] == "running" and job.status == "submitted":
                        job.status = "running"
                        job.started_at = datetime.utcnow()
                        
                    elif workflow_status["status"] == "completed":
                        job.status = "completed"
                        job.completed_at = datetime.utcnow()
                        
                        # Extract results
                        results = workflow_status.get("results", {})
                        if "register_model" in results:
                            job.new_version = results["register_model"]["version"]
                            
                        if "evaluate_model" in results:
                            job.improvement_metrics = results["evaluate_model"]
                            
                        logger.info(f"Retraining job {job_id} completed successfully")
                        
                    elif workflow_status["status"] == "failed":
                        job.status = "failed"
                        job.completed_at = datetime.utcnow()
                        logger.error(f"Retraining job {job_id} failed")
                        
    async def _get_last_training_time(self, model_name: str, tenant_id: str) -> Optional[datetime]:
        """Get last training time for a model"""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.mlflow_url}/api/2.0/mlflow/registered-models/get",
                params={"name": f"{tenant_id}_{model_name}"}
            )
            
            if response.status_code == 200:
                model_info = response.json()
                if model_info.get("registered_model", {}).get("latest_versions"):
                    latest = model_info["registered_model"]["latest_versions"][0]
                    return datetime.fromtimestamp(latest["creation_timestamp"] / 1000)
                    
        return None
        
    async def _get_feature_config(self, model_name: str) -> Dict[str, Any]:
        """Get feature configuration for model"""
        # In production, fetch from feature store or model metadata
        return {
            "numeric_features": ["feature1", "feature2", "feature3"],
            "categorical_features": ["cat1", "cat2"],
            "text_features": ["text1"],
            "scaling": "standard",
            "encoding": "onehot"
        }
        
    async def get_retraining_status(self, job_id: str) -> Optional[RetrainingJob]:
        """Get status of a retraining job"""
        return self.retraining_jobs.get(job_id)
        
    async def get_retraining_history(self, model_name: str, tenant_id: str) -> List[RetrainingJob]:
        """Get retraining history for a model"""
        key_prefix = f"{tenant_id}:{model_name}"
        return [
            job for job_id, job in self.retraining_jobs.items()
            if job.tenant_id == tenant_id and job.model_name == model_name
        ] 