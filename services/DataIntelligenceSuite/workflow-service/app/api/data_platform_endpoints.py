"""
Data Platform Workflow API Endpoints

Provides endpoints for triggering data platform integration workflows.
"""

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

from ..api.deps import get_current_tenant_and_user
from ..airflow_bridge import AirflowBridge

router = APIRouter()
logger = logging.getLogger(__name__)


class MLTrainingWorkflowRequest(BaseModel):
    """Request to trigger ML training workflow"""
    dataset_id: str = Field(..., description="Dataset ID to use for training")
    experiment_name: str = Field(..., description="ML experiment name")
    model_config: Dict[str, Any] = Field(..., description="Model configuration")
    training_config: Dict[str, Any] = Field(default_factory=dict, description="Training configuration")
    export_to_feature_store: bool = Field(False, description="Export features to feature store")
    feature_group_name: Optional[str] = Field(None, description="Feature group name if exporting")


class RealtimeAnalyticsRequest(BaseModel):
    """Request to set up real-time analytics pipeline"""
    pipeline_name: str = Field(..., description="Name for the analytics pipeline")
    event_types: List[str] = Field(..., description="Event types to subscribe to")
    transformations: Optional[List[Dict[str, Any]]] = Field(None, description="Data transformations")
    sink_type: str = Field("elasticsearch", description="Sink type for processed data")
    sink_index: str = Field(..., description="Index/table for sink")
    enable_anomaly_detection: bool = Field(True, description="Enable anomaly detection")
    visualizations: Optional[List[Dict[str, Any]]] = Field(None, description="Dashboard visualizations")


class DataArchivalRequest(BaseModel):
    """Request to trigger data archival workflow"""
    archive_policies: Optional[Dict[str, Any]] = Field(None, description="Archival policies")
    backup_type: str = Field("incremental", description="Backup type")
    archive_tier: str = Field("glacier", description="Archive storage tier")
    delete_original: bool = Field(False, description="Delete original after archival")
    enable_disaster_recovery: bool = Field(True, description="Enable disaster recovery")
    report_recipients: List[str] = Field(default_factory=list, description="Report email recipients")


@router.post("/workflows/data-platform/ml-training", response_model=Dict[str, Any])
async def trigger_ml_training_workflow(
    request: MLTrainingWorkflowRequest,
    airflow_bridge: AirflowBridge = Depends(lambda: router.app.state.airflow_bridge),
    context: dict = Depends(get_current_tenant_and_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Trigger ML training workflow using data platform datasets"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    
    try:
        # Prepare DAG configuration
        dag_conf = {
            "tenant_id": tenant_id,
            "triggered_by": user_id,
            "dataset_id": request.dataset_id,
            "experiment_name": request.experiment_name,
            "model_config": request.model_config,
            "training_config": request.training_config,
            "export_to_feature_store": request.export_to_feature_store,
            "feature_group_name": request.feature_group_name,
            "triggered_at": datetime.utcnow().isoformat()
        }
        
        # Trigger DAG
        result = airflow_bridge.trigger_dag(
            dag_id="data_ml_training_workflow",
            conf=dag_conf
        )
        
        logger.info(f"Triggered ML training workflow: {result['dag_run_id']}")
        
        return {
            "workflow_id": result["dag_run_id"],
            "status": "triggered",
            "message": f"ML training workflow triggered for dataset {request.dataset_id}",
            "experiment_name": request.experiment_name,
            "estimated_duration_minutes": 30
        }
        
    except Exception as e:
        logger.error(f"Failed to trigger ML training workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workflows/data-platform/realtime-analytics", response_model=Dict[str, Any])
async def setup_realtime_analytics_pipeline(
    request: RealtimeAnalyticsRequest,
    airflow_bridge: AirflowBridge = Depends(lambda: router.app.state.airflow_bridge),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Set up real-time analytics pipeline"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    
    try:
        # Prepare DAG configuration
        dag_conf = {
            "tenant_id": tenant_id,
            "triggered_by": user_id,
            "pipeline_name": request.pipeline_name,
            "event_types": request.event_types,
            "transformations": request.transformations,
            "sink_type": request.sink_type,
            "sink_index": request.sink_index,
            "enable_anomaly_detection": request.enable_anomaly_detection,
            "visualizations": request.visualizations,
            "triggered_at": datetime.utcnow().isoformat()
        }
        
        # Trigger DAG
        result = airflow_bridge.trigger_dag(
            dag_id="realtime_analytics_pipeline",
            conf=dag_conf
        )
        
        logger.info(f"Triggered real-time analytics pipeline setup: {result['dag_run_id']}")
        
        return {
            "workflow_id": result["dag_run_id"],
            "status": "triggered",
            "message": f"Real-time analytics pipeline '{request.pipeline_name}' setup initiated",
            "event_types": request.event_types,
            "sink": f"{request.sink_type}:{request.sink_index}"
        }
        
    except Exception as e:
        logger.error(f"Failed to setup real-time analytics pipeline: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workflows/data-platform/archival", response_model=Dict[str, Any])
async def trigger_data_archival_workflow(
    request: DataArchivalRequest,
    airflow_bridge: AirflowBridge = Depends(lambda: router.app.state.airflow_bridge),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Trigger data archival and lifecycle management workflow"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    
    try:
        # Prepare DAG configuration
        dag_conf = {
            "tenant_id": tenant_id,
            "triggered_by": user_id,
            "archive_policies": request.archive_policies,
            "backup_type": request.backup_type,
            "archive_tier": request.archive_tier,
            "delete_original": request.delete_original,
            "enable_disaster_recovery": request.enable_disaster_recovery,
            "report_recipients": request.report_recipients,
            "triggered_at": datetime.utcnow().isoformat()
        }
        
        # Trigger DAG
        result = airflow_bridge.trigger_dag(
            dag_id="data_archival_workflow",
            conf=dag_conf
        )
        
        logger.info(f"Triggered data archival workflow: {result['dag_run_id']}")
        
        return {
            "workflow_id": result["dag_run_id"],
            "status": "triggered",
            "message": "Data archival workflow initiated",
            "archive_tier": request.archive_tier,
            "disaster_recovery_enabled": request.enable_disaster_recovery
        }
        
    except Exception as e:
        logger.error(f"Failed to trigger data archival workflow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflows/data-platform/status/{workflow_id}", response_model=Dict[str, Any])
async def get_data_platform_workflow_status(
    workflow_id: str,
    airflow_bridge: AirflowBridge = Depends(lambda: router.app.state.airflow_bridge),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get status of a data platform workflow"""
    try:
        # Determine DAG ID from workflow ID
        dag_id = None
        if "ml_training" in workflow_id:
            dag_id = "data_ml_training_workflow"
        elif "realtime_analytics" in workflow_id:
            dag_id = "realtime_analytics_pipeline"
        elif "archival" in workflow_id:
            dag_id = "data_archival_workflow"
        
        if not dag_id:
            raise HTTPException(status_code=404, detail="Workflow not found")
        
        # Get DAG run status
        status = airflow_bridge.get_dag_run_status(dag_id, workflow_id)
        
        return {
            "workflow_id": workflow_id,
            "dag_id": dag_id,
            "state": status.get("state"),
            "start_date": status.get("start_date"),
            "end_date": status.get("end_date"),
            "execution_date": status.get("execution_date"),
            "conf": status.get("conf", {})
        }
        
    except Exception as e:
        logger.error(f"Failed to get workflow status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflows/data-platform/templates", response_model=Dict[str, Any])
async def get_workflow_templates():
    """Get available data platform workflow templates"""
    templates = {
        "ml_training": {
            "name": "ML Training on Data Lake",
            "description": "Train machine learning models using datasets from the data lake",
            "required_params": ["dataset_id", "experiment_name", "model_config"],
            "optional_params": ["training_config", "export_to_feature_store"],
            "example_config": {
                "model_config": {
                    "model_type": "classification",
                    "algorithm": "random_forest",
                    "features": ["feature1", "feature2"],
                    "target": "label"
                },
                "training_config": {
                    "validation_split": 0.2,
                    "hyperparameters": {
                        "n_estimators": 100,
                        "max_depth": 10
                    }
                }
            }
        },
        "realtime_analytics": {
            "name": "Real-time Analytics Pipeline",
            "description": "Set up streaming analytics pipeline for real-time data processing",
            "required_params": ["pipeline_name", "event_types", "sink_index"],
            "optional_params": ["transformations", "visualizations"],
            "example_config": {
                "event_types": ["user_activity", "system_metrics"],
                "transformations": [
                    {"type": "filter", "condition": "status == 'active'"},
                    {"type": "aggregate", "window": "5m", "function": "count"}
                ]
            }
        },
        "data_archival": {
            "name": "Data Archival and Lifecycle",
            "description": "Archive old data and manage data lifecycle policies",
            "required_params": [],
            "optional_params": ["archive_policies", "backup_type", "archive_tier"],
            "example_config": {
                "archive_policies": {
                    "age_days": 90,
                    "access_count_threshold": 5,
                    "size_threshold_gb": 100
                }
            }
        }
    }
    
    return {
        "templates": templates,
        "total": len(templates)
    } 