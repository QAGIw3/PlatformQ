"""
Service Integration API endpoints

Provides endpoints for cross-service operations:
- ML training workflows
- Real-time analytics pipelines  
- Data archival and backup
- Federated learning
- Platform reports
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from fastapi import APIRouter, Request, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field

router = APIRouter(prefix="/integrations", tags=["Service Integrations"])


# Request models

class MLTrainingWorkflowRequest(BaseModel):
    """ML training workflow request"""
    data_source_query: str = Field(..., description="Query to extract training data")
    feature_config: Dict[str, Any] = Field(..., description="Feature engineering configuration")
    model_config: Dict[str, Any] = Field(..., description="Model training configuration")
    training_schedule: Optional[str] = Field(None, description="Cron schedule for recurring training")


class AnalyticsPipelineRequest(BaseModel):
    """Real-time analytics pipeline request"""
    event_sources: List[str] = Field(..., description="Event types to consume")
    analytics_config: Dict[str, Any] = Field(..., description="Analytics configuration")
    dashboard_config: Optional[Dict[str, Any]] = Field(None, description="Dashboard configuration")


class ArchivalWorkflowRequest(BaseModel):
    """Data archival workflow request"""
    dataset_patterns: List[str] = Field(..., description="Dataset patterns to archive")
    archival_policy: Dict[str, Any] = Field(..., description="Archival policy")
    disaster_recovery: bool = Field(False, description="Enable disaster recovery")


class FederatedLearningRequest(BaseModel):
    """Federated learning workflow request"""
    data_query: str = Field(..., description="Query to extract participant data")
    participant_column: str = Field(..., description="Column identifying participants")
    model_config: Dict[str, Any] = Field(..., description="Federated learning configuration")


class DataEventRequest(BaseModel):
    """Data event subscription request"""
    event_types: List[str] = Field(..., description="Event types to subscribe to")
    ingestion_config: Dict[str, Any] = Field(..., description="Ingestion configuration")
    auto_start: bool = Field(True, description="Start consuming immediately")


class ReportGenerationRequest(BaseModel):
    """Platform report generation request"""
    report_type: str = Field("comprehensive", description="Type of report")
    time_range: str = Field("30d", description="Time range for report")
    format: str = Field("pdf", description="Output format")


# ML Platform Integration endpoints

@router.post("/ml/training-workflow")
async def create_ml_training_workflow(
    request: Request,
    workflow: MLTrainingWorkflowRequest
) -> Dict[str, Any]:
    """Create end-to-end ML training workflow"""
    try:
        orchestrator = request.app.state.service_orchestrator
        
        workflow_id = await orchestrator.create_ml_training_workflow(
            data_source_query=workflow.data_source_query,
            feature_config=workflow.feature_config,
            model_config=workflow.model_config,
            training_schedule=workflow.training_schedule
        )
        
        return {
            "workflow_id": workflow_id,
            "status": "created",
            "message": "ML training workflow created successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ml/prepare-training-data")
async def prepare_training_data(
    request: Request,
    source_path: str = Query(..., description="Source data path"),
    experiment_id: Optional[str] = Query(None, description="MLflow experiment ID"),
    feature_config: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Prepare training data from data lake"""
    try:
        ml_integration = request.app.state.ml_integration
        
        result = await ml_integration.prepare_training_data(
            source_path=source_path,
            feature_config=feature_config or {},
            experiment_id=experiment_id
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ml/feature-store")
async def create_feature_store_dataset(
    request: Request,
    query: str = Query(..., description="SQL query for features"),
    feature_group: str = Query(..., description="Feature group name"),
    feature_definitions: List[Dict[str, Any]] = None,
    refresh_schedule: Optional[str] = Query(None, description="Refresh schedule")
) -> Dict[str, Any]:
    """Create feature store dataset"""
    try:
        ml_integration = request.app.state.ml_integration
        
        result = await ml_integration.create_feature_store_dataset(
            query=query,
            feature_group=feature_group,
            feature_definitions=feature_definitions or [],
            refresh_schedule=refresh_schedule
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Event Router Integration endpoints

@router.post("/events/subscribe")
async def subscribe_to_events(
    request: Request,
    subscription: DataEventRequest
) -> Dict[str, Any]:
    """Subscribe to data events for ingestion"""
    try:
        event_integration = request.app.state.event_integration
        
        subscription_id = await event_integration.subscribe_to_data_events(
            event_types=subscription.event_types,
            ingestion_config=subscription.ingestion_config,
            auto_start=subscription.auto_start
        )
        
        return {
            "subscription_id": subscription_id,
            "status": "active" if subscription.auto_start else "created",
            "event_types": subscription.event_types
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/events/blockchain-ingestion")
async def setup_blockchain_ingestion(
    request: Request,
    chains: List[str] = Query(..., description="Blockchain chains"),
    event_types: List[str] = Query(..., description="Event types"),
    target_zone: str = Query("bronze", description="Target lake zone")
) -> Dict[str, Any]:
    """Set up blockchain event ingestion"""
    try:
        event_integration = request.app.state.event_integration
        
        result = await event_integration.ingest_blockchain_events(
            chains=chains,
            event_types=event_types,
            target_zone=target_zone
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/events/pipeline-trigger")
async def register_pipeline_trigger(
    request: Request,
    event_type: str = Query(..., description="Triggering event type"),
    pipeline_id: str = Query(..., description="Pipeline to trigger"),
    trigger_config: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Register event-based pipeline trigger"""
    try:
        event_integration = request.app.state.event_integration
        
        trigger_id = await event_integration.register_pipeline_trigger(
            event_type=event_type,
            pipeline_id=pipeline_id,
            trigger_config=trigger_config or {}
        )
        
        return {
            "trigger_id": trigger_id,
            "event_type": event_type,
            "pipeline_id": pipeline_id,
            "status": "active"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Analytics Integration endpoints

@router.post("/analytics/pipeline")
async def create_analytics_pipeline(
    request: Request,
    pipeline: AnalyticsPipelineRequest
) -> Dict[str, Any]:
    """Create real-time analytics pipeline"""
    try:
        orchestrator = request.app.state.service_orchestrator
        
        pipeline_id = await orchestrator.setup_real_time_analytics_pipeline(
            event_sources=pipeline.event_sources,
            analytics_config=pipeline.analytics_config,
            dashboard_config=pipeline.dashboard_config
        )
        
        return {
            "pipeline_id": pipeline_id,
            "status": "active",
            "message": "Analytics pipeline created successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analytics/unified-query")
async def execute_unified_analytics_query(
    request: Request,
    query: str = Query(..., description="Analytics query"),
    query_type: Optional[str] = Query(None, description="Query type"),
    time_range: str = Query("7d", description="Time range"),
    cache_results: bool = Query(True, description="Cache results")
) -> Dict[str, Any]:
    """Execute unified analytics query"""
    try:
        analytics_integration = request.app.state.analytics_integration
        
        result = await analytics_integration.execute_unified_query(
            query=query,
            query_type=query_type,
            time_range=time_range,
            cache_results=cache_results
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analytics/anomaly-detection")
async def detect_anomalies(
    request: Request,
    data_source: str = Query(..., description="Data source"),
    metrics: List[str] = Query(..., description="Metrics to analyze"),
    time_window: str = Query("1h", description="Time window"),
    sensitivity: float = Query(0.95, description="Detection sensitivity")
) -> List[Dict[str, Any]]:
    """Detect anomalies in data"""
    try:
        analytics_integration = request.app.state.analytics_integration
        
        anomalies = await analytics_integration.detect_anomalies(
            data_source=data_source,
            metrics=metrics,
            time_window=time_window,
            sensitivity=sensitivity
        )
        
        return anomalies
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analytics/forecast")
async def create_forecast(
    request: Request,
    query: str = Query(..., description="Query for historical data"),
    target_column: str = Query(..., description="Column to forecast"),
    horizon_days: int = Query(7, description="Forecast horizon"),
    confidence_interval: float = Query(0.95, description="Confidence interval")
) -> Dict[str, Any]:
    """Create time series forecast"""
    try:
        analytics_integration = request.app.state.analytics_integration
        
        forecast = await analytics_integration.create_time_series_forecast(
            query=query,
            target_column=target_column,
            horizon_days=horizon_days,
            confidence_interval=confidence_interval
        )
        
        return forecast
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Storage Integration endpoints

@router.post("/storage/archive")
async def archive_dataset(
    request: Request,
    dataset_path: str = Query(..., description="Dataset path"),
    archive_policy: Dict[str, Any] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Archive dataset to long-term storage"""
    try:
        storage_integration = request.app.state.storage_integration
        
        result = await storage_integration.archive_dataset(
            dataset_path=dataset_path,
            archive_policy=archive_policy or {"type": "standard", "retention_days": 365},
            metadata=metadata
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/storage/restore")
async def restore_from_archive(
    request: Request,
    archive_id: str = Query(..., description="Archive ID"),
    target_path: Optional[str] = Query(None, description="Target path"),
    target_zone: str = Query("bronze", description="Target zone")
) -> Dict[str, Any]:
    """Restore dataset from archive"""
    try:
        storage_integration = request.app.state.storage_integration
        
        result = await storage_integration.restore_from_archive(
            archive_id=archive_id,
            target_path=target_path,
            target_zone=target_zone
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/storage/workflow")
async def create_archival_workflow(
    request: Request,
    workflow: ArchivalWorkflowRequest
) -> Dict[str, Any]:
    """Create data archival workflow"""
    try:
        orchestrator = request.app.state.service_orchestrator
        
        workflow_id = await orchestrator.create_data_archival_workflow(
            dataset_patterns=workflow.dataset_patterns,
            archival_policy=workflow.archival_policy,
            disaster_recovery=workflow.disaster_recovery
        )
        
        return {
            "workflow_id": workflow_id,
            "status": "active",
            "message": "Archival workflow created successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/storage/cold-migration")
async def migrate_to_cold_storage(
    request: Request,
    age_threshold_days: int = Query(90, description="Age threshold"),
    zones: List[str] = Query(["bronze", "silver"], description="Zones to check"),
    dry_run: bool = Query(True, description="Dry run mode")
) -> Dict[str, Any]:
    """Migrate old data to cold storage"""
    try:
        storage_integration = request.app.state.storage_integration
        
        result = await storage_integration.migrate_to_cold_storage(
            age_threshold_days=age_threshold_days,
            zones=zones,
            dry_run=dry_run
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Orchestrated workflows

@router.post("/workflows/federated-learning")
async def create_federated_learning_workflow(
    request: Request,
    workflow: FederatedLearningRequest
) -> Dict[str, Any]:
    """Create federated learning workflow"""
    try:
        orchestrator = request.app.state.service_orchestrator
        
        workflow_id = await orchestrator.create_federated_learning_workflow(
            data_query=workflow.data_query,
            participant_column=workflow.participant_column,
            model_config=workflow.model_config
        )
        
        return {
            "workflow_id": workflow_id,
            "status": "active",
            "message": "Federated learning workflow created"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reports/generate")
async def generate_platform_report(
    request: Request,
    report: ReportGenerationRequest,
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """Generate platform report"""
    try:
        orchestrator = request.app.state.service_orchestrator
        
        # Generate report in background if it's large
        if report.report_type == "comprehensive":
            task_id = f"report_{datetime.utcnow().timestamp()}"
            
            background_tasks.add_task(
                orchestrator.generate_platform_report,
                report_type=report.report_type,
                time_range=report.time_range,
                format=report.format
            )
            
            return {
                "task_id": task_id,
                "status": "processing",
                "message": "Report generation started"
            }
        else:
            # Generate smaller reports synchronously
            result = await orchestrator.generate_platform_report(
                report_type=report.report_type,
                time_range=report.time_range,
                format=report.format
            )
            
            return result
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflows/{workflow_id}")
async def get_workflow_status(
    request: Request,
    workflow_id: str
) -> Dict[str, Any]:
    """Get workflow status"""
    try:
        orchestrator = request.app.state.service_orchestrator
        
        status = await orchestrator.get_workflow_status(workflow_id)
        
        return status
        
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/workflows/{workflow_id}")
async def cancel_workflow(
    request: Request,
    workflow_id: str
) -> Dict[str, Any]:
    """Cancel active workflow"""
    try:
        orchestrator = request.app.state.service_orchestrator
        
        await orchestrator.cancel_workflow(workflow_id)
        
        return {
            "workflow_id": workflow_id,
            "status": "cancelled",
            "message": "Workflow cancelled successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e)) 