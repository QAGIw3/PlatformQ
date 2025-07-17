"""
MLOps Service

Centralized service for ML model lifecycle management, including:
- Model registry integration
- Deployment orchestration  
- Monitoring configuration
- A/B testing management
- Retraining coordination
- Model marketplace integration
"""

import logging
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import asyncio

from fastapi import Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
import mlflow
from mlflow.tracking import MlflowClient

from platformq.shared.base_service import create_base_app
from platformq.shared.event_publisher import EventPublisher
from platformq_shared.jwt import get_current_tenant_and_user

from .feedback_loop import FeedbackLoopManager, ABTestManager
from .model_registry import ModelRegistryManager
from .deployment_manager import DeploymentManager
from .monitoring_config import MonitoringConfigManager
from .model_marketplace import ModelMarketplaceManager

logger = logging.getLogger(__name__)

# Initialize base app
app = create_base_app(service_name="mlops-service")

# Configuration
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
PULSAR_URL = os.getenv("PULSAR_URL", "pulsar://pulsar:6650")
FUNCTIONS_SERVICE_URL = os.getenv("FUNCTIONS_SERVICE_URL", "http://functions-service:80")
WORKFLOW_SERVICE_URL = os.getenv("WORKFLOW_SERVICE_URL", "http://workflow-service:80")
DIGITAL_ASSET_SERVICE_URL = os.getenv("DIGITAL_ASSET_SERVICE_URL", "http://digital-asset-service:80")

# Initialize MLflow
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow_client = MlflowClient()

# Initialize services
event_publisher = EventPublisher(pulsar_url=PULSAR_URL)
feedback_loop_manager = None
ab_test_manager = None
model_registry_manager = None
deployment_manager = None
monitoring_config_manager = None
model_marketplace_manager = None


# Pydantic models
class ModelRegistrationRequest(BaseModel):
    """Request to register a model"""
    model_name: str = Field(..., description="Name of the model")
    run_id: str = Field(..., description="MLflow run ID")
    description: Optional[str] = Field(None, description="Model description")
    tags: Dict[str, str] = Field(default_factory=dict, description="Model tags")
    stage: Optional[str] = Field("None", description="Initial stage: None, Staging, Production")
    # Marketplace fields
    for_sale: bool = Field(False, description="List model for sale")
    sale_price: Optional[float] = Field(None, description="Sale price in ETH/MATIC")
    licensable: bool = Field(False, description="Make model available for licensing")
    license_terms: Optional[Dict[str, Any]] = Field(None, description="License terms")
    royalty_percentage: int = Field(250, description="Royalty percentage in basis points (250 = 2.5%)")


class ModelDeploymentRequest(BaseModel):
    """Request to deploy a model"""
    model_name: str = Field(..., description="Name of the model")
    version: Optional[str] = Field(None, description="Model version (latest if not specified)")
    environment: str = Field(..., description="Target environment: staging, production")
    deployment_config: Dict[str, Any] = Field(
        default_factory=lambda: {
            "replicas": 2,
            "cpu": "1000m",
            "memory": "2Gi",
            "autoscaling": {"min": 1, "max": 10, "target_cpu": 80}
        }
    )
    canary_config: Optional[Dict[str, Any]] = Field(
        None,
        description="Canary deployment configuration"
    )


class ABTestRequest(BaseModel):
    """Request to start A/B test"""
    model_name: str = Field(..., description="Name of the model")
    version_a: str = Field(..., description="Version A")
    version_b: str = Field(..., description="Version B")
    traffic_split: float = Field(0.5, description="Traffic percentage for version B (0-1)")
    duration_hours: int = Field(24, description="Test duration in hours")


class ModelPurchaseRequest(BaseModel):
    """Request to purchase a model"""
    model_cid: str = Field(..., description="Digital asset CID of the model")
    license_type: str = Field("perpetual", description="License type: perpetual, time_based, usage_based")
    license_duration_days: Optional[int] = Field(None, description="Duration for time-based licenses")
    usage_limit: Optional[int] = Field(None, description="Usage limit for usage-based licenses")


class ModelReviewRequest(BaseModel):
    """Request to review a model"""
    model_cid: str = Field(..., description="Digital asset CID of the model")
    rating: int = Field(..., ge=1, le=5, description="Rating from 1-5")
    review_text: str = Field(..., description="Review text")
    verified_purchase: bool = Field(False, description="Whether this is from a verified purchase")


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    global feedback_loop_manager, ab_test_manager, model_registry_manager
    global deployment_manager, monitoring_config_manager, model_marketplace_manager
    
    # Connect event publisher
    event_publisher.connect()
    
    # Initialize managers
    model_registry_manager = ModelRegistryManager(
        mlflow_client, 
        event_publisher,
        digital_asset_service_url=DIGITAL_ASSET_SERVICE_URL
    )
    deployment_manager = DeploymentManager(FUNCTIONS_SERVICE_URL, mlflow_client)
    monitoring_config_manager = MonitoringConfigManager()
    ab_test_manager = ABTestManager(monitoring_service_url="http://monitoring-service:80")
    
    model_marketplace_manager = ModelMarketplaceManager(
        digital_asset_service_url=DIGITAL_ASSET_SERVICE_URL,
        blockchain_service_url="http://blockchain-event-bridge:80"
    )
    
    feedback_loop_manager = FeedbackLoopManager(
        pulsar_url=PULSAR_URL,
        mlflow_url=MLFLOW_TRACKING_URI,
        workflow_service_url=WORKFLOW_SERVICE_URL,
        event_publisher=event_publisher
    )
    
    # Start feedback loop in background
    asyncio.create_task(feedback_loop_manager.start())
    
    logger.info("MLOps service initialized")


@app.on_event("shutdown")
def shutdown_event():
    """Cleanup on shutdown"""
    event_publisher.disconnect()


# API Endpoints

@app.post("/api/v1/models/register", response_model=Dict[str, Any])
async def register_model(
    request: ModelRegistrationRequest,
    context: dict = Depends(get_current_tenant_and_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Register a new model version in MLflow registry and as digital asset"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    
    try:
        # Add tenant prefix to model name
        full_model_name = f"{tenant_id}_{request.model_name}"
        
        # Register model as digital asset if for sale or licensable
        if request.for_sale or request.licensable:
            model_version = await model_registry_manager.register_model_as_asset(
                model_name=full_model_name,
                run_id=request.run_id,
                tenant_id=tenant_id,
                user_id=user_id,
                description=request.description,
                tags={
                    **request.tags,
                    "tenant_id": tenant_id,
                    "registered_by": user_id,
                    "registration_time": datetime.utcnow().isoformat()
                },
                license_terms=request.license_terms,
                sale_price=request.sale_price,
                royalty_percentage=request.royalty_percentage
            )
        else:
            # Traditional registration
            model_version = model_registry_manager.register_model(
                model_name=full_model_name,
                run_id=request.run_id,
                description=request.description,
                tags={
                    **request.tags,
                    "tenant_id": tenant_id,
                    "registered_by": user_id,
                    "registration_time": datetime.utcnow().isoformat()
                }
            )
        
        # Transition to initial stage if specified
        if request.stage and request.stage != "None":
            model_registry_manager.transition_model_stage(
                model_name=full_model_name,
                version=model_version.version,
                stage=request.stage,
                archive_existing=True
            )
        
        # Trigger validation in background
        background_tasks.add_task(
            model_registry_manager.validate_model,
            full_model_name,
            model_version.version
        )
        
        # Get digital asset CID if available
        asset_cid = None
        if hasattr(model_version, 'tags'):
            for tag in model_version.tags:
                if tag.key == "digital_asset_cid":
                    asset_cid = tag.value
                    break
        
        return {
            "model_name": request.model_name,
            "version": model_version.version,
            "status": "registered",
            "stage": request.stage or "None",
            "run_id": request.run_id,
            "digital_asset_cid": asset_cid,
            "marketplace_status": "listed" if (request.for_sale or request.licensable) else "private"
        }
        
    except Exception as e:
        logger.error(f"Failed to register model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/models/{model_name}/versions", response_model=List[Dict[str, Any]])
async def list_model_versions(
    model_name: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """List all versions of a model"""
    tenant_id = context["tenant_id"]
    full_model_name = f"{tenant_id}_{model_name}"
    
    try:
        versions = model_registry_manager.list_model_versions(full_model_name)
        
        return [
            {
                "version": v.version,
                "stage": v.current_stage,
                "description": v.description,
                "tags": v.tags,
                "creation_time": v.creation_timestamp,
                "last_updated": v.last_updated_timestamp,
                "run_id": v.run_id,
                "digital_asset_cid": next((tag.value for tag in v.tags if tag.key == "digital_asset_cid"), None)
            }
            for v in versions
        ]
        
    except Exception as e:
        logger.error(f"Failed to list model versions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/deployments/create", response_model=Dict[str, Any])
async def deploy_model(
    request: ModelDeploymentRequest,
    context: dict = Depends(get_current_tenant_and_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Deploy a model to specified environment"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    
    try:
        # Deploy model
        deployment_result = await deployment_manager.deploy_model(
            tenant_id=tenant_id,
            model_name=request.model_name,
            version=request.version,
            environment=request.environment,
            deployment_config=request.deployment_config,
            canary_config=request.canary_config
        )
        
        # Configure monitoring in background
        if deployment_result["status"] == "deployed":
            background_tasks.add_task(
                monitoring_config_manager.configure_monitoring,
                tenant_id,
                request.model_name,
                deployment_result["version"],
                request.environment
            )
        
        return deployment_result
        
    except Exception as e:
        logger.error(f"Failed to deploy model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/deployments", response_model=List[Dict[str, Any]])
async def list_deployments(
    context: dict = Depends(get_current_tenant_and_user)
):
    """List all model deployments for tenant"""
    tenant_id = context["tenant_id"]
    
    try:
        deployments = await deployment_manager.list_deployments(tenant_id)
        return deployments
        
    except Exception as e:
        logger.error(f"Failed to list deployments: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/ab-tests/start", response_model=Dict[str, Any])
async def start_ab_test(
    request: ABTestRequest,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Start A/B test between two model versions"""
    tenant_id = context["tenant_id"]
    
    try:
        test_id = await ab_test_manager.start_ab_test(
            model_name=f"{tenant_id}_{request.model_name}",
            version_a=request.version_a,
            version_b=request.version_b,
            traffic_split=request.traffic_split,
            duration_hours=request.duration_hours
        )
        
        return {
            "test_id": test_id,
            "status": "started",
            "model_name": request.model_name,
            "version_a": request.version_a,
            "version_b": request.version_b,
            "traffic_split": request.traffic_split,
            "end_time": datetime.utcnow() + timedelta(hours=request.duration_hours)
        }
        
    except Exception as e:
        logger.error(f"Failed to start A/B test: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/ab-tests/{test_id}/results", response_model=Dict[str, Any])
async def get_ab_test_results(
    test_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get A/B test results"""
    try:
        results = await ab_test_manager.get_test_results(test_id)
        return results
        
    except Exception as e:
        logger.error(f"Failed to get A/B test results: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/models/{model_name}/promote", response_model=Dict[str, Any])
async def promote_model(
    model_name: str,
    version: str,
    target_stage: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Promote model to a new stage"""
    tenant_id = context["tenant_id"]
    full_model_name = f"{tenant_id}_{model_name}"
    
    if target_stage not in ["Staging", "Production"]:
        raise HTTPException(status_code=400, detail="Invalid target stage")
    
    try:
        model_registry_manager.transition_model_stage(
            model_name=full_model_name,
            version=version,
            stage=target_stage,
            archive_existing=True
        )
        
        return {
            "model_name": model_name,
            "version": version,
            "stage": target_stage,
            "status": "promoted"
        }
        
    except Exception as e:
        logger.error(f"Failed to promote model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/models/{model_name}/metrics", response_model=Dict[str, Any])
async def get_model_metrics(
    model_name: str,
    version: Optional[str] = None,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get model performance metrics"""
    tenant_id = context["tenant_id"]
    
    try:
        metrics = await monitoring_config_manager.get_model_metrics(
            tenant_id=tenant_id,
            model_name=model_name,
            version=version
        )
        
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to get model metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/models/{model_name}/lineage", response_model=List[Dict[str, Any]])
async def get_model_lineage(
    model_name: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get model training lineage"""
    tenant_id = context["tenant_id"]
    full_model_name = f"{tenant_id}_{model_name}"
    
    try:
        lineage = model_registry_manager.get_model_lineage(full_model_name)
        return lineage
        
    except Exception as e:
        logger.error(f"Failed to get model lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/models/{model_name}/check-license")
async def check_model_license(
    model_name: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Check if user has valid license for model"""
    try:
        tenant_id = context["tenant_id"]
        user_id = context["user_id"]
        
        # Get model's digital asset CID
        full_model_name = f"{tenant_id}_{model_name}"
        versions = model_registry_manager.list_model_versions(full_model_name)
        if not versions:
            raise HTTPException(status_code=404, detail="Model not found")
        
        latest_version = versions[0]
        asset_cid = next((tag.value for tag in latest_version.tags if tag.key == "digital_asset_cid"), None)
        
        if not asset_cid:
            return {"has_license": True, "reason": "Model not marketplace-enabled"}
        
        # Check license via marketplace manager
        has_license = await model_marketplace_manager.check_user_license(
            user_id=user_id,
            tenant_id=tenant_id,
            model_cid=asset_cid
        )
        
        return {
            "has_license": has_license,
            "model_name": model_name,
            "asset_cid": asset_cid
        }
        
    except Exception as e:
        logger.error(f"Failed to check license: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Marketplace endpoints

@app.get("/api/v1/marketplace/models", response_model=List[Dict[str, Any]])
async def browse_model_marketplace(
    framework: Optional[str] = None,
    min_accuracy: Optional[float] = None,
    max_price: Optional[float] = None,
    license_type: Optional[str] = None,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Browse available models in the marketplace"""
    try:
        models = await model_marketplace_manager.browse_models(
            framework=framework,
            min_accuracy=min_accuracy,
            max_price=max_price,
            license_type=license_type,
            exclude_tenant=context["tenant_id"]  # Don't show own models
        )
        
        return models
        
    except Exception as e:
        logger.error(f"Failed to browse marketplace: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/marketplace/purchase", response_model=Dict[str, Any])
async def purchase_model(
    request: ModelPurchaseRequest,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Purchase or license a model from the marketplace"""
    try:
        purchase_result = await model_marketplace_manager.purchase_model(
            buyer_id=context["user_id"],
            buyer_tenant_id=context["tenant_id"],
            model_cid=request.model_cid,
            license_type=request.license_type,
            license_duration_days=request.license_duration_days,
            usage_limit=request.usage_limit
        )
        
        return purchase_result
        
    except Exception as e:
        logger.error(f"Failed to purchase model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/marketplace/reviews", response_model=Dict[str, Any])
async def submit_model_review(
    request: ModelReviewRequest,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Submit a review for a model"""
    try:
        review_result = await model_marketplace_manager.submit_review(
            reviewer_id=context["user_id"],
            model_cid=request.model_cid,
            rating=request.rating,
            review_text=request.review_text,
            verified_purchase=request.verified_purchase
        )
        
        return review_result
        
    except Exception as e:
        logger.error(f"Failed to submit review: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/marketplace/models/{model_cid}/reviews", response_model=List[Dict[str, Any]])
async def get_model_reviews(
    model_cid: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get reviews for a model"""
    try:
        reviews = await model_marketplace_manager.get_model_reviews(model_cid)
        return reviews
        
    except Exception as e:
        logger.error(f"Failed to get reviews: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/marketplace/analytics/top-models", response_model=List[Dict[str, Any]])
async def get_top_models(
    period: str = "week",  # week, month, all-time
    metric: str = "revenue",  # revenue, downloads, rating
    limit: int = 10,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get top performing models in marketplace"""
    try:
        top_models = await model_marketplace_manager.get_top_models(
            period=period,
            metric=metric,
            limit=limit
        )
        
        return top_models
        
    except Exception as e:
        logger.error(f"Failed to get top models: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/marketplace/revenue/{model_name}", response_model=Dict[str, Any])
async def get_model_revenue(
    model_name: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get revenue analytics for a model"""
    tenant_id = context["tenant_id"]
    
    try:
        revenue_data = await model_marketplace_manager.get_model_revenue(
            tenant_id=tenant_id,
            model_name=model_name
        )
        
        return revenue_data
        
    except Exception as e:
        logger.error(f"Failed to get revenue data: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 


@app.get("/api/v1/marketplace/analytics/{model_name}")
async def get_marketplace_analytics(
    model_name: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get marketplace analytics for a model"""
    try:
        tenant_id = context["tenant_id"]
        user_id = context["user_id"]
        full_model_name = f"{tenant_id}_{model_name}"
        
        # Get model versions
        versions = model_registry_manager.list_model_versions(full_model_name)
        if not versions:
            raise HTTPException(status_code=404, detail="Model not found")
        
        # Check ownership
        first_version = versions[0]
        owner_tag = next((tag.value for tag in first_version.tags if tag.key == "registered_by"), None)
        if owner_tag != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to view analytics")
        
        # Get analytics from marketplace manager
        analytics = await model_marketplace_manager.get_model_analytics(
            model_name=full_model_name,
            tenant_id=tenant_id
        )
        
        return {
            "model_name": model_name,
            "total_sales": analytics.get("total_sales", 0),
            "total_revenue": analytics.get("total_revenue", 0),
            "total_licenses": analytics.get("total_licenses", 0),
            "active_licenses": analytics.get("active_licenses", 0),
            "royalties_earned": analytics.get("royalties_earned", 0),
            "average_rating": analytics.get("average_rating", 0),
            "download_count": analytics.get("download_count", 0),
            "revenue_by_month": analytics.get("revenue_by_month", {}),
            "top_buyers": analytics.get("top_buyers", [])
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get marketplace analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 