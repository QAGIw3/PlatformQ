"""
Trust-Weighted Data Marketplace API Endpoints

Endpoints for data quality assessment, trust-based access control,
dynamic pricing, and data quality derivatives.
"""

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
import logging

from app.engines.trust_weighted_data_engine import (
    TrustWeightedDataEngine,
    AccessLevel,
    DataQualityDimension
)
from app.integrations.seatunnel_quality_integration import SeaTunnelQualityIntegration
from app.deps import (
    get_current_user,
    get_trust_engine,
    get_seatunnel_integration,
    verify_dataset_owner
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/trust-data", tags=["trust-weighted-data"])


# Request/Response Models
class QualityAssessmentRequest(BaseModel):
    """Request for data quality assessment"""
    dataset_id: str
    asset_id: str
    data_uri: str
    schema_info: Optional[Dict[str, Any]] = None
    automated_pipeline: bool = False


class QualityAssessmentResponse(BaseModel):
    """Response for data quality assessment"""
    assessment_id: str
    dataset_id: str
    overall_quality_score: float
    trust_adjusted_score: float
    quality_dimensions: Dict[str, float]
    quality_issues: List[Dict[str, Any]]
    assessor_trust_score: float
    timestamp: datetime


class DataAccessRequest(BaseModel):
    """Request for data access"""
    dataset_id: str
    access_level: AccessLevel
    purpose: str
    duration_days: Optional[int] = None
    volume: Optional[int] = None


class DataAccessResponse(BaseModel):
    """Response for data access request"""
    granted: bool
    reason: str
    access_details: Optional[Dict[str, Any]] = None
    access_uri: Optional[str] = None
    price: Optional[str] = None
    expires_at: Optional[datetime] = None


class PricingRequest(BaseModel):
    """Request for dynamic pricing"""
    dataset_id: str
    access_level: AccessLevel
    volume: Optional[int] = None


class PricingResponse(BaseModel):
    """Response for pricing request"""
    base_price: str
    final_price: str
    quality_multiplier: float
    trust_discount: float
    volume_discount: float = 0.0
    price_breakdown: Dict[str, Any]


class QualityDerivativeRequest(BaseModel):
    """Request to create a quality derivative"""
    dataset_id: str
    derivative_type: str = Field(..., regex="^(QUALITY_FUTURE|FRESHNESS_SWAP|ACCURACY_OPTION|COMPLETENESS_BOND)$")
    strike_quality: float = Field(..., ge=0.0, le=1.0)
    expiry_days: int = Field(..., ge=1, le=365)


class QualityDerivativeResponse(BaseModel):
    """Response for quality derivative creation"""
    derivative_id: str
    dataset_id: str
    derivative_type: str
    strike_quality: float
    current_quality: float
    expiry: datetime
    premium: str
    collateral_required: str


class TrustAccessConfigRequest(BaseModel):
    """Request to configure trust-based access"""
    dataset_id: str
    access_configs: List[Dict[str, Any]]
    pricing_tiers: List[Dict[str, Any]]


# Endpoints
@router.post("/assess-quality", response_model=QualityAssessmentResponse)
async def assess_data_quality(
    request: QualityAssessmentRequest,
    background_tasks: BackgroundTasks,
    current_user: Dict = Depends(get_current_user),
    trust_engine: TrustWeightedDataEngine = Depends(get_trust_engine),
    seatunnel: SeaTunnelQualityIntegration = Depends(get_seatunnel_integration)
):
    """
    Perform data quality assessment with trust weighting
    """
    try:
        # Verify user has permission to assess this dataset
        if not await verify_dataset_access(request.dataset_id, current_user["user_id"], "assess"):
            raise HTTPException(status_code=403, detail="Not authorized to assess this dataset")
            
        # Perform quality assessment
        assessment = await trust_engine.assess_data_quality(
            dataset_id=request.dataset_id,
            asset_id=request.asset_id,
            assessor_id=current_user["user_id"],
            data_uri=request.data_uri,
            schema_info=request.schema_info
        )
        
        # Create automated pipeline if requested
        if request.automated_pipeline:
            background_tasks.add_task(
                create_quality_pipeline,
                seatunnel,
                request.dataset_id,
                request.data_uri,
                request.schema_info
            )
            
        return QualityAssessmentResponse(
            assessment_id=assessment.assessment_id,
            dataset_id=assessment.dataset_id,
            overall_quality_score=assessment.overall_quality_score,
            trust_adjusted_score=assessment.trust_adjusted_score,
            quality_dimensions={
                dim.value: score 
                for dim, score in assessment.quality_dimensions.items()
            },
            quality_issues=assessment.quality_issues,
            assessor_trust_score=assessment.assessor_trust_score,
            timestamp=assessment.timestamp
        )
        
    except Exception as e:
        logger.error(f"Error assessing data quality: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/request-access", response_model=DataAccessResponse)
async def request_data_access(
    request: DataAccessRequest,
    current_user: Dict = Depends(get_current_user),
    trust_engine: TrustWeightedDataEngine = Depends(get_trust_engine)
):
    """
    Request access to dataset based on trust scores
    """
    try:
        # Check access permission
        has_access, reason, access_details = await trust_engine.check_access_permission(
            dataset_id=request.dataset_id,
            user_id=current_user["user_id"],
            requested_access=request.access_level,
            purpose=request.purpose
        )
        
        response = DataAccessResponse(
            granted=has_access,
            reason=reason,
            access_details=access_details
        )
        
        if has_access:
            # Calculate price
            price = await trust_engine.calculate_dynamic_price(
                dataset_id=request.dataset_id,
                user_id=current_user["user_id"],
                access_level=request.access_level,
                volume=request.volume
            )
            
            response.price = str(price)
            
            # Generate access URI and expiry
            access_uri, expires_at = await generate_access_credentials(
                request.dataset_id,
                current_user["user_id"],
                request.access_level,
                request.duration_days or access_details.get("expires_in_hours", 24) // 24
            )
            
            response.access_uri = access_uri
            response.expires_at = expires_at
            
        return response
        
    except Exception as e:
        logger.error(f"Error processing access request: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/calculate-price", response_model=PricingResponse)
async def calculate_dynamic_price(
    request: PricingRequest,
    current_user: Dict = Depends(get_current_user),
    trust_engine: TrustWeightedDataEngine = Depends(get_trust_engine)
):
    """
    Calculate dynamic price for dataset access
    """
    try:
        # Get base pricing info
        pricing_info = await trust_engine.get_pricing_info(
            dataset_id=request.dataset_id,
            user_id=current_user["user_id"],
            access_level=request.access_level
        )
        
        # Calculate final price
        final_price = await trust_engine.calculate_dynamic_price(
            dataset_id=request.dataset_id,
            user_id=current_user["user_id"],
            access_level=request.access_level,
            volume=request.volume
        )
        
        return PricingResponse(
            base_price=str(pricing_info["base_price"]),
            final_price=str(final_price),
            quality_multiplier=pricing_info["quality_multiplier"],
            trust_discount=pricing_info["trust_discount"],
            volume_discount=pricing_info.get("volume_discount", 0.0),
            price_breakdown=pricing_info["breakdown"]
        )
        
    except Exception as e:
        logger.error(f"Error calculating price: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/create-derivative", response_model=QualityDerivativeResponse)
async def create_quality_derivative(
    request: QualityDerivativeRequest,
    current_user: Dict = Depends(get_current_user),
    trust_engine: TrustWeightedDataEngine = Depends(get_trust_engine)
):
    """
    Create a data quality derivative instrument
    """
    try:
        # Calculate expiry date
        expiry = datetime.utcnow() + timedelta(days=request.expiry_days)
        
        # Create derivative
        derivative = await trust_engine.create_quality_derivative(
            dataset_id=request.dataset_id,
            derivative_type=request.derivative_type,
            strike_quality=request.strike_quality,
            expiry=expiry,
            creator_id=current_user["user_id"]
        )
        
        return QualityDerivativeResponse(
            derivative_id=derivative["derivative_id"],
            dataset_id=derivative["dataset_id"],
            derivative_type=derivative["derivative_type"],
            strike_quality=derivative["strike_quality"],
            current_quality=derivative["current_quality"],
            expiry=derivative["expiry"],
            premium=derivative["premium"],
            collateral_required=derivative["collateral_required"]
        )
        
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Error creating quality derivative: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/configure-access")
async def configure_trust_access(
    request: TrustAccessConfigRequest,
    current_user: Dict = Depends(get_current_user),
    trust_engine: TrustWeightedDataEngine = Depends(get_trust_engine),
    _: None = Depends(lambda: verify_dataset_owner(request.dataset_id, current_user["user_id"]))
):
    """
    Configure trust-based access controls for a dataset
    """
    try:
        # Update access configurations
        await trust_engine.update_access_configs(
            dataset_id=request.dataset_id,
            access_configs=request.access_configs,
            pricing_tiers=request.pricing_tiers
        )
        
        return {
            "status": "success",
            "message": "Access configuration updated",
            "dataset_id": request.dataset_id
        }
        
    except Exception as e:
        logger.error(f"Error configuring access: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quality-history/{dataset_id}")
async def get_quality_history(
    dataset_id: str,
    limit: int = Query(10, le=100),
    current_user: Dict = Depends(get_current_user),
    trust_engine: TrustWeightedDataEngine = Depends(get_trust_engine)
):
    """
    Get quality assessment history for a dataset
    """
    try:
        # Verify user has access to view history
        if not await verify_dataset_access(dataset_id, current_user["user_id"], "view"):
            raise HTTPException(status_code=403, detail="Not authorized to view this dataset")
            
        # Get quality history
        history = await trust_engine.get_quality_history(dataset_id, limit)
        
        return {
            "dataset_id": dataset_id,
            "assessments": history,
            "trend": calculate_quality_trend(history)
        }
        
    except Exception as e:
        logger.error(f"Error getting quality history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/access-logs/{dataset_id}")
async def get_access_logs(
    dataset_id: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    current_user: Dict = Depends(get_current_user),
    trust_engine: TrustWeightedDataEngine = Depends(get_trust_engine),
    _: None = Depends(lambda: verify_dataset_owner(dataset_id, current_user["user_id"]))
):
    """
    Get access logs for a dataset (owner only)
    """
    try:
        # Get access logs
        logs = await trust_engine.get_access_logs(
            dataset_id=dataset_id,
            start_date=start_date,
            end_date=end_date
        )
        
        # Calculate analytics
        analytics = calculate_access_analytics(logs)
        
        return {
            "dataset_id": dataset_id,
            "access_logs": logs,
            "analytics": analytics
        }
        
    except Exception as e:
        logger.error(f"Error getting access logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/quality-alert-subscription")
async def subscribe_quality_alerts(
    dataset_id: str,
    min_quality_threshold: float = Field(..., ge=0.0, le=1.0),
    alert_channels: List[str] = Field(..., description="email, webhook, pulsar"),
    current_user: Dict = Depends(get_current_user),
    seatunnel: SeaTunnelQualityIntegration = Depends(get_seatunnel_integration)
):
    """
    Subscribe to quality alerts for a dataset
    """
    try:
        # Create alert configuration
        alert_config = {
            "type": alert_channels[0] if alert_channels else "email",
            "channels": alert_channels,
            "user_id": current_user["user_id"],
            "email": current_user.get("email")
        }
        
        # Create streaming monitor
        job_id = await seatunnel.create_streaming_quality_monitor(
            dataset_id=dataset_id,
            stream_source={
                "type": "pulsar",
                "servers": ["pulsar://pulsar:6650"],
                "topic": f"persistent://platformq/public/data-stream-{dataset_id}"
            },
            quality_thresholds={"min_quality": min_quality_threshold},
            alert_config=alert_config
        )
        
        return {
            "status": "success",
            "subscription_id": job_id,
            "dataset_id": dataset_id,
            "threshold": min_quality_threshold,
            "channels": alert_channels
        }
        
    except Exception as e:
        logger.error(f"Error creating quality alert subscription: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Helper functions
async def verify_dataset_access(dataset_id: str, user_id: str, action: str) -> bool:
    """Verify user has access to perform action on dataset"""
    # Implementation depends on your authorization logic
    return True  # Placeholder


async def generate_access_credentials(
    dataset_id: str,
    user_id: str,
    access_level: AccessLevel,
    duration_days: int
) -> tuple[str, datetime]:
    """Generate temporary access credentials"""
    # Implementation for generating secure access URIs
    expires_at = datetime.utcnow() + timedelta(days=duration_days)
    access_uri = f"s3://platformq-data/{dataset_id}/access/{user_id}/{access_level.value}"
    return access_uri, expires_at


def calculate_quality_trend(history: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate quality trend from history"""
    if not history:
        return {"trend": "unknown", "change": 0.0}
        
    recent_scores = [h["trust_adjusted_score"] for h in history[:5]]
    older_scores = [h["trust_adjusted_score"] for h in history[5:10]]
    
    if not older_scores:
        return {"trend": "stable", "change": 0.0}
        
    recent_avg = sum(recent_scores) / len(recent_scores)
    older_avg = sum(older_scores) / len(older_scores)
    change = recent_avg - older_avg
    
    if change > 0.05:
        trend = "improving"
    elif change < -0.05:
        trend = "declining"
    else:
        trend = "stable"
        
    return {"trend": trend, "change": change}


def calculate_access_analytics(logs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate analytics from access logs"""
    if not logs:
        return {}
        
    total_accesses = len(logs)
    unique_users = len(set(log["user_id"] for log in logs))
    
    access_levels = {}
    for log in logs:
        level = log.get("access_level", "unknown")
        access_levels[level] = access_levels.get(level, 0) + 1
        
    avg_trust_score = sum(log.get("user_trust_score", 0) for log in logs) / total_accesses
    
    return {
        "total_accesses": total_accesses,
        "unique_users": unique_users,
        "access_levels": access_levels,
        "average_user_trust": avg_trust_score
    }


async def create_quality_pipeline(
    seatunnel: SeaTunnelQualityIntegration,
    dataset_id: str,
    data_uri: str,
    schema_info: Optional[Dict[str, Any]]
):
    """Background task to create quality assessment pipeline"""
    try:
        # Determine data source from URI
        if data_uri.startswith("s3://") or data_uri.startswith("minio://"):
            data_source = {
                "type": "file",
                "path": data_uri,
                "format": "parquet"
            }
        elif data_uri.startswith("jdbc://"):
            data_source = {
                "type": "jdbc",
                "url": data_uri
            }
        else:
            data_source = {
                "type": "file",
                "path": data_uri
            }
            
        # Create quality pipeline
        await seatunnel.create_quality_pipeline(
            dataset_id=dataset_id,
            data_source=data_source,
            quality_requirements={
                "schedule": "daily",
                "statistical_validation": True,
                "trust_validation": True
            },
            trust_requirements={"min_assessor_trust": 0.7}
        )
        
        logger.info(f"Created automated quality pipeline for dataset {dataset_id}")
        
    except Exception as e:
        logger.error(f"Error creating quality pipeline: {e}") 