"""
Asset-Compute-Model Nexus API endpoints

Enables using digital assets as collateral and creating new financial instruments
combining assets, compute, and ML models.
"""

from typing import List, Dict, Any, Optional
from decimal import Decimal
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.main import asset_compute_nexus


router = APIRouter(prefix="/api/v1/asset-compute-nexus", tags=["asset-compute-nexus"])


class AssetValuationRequest(BaseModel):
    """Request for digital asset valuation"""
    asset_id: str = Field(..., description="Digital asset ID")
    asset_type: str = Field(..., description="Type of asset (model_nft, dataset_nft, etc.)")


class ModelComputeBundleRequest(BaseModel):
    """Request to create model-compute bundle"""
    model_id: str = Field(..., description="ML model ID")
    compute_hours: str = Field(..., description="Compute hours to bundle")
    resource_type: str = Field(default="gpu", description="Type of compute resource")
    guaranteed_performance: float = Field(0.9, description="Guaranteed performance level")


class AssetBackedFutureRequest(BaseModel):
    """Request to create asset-backed compute future"""
    asset_id: str = Field(..., description="Backing asset ID")
    asset_type: str = Field(..., description="Type of backing asset")
    compute_hours: str = Field(..., description="Compute hours in future")
    strike_price: str = Field(..., description="Strike price per hour")
    expiry_days: int = Field(30, description="Days until expiry")


class SyntheticDataRequest(BaseModel):
    """Request to create synthetic data derivative"""
    dataset_specs: Dict[str, Any] = Field(..., description="Dataset specifications")
    generation_model_id: str = Field(..., description="Model to generate data")
    compute_budget: str = Field(..., description="Compute budget in USD")


class PortfolioOptimizationRequest(BaseModel):
    """Request for portfolio optimization"""
    assets: List[str] = Field(..., description="List of asset IDs")
    compute_needs: Dict[str, str] = Field(..., description="Compute needs by resource type")
    risk_tolerance: float = Field(0.5, ge=0.0, le=1.0, description="Risk tolerance (0=conservative, 1=aggressive)")


@router.post("/value-asset", response_model=Dict[str, Any])
async def value_digital_asset(
    request: AssetValuationRequest,
    current_user: dict = Depends(get_current_user)
):
    """Value a digital asset for use as collateral"""
    if not asset_compute_nexus:
        raise HTTPException(status_code=503, detail="Asset compute nexus not initialized")
    
    try:
        # Import AssetType enum
        from app.integrations.asset_compute_nexus import AssetType
        
        # Convert string to AssetType
        try:
            asset_type = AssetType(request.asset_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid asset type: {request.asset_type}")
        
        valuation = await asset_compute_nexus.value_digital_asset_as_collateral(
            asset_id=request.asset_id,
            asset_type=asset_type,
            owner_id=current_user["user_id"]
        )
        
        return {
            "asset_id": valuation.asset_id,
            "asset_type": valuation.asset_type.value,
            "intrinsic_value": str(valuation.intrinsic_value),
            "liquidity_discount": str(valuation.liquidity_discount),
            "volatility_adjustment": str(valuation.volatility_adjustment),
            "collateral_value": str(valuation.collateral_value),
            "confidence_score": valuation.confidence_score,
            "valuation_date": valuation.valuation_date.isoformat(),
            "metadata": valuation.metadata
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/create-bundle", response_model=Dict[str, Any])
async def create_model_compute_bundle(
    request: ModelComputeBundleRequest,
    current_user: dict = Depends(get_current_user)
):
    """Create a bundle of ML model + compute resources"""
    if not asset_compute_nexus:
        raise HTTPException(status_code=503, detail="Asset compute nexus not initialized")
    
    try:
        bundle = await asset_compute_nexus.create_model_compute_bundle(
            model_id=request.model_id,
            compute_hours=Decimal(request.compute_hours),
            resource_type=request.resource_type,
            guaranteed_performance=request.guaranteed_performance
        )
        
        return {
            "bundle_id": bundle.bundle_id,
            "model_id": bundle.model_id,
            "compute_hours": str(bundle.compute_hours),
            "resource_type": bundle.resource_type,
            "guaranteed_performance": bundle.guaranteed_performance,
            "price_per_inference": str(bundle.price_per_inference),
            "expiry_date": bundle.expiry_date.isoformat(),
            "transferable": bundle.transferable
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/create-asset-future", response_model=Dict[str, Any])
async def create_asset_backed_compute_future(
    request: AssetBackedFutureRequest,
    current_user: dict = Depends(get_current_user)
):
    """Create a compute future backed by a digital asset"""
    if not asset_compute_nexus:
        raise HTTPException(status_code=503, detail="Asset compute nexus not initialized")
    
    try:
        # Import AssetType enum
        from app.integrations.asset_compute_nexus import AssetType
        
        # Convert string to AssetType
        try:
            asset_type = AssetType(request.asset_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid asset type: {request.asset_type}")
        
        future = await asset_compute_nexus.create_asset_backed_compute_future(
            asset_id=request.asset_id,
            asset_type=asset_type,
            compute_hours=Decimal(request.compute_hours),
            strike_price=Decimal(request.strike_price),
            expiry_days=request.expiry_days
        )
        
        return {
            "future_id": future.future_id,
            "underlying_asset_id": future.underlying_asset_id,
            "asset_type": future.asset_type.value,
            "compute_equivalent": str(future.compute_equivalent),
            "strike_price": str(future.strike_price),
            "expiry_date": future.expiry_date.isoformat(),
            "is_american": future.is_american
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/model-metrics/{model_id}", response_model=Dict[str, Any])
async def get_model_performance_metrics(
    model_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Get performance metrics for an ML model"""
    if not asset_compute_nexus:
        raise HTTPException(status_code=503, detail="Asset compute nexus not initialized")
    
    try:
        metrics = await asset_compute_nexus.get_model_performance_metrics(model_id)
        
        return {
            "model_id": metrics.model_id,
            "accuracy": metrics.accuracy,
            "precision": metrics.precision,
            "recall": metrics.recall,
            "f1_score": metrics.f1_score,
            "inference_time_ms": metrics.inference_time_ms,
            "resource_efficiency": metrics.resource_efficiency,
            "last_updated": metrics.last_updated.isoformat(),
            "usage_count": metrics.usage_count,
            "revenue_generated": str(metrics.revenue_generated)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/model-collateral-value/{model_id}", response_model=Dict[str, Any])
async def evaluate_model_collateral_value(
    model_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Evaluate an ML model's value as collateral"""
    if not asset_compute_nexus:
        raise HTTPException(status_code=503, detail="Asset compute nexus not initialized")
    
    try:
        collateral_value = await asset_compute_nexus.evaluate_model_collateral_value(
            model_id=model_id,
            owner_id=current_user["user_id"]
        )
        
        return {
            "model_id": model_id,
            "collateral_value": str(collateral_value),
            "currency": "USD",
            "valuation_date": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/create-synthetic-data", response_model=Dict[str, Any])
async def create_synthetic_data_derivative(
    request: SyntheticDataRequest,
    current_user: dict = Depends(get_current_user)
):
    """Create a derivative for synthetic data generation"""
    if not asset_compute_nexus:
        raise HTTPException(status_code=503, detail="Asset compute nexus not initialized")
    
    try:
        future = await asset_compute_nexus.create_synthetic_data_derivative(
            dataset_specs=request.dataset_specs,
            generation_model_id=request.generation_model_id,
            compute_budget=Decimal(request.compute_budget)
        )
        
        return future
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/optimize-portfolio", response_model=Dict[str, Any])
async def optimize_asset_compute_portfolio(
    request: PortfolioOptimizationRequest,
    current_user: dict = Depends(get_current_user)
):
    """Optimize portfolio of digital assets and compute resources"""
    if not asset_compute_nexus:
        raise HTTPException(status_code=503, detail="Asset compute nexus not initialized")
    
    try:
        # Convert compute needs to Decimal
        compute_needs = {
            k: Decimal(v) for k, v in request.compute_needs.items()
        }
        
        portfolio_metrics = await asset_compute_nexus.optimize_asset_compute_portfolio(
            user_id=current_user["user_id"],
            assets=request.assets,
            compute_needs=compute_needs,
            risk_tolerance=request.risk_tolerance
        )
        
        return portfolio_metrics
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/asset-types", response_model=List[str])
async def get_supported_asset_types(
    current_user: dict = Depends(get_current_user)
):
    """Get list of supported digital asset types"""
    from app.integrations.asset_compute_nexus import AssetType
    
    return [asset_type.value for asset_type in AssetType] 