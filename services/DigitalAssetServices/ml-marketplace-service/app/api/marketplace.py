"""
ML Model marketplace API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
import logging

from ..core.marketplace import (
    ModelMarketplace,
    ModelVisibility,
    ModelCategory,
    ModelLicense
)

logger = logging.getLogger(__name__)
router = APIRouter()


class ModelPublishRequest(BaseModel):
    """Request to publish a model"""
    model_id: str = Field(..., description="Model ID from registry")
    name: str = Field(..., description="Model name")
    description: str = Field(..., description="Model description")
    category: str = Field(ModelCategory.OTHER.value, description="Model category")
    visibility: str = Field(ModelVisibility.PRIVATE.value, description="Model visibility")
    license: str = Field(ModelLicense.PROPRIETARY.value, description="Model license")
    price: float = Field(0.0, ge=0, description="Model price")
    currency: str = Field("USD", description="Price currency")
    tags: List[str] = Field(default_factory=list, description="Model tags")
    framework: str = Field("unknown", description="ML framework")
    version: str = Field("1.0.0", description="Model version")
    metrics: Dict[str, Any] = Field(default_factory=dict, description="Performance metrics")
    requirements: Dict[str, Any] = Field(default_factory=dict, description="System requirements")
    documentation_url: Optional[str] = Field(None, description="Documentation URL")
    repository_url: Optional[str] = Field(None, description="Repository URL")


class ModelSearchRequest(BaseModel):
    """Model search request"""
    query: Optional[str] = Field(None, description="Search query")
    category: Optional[str] = Field(None, description="Filter by category")
    tags: Optional[List[str]] = Field(None, description="Filter by tags")
    min_rating: Optional[float] = Field(None, ge=0, le=5, description="Minimum rating")
    max_price: Optional[float] = Field(None, ge=0, description="Maximum price")
    framework: Optional[str] = Field(None, description="ML framework")
    license: Optional[str] = Field(None, description="License type")
    sort_by: str = Field("relevance", description="Sort by: relevance, rating, downloads, recent, price_low, price_high")
    limit: int = Field(20, ge=1, le=100, description="Results per page")
    offset: int = Field(0, ge=0, description="Pagination offset")


class ModelRatingRequest(BaseModel):
    """Model rating request"""
    rating: int = Field(..., ge=1, le=5, description="Rating (1-5)")
    review: Optional[str] = Field(None, description="Review text")


def get_marketplace(request: Request) -> ModelMarketplace:
    """Get marketplace instance from app state"""
    return request.app.state.marketplace


@router.post("/models/publish")
async def publish_model(
    request: ModelPublishRequest,
    publisher_id: str = Query(..., description="Publisher user ID"),
    marketplace: ModelMarketplace = Depends(get_marketplace)
):
    """Publish a model to the marketplace"""
    try:
        metadata = request.dict(exclude={"model_id"})
        
        result = await marketplace.publish_model(
            model_id=request.model_id,
            publisher_id=publisher_id,
            metadata=metadata
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error publishing model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/models/search")
async def search_models(
    request: ModelSearchRequest,
    marketplace: ModelMarketplace = Depends(get_marketplace)
):
    """Search for models in the marketplace"""
    try:
        result = await marketplace.search_models(
            query=request.query,
            category=request.category,
            tags=request.tags,
            min_rating=request.min_rating,
            max_price=request.max_price,
            framework=request.framework,
            license=request.license,
            sort_by=request.sort_by,
            limit=request.limit,
            offset=request.offset
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error searching models: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/models/{marketplace_id}")
async def get_model_details(
    marketplace_id: str,
    marketplace: ModelMarketplace = Depends(get_marketplace)
):
    """Get detailed information about a model"""
    try:
        details = await marketplace.get_model_details(marketplace_id)
        
        if not details:
            raise HTTPException(status_code=404, detail="Model not found")
            
        return details
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting model details: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/models/{marketplace_id}/download")
async def download_model(
    marketplace_id: str,
    user_id: str = Query(..., description="User ID"),
    marketplace: ModelMarketplace = Depends(get_marketplace)
):
    """Download a model from the marketplace"""
    try:
        result = await marketplace.download_model(
            marketplace_id=marketplace_id,
            user_id=user_id
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        logger.error(f"Error downloading model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/models/{marketplace_id}/rate")
async def rate_model(
    marketplace_id: str,
    request: ModelRatingRequest,
    user_id: str = Query(..., description="User ID"),
    marketplace: ModelMarketplace = Depends(get_marketplace)
):
    """Rate a model in the marketplace"""
    try:
        result = await marketplace.rate_model(
            marketplace_id=marketplace_id,
            user_id=user_id,
            rating=request.rating,
            review=request.review
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error rating model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trending")
async def get_trending_models(
    limit: int = Query(10, ge=1, le=50, description="Number of results"),
    marketplace: ModelMarketplace = Depends(get_marketplace)
):
    """Get trending models"""
    try:
        trending = await marketplace.get_trending_models(limit=limit)
        
        return {
            "trending": trending,
            "count": len(trending)
        }
        
    except Exception as e:
        logger.error(f"Error getting trending models: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recommendations")
async def get_recommendations(
    user_id: str = Query(..., description="User ID"),
    limit: int = Query(10, ge=1, le=50, description="Number of recommendations"),
    marketplace: ModelMarketplace = Depends(get_marketplace)
):
    """Get personalized model recommendations"""
    try:
        recommendations = await marketplace.get_recommendations(
            user_id=user_id,
            limit=limit
        )
        
        return {
            "recommendations": recommendations,
            "count": len(recommendations)
        }
        
    except Exception as e:
        logger.error(f"Error getting recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/categories")
async def list_categories():
    """List all model categories"""
    return {
        "categories": [
            {
                "id": category.value,
                "name": category.value.replace("_", " ").title(),
                "description": f"Models for {category.value.replace('_', ' ')}"
            }
            for category in ModelCategory
        ]
    }


@router.get("/licenses")
async def list_licenses():
    """List all license types"""
    return {
        "licenses": [
            {
                "id": license.value,
                "name": license.value.replace("_", " ").title(),
                "description": f"{license.value.replace('_', ' ')} license"
            }
            for license in ModelLicense
        ]
    }


@router.get("/stats")
async def get_marketplace_stats(
    marketplace: ModelMarketplace = Depends(get_marketplace)
):
    """Get marketplace statistics"""
    try:
        # Count models by category
        category_counts = {}
        visibility_counts = {}
        total_models = 0
        total_downloads = 0
        
        for key, model_data in marketplace.models_cache.scan():
            if model_data.get("is_active", False):
                total_models += 1
                
                category = model_data.get("category", "other")
                category_counts[category] = category_counts.get(category, 0) + 1
                
                visibility = model_data.get("visibility", "private")
                visibility_counts[visibility] = visibility_counts.get(visibility, 0) + 1
                
                total_downloads += model_data.get("download_count", 0)
                
        return {
            "total_models": total_models,
            "total_downloads": total_downloads,
            "models_by_category": category_counts,
            "models_by_visibility": visibility_counts,
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Error getting marketplace stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check(
    marketplace: ModelMarketplace = Depends(get_marketplace)
):
    """Health check endpoint"""
    try:
        # Check connections
        ignite_connected = marketplace.ignite_client is not None
        pulsar_connected = marketplace.pulsar_client is not None
        
        return {
            "status": "healthy" if ignite_connected and pulsar_connected else "unhealthy",
            "checks": {
                "ignite": "connected" if ignite_connected else "disconnected",
                "pulsar": "connected" if pulsar_connected else "disconnected",
                "models_cached": len(list(marketplace.models_cache.scan()))
            },
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow()
        } 