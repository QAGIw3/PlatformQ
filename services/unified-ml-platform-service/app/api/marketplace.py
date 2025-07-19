"""
Model marketplace API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum
from decimal import Decimal

router = APIRouter()


class ModelLicenseType(str, Enum):
    OPEN_SOURCE = "open_source"
    COMMERCIAL = "commercial"
    SUBSCRIPTION = "subscription"
    PAY_PER_USE = "pay_per_use"
    CUSTOM = "custom"


class ModelCategory(str, Enum):
    COMPUTER_VISION = "computer_vision"
    NLP = "nlp"
    TABULAR = "tabular"
    TIME_SERIES = "time_series"
    REINFORCEMENT_LEARNING = "reinforcement_learning"
    GENERATIVE = "generative"
    OTHER = "other"


class ModelListing(BaseModel):
    """Model marketplace listing"""
    model_id: str = Field(..., description="Model ID from registry")
    title: str = Field(..., description="Listing title")
    description: str = Field(..., description="Detailed description")
    category: ModelCategory
    license_type: ModelLicenseType
    price: Decimal = Field(Decimal("0"), ge=0, description="Price in platform tokens")
    pricing_model: str = Field("one_time", description="one_time, subscription, usage_based")
    demo_available: bool = Field(False)
    trial_period_days: int = Field(0, ge=0, le=365)
    tags: List[str] = Field(default_factory=list)
    performance_metrics: Dict[str, Any] = Field(default_factory=dict)
    requirements: Dict[str, Any] = Field(default_factory=dict)


class ModelListingResponse(ModelListing):
    """Model listing response"""
    listing_id: str
    seller_id: str
    seller_reputation: float
    total_sales: int
    average_rating: float
    review_count: int
    created_at: datetime
    updated_at: datetime
    status: str  # active, inactive, under_review


class PurchaseRequest(BaseModel):
    """Model purchase request"""
    listing_id: str
    license_duration_days: Optional[int] = None
    payment_method: str = Field("platform_tokens", description="Payment method")
    deployment_target: Optional[str] = None


class ReviewCreate(BaseModel):
    """Create model review"""
    rating: int = Field(..., ge=1, le=5)
    title: str = Field(..., max_length=200)
    content: str = Field(..., max_length=2000)
    verified_purchase: bool = Field(True)


@router.post("/listings", response_model=ModelListingResponse)
async def create_model_listing(
    listing: ModelListing,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Create a new model marketplace listing"""
    listing_id = f"listing_{datetime.utcnow().timestamp()}"
    
    return ModelListingResponse(
        **listing.dict(),
        listing_id=listing_id,
        seller_id=user_id,
        seller_reputation=4.5,
        total_sales=0,
        average_rating=0.0,
        review_count=0,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        status="under_review"
    )


@router.get("/listings", response_model=List[ModelListingResponse])
async def list_marketplace_models(
    category: Optional[ModelCategory] = Query(None),
    license_type: Optional[ModelLicenseType] = Query(None),
    min_price: Optional[Decimal] = Query(None, ge=0),
    max_price: Optional[Decimal] = Query(None, ge=0),
    min_rating: Optional[float] = Query(None, ge=0, le=5),
    search: Optional[str] = Query(None, description="Search in title and description"),
    sort_by: str = Query("popularity", description="popularity, price, rating, newest"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0)
):
    """Browse marketplace listings"""
    # Mock response
    return [
        ModelListingResponse(
            listing_id="listing_123",
            model_id="model_456",
            title="GPT-based Text Classifier",
            description="State-of-the-art text classification model",
            category=ModelCategory.NLP,
            license_type=ModelLicenseType.COMMERCIAL,
            price=Decimal("99.99"),
            pricing_model="one_time",
            demo_available=True,
            trial_period_days=7,
            tags=["nlp", "classification", "transformer"],
            performance_metrics={"accuracy": 0.95, "f1_score": 0.94},
            requirements={"gpu": True, "min_memory_gb": 8},
            seller_id="seller_789",
            seller_reputation=4.8,
            total_sales=150,
            average_rating=4.7,
            review_count=45,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            status="active"
        )
    ]


@router.get("/listings/{listing_id}", response_model=ModelListingResponse)
async def get_listing_details(
    listing_id: str
):
    """Get detailed marketplace listing"""
    raise HTTPException(status_code=404, detail="Listing not found")


@router.post("/listings/{listing_id}/purchase")
async def purchase_model(
    listing_id: str,
    purchase: PurchaseRequest,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Purchase a model from marketplace"""
    transaction_id = f"tx_{datetime.utcnow().timestamp()}"
    
    return {
        "transaction_id": transaction_id,
        "listing_id": listing_id,
        "buyer_id": user_id,
        "status": "completed",
        "amount": 99.99,
        "currency": "QAGI",
        "license_key": f"license_{transaction_id}",
        "valid_until": datetime.utcnow(),
        "download_url": f"https://models.platformq.io/{listing_id}/download"
    }


@router.get("/purchases")
async def list_my_purchases(
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID"),
    status: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List user's purchased models"""
    return {
        "purchases": [
            {
                "transaction_id": "tx_123",
                "listing_id": "listing_456",
                "model_name": "Text Classifier Pro",
                "purchased_at": datetime.utcnow(),
                "amount": 99.99,
                "license_valid_until": datetime.utcnow(),
                "downloads_remaining": 5
            }
        ],
        "total": 1
    }


@router.get("/sales")
async def list_my_sales(
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID"),
    period: str = Query("30d", description="Time period (7d, 30d, 90d, all)"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List user's model sales"""
    return {
        "sales": [
            {
                "transaction_id": "tx_789",
                "listing_id": "listing_123",
                "buyer_id": "buyer_456",
                "amount": 99.99,
                "commission": 9.99,
                "net_amount": 89.90,
                "sold_at": datetime.utcnow()
            }
        ],
        "total_sales": 1,
        "total_revenue": 89.90,
        "period": period
    }


@router.post("/listings/{listing_id}/reviews", response_model=Dict[str, Any])
async def create_review(
    listing_id: str,
    review: ReviewCreate,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Create a review for a model listing"""
    review_id = f"review_{datetime.utcnow().timestamp()}"
    
    return {
        "review_id": review_id,
        "listing_id": listing_id,
        "reviewer_id": user_id,
        "rating": review.rating,
        "title": review.title,
        "content": review.content,
        "verified_purchase": review.verified_purchase,
        "created_at": datetime.utcnow(),
        "helpful_count": 0
    }


@router.get("/listings/{listing_id}/reviews")
async def list_reviews(
    listing_id: str,
    sort_by: str = Query("helpful", description="helpful, newest, rating"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0)
):
    """List reviews for a model listing"""
    return {
        "reviews": [
            {
                "review_id": "review_123",
                "reviewer_name": "John D.",
                "rating": 5,
                "title": "Excellent model!",
                "content": "Works great for my use case...",
                "verified_purchase": True,
                "created_at": datetime.utcnow(),
                "helpful_count": 15
            }
        ],
        "total": 1,
        "average_rating": 4.7
    }


@router.get("/trending")
async def get_trending_models(
    category: Optional[ModelCategory] = Query(None),
    time_period: str = Query("7d", description="Time period (24h, 7d, 30d)")
):
    """Get trending models in marketplace"""
    return {
        "trending": [
            {
                "listing_id": "listing_123",
                "title": "GPT Text Classifier",
                "category": "nlp",
                "trend_score": 95.5,
                "sales_growth": 0.45,
                "view_count": 1250
            }
        ],
        "period": time_period
    }


@router.get("/recommendations")
async def get_recommendations(
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID"),
    based_on: str = Query("history", description="history, similar_users, category"),
    limit: int = Query(10, ge=1, le=50)
):
    """Get personalized model recommendations"""
    return {
        "recommendations": [
            {
                "listing_id": "listing_456",
                "title": "Advanced NLP Toolkit",
                "reason": "Based on your recent purchases",
                "match_score": 0.89,
                "price": 149.99
            }
        ]
    }


@router.post("/listings/{listing_id}/report")
async def report_listing(
    listing_id: str,
    reason: str = Query(..., description="Reason for reporting"),
    details: str = Query(..., description="Additional details"),
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Report a problematic listing"""
    report_id = f"report_{datetime.utcnow().timestamp()}"
    
    return {
        "report_id": report_id,
        "listing_id": listing_id,
        "status": "submitted",
        "message": "Report submitted for review"
    } 