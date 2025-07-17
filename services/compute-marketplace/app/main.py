"""
Compute Marketplace Service

Sell compute time for model inference
"""

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging
from pydantic import BaseModel
import asyncio
import httpx
from enum import Enum
from platformq_shared.event_publisher import EventPublisher
from pulsar.schema import Record, String, Long, Double

class AssetUsed(Record):
    tenant_id = String()
    asset_id = String()
    user_id = String()
    usage_duration_minutes = Long()
    usage_type = String()
    event_timestamp = Long()

from .compute_manager import ComputeManager, ComputeOffering, ComputePurchase
from .pricing_engine import PricingEngine
from .resource_scheduler import ResourceScheduler

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Compute Marketplace Service",
    description="Marketplace for buying and selling compute time for model inference",
    version="1.0.0"
)

@app.on_event("startup")
def startup():
    app.state.event_publisher = EventPublisher(pulsar_url='pulsar://pulsar:6650')
    app.state.event_publisher.connect()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize components
compute_manager = ComputeManager()
pricing_engine = PricingEngine()
resource_scheduler = ResourceScheduler()


class ComputeOfferingRequest(BaseModel):
    provider_id: str
    resource_type: str  # "cpu", "gpu", "tpu"
    resource_specs: Dict[str, Any]
    availability_hours: List[int]  # Hours of day available
    min_duration_minutes: int
    max_duration_minutes: Optional[int]
    price_per_hour: float
    location: str
    tags: List[str] = []


class ComputePurchaseRequest(BaseModel):
    buyer_id: str
    offering_id: str
    duration_minutes: int
    start_time: Optional[datetime] = None
    model_requirements: Dict[str, Any]
    priority: str = "normal"  # "low", "normal", "high", "urgent"


class ComputeSearchRequest(BaseModel):
    resource_type: Optional[str] = None
    min_memory_gb: Optional[int] = None
    min_vcpus: Optional[int] = None
    gpu_type: Optional[str] = None
    max_price_per_hour: Optional[float] = None
    location: Optional[str] = None
    required_duration_minutes: Optional[int] = None


@app.get("/")
async def root():
    return {"service": "Compute Marketplace", "status": "operational"}


@app.post("/api/v1/offerings", response_model=Dict[str, Any])
async def create_compute_offering(
    request: ComputeOfferingRequest,
    background_tasks: BackgroundTasks
):
    """Create a new compute offering"""
    try:
        # Validate resource specifications
        if not pricing_engine.validate_resource_specs(request.resource_type, request.resource_specs):
            raise HTTPException(status_code=400, detail="Invalid resource specifications")
        
        # Create offering
        offering = await compute_manager.create_offering(
            provider_id=request.provider_id,
            resource_type=request.resource_type,
            resource_specs=request.resource_specs,
            availability_hours=request.availability_hours,
            min_duration=request.min_duration_minutes,
            max_duration=request.max_duration_minutes,
            price_per_hour=request.price_per_hour,
            location=request.location,
            tags=request.tags
        )
        
        # Schedule resource monitoring
        background_tasks.add_task(
            resource_scheduler.monitor_offering,
            offering.offering_id
        )
        
        return {
            "offering_id": offering.offering_id,
            "status": "active",
            "created_at": offering.created_at.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error creating compute offering: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/purchases", response_model=Dict[str, Any])
async def purchase_compute_time(
    request: ComputePurchaseRequest,
    background_tasks: BackgroundTasks
):
    """Purchase compute time from an offering"""
    try:
        # Get offering
        offering = await compute_manager.get_offering(request.offering_id)
        if not offering:
            raise HTTPException(status_code=404, detail="Offering not found")
        
        # Validate purchase
        if request.duration_minutes < offering.min_duration_minutes:
            raise HTTPException(
                status_code=400,
                detail=f"Duration must be at least {offering.min_duration_minutes} minutes"
            )
        
        if offering.max_duration_minutes and request.duration_minutes > offering.max_duration_minutes:
            raise HTTPException(
                status_code=400,
                detail=f"Duration cannot exceed {offering.max_duration_minutes} minutes"
            )
        
        # Check resource availability
        start_time = request.start_time or datetime.utcnow()
        if not await resource_scheduler.check_availability(
            offering.offering_id,
            start_time,
            request.duration_minutes
        ):
            raise HTTPException(status_code=409, detail="Resources not available for requested time")
        
        # Calculate price
        price = pricing_engine.calculate_price(
            base_price_per_hour=offering.price_per_hour,
            duration_minutes=request.duration_minutes,
            priority=request.priority,
            resource_type=offering.resource_type
        )
        
        # Create purchase
        purchase = await compute_manager.create_purchase(
            buyer_id=request.buyer_id,
            offering=offering,
            duration_minutes=request.duration_minutes,
            start_time=start_time,
            price=price,
            model_requirements=request.model_requirements,
            priority=request.priority
        )
        
        publisher = request.app.state.event_publisher
        asset_id = request.model_requirements.get('asset_id')
        if asset_id:
            event = AssetUsed(
                tenant_id='default',  # Add tenant context
                asset_id=asset_id,
                user_id=request.buyer_id,
                usage_duration_minutes=request.duration_minutes,
                usage_type='compute',
                event_timestamp=int(datetime.utcnow().timestamp() * 1000)
            )
            publisher.publish(
                topic_base='asset-usage-events',
                tenant_id='default',
                schema_class=AssetUsed,
                data=event
            )
            logger.info(f"Published AssetUsed event for asset {asset_id}")
        
        # Schedule resource allocation
        background_tasks.add_task(
            resource_scheduler.allocate_resources,
            purchase.purchase_id,
            start_time
        )
        
        return {
            "purchase_id": purchase.purchase_id,
            "offering_id": purchase.offering_id,
            "start_time": purchase.start_time.isoformat(),
            "end_time": purchase.end_time.isoformat(),
            "total_price": purchase.total_price,
            "status": purchase.status,
            "access_credentials": await compute_manager.generate_access_credentials(purchase.purchase_id)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error purchasing compute time: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/offerings/search", response_model=List[Dict[str, Any]])
async def search_compute_offerings(
    resource_type: Optional[str] = None,
    min_memory_gb: Optional[int] = None,
    min_vcpus: Optional[int] = None,
    gpu_type: Optional[str] = None,
    max_price_per_hour: Optional[float] = None,
    location: Optional[str] = None,
    required_duration_minutes: Optional[int] = None
):
    """Search for available compute offerings"""
    try:
        offerings = await compute_manager.search_offerings(
            resource_type=resource_type,
            min_memory_gb=min_memory_gb,
            min_vcpus=min_vcpus,
            gpu_type=gpu_type,
            max_price_per_hour=max_price_per_hour,
            location=location,
            required_duration_minutes=required_duration_minutes
        )
        
        return [
            {
                "offering_id": o.offering_id,
                "provider_id": o.provider_id,
                "resource_type": o.resource_type,
                "resource_specs": o.resource_specs,
                "price_per_hour": o.price_per_hour,
                "location": o.location,
                "availability": await resource_scheduler.get_availability_summary(o.offering_id),
                "rating": o.provider_rating,
                "total_hours_sold": o.total_hours_sold
            }
            for o in offerings
        ]
        
    except Exception as e:
        logger.error(f"Error searching offerings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/purchases/{purchase_id}", response_model=Dict[str, Any])
async def get_purchase_details(purchase_id: str):
    """Get details of a compute purchase"""
    try:
        purchase = await compute_manager.get_purchase(purchase_id)
        if not purchase:
            raise HTTPException(status_code=404, detail="Purchase not found")
        
        return {
            "purchase_id": purchase.purchase_id,
            "buyer_id": purchase.buyer_id,
            "offering_id": purchase.offering_id,
            "start_time": purchase.start_time.isoformat(),
            "end_time": purchase.end_time.isoformat(),
            "duration_minutes": purchase.duration_minutes,
            "total_price": purchase.total_price,
            "status": purchase.status,
            "resource_details": purchase.resource_details,
            "usage_metrics": await compute_manager.get_usage_metrics(purchase_id)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting purchase details: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/purchases/{purchase_id}/extend", response_model=Dict[str, Any])
async def extend_compute_purchase(
    purchase_id: str,
    additional_minutes: int,
    background_tasks: BackgroundTasks
):
    """Extend an active compute purchase"""
    try:
        purchase = await compute_manager.get_purchase(purchase_id)
        if not purchase:
            raise HTTPException(status_code=404, detail="Purchase not found")
        
        if purchase.status != "active":
            raise HTTPException(status_code=400, detail="Can only extend active purchases")
        
        # Check if extension is possible
        offering = await compute_manager.get_offering(purchase.offering_id)
        new_end_time = purchase.end_time + timedelta(minutes=additional_minutes)
        
        if not await resource_scheduler.check_availability(
            offering.offering_id,
            purchase.end_time,
            additional_minutes
        ):
            raise HTTPException(status_code=409, detail="Resources not available for extension")
        
        # Calculate additional cost
        additional_cost = pricing_engine.calculate_price(
            base_price_per_hour=offering.price_per_hour,
            duration_minutes=additional_minutes,
            priority=purchase.priority,
            resource_type=offering.resource_type
        )
        
        # Extend purchase
        await compute_manager.extend_purchase(
            purchase_id,
            additional_minutes,
            additional_cost
        )
        
        # Update resource allocation
        background_tasks.add_task(
            resource_scheduler.extend_allocation,
            purchase_id,
            additional_minutes
        )
        
        return {
            "purchase_id": purchase_id,
            "new_end_time": new_end_time.isoformat(),
            "additional_cost": additional_cost,
            "total_duration_minutes": purchase.duration_minutes + additional_minutes
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error extending purchase: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/providers/{provider_id}/earnings", response_model=Dict[str, Any])
async def get_provider_earnings(
    provider_id: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
):
    """Get earnings summary for a compute provider"""
    try:
        earnings = await compute_manager.get_provider_earnings(
            provider_id,
            start_date or datetime.utcnow() - timedelta(days=30),
            end_date or datetime.utcnow()
        )
        
        return {
            "provider_id": provider_id,
            "period": {
                "start": (start_date or datetime.utcnow() - timedelta(days=30)).isoformat(),
                "end": (end_date or datetime.utcnow()).isoformat()
            },
            "total_earnings": earnings["total"],
            "total_hours": earnings["hours"],
            "average_price_per_hour": earnings["average_price"],
            "earnings_by_resource_type": earnings["by_resource_type"],
            "top_buyers": earnings["top_buyers"]
        }
        
    except Exception as e:
        logger.error(f"Error getting provider earnings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/pricing/estimate", response_model=Dict[str, Any])
async def estimate_compute_cost(
    resource_type: str,
    duration_minutes: int,
    memory_gb: Optional[int] = None,
    vcpus: Optional[int] = None,
    gpu_type: Optional[str] = None,
    location: Optional[str] = None,
    priority: str = "normal"
):
    """Estimate cost for compute resources"""
    try:
        estimate = await pricing_engine.estimate_cost(
            resource_type=resource_type,
            duration_minutes=duration_minutes,
            memory_gb=memory_gb,
            vcpus=vcpus,
            gpu_type=gpu_type,
            location=location,
            priority=priority
        )
        
        return {
            "estimated_cost": estimate["total"],
            "breakdown": {
                "base_cost": estimate["base_cost"],
                "priority_multiplier": estimate["priority_multiplier"],
                "location_adjustment": estimate["location_adjustment"]
            },
            "recommended_offerings": estimate["recommended_offerings"]
        }
        
    except Exception as e:
        logger.error(f"Error estimating cost: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 