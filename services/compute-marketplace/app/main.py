"""
Compute Marketplace Service

Sell compute time for model inference
"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from enum import Enum

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
import asyncio
import httpx
import uuid

from platformq_shared import (
    create_base_app,
    ErrorCode,
    AppException,
    EventProcessor,
    get_pulsar_client
)
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.database import get_db
from sqlalchemy.orm import Session

from .models import (
    ComputeOffering,
    ComputePurchase,
    ResourceType,
    OfferingStatus,
    PurchaseStatus,
    Priority
)
from .repository import (
    ComputeOfferingRepository,
    ComputePurchaseRepository,
    ResourceAvailabilityRepository,
    PricingRuleRepository
)
from .event_processors import ComputeMarketplaceEventProcessor

# Setup logging
logger = logging.getLogger(__name__)

# Event processor instance
event_processor = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global event_processor
    
    # Startup
    logger.info("Initializing Compute Marketplace Service...")
    
    # Initialize event processor
    pulsar_client = get_pulsar_client()
    event_processor = ComputeMarketplaceEventProcessor(
        pulsar_client=pulsar_client,
        service_name="compute-marketplace-service"
    )
    
    # Start event processor
    await event_processor.start()
    
    logger.info("Compute Marketplace Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Compute Marketplace Service...")
    
    if event_processor:
        await event_processor.stop()
    
    logger.info("Compute Marketplace Service shutdown complete")

# Create FastAPI app
app = create_base_app(
    title="Compute Marketplace Service",
    description="Marketplace for buying and selling compute time for model inference",
    version="1.0.0",
    lifespan=lifespan,
    event_processors=[event_processor] if event_processor else []
)

# Pydantic models for API requests/responses
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
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")  # TODO: Get from auth
):
    """Create a new compute offering"""
    try:
        # Initialize repositories
        offering_repo = ComputeOfferingRepository(db)
        
        # Create offering
        offering = offering_repo.create({
            "offering_id": str(uuid.uuid4()),
            "tenant_id": tenant_id,
            "provider_id": request.provider_id,
            "resource_type": request.resource_type,
            "resource_specs": request.resource_specs,
            "availability_hours": request.availability_hours,
            "min_duration_minutes": request.min_duration_minutes,
            "max_duration_minutes": request.max_duration_minutes,
            "price_per_hour": request.price_per_hour,
            "location": request.location,
            "tags": request.tags,
            "status": OfferingStatus.ACTIVE
        })
        
        # Publish offering created event
        event_publisher = EventPublisher()
        await event_publisher.publish_event(
            {
                "offering_id": offering.offering_id,
                "provider_id": offering.provider_id,
                "resource_type": offering.resource_type,
                "availability_hours": offering.availability_hours,
                "capacity_units": 1.0,
                "tenant_id": tenant_id
            },
            "persistent://public/default/offering-events"
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
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")  # TODO: Get from auth
):
    """Purchase compute time from an offering"""
    try:
        # Initialize repositories
        offering_repo = ComputeOfferingRepository(db)
        availability_repo = ResourceAvailabilityRepository(db)
        
        # Get offering
        offering = offering_repo.get_by_offering_id(request.offering_id)
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
        if not availability_repo.check_availability(
            offering.offering_id,
            start_time,
            request.duration_minutes
        ):
            raise HTTPException(status_code=409, detail="Resources not available for requested time")
        
        # Calculate base price (dynamic pricing will be handled by event processor)
        base_price = offering.price_per_hour * (request.duration_minutes / 60)
        
        # Publish purchase request event
        event_publisher = EventPublisher()
        purchase_id = str(uuid.uuid4())
        
        await event_publisher.publish_event(
            {
                "request_id": purchase_id,
                "buyer_id": request.buyer_id,
                "offering_id": request.offering_id,
                "duration_minutes": request.duration_minutes,
                "start_time": start_time.isoformat(),
                "model_requirements": request.model_requirements,
                "priority": request.priority,
                "tenant_id": tenant_id
            },
            "persistent://public/default/compute-purchase-events"
        )
        
        return {
            "purchase_id": purchase_id,
            "status": "pending",
            "estimated_price": base_price,
            "start_time": start_time.isoformat(),
            "message": "Purchase request submitted. You will be notified once confirmed."
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
    required_duration_minutes: Optional[int] = None,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")  # TODO: Get from auth
):
    """Search for available compute offerings"""
    try:
        offering_repo = ComputeOfferingRepository(db)
        
        # Search offerings
        offerings = offering_repo.search_offerings(
            resource_type=ResourceType(resource_type) if resource_type else None,
            max_price=max_price_per_hour,
            location=location,
            min_duration=required_duration_minutes,
            tenant_id=tenant_id
        )
        
        # Filter by resource specs if provided
        filtered_offerings = []
        for offering in offerings:
            specs = offering.resource_specs
            
            # Check memory requirement
            if min_memory_gb and specs.get("memory_gb", 0) < min_memory_gb:
                continue
                
            # Check CPU requirement
            if min_vcpus and specs.get("vcpus", 0) < min_vcpus:
                continue
                
            # Check GPU type
            if gpu_type and specs.get("gpu_type") != gpu_type:
                continue
                
            filtered_offerings.append(offering)
        
        return [
            {
                "offering_id": o.offering_id,
                "provider_id": o.provider_id,
                "resource_type": o.resource_type.value,
                "resource_specs": o.resource_specs,
                "price_per_hour": o.price_per_hour,
                "location": o.location,
                "rating": o.rating,
                "total_hours_sold": o.total_hours_sold,
                "tags": o.tags
            }
            for o in filtered_offerings
        ]
        
    except Exception as e:
        logger.error(f"Error searching offerings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/purchases/{purchase_id}", response_model=Dict[str, Any])
async def get_purchase_details(
    purchase_id: str,
    db: Session = Depends(get_db)
):
    """Get details of a compute purchase"""
    try:
        purchase_repo = ComputePurchaseRepository(db)
        
        purchase = purchase_repo.get_by_purchase_id(purchase_id)
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
            "status": purchase.status.value,
            "priority": purchase.priority.value,
            "model_requirements": purchase.model_requirements,
            "allocated_resources": purchase.allocated_resources,
            "usage_metrics": purchase.usage_metrics
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