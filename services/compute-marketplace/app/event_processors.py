"""
Compute Marketplace Event Processors

Handles events for compute offerings, purchases, and resource management.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import uuid

from platformq_shared.event_framework import EventProcessor, event_handler
from platformq_shared.events import (
    AssetUsed,
    ComputeOfferingCreated,
    ComputePurchaseRequested,
    ComputePurchaseConfirmed,
    ComputeResourceAllocated,
    ComputeJobStarted,
    ComputeJobCompleted,
    PaymentProcessed,
    ResourceAvailabilityUpdated
)
from platformq_shared.database import get_db
from .repository import (
    ComputeOfferingRepository,
    ComputePurchaseRepository,
    ResourceAvailabilityRepository,
    PricingRuleRepository
)
from .models import (
    ComputeOffering,
    ComputePurchase,
    OfferingStatus,
    PurchaseStatus,
    Priority
)
from .models import ResourceAvailability
from sqlalchemy import and_

logger = logging.getLogger(__name__)


class ComputeMarketplaceEventProcessor(EventProcessor):
    """Event processor for compute marketplace operations"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.offering_repo = None
        self.purchase_repo = None
        self.availability_repo = None
        self.pricing_repo = None
        
    def initialize_repos(self):
        """Initialize repositories"""
        if not self.offering_repo:
            db = next(get_db())
            self.offering_repo = ComputeOfferingRepository(db)
            self.purchase_repo = ComputePurchaseRepository(db)
            self.availability_repo = ResourceAvailabilityRepository(db)
            self.pricing_repo = PricingRuleRepository(db)
    
    @event_handler("persistent://public/default/compute-purchase-events")
    async def handle_purchase_request(self, event: ComputePurchaseRequested):
        """Handle compute purchase request"""
        self.initialize_repos()
        
        try:
            # Get offering
            offering = self.offering_repo.get_by_offering_id(event.offering_id)
            if not offering or offering.status != OfferingStatus.ACTIVE:
                logger.error(f"Offering {event.offering_id} not available")
                await self._publish_purchase_failed(event, "Offering not available")
                return
            
            # Validate duration
            if event.duration_minutes < offering.min_duration_minutes:
                await self._publish_purchase_failed(
                    event, 
                    f"Duration below minimum: {offering.min_duration_minutes} minutes"
                )
                return
            
            if offering.max_duration_minutes and event.duration_minutes > offering.max_duration_minutes:
                await self._publish_purchase_failed(
                    event,
                    f"Duration exceeds maximum: {offering.max_duration_minutes} minutes"
                )
                return
            
            # Check availability
            start_time = event.start_time or datetime.utcnow()
            if not self.availability_repo.check_availability(
                event.offering_id, 
                start_time, 
                event.duration_minutes
            ):
                await self._publish_purchase_failed(event, "Resources not available for requested time")
                return
            
            # Calculate price with dynamic pricing rules
            base_price = offering.price_per_hour * (event.duration_minutes / 60)
            final_price = await self._calculate_final_price(
                offering,
                event.priority,
                event.duration_minutes,
                start_time,
                base_price
            )
            
            # Create purchase record
            purchase = self.purchase_repo.create({
                "purchase_id": str(uuid.uuid4()),
                "tenant_id": event.tenant_id,
                "buyer_id": event.buyer_id,
                "offering_id": event.offering_id,
                "duration_minutes": event.duration_minutes,
                "start_time": start_time,
                "end_time": start_time + timedelta(minutes=event.duration_minutes),
                "price_per_hour": offering.price_per_hour,
                "total_price": final_price,
                "model_requirements": event.model_requirements,
                "priority": event.priority,
                "status": PurchaseStatus.PENDING
            })
            
            # Reserve capacity
            self.availability_repo.reserve_capacity(
                event.offering_id,
                start_time,
                event.duration_minutes
            )
            
            # Publish confirmation event
            await self.publish_event(
                ComputePurchaseConfirmed(
                    purchase_id=purchase.purchase_id,
                    offering_id=event.offering_id,
                    buyer_id=event.buyer_id,
                    start_time=start_time,
                    duration_minutes=event.duration_minutes,
                    total_price=final_price,
                    tenant_id=event.tenant_id
                ),
                "persistent://public/default/compute-purchase-events"
            )
            
            logger.info(f"Purchase {purchase.purchase_id} created for buyer {event.buyer_id}")
            
        except Exception as e:
            logger.error(f"Error handling purchase request: {e}")
            await self._publish_purchase_failed(event, str(e))
    
    @event_handler("persistent://public/default/payment-events")
    async def handle_payment_processed(self, event: PaymentProcessed):
        """Handle payment confirmation for compute purchase"""
        self.initialize_repos()
        
        try:
            # Only process compute-related payments
            if event.payment_type != "compute_purchase":
                return
            
            purchase = self.purchase_repo.get_by_purchase_id(event.reference_id)
            if not purchase:
                logger.error(f"Purchase {event.reference_id} not found")
                return
            
            if event.status == "success":
                # Update purchase status
                self.purchase_repo.update(purchase.id, {
                    "status": PurchaseStatus.CONFIRMED
                })
                
                # Publish resource allocation request
                await self.publish_event(
                    ComputeResourceAllocated(
                        purchase_id=purchase.purchase_id,
                        offering_id=purchase.offering_id,
                        resources=purchase.allocated_resources,
                        start_time=purchase.start_time,
                        duration_minutes=purchase.duration_minutes,
                        tenant_id=purchase.tenant_id
                    ),
                    "persistent://public/default/resource-allocation-events"
                )
                
                # Update offering stats
                hours_sold = purchase.duration_minutes / 60
                self.offering_repo.update_offering_stats(
                    purchase.offering_id,
                    hours_sold,
                    purchase.total_price
                )
                
            else:
                # Payment failed - cancel purchase
                self.purchase_repo.update(purchase.id, {
                    "status": PurchaseStatus.CANCELLED
                })
                
                # Release reserved capacity
                self._release_capacity(purchase)
                
        except Exception as e:
            logger.error(f"Error handling payment processed: {e}")
    
    @event_handler("persistent://public/default/compute-job-events")
    async def handle_job_started(self, event: ComputeJobStarted):
        """Handle compute job start notification"""
        self.initialize_repos()
        
        try:
            purchase = self.purchase_repo.get_by_purchase_id(event.purchase_id)
            if not purchase:
                logger.error(f"Purchase {event.purchase_id} not found")
                return
            
            # Update purchase with execution details
            self.purchase_repo.update(purchase.id, {
                "status": PurchaseStatus.ACTIVE,
                "actual_start_time": event.start_time,
                "execution_node": event.execution_node,
                "allocated_resources": event.allocated_resources
            })
            
            # Convert reserved capacity to used capacity
            self.availability_repo.reserve_capacity(
                purchase.offering_id,
                purchase.start_time,
                purchase.duration_minutes,
                capacity_units=-1.0  # Negative to reduce reserved
            )
            
            # Track asset usage
            await self.publish_event(
                AssetUsed(
                    tenant_id=purchase.tenant_id,
                    asset_id=purchase.offering_id,
                    user_id=purchase.buyer_id,
                    usage_duration_minutes=purchase.duration_minutes,
                    usage_type="compute_time",
                    event_timestamp=int(datetime.utcnow().timestamp() * 1000)
                ),
                "persistent://public/default/asset-usage-events"
            )
            
        except Exception as e:
            logger.error(f"Error handling job started: {e}")
    
    @event_handler("persistent://public/default/compute-job-events")
    async def handle_job_completed(self, event: ComputeJobCompleted):
        """Handle compute job completion"""
        self.initialize_repos()
        
        try:
            purchase = self.purchase_repo.get_by_purchase_id(event.purchase_id)
            if not purchase:
                logger.error(f"Purchase {event.purchase_id} not found")
                return
            
            # Update purchase completion
            self.purchase_repo.update(purchase.id, {
                "status": PurchaseStatus.COMPLETED,
                "actual_end_time": event.end_time,
                "usage_metrics": event.usage_metrics,
                "output_location": event.output_location
            })
            
            # Release used capacity
            actual_duration = int((event.end_time - purchase.actual_start_time).total_seconds() / 60)
            remaining_duration = purchase.duration_minutes - actual_duration
            
            if remaining_duration > 0:
                # Release unused capacity
                end_time = purchase.start_time + timedelta(minutes=actual_duration)
                self._release_capacity_range(
                    purchase.offering_id,
                    end_time,
                    purchase.end_time
                )
            
            logger.info(f"Compute job {event.purchase_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Error handling job completed: {e}")
    
    @event_handler("persistent://public/default/offering-events")
    async def handle_offering_created(self, event: ComputeOfferingCreated):
        """Handle new compute offering creation"""
        self.initialize_repos()
        
        try:
            # Create initial availability records
            start_date = datetime.utcnow().date()
            end_date = (datetime.utcnow() + timedelta(days=30)).date()
            
            current_date = start_date
            while current_date <= end_date:
                for hour in event.availability_hours:
                    self.availability_repo.create({
                        "offering_id": event.offering_id,
                        "date": current_date,
                        "hour": hour,
                        "total_capacity": event.capacity_units or 1.0,
                        "used_capacity": 0,
                        "reserved_capacity": 0
                    })
                current_date += timedelta(days=1)
            
            logger.info(f"Created availability for offering {event.offering_id}")
            
        except Exception as e:
            logger.error(f"Error handling offering created: {e}")
    
    async def _calculate_final_price(
        self,
        offering: ComputeOffering,
        priority: Priority,
        duration_minutes: int,
        start_time: datetime,
        base_price: float
    ) -> float:
        """Calculate final price with dynamic pricing rules"""
        # Get applicable pricing rules
        rules = self.pricing_repo.get_applicable_rules(
            offering.tenant_id,
            offering.resource_type,
            priority,
            duration_minutes,
            start_time
        )
        
        final_price = base_price
        
        # Apply pricing rules
        for rule in rules:
            # Check day of week if specified
            if rule.day_of_week:
                if start_time.weekday() not in rule.day_of_week:
                    continue
            
            # Apply adjustments
            final_price = final_price * rule.multiplier + rule.fixed_adjustment
        
        # Priority-based pricing
        priority_multipliers = {
            Priority.LOW: 0.8,
            Priority.NORMAL: 1.0,
            Priority.HIGH: 1.5,
            Priority.URGENT: 2.0
        }
        final_price *= priority_multipliers.get(priority, 1.0)
        
        return max(final_price, 0)  # Ensure non-negative price
    
    async def _publish_purchase_failed(self, event: ComputePurchaseRequested, reason: str):
        """Publish purchase failed event"""
        await self.publish_event(
            {
                "purchase_request_id": event.request_id,
                "buyer_id": event.buyer_id,
                "offering_id": event.offering_id,
                "reason": reason,
                "timestamp": datetime.utcnow().isoformat()
            },
            "persistent://public/default/compute-purchase-failed-events"
        )
    
    def _release_capacity(self, purchase: ComputePurchase):
        """Release reserved capacity for a cancelled purchase"""
        self._release_capacity_range(
            purchase.offering_id,
            purchase.start_time,
            purchase.end_time
        )
    
    def _release_capacity_range(self, offering_id: str, start_time: datetime, end_time: datetime):
        """Release capacity for a time range"""
        current_time = start_time
        while current_time < end_time:
            availability = self.availability_repo.db.query(ResourceAvailability).filter(
                and_(
                    ResourceAvailability.offering_id == offering_id,
                    ResourceAvailability.date == current_time.date(),
                    ResourceAvailability.hour == current_time.hour
                )
            ).first()
            
            if availability:
                availability.reserved_capacity = max(0, availability.reserved_capacity - 1.0)
            
            current_time += timedelta(hours=1)
        
        self.availability_repo.db.commit() 