"""
Compute Marketplace Repository

Handles database operations for compute offerings and purchases.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy import select, and_, or_, func
from sqlalchemy.orm import Session

from platformq_shared.repository import BaseRepository, AsyncBaseRepository
from .models import (
    ComputeOffering,
    ComputePurchase,
    ResourceAvailability,
    PricingRule,
    ResourceType,
    OfferingStatus,
    PurchaseStatus,
    Priority
)


class ComputeOfferingRepository(BaseRepository[ComputeOffering]):
    """Repository for compute offerings"""
    
    def __init__(self, db: Session):
        super().__init__(ComputeOffering, db)
    
    def search_offerings(
        self, 
        resource_type: Optional[ResourceType] = None,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
        location: Optional[str] = None,
        min_duration: Optional[int] = None,
        tenant_id: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> List[ComputeOffering]:
        """Search for compute offerings based on criteria"""
        query = self.db.query(ComputeOffering).filter(
            ComputeOffering.status == OfferingStatus.ACTIVE
        )
        
        if resource_type:
            query = query.filter(ComputeOffering.resource_type == resource_type)
        
        if min_price is not None:
            query = query.filter(ComputeOffering.price_per_hour >= min_price)
        
        if max_price is not None:
            query = query.filter(ComputeOffering.price_per_hour <= max_price)
        
        if location:
            query = query.filter(ComputeOffering.location == location)
        
        if min_duration:
            query = query.filter(ComputeOffering.min_duration_minutes <= min_duration)
        
        if tenant_id:
            query = query.filter(ComputeOffering.tenant_id == tenant_id)
        
        if tags:
            # Filter by tags (assuming JSON array contains)
            for tag in tags:
                query = query.filter(ComputeOffering.tags.contains([tag]))
        
        return query.all()
    
    def get_by_offering_id(self, offering_id: str) -> Optional[ComputeOffering]:
        """Get offering by offering_id"""
        return self.db.query(ComputeOffering).filter(
            ComputeOffering.offering_id == offering_id
        ).first()
    
    def get_provider_offerings(self, provider_id: str, tenant_id: str) -> List[ComputeOffering]:
        """Get all offerings from a provider"""
        return self.db.query(ComputeOffering).filter(
            and_(
                ComputeOffering.provider_id == provider_id,
                ComputeOffering.tenant_id == tenant_id
            )
        ).all()
    
    def update_offering_stats(self, offering_id: str, hours_sold: float, revenue: float):
        """Update offering statistics"""
        offering = self.get_by_offering_id(offering_id)
        if offering:
            offering.total_hours_sold += hours_sold
            offering.total_revenue += revenue
            self.db.commit()


class ComputePurchaseRepository(BaseRepository[ComputePurchase]):
    """Repository for compute purchases"""
    
    def __init__(self, db: Session):
        super().__init__(ComputePurchase, db)
    
    def get_by_purchase_id(self, purchase_id: str) -> Optional[ComputePurchase]:
        """Get purchase by purchase_id"""
        return self.db.query(ComputePurchase).filter(
            ComputePurchase.purchase_id == purchase_id
        ).first()
    
    def get_buyer_purchases(
        self, 
        buyer_id: str, 
        tenant_id: str,
        status: Optional[PurchaseStatus] = None
    ) -> List[ComputePurchase]:
        """Get all purchases for a buyer"""
        query = self.db.query(ComputePurchase).filter(
            and_(
                ComputePurchase.buyer_id == buyer_id,
                ComputePurchase.tenant_id == tenant_id
            )
        )
        
        if status:
            query = query.filter(ComputePurchase.status == status)
        
        return query.order_by(ComputePurchase.created_at.desc()).all()
    
    def get_active_purchases(self, tenant_id: str) -> List[ComputePurchase]:
        """Get all active purchases"""
        now = datetime.utcnow()
        return self.db.query(ComputePurchase).filter(
            and_(
                ComputePurchase.tenant_id == tenant_id,
                ComputePurchase.status == PurchaseStatus.ACTIVE,
                ComputePurchase.start_time <= now,
                ComputePurchase.end_time >= now
            )
        ).all()
    
    def get_upcoming_purchases(self, tenant_id: str, hours_ahead: int = 24) -> List[ComputePurchase]:
        """Get purchases starting in the next N hours"""
        now = datetime.utcnow()
        future_time = now + timedelta(hours=hours_ahead)
        
        return self.db.query(ComputePurchase).filter(
            and_(
                ComputePurchase.tenant_id == tenant_id,
                ComputePurchase.status.in_([PurchaseStatus.PENDING, PurchaseStatus.CONFIRMED]),
                ComputePurchase.start_time >= now,
                ComputePurchase.start_time <= future_time
            )
        ).all()
    
    def calculate_buyer_spending(self, buyer_id: str, tenant_id: str, days: int = 30) -> Dict[str, Any]:
        """Calculate buyer spending over time period"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        result = self.db.query(
            func.count(ComputePurchase.id).label('total_purchases'),
            func.sum(ComputePurchase.total_price).label('total_spent'),
            func.sum(ComputePurchase.duration_minutes).label('total_minutes')
        ).filter(
            and_(
                ComputePurchase.buyer_id == buyer_id,
                ComputePurchase.tenant_id == tenant_id,
                ComputePurchase.created_at >= cutoff_date,
                ComputePurchase.status.in_([
                    PurchaseStatus.COMPLETED,
                    PurchaseStatus.ACTIVE
                ])
            )
        ).first()
        
        return {
            "total_purchases": result.total_purchases or 0,
            "total_spent": float(result.total_spent or 0),
            "total_hours": (result.total_minutes or 0) / 60
        }


class ResourceAvailabilityRepository(BaseRepository[ResourceAvailability]):
    """Repository for resource availability tracking"""
    
    def __init__(self, db: Session):
        super().__init__(ResourceAvailability, db)
    
    def get_availability(
        self, 
        offering_id: str, 
        start_time: datetime, 
        end_time: datetime
    ) -> List[ResourceAvailability]:
        """Get availability for an offering in a time range"""
        return self.db.query(ResourceAvailability).filter(
            and_(
                ResourceAvailability.offering_id == offering_id,
                ResourceAvailability.date >= start_time.date(),
                ResourceAvailability.date <= end_time.date()
            )
        ).all()
    
    def check_availability(
        self, 
        offering_id: str, 
        start_time: datetime, 
        duration_minutes: int
    ) -> bool:
        """Check if resources are available for the requested time"""
        end_time = start_time + timedelta(minutes=duration_minutes)
        
        # Get all availability records in the time range
        availabilities = self.get_availability(offering_id, start_time, end_time)
        
        # Check each hour slot
        current_time = start_time
        while current_time < end_time:
            hour_availability = next(
                (a for a in availabilities 
                 if a.date.date() == current_time.date() and a.hour == current_time.hour),
                None
            )
            
            if not hour_availability:
                return False  # No availability record
            
            if hour_availability.used_capacity + hour_availability.reserved_capacity >= hour_availability.total_capacity:
                return False  # No capacity available
            
            current_time += timedelta(hours=1)
        
        return True
    
    def reserve_capacity(
        self, 
        offering_id: str, 
        start_time: datetime, 
        duration_minutes: int,
        capacity_units: float = 1.0
    ):
        """Reserve capacity for a purchase"""
        end_time = start_time + timedelta(minutes=duration_minutes)
        current_time = start_time
        
        while current_time < end_time:
            # Get or create availability record
            availability = self.db.query(ResourceAvailability).filter(
                and_(
                    ResourceAvailability.offering_id == offering_id,
                    ResourceAvailability.date == current_time.date(),
                    ResourceAvailability.hour == current_time.hour
                )
            ).first()
            
            if availability:
                availability.reserved_capacity += capacity_units
            else:
                # Create new availability record
                availability = ResourceAvailability(
                    offering_id=offering_id,
                    date=current_time.date(),
                    hour=current_time.hour,
                    total_capacity=10.0,  # Default capacity
                    reserved_capacity=capacity_units
                )
                self.db.add(availability)
            
            current_time += timedelta(hours=1)
        
        self.db.commit()


class PricingRuleRepository(BaseRepository[PricingRule]):
    """Repository for pricing rules"""
    
    def __init__(self, db: Session):
        super().__init__(PricingRule, db)
    
    def get_applicable_rules(
        self,
        tenant_id: str,
        resource_type: ResourceType,
        priority: Priority,
        duration_minutes: int,
        start_time: datetime
    ) -> List[PricingRule]:
        """Get all pricing rules that apply to a purchase"""
        hour = start_time.hour
        day_of_week = start_time.weekday()
        
        query = self.db.query(PricingRule).filter(
            and_(
                PricingRule.tenant_id == tenant_id,
                PricingRule.is_active == True,
                or_(
                    PricingRule.start_date.is_(None),
                    PricingRule.start_date <= start_time
                ),
                or_(
                    PricingRule.end_date.is_(None),
                    PricingRule.end_date >= start_time
                )
            )
        )
        
        # Filter by resource type if specified
        query = query.filter(
            or_(
                PricingRule.resource_type.is_(None),
                PricingRule.resource_type == resource_type
            )
        )
        
        # Filter by priority if specified
        query = query.filter(
            or_(
                PricingRule.priority.is_(None),
                PricingRule.priority == priority
            )
        )
        
        # Filter by duration
        query = query.filter(
            or_(
                PricingRule.min_duration.is_(None),
                PricingRule.min_duration <= duration_minutes
            )
        )
        query = query.filter(
            or_(
                PricingRule.max_duration.is_(None),
                PricingRule.max_duration >= duration_minutes
            )
        )
        
        # Filter by time of day
        query = query.filter(
            or_(
                PricingRule.time_of_day_start.is_(None),
                PricingRule.time_of_day_start <= hour
            )
        )
        query = query.filter(
            or_(
                PricingRule.time_of_day_end.is_(None),
                PricingRule.time_of_day_end >= hour
            )
        )
        
        # Note: Day of week filtering would need to be done in Python
        # as PostgreSQL array contains is complex in SQLAlchemy
        
        return query.all() 