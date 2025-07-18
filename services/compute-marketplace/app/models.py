"""
Compute Marketplace Models

SQLAlchemy models for compute offerings and purchases.
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlalchemy import (
    Column, String, DateTime, Float, Integer, JSON, 
    Boolean, Text, ForeignKey, Index, Enum, ARRAY
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid
import enum

from platformq_shared.database import Base


class ResourceType(str, enum.Enum):
    """Compute resource types"""
    CPU = "cpu"
    GPU = "gpu"
    TPU = "tpu"
    FPGA = "fpga"


class OfferingStatus(str, enum.Enum):
    """Offering status"""
    ACTIVE = "active"
    PAUSED = "paused"
    SOLD_OUT = "sold_out"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class PurchaseStatus(str, enum.Enum):
    """Purchase status"""
    PENDING = "pending"
    CONFIRMED = "confirmed"
    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


class Priority(str, enum.Enum):
    """Job priority levels"""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class ComputeOffering(Base):
    """Compute offerings in the marketplace"""
    __tablename__ = "compute_offerings"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    offering_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    provider_id = Column(String(255), nullable=False, index=True)
    
    # Resource details
    resource_type = Column(Enum(ResourceType), nullable=False, index=True)
    resource_specs = Column(JSON, nullable=False)  # CPU cores, RAM, GPU model, etc.
    location = Column(String(255), nullable=False, index=True)
    
    # Availability
    availability_hours = Column(JSON, default=list)  # List of hours available
    min_duration_minutes = Column(Integer, nullable=False)
    max_duration_minutes = Column(Integer)
    
    # Pricing
    price_per_hour = Column(Float, nullable=False, index=True)
    currency = Column(String(10), default="USD")
    
    # Metadata
    status = Column(Enum(OfferingStatus), default=OfferingStatus.ACTIVE, index=True)
    tags = Column(JSON, default=list)
    description = Column(Text)
    
    # Tracking
    total_hours_sold = Column(Float, default=0)
    total_revenue = Column(Float, default=0)
    rating = Column(Float)
    rating_count = Column(Integer, default=0)
    
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    expires_at = Column(DateTime, index=True)
    
    # Relationships
    purchases = relationship("ComputePurchase", back_populates="offering")
    
    # Indexes
    __table_args__ = (
        Index('idx_offering_search', 'resource_type', 'status', 'location'),
        Index('idx_offering_price', 'price_per_hour', 'status'),
        Index('idx_offering_provider', 'provider_id', 'status'),
    )


class ComputePurchase(Base):
    """Compute time purchases"""
    __tablename__ = "compute_purchases"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    purchase_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    buyer_id = Column(String(255), nullable=False, index=True)
    offering_id = Column(String(255), ForeignKey('compute_offerings.offering_id'), nullable=False)
    
    # Purchase details
    duration_minutes = Column(Integer, nullable=False)
    start_time = Column(DateTime, nullable=False, index=True)
    end_time = Column(DateTime, nullable=False, index=True)
    
    # Pricing
    price_per_hour = Column(Float, nullable=False)
    total_price = Column(Float, nullable=False)
    currency = Column(String(10), default="USD")
    
    # Execution details
    model_requirements = Column(JSON, default=dict)
    priority = Column(Enum(Priority), default=Priority.NORMAL, index=True)
    status = Column(Enum(PurchaseStatus), default=PurchaseStatus.PENDING, index=True)
    
    # Resource allocation
    allocated_resources = Column(JSON)  # Actual allocated resources
    execution_node = Column(String(255))  # Which node is running the job
    
    # Tracking
    actual_start_time = Column(DateTime)
    actual_end_time = Column(DateTime)
    usage_metrics = Column(JSON, default=dict)  # CPU%, memory%, etc.
    output_location = Column(String(1000))  # Where results are stored
    
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    offering = relationship("ComputeOffering", back_populates="purchases")
    
    # Indexes
    __table_args__ = (
        Index('idx_purchase_buyer', 'buyer_id', 'status'),
        Index('idx_purchase_time', 'start_time', 'end_time'),
        Index('idx_purchase_status', 'status', 'priority'),
    )


class ResourceAvailability(Base):
    """Track resource availability over time"""
    __tablename__ = "resource_availability"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    offering_id = Column(String(255), ForeignKey('compute_offerings.offering_id'), nullable=False)
    date = Column(DateTime, nullable=False, index=True)
    hour = Column(Integer, nullable=False)  # 0-23
    
    # Capacity
    total_capacity = Column(Float, nullable=False)  # Total available units
    used_capacity = Column(Float, default=0)  # Currently allocated
    reserved_capacity = Column(Float, default=0)  # Reserved but not yet used
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_availability_lookup', 'offering_id', 'date', 'hour'),
        Index('idx_availability_date', 'date'),
    )


class PricingRule(Base):
    """Dynamic pricing rules"""
    __tablename__ = "pricing_rules"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(String(255), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    
    # Rule conditions
    resource_type = Column(Enum(ResourceType), index=True)
    priority = Column(Enum(Priority), index=True)
    min_duration = Column(Integer)
    max_duration = Column(Integer)
    time_of_day_start = Column(Integer)  # Hour 0-23
    time_of_day_end = Column(Integer)
    day_of_week = Column(ARRAY(Integer))  # 0-6 (Monday-Sunday)
    
    # Pricing adjustments
    multiplier = Column(Float, default=1.0)  # Price multiplier
    fixed_adjustment = Column(Float, default=0)  # Fixed price adjustment
    
    # Status
    is_active = Column(Boolean, default=True, index=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow) 