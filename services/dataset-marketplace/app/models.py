"""
Dataset Marketplace Models

SQLAlchemy models for dataset listings and transactions.
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlalchemy import (
    Column, String, DateTime, Float, Integer, JSON, 
    Boolean, Text, ForeignKey, Index, Enum, ARRAY, BigInteger
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid
import enum

from platformq_shared.database import Base


class DatasetType(str, enum.Enum):
    """Types of datasets"""
    IMAGE = "image"
    TEXT = "text"
    AUDIO = "audio"
    VIDEO = "video"
    TABULAR = "tabular"
    MULTIMODAL = "multimodal"
    SYNTHETIC = "synthetic"
    GRAPH = "graph"
    TIMESERIES = "timeseries"


class LicenseType(str, enum.Enum):
    """Dataset license types"""
    PROPRIETARY = "proprietary"
    CC_BY = "cc-by"
    CC_BY_SA = "cc-by-sa"
    CC_BY_NC = "cc-by-nc"
    CC_BY_NC_SA = "cc-by-nc-sa"
    MIT = "mit"
    APACHE = "apache"
    CUSTOM = "custom"


class DatasetStatus(str, enum.Enum):
    """Dataset listing status"""
    DRAFT = "draft"
    ACTIVE = "active"
    SOLD_OUT = "sold_out"
    SUSPENDED = "suspended"
    ARCHIVED = "archived"


class PurchaseStatus(str, enum.Enum):
    """Purchase transaction status"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    REFUNDED = "refunded"


class DatasetListing(Base):
    """Dataset marketplace listings"""
    __tablename__ = "dataset_listings"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    listing_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    seller_id = Column(String(255), nullable=False, index=True)
    
    # Dataset information
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text, nullable=False)
    dataset_type = Column(Enum(DatasetType), nullable=False, index=True)
    format = Column(String(100))  # parquet, csv, json, etc.
    size_bytes = Column(BigInteger, nullable=False)
    num_samples = Column(Integer, nullable=False)
    
    # Content details
    schema_info = Column(JSON)  # Column names, data types, etc.
    sample_data = Column(JSON)  # Preview samples
    statistics = Column(JSON)  # Mean, std, distributions, etc.
    tags = Column(JSON, default=list)  # Searchable tags
    categories = Column(JSON, default=list)  # Hierarchical categories
    
    # Quality metrics
    quality_score = Column(Float)  # 0-100
    completeness = Column(Float)  # Percentage of non-null values
    annotation_quality = Column(Float)  # For labeled datasets
    validation_results = Column(JSON)
    
    # Licensing and pricing
    license_type = Column(Enum(LicenseType), nullable=False)
    license_details = Column(Text)
    price = Column(Float, nullable=False, index=True)
    currency = Column(String(10), default="USD")
    max_licenses = Column(Integer)  # None = unlimited
    licenses_sold = Column(Integer, default=0)
    
    # Access control
    download_url = Column(String(1000))  # Secured URL
    access_method = Column(String(50))  # direct, api, streaming
    encryption_key = Column(String(500))  # For encrypted datasets
    
    # Status and tracking
    status = Column(Enum(DatasetStatus), default=DatasetStatus.DRAFT, index=True)
    total_revenue = Column(Float, default=0)
    view_count = Column(Integer, default=0)
    rating = Column(Float)
    rating_count = Column(Integer, default=0)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    published_at = Column(DateTime)
    expires_at = Column(DateTime)
    
    # Relationships
    purchases = relationship("DatasetPurchase", back_populates="listing")
    reviews = relationship("DatasetReview", back_populates="listing")
    
    # Indexes
    __table_args__ = (
        Index('idx_dataset_search', 'dataset_type', 'status', 'tenant_id'),
        Index('idx_dataset_price', 'price', 'status'),
        Index('idx_dataset_seller', 'seller_id', 'status'),
    )


class DatasetPurchase(Base):
    """Dataset purchase transactions"""
    __tablename__ = "dataset_purchases"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    purchase_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    buyer_id = Column(String(255), nullable=False, index=True)
    listing_id = Column(String(255), ForeignKey('dataset_listings.listing_id'), nullable=False)
    
    # Purchase details
    price = Column(Float, nullable=False)
    currency = Column(String(10), default="USD")
    license_count = Column(Integer, default=1)  # Number of licenses purchased
    total_amount = Column(Float, nullable=False)
    
    # Transaction info
    payment_method = Column(String(50))
    transaction_id = Column(String(255))
    status = Column(Enum(PurchaseStatus), default=PurchaseStatus.PENDING, index=True)
    
    # Access details
    access_token = Column(String(500), unique=True)  # For download authentication
    download_count = Column(Integer, default=0)
    max_downloads = Column(Integer, default=5)  # Download limit
    download_expiry = Column(DateTime)
    
    # Usage tracking
    first_download_at = Column(DateTime)
    last_download_at = Column(DateTime)
    download_ips = Column(JSON, default=list)  # Track download locations
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    completed_at = Column(DateTime)
    
    # Relationships
    listing = relationship("DatasetListing", back_populates="purchases")
    
    # Indexes
    __table_args__ = (
        Index('idx_purchase_buyer', 'buyer_id', 'status'),
        Index('idx_purchase_listing', 'listing_id', 'status'),
        Index('idx_purchase_token', 'access_token'),
    )


class DatasetReview(Base):
    """Dataset reviews and ratings"""
    __tablename__ = "dataset_reviews"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    listing_id = Column(String(255), ForeignKey('dataset_listings.listing_id'), nullable=False)
    buyer_id = Column(String(255), nullable=False, index=True)
    purchase_id = Column(String(255), ForeignKey('dataset_purchases.purchase_id'))
    
    # Review content
    rating = Column(Integer, nullable=False)  # 1-5 stars
    title = Column(String(255))
    comment = Column(Text)
    
    # Quality assessment
    data_quality_rating = Column(Integer)  # 1-5
    documentation_rating = Column(Integer)  # 1-5
    value_rating = Column(Integer)  # 1-5
    
    # Verification
    verified_purchase = Column(Boolean, default=True)
    helpful_count = Column(Integer, default=0)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    listing = relationship("DatasetListing", back_populates="reviews")
    
    # Indexes
    __table_args__ = (
        Index('idx_review_listing', 'listing_id', 'created_at'),
        Index('idx_review_buyer', 'buyer_id'),
    )


class DatasetAnalytics(Base):
    """Analytics tracking for datasets"""
    __tablename__ = "dataset_analytics"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    listing_id = Column(String(255), ForeignKey('dataset_listings.listing_id'), nullable=False, index=True)
    date = Column(DateTime, nullable=False, index=True)
    
    # Metrics
    views = Column(Integer, default=0)
    unique_viewers = Column(Integer, default=0)
    purchases = Column(Integer, default=0)
    revenue = Column(Float, default=0)
    downloads = Column(Integer, default=0)
    
    # User engagement
    avg_time_on_page = Column(Float)  # seconds
    bounce_rate = Column(Float)  # percentage
    conversion_rate = Column(Float)  # views to purchases
    
    # Geographic data
    viewer_countries = Column(JSON, default=dict)  # country: count
    buyer_countries = Column(JSON, default=dict)
    
    # Traffic sources
    referrers = Column(JSON, default=dict)  # source: count
    search_keywords = Column(JSON, default=list)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_analytics_listing_date', 'listing_id', 'date'),
    )


class DatasetQualityCheck(Base):
    """Automated quality checks for datasets"""
    __tablename__ = "dataset_quality_checks"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    listing_id = Column(String(255), ForeignKey('dataset_listings.listing_id'), nullable=False, index=True)
    check_type = Column(String(100), nullable=False)  # schema, format, content, etc.
    
    # Check results
    status = Column(String(50), nullable=False)  # passed, failed, warning
    score = Column(Float)
    details = Column(JSON)
    error_count = Column(Integer, default=0)
    warning_count = Column(Integer, default=0)
    
    # Recommendations
    recommendations = Column(JSON, default=list)
    auto_fixable = Column(Boolean, default=False)
    
    # Timestamps
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    
    # Indexes
    __table_args__ = (
        Index('idx_quality_listing', 'listing_id', 'check_type'),
    ) 