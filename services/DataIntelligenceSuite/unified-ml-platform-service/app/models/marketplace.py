"""
Database models for ML Marketplace
"""

from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy import Column, String, Float, Integer, Boolean, DateTime, JSON, ForeignKey, Text, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class MarketplaceModel(Base):
    """Model listing in the marketplace"""
    __tablename__ = "marketplace_models"
    
    id = Column(String, primary_key=True)
    model_id = Column(String, nullable=False, index=True)
    publisher_id = Column(String, nullable=False, index=True)
    
    # Basic information
    name = Column(String, nullable=False)
    description = Column(Text)
    category = Column(String, nullable=False, index=True)
    visibility = Column(String, nullable=False, default="private")
    
    # Pricing
    price = Column(Float, default=0.0)
    currency = Column(String, default="USD")
    
    # Technical details
    framework = Column(String)
    version = Column(String)
    tags = Column(ARRAY(String), default=[])
    requirements = Column(JSON, default={})
    metrics = Column(JSON, default={})
    
    # Statistics
    download_count = Column(Integer, default=0)
    view_count = Column(Integer, default=0)
    average_rating = Column(Float, default=0.0)
    rating_count = Column(Integer, default=0)
    
    # Timestamps
    published_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, onupdate=datetime.utcnow)
    
    # Status
    is_active = Column(Boolean, default=True)
    
    # Relationships
    ratings = relationship("ModelRating", back_populates="marketplace_model", cascade="all, delete-orphan")
    usage_records = relationship("ModelUsage", back_populates="marketplace_model", cascade="all, delete-orphan")


class ModelRating(Base):
    """User ratings for marketplace models"""
    __tablename__ = "model_ratings"
    
    id = Column(String, primary_key=True)
    marketplace_model_id = Column(String, ForeignKey("marketplace_models.id"), nullable=False)
    user_id = Column(String, nullable=False)
    
    rating = Column(Integer, nullable=False)  # 1-5 stars
    review = Column(Text)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, onupdate=datetime.utcnow)
    
    # Relationships
    marketplace_model = relationship("MarketplaceModel", back_populates="ratings")


class ModelUsage(Base):
    """Track model usage and downloads"""
    __tablename__ = "model_usage"
    
    id = Column(String, primary_key=True)
    marketplace_model_id = Column(String, ForeignKey("marketplace_models.id"), nullable=False)
    user_id = Column(String, nullable=False)
    
    action = Column(String, nullable=False)  # download, view, deploy
    metadata = Column(JSON, default={})
    timestamp = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    marketplace_model = relationship("MarketplaceModel", back_populates="usage_records") 