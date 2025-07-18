"""
Data Lake Service Models

SQLAlchemy models for data lake metadata and tracking.
"""

from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy import (
    Column, String, DateTime, Float, Integer, JSON, 
    Boolean, Text, ForeignKey, Index, Enum
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid
import enum

from platformq_shared.database import Base


class IngestionStatus(str, enum.Enum):
    """Ingestion status enum"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    QUARANTINED = "quarantined"
    REJECTED = "rejected"


class DataLayer(str, enum.Enum):
    """Data lake layer enum"""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class JobStatus(str, enum.Enum):
    """Job status enum"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class QualityCheckStatus(str, enum.Enum):
    """Quality check status enum"""
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"


class DataIngestion(Base):
    """Data ingestion tracking"""
    __tablename__ = "data_ingestions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(String(255), nullable=False, index=True)
    asset_id = Column(String(255), index=True)
    source_name = Column(String(255), nullable=False)
    source_type = Column(String(100), nullable=False)
    source_path = Column(String(1000))
    destination_path = Column(String(1000))
    status = Column(Enum(IngestionStatus), default=IngestionStatus.PENDING, index=True)
    quality_score = Column(Float)
    records_processed = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    error_message = Column(Text)
    metadata = Column(JSON, default=dict)
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_ingestion_tenant_status', 'tenant_id', 'status'),
        Index('idx_ingestion_created', 'created_at'),
    )


class DataQualityCheck(Base):
    """Data quality check results"""
    __tablename__ = "data_quality_checks"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(String(255), nullable=False, index=True)
    dataset_id = Column(String(255), nullable=False, index=True)
    layer = Column(Enum(DataLayer), nullable=False)
    status = Column(Enum(QualityCheckStatus), default=QualityCheckStatus.RUNNING)
    total_checks = Column(Integer, default=0)
    passed_checks = Column(Integer, default=0)
    failed_checks = Column(Integer, default=0)
    quality_score = Column(Float)
    results = Column(JSON, default=dict)
    error_message = Column(Text)
    check_date = Column(DateTime, default=datetime.utcnow, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_quality_tenant_dataset', 'tenant_id', 'dataset_id'),
        Index('idx_quality_check_date', 'check_date'),
    )


class DataLineage(Base):
    """Data lineage tracking"""
    __tablename__ = "data_lineage"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(String(255), nullable=False, index=True)
    source_dataset_id = Column(String(255), nullable=False, index=True)
    target_dataset_id = Column(String(255), nullable=False, index=True)
    transformation_type = Column(String(100), nullable=False)
    transformation_details = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_lineage_source', 'tenant_id', 'source_dataset_id'),
        Index('idx_lineage_target', 'tenant_id', 'target_dataset_id'),
    )


class DataCatalogEntry(Base):
    """Data catalog entries"""
    __tablename__ = "data_catalog_entries"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(String(255), nullable=False, index=True)
    dataset_id = Column(String(255), nullable=False, unique=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    layer = Column(Enum(DataLayer), nullable=False, index=True)
    path = Column(String(1000), nullable=False)
    format = Column(String(50))  # parquet, delta, csv, etc.
    schema = Column(JSON)  # Column definitions
    statistics = Column(JSON)  # Row count, size, etc.
    tags = Column(JSON, default=list)  # Searchable tags
    owner = Column(String(255))
    classification = Column(String(100))  # PII, sensitive, public, etc.
    retention_days = Column(Integer)
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_catalog_tenant_layer', 'tenant_id', 'layer'),
        Index('idx_catalog_name', 'name'),
        Index('idx_catalog_active', 'is_active'),
    )


class ProcessingJob(Base):
    """Processing job tracking"""
    __tablename__ = "processing_jobs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(String(255), nullable=False, index=True)
    job_type = Column(String(100), nullable=False)  # ingestion, transformation, promotion, etc.
    dataset_id = Column(String(255), index=True)
    status = Column(Enum(JobStatus), default=JobStatus.PENDING, index=True)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    error_message = Column(Text)
    metadata = Column(JSON, default=dict)
    output_path = Column(String(1000))
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_job_tenant_status', 'tenant_id', 'status'),
        Index('idx_job_dataset', 'dataset_id'),
        Index('idx_job_created', 'created_at'),
    ) 