"""
Data Governance Service Models

SQLAlchemy models for data governance, quality, lineage, and compliance.
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlalchemy import (
    Column, String, DateTime, Float, Integer, JSON, 
    Boolean, Text, ForeignKey, Index, Enum
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid
import enum

from platformq_shared.database import Base


class AssetType(str, enum.Enum):
    """Types of data assets"""
    TABLE = "table"
    VIEW = "view"
    STREAM = "stream"
    FILE = "file"
    API = "api"
    DATABASE = "database"
    SCHEMA = "schema"
    DASHBOARD = "dashboard"
    MODEL = "model"


class PolicyType(str, enum.Enum):
    """Types of policies"""
    ACCESS = "access"
    RETENTION = "retention"
    QUALITY = "quality"
    CLASSIFICATION = "classification"
    ENCRYPTION = "encryption"
    MASKING = "masking"


class ComplianceType(str, enum.Enum):
    """Compliance framework types"""
    GDPR = "gdpr"
    CCPA = "ccpa"
    HIPAA = "hipaa"
    SOX = "sox"
    PCI_DSS = "pci_dss"
    ISO_27001 = "iso_27001"
    CUSTOM = "custom"


class ComplianceStatus(str, enum.Enum):
    """Compliance check status"""
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    NOT_APPLICABLE = "not_applicable"
    IN_PROGRESS = "in_progress"


class DataClassification(str, enum.Enum):
    """Data classification levels"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    TOP_SECRET = "top_secret"


class DataAsset(Base):
    """Data asset registry"""
    __tablename__ = "data_assets"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    asset_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    asset_name = Column(String(255), nullable=False, index=True)
    asset_type = Column(Enum(AssetType), nullable=False, index=True)
    
    # Location information
    catalog = Column(String(255), nullable=False)
    schema = Column(String(255), nullable=False)
    table = Column(String(255))  # For table/view assets
    path = Column(String(1000))  # For file assets
    
    # Metadata
    owner = Column(String(255), index=True)
    description = Column(Text)
    tags = Column(JSON, default=list)
    metadata = Column(JSON, default=dict)
    
    # Classification and sensitivity
    classification = Column(Enum(DataClassification), default=DataClassification.INTERNAL)
    sensitivity_labels = Column(JSON, default=list)  # PII, PHI, etc.
    
    # External system references
    atlas_guid = Column(String(255))  # Apache Atlas GUID
    ranger_resource_id = Column(String(255))  # Apache Ranger resource ID
    
    # Statistics
    size_bytes = Column(Integer)
    row_count = Column(Integer)
    column_count = Column(Integer)
    last_accessed = Column(DateTime)
    access_count = Column(Integer, default=0)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    quality_profiles = relationship("DataQualityProfile", back_populates="asset")
    access_policies = relationship("AccessPolicy", back_populates="asset")
    compliance_checks = relationship("ComplianceCheck", back_populates="asset")
    
    # Indexes
    __table_args__ = (
        Index('idx_asset_tenant_type', 'tenant_id', 'asset_type'),
        Index('idx_asset_catalog_schema', 'catalog', 'schema'),
        Index('idx_asset_classification', 'classification'),
    )


class DataLineage(Base):
    """Data lineage tracking"""
    __tablename__ = "data_lineage"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    lineage_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    
    # Lineage relationship
    source_asset_id = Column(String(255), nullable=False, index=True)
    target_asset_id = Column(String(255), nullable=False, index=True)
    
    # Transformation details
    transformation_type = Column(String(100))  # ETL, join, aggregate, etc.
    transformation_details = Column(JSON)
    pipeline_id = Column(String(255))
    job_id = Column(String(255))
    
    # Execution details
    execution_time = Column(DateTime)
    duration_seconds = Column(Integer)
    records_processed = Column(Integer)
    
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Indexes
    __table_args__ = (
        Index('idx_lineage_source_target', 'source_asset_id', 'target_asset_id'),
        Index('idx_lineage_pipeline', 'pipeline_id'),
    )


class DataQualityMetric(Base):
    """Individual quality metric measurements"""
    __tablename__ = "data_quality_metrics"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    metric_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    asset_id = Column(String(255), nullable=False, index=True)
    
    # Metric details
    metric_type = Column(String(100), nullable=False)  # completeness, accuracy, etc.
    metric_name = Column(String(255), nullable=False)
    metric_value = Column(Float, nullable=False)
    threshold = Column(Float)
    passed = Column(Boolean)
    
    # Additional context
    column_name = Column(String(255))  # For column-level metrics
    details = Column(JSON)
    
    measured_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Indexes
    __table_args__ = (
        Index('idx_metric_asset_type', 'asset_id', 'metric_type'),
        Index('idx_metric_measured', 'measured_at'),
    )


class DataQualityProfile(Base):
    """Comprehensive quality profile for data assets"""
    __tablename__ = "data_quality_profiles"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    profile_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    asset_id = Column(String(255), ForeignKey("data_assets.asset_id"), nullable=False)
    
    # Profile metadata
    profile_date = Column(DateTime, default=datetime.utcnow, index=True)
    profile_type = Column(String(50))  # full, sample, incremental
    
    # Basic statistics
    row_count = Column(Integer)
    null_count = Column(Integer)
    duplicate_count = Column(Integer)
    
    # Quality scores (0-100)
    completeness_score = Column(Float)
    validity_score = Column(Float)
    consistency_score = Column(Float)
    accuracy_score = Column(Float)
    uniqueness_score = Column(Float)
    timeliness_score = Column(Float)
    overall_score = Column(Float)
    
    # Detailed profiling
    column_profiles = Column(JSON)  # Detailed column statistics
    data_patterns = Column(JSON)  # Detected patterns
    anomalies = Column(JSON)  # Detected anomalies
    value_distributions = Column(JSON)  # Value frequency distributions
    
    # Relationships
    asset = relationship("DataAsset", back_populates="quality_profiles")


class DataPolicy(Base):
    """Data governance policies"""
    __tablename__ = "data_policies"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    policy_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    
    # Policy details
    policy_name = Column(String(255), nullable=False, index=True)
    policy_type = Column(Enum(PolicyType), nullable=False, index=True)
    description = Column(Text)
    
    # Policy configuration
    rules = Column(JSON, nullable=False)  # Policy rules definition
    applies_to = Column(JSON)  # Asset patterns/filters
    exceptions = Column(JSON)  # Exception patterns
    
    # Status
    active = Column(Boolean, default=True, index=True)
    enforcement_level = Column(String(50))  # enforce, warn, monitor
    
    # External references
    ranger_policy_id = Column(String(255))  # Apache Ranger policy ID
    atlas_policy_id = Column(String(255))  # Apache Atlas policy ID
    
    # Audit
    created_by = Column(String(255))
    approved_by = Column(String(255))
    approval_date = Column(DateTime)
    
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_policy_tenant_type', 'tenant_id', 'policy_type'),
        Index('idx_policy_active', 'active'),
    )


class AccessPolicy(Base):
    """Asset-specific access policies"""
    __tablename__ = "access_policies"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    policy_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    asset_id = Column(String(255), ForeignKey("data_assets.asset_id"), nullable=False)
    
    # Policy details
    policy_name = Column(String(255), nullable=False)
    policy_type = Column(String(50))  # read, write, admin, execute
    
    # Access control
    principals = Column(JSON)  # Users/groups/roles with access
    conditions = Column(JSON)  # Access conditions (time, location, etc.)
    permissions = Column(JSON)  # Specific permissions granted
    
    # Integration
    ranger_policy_id = Column(String(255))
    ranger_service_name = Column(String(255))
    
    # Status
    active = Column(Boolean, default=True)
    expires_at = Column(DateTime)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    asset = relationship("DataAsset", back_populates="access_policies")


class ComplianceCheck(Base):
    """Compliance check results"""
    __tablename__ = "compliance_checks"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    check_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    asset_id = Column(String(255), ForeignKey("data_assets.asset_id"), nullable=False)
    
    # Check details
    compliance_type = Column(Enum(ComplianceType), nullable=False, index=True)
    check_name = Column(String(255))
    check_date = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Results
    status = Column(Enum(ComplianceStatus), nullable=False, index=True)
    score = Column(Float)  # Compliance score 0-100
    
    # Findings
    findings = Column(JSON)  # Detailed findings
    violations = Column(JSON)  # Specific violations found
    recommendations = Column(JSON)  # Remediation recommendations
    
    # Remediation
    remediation_required = Column(Boolean, default=False)
    remediation_details = Column(Text)
    remediation_deadline = Column(DateTime)
    remediation_status = Column(String(50))  # pending, in_progress, completed
    
    # Audit trail
    checked_by = Column(String(255))  # System or user
    reviewed_by = Column(String(255))
    review_date = Column(DateTime)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    asset = relationship("DataAsset", back_populates="compliance_checks")
    
    # Indexes
    __table_args__ = (
        Index('idx_compliance_asset_type', 'asset_id', 'compliance_type'),
        Index('idx_compliance_status', 'status', 'remediation_required'),
        Index('idx_compliance_date', 'check_date'),
    )


class DataCatalogEntry(Base):
    """Business metadata catalog"""
    __tablename__ = "data_catalog_entries"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    catalog_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    asset_id = Column(String(255), nullable=False, index=True)
    
    # Business metadata
    business_name = Column(String(255), nullable=False, index=True)
    business_description = Column(Text)
    business_owner = Column(String(255))
    data_steward = Column(String(255))
    
    # Business context
    business_domain = Column(String(255))
    business_process = Column(String(255))
    business_criticality = Column(String(50))  # low, medium, high, critical
    
    # Documentation
    documentation_url = Column(String(1000))
    glossary_terms = Column(JSON)  # Business glossary terms
    
    # Discovery
    keywords = Column(JSON)  # Searchable keywords
    categories = Column(JSON)  # Hierarchical categories
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_catalog_business_name', 'business_name'),
        Index('idx_catalog_domain', 'business_domain'),
    ) 