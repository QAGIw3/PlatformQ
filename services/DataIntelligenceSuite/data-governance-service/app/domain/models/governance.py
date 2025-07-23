"""
Governance domain models extending data-intelligence-common
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from enum import Enum

# Import from common library
from data_intelligence_common.models.base_models import BaseModel
from data_intelligence_common.models.catalog_models import (
    CatalogEntity,
    DataClassification,
    DataPolicy,
    AccessControl
)
from data_intelligence_common.core.governance.policy_engine import (
    PolicyRule,
    PolicyDecision,
    PolicyEvaluationContext
)


class PolicyType(str, Enum):
    """Policy types"""
    ACCESS_CONTROL = "access_control"
    DATA_RETENTION = "data_retention"
    DATA_QUALITY = "data_quality"
    DATA_PRIVACY = "data_privacy"
    DATA_USAGE = "data_usage"
    COMPLIANCE = "compliance"
    COST_CONTROL = "cost_control"
    CUSTOM = "custom"


class ComplianceFramework(str, Enum):
    """Compliance frameworks"""
    GDPR = "gdpr"
    CCPA = "ccpa"
    HIPAA = "hipaa"
    SOC2 = "soc2"
    PCI_DSS = "pci_dss"
    ISO_27001 = "iso_27001"
    CUSTOM = "custom"


class DataSensitivity(str, Enum):
    """Data sensitivity levels"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    TOP_SECRET = "top_secret"


@dataclass
class GovernancePolicy(DataPolicy):
    """Extended governance policy"""
    policy_type: PolicyType = PolicyType.ACCESS_CONTROL
    
    # Compliance
    compliance_frameworks: List[ComplianceFramework] = field(default_factory=list)
    regulatory_requirements: List[str] = field(default_factory=list)
    
    # Automation
    auto_enforce: bool = True
    enforcement_actions: List[Dict[str, Any]] = field(default_factory=list)
    
    # Approval workflow
    requires_approval: bool = False
    approval_levels: List[str] = field(default_factory=list)
    approved_by: Optional[str] = None
    approval_date: Optional[datetime] = None
    
    # Monitoring
    monitor_compliance: bool = True
    alert_on_violation: bool = True
    violation_threshold: Optional[int] = None
    
    # Exceptions
    exception_rules: List[Dict[str, Any]] = field(default_factory=list)
    temporary_exceptions: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class DataAsset(CatalogEntity):
    """Extended data asset with governance"""
    # Classification
    data_classification: DataClassification
    sensitivity_level: DataSensitivity = DataSensitivity.INTERNAL
    
    # Ownership
    data_owner: str
    data_steward: Optional[str] = None
    technical_owner: Optional[str] = None
    
    # Compliance
    compliance_tags: List[str] = field(default_factory=list)
    retention_period_days: Optional[int] = None
    deletion_date: Optional[datetime] = None
    
    # Privacy
    contains_pii: bool = False
    pii_fields: List[str] = field(default_factory=list)
    anonymization_applied: bool = False
    
    # Quality
    quality_score: Optional[float] = None
    last_quality_check: Optional[datetime] = None
    quality_sla: Optional[float] = None
    
    # Usage
    allowed_uses: List[str] = field(default_factory=list)
    prohibited_uses: List[str] = field(default_factory=list)
    
    # Lineage
    source_systems: List[str] = field(default_factory=list)
    transformation_history: List[Dict[str, Any]] = field(default_factory=list)
    
    # Access
    access_groups: List[str] = field(default_factory=list)
    require_approval_for_access: bool = False
    
    # Audit
    last_accessed: Optional[datetime] = None
    access_frequency: Optional[int] = None
    last_modified_by: Optional[str] = None


@dataclass
class AccessRequest(BaseModel):
    """Data access request"""
    request_id: str
    requester_id: str
    asset_id: str
    
    # Request details
    purpose: str
    duration_days: Optional[int] = None
    access_type: str = "read"  # read, write, delete
    
    # Justification
    business_justification: str
    project_id: Optional[str] = None
    
    # Status
    status: str = "pending"  # pending, approved, denied, expired
    
    # Approval
    approver_id: Optional[str] = None
    approval_date: Optional[datetime] = None
    denial_reason: Optional[str] = None
    
    # Conditions
    conditions: List[str] = field(default_factory=list)
    restrictions: List[str] = field(default_factory=list)
    
    # Audit
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    revoked_at: Optional[datetime] = None


@dataclass
class ComplianceReport(BaseModel):
    """Compliance report"""
    report_id: str
    framework: ComplianceFramework
    
    # Report details
    report_date: datetime = field(default_factory=datetime.utcnow)
    reporting_period_start: datetime
    reporting_period_end: datetime
    
    # Compliance status
    overall_compliance_score: float
    compliant_controls: int
    non_compliant_controls: int
    total_controls: int
    
    # Findings
    findings: List[Dict[str, Any]] = field(default_factory=list)
    critical_issues: List[Dict[str, Any]] = field(default_factory=list)
    
    # Recommendations
    recommendations: List[Dict[str, Any]] = field(default_factory=list)
    remediation_plan: Optional[Dict[str, Any]] = None
    
    # Attestation
    attested_by: Optional[str] = None
    attestation_date: Optional[datetime] = None
    
    # Evidence
    evidence_links: List[str] = field(default_factory=list)
    audit_logs: List[str] = field(default_factory=list)


@dataclass
class DataPrivacyRequest(BaseModel):
    """Data privacy request (GDPR, CCPA, etc.)"""
    request_id: str
    request_type: str  # access, deletion, portability, rectification
    
    # Subject
    subject_id: str
    subject_email: Optional[str] = None
    
    # Request details
    submitted_at: datetime = field(default_factory=datetime.utcnow)
    deadline: datetime
    
    # Processing
    status: str = "received"  # received, verified, processing, completed, rejected
    assigned_to: Optional[str] = None
    
    # Verification
    identity_verified: bool = False
    verification_method: Optional[str] = None
    verified_by: Optional[str] = None
    
    # Results
    data_found: bool = False
    systems_searched: List[str] = field(default_factory=list)
    data_locations: List[str] = field(default_factory=list)
    
    # Actions taken
    actions_taken: List[Dict[str, Any]] = field(default_factory=list)
    completion_date: Optional[datetime] = None
    
    # Communication
    response_sent: bool = False
    response_date: Optional[datetime] = None


@dataclass
class DataContract(BaseModel):
    """Data contract between producer and consumer"""
    contract_id: str
    name: str
    version: str
    
    # Parties
    producer: str
    consumer: str
    
    # Contract terms
    description: str
    sla_terms: Dict[str, Any] = field(default_factory=dict)
    
    # Schema
    schema_definition: Dict[str, Any] = field(default_factory=dict)
    schema_version: str
    backward_compatible: bool = True
    
    # Quality guarantees
    quality_guarantees: Dict[str, float] = field(default_factory=dict)
    
    # Delivery
    delivery_frequency: str
    delivery_method: str
    
    # Validity
    effective_date: datetime
    expiration_date: Optional[datetime] = None
    
    # Status
    status: str = "draft"  # draft, active, suspended, terminated
    
    # Signatures
    producer_signed: bool = False
    consumer_signed: bool = False
    signed_date: Optional[datetime] = None
    
    # Monitoring
    monitor_compliance: bool = True
    breach_notifications: List[str] = field(default_factory=list)


@dataclass
class GovernanceMetrics(BaseModel):
    """Governance metrics dashboard"""
    # Compliance
    compliance_score: float
    policies_enforced: int
    policy_violations: int
    
    # Privacy
    privacy_requests_pending: int
    privacy_requests_completed: int
    avg_privacy_request_time_hours: float
    
    # Access
    active_access_requests: int
    access_approvals: int
    access_denials: int
    unauthorized_access_attempts: int
    
    # Quality
    assets_meeting_quality_sla: int
    assets_below_quality_threshold: int
    avg_data_quality_score: float
    
    # Classification
    classified_assets: int
    unclassified_assets: int
    high_sensitivity_assets: int
    
    # Contracts
    active_contracts: int
    contracts_in_breach: int
    
    # Trends
    compliance_trend: str  # improving, stable, declining
    quality_trend: str
    
    # Last updated
    last_updated: datetime = field(default_factory=datetime.utcnow) 