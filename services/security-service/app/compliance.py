"""
Security Compliance Service

Manages security compliance, audit logging, and regulatory requirements.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
import hashlib
import csv
import io

import hvac
import consul.aio
from platformq_shared.authorization.opa_client import OPAClient
from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


class ComplianceFramework(Enum):
    """Compliance frameworks"""
    GDPR = "gdpr"
    HIPAA = "hipaa"
    PCI_DSS = "pci_dss"
    SOC2 = "soc2"
    ISO27001 = "iso27001"
    NIST = "nist"
    CCPA = "ccpa"


class ComplianceStatus(Enum):
    """Compliance check status"""
    COMPLIANT = "compliant"
    NON_COMPLIANT = "non_compliant"
    PARTIALLY_COMPLIANT = "partially_compliant"
    NOT_APPLICABLE = "not_applicable"
    PENDING = "pending"


@dataclass
class AuditEvent:
    """Audit event record"""
    id: str
    timestamp: datetime
    actor: Dict[str, Any]  # User/service performing action
    action: str
    resource: Dict[str, Any]
    outcome: str  # success/failure
    details: Dict[str, Any]
    compliance_frameworks: List[ComplianceFramework] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ComplianceRequirement:
    """Compliance requirement"""
    id: str
    framework: ComplianceFramework
    category: str
    description: str
    controls: List[str]
    validation_rules: Dict[str, Any]
    evidence_required: List[str]
    frequency: str  # continuous/daily/weekly/monthly
    severity: str  # critical/high/medium/low


@dataclass
class ComplianceCheck:
    """Compliance check result"""
    requirement_id: str
    framework: ComplianceFramework
    timestamp: datetime
    status: ComplianceStatus
    evidence: List[Dict[str, Any]]
    findings: List[str]
    remediation: Optional[str] = None
    next_check: Optional[datetime] = None


@dataclass
class ComplianceReport:
    """Compliance report"""
    id: str
    framework: ComplianceFramework
    period_start: datetime
    period_end: datetime
    overall_status: ComplianceStatus
    checks: List[ComplianceCheck]
    summary: Dict[str, Any]
    recommendations: List[str]
    generated_at: datetime


class ComplianceManager:
    """
    Security compliance management service.
    
    Features:
    - Audit logging
    - Compliance checking
    - Evidence collection
    - Report generation
    - Regulatory mapping
    - Automated remediation
    """
    
    def __init__(self,
                 vault_client: hvac.Client,
                 consul_client: consul.aio.Consul,
                 opa_client: OPAClient,
                 event_publisher: EventPublisher):
        self.vault = vault_client
        self.consul = consul_client
        self.opa = opa_client
        self.event_publisher = event_publisher
        
        # Storage
        self._audit_buffer: List[AuditEvent] = []
        self._requirements: Dict[str, ComplianceRequirement] = {}
        self._check_results: Dict[str, List[ComplianceCheck]] = {}
        self._reports: Dict[str, ComplianceReport] = {}
        
        # Configuration
        self._retention_policies = {
            ComplianceFramework.GDPR: timedelta(days=1095),  # 3 years
            ComplianceFramework.HIPAA: timedelta(days=2190),  # 6 years
            ComplianceFramework.PCI_DSS: timedelta(days=365),  # 1 year
            ComplianceFramework.SOC2: timedelta(days=2555),  # 7 years
            ComplianceFramework.ISO27001: timedelta(days=1095),  # 3 years
            ComplianceFramework.NIST: timedelta(days=1095),  # 3 years
            ComplianceFramework.CCPA: timedelta(days=365)  # 1 year
        }
        
        # Tasks
        self._monitoring_tasks: List[asyncio.Task] = []
        self.is_running = False
        
    async def start(self):
        """Start compliance manager"""
        logger.info("Starting compliance manager")
        
        # Load compliance requirements
        await self._load_requirements()
        
        # Start monitoring tasks
        self._monitoring_tasks = [
            asyncio.create_task(self._process_audit_buffer()),
            asyncio.create_task(self._run_compliance_checks()),
            asyncio.create_task(self._cleanup_old_data()),
            asyncio.create_task(self._generate_reports())
        ]
        
        self.is_running = True
        logger.info("Compliance manager started")
        
    async def stop(self):
        """Stop compliance manager"""
        logger.info("Stopping compliance manager")
        
        self.is_running = False
        
        # Flush audit buffer
        await self._flush_audit_buffer()
        
        # Cancel tasks
        for task in self._monitoring_tasks:
            task.cancel()
            
        logger.info("Compliance manager stopped")
        
    async def log_audit_event(self, event_data: Dict[str, Any]):
        """Log an audit event"""
        try:
            # Create audit event
            event = AuditEvent(
                id=event_data.get("id", str(uuid.uuid4())),
                timestamp=datetime.fromisoformat(event_data["timestamp"]),
                actor=event_data["actor"],
                action=event_data["action"],
                resource=event_data["resource"],
                outcome=event_data["outcome"],
                details=event_data.get("details", {}),
                compliance_frameworks=self._get_applicable_frameworks(event_data),
                metadata=event_data.get("metadata", {})
            )
            
            # Add to buffer
            self._audit_buffer.append(event)
            
            # Check if immediate flush needed
            if len(self._audit_buffer) >= 100 or self._is_critical_event(event):
                await self._flush_audit_buffer()
                
        except Exception as e:
            logger.error(f"Failed to log audit event: {e}")
            
    async def check_compliance(self,
                             framework: ComplianceFramework,
                             scope: Optional[Dict[str, Any]] = None) -> List[ComplianceCheck]:
        """Run compliance checks for a framework"""
        checks = []
        
        # Get requirements for framework
        framework_requirements = [
            req for req in self._requirements.values()
            if req.framework == framework
        ]
        
        for requirement in framework_requirements:
            # Run check
            check = await self._run_single_check(requirement, scope)
            checks.append(check)
            
            # Store result
            if requirement.id not in self._check_results:
                self._check_results[requirement.id] = []
            self._check_results[requirement.id].append(check)
            
        return checks
        
    async def get_compliance_status(self,
                                  framework: Optional[ComplianceFramework] = None) -> Dict[str, Any]:
        """Get current compliance status"""
        status = {}
        
        frameworks = [framework] if framework else list(ComplianceFramework)
        
        for fw in frameworks:
            # Get latest checks for framework
            fw_checks = []
            for req_id, checks in self._check_results.items():
                if self._requirements[req_id].framework == fw and checks:
                    fw_checks.append(checks[-1])  # Latest check
                    
            if fw_checks:
                compliant = sum(1 for c in fw_checks if c.status == ComplianceStatus.COMPLIANT)
                total = len(fw_checks)
                
                status[fw.value] = {
                    "compliant": compliant,
                    "total": total,
                    "percentage": (compliant / total) * 100 if total > 0 else 0,
                    "last_check": max(c.timestamp for c in fw_checks).isoformat()
                }
                
        return status
        
    async def generate_compliance_report(self,
                                       framework: ComplianceFramework,
                                       start_date: datetime,
                                       end_date: datetime) -> ComplianceReport:
        """Generate compliance report"""
        # Get checks within period
        period_checks = []
        for req_id, checks in self._check_results.items():
            if self._requirements[req_id].framework == framework:
                period_checks.extend([
                    c for c in checks
                    if start_date <= c.timestamp <= end_date
                ])
                
        # Calculate overall status
        if not period_checks:
            overall_status = ComplianceStatus.PENDING
        else:
            statuses = [c.status for c in period_checks]
            if all(s == ComplianceStatus.COMPLIANT for s in statuses):
                overall_status = ComplianceStatus.COMPLIANT
            elif any(s == ComplianceStatus.NON_COMPLIANT for s in statuses):
                overall_status = ComplianceStatus.NON_COMPLIANT
            else:
                overall_status = ComplianceStatus.PARTIALLY_COMPLIANT
                
        # Generate summary
        summary = {
            "total_checks": len(period_checks),
            "compliant": sum(1 for c in period_checks if c.status == ComplianceStatus.COMPLIANT),
            "non_compliant": sum(1 for c in period_checks if c.status == ComplianceStatus.NON_COMPLIANT),
            "partially_compliant": sum(1 for c in period_checks if c.status == ComplianceStatus.PARTIALLY_COMPLIANT),
            "categories": self._summarize_by_category(period_checks)
        }
        
        # Generate recommendations
        recommendations = self._generate_recommendations(period_checks)
        
        # Create report
        report = ComplianceReport(
            id=str(uuid.uuid4()),
            framework=framework,
            period_start=start_date,
            period_end=end_date,
            overall_status=overall_status,
            checks=period_checks,
            summary=summary,
            recommendations=recommendations,
            generated_at=datetime.utcnow()
        )
        
        # Store report
        self._reports[report.id] = report
        await self._store_report(report)
        
        return report
        
    async def search_audit_logs(self,
                              start_time: Optional[datetime] = None,
                              end_time: Optional[datetime] = None,
                              actor: Optional[str] = None,
                              action: Optional[str] = None,
                              resource: Optional[str] = None,
                              outcome: Optional[str] = None,
                              limit: int = 1000) -> List[AuditEvent]:
        """Search audit logs"""
        # Search in Consul
        results = []
        
        # Build search pattern
        if start_time and end_time:
            # Get all days in range
            current = start_time.date()
            end = end_time.date()
            
            while current <= end:
                # Get logs for this day
                _, day_logs = await self.consul.kv.get(
                    f"compliance/audit-logs/{current.isoformat()}",
                    recurse=True
                )
                
                if day_logs:
                    for log_kv in day_logs:
                        if log_kv["Value"]:
                            event_data = json.loads(log_kv["Value"])
                            
                            # Apply filters
                            if actor and event_data["actor"].get("id") != actor:
                                continue
                            if action and event_data["action"] != action:
                                continue
                            if resource and not self._matches_resource(event_data["resource"], resource):
                                continue
                            if outcome and event_data["outcome"] != outcome:
                                continue
                                
                            # Create event object
                            event = AuditEvent(
                                id=event_data["id"],
                                timestamp=datetime.fromisoformat(event_data["timestamp"]),
                                actor=event_data["actor"],
                                action=event_data["action"],
                                resource=event_data["resource"],
                                outcome=event_data["outcome"],
                                details=event_data.get("details", {}),
                                compliance_frameworks=[
                                    ComplianceFramework(f) for f in event_data.get("compliance_frameworks", [])
                                ],
                                metadata=event_data.get("metadata", {})
                            )
                            
                            results.append(event)
                            
                            if len(results) >= limit:
                                return results
                                
                current += timedelta(days=1)
                
        return results[:limit]
        
    async def export_audit_logs(self,
                              format: str,
                              start_time: datetime,
                              end_time: datetime,
                              filters: Optional[Dict[str, Any]] = None) -> bytes:
        """Export audit logs in specified format"""
        # Get logs
        logs = await self.search_audit_logs(
            start_time=start_time,
            end_time=end_time,
            **filters or {}
        )
        
        if format == "json":
            return json.dumps([self._audit_event_to_dict(e) for e in logs], indent=2).encode()
            
        elif format == "csv":
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Header
            writer.writerow([
                "Timestamp", "Actor", "Action", "Resource", 
                "Outcome", "Details", "Frameworks"
            ])
            
            # Data
            for event in logs:
                writer.writerow([
                    event.timestamp.isoformat(),
                    event.actor.get("id", ""),
                    event.action,
                    json.dumps(event.resource),
                    event.outcome,
                    json.dumps(event.details),
                    ",".join(f.value for f in event.compliance_frameworks)
                ])
                
            return output.getvalue().encode()
            
        else:
            raise ValueError(f"Unsupported format: {format}")
            
    async def verify_evidence(self,
                            requirement_id: str,
                            evidence: List[Dict[str, Any]]) -> bool:
        """Verify evidence for a requirement"""
        try:
            requirement = self._requirements.get(requirement_id)
            if not requirement:
                return False
                
            # Verify each piece of evidence
            for ev in evidence:
                # Check evidence type
                if ev["type"] not in requirement.evidence_required:
                    return False
                    
                # Verify integrity
                if "hash" in ev:
                    calculated_hash = hashlib.sha256(
                        ev["content"].encode()
                    ).hexdigest()
                    
                    if calculated_hash != ev["hash"]:
                        return False
                        
            return True
            
        except Exception as e:
            logger.error(f"Failed to verify evidence: {e}")
            return False
            
    async def _run_single_check(self,
                              requirement: ComplianceRequirement,
                              scope: Optional[Dict[str, Any]] = None) -> ComplianceCheck:
        """Run a single compliance check"""
        try:
            # Collect evidence
            evidence = await self._collect_evidence(requirement, scope)
            
            # Validate against rules
            validation_result = await self._validate_compliance(
                requirement, evidence, scope
            )
            
            # Determine status
            if validation_result["compliant"]:
                status = ComplianceStatus.COMPLIANT
            elif validation_result.get("partial", False):
                status = ComplianceStatus.PARTIALLY_COMPLIANT
            else:
                status = ComplianceStatus.NON_COMPLIANT
                
            # Create check result
            check = ComplianceCheck(
                requirement_id=requirement.id,
                framework=requirement.framework,
                timestamp=datetime.utcnow(),
                status=status,
                evidence=evidence,
                findings=validation_result.get("findings", []),
                remediation=validation_result.get("remediation"),
                next_check=self._calculate_next_check(requirement)
            )
            
            return check
            
        except Exception as e:
            logger.error(f"Failed to run compliance check: {e}")
            
            # Return failed check
            return ComplianceCheck(
                requirement_id=requirement.id,
                framework=requirement.framework,
                timestamp=datetime.utcnow(),
                status=ComplianceStatus.NON_COMPLIANT,
                evidence=[],
                findings=[f"Check failed: {str(e)}"],
                next_check=self._calculate_next_check(requirement)
            )
            
    async def _collect_evidence(self,
                              requirement: ComplianceRequirement,
                              scope: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Collect evidence for a requirement"""
        evidence = []
        
        for evidence_type in requirement.evidence_required:
            if evidence_type == "audit_logs":
                # Get relevant audit logs
                logs = await self.search_audit_logs(
                    start_time=datetime.utcnow() - timedelta(days=30),
                    end_time=datetime.utcnow(),
                    limit=100
                )
                
                evidence.append({
                    "type": "audit_logs",
                    "count": len(logs),
                    "sample": [self._audit_event_to_dict(l) for l in logs[:10]]
                })
                
            elif evidence_type == "configuration":
                # Get configuration evidence
                config_evidence = await self._get_configuration_evidence(requirement)
                evidence.append(config_evidence)
                
            elif evidence_type == "encryption":
                # Get encryption evidence
                encryption_evidence = await self._get_encryption_evidence(requirement)
                evidence.append(encryption_evidence)
                
            elif evidence_type == "access_control":
                # Get access control evidence
                access_evidence = await self._get_access_control_evidence(requirement)
                evidence.append(access_evidence)
                
        return evidence
        
    async def _validate_compliance(self,
                                 requirement: ComplianceRequirement,
                                 evidence: List[Dict[str, Any]],
                                 scope: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Validate compliance against rules"""
        # Use OPA for validation if available
        if "opa_policy" in requirement.validation_rules:
            input_data = {
                "requirement": requirement.id,
                "evidence": evidence,
                "scope": scope or {}
            }
            
            result = await self.opa.evaluate_policy(
                f"compliance.{requirement.validation_rules['opa_policy']}",
                input_data
            )
            
            return {
                "compliant": result.get("compliant", False),
                "findings": result.get("findings", []),
                "remediation": result.get("remediation")
            }
            
        # Default validation
        findings = []
        compliant = True
        
        # Check evidence completeness
        for required_evidence in requirement.evidence_required:
            if not any(e["type"] == required_evidence for e in evidence):
                findings.append(f"Missing required evidence: {required_evidence}")
                compliant = False
                
        return {
            "compliant": compliant,
            "findings": findings
        }
        
    async def _get_configuration_evidence(self, requirement: ComplianceRequirement) -> Dict[str, Any]:
        """Get configuration evidence"""
        # This would gather configuration data
        return {
            "type": "configuration",
            "data": {
                "encryption_enabled": True,
                "mfa_required": True,
                "audit_logging_enabled": True
            }
        }
        
    async def _get_encryption_evidence(self, requirement: ComplianceRequirement) -> Dict[str, Any]:
        """Get encryption evidence"""
        # This would verify encryption settings
        return {
            "type": "encryption",
            "data": {
                "data_at_rest_encrypted": True,
                "data_in_transit_encrypted": True,
                "key_rotation_enabled": True
            }
        }
        
    async def _get_access_control_evidence(self, requirement: ComplianceRequirement) -> Dict[str, Any]:
        """Get access control evidence"""
        # This would verify access controls
        return {
            "type": "access_control",
            "data": {
                "rbac_enabled": True,
                "least_privilege_enforced": True,
                "regular_access_reviews": True
            }
        }
        
    def _get_applicable_frameworks(self, event_data: Dict[str, Any]) -> List[ComplianceFramework]:
        """Determine applicable compliance frameworks for an event"""
        frameworks = []
        
        # Check for PII data (GDPR, CCPA)
        if self._involves_pii(event_data):
            frameworks.extend([ComplianceFramework.GDPR, ComplianceFramework.CCPA])
            
        # Check for health data (HIPAA)
        if self._involves_health_data(event_data):
            frameworks.append(ComplianceFramework.HIPAA)
            
        # Check for payment data (PCI-DSS)
        if self._involves_payment_data(event_data):
            frameworks.append(ComplianceFramework.PCI_DSS)
            
        # All events relevant for SOC2, ISO27001, NIST
        frameworks.extend([
            ComplianceFramework.SOC2,
            ComplianceFramework.ISO27001,
            ComplianceFramework.NIST
        ])
        
        return list(set(frameworks))
        
    def _involves_pii(self, event_data: Dict[str, Any]) -> bool:
        """Check if event involves PII"""
        pii_indicators = ["user", "email", "name", "address", "phone", "ssn"]
        
        # Check resource and details
        resource_str = json.dumps(event_data.get("resource", {})).lower()
        details_str = json.dumps(event_data.get("details", {})).lower()
        
        return any(indicator in resource_str or indicator in details_str 
                  for indicator in pii_indicators)
        
    def _involves_health_data(self, event_data: Dict[str, Any]) -> bool:
        """Check if event involves health data"""
        health_indicators = ["health", "medical", "patient", "diagnosis", "treatment"]
        
        resource_str = json.dumps(event_data.get("resource", {})).lower()
        details_str = json.dumps(event_data.get("details", {})).lower()
        
        return any(indicator in resource_str or indicator in details_str 
                  for indicator in health_indicators)
        
    def _involves_payment_data(self, event_data: Dict[str, Any]) -> bool:
        """Check if event involves payment data"""
        payment_indicators = ["payment", "card", "transaction", "billing", "credit"]
        
        resource_str = json.dumps(event_data.get("resource", {})).lower()
        details_str = json.dumps(event_data.get("details", {})).lower()
        
        return any(indicator in resource_str or indicator in details_str 
                  for indicator in payment_indicators)
        
    def _is_critical_event(self, event: AuditEvent) -> bool:
        """Check if event is critical and needs immediate logging"""
        critical_actions = [
            "delete", "modify_permissions", "access_denied",
            "authentication_failed", "privilege_escalation"
        ]
        
        return any(action in event.action.lower() for action in critical_actions)
        
    def _matches_resource(self, resource: Dict[str, Any], pattern: str) -> bool:
        """Check if resource matches pattern"""
        resource_str = json.dumps(resource).lower()
        return pattern.lower() in resource_str
        
    def _audit_event_to_dict(self, event: AuditEvent) -> Dict[str, Any]:
        """Convert audit event to dictionary"""
        return {
            "id": event.id,
            "timestamp": event.timestamp.isoformat(),
            "actor": event.actor,
            "action": event.action,
            "resource": event.resource,
            "outcome": event.outcome,
            "details": event.details,
            "compliance_frameworks": [f.value for f in event.compliance_frameworks],
            "metadata": event.metadata
        }
        
    def _calculate_next_check(self, requirement: ComplianceRequirement) -> datetime:
        """Calculate next check time based on frequency"""
        now = datetime.utcnow()
        
        if requirement.frequency == "continuous":
            return now + timedelta(minutes=5)
        elif requirement.frequency == "daily":
            return now + timedelta(days=1)
        elif requirement.frequency == "weekly":
            return now + timedelta(weeks=1)
        elif requirement.frequency == "monthly":
            return now + timedelta(days=30)
        else:
            return now + timedelta(days=1)
            
    def _summarize_by_category(self, checks: List[ComplianceCheck]) -> Dict[str, Dict[str, int]]:
        """Summarize checks by category"""
        summary = {}
        
        for check in checks:
            req = self._requirements.get(check.requirement_id)
            if req:
                if req.category not in summary:
                    summary[req.category] = {
                        "total": 0,
                        "compliant": 0,
                        "non_compliant": 0,
                        "partially_compliant": 0
                    }
                    
                summary[req.category]["total"] += 1
                
                if check.status == ComplianceStatus.COMPLIANT:
                    summary[req.category]["compliant"] += 1
                elif check.status == ComplianceStatus.NON_COMPLIANT:
                    summary[req.category]["non_compliant"] += 1
                else:
                    summary[req.category]["partially_compliant"] += 1
                    
        return summary
        
    def _generate_recommendations(self, checks: List[ComplianceCheck]) -> List[str]:
        """Generate recommendations based on checks"""
        recommendations = []
        
        # Group non-compliant checks by category
        non_compliant_by_category = {}
        for check in checks:
            if check.status != ComplianceStatus.COMPLIANT:
                req = self._requirements.get(check.requirement_id)
                if req:
                    if req.category not in non_compliant_by_category:
                        non_compliant_by_category[req.category] = []
                    non_compliant_by_category[req.category].append(check)
                    
        # Generate recommendations
        for category, category_checks in non_compliant_by_category.items():
            if len(category_checks) > 3:
                recommendations.append(
                    f"Focus on improving {category} controls - "
                    f"{len(category_checks)} non-compliant checks found"
                )
                
            # Add specific remediation
            for check in category_checks[:3]:  # Top 3
                if check.remediation:
                    recommendations.append(check.remediation)
                    
        return recommendations
        
    async def _flush_audit_buffer(self):
        """Flush audit events to storage"""
        if not self._audit_buffer:
            return
            
        try:
            # Group by day
            events_by_day = {}
            for event in self._audit_buffer:
                day = event.timestamp.date().isoformat()
                if day not in events_by_day:
                    events_by_day[day] = []
                events_by_day[day].append(event)
                
            # Store each day's events
            for day, events in events_by_day.items():
                for event in events:
                    await self.consul.kv.put(
                        f"compliance/audit-logs/{day}/{event.id}",
                        json.dumps(self._audit_event_to_dict(event))
                    )
                    
            # Clear buffer
            self._audit_buffer.clear()
            
            logger.info(f"Flushed {len(events)} audit events to storage")
            
        except Exception as e:
            logger.error(f"Failed to flush audit buffer: {e}")
            
    async def _store_report(self, report: ComplianceReport):
        """Store compliance report"""
        try:
            report_data = {
                "id": report.id,
                "framework": report.framework.value,
                "period_start": report.period_start.isoformat(),
                "period_end": report.period_end.isoformat(),
                "overall_status": report.overall_status.value,
                "summary": report.summary,
                "recommendations": report.recommendations,
                "generated_at": report.generated_at.isoformat()
            }
            
            await self.consul.kv.put(
                f"compliance/reports/{report.framework.value}/{report.id}",
                json.dumps(report_data)
            )
            
        except Exception as e:
            logger.error(f"Failed to store report: {e}")
            
    async def _load_requirements(self):
        """Load compliance requirements"""
        # Load default requirements
        default_requirements = self._get_default_requirements()
        
        for req_data in default_requirements:
            requirement = ComplianceRequirement(
                id=req_data["id"],
                framework=ComplianceFramework(req_data["framework"]),
                category=req_data["category"],
                description=req_data["description"],
                controls=req_data["controls"],
                validation_rules=req_data["validation_rules"],
                evidence_required=req_data["evidence_required"],
                frequency=req_data["frequency"],
                severity=req_data["severity"]
            )
            
            self._requirements[requirement.id] = requirement
            
        logger.info(f"Loaded {len(self._requirements)} compliance requirements")
        
    def _get_default_requirements(self) -> List[Dict[str, Any]]:
        """Get default compliance requirements"""
        return [
            # GDPR Requirements
            {
                "id": "gdpr-encryption",
                "framework": "gdpr",
                "category": "Data Protection",
                "description": "Personal data must be encrypted at rest and in transit",
                "controls": ["encryption", "key_management"],
                "validation_rules": {"opa_policy": "gdpr_encryption"},
                "evidence_required": ["encryption", "configuration"],
                "frequency": "daily",
                "severity": "critical"
            },
            {
                "id": "gdpr-access-logs",
                "framework": "gdpr",
                "category": "Accountability",
                "description": "All access to personal data must be logged",
                "controls": ["audit_logging", "monitoring"],
                "validation_rules": {"opa_policy": "gdpr_access_logs"},
                "evidence_required": ["audit_logs"],
                "frequency": "continuous",
                "severity": "high"
            },
            # PCI-DSS Requirements
            {
                "id": "pci-access-control",
                "framework": "pci_dss",
                "category": "Access Control",
                "description": "Restrict access to cardholder data by business need-to-know",
                "controls": ["rbac", "least_privilege"],
                "validation_rules": {"opa_policy": "pci_access_control"},
                "evidence_required": ["access_control", "audit_logs"],
                "frequency": "weekly",
                "severity": "critical"
            },
            # SOC2 Requirements
            {
                "id": "soc2-availability",
                "framework": "soc2",
                "category": "Availability",
                "description": "System must maintain agreed availability levels",
                "controls": ["monitoring", "redundancy"],
                "validation_rules": {"opa_policy": "soc2_availability"},
                "evidence_required": ["configuration", "audit_logs"],
                "frequency": "daily",
                "severity": "high"
            }
        ]
        
    async def _process_audit_buffer(self):
        """Process audit buffer periodically"""
        while self.is_running:
            try:
                # Flush buffer every 30 seconds or when full
                if len(self._audit_buffer) > 0:
                    await self._flush_audit_buffer()
                    
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Audit buffer processing error: {e}")
                await asyncio.sleep(60)
                
    async def _run_compliance_checks(self):
        """Run scheduled compliance checks"""
        while self.is_running:
            try:
                # Find checks due to run
                now = datetime.utcnow()
                
                for requirement in self._requirements.values():
                    # Check if due
                    last_check = None
                    if requirement.id in self._check_results and self._check_results[requirement.id]:
                        last_check = self._check_results[requirement.id][-1]
                        
                    if not last_check or last_check.next_check <= now:
                        # Run check
                        check = await self._run_single_check(requirement)
                        
                        # Store result
                        if requirement.id not in self._check_results:
                            self._check_results[requirement.id] = []
                        self._check_results[requirement.id].append(check)
                        
                        # Alert on non-compliance
                        if check.status == ComplianceStatus.NON_COMPLIANT:
                            await self.event_publisher.publish_event(
                                "platformq.compliance.check-failed",
                                {
                                    "requirement_id": requirement.id,
                                    "framework": requirement.framework.value,
                                    "findings": check.findings
                                }
                            )
                            
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logger.error(f"Compliance check error: {e}")
                await asyncio.sleep(60)
                
    async def _cleanup_old_data(self):
        """Clean up old data based on retention policies"""
        while self.is_running:
            try:
                # Clean up old audit logs
                for framework, retention in self._retention_policies.items():
                    cutoff = datetime.utcnow() - retention
                    
                    # This would delete old logs from storage
                    # For now, just log the action
                    logger.info(
                        f"Would clean up {framework.value} audit logs "
                        f"older than {cutoff.isoformat()}"
                    )
                    
                await asyncio.sleep(86400)  # Daily
                
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
                await asyncio.sleep(3600)
                
    async def _generate_reports(self):
        """Generate scheduled compliance reports"""
        while self.is_running:
            try:
                # Check if monthly reports are due
                now = datetime.utcnow()
                if now.day == 1:  # First day of month
                    # Generate reports for previous month
                    start_date = (now - timedelta(days=30)).replace(day=1)
                    end_date = now.replace(day=1) - timedelta(days=1)
                    
                    for framework in ComplianceFramework:
                        report = await self.generate_compliance_report(
                            framework, start_date, end_date
                        )
                        
                        # Notify
                        await self.event_publisher.publish_event(
                            "platformq.compliance.report-generated",
                            {
                                "report_id": report.id,
                                "framework": framework.value,
                                "status": report.overall_status.value
                            }
                        )
                        
                await asyncio.sleep(86400)  # Daily check
                
            except Exception as e:
                logger.error(f"Report generation error: {e}")
                await asyncio.sleep(3600) 