"""
Model mixins for common patterns across data intelligence models.

Provides reusable mixins for metadata, ownership, lifecycle, and other common patterns.
"""

from typing import Any, Dict, List, Optional, Set
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum


@dataclass 
class MetadataMixin:
    """Mixin for models with metadata."""
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    
    def add_tag(self, tag: str):
        """Add a tag if not already present."""
        if tag not in self.tags:
            self.tags.append(tag)
            
    def remove_tag(self, tag: str):
        """Remove a tag."""
        if tag in self.tags:
            self.tags.remove(tag)
            
    def has_tag(self, tag: str) -> bool:
        """Check if tag exists."""
        return tag in self.tags
        
    def set_metadata(self, key: str, value: Any):
        """Set metadata value."""
        self.metadata[key] = value
        
    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get metadata value."""
        return self.metadata.get(key, default)


@dataclass
class OwnershipMixin:
    """Mixin for models with ownership information."""
    owner: Optional[str] = None
    team: Optional[str] = None
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    
    def is_owned_by(self, user: str) -> bool:
        """Check if owned by user."""
        return self.owner == user or self.created_by == user


@dataclass
class LifecycleMixin:
    """Mixin for models with lifecycle management."""
    is_active: bool = True
    is_archived: bool = False
    is_deleted: bool = False
    archived_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None
    
    def archive(self):
        """Archive the model."""
        self.is_archived = True
        self.archived_at = datetime.utcnow()
        self.is_active = False
        
    def unarchive(self):
        """Unarchive the model."""
        self.is_archived = False
        self.archived_at = None
        self.is_active = True
        
    def soft_delete(self):
        """Soft delete the model."""
        self.is_deleted = True
        self.deleted_at = datetime.utcnow()
        self.is_active = False
        
    def restore(self):
        """Restore soft deleted model."""
        self.is_deleted = False
        self.deleted_at = None
        self.is_active = True


@dataclass
class QualityMixin:
    """Mixin for models with quality metrics."""
    quality_score: Optional[float] = None
    quality_checks: List[Dict[str, Any]] = field(default_factory=list)
    validation_errors: List[Dict[str, Any]] = field(default_factory=list)
    
    def add_quality_check(self, check_name: str, passed: bool, details: Optional[Dict[str, Any]] = None):
        """Add a quality check result."""
        self.quality_checks.append({
            "name": check_name,
            "passed": passed,
            "timestamp": datetime.utcnow(),
            "details": details or {}
        })
        
    def add_validation_error(self, field: str, error: str, severity: str = "error"):
        """Add a validation error."""
        self.validation_errors.append({
            "field": field,
            "error": error,
            "severity": severity,
            "timestamp": datetime.utcnow()
        })
        
    def calculate_quality_score(self) -> float:
        """Calculate overall quality score based on checks."""
        if not self.quality_checks:
            return 0.0
            
        passed = sum(1 for check in self.quality_checks if check["passed"])
        total = len(self.quality_checks)
        
        self.quality_score = passed / total if total > 0 else 0.0
        return self.quality_score


@dataclass
class LineageMixin:
    """Mixin for models with lineage tracking."""
    upstream_ids: List[str] = field(default_factory=list)
    downstream_ids: List[str] = field(default_factory=list)
    lineage_metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_upstream(self, entity_id: str, relationship_type: str = "derived_from"):
        """Add upstream lineage."""
        if entity_id not in self.upstream_ids:
            self.upstream_ids.append(entity_id)
            self.lineage_metadata[f"upstream_{entity_id}"] = {
                "type": relationship_type,
                "added_at": datetime.utcnow()
            }
            
    def add_downstream(self, entity_id: str, relationship_type: str = "feeds_into"):
        """Add downstream lineage."""
        if entity_id not in self.downstream_ids:
            self.downstream_ids.append(entity_id)
            self.lineage_metadata[f"downstream_{entity_id}"] = {
                "type": relationship_type,
                "added_at": datetime.utcnow()
            }
            
    def get_lineage_graph(self) -> Dict[str, List[str]]:
        """Get lineage as a graph structure."""
        return {
            "upstream": self.upstream_ids,
            "downstream": self.downstream_ids
        }


@dataclass
class SchemaEvolutionMixin:
    """Mixin for models with schema evolution tracking."""
    schema_version: str = "1.0.0"
    schema_history: List[Dict[str, Any]] = field(default_factory=list)
    backward_compatible: bool = True
    
    def evolve_schema(self, new_version: str, changes: List[Dict[str, Any]], backward_compatible: bool = True):
        """Record schema evolution."""
        self.schema_history.append({
            "from_version": self.schema_version,
            "to_version": new_version,
            "changes": changes,
            "backward_compatible": backward_compatible,
            "evolved_at": datetime.utcnow()
        })
        self.schema_version = new_version
        self.backward_compatible = backward_compatible


@dataclass
class AccessControlMixin:
    """Mixin for models with access control."""
    access_level: str = "private"  # private, team, public
    allowed_users: Set[str] = field(default_factory=set)
    allowed_teams: Set[str] = field(default_factory=set)
    permissions: Dict[str, List[str]] = field(default_factory=dict)  # user/team -> [read, write, delete]
    
    def grant_access(self, principal: str, permissions: List[str], principal_type: str = "user"):
        """Grant access to a user or team."""
        if principal_type == "user":
            self.allowed_users.add(principal)
        else:
            self.allowed_teams.add(principal)
            
        self.permissions[principal] = permissions
        
    def revoke_access(self, principal: str):
        """Revoke access from a user or team."""
        self.allowed_users.discard(principal)
        self.allowed_teams.discard(principal)
        self.permissions.pop(principal, None)
        
    def has_permission(self, principal: str, permission: str) -> bool:
        """Check if principal has specific permission."""
        if self.access_level == "public" and permission == "read":
            return True
            
        perms = self.permissions.get(principal, [])
        return permission in perms


@dataclass
class MonitoringMixin:
    """Mixin for models with monitoring capabilities."""
    monitoring_enabled: bool = True
    metrics: Dict[str, float] = field(default_factory=dict)
    alerts: List[Dict[str, Any]] = field(default_factory=list)
    last_monitored: Optional[datetime] = None
    
    def record_metric(self, name: str, value: float):
        """Record a monitoring metric."""
        self.metrics[name] = value
        self.last_monitored = datetime.utcnow()
        
    def add_alert(self, alert_type: str, message: str, severity: str = "warning"):
        """Add a monitoring alert."""
        self.alerts.append({
            "type": alert_type,
            "message": message,
            "severity": severity,
            "timestamp": datetime.utcnow(),
            "resolved": False
        })
        
    def resolve_alerts(self, alert_type: Optional[str] = None):
        """Resolve alerts of a specific type or all alerts."""
        for alert in self.alerts:
            if alert_type is None or alert["type"] == alert_type:
                alert["resolved"] = True
                alert["resolved_at"] = datetime.utcnow() 