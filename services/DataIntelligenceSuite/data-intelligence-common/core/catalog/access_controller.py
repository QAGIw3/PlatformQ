"""
Access control for catalog entities.

Provides fine-grained access control with RBAC, ABAC, and policy management.
"""

import uuid
from typing import Any, Dict, List, Optional, Set, Union, Callable
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
import re
from collections import defaultdict

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class Permission(str, Enum):
    """Catalog permissions"""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    SHARE = "share"
    ADMIN = "admin"
    EXECUTE = "execute"
    APPROVE = "approve"
    EXPORT = "export"


class PrincipalType(str, Enum):
    """Types of security principals"""
    USER = "user"
    GROUP = "group"
    ROLE = "role"
    SERVICE_ACCOUNT = "service_account"
    APPLICATION = "application"


class PolicyEffect(str, Enum):
    """Policy effects"""
    ALLOW = "allow"
    DENY = "deny"


class PolicyConditionOperator(str, Enum):
    """Policy condition operators"""
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    MATCHES = "matches"  # regex
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    EXISTS = "exists"


@dataclass
class Principal:
    """Security principal"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    principal_type: PrincipalType = PrincipalType.USER
    
    # Attributes for ABAC
    attributes: Dict[str, Any] = field(default_factory=dict)
    
    # Group memberships
    groups: List[str] = field(default_factory=list)
    
    # Assigned roles
    roles: List[str] = field(default_factory=list)
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    is_active: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "principal_type": self.principal_type.value,
            "attributes": self.attributes,
            "groups": self.groups,
            "roles": self.roles,
            "created_at": self.created_at.isoformat(),
            "is_active": self.is_active
        }


@dataclass
class Role:
    """Security role"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: Optional[str] = None
    
    # Permissions
    permissions: List[Permission] = field(default_factory=list)
    
    # Resource patterns this role applies to
    resource_patterns: List[str] = field(default_factory=list)
    
    # Parent roles (for inheritance)
    parent_roles: List[str] = field(default_factory=list)
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    is_system: bool = False  # System roles cannot be modified
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "permissions": [p.value for p in self.permissions],
            "resource_patterns": self.resource_patterns,
            "parent_roles": self.parent_roles,
            "created_at": self.created_at.isoformat(),
            "is_system": self.is_system
        }


@dataclass
class PolicyCondition:
    """Policy condition"""
    attribute: str  # e.g., "principal.department", "resource.classification"
    operator: PolicyConditionOperator
    value: Any
    
    def evaluate(self, context: Dict[str, Any]) -> bool:
        """Evaluate condition against context"""
        # Extract attribute value from context
        attr_value = self._get_attribute_value(context, self.attribute)
        
        if self.operator == PolicyConditionOperator.EXISTS:
            return attr_value is not None
        elif self.operator == PolicyConditionOperator.EQUALS:
            return attr_value == self.value
        elif self.operator == PolicyConditionOperator.NOT_EQUALS:
            return attr_value != self.value
        elif self.operator == PolicyConditionOperator.IN:
            return attr_value in self.value
        elif self.operator == PolicyConditionOperator.NOT_IN:
            return attr_value not in self.value
        elif self.operator == PolicyConditionOperator.CONTAINS:
            return self.value in str(attr_value)
        elif self.operator == PolicyConditionOperator.STARTS_WITH:
            return str(attr_value).startswith(self.value)
        elif self.operator == PolicyConditionOperator.ENDS_WITH:
            return str(attr_value).endswith(self.value)
        elif self.operator == PolicyConditionOperator.MATCHES:
            return bool(re.match(self.value, str(attr_value)))
        elif self.operator == PolicyConditionOperator.GREATER_THAN:
            return attr_value > self.value
        elif self.operator == PolicyConditionOperator.LESS_THAN:
            return attr_value < self.value
        else:
            return False
            
    def _get_attribute_value(self, context: Dict[str, Any], attribute: str) -> Any:
        """Extract attribute value from context"""
        parts = attribute.split(".")
        value = context
        
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
                
        return value
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "attribute": self.attribute,
            "operator": self.operator.value,
            "value": self.value
        }


@dataclass
class AccessPolicy:
    """Access control policy"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: Optional[str] = None
    
    # Policy effect
    effect: PolicyEffect = PolicyEffect.ALLOW
    
    # Principals this policy applies to
    principals: List[str] = field(default_factory=list)  # IDs or patterns
    
    # Resources this policy applies to
    resources: List[str] = field(default_factory=list)  # IDs or patterns
    
    # Actions/permissions
    actions: List[Permission] = field(default_factory=list)
    
    # Conditions (all must be true)
    conditions: List[PolicyCondition] = field(default_factory=list)
    
    # Validity period
    valid_from: Optional[datetime] = None
    valid_until: Optional[datetime] = None
    
    # Metadata
    created_by: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    priority: int = 0  # Higher priority policies are evaluated first
    is_active: bool = True
    
    def is_valid(self) -> bool:
        """Check if policy is currently valid"""
        if not self.is_active:
            return False
            
        now = datetime.utcnow()
        if self.valid_from and now < self.valid_from:
            return False
        if self.valid_until and now > self.valid_until:
            return False
            
        return True
        
    def matches_principal(self, principal_id: str) -> bool:
        """Check if policy applies to principal"""
        for pattern in self.principals:
            if pattern == "*" or pattern == principal_id:
                return True
            if pattern.endswith("*") and principal_id.startswith(pattern[:-1]):
                return True
        return False
        
    def matches_resource(self, resource_id: str) -> bool:
        """Check if policy applies to resource"""
        for pattern in self.resources:
            if pattern == "*" or pattern == resource_id:
                return True
            if pattern.endswith("*") and resource_id.startswith(pattern[:-1]):
                return True
            if "*" in pattern:
                # Convert pattern to regex
                regex_pattern = pattern.replace("*", ".*")
                if re.match(f"^{regex_pattern}$", resource_id):
                    return True
        return False
        
    def matches_action(self, action: Permission) -> bool:
        """Check if policy applies to action"""
        return action in self.actions or Permission.ADMIN in self.actions
        
    def evaluate_conditions(self, context: Dict[str, Any]) -> bool:
        """Evaluate all conditions"""
        if not self.conditions:
            return True
            
        return all(condition.evaluate(context) for condition in self.conditions)
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "effect": self.effect.value,
            "principals": self.principals,
            "resources": self.resources,
            "actions": [a.value for a in self.actions],
            "conditions": [c.to_dict() for c in self.conditions],
            "valid_from": self.valid_from.isoformat() if self.valid_from else None,
            "valid_until": self.valid_until.isoformat() if self.valid_until else None,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "priority": self.priority,
            "is_active": self.is_active
        }


@dataclass
class AccessRequest:
    """Access request for audit"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    principal_id: str = ""
    resource_id: str = ""
    action: Permission = Permission.READ
    
    # Context for ABAC
    context: Dict[str, Any] = field(default_factory=dict)
    
    # Result
    granted: bool = False
    denial_reason: Optional[str] = None
    evaluated_policies: List[str] = field(default_factory=list)
    
    # Metadata
    requested_at: datetime = field(default_factory=datetime.utcnow)
    response_time_ms: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "principal_id": self.principal_id,
            "resource_id": self.resource_id,
            "action": self.action.value,
            "context": self.context,
            "granted": self.granted,
            "denial_reason": self.denial_reason,
            "evaluated_policies": self.evaluated_policies,
            "requested_at": self.requested_at.isoformat(),
            "response_time_ms": self.response_time_ms
        }


class AccessController:
    """
    Manages access control for catalog entities.
    
    Features:
    - Role-Based Access Control (RBAC)
    - Attribute-Based Access Control (ABAC)
    - Policy management
    - Access auditing
    - Permission inheritance
    """
    
    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # Storage
        self._principals: Dict[str, Principal] = {}
        self._roles: Dict[str, Role] = {}
        self._policies: Dict[str, AccessPolicy] = {}
        self._access_log: List[AccessRequest] = []
        
        # Indexes
        self._principal_roles: Dict[str, Set[str]] = defaultdict(set)
        self._role_principals: Dict[str, Set[str]] = defaultdict(set)
        self._resource_policies: Dict[str, Set[str]] = defaultdict(set)
        
        # Initialize default roles
        self._initialize_default_roles()
        
    def _initialize_default_roles(self):
        """Initialize default system roles"""
        default_roles = [
            Role(
                name="catalog_admin",
                description="Full catalog administration",
                permissions=[Permission.ADMIN],
                resource_patterns=["*"],
                is_system=True
            ),
            Role(
                name="catalog_editor",
                description="Create and modify catalog entries",
                permissions=[Permission.READ, Permission.WRITE, Permission.DELETE],
                resource_patterns=["*"],
                is_system=True
            ),
            Role(
                name="catalog_viewer",
                description="View catalog entries",
                permissions=[Permission.READ],
                resource_patterns=["*"],
                is_system=True
            ),
            Role(
                name="data_steward",
                description="Data governance and quality management",
                permissions=[Permission.READ, Permission.WRITE, Permission.APPROVE],
                resource_patterns=["*"],
                is_system=True
            )
        ]
        
        for role in default_roles:
            self._roles[role.id] = role
            
    def create_principal(
        self,
        name: str,
        principal_type: PrincipalType = PrincipalType.USER,
        attributes: Optional[Dict[str, Any]] = None
    ) -> Principal:
        """Create security principal"""
        principal = Principal(
            name=name,
            principal_type=principal_type,
            attributes=attributes or {}
        )
        
        self._principals[principal.id] = principal
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="access.principal.created",
                source="access_controller",
                data={
                    "principal_id": principal.id,
                    "principal_type": principal_type.value
                }
            ))
            
        logger.info(f"Created principal: {name} ({principal_type.value})")
        return principal
        
    def create_role(
        self,
        name: str,
        permissions: List[Permission],
        resource_patterns: Optional[List[str]] = None,
        description: Optional[str] = None
    ) -> Role:
        """Create security role"""
        role = Role(
            name=name,
            description=description,
            permissions=permissions,
            resource_patterns=resource_patterns or ["*"]
        )
        
        self._roles[role.id] = role
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="access.role.created",
                source="access_controller",
                data={
                    "role_id": role.id,
                    "role_name": name,
                    "permissions": [p.value for p in permissions]
                }
            ))
            
        logger.info(f"Created role: {name}")
        return role
        
    def assign_role(
        self,
        principal_id: str,
        role_id: str
    ):
        """Assign role to principal"""
        principal = self._principals.get(principal_id)
        role = self._roles.get(role_id)
        
        if not principal:
            raise ValueError(f"Principal not found: {principal_id}")
        if not role:
            raise ValueError(f"Role not found: {role_id}")
            
        if role_id not in principal.roles:
            principal.roles.append(role_id)
            self._principal_roles[principal_id].add(role_id)
            self._role_principals[role_id].add(principal_id)
            
            # Clear cache
            if self.cache:
                self._clear_principal_cache(principal_id)
                
            # Publish event
            if self.event_bus:
                self.event_bus.publish(Event(
                    type="access.role.assigned",
                    source="access_controller",
                    data={
                        "principal_id": principal_id,
                        "role_id": role_id
                    }
                ))
                
            logger.info(f"Assigned role {role.name} to principal {principal.name}")
            
    def revoke_role(
        self,
        principal_id: str,
        role_id: str
    ):
        """Revoke role from principal"""
        principal = self._principals.get(principal_id)
        
        if not principal:
            raise ValueError(f"Principal not found: {principal_id}")
            
        if role_id in principal.roles:
            principal.roles.remove(role_id)
            self._principal_roles[principal_id].discard(role_id)
            self._role_principals[role_id].discard(principal_id)
            
            # Clear cache
            if self.cache:
                self._clear_principal_cache(principal_id)
                
            # Publish event
            if self.event_bus:
                self.event_bus.publish(Event(
                    type="access.role.revoked",
                    source="access_controller",
                    data={
                        "principal_id": principal_id,
                        "role_id": role_id
                    }
                ))
                
    def create_policy(
        self,
        name: str,
        effect: PolicyEffect,
        principals: List[str],
        resources: List[str],
        actions: List[Permission],
        conditions: Optional[List[PolicyCondition]] = None,
        **kwargs
    ) -> AccessPolicy:
        """Create access policy"""
        policy = AccessPolicy(
            name=name,
            effect=effect,
            principals=principals,
            resources=resources,
            actions=actions,
            conditions=conditions or [],
            **kwargs
        )
        
        self._policies[policy.id] = policy
        
        # Update indexes
        for resource_pattern in resources:
            self._resource_policies[resource_pattern].add(policy.id)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="access.policy.created",
                source="access_controller",
                data={
                    "policy_id": policy.id,
                    "policy_name": name,
                    "effect": effect.value
                }
            ))
            
        logger.info(f"Created policy: {name}")
        return policy
        
    def check_access(
        self,
        principal_id: str,
        resource_id: str,
        action: Permission,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Check if principal has access to resource"""
        start_time = datetime.utcnow()
        context = context or {}
        
        # Create access request for audit
        request = AccessRequest(
            principal_id=principal_id,
            resource_id=resource_id,
            action=action,
            context=context
        )
        
        # Check cache
        if self.cache:
            cache_key = f"access:{principal_id}:{resource_id}:{action.value}"
            cached = self.cache.get(cache_key)
            if cached is not None:
                request.granted = cached
                request.response_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
                self._log_access_request(request)
                return cached
                
        # Get principal
        principal = self._principals.get(principal_id)
        if not principal or not principal.is_active:
            request.granted = False
            request.denial_reason = "Principal not found or inactive"
            self._log_access_request(request)
            return False
            
        # Build evaluation context
        eval_context = {
            "principal": principal.to_dict(),
            "resource": {"id": resource_id},
            "action": action.value,
            "environment": {
                "time": datetime.utcnow().isoformat(),
                "ip_address": context.get("ip_address"),
                "user_agent": context.get("user_agent")
            }
        }
        eval_context.update(context)
        
        # Get effective permissions from roles
        effective_permissions = self._get_effective_permissions(principal, resource_id)
        
        # Check role-based permissions
        if action in effective_permissions or Permission.ADMIN in effective_permissions:
            # Check if any deny policies override
            if not self._check_deny_policies(principal_id, resource_id, action, eval_context, request):
                request.granted = False
                request.denial_reason = "Denied by policy"
            else:
                request.granted = True
        else:
            # Check allow policies
            request.granted = self._check_allow_policies(
                principal_id, resource_id, action, eval_context, request
            )
            if not request.granted:
                request.denial_reason = "No matching allow policy"
                
        # Cache result
        if self.cache:
            cache_key = f"access:{principal_id}:{resource_id}:{action.value}"
            self.cache.set(cache_key, request.granted, ttl=300)
            
        # Calculate response time
        request.response_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        # Log request
        self._log_access_request(request)
        
        return request.granted
        
    def _get_effective_permissions(
        self,
        principal: Principal,
        resource_id: str
    ) -> Set[Permission]:
        """Get effective permissions from roles"""
        permissions = set()
        
        # Get permissions from assigned roles
        for role_id in principal.roles:
            role = self._roles.get(role_id)
            if role:
                # Check if role applies to resource
                for pattern in role.resource_patterns:
                    if self._matches_resource_pattern(resource_id, pattern):
                        permissions.update(role.permissions)
                        break
                        
                # Get permissions from parent roles
                for parent_role_id in role.parent_roles:
                    parent_role = self._roles.get(parent_role_id)
                    if parent_role:
                        for pattern in parent_role.resource_patterns:
                            if self._matches_resource_pattern(resource_id, pattern):
                                permissions.update(parent_role.permissions)
                                break
                                
        return permissions
        
    def _matches_resource_pattern(self, resource_id: str, pattern: str) -> bool:
        """Check if resource matches pattern"""
        if pattern == "*":
            return True
        if pattern == resource_id:
            return True
        if pattern.endswith("*") and resource_id.startswith(pattern[:-1]):
            return True
        if "*" in pattern:
            regex_pattern = pattern.replace("*", ".*")
            return bool(re.match(f"^{regex_pattern}$", resource_id))
        return False
        
    def _check_deny_policies(
        self,
        principal_id: str,
        resource_id: str,
        action: Permission,
        context: Dict[str, Any],
        request: AccessRequest
    ) -> bool:
        """Check if any deny policies apply (returns False if denied)"""
        # Get applicable policies
        policies = self._get_applicable_policies(principal_id, resource_id, action, PolicyEffect.DENY)
        
        for policy in policies:
            request.evaluated_policies.append(policy.id)
            
            if policy.evaluate_conditions(context):
                # Deny policy matched
                return False
                
        return True  # No deny policies matched
        
    def _check_allow_policies(
        self,
        principal_id: str,
        resource_id: str,
        action: Permission,
        context: Dict[str, Any],
        request: AccessRequest
    ) -> bool:
        """Check if any allow policies apply"""
        # Get applicable policies
        policies = self._get_applicable_policies(principal_id, resource_id, action, PolicyEffect.ALLOW)
        
        for policy in policies:
            request.evaluated_policies.append(policy.id)
            
            if policy.evaluate_conditions(context):
                # Allow policy matched
                return True
                
        return False  # No allow policies matched
        
    def _get_applicable_policies(
        self,
        principal_id: str,
        resource_id: str,
        action: Permission,
        effect: PolicyEffect
    ) -> List[AccessPolicy]:
        """Get policies that apply to the request"""
        applicable = []
        
        for policy in self._policies.values():
            if (policy.effect == effect and
                policy.is_valid() and
                policy.matches_principal(principal_id) and
                policy.matches_resource(resource_id) and
                policy.matches_action(action)):
                applicable.append(policy)
                
        # Sort by priority (higher first)
        applicable.sort(key=lambda p: p.priority, reverse=True)
        
        return applicable
        
    def _log_access_request(self, request: AccessRequest):
        """Log access request for audit"""
        self._access_log.append(request)
        
        # Limit log size
        if len(self._access_log) > 10000:
            self._access_log = self._access_log[-5000:]
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="access.request",
                source="access_controller",
                data=request.to_dict()
            ))
            
    def _clear_principal_cache(self, principal_id: str):
        """Clear cache entries for principal"""
        if self.cache:
            # Clear with pattern matching if supported
            # For now, just log
            logger.info(f"Clearing cache for principal: {principal_id}")
            
    def grant_access(
        self,
        principal_id: str,
        resource_id: str,
        permissions: List[Permission],
        valid_for: Optional[timedelta] = None
    ) -> AccessPolicy:
        """Grant access to resource (convenience method)"""
        valid_until = None
        if valid_for:
            valid_until = datetime.utcnow() + valid_for
            
        policy = self.create_policy(
            name=f"grant_{principal_id}_{resource_id}",
            effect=PolicyEffect.ALLOW,
            principals=[principal_id],
            resources=[resource_id],
            actions=permissions,
            valid_until=valid_until,
            created_by="system"
        )
        
        return policy
        
    def revoke_access(
        self,
        principal_id: str,
        resource_id: str,
        permissions: Optional[List[Permission]] = None
    ):
        """Revoke access to resource"""
        # Find and deactivate matching policies
        for policy in self._policies.values():
            if (principal_id in policy.principals and
                resource_id in policy.resources and
                policy.effect == PolicyEffect.ALLOW):
                
                if permissions:
                    # Remove specific permissions
                    policy.actions = [a for a in policy.actions if a not in permissions]
                    if not policy.actions:
                        policy.is_active = False
                else:
                    # Revoke all access
                    policy.is_active = False
                    
        # Clear cache
        if self.cache:
            self._clear_principal_cache(principal_id)
            
    def get_principal_permissions(
        self,
        principal_id: str,
        resource_id: str
    ) -> List[Permission]:
        """Get all permissions for principal on resource"""
        principal = self._principals.get(principal_id)
        if not principal:
            return []
            
        # Get permissions from roles
        permissions = list(self._get_effective_permissions(principal, resource_id))
        
        # Get permissions from policies
        context = {"principal": principal.to_dict(), "resource": {"id": resource_id}}
        
        for policy in self._policies.values():
            if (policy.is_valid() and
                policy.effect == PolicyEffect.ALLOW and
                policy.matches_principal(principal_id) and
                policy.matches_resource(resource_id) and
                policy.evaluate_conditions(context)):
                permissions.extend(policy.actions)
                
        return list(set(permissions))
        
    def get_resource_principals(
        self,
        resource_id: str,
        permission: Optional[Permission] = None
    ) -> List[Principal]:
        """Get principals with access to resource"""
        principals = []
        
        for principal in self._principals.values():
            if permission:
                if self.check_access(principal.id, resource_id, permission):
                    principals.append(principal)
            else:
                # Check if principal has any access
                if any(self.check_access(principal.id, resource_id, p) for p in Permission):
                    principals.append(principal)
                    
        return principals
        
    def get_access_log(
        self,
        principal_id: Optional[str] = None,
        resource_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        granted_only: bool = False
    ) -> List[AccessRequest]:
        """Get access log entries"""
        logs = self._access_log
        
        # Apply filters
        if principal_id:
            logs = [l for l in logs if l.principal_id == principal_id]
        if resource_id:
            logs = [l for l in logs if l.resource_id == resource_id]
        if start_time:
            logs = [l for l in logs if l.requested_at >= start_time]
        if end_time:
            logs = [l for l in logs if l.requested_at <= end_time]
        if granted_only:
            logs = [l for l in logs if l.granted]
            
        return logs
        
    def export_policies(self) -> Dict[str, Any]:
        """Export all policies"""
        return {
            "principals": {
                p_id: p.to_dict() for p_id, p in self._principals.items()
            },
            "roles": {
                r_id: r.to_dict() for r_id, r in self._roles.items()
            },
            "policies": {
                p_id: p.to_dict() for p_id, p in self._policies.items()
            },
            "exported_at": datetime.utcnow().isoformat()
        }
        
    def import_policies(self, data: Dict[str, Any]):
        """Import policies"""
        # Import principals
        for p_data in data.get("principals", {}).values():
            p_data["principal_type"] = PrincipalType(p_data["principal_type"])
            p_data["created_at"] = datetime.fromisoformat(p_data["created_at"])
            principal = Principal(**p_data)
            self._principals[principal.id] = principal
            
        # Import roles
        for r_data in data.get("roles", {}).values():
            r_data["permissions"] = [Permission(p) for p in r_data["permissions"]]
            r_data["created_at"] = datetime.fromisoformat(r_data["created_at"])
            role = Role(**r_data)
            self._roles[role.id] = role
            
        # Import policies
        for p_data in data.get("policies", {}).values():
            p_data["effect"] = PolicyEffect(p_data["effect"])
            p_data["actions"] = [Permission(a) for a in p_data["actions"]]
            p_data["created_at"] = datetime.fromisoformat(p_data["created_at"])
            
            # Handle conditions
            conditions = []
            for c_data in p_data.get("conditions", []):
                c_data["operator"] = PolicyConditionOperator(c_data["operator"])
                conditions.append(PolicyCondition(**c_data))
            p_data["conditions"] = conditions
            
            # Handle dates
            for field in ["valid_from", "valid_until"]:
                if p_data.get(field):
                    p_data[field] = datetime.fromisoformat(p_data[field])
                    
            policy = AccessPolicy(**p_data)
            self._policies[policy.id] = policy 