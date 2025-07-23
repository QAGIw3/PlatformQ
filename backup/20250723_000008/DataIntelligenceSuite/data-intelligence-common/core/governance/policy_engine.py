"""
Policy-based Data Access Control Engine

Provides fine-grained access control using policy-as-code with OPA integration.
"""

from typing import Any, Dict, List, Optional, Union, Set, Tuple
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import json
import asyncio
from abc import ABC, abstractmethod
import aiohttp
import rego
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives import serialization

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class PolicyEffect(str, Enum):
    """Policy decision effects"""
    ALLOW = "allow"
    DENY = "deny"
    CONDITIONAL = "conditional"


class ResourceType(str, Enum):
    """Resource types"""
    DATASET = "dataset"
    TABLE = "table"
    COLUMN = "column"
    ROW = "row"
    FILE = "file"
    MODEL = "model"
    FEATURE = "feature"
    METRIC = "metric"
    REPORT = "report"
    API = "api"


class Action(str, Enum):
    """Actions on resources"""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    EXECUTE = "execute"
    SHARE = "share"
    GRANT = "grant"
    REVOKE = "revoke"


class PolicyType(str, Enum):
    """Policy types"""
    RBAC = "rbac"  # Role-based
    ABAC = "abac"  # Attribute-based
    PBAC = "pbac"  # Purpose-based
    TBAC = "tbac"  # Tag-based
    CBAC = "cbac"  # Context-based


@dataclass
class Principal:
    """Security principal (user/service)"""
    id: str
    type: str  # user, service, group
    attributes: Dict[str, Any] = field(default_factory=dict)
    roles: List[str] = field(default_factory=list)
    groups: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "attributes": self.attributes,
            "roles": self.roles,
            "groups": self.groups
        }


@dataclass
class Resource:
    """Resource being accessed"""
    id: str
    type: ResourceType
    attributes: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    owner: Optional[str] = None
    classification: Optional[str] = None  # public, internal, confidential, restricted
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type.value,
            "attributes": self.attributes,
            "tags": self.tags,
            "owner": self.owner,
            "classification": self.classification
        }


@dataclass
class Context:
    """Request context"""
    timestamp: datetime
    ip_address: Optional[str] = None
    location: Optional[str] = None
    purpose: Optional[str] = None
    session_id: Optional[str] = None
    request_id: Optional[str] = None
    additional: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "ip_address": self.ip_address,
            "location": self.location,
            "purpose": self.purpose,
            "session_id": self.session_id,
            "request_id": self.request_id,
            **self.additional
        }


@dataclass
class PolicyDecision:
    """Policy evaluation decision"""
    effect: PolicyEffect
    resource: Resource
    action: Action
    principal: Principal
    context: Context
    reasons: List[str] = field(default_factory=list)
    obligations: List[Dict[str, Any]] = field(default_factory=list)
    conditions: List[Dict[str, Any]] = field(default_factory=list)
    evaluated_policies: List[str] = field(default_factory=list)
    
    @property
    def is_allowed(self) -> bool:
        return self.effect == PolicyEffect.ALLOW
    
    @property
    def is_denied(self) -> bool:
        return self.effect == PolicyEffect.DENY
    
    @property
    def is_conditional(self) -> bool:
        return self.effect == PolicyEffect.CONDITIONAL


@dataclass
class Policy:
    """Access control policy"""
    id: str
    name: str
    type: PolicyType
    version: int
    rules: List[Dict[str, Any]]
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    
    def to_rego(self) -> str:
        """Convert policy to Rego"""
        # This is a simplified conversion
        rego_rules = []
        
        for rule in self.rules:
            conditions = []
            
            # Principal conditions
            if "principal" in rule:
                for key, value in rule["principal"].items():
                    if isinstance(value, list):
                        conditions.append(f'input.principal.{key}[_] == "{value}"')
                    else:
                        conditions.append(f'input.principal.{key} == "{value}"')
            
            # Resource conditions
            if "resource" in rule:
                for key, value in rule["resource"].items():
                    conditions.append(f'input.resource.{key} == "{value}"')
            
            # Action conditions
            if "action" in rule:
                actions = rule["action"] if isinstance(rule["action"], list) else [rule["action"]]
                action_conditions = [f'input.action == "{a}"' for a in actions]
                conditions.append(f'({" or ".join(action_conditions)})')
            
            # Effect
            effect = rule.get("effect", "allow")
            rule_name = f'{effect}_{self.id}_{rule.get("id", "default")}'
            
            rego_rule = f'{rule_name} {{\n'
            for condition in conditions:
                rego_rule += f'    {condition}\n'
            rego_rule += '}\n'
            
            rego_rules.append(rego_rule)
        
        return '\n'.join(rego_rules)


class PolicyEngine(ABC):
    """Abstract policy engine interface"""
    
    @abstractmethod
    async def evaluate(
        self,
        principal: Principal,
        resource: Resource,
        action: Action,
        context: Context
    ) -> PolicyDecision:
        """Evaluate access request"""
        pass
    
    @abstractmethod
    async def add_policy(self, policy: Policy) -> bool:
        """Add new policy"""
        pass
    
    @abstractmethod
    async def remove_policy(self, policy_id: str) -> bool:
        """Remove policy"""
        pass
    
    @abstractmethod
    async def update_policy(self, policy: Policy) -> bool:
        """Update existing policy"""
        pass
    
    @abstractmethod
    async def list_policies(
        self,
        resource_type: Optional[ResourceType] = None,
        policy_type: Optional[PolicyType] = None
    ) -> List[Policy]:
        """List policies"""
        pass


class OPAPolicyEngine(PolicyEngine):
    """Open Policy Agent based policy engine"""
    
    def __init__(
        self,
        opa_url: str = "http://localhost:8181",
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None
    ):
        self.opa_url = opa_url
        self.vault_client = vault_client
        self.consul_client = consul_client
        self._session: Optional[aiohttp.ClientSession] = None
        self._policies: Dict[str, Policy] = {}
        
    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        await self._load_policies()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
    
    async def _load_policies(self):
        """Load policies from Consul"""
        if self.consul_client:
            policies_data = await self.consul_client.get_prefix("policies/")
            for key, value in policies_data.items():
                policy_data = json.loads(value)
                policy = Policy(**policy_data)
                self._policies[policy.id] = policy
                
                # Push to OPA
                await self._push_policy_to_opa(policy)
    
    async def _push_policy_to_opa(self, policy: Policy) -> bool:
        """Push policy to OPA"""
        try:
            # Convert policy to Rego
            rego_policy = policy.to_rego()
            
            # Push to OPA
            url = f"{self.opa_url}/v1/policies/{policy.id}"
            async with self._session.put(
                url,
                data=rego_policy,
                headers={"Content-Type": "text/plain"}
            ) as response:
                return response.status == 200
                
        except Exception as e:
            logger.error(f"Failed to push policy to OPA: {e}")
            return False
    
    async def evaluate(
        self,
        principal: Principal,
        resource: Resource,
        action: Action,
        context: Context
    ) -> PolicyDecision:
        """Evaluate access request using OPA"""
        try:
            # Prepare input
            input_data = {
                "principal": principal.to_dict(),
                "resource": resource.to_dict(),
                "action": action.value,
                "context": context.to_dict()
            }
            
            # Query OPA
            url = f"{self.opa_url}/v1/data/authz/allow"
            async with self._session.post(url, json={"input": input_data}) as response:
                result = await response.json()
            
            # Parse result
            allowed = result.get("result", False)
            
            # Get detailed decision
            url = f"{self.opa_url}/v1/data/authz/decision"
            async with self._session.post(url, json={"input": input_data}) as response:
                decision_data = await response.json()
            
            decision_result = decision_data.get("result", {})
            
            return PolicyDecision(
                effect=PolicyEffect.ALLOW if allowed else PolicyEffect.DENY,
                resource=resource,
                action=action,
                principal=principal,
                context=context,
                reasons=decision_result.get("reasons", []),
                obligations=decision_result.get("obligations", []),
                conditions=decision_result.get("conditions", []),
                evaluated_policies=decision_result.get("evaluated_policies", [])
            )
            
        except Exception as e:
            logger.error(f"Failed to evaluate policy: {e}")
            # Fail closed - deny on error
            return PolicyDecision(
                effect=PolicyEffect.DENY,
                resource=resource,
                action=action,
                principal=principal,
                context=context,
                reasons=[f"Policy evaluation error: {str(e)}"]
            )
    
    async def add_policy(self, policy: Policy) -> bool:
        """Add new policy"""
        try:
            # Store in memory
            self._policies[policy.id] = policy
            
            # Push to OPA
            success = await self._push_policy_to_opa(policy)
            
            # Store in Consul
            if self.consul_client and success:
                await self.consul_client.put(
                    f"policies/{policy.id}",
                    json.dumps(policy.__dict__, default=str)
                )
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to add policy: {e}")
            return False
    
    async def remove_policy(self, policy_id: str) -> bool:
        """Remove policy"""
        try:
            # Remove from memory
            if policy_id in self._policies:
                del self._policies[policy_id]
            
            # Remove from OPA
            url = f"{self.opa_url}/v1/policies/{policy_id}"
            async with self._session.delete(url) as response:
                success = response.status == 200
            
            # Remove from Consul
            if self.consul_client and success:
                await self.consul_client.delete(f"policies/{policy_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to remove policy: {e}")
            return False
    
    async def update_policy(self, policy: Policy) -> bool:
        """Update existing policy"""
        # Remove and re-add
        await self.remove_policy(policy.id)
        return await self.add_policy(policy)
    
    async def list_policies(
        self,
        resource_type: Optional[ResourceType] = None,
        policy_type: Optional[PolicyType] = None
    ) -> List[Policy]:
        """List policies"""
        policies = list(self._policies.values())
        
        # Filter by resource type
        if resource_type:
            policies = [
                p for p in policies
                if any(
                    rule.get("resource", {}).get("type") == resource_type.value
                    for rule in p.rules
                )
            ]
        
        # Filter by policy type
        if policy_type:
            policies = [p for p in policies if p.type == policy_type]
        
        return policies


class DataMaskingEngine:
    """Data masking engine for sensitive data"""
    
    def __init__(self, vault_client: Optional[VaultClient] = None):
        self.vault_client = vault_client
        self._masking_rules: Dict[str, Callable] = {}
        self._setup_default_rules()
    
    def _setup_default_rules(self):
        """Setup default masking rules"""
        import re
        
        # SSN masking
        self._masking_rules["ssn"] = lambda x: re.sub(
            r'\d{3}-\d{2}-\d{4}',
            'XXX-XX-XXXX',
            str(x)
        )
        
        # Email masking
        self._masking_rules["email"] = lambda x: re.sub(
            r'([^@]+)@([^@]+)',
            r'****@\2',
            str(x)
        )
        
        # Credit card masking
        self._masking_rules["credit_card"] = lambda x: re.sub(
            r'\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?(\d{4})',
            r'XXXX-XXXX-XXXX-\1',
            str(x)
        )
        
        # Phone masking
        self._masking_rules["phone"] = lambda x: re.sub(
            r'(\d{3})[\s-]?(\d{3})[\s-]?(\d{4})',
            r'XXX-XXX-\3',
            str(x)
        )
    
    async def mask_data(
        self,
        data: Any,
        masking_policy: Dict[str, str],
        context: Optional[Context] = None
    ) -> Any:
        """
        Mask sensitive data based on policy.
        
        Args:
            data: Data to mask
            masking_policy: Field to masking type mapping
            context: Request context
            
        Returns:
            Masked data
        """
        if isinstance(data, dict):
            masked = {}
            for key, value in data.items():
                if key in masking_policy:
                    mask_type = masking_policy[key]
                    if mask_type in self._masking_rules:
                        masked[key] = self._masking_rules[mask_type](value)
                    elif mask_type == "encrypt":
                        # Use Vault transit encryption
                        if self.vault_client:
                            encrypted = await self.vault_client.encrypt_data(
                                "data-masking",
                                str(value)
                            )
                            masked[key] = encrypted
                        else:
                            masked[key] = "***ENCRYPTED***"
                    elif mask_type == "hash":
                        # Hash the value
                        import hashlib
                        masked[key] = hashlib.sha256(
                            str(value).encode()
                        ).hexdigest()[:8] + "..."
                    else:
                        masked[key] = "***MASKED***"
                else:
                    # Recursively mask nested data
                    if isinstance(value, (dict, list)):
                        masked[key] = await self.mask_data(
                            value,
                            masking_policy,
                            context
                        )
                    else:
                        masked[key] = value
            return masked
            
        elif isinstance(data, list):
            return [
                await self.mask_data(item, masking_policy, context)
                for item in data
            ]
        
        else:
            return data


class AuditLogger:
    """Audit logger for access decisions"""
    
    def __init__(
        self,
        storage_backend: str = "elasticsearch",
        backend_config: Optional[Dict[str, Any]] = None
    ):
        self.storage_backend = storage_backend
        self.backend_config = backend_config or {}
        self._buffer: List[Dict[str, Any]] = []
        self._buffer_size = 100
        
    async def log_decision(self, decision: PolicyDecision):
        """Log policy decision"""
        audit_entry = {
            "timestamp": datetime.now().isoformat(),
            "principal_id": decision.principal.id,
            "principal_type": decision.principal.type,
            "resource_id": decision.resource.id,
            "resource_type": decision.resource.type.value,
            "action": decision.action.value,
            "effect": decision.effect.value,
            "reasons": decision.reasons,
            "context": decision.context.to_dict(),
            "evaluated_policies": decision.evaluated_policies
        }
        
        self._buffer.append(audit_entry)
        
        # Flush buffer if full
        if len(self._buffer) >= self._buffer_size:
            await self._flush_buffer()
    
    async def _flush_buffer(self):
        """Flush audit buffer to storage"""
        if not self._buffer:
            return
        
        try:
            if self.storage_backend == "elasticsearch":
                # Send to Elasticsearch
                # This would use the actual ES client
                logger.info(f"Flushing {len(self._buffer)} audit entries to Elasticsearch")
            
            elif self.storage_backend == "s3":
                # Send to S3
                logger.info(f"Flushing {len(self._buffer)} audit entries to S3")
            
            self._buffer.clear()
            
        except Exception as e:
            logger.error(f"Failed to flush audit buffer: {e}")
    
    async def query_audit_log(
        self,
        principal_id: Optional[str] = None,
        resource_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Query audit log"""
        # This would query the actual storage backend
        return []


class PolicyBuilder:
    """Helper for building policies"""
    
    @staticmethod
    def create_rbac_policy(
        policy_id: str,
        policy_name: str,
        role: str,
        resource_type: ResourceType,
        actions: List[Action],
        conditions: Optional[Dict[str, Any]] = None
    ) -> Policy:
        """Create RBAC policy"""
        rules = [{
            "id": "default",
            "effect": "allow",
            "principal": {"roles": role},
            "resource": {"type": resource_type.value},
            "action": [a.value for a in actions]
        }]
        
        if conditions:
            rules[0]["conditions"] = conditions
        
        return Policy(
            id=policy_id,
            name=policy_name,
            type=PolicyType.RBAC,
            version=1,
            rules=rules
        )
    
    @staticmethod
    def create_abac_policy(
        policy_id: str,
        policy_name: str,
        attributes: Dict[str, Any],
        resource_type: ResourceType,
        actions: List[Action]
    ) -> Policy:
        """Create ABAC policy"""
        rules = [{
            "id": "default",
            "effect": "allow",
            "principal": {"attributes": attributes},
            "resource": {"type": resource_type.value},
            "action": [a.value for a in actions]
        }]
        
        return Policy(
            id=policy_id,
            name=policy_name,
            type=PolicyType.ABAC,
            version=1,
            rules=rules
        )
    
    @staticmethod
    def create_data_classification_policy(
        policy_id: str,
        policy_name: str,
        classification: str,
        allowed_roles: List[str],
        allowed_purposes: List[str]
    ) -> Policy:
        """Create data classification policy"""
        rules = [{
            "id": "classification_check",
            "effect": "allow",
            "principal": {"roles": allowed_roles},
            "resource": {"classification": classification},
            "action": ["read"],
            "conditions": {
                "purpose_in": allowed_purposes
            }
        }]
        
        return Policy(
            id=policy_id,
            name=policy_name,
            type=PolicyType.PBAC,
            version=1,
            rules=rules
        )


# Example Rego policies

EXAMPLE_REGO_POLICIES = """
package authz

import future.keywords.contains
import future.keywords.if
import future.keywords.in

# Default deny
default allow := false

# RBAC: Allow data scientists to read datasets
allow if {
    input.action == "read"
    input.resource.type == "dataset"
    "data_scientist" in input.principal.roles
}

# ABAC: Allow access based on department
allow if {
    input.action in ["read", "write"]
    input.resource.type == "table"
    input.principal.attributes.department == input.resource.attributes.department
}

# PBAC: Allow access for specific purposes
allow if {
    input.action == "read"
    input.resource.classification in ["public", "internal"]
    input.context.purpose in ["analytics", "reporting"]
}

# Data residency check
allow if {
    input.action == "read"
    input.resource.attributes.location == input.principal.attributes.location
}

# Time-based access
allow if {
    input.action == "read"
    time.now_ns() >= input.resource.attributes.embargo_until
}

# Conditional access with obligations
decision := {
    "allow": allow,
    "reasons": reasons,
    "obligations": obligations,
    "conditions": conditions
}

reasons[msg] {
    allow
    msg := "Access granted based on role"
    "data_scientist" in input.principal.roles
}

reasons[msg] {
    not allow
    msg := "Access denied - insufficient privileges"
}

obligations[obj] {
    allow
    input.resource.classification == "confidential"
    obj := {
        "type": "audit",
        "detail": "Accessed confidential data"
    }
}

obligations[obj] {
    allow
    input.resource.tags[_] == "pii"
    obj := {
        "type": "mask",
        "fields": ["ssn", "email", "phone"]
    }
}

conditions[cond] {
    allow
    input.resource.attributes.temporary_access
    cond := {
        "type": "time_limit",
        "expires_at": input.resource.attributes.access_expires_at
    }
}
"""


# Example usage

async def example_usage():
    """Example of using the policy engine"""
    
    # Create policy engine
    async with OPAPolicyEngine() as engine:
        
        # Create a data classification policy
        policy = PolicyBuilder.create_data_classification_policy(
            policy_id="confidential_data_policy",
            policy_name="Confidential Data Access",
            classification="confidential",
            allowed_roles=["data_scientist", "analyst"],
            allowed_purposes=["research", "analytics"]
        )
        
        # Add policy
        await engine.add_policy(policy)
        
        # Create access request
        principal = Principal(
            id="user123",
            type="user",
            roles=["data_scientist"],
            attributes={"department": "research", "clearance": "secret"}
        )
        
        resource = Resource(
            id="dataset:customer_data",
            type=ResourceType.DATASET,
            classification="confidential",
            tags=["pii", "sensitive"],
            attributes={"department": "research"}
        )
        
        context = Context(
            timestamp=datetime.now(),
            purpose="analytics",
            ip_address="192.168.1.100"
        )
        
        # Evaluate access
        decision = await engine.evaluate(
            principal=principal,
            resource=resource,
            action=Action.READ,
            context=context
        )
        
        print(f"Access decision: {decision.effect.value}")
        print(f"Reasons: {decision.reasons}")
        
        # Check obligations
        if decision.obligations:
            print(f"Obligations: {decision.obligations}")
            
            # Apply data masking if required
            for obligation in decision.obligations:
                if obligation["type"] == "mask":
                    masking_engine = DataMaskingEngine()
                    
                    sample_data = {
                        "customer_id": "12345",
                        "ssn": "123-45-6789",
                        "email": "user@example.com",
                        "revenue": 50000
                    }
                    
                    masked_data = await masking_engine.mask_data(
                        sample_data,
                        {field: field for field in obligation["fields"]},
                        context
                    )
                    
                    print(f"Masked data: {masked_data}")
        
        # Log audit
        audit_logger = AuditLogger()
        await audit_logger.log_decision(decision)


if __name__ == "__main__":
    asyncio.run(example_usage()) 