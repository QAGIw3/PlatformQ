"""
Open Policy Agent (OPA) Integration

Provides centralized, fine-grained authorization using policy-as-code.
"""

import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import httpx
import json
from datetime import datetime
import asyncio

logger = logging.getLogger(__name__)


@dataclass
class OPAConfig:
    """OPA configuration"""
    host: str = "localhost"
    port: int = 8181
    scheme: str = "http"
    policy_path: str = "/v1/data/platformq/authz"
    bundle_path: str = "/v1/policies"
    timeout: float = 5.0
    enable_metrics: bool = True


@dataclass
class AuthzRequest:
    """Authorization request"""
    subject: str  # User ID or service name
    resource: str  # Resource being accessed
    action: str  # Action being performed
    context: Dict[str, Any]  # Additional context (tenant, roles, etc.)


@dataclass
class AuthzResponse:
    """Authorization response"""
    allowed: bool
    reason: Optional[str] = None
    obligations: Optional[Dict[str, Any]] = None  # Additional requirements
    


class OPAClient:
    """
    Client for Open Policy Agent integration.
    
    Features:
    - Policy evaluation
    - Dynamic policy updates
    - Decision logging
    - Performance metrics
    - Policy testing
    """
    
    def __init__(self, config: OPAConfig):
        self.config = config
        self.base_url = f"{config.scheme}://{config.host}:{config.port}"
        self._client: Optional[httpx.AsyncClient] = None
        self._policy_cache: Dict[str, Any] = {}
        self._decision_log: List[Dict[str, Any]] = []
        
    async def initialize(self) -> None:
        """Initialize OPA client"""
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.config.timeout
        )
        
        # Load initial policies
        await self._load_default_policies()
        
        # Test connection
        await self.health_check()
        
        logger.info("OPA client initialized")
        
    async def _load_default_policies(self) -> None:
        """Load default authorization policies"""
        # Default RBAC policy
        rbac_policy = """
        package platformq.authz

        default allow = false

        # Allow if user has required role
        allow {
            input.action == data.permissions[input.resource][_]
            input.context.roles[_] == data.roles[input.resource][input.action][_]
        }

        # Allow service-to-service communication
        allow {
            input.context.service_name != null
            data.service_permissions[input.context.service_name][_] == input.resource
        }

        # Allow admin override
        allow {
            input.context.roles[_] == "platform_admin"
        }

        # Resource-specific policies
        
        # Digital assets - owner can always access
        allow {
            input.resource == "digital_asset"
            input.context.user_id == data.assets[input.context.asset_id].owner_id
        }
        
        # Workflows - participants can view
        allow {
            input.resource == "workflow"
            input.action == "view"
            input.context.user_id == data.workflows[input.context.workflow_id].participants[_]
        }
        
        # Blockchain transactions - compliance check
        allow {
            input.resource == "blockchain_transaction"
            input.context.compliance_verified == true
            input.context.risk_score < 0.7
        }
        
        # Data access - column level security
        allow {
            input.resource == "data_query"
            not contains_restricted_columns(input.context.query_columns)
        }
        
        contains_restricted_columns(columns) {
            restricted := data.restricted_columns[input.context.tenant_id]
            columns[_] == restricted[_]
        }
        
        # Rate limiting by role
        rate_limit[limit] {
            role := input.context.roles[_]
            limit := data.rate_limits[role]
        }
        
        # Obligations - additional requirements
        obligations[key] = value {
            input.resource == "sensitive_data"
            key := "audit_log"
            value := true
        }
        
        obligations[key] = value {
            input.resource == "financial_transaction"
            input.context.amount > 10000
            key := "requires_2fa"
            value := true
        }
        """
        
        await self.update_policy("rbac", rbac_policy)
        
        # Data governance policy
        data_governance_policy = """
        package platformq.data_governance

        # GDPR compliance
        allow_data_access {
            input.purpose != null
            valid_purposes[input.purpose]
            consent_given
        }

        valid_purposes := {
            "service_delivery",
            "analytics",
            "compliance",
            "security"
        }

        consent_given {
            data.user_consent[input.data_subject][input.purpose] == true
        }

        # Data retention
        data_expired {
            retention_days := data.retention_policies[input.data_type]
            age_days := time.now_ns() - input.created_at
            age_days > retention_days * 24 * 60 * 60 * 1000000000
        }

        # Data masking requirements
        requires_masking[field] {
            field := data.pii_fields[_]
            not input.context.roles[_] == "data_privacy_officer"
        }
        """
        
        await self.update_policy("data_governance", data_governance_policy)
        
        # Multi-tenant isolation policy
        tenant_isolation_policy = """
        package platformq.tenant_isolation

        # Ensure tenant isolation
        allow {
            input.context.tenant_id == data.resources[input.resource_id].tenant_id
        }

        # Cross-tenant access for platform services
        allow {
            input.context.service_name == "analytics-service"
            input.action == "aggregate"
            input.context.cross_tenant_token != null
        }
        """
        
        await self.update_policy("tenant_isolation", tenant_isolation_policy)
        
        # Zero-trust network policy
        network_policy = """
        package platformq.network

        # Service mesh authorization
        allow_connection {
            source_service := input.source.service_name
            dest_service := input.destination.service_name
            
            data.service_graph[source_service].allowed_destinations[_] == dest_service
            valid_mtls_cert
            not service_deprecated(dest_service)
        }

        valid_mtls_cert {
            input.source.mtls_verified == true
            input.source.cert_expiry > time.now_ns()
        }

        service_deprecated(service) {
            data.deprecated_services[_] == service
        }
        """
        
        await self.update_policy("network", network_policy)
        
    async def authorize(self, request: AuthzRequest) -> AuthzResponse:
        """Evaluate authorization request"""
        start_time = datetime.utcnow()
        
        try:
            # Prepare input for OPA
            opa_input = {
                "subject": request.subject,
                "resource": request.resource,
                "action": request.action,
                "context": request.context,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Query OPA
            response = await self._client.post(
                self.config.policy_path,
                json={"input": opa_input}
            )
            response.raise_for_status()
            
            result = response.json()["result"]
            
            # Extract decision
            allowed = result.get("allow", False)
            obligations = result.get("obligations", {})
            
            # Generate reason
            reason = self._generate_reason(allowed, result)
            
            # Log decision
            if self.config.enable_metrics:
                await self._log_decision(request, allowed, start_time)
                
            return AuthzResponse(
                allowed=allowed,
                reason=reason,
                obligations=obligations
            )
            
        except Exception as e:
            logger.error(f"Authorization error: {e}")
            # Fail closed - deny on error
            return AuthzResponse(
                allowed=False,
                reason=f"Authorization service error: {str(e)}"
            )
            
    async def batch_authorize(self, requests: List[AuthzRequest]) -> List[AuthzResponse]:
        """Evaluate multiple authorization requests"""
        tasks = [self.authorize(req) for req in requests]
        return await asyncio.gather(*tasks)
        
    async def update_policy(self, name: str, policy: str) -> None:
        """Update an OPA policy"""
        try:
            response = await self._client.put(
                f"/v1/policies/{name}",
                content=policy,
                headers={"Content-Type": "text/plain"}
            )
            response.raise_for_status()
            
            # Clear policy cache
            self._policy_cache.pop(name, None)
            
            logger.info(f"Updated policy: {name}")
            
        except Exception as e:
            logger.error(f"Failed to update policy {name}: {e}")
            raise
            
    async def update_data(self, path: str, data: Dict[str, Any]) -> None:
        """Update OPA data"""
        try:
            response = await self._client.put(
                f"/v1/data/{path}",
                json=data
            )
            response.raise_for_status()
            
            logger.info(f"Updated data: {path}")
            
        except Exception as e:
            logger.error(f"Failed to update data {path}: {e}")
            raise
            
    async def add_role_permission(self, role: str, resource: str, 
                                 actions: List[str]) -> None:
        """Add permissions for a role"""
        data = {
            "roles": {
                resource: {
                    action: [role] for action in actions
                }
            }
        }
        
        await self.update_data("platformq/authz/roles", data)
        
    async def add_service_permission(self, service: str, 
                                   resources: List[str]) -> None:
        """Add service-to-service permissions"""
        data = {
            "service_permissions": {
                service: resources
            }
        }
        
        await self.update_data("platformq/authz/service_permissions", data)
        
    async def set_rate_limit(self, role: str, limit: int) -> None:
        """Set rate limit for a role"""
        data = {
            "rate_limits": {
                role: limit
            }
        }
        
        await self.update_data("platformq/authz/rate_limits", data)
        
    async def add_restricted_columns(self, tenant_id: str, 
                                   columns: List[str]) -> None:
        """Add restricted columns for a tenant"""
        data = {
            "restricted_columns": {
                tenant_id: columns
            }
        }
        
        await self.update_data("platformq/authz/restricted_columns", data)
        
    async def test_policy(self, policy: str, test_cases: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Test a policy with test cases"""
        results = []
        
        for test_case in test_cases:
            # Create temporary policy
            test_policy_name = f"test_{datetime.utcnow().timestamp()}"
            await self.update_policy(test_policy_name, policy)
            
            try:
                # Evaluate test case
                response = await self._client.post(
                    f"/v1/data/{test_policy_name}",
                    json={"input": test_case["input"]}
                )
                response.raise_for_status()
                
                result = response.json()["result"]
                expected = test_case["expected"]
                
                results.append({
                    "test": test_case.get("name", "unnamed"),
                    "passed": result == expected,
                    "expected": expected,
                    "actual": result
                })
                
            finally:
                # Clean up test policy
                await self._client.delete(f"/v1/policies/{test_policy_name}")
                
        return {
            "total": len(results),
            "passed": sum(1 for r in results if r["passed"]),
            "failed": sum(1 for r in results if not r["passed"]),
            "results": results
        }
        
    def _generate_reason(self, allowed: bool, result: Dict[str, Any]) -> str:
        """Generate human-readable reason for decision"""
        if allowed:
            if "platform_admin" in result.get("input", {}).get("context", {}).get("roles", []):
                return "Allowed: Platform admin override"
            elif result.get("input", {}).get("context", {}).get("service_name"):
                return "Allowed: Service-to-service communication"
            else:
                return "Allowed: User has required permissions"
        else:
            if result.get("data_expired"):
                return "Denied: Data retention period exceeded"
            elif result.get("input", {}).get("context", {}).get("tenant_id") != result.get("resource_tenant_id"):
                return "Denied: Cross-tenant access violation"
            elif not result.get("input", {}).get("context", {}).get("roles"):
                return "Denied: No roles assigned"
            else:
                return "Denied: Insufficient permissions"
                
    async def _log_decision(self, request: AuthzRequest, allowed: bool, 
                          start_time: datetime) -> None:
        """Log authorization decision"""
        duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        decision = {
            "timestamp": datetime.utcnow().isoformat(),
            "subject": request.subject,
            "resource": request.resource,
            "action": request.action,
            "tenant_id": request.context.get("tenant_id"),
            "allowed": allowed,
            "duration_ms": duration_ms
        }
        
        self._decision_log.append(decision)
        
        # Trim log if too large
        if len(self._decision_log) > 10000:
            self._decision_log = self._decision_log[-5000:]
            
        # Send metrics
        await self._send_metrics(decision)
        
    async def _send_metrics(self, decision: Dict[str, Any]) -> None:
        """Send authorization metrics"""
        # In production, send to Prometheus/StatsD
        logger.debug(f"Authorization metric: {decision}")
        
    async def get_decision_log(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Get authorization decision log"""
        if not filters:
            return self._decision_log
            
        filtered = []
        for decision in self._decision_log:
            match = True
            for key, value in filters.items():
                if decision.get(key) != value:
                    match = False
                    break
            if match:
                filtered.append(decision)
                
        return filtered
        
    async def health_check(self) -> Dict[str, Any]:
        """Check OPA health"""
        try:
            response = await self._client.get("/health")
            response.raise_for_status()
            
            return {
                "healthy": True,
                "version": response.headers.get("X-OPA-Version", "unknown"),
                "policies_loaded": len(self._policy_cache),
                "decisions_logged": len(self._decision_log)
            }
            
        except Exception as e:
            return {
                "healthy": False,
                "error": str(e)
            }
            
    async def close(self) -> None:
        """Close OPA client"""
        if self._client:
            await self._client.aclose()
            
        logger.info("OPA client closed") 