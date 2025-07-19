"""
Policy Engine Module

Handles policy evaluation for authorization decisions.
Designed to integrate with Open Policy Agent (OPA) when needed.
"""

import logging
from typing import Dict, Any, Optional
import httpx
from fastapi import HTTPException

logger = logging.getLogger(__name__)


class PolicyEngine:
    """
    Policy engine for evaluating access control policies.
    Currently implements RBAC, with plans for OPA integration.
    """
    
    def __init__(self, opa_url: Optional[str] = None):
        """
        Initialize the policy engine.
        
        Args:
            opa_url: URL to OPA server (for future integration)
        """
        self.opa_url = opa_url
        self.opa_client = httpx.AsyncClient() if opa_url else None
        
        # Default policies for RBAC
        self.rbac_policies = {
            "read_asset": ["admin", "member", "viewer"],
            "write_asset": ["admin", "member"],
            "delete_asset": ["admin"],
            "create_asset": ["admin", "member"],
            "manage_users": ["admin"],
            "manage_tenant": ["admin"],
            "view_analytics": ["admin", "member", "viewer"],
            "manage_policies": ["admin"]
        }
        
    async def evaluate(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate a policy decision.
        
        Args:
            input_data: Dictionary containing:
                - subject: Information about the requester (user_id, roles, etc.)
                - action: The action being attempted
                - resource: The resource being accessed
                - context: Additional context (tenant_id, etc.)
                
        Returns:
            Dictionary with:
                - allow: Boolean indicating if action is allowed
                - reason: String explaining the decision
        """
        action = input_data.get("action")
        subject = input_data.get("subject", {})
        resource = input_data.get("resource", {})
        context = input_data.get("context", {})
        
        # Extract user roles
        user_roles = subject.get("roles", [])
        
        # If OPA is configured, delegate to OPA
        if self.opa_client and self.opa_url:
            return await self._evaluate_with_opa(input_data)
        
        # Otherwise, use built-in RBAC
        return self._evaluate_rbac(action, user_roles, subject, resource, context)
        
    def _evaluate_rbac(self, 
                      action: str, 
                      user_roles: list, 
                      subject: Dict[str, Any],
                      resource: Dict[str, Any],
                      context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate using Role-Based Access Control.
        """
        # Check if action has required roles defined
        required_roles = self.rbac_policies.get(action, [])
        
        # Admin can do anything
        if "admin" in user_roles:
            return {
                "allow": True,
                "reason": "Admin role has full access"
            }
        
        # Check resource ownership for certain actions
        if action in ["read_asset", "write_asset", "delete_asset"]:
            resource_owner = resource.get("owner_id")
            user_id = subject.get("user_id")
            
            # Owner can always access their own resources
            if resource_owner and user_id and str(resource_owner) == str(user_id):
                return {
                    "allow": True,
                    "reason": "User owns the resource"
                }
        
        # Check if user has any of the required roles
        if any(role in user_roles for role in required_roles):
            return {
                "allow": True,
                "reason": f"User has required role for action '{action}'"
            }
        
        # Default deny
        return {
            "allow": False,
            "reason": f"User lacks required roles for action '{action}'. Required: {required_roles}, User has: {user_roles}"
        }
        
    async def _evaluate_with_opa(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Evaluate policy using OPA server.
        """
        try:
            # OPA expects input wrapped in an "input" key
            opa_input = {"input": input_data}
            
            response = await self.opa_client.post(
                f"{self.opa_url}/v1/data/platformq/authz/allow",
                json=opa_input
            )
            
            if response.status_code != 200:
                logger.error(f"OPA returned status {response.status_code}: {response.text}")
                # Fall back to RBAC
                return self._evaluate_rbac(
                    input_data.get("action"),
                    input_data.get("subject", {}).get("roles", []),
                    input_data.get("subject", {}),
                    input_data.get("resource", {}),
                    input_data.get("context", {})
                )
            
            result = response.json()
            allow = result.get("result", False)
            
            return {
                "allow": allow,
                "reason": "Decision made by OPA" if allow else "Denied by OPA policy"
            }
            
        except Exception as e:
            logger.error(f"Error calling OPA: {e}")
            # Fall back to RBAC
            return self._evaluate_rbac(
                input_data.get("action"),
                input_data.get("subject", {}).get("roles", []),
                input_data.get("subject", {}),
                input_data.get("resource", {}),
                input_data.get("context", {})
            )
            
    async def add_policy(self, policy_name: str, policy_content: str) -> bool:
        """
        Add or update a policy in OPA.
        
        Args:
            policy_name: Name of the policy
            policy_content: Rego policy content
            
        Returns:
            True if successful, False otherwise
        """
        if not self.opa_client:
            logger.warning("OPA not configured, cannot add policy")
            return False
            
        try:
            response = await self.opa_client.put(
                f"{self.opa_url}/v1/policies/{policy_name}",
                content=policy_content,
                headers={"Content-Type": "text/plain"}
            )
            
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"Error adding policy to OPA: {e}")
            return False
            
    async def close(self):
        """Clean up resources."""
        if self.opa_client:
            await self.opa_client.aclose()


# Global policy engine instance
_policy_engine: Optional[PolicyEngine] = None


async def get_policy_engine() -> PolicyEngine:
    """Get or create policy engine instance."""
    global _policy_engine
    
    if _policy_engine is None:
        # Get OPA URL from environment or config
        opa_url = None  # Will be configured when OPA is deployed
        _policy_engine = PolicyEngine(opa_url=opa_url)
        
    return _policy_engine 