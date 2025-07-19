"""
Policy-related schemas for API requests and responses.
"""

from pydantic import BaseModel
from typing import Dict, Any, Optional


class PolicySubject(BaseModel):
    """Information about the subject making the request"""
    user_id: str
    roles: list[str] = []
    tenant_id: Optional[str] = None
    groups: list[str] = []
    attributes: Dict[str, Any] = {}


class PolicyResource(BaseModel):
    """Information about the resource being accessed"""
    resource_id: Optional[str] = None
    resource_type: Optional[str] = None
    owner_id: Optional[str] = None
    tenant_id: Optional[str] = None
    attributes: Dict[str, Any] = {}


class PolicyContext(BaseModel):
    """Additional context for policy evaluation"""
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    request_time: Optional[str] = None
    environment: Optional[str] = None
    attributes: Dict[str, Any] = {}


class PolicyEvaluationRequest(BaseModel):
    """Request for policy evaluation"""
    subject: Dict[str, Any]  # Flexible to support existing format
    action: str
    resource: Dict[str, Any]  # Flexible to support existing format
    context: Optional[Dict[str, Any]] = {}


class PolicyEvaluationResponse(BaseModel):
    """Response from policy evaluation"""
    allow: bool
    reason: Optional[str] = None
    
    
class PolicyDefinition(BaseModel):
    """Definition of a policy"""
    name: str
    description: Optional[str] = None
    content: str  # Rego policy content for OPA
    
    
class PolicyUpdateRequest(BaseModel):
    """Request to update a policy"""
    name: str
    content: str 