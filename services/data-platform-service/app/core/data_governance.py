"""
Data Governance for managing policies, access control, and compliance
"""
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from enum import Enum
import json
from platformq_shared.utils.logger import get_logger
from platformq_shared.cache.ignite_client import IgniteClient
from platformq_shared.errors import ValidationError, AuthorizationError
from platformq_shared.events import EventPublisher

logger = get_logger(__name__)


class PolicyType(Enum):
    ACCESS_CONTROL = "access_control"
    DATA_RETENTION = "data_retention"
    DATA_QUALITY = "data_quality"
    PRIVACY = "privacy"
    SECURITY = "security"


class ComplianceFramework(Enum):
    GDPR = "gdpr"
    CCPA = "ccpa"
    HIPAA = "hipaa"
    SOC2 = "soc2"
    PCI_DSS = "pci_dss"


class DataGovernance:
    """Data governance engine for policy management and compliance"""
    
    def __init__(self):
        self.cache = IgniteClient()
        self.event_publisher = EventPublisher()
        self.policies: Dict[str, Dict] = {}
        self.access_requests: Dict[str, Dict] = {}
        
    async def create_policy(self, policy_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new governance policy"""
        try:
            # Validate policy
            required_fields = ["name", "type", "rules", "scope"]
            for field in required_fields:
                if field not in policy_data:
                    raise ValidationError(f"Missing required field: {field}")
            
            # Generate policy ID
            policy_id = f"policy_{policy_data['name']}_{datetime.utcnow().timestamp()}"
            
            # Create policy document
            policy = {
                "policy_id": policy_id,
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
                "status": "active",
                "version": 1,
                **policy_data
            }
            
            # Store policy
            self.policies[policy_id] = policy
            await self.cache.put(f"governance:policy:{policy_id}", policy)
            
            # Publish event
            await self.event_publisher.publish("governance.policy.created", {
                "policy_id": policy_id,
                "type": policy_data["type"]
            })
            
            logger.info(f"Created policy: {policy_id}")
            return policy
            
        except Exception as e:
            logger.error(f"Failed to create policy: {str(e)}")
            raise
    
    async def evaluate_access(self, 
                            user_id: str, 
                            resource_id: str,
                            action: str) -> Dict[str, Any]:
        """Evaluate access request against policies"""
        try:
            # Get applicable policies
            policies = await self._get_applicable_policies(resource_id, PolicyType.ACCESS_CONTROL)
            
            # Default to deny
            decision = "deny"
            applicable_rules = []
            
            for policy in policies:
                for rule in policy.get("rules", []):
                    if self._evaluate_rule(rule, user_id, resource_id, action):
                        if rule.get("effect") == "allow":
                            decision = "allow"
                        applicable_rules.append(rule)
            
            # Log access decision
            await self._log_access_decision(user_id, resource_id, action, decision)
            
            return {
                "decision": decision,
                "user_id": user_id,
                "resource_id": resource_id,
                "action": action,
                "applicable_rules": applicable_rules,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to evaluate access: {str(e)}")
            return {"decision": "deny", "error": str(e)}
    
    async def request_access(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Submit an access request"""
        try:
            # Validate request
            required_fields = ["user_id", "resource_id", "action", "justification"]
            for field in required_fields:
                if field not in request_data:
                    raise ValidationError(f"Missing required field: {field}")
            
            # Generate request ID
            request_id = f"req_{datetime.utcnow().timestamp()}"
            
            # Create request
            access_request = {
                "request_id": request_id,
                "created_at": datetime.utcnow().isoformat(),
                "status": "pending",
                "expires_at": (datetime.utcnow() + timedelta(days=30)).isoformat(),
                **request_data
            }
            
            # Store request
            self.access_requests[request_id] = access_request
            await self.cache.put(f"governance:request:{request_id}", access_request)
            
            # Publish event for approval workflow
            await self.event_publisher.publish("governance.access.requested", access_request)
            
            return access_request
            
        except Exception as e:
            logger.error(f"Failed to create access request: {str(e)}")
            raise
    
    async def approve_access(self, 
                           request_id: str, 
                           approver_id: str,
                           conditions: Optional[Dict] = None) -> Dict[str, Any]:
        """Approve an access request"""
        try:
            # Get request
            request = self.access_requests.get(request_id)
            if not request:
                cached = await self.cache.get(f"governance:request:{request_id}")
                if not cached:
                    raise ValidationError(f"Request {request_id} not found")
                request = cached
            
            # Update request
            request["status"] = "approved"
            request["approved_by"] = approver_id
            request["approved_at"] = datetime.utcnow().isoformat()
            
            if conditions:
                request["conditions"] = conditions
            
            # Create access grant
            grant = await self._create_access_grant(request)
            
            # Update cache
            await self.cache.put(f"governance:request:{request_id}", request)
            
            # Publish event
            await self.event_publisher.publish("governance.access.approved", {
                "request_id": request_id,
                "grant_id": grant["grant_id"]
            })
            
            return grant
            
        except Exception as e:
            logger.error(f"Failed to approve access: {str(e)}")
            raise
    
    async def check_compliance(self, 
                             resource_id: str,
                             frameworks: Optional[List[str]] = None) -> Dict[str, Any]:
        """Check compliance status for a resource"""
        try:
            if not frameworks:
                frameworks = [f.value for f in ComplianceFramework]
            
            compliance_status = {}
            issues = []
            
            for framework in frameworks:
                # Get framework-specific policies
                policies = await self._get_compliance_policies(framework)
                
                framework_compliant = True
                framework_issues = []
                
                for policy in policies:
                    result = await self._evaluate_compliance_policy(policy, resource_id)
                    if not result["compliant"]:
                        framework_compliant = False
                        framework_issues.extend(result["issues"])
                
                compliance_status[framework] = {
                    "compliant": framework_compliant,
                    "issues": framework_issues
                }
                
                if framework_issues:
                    issues.extend(framework_issues)
            
            return {
                "resource_id": resource_id,
                "overall_compliant": len(issues) == 0,
                "frameworks": compliance_status,
                "total_issues": len(issues),
                "checked_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to check compliance: {str(e)}")
            raise
    
    async def apply_data_privacy(self, 
                               data: Dict[str, Any],
                               privacy_level: str) -> Dict[str, Any]:
        """Apply privacy controls to data"""
        try:
            if privacy_level == "public":
                return data
            
            elif privacy_level == "internal":
                # Mask sensitive fields
                return self._mask_fields(data, ["email", "phone", "ssn"])
            
            elif privacy_level == "confidential":
                # Encrypt sensitive fields
                return self._encrypt_fields(data, ["email", "phone", "ssn", "address"])
            
            elif privacy_level == "restricted":
                # Tokenize all PII
                return self._tokenize_pii(data)
            
            else:
                raise ValidationError(f"Unknown privacy level: {privacy_level}")
                
        except Exception as e:
            logger.error(f"Failed to apply privacy controls: {str(e)}")
            raise
    
    async def get_audit_logs(self, 
                           filters: Optional[Dict] = None,
                           limit: int = 100) -> List[Dict[str, Any]]:
        """Get audit logs for governance activities"""
        # In production, query from audit log storage
        # For now, return mock data
        logs = []
        
        # Add access logs
        for i in range(min(limit, 10)):
            logs.append({
                "event_type": "access_evaluation",
                "timestamp": datetime.utcnow().isoformat(),
                "user_id": f"user_{i}",
                "resource_id": f"resource_{i}",
                "action": "read",
                "decision": "allow" if i % 2 == 0 else "deny"
            })
        
        return logs
    
    def _evaluate_rule(self, rule: Dict, user_id: str, resource_id: str, action: str) -> bool:
        """Evaluate a single access rule"""
        # Check if rule applies to user
        if "users" in rule and user_id not in rule["users"]:
            if "groups" not in rule:  # No group check, so rule doesn't apply
                return False
        
        # Check if rule applies to resource
        if "resources" in rule:
            if resource_id not in rule["resources"] and \
               not any(resource_id.startswith(pattern) for pattern in rule.get("resource_patterns", [])):
                return False
        
        # Check if rule applies to action
        if "actions" in rule and action not in rule["actions"]:
            return False
        
        return True
    
    async def _get_applicable_policies(self, resource_id: str, policy_type: PolicyType) -> List[Dict]:
        """Get policies applicable to a resource"""
        applicable = []
        
        for policy in self.policies.values():
            if policy["type"] != policy_type.value:
                continue
            
            # Check scope
            scope = policy.get("scope", {})
            if "all" in scope or resource_id in scope.get("resources", []):
                applicable.append(policy)
        
        return applicable
    
    async def _log_access_decision(self, user_id: str, resource_id: str, action: str, decision: str):
        """Log access decision for audit"""
        await self.event_publisher.publish("governance.access.evaluated", {
            "user_id": user_id,
            "resource_id": resource_id,
            "action": action,
            "decision": decision,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    async def _create_access_grant(self, request: Dict) -> Dict[str, Any]:
        """Create an access grant from approved request"""
        grant_id = f"grant_{datetime.utcnow().timestamp()}"
        
        grant = {
            "grant_id": grant_id,
            "request_id": request["request_id"],
            "user_id": request["user_id"],
            "resource_id": request["resource_id"],
            "action": request["action"],
            "granted_at": datetime.utcnow().isoformat(),
            "expires_at": request.get("expires_at"),
            "conditions": request.get("conditions", {})
        }
        
        await self.cache.put(f"governance:grant:{grant_id}", grant)
        
        return grant
    
    async def _get_compliance_policies(self, framework: str) -> List[Dict]:
        """Get policies for a compliance framework"""
        return [p for p in self.policies.values() 
                if framework in p.get("compliance_frameworks", [])]
    
    async def _evaluate_compliance_policy(self, policy: Dict, resource_id: str) -> Dict[str, Any]:
        """Evaluate a compliance policy"""
        # Mock evaluation for now
        return {
            "policy_id": policy["policy_id"],
            "compliant": True,
            "issues": []
        }
    
    def _mask_fields(self, data: Dict, fields: List[str]) -> Dict:
        """Mask sensitive fields"""
        masked_data = data.copy()
        for field in fields:
            if field in masked_data:
                masked_data[field] = "***MASKED***"
        return masked_data
    
    def _encrypt_fields(self, data: Dict, fields: List[str]) -> Dict:
        """Encrypt sensitive fields"""
        encrypted_data = data.copy()
        for field in fields:
            if field in encrypted_data:
                # In production, use real encryption
                encrypted_data[field] = f"ENC({data[field]})"
        return encrypted_data
    
    def _tokenize_pii(self, data: Dict) -> Dict:
        """Tokenize PII fields"""
        tokenized_data = {}
        for key, value in data.items():
            if isinstance(value, str) and len(value) > 5:
                # In production, use real tokenization service
                tokenized_data[key] = f"TOK_{hash(value)}"
            else:
                tokenized_data[key] = value
        return tokenized_data 