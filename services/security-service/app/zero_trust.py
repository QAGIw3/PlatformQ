"""
Zero-Trust Policy Engine

Implements Zero-Trust security architecture with continuous verification and adaptive policies.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
import uuid

import hvac
import consul.aio
from platformq_shared.authorization.opa_client import OPAClient
from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


class TrustLevel(Enum):
    """Trust levels for Zero-Trust evaluation"""
    UNTRUSTED = 0
    LOW = 25
    MEDIUM = 50
    HIGH = 75
    VERIFIED = 100


class PolicyAction(Enum):
    """Policy enforcement actions"""
    ALLOW = "allow"
    DENY = "deny"
    CHALLENGE = "challenge"
    STEP_UP = "step_up"
    MONITOR = "monitor"
    QUARANTINE = "quarantine"


class RiskFactor(Enum):
    """Risk factors for trust evaluation"""
    UNKNOWN_DEVICE = "unknown_device"
    UNUSUAL_LOCATION = "unusual_location"
    IMPOSSIBLE_TRAVEL = "impossible_travel"
    UNUSUAL_TIME = "unusual_time"
    FAILED_ATTEMPTS = "failed_attempts"
    SUSPICIOUS_BEHAVIOR = "suspicious_behavior"
    UNPATCHED_DEVICE = "unpatched_device"
    WEAK_AUTHENTICATION = "weak_authentication"
    DATA_EXFILTRATION = "data_exfiltration"
    ANOMALOUS_ACCESS = "anomalous_access"


@dataclass
class SecurityContext:
    """Security context for Zero-Trust evaluation"""
    user_id: str
    device_id: str
    session_id: str
    ip_address: str
    location: Optional[Dict[str, Any]] = None
    authentication_method: Optional[str] = None
    authentication_time: Optional[datetime] = None
    device_trust_score: int = 0
    user_trust_score: int = 0
    risk_factors: List[RiskFactor] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PolicyDecision:
    """Zero-Trust policy decision"""
    action: PolicyAction
    trust_level: TrustLevel
    risk_score: int
    reasons: List[str]
    requirements: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ZeroTrustPolicy:
    """Zero-Trust security policy"""
    id: str
    name: str
    description: str
    resource_pattern: str
    min_trust_level: TrustLevel
    conditions: Dict[str, Any]
    actions: Dict[PolicyAction, Dict[str, Any]]
    risk_weights: Dict[RiskFactor, float]
    enabled: bool = True
    priority: int = 100
    tags: List[str] = field(default_factory=list)


class ZeroTrustPolicyEngine:
    """
    Zero-Trust policy engine for continuous security verification.
    
    Features:
    - Identity verification
    - Device trust evaluation
    - Behavioral analysis
    - Risk scoring
    - Adaptive policies
    - Continuous monitoring
    - Policy enforcement
    """
    
    def __init__(self,
                 opa_client: OPAClient,
                 vault_client: hvac.Client,
                 consul_client: consul.aio.Consul,
                 event_publisher: EventPublisher):
        self.opa = opa_client
        self.vault = vault_client
        self.consul = consul_client
        self.event_publisher = event_publisher
        self._policies: Dict[str, ZeroTrustPolicy] = {}
        self._trust_cache: Dict[str, Tuple[TrustLevel, datetime]] = {}
        self._session_contexts: Dict[str, SecurityContext] = {}
        self._risk_thresholds = {
            TrustLevel.VERIFIED: 10,
            TrustLevel.HIGH: 30,
            TrustLevel.MEDIUM: 50,
            TrustLevel.LOW: 70,
            TrustLevel.UNTRUSTED: 100
        }
        self._monitoring_tasks: Dict[str, asyncio.Task] = {}
        self._active = False
        
    async def initialize(self):
        """Initialize Zero-Trust policy engine"""
        logger.info("Initializing Zero-Trust policy engine")
        
        # Load default policies
        await self._load_default_policies()
        
        # Start monitoring
        asyncio.create_task(self._monitor_sessions())
        asyncio.create_task(self._monitor_trust_levels())
        
        self._active = True
        logger.info("Zero-Trust policy engine initialized")
        
    async def evaluate_access(self,
                            context: SecurityContext,
                            resource: str,
                            action: str) -> PolicyDecision:
        """Evaluate access request against Zero-Trust policies"""
        try:
            # Calculate trust level
            trust_level = await self._calculate_trust_level(context)
            
            # Calculate risk score
            risk_score = await self._calculate_risk_score(context)
            
            # Find applicable policies
            applicable_policies = self._find_applicable_policies(resource)
            
            # Evaluate policies
            decision = await self._evaluate_policies(
                context, resource, action, 
                trust_level, risk_score, 
                applicable_policies
            )
            
            # Log decision
            await self._log_decision(context, resource, action, decision)
            
            # Update session context
            self._session_contexts[context.session_id] = context
            
            # Start monitoring if needed
            if decision.action in [PolicyAction.ALLOW, PolicyAction.MONITOR]:
                await self._start_session_monitoring(context.session_id)
                
            return decision
            
        except Exception as e:
            logger.error(f"Failed to evaluate access: {e}")
            # Fail secure - deny on error
            return PolicyDecision(
                action=PolicyAction.DENY,
                trust_level=TrustLevel.UNTRUSTED,
                risk_score=100,
                reasons=[f"Evaluation error: {str(e)}"]
            )
            
    async def create_policy(self, policy_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new Zero-Trust policy"""
        try:
            policy = ZeroTrustPolicy(
                id=str(uuid.uuid4()),
                name=policy_data["name"],
                description=policy_data["description"],
                resource_pattern=policy_data["resource_pattern"],
                min_trust_level=TrustLevel(policy_data.get("min_trust_level", 50)),
                conditions=policy_data.get("conditions", {}),
                actions=policy_data.get("actions", {}),
                risk_weights=policy_data.get("risk_weights", {}),
                enabled=policy_data.get("enabled", True),
                priority=policy_data.get("priority", 100),
                tags=policy_data.get("tags", [])
            )
            
            # Store policy
            self._policies[policy.id] = policy
            
            # Store in Consul
            await self._store_policy(policy)
            
            # Update OPA
            await self._update_opa_policy(policy)
            
            logger.info(f"Created Zero-Trust policy: {policy.name}")
            return {"id": policy.id, "name": policy.name, "status": "created"}
            
        except Exception as e:
            logger.error(f"Failed to create policy: {e}")
            raise
            
    async def update_policy(self, policy_id: str, updates: Dict[str, Any]):
        """Update existing policy"""
        if policy_id not in self._policies:
            raise ValueError(f"Policy {policy_id} not found")
            
        policy = self._policies[policy_id]
        
        # Update fields
        for key, value in updates.items():
            if hasattr(policy, key):
                setattr(policy, key, value)
                
        # Store updated policy
        await self._store_policy(policy)
        
        # Update OPA
        await self._update_opa_policy(policy)
        
        logger.info(f"Updated policy: {policy.name}")
        
    async def get_policy(self, policy_id: str) -> Optional[Dict[str, Any]]:
        """Get policy by ID"""
        if policy_id in self._policies:
            policy = self._policies[policy_id]
            return {
                "id": policy.id,
                "name": policy.name,
                "description": policy.description,
                "resource_pattern": policy.resource_pattern,
                "min_trust_level": policy.min_trust_level.value,
                "conditions": policy.conditions,
                "actions": policy.actions,
                "risk_weights": policy.risk_weights,
                "enabled": policy.enabled,
                "priority": policy.priority,
                "tags": policy.tags
            }
        return None
        
    async def check_policy_violations(self) -> List[Dict[str, Any]]:
        """Check for policy violations across all sessions"""
        violations = []
        
        for session_id, context in self._session_contexts.items():
            # Re-evaluate trust level
            trust_level = await self._calculate_trust_level(context)
            
            # Check for trust degradation
            if trust_level.value < context.user_trust_score:
                violations.append({
                    "type": "trust_degradation",
                    "session_id": session_id,
                    "user_id": context.user_id,
                    "previous_trust": context.user_trust_score,
                    "current_trust": trust_level.value,
                    "risk_factors": [rf.value for rf in context.risk_factors]
                })
                
            # Check for anomalies
            anomalies = await self._detect_anomalies(context)
            if anomalies:
                violations.append({
                    "type": "anomaly_detected",
                    "session_id": session_id,
                    "user_id": context.user_id,
                    "anomalies": anomalies
                })
                
        return violations
        
    async def enforce_policy(self, violation: Dict[str, Any]):
        """Enforce policy based on violation"""
        try:
            session_id = violation.get("session_id")
            if not session_id or session_id not in self._session_contexts:
                return
                
            context = self._session_contexts[session_id]
            
            # Determine enforcement action
            if violation["type"] == "trust_degradation":
                # Require re-authentication
                await self._require_step_up_auth(context)
                
            elif violation["type"] == "anomaly_detected":
                # Quarantine session
                await self._quarantine_session(context)
                
            # Emit security event
            await self.event_publisher.publish_event(
                "platformq.security.policy-enforced",
                {
                    "violation": violation,
                    "action_taken": "enforcement",
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to enforce policy: {e}")
            
    async def get_status(self) -> Dict[str, Any]:
        """Get Zero-Trust engine status"""
        return {
            "active": self._active,
            "total_policies": len(self._policies),
            "enabled_policies": sum(1 for p in self._policies.values() if p.enabled),
            "active_sessions": len(self._session_contexts),
            "monitoring_tasks": len(self._monitoring_tasks)
        }
        
    async def get_violation_metrics(self) -> Dict[str, Any]:
        """Get policy violation metrics"""
        # This would be implemented with actual metrics storage
        return {
            "total_violations_24h": 0,
            "violations_by_type": {},
            "top_violating_users": [],
            "violation_trend": []
        }
        
    async def load_policies_from_consul(self):
        """Load policies from Consul"""
        try:
            _, policies = await self.consul.kv.get("zero-trust/policies", recurse=True)
            
            if policies:
                for policy_kv in policies:
                    if policy_kv["Key"].endswith("/policy.json"):
                        policy_data = json.loads(policy_kv["Value"])
                        policy = ZeroTrustPolicy(**policy_data)
                        self._policies[policy.id] = policy
                        
                logger.info(f"Loaded {len(self._policies)} Zero-Trust policies from Consul")
                
        except Exception as e:
            logger.error(f"Failed to load policies from Consul: {e}")
            
    async def _calculate_trust_level(self, context: SecurityContext) -> TrustLevel:
        """Calculate trust level based on context"""
        # Check cache
        cache_key = f"{context.user_id}:{context.device_id}"
        if cache_key in self._trust_cache:
            cached_level, cached_time = self._trust_cache[cache_key]
            if datetime.utcnow() - cached_time < timedelta(minutes=5):
                return cached_level
                
        # Calculate base trust from authentication
        base_trust = 50  # Default medium trust
        
        if context.authentication_method == "mfa":
            base_trust += 25
        elif context.authentication_method == "password":
            base_trust += 10
            
        # Adjust for device trust
        base_trust = (base_trust + context.device_trust_score) // 2
        
        # Reduce for risk factors
        for risk_factor in context.risk_factors:
            if risk_factor == RiskFactor.UNKNOWN_DEVICE:
                base_trust -= 20
            elif risk_factor == RiskFactor.UNUSUAL_LOCATION:
                base_trust -= 15
            elif risk_factor == RiskFactor.IMPOSSIBLE_TRAVEL:
                base_trust -= 30
            elif risk_factor == RiskFactor.SUSPICIOUS_BEHAVIOR:
                base_trust -= 25
                
        # Determine trust level
        trust_level = TrustLevel.UNTRUSTED
        for level in reversed(list(TrustLevel)):
            if base_trust >= level.value:
                trust_level = level
                break
                
        # Cache result
        self._trust_cache[cache_key] = (trust_level, datetime.utcnow())
        
        return trust_level
        
    async def _calculate_risk_score(self, context: SecurityContext) -> int:
        """Calculate risk score based on context"""
        risk_score = 0
        
        # Base risk from risk factors
        risk_weights = {
            RiskFactor.UNKNOWN_DEVICE: 20,
            RiskFactor.UNUSUAL_LOCATION: 15,
            RiskFactor.IMPOSSIBLE_TRAVEL: 40,
            RiskFactor.UNUSUAL_TIME: 10,
            RiskFactor.FAILED_ATTEMPTS: 25,
            RiskFactor.SUSPICIOUS_BEHAVIOR: 30,
            RiskFactor.UNPATCHED_DEVICE: 20,
            RiskFactor.WEAK_AUTHENTICATION: 15,
            RiskFactor.DATA_EXFILTRATION: 50,
            RiskFactor.ANOMALOUS_ACCESS: 35
        }
        
        for risk_factor in context.risk_factors:
            risk_score += risk_weights.get(risk_factor, 10)
            
        # Adjust for authentication age
        if context.authentication_time:
            auth_age = datetime.utcnow() - context.authentication_time
            if auth_age > timedelta(hours=8):
                risk_score += 10
            elif auth_age > timedelta(hours=4):
                risk_score += 5
                
        # Cap at 100
        return min(risk_score, 100)
        
    def _find_applicable_policies(self, resource: str) -> List[ZeroTrustPolicy]:
        """Find policies applicable to a resource"""
        applicable = []
        
        for policy in self._policies.values():
            if not policy.enabled:
                continue
                
            # Simple pattern matching - could be enhanced
            if (resource == policy.resource_pattern or 
                resource.startswith(policy.resource_pattern.rstrip("*"))):
                applicable.append(policy)
                
        # Sort by priority
        return sorted(applicable, key=lambda p: p.priority)
        
    async def _evaluate_policies(self,
                               context: SecurityContext,
                               resource: str,
                               action: str,
                               trust_level: TrustLevel,
                               risk_score: int,
                               policies: List[ZeroTrustPolicy]) -> PolicyDecision:
        """Evaluate policies and make decision"""
        # Default deny
        decision = PolicyDecision(
            action=PolicyAction.DENY,
            trust_level=trust_level,
            risk_score=risk_score,
            reasons=["No matching policy allows access"]
        )
        
        for policy in policies:
            # Check minimum trust level
            if trust_level.value < policy.min_trust_level.value:
                decision.reasons.append(
                    f"Policy '{policy.name}' requires trust level {policy.min_trust_level.name}"
                )
                continue
                
            # Evaluate OPA policy if configured
            if "opa_policy" in policy.conditions:
                opa_result = await self._evaluate_opa_policy(
                    policy.conditions["opa_policy"],
                    context, resource, action
                )
                if not opa_result.get("allow", False):
                    decision.reasons.append(
                        f"Policy '{policy.name}' OPA evaluation denied"
                    )
                    continue
                    
            # Check risk threshold
            risk_threshold = policy.conditions.get("max_risk_score", 100)
            if risk_score > risk_threshold:
                decision.reasons.append(
                    f"Policy '{policy.name}' risk score {risk_score} exceeds threshold {risk_threshold}"
                )
                continue
                
            # Policy allows access - determine action
            if risk_score < 30 and trust_level.value >= 75:
                decision.action = PolicyAction.ALLOW
            elif risk_score < 50:
                decision.action = PolicyAction.MONITOR
            elif trust_level.value >= 50:
                decision.action = PolicyAction.STEP_UP
                decision.requirements.append("mfa_required")
            else:
                decision.action = PolicyAction.CHALLENGE
                decision.requirements.extend([
                    "device_verification",
                    "mfa_required",
                    "location_verification"
                ])
                
            decision.reasons = [f"Allowed by policy '{policy.name}'"]
            break
            
        return decision
        
    async def _evaluate_opa_policy(self,
                                 policy_name: str,
                                 context: SecurityContext,
                                 resource: str,
                                 action: str) -> Dict[str, Any]:
        """Evaluate OPA policy"""
        try:
            input_data = {
                "user_id": context.user_id,
                "device_id": context.device_id,
                "session_id": context.session_id,
                "resource": resource,
                "action": action,
                "trust_level": context.user_trust_score,
                "risk_factors": [rf.value for rf in context.risk_factors],
                "attributes": context.attributes
            }
            
            return await self.opa.evaluate_policy(f"zero_trust.{policy_name}", input_data)
            
        except Exception as e:
            logger.error(f"OPA evaluation failed: {e}")
            return {"allow": False}
            
    async def _detect_anomalies(self, context: SecurityContext) -> List[str]:
        """Detect anomalies in user behavior"""
        anomalies = []
        
        # Check for impossible travel
        if await self._check_impossible_travel(context):
            anomalies.append("impossible_travel")
            
        # Check for unusual access patterns
        if await self._check_unusual_access(context):
            anomalies.append("unusual_access_pattern")
            
        # Check for data exfiltration
        if await self._check_data_exfiltration(context):
            anomalies.append("potential_data_exfiltration")
            
        return anomalies
        
    async def _check_impossible_travel(self, context: SecurityContext) -> bool:
        """Check for impossible travel scenarios"""
        # Get last known location from Consul
        last_location_key = f"user-locations/{context.user_id}/last"
        _, last_data = await self.consul.kv.get(last_location_key)
        
        if last_data and context.location:
            last_location = json.loads(last_data["Value"])
            last_time = datetime.fromisoformat(last_location["timestamp"])
            
            # Calculate distance and time
            distance = self._calculate_distance(
                last_location["location"], 
                context.location
            )
            time_diff = datetime.utcnow() - last_time
            
            # Check if travel is possible (assume 900 mph max)
            max_distance = (time_diff.total_seconds() / 3600) * 900
            
            if distance > max_distance:
                return True
                
        # Store current location
        if context.location:
            await self.consul.kv.put(
                last_location_key,
                json.dumps({
                    "location": context.location,
                    "timestamp": datetime.utcnow().isoformat()
                })
            )
            
        return False
        
    async def _check_unusual_access(self, context: SecurityContext) -> bool:
        """Check for unusual access patterns"""
        # This would implement behavioral analysis
        # For now, return False
        return False
        
    async def _check_data_exfiltration(self, context: SecurityContext) -> bool:
        """Check for potential data exfiltration"""
        # This would monitor data access patterns
        # For now, return False
        return False
        
    def _calculate_distance(self, loc1: Dict[str, float], loc2: Dict[str, float]) -> float:
        """Calculate distance between two locations (simplified)"""
        # Haversine formula would be used in production
        import math
        
        lat1, lon1 = loc1.get("latitude", 0), loc1.get("longitude", 0)
        lat2, lon2 = loc2.get("latitude", 0), loc2.get("longitude", 0)
        
        # Simplified distance calculation
        return math.sqrt((lat2 - lat1)**2 + (lon2 - lon1)**2) * 69  # miles
        
    async def _require_step_up_auth(self, context: SecurityContext):
        """Require step-up authentication"""
        await self.event_publisher.publish_event(
            "platformq.security.step-up-required",
            {
                "user_id": context.user_id,
                "session_id": context.session_id,
                "reason": "trust_degradation"
            }
        )
        
    async def _quarantine_session(self, context: SecurityContext):
        """Quarantine a suspicious session"""
        await self.event_publisher.publish_event(
            "platformq.security.session-quarantined",
            {
                "user_id": context.user_id,
                "session_id": context.session_id,
                "reason": "anomaly_detected"
            }
        )
        
        # Remove from active sessions
        if context.session_id in self._session_contexts:
            del self._session_contexts[context.session_id]
            
    async def _log_decision(self,
                          context: SecurityContext,
                          resource: str,
                          action: str,
                          decision: PolicyDecision):
        """Log policy decision for audit"""
        await self.event_publisher.publish_event(
            "platformq.security.policy-decision",
            {
                "user_id": context.user_id,
                "session_id": context.session_id,
                "resource": resource,
                "action": action,
                "decision": decision.action.value,
                "trust_level": decision.trust_level.value,
                "risk_score": decision.risk_score,
                "reasons": decision.reasons,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
    async def _start_session_monitoring(self, session_id: str):
        """Start monitoring a session"""
        if session_id in self._monitoring_tasks:
            return
            
        async def monitor():
            while session_id in self._session_contexts:
                try:
                    context = self._session_contexts[session_id]
                    
                    # Re-evaluate trust periodically
                    trust_level = await self._calculate_trust_level(context)
                    risk_score = await self._calculate_risk_score(context)
                    
                    # Check for significant changes
                    if (abs(trust_level.value - context.user_trust_score) > 20 or
                        risk_score > 70):
                        await self.event_publisher.publish_event(
                            "platformq.security.trust-change",
                            {
                                "session_id": session_id,
                                "user_id": context.user_id,
                                "old_trust": context.user_trust_score,
                                "new_trust": trust_level.value,
                                "risk_score": risk_score
                            }
                        )
                        
                    await asyncio.sleep(60)  # Check every minute
                    
                except Exception as e:
                    logger.error(f"Session monitoring error: {e}")
                    break
                    
        self._monitoring_tasks[session_id] = asyncio.create_task(monitor())
        
    async def _monitor_sessions(self):
        """Monitor all active sessions"""
        while self._active:
            try:
                # Clean up expired sessions
                expired = []
                for session_id, context in self._session_contexts.items():
                    if context.authentication_time:
                        age = datetime.utcnow() - context.authentication_time
                        if age > timedelta(hours=24):
                            expired.append(session_id)
                            
                for session_id in expired:
                    del self._session_contexts[session_id]
                    if session_id in self._monitoring_tasks:
                        self._monitoring_tasks[session_id].cancel()
                        del self._monitoring_tasks[session_id]
                        
            except Exception as e:
                logger.error(f"Session monitor error: {e}")
                
            await asyncio.sleep(300)  # Every 5 minutes
            
    async def _monitor_trust_levels(self):
        """Monitor and update trust levels"""
        while self._active:
            try:
                # Clear old cache entries
                now = datetime.utcnow()
                expired_keys = [
                    key for key, (_, time) in self._trust_cache.items()
                    if now - time > timedelta(minutes=30)
                ]
                
                for key in expired_keys:
                    del self._trust_cache[key]
                    
            except Exception as e:
                logger.error(f"Trust monitor error: {e}")
                
            await asyncio.sleep(600)  # Every 10 minutes
            
    async def _load_default_policies(self):
        """Load default Zero-Trust policies"""
        default_policies = [
            {
                "name": "admin_access",
                "description": "Admin access requires high trust",
                "resource_pattern": "/admin/*",
                "min_trust_level": 75,
                "conditions": {
                    "max_risk_score": 30,
                    "require_mfa": True
                },
                "actions": {},
                "risk_weights": {},
                "priority": 10
            },
            {
                "name": "sensitive_data",
                "description": "Sensitive data access policy",
                "resource_pattern": "/api/*/sensitive/*",
                "min_trust_level": 50,
                "conditions": {
                    "max_risk_score": 50,
                    "require_encryption": True
                },
                "actions": {},
                "risk_weights": {},
                "priority": 20
            },
            {
                "name": "public_read",
                "description": "Public read access",
                "resource_pattern": "/api/public/*",
                "min_trust_level": 0,
                "conditions": {
                    "max_risk_score": 80
                },
                "actions": {},
                "risk_weights": {},
                "priority": 100
            }
        ]
        
        for policy_data in default_policies:
            await self.create_policy(policy_data)
            
    async def _store_policy(self, policy: ZeroTrustPolicy):
        """Store policy in Consul"""
        try:
            policy_data = {
                "id": policy.id,
                "name": policy.name,
                "description": policy.description,
                "resource_pattern": policy.resource_pattern,
                "min_trust_level": policy.min_trust_level.value,
                "conditions": policy.conditions,
                "actions": {k.value: v for k, v in policy.actions.items()},
                "risk_weights": {k.value: v for k, v in policy.risk_weights.items()},
                "enabled": policy.enabled,
                "priority": policy.priority,
                "tags": policy.tags
            }
            
            await self.consul.kv.put(
                f"zero-trust/policies/{policy.id}/policy.json",
                json.dumps(policy_data)
            )
            
        except Exception as e:
            logger.error(f"Failed to store policy: {e}")
            
    async def _update_opa_policy(self, policy: ZeroTrustPolicy):
        """Update OPA with Zero-Trust policy"""
        try:
            # Generate OPA policy from Zero-Trust policy
            opa_policy = f"""
            package zero_trust.{policy.id.replace('-', '_')}
            
            default allow = false
            
            allow {{
                input.trust_level >= {policy.min_trust_level.value}
                input.risk_score <= {policy.conditions.get('max_risk_score', 100)}
            }}
            """
            
            await self.opa.update_policy(f"zero_trust_{policy.id}", opa_policy)
            
        except Exception as e:
            logger.error(f"Failed to update OPA policy: {e}")
            
    async def shutdown(self):
        """Shutdown Zero-Trust engine"""
        logger.info("Shutting down Zero-Trust policy engine")
        
        self._active = False
        
        # Cancel monitoring tasks
        for task in self._monitoring_tasks.values():
            task.cancel()
            
        logger.info("Zero-Trust policy engine shutdown complete") 