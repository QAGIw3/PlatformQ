"""
Security Monitoring Service

Real-time security monitoring, threat detection, and incident response.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
from collections import defaultdict, deque

import hvac
import consul.aio
from platformq_shared.authorization.opa_client import OPAClient
from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


class ThreatLevel(Enum):
    """Threat severity levels"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class EventType(Enum):
    """Security event types"""
    AUTHENTICATION_FAILED = "auth_failed"
    AUTHENTICATION_SUCCESS = "auth_success"
    AUTHORIZATION_DENIED = "authz_denied"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"
    DATA_EXFILTRATION = "data_exfiltration"
    PRIVILEGE_ESCALATION = "privilege_escalation"
    POLICY_VIOLATION = "policy_violation"
    CERTIFICATE_EXPIRY = "cert_expiry"
    SECRET_ACCESS = "secret_access"
    ANOMALY_DETECTED = "anomaly_detected"
    BRUTE_FORCE = "brute_force"
    IMPOSSIBLE_TRAVEL = "impossible_travel"
    SERVICE_COMPROMISE = "service_compromise"


@dataclass
class SecurityEvent:
    """Security event data"""
    id: str
    timestamp: datetime
    event_type: EventType
    threat_level: ThreatLevel
    source: Dict[str, Any]
    target: Dict[str, Any]
    details: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ThreatIndicator:
    """Threat indicator"""
    indicator_type: str
    value: str
    threat_level: ThreatLevel
    confidence: float
    source: str
    last_seen: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SecurityIncident:
    """Security incident"""
    id: str
    start_time: datetime
    end_time: Optional[datetime]
    threat_level: ThreatLevel
    affected_resources: List[str]
    events: List[SecurityEvent]
    status: str = "active"
    response_actions: List[Dict[str, Any]] = field(default_factory=list)


class SecurityMonitor:
    """
    Security monitoring and threat detection service.
    
    Features:
    - Real-time event monitoring
    - Threat detection
    - Anomaly detection
    - Incident correlation
    - Automated response
    - Threat intelligence
    """
    
    def __init__(self,
                 vault_client: hvac.Client,
                 consul_client: consul.aio.Consul,
                 opa_client: OPAClient,
                 event_publisher: EventPublisher,
                 zero_trust_engine=None):
        self.vault = vault_client
        self.consul = consul_client
        self.opa = opa_client
        self.event_publisher = event_publisher
        self.zero_trust_engine = zero_trust_engine
        
        # Event storage
        self._events: deque = deque(maxlen=10000)
        self._incidents: Dict[str, SecurityIncident] = {}
        self._threat_indicators: Dict[str, ThreatIndicator] = {}
        
        # Detection state
        self._user_activity: Dict[str, List[SecurityEvent]] = defaultdict(list)
        self._service_activity: Dict[str, List[SecurityEvent]] = defaultdict(list)
        self._ip_activity: Dict[str, List[SecurityEvent]] = defaultdict(list)
        
        # Thresholds and rules
        self._thresholds = {
            "failed_auth_attempts": 5,
            "failed_auth_window": 300,  # 5 minutes
            "data_access_rate": 100,  # requests per minute
            "privilege_escalation_attempts": 3,
            "anomaly_score_threshold": 0.8
        }
        
        # Monitoring tasks
        self._monitoring_tasks: List[asyncio.Task] = []
        self.is_running = False
        
    async def start(self):
        """Start security monitoring"""
        logger.info("Starting security monitor")
        
        # Load threat indicators
        await self._load_threat_indicators()
        
        # Load detection rules
        await self._load_detection_rules()
        
        # Start monitoring tasks
        self._monitoring_tasks = [
            asyncio.create_task(self._monitor_events()),
            asyncio.create_task(self._detect_threats()),
            asyncio.create_task(self._correlate_incidents()),
            asyncio.create_task(self._monitor_metrics())
        ]
        
        # Subscribe to security events
        await self._subscribe_to_events()
        
        self.is_running = True
        logger.info("Security monitor started")
        
    async def stop(self):
        """Stop security monitoring"""
        logger.info("Stopping security monitor")
        
        self.is_running = False
        
        # Cancel monitoring tasks
        for task in self._monitoring_tasks:
            task.cancel()
            
        logger.info("Security monitor stopped")
        
    async def process_event(self, event_data: Dict[str, Any]):
        """Process a security event"""
        try:
            # Create event object
            event = SecurityEvent(
                id=event_data.get("id", str(uuid.uuid4())),
                timestamp=datetime.fromisoformat(event_data["timestamp"]),
                event_type=EventType(event_data["event_type"]),
                threat_level=ThreatLevel(event_data.get("threat_level", "info")),
                source=event_data.get("source", {}),
                target=event_data.get("target", {}),
                details=event_data.get("details", {}),
                metadata=event_data.get("metadata", {})
            )
            
            # Store event
            self._events.append(event)
            
            # Update activity tracking
            if "user_id" in event.source:
                self._user_activity[event.source["user_id"]].append(event)
                
            if "service" in event.source:
                self._service_activity[event.source["service"]].append(event)
                
            if "ip_address" in event.source:
                self._ip_activity[event.source["ip_address"]].append(event)
                
            # Check for immediate threats
            await self._check_immediate_threats(event)
            
            # Store in Consul for persistence
            await self._store_event(event)
            
        except Exception as e:
            logger.error(f"Failed to process event: {e}")
            
    async def get_events(self,
                        start_time: Optional[datetime] = None,
                        end_time: Optional[datetime] = None,
                        event_type: Optional[str] = None,
                        threat_level: Optional[str] = None,
                        user_id: Optional[str] = None,
                        limit: int = 100) -> List[SecurityEvent]:
        """Get security events with filters"""
        events = list(self._events)
        
        # Apply filters
        if start_time:
            events = [e for e in events if e.timestamp >= start_time]
            
        if end_time:
            events = [e for e in events if e.timestamp <= end_time]
            
        if event_type:
            events = [e for e in events if e.event_type.value == event_type]
            
        if threat_level:
            events = [e for e in events if e.threat_level.value == threat_level]
            
        if user_id:
            events = [e for e in events if e.source.get("user_id") == user_id]
            
        # Sort by timestamp descending
        events.sort(key=lambda e: e.timestamp, reverse=True)
        
        return events[:limit]
        
    async def get_active_incidents(self) -> List[SecurityIncident]:
        """Get active security incidents"""
        return [
            incident for incident in self._incidents.values()
            if incident.status == "active"
        ]
        
    async def get_threat_indicators(self,
                                  indicator_type: Optional[str] = None,
                                  threat_level: Optional[str] = None) -> List[ThreatIndicator]:
        """Get threat indicators"""
        indicators = list(self._threat_indicators.values())
        
        if indicator_type:
            indicators = [i for i in indicators if i.indicator_type == indicator_type]
            
        if threat_level:
            indicators = [i for i in indicators if i.threat_level.value == threat_level]
            
        return indicators
        
    async def add_threat_indicator(self, indicator: ThreatIndicator):
        """Add a threat indicator"""
        key = f"{indicator.indicator_type}:{indicator.value}"
        self._threat_indicators[key] = indicator
        
        # Store in Consul
        await self._store_threat_indicator(indicator)
        
        logger.info(f"Added threat indicator: {key}")
        
    async def _check_immediate_threats(self, event: SecurityEvent):
        """Check for immediate threats requiring action"""
        # Check for brute force
        if event.event_type == EventType.AUTHENTICATION_FAILED:
            user_id = event.source.get("user_id")
            if user_id:
                await self._check_brute_force(user_id)
                
        # Check for data exfiltration
        elif event.event_type == EventType.DATA_EXFILTRATION:
            await self._handle_data_exfiltration(event)
            
        # Check for privilege escalation
        elif event.event_type == EventType.PRIVILEGE_ESCALATION:
            await self._handle_privilege_escalation(event)
            
        # Check against threat indicators
        await self._check_threat_indicators(event)
        
    async def _check_brute_force(self, user_id: str):
        """Check for brute force attacks"""
        recent_events = [
            e for e in self._user_activity[user_id]
            if e.event_type == EventType.AUTHENTICATION_FAILED and
            datetime.utcnow() - e.timestamp < timedelta(seconds=self._thresholds["failed_auth_window"])
        ]
        
        if len(recent_events) >= self._thresholds["failed_auth_attempts"]:
            # Create incident
            incident = SecurityIncident(
                id=str(uuid.uuid4()),
                start_time=recent_events[0].timestamp,
                end_time=None,
                threat_level=ThreatLevel.HIGH,
                affected_resources=[f"user:{user_id}"],
                events=recent_events
            )
            
            self._incidents[incident.id] = incident
            
            # Take action
            await self._respond_to_incident(incident)
            
    async def _handle_data_exfiltration(self, event: SecurityEvent):
        """Handle potential data exfiltration"""
        # Create high priority incident
        incident = SecurityIncident(
            id=str(uuid.uuid4()),
            start_time=event.timestamp,
            end_time=None,
            threat_level=ThreatLevel.CRITICAL,
            affected_resources=event.details.get("affected_resources", []),
            events=[event]
        )
        
        self._incidents[incident.id] = incident
        
        # Immediate response
        await self._respond_to_incident(incident)
        
    async def _handle_privilege_escalation(self, event: SecurityEvent):
        """Handle privilege escalation attempts"""
        user_id = event.source.get("user_id")
        if not user_id:
            return
            
        # Check recent attempts
        recent_attempts = [
            e for e in self._user_activity[user_id]
            if e.event_type == EventType.PRIVILEGE_ESCALATION and
            datetime.utcnow() - e.timestamp < timedelta(hours=1)
        ]
        
        if len(recent_attempts) >= self._thresholds["privilege_escalation_attempts"]:
            # Create incident
            incident = SecurityIncident(
                id=str(uuid.uuid4()),
                start_time=recent_attempts[0].timestamp,
                end_time=None,
                threat_level=ThreatLevel.CRITICAL,
                affected_resources=[f"user:{user_id}"],
                events=recent_attempts
            )
            
            self._incidents[incident.id] = incident
            
            # Take action
            await self._respond_to_incident(incident)
            
    async def _check_threat_indicators(self, event: SecurityEvent):
        """Check event against threat indicators"""
        # Check IP addresses
        if "ip_address" in event.source:
            indicator_key = f"ip:{event.source['ip_address']}"
            if indicator_key in self._threat_indicators:
                indicator = self._threat_indicators[indicator_key]
                if indicator.threat_level.value in ["critical", "high"]:
                    # Create incident
                    await self._create_indicator_incident(event, indicator)
                    
        # Check user patterns
        if "user_id" in event.source:
            # Check for compromised account indicators
            await self._check_user_indicators(event)
            
    async def _create_indicator_incident(self, event: SecurityEvent, indicator: ThreatIndicator):
        """Create incident from threat indicator match"""
        incident = SecurityIncident(
            id=str(uuid.uuid4()),
            start_time=event.timestamp,
            end_time=None,
            threat_level=indicator.threat_level,
            affected_resources=[f"{k}:{v}" for k, v in event.source.items()],
            events=[event]
        )
        
        incident.metadata["threat_indicator"] = {
            "type": indicator.indicator_type,
            "value": indicator.value,
            "confidence": indicator.confidence
        }
        
        self._incidents[incident.id] = incident
        
        await self._respond_to_incident(incident)
        
    async def _check_user_indicators(self, event: SecurityEvent):
        """Check for user-specific threat indicators"""
        user_id = event.source["user_id"]
        
        # Analyze user behavior
        user_events = self._user_activity[user_id]
        
        # Check for anomalies
        anomaly_score = await self._calculate_anomaly_score(user_events)
        
        if anomaly_score > self._thresholds["anomaly_score_threshold"]:
            # Create anomaly event
            anomaly_event = SecurityEvent(
                id=str(uuid.uuid4()),
                timestamp=datetime.utcnow(),
                event_type=EventType.ANOMALY_DETECTED,
                threat_level=ThreatLevel.MEDIUM,
                source={"user_id": user_id},
                target={},
                details={
                    "anomaly_score": anomaly_score,
                    "contributing_events": len(user_events)
                }
            )
            
            await self.process_event(anomaly_event.__dict__)
            
    async def _calculate_anomaly_score(self, events: List[SecurityEvent]) -> float:
        """Calculate anomaly score for a set of events"""
        if not events:
            return 0.0
            
        # Simple anomaly scoring based on:
        # - Unusual time patterns
        # - Unusual access patterns
        # - Failed attempts
        # - Rapid activity
        
        score = 0.0
        
        # Check time patterns
        hour_counts = defaultdict(int)
        for event in events:
            hour_counts[event.timestamp.hour] += 1
            
        # Unusual hours (0-6 AM)
        unusual_hour_activity = sum(
            count for hour, count in hour_counts.items()
            if hour in range(0, 6)
        )
        if unusual_hour_activity > len(events) * 0.3:
            score += 0.3
            
        # Failed attempts ratio
        failed_attempts = sum(
            1 for e in events 
            if e.event_type in [EventType.AUTHENTICATION_FAILED, EventType.AUTHORIZATION_DENIED]
        )
        if failed_attempts > len(events) * 0.5:
            score += 0.4
            
        # Rapid activity
        if len(events) > 2:
            time_diffs = []
            for i in range(1, len(events)):
                diff = (events[i].timestamp - events[i-1].timestamp).total_seconds()
                time_diffs.append(diff)
                
            avg_diff = sum(time_diffs) / len(time_diffs)
            if avg_diff < 5:  # Less than 5 seconds between events
                score += 0.3
                
        return min(score, 1.0)
        
    async def _respond_to_incident(self, incident: SecurityIncident):
        """Respond to security incident"""
        logger.warning(f"Responding to incident {incident.id}: {incident.threat_level.value}")
        
        response_actions = []
        
        # Determine response based on threat level
        if incident.threat_level == ThreatLevel.CRITICAL:
            # Immediate containment
            for resource in incident.affected_resources:
                if resource.startswith("user:"):
                    user_id = resource.split(":")[1]
                    # Suspend user
                    await self._suspend_user(user_id)
                    response_actions.append({
                        "action": "suspend_user",
                        "target": user_id,
                        "timestamp": datetime.utcnow().isoformat()
                    })
                    
                elif resource.startswith("service:"):
                    service = resource.split(":")[1]
                    # Isolate service
                    await self._isolate_service(service)
                    response_actions.append({
                        "action": "isolate_service",
                        "target": service,
                        "timestamp": datetime.utcnow().isoformat()
                    })
                    
        elif incident.threat_level == ThreatLevel.HIGH:
            # Enforce step-up authentication
            for resource in incident.affected_resources:
                if resource.startswith("user:"):
                    user_id = resource.split(":")[1]
                    await self._require_mfa(user_id)
                    response_actions.append({
                        "action": "require_mfa",
                        "target": user_id,
                        "timestamp": datetime.utcnow().isoformat()
                    })
                    
        # Update incident
        incident.response_actions = response_actions
        
        # Notify
        await self.event_publisher.publish_event(
            "platformq.security.incident-created",
            {
                "incident_id": incident.id,
                "threat_level": incident.threat_level.value,
                "affected_resources": incident.affected_resources,
                "response_actions": response_actions
            }
        )
        
    async def _suspend_user(self, user_id: str):
        """Suspend user account"""
        try:
            # Update user status in Vault
            self.vault.write(
                f"secret/suspended-users/{user_id}",
                suspended_at=datetime.utcnow().isoformat(),
                reason="security_incident"
            )
            
            # Revoke all sessions
            await self.event_publisher.publish_event(
                "platformq.security.revoke-user-sessions",
                {"user_id": user_id}
            )
            
            logger.info(f"Suspended user {user_id}")
            
        except Exception as e:
            logger.error(f"Failed to suspend user {user_id}: {e}")
            
    async def _isolate_service(self, service_name: str):
        """Isolate a service from the mesh"""
        try:
            # Update Consul service mesh intention to deny all traffic
            await self.consul.connect.intentions.create({
                "SourceName": "*",
                "DestinationName": service_name,
                "Action": "deny",
                "Description": f"Security incident isolation - {datetime.utcnow().isoformat()}",
                "Meta": {
                    "security_incident": "true",
                    "isolated_at": datetime.utcnow().isoformat()
                }
            })
            
            logger.info(f"Isolated service {service_name}")
            
        except Exception as e:
            logger.error(f"Failed to isolate service {service_name}: {e}")
            
    async def _require_mfa(self, user_id: str):
        """Require MFA for user"""
        try:
            # Update user security requirements
            await self.consul.kv.put(
                f"security/user-requirements/{user_id}",
                json.dumps({
                    "mfa_required": True,
                    "reason": "security_incident",
                    "enforced_at": datetime.utcnow().isoformat()
                })
            )
            
            logger.info(f"Enforced MFA for user {user_id}")
            
        except Exception as e:
            logger.error(f"Failed to enforce MFA for user {user_id}: {e}")
            
    async def _monitor_events(self):
        """Monitor events for patterns"""
        while self.is_running:
            try:
                # Clean old events from memory
                cutoff = datetime.utcnow() - timedelta(hours=24)
                
                # Clean user activity
                for user_id in list(self._user_activity.keys()):
                    self._user_activity[user_id] = [
                        e for e in self._user_activity[user_id]
                        if e.timestamp > cutoff
                    ]
                    if not self._user_activity[user_id]:
                        del self._user_activity[user_id]
                        
                # Similar cleanup for service and IP activity
                
                await asyncio.sleep(300)  # Every 5 minutes
                
            except Exception as e:
                logger.error(f"Event monitor error: {e}")
                await asyncio.sleep(60)
                
    async def _detect_threats(self):
        """Detect threats from event patterns"""
        while self.is_running:
            try:
                # Run detection rules
                await self._run_detection_rules()
                
                # Check for anomalies
                await self._detect_anomalies()
                
                # Update threat indicators
                await self._update_threat_indicators()
                
                await asyncio.sleep(60)  # Every minute
                
            except Exception as e:
                logger.error(f"Threat detection error: {e}")
                await asyncio.sleep(60)
                
    async def _correlate_incidents(self):
        """Correlate events into incidents"""
        while self.is_running:
            try:
                # Look for related events
                for incident in self._incidents.values():
                    if incident.status == "active":
                        # Find related events
                        related = await self._find_related_events(incident)
                        
                        # Add to incident
                        for event in related:
                            if event not in incident.events:
                                incident.events.append(event)
                                
                # Check for incident resolution
                for incident_id, incident in list(self._incidents.items()):
                    if incident.status == "active":
                        # Check if threat is resolved
                        if await self._is_incident_resolved(incident):
                            incident.status = "resolved"
                            incident.end_time = datetime.utcnow()
                            
                            await self.event_publisher.publish_event(
                                "platformq.security.incident-resolved",
                                {"incident_id": incident_id}
                            )
                            
                await asyncio.sleep(300)  # Every 5 minutes
                
            except Exception as e:
                logger.error(f"Incident correlation error: {e}")
                await asyncio.sleep(60)
                
    async def _monitor_metrics(self):
        """Monitor security metrics"""
        while self.is_running:
            try:
                metrics = {
                    "total_events_24h": len([
                        e for e in self._events
                        if datetime.utcnow() - e.timestamp < timedelta(hours=24)
                    ]),
                    "active_incidents": len([
                        i for i in self._incidents.values()
                        if i.status == "active"
                    ]),
                    "threat_indicators": len(self._threat_indicators),
                    "monitored_users": len(self._user_activity),
                    "monitored_services": len(self._service_activity),
                    "monitored_ips": len(self._ip_activity)
                }
                
                # Store metrics
                await self.consul.kv.put(
                    "security/metrics/current",
                    json.dumps({
                        "timestamp": datetime.utcnow().isoformat(),
                        "metrics": metrics
                    })
                )
                
                await asyncio.sleep(60)  # Every minute
                
            except Exception as e:
                logger.error(f"Metrics monitor error: {e}")
                await asyncio.sleep(60)
                
    async def _load_threat_indicators(self):
        """Load threat indicators from Consul"""
        try:
            _, indicators = await self.consul.kv.get("security/threat-indicators", recurse=True)
            
            if indicators:
                for indicator_kv in indicators:
                    if indicator_kv["Value"]:
                        indicator_data = json.loads(indicator_kv["Value"])
                        indicator = ThreatIndicator(
                            indicator_type=indicator_data["indicator_type"],
                            value=indicator_data["value"],
                            threat_level=ThreatLevel(indicator_data["threat_level"]),
                            confidence=indicator_data["confidence"],
                            source=indicator_data["source"],
                            last_seen=datetime.fromisoformat(indicator_data["last_seen"]),
                            metadata=indicator_data.get("metadata", {})
                        )
                        
                        key = f"{indicator.indicator_type}:{indicator.value}"
                        self._threat_indicators[key] = indicator
                        
            logger.info(f"Loaded {len(self._threat_indicators)} threat indicators")
            
        except Exception as e:
            logger.error(f"Failed to load threat indicators: {e}")
            
    async def _load_detection_rules(self):
        """Load detection rules from OPA"""
        try:
            # Load security detection policies
            policies = [
                "security.brute_force_detection",
                "security.data_exfiltration_detection",
                "security.privilege_escalation_detection",
                "security.anomaly_detection"
            ]
            
            for policy in policies:
                await self.opa.get_policy(policy)
                
            logger.info("Loaded detection rules")
            
        except Exception as e:
            logger.error(f"Failed to load detection rules: {e}")
            
    async def _subscribe_to_events(self):
        """Subscribe to security events"""
        # This would subscribe to various event streams
        # For now, events are pushed via process_event method
        pass
        
    async def _store_event(self, event: SecurityEvent):
        """Store event in Consul"""
        try:
            event_data = {
                "id": event.id,
                "timestamp": event.timestamp.isoformat(),
                "event_type": event.event_type.value,
                "threat_level": event.threat_level.value,
                "source": event.source,
                "target": event.target,
                "details": event.details,
                "metadata": event.metadata
            }
            
            # Store with TTL (7 days)
            await self.consul.kv.put(
                f"security/events/{event.timestamp.strftime('%Y-%m-%d')}/{event.id}",
                json.dumps(event_data),
                ttl=604800
            )
            
        except Exception as e:
            logger.error(f"Failed to store event: {e}")
            
    async def _store_threat_indicator(self, indicator: ThreatIndicator):
        """Store threat indicator in Consul"""
        try:
            indicator_data = {
                "indicator_type": indicator.indicator_type,
                "value": indicator.value,
                "threat_level": indicator.threat_level.value,
                "confidence": indicator.confidence,
                "source": indicator.source,
                "last_seen": indicator.last_seen.isoformat(),
                "metadata": indicator.metadata
            }
            
            await self.consul.kv.put(
                f"security/threat-indicators/{indicator.indicator_type}/{indicator.value}",
                json.dumps(indicator_data)
            )
            
        except Exception as e:
            logger.error(f"Failed to store threat indicator: {e}")
            
    async def _run_detection_rules(self):
        """Run detection rules against recent events"""
        # This would evaluate OPA policies for threat detection
        pass
        
    async def _detect_anomalies(self):
        """Detect anomalies using ML models"""
        # This would use ML models for anomaly detection
        pass
        
    async def _update_threat_indicators(self):
        """Update threat indicators from threat intelligence feeds"""
        # This would integrate with threat intelligence sources
        pass
        
    async def _find_related_events(self, incident: SecurityIncident) -> List[SecurityEvent]:
        """Find events related to an incident"""
        related = []
        
        # Look for events with same resources
        for event in self._events:
            if event not in incident.events:
                # Check if event involves same resources
                event_resources = set()
                if "user_id" in event.source:
                    event_resources.add(f"user:{event.source['user_id']}")
                if "service" in event.source:
                    event_resources.add(f"service:{event.source['service']}")
                    
                if event_resources.intersection(incident.affected_resources):
                    # Check time proximity
                    if incident.start_time <= event.timestamp <= (incident.end_time or datetime.utcnow()):
                        related.append(event)
                        
        return related
        
    async def _is_incident_resolved(self, incident: SecurityIncident) -> bool:
        """Check if incident is resolved"""
        # Check if threat is still active
        if incident.events:
            last_event = max(incident.events, key=lambda e: e.timestamp)
            
            # If no new events for 30 minutes, consider resolved
            if datetime.utcnow() - last_event.timestamp > timedelta(minutes=30):
                return True
                
        return False 