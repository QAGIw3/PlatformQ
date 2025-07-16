"""
Trust-Aware Event Publisher

Extends the base EventPublisher with trust-based routing capabilities,
including priority processing for high-trust entities and credential-gated access.
"""

import json
import logging
import time
from typing import Dict, List, Optional, Any, Type
from datetime import datetime
import httpx
from pulsar import Client, ConsumerType, Producer
from pulsar.schema import AvroSchema, Schema

from .event_publisher import EventPublisher
from .config import ConfigLoader

logger = logging.getLogger(__name__)


class TrustLevelPriority:
    """Maps trust levels to routing priorities"""
    VERIFIED = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "NORMAL"
    LOW = "LOW"
    UNTRUSTED = "LOW"


class TrustAwareEventPublisher(EventPublisher):
    """
    Event publisher with trust-based routing and credential gating.
    
    Features:
    - Trust-based priority routing
    - Credential-gated event access
    - Automated reputation events
    - Trust score caching
    """
    
    def __init__(self, pulsar_url: str = None, vc_service_url: str = None):
        super().__init__(pulsar_url)
        self.config = ConfigLoader().load_settings()
        self.vc_service_url = vc_service_url or self.config.get(
            "VC_SERVICE_URL", "http://verifiable-credential-service:8000"
        )
        self.trust_cache: Dict[str, Dict[str, Any]] = {}
        self.cache_ttl = 300  # 5 minutes
        
        # Priority producers for different trust levels
        self.priority_producers: Dict[str, Producer] = {}
        
    def connect(self):
        """Connect to Pulsar with priority topics"""
        super().connect()
        
        # Create priority producers
        priority_levels = ["critical", "high", "normal", "low"]
        for priority in priority_levels:
            topic = f"persistent://platformq/priority/{priority}-events"
            self.priority_producers[priority] = self.client.create_producer(topic)
            
    def get_entity_trust_info(self, entity_id: str) -> Dict[str, Any]:
        """
        Get trust information for an entity.
        Uses caching to reduce API calls.
        """
        # Check cache first
        cached = self.trust_cache.get(entity_id)
        if cached and cached['expires'] > time.time():
            return cached['data']
            
        # Fetch from VC service
        try:
            with httpx.Client() as client:
                response = client.get(
                    f"{self.vc_service_url}/api/v1/trust/entities/{entity_id}"
                )
                if response.status_code == 200:
                    trust_info = response.json()
                    # Cache the result
                    self.trust_cache[entity_id] = {
                        'data': trust_info,
                        'expires': time.time() + self.cache_ttl
                    }
                    return trust_info
        except Exception as e:
            logger.error(f"Failed to fetch trust info for {entity_id}: {e}")
            
        # Return default low trust
        return {
            'trust_score': 0.0,
            'trust_level': 'UNTRUSTED',
            'credentials': []
        }
        
    def check_credential_gates(
        self, 
        entity_id: str, 
        required_gates: List[Dict[str, Any]]
    ) -> bool:
        """
        Check if an entity meets credential gate requirements.
        
        Args:
            entity_id: The entity to check
            required_gates: List of credential requirements
            
        Returns:
            True if all gates are satisfied, False otherwise
        """
        if not required_gates:
            return True
            
        trust_info = self.get_entity_trust_info(entity_id)
        entity_credentials = trust_info.get('credentials', [])
        
        for gate in required_gates:
            cred_type = gate['credential_type']
            required = gate['required']
            min_score = gate.get('minimum_score')
            
            # Find matching credential
            found = False
            for cred in entity_credentials:
                if cred['type'] == cred_type:
                    if min_score is None or cred.get('score', 0) >= min_score:
                        found = True
                        break
                        
            if required and not found:
                logger.info(f"Entity {entity_id} missing required credential: {cred_type}")
                return False
                
        return True
        
    def publish_with_trust(
        self,
        topic_base: str,
        tenant_id: str,
        schema_class: Type[Schema],
        data: Any,
        sender_entity_id: str,
        credential_gates: Optional[List[Dict[str, Any]]] = None,
        minimum_trust_level: Optional[str] = None
    ):
        """
        Publish an event with trust-based routing.
        
        Args:
            topic_base: Base topic name
            tenant_id: Tenant identifier
            schema_class: Avro schema class
            data: Event data
            sender_entity_id: Entity ID of the sender
            credential_gates: Optional credential requirements
            minimum_trust_level: Minimum trust level required
        """
        # Get sender's trust information
        trust_info = self.get_entity_trust_info(sender_entity_id)
        trust_score = trust_info['trust_score']
        trust_level = trust_info['trust_level']
        
        # Check minimum trust level if specified
        trust_levels = ['UNTRUSTED', 'LOW', 'MEDIUM', 'HIGH', 'VERIFIED']
        if minimum_trust_level:
            sender_level_idx = trust_levels.index(trust_level)
            min_level_idx = trust_levels.index(minimum_trust_level)
            
            if sender_level_idx < min_level_idx:
                logger.warning(
                    f"Entity {sender_entity_id} with trust level {trust_level} "
                    f"below minimum {minimum_trust_level}"
                )
                raise PermissionError(f"Insufficient trust level: {trust_level}")
                
        # Check credential gates
        if credential_gates and not self.check_credential_gates(sender_entity_id, credential_gates):
            raise PermissionError("Credential requirements not met")
            
        # Determine routing priority
        priority = getattr(TrustLevelPriority, trust_level, "LOW").lower()
        
        # Create trust-based routing event
        routing_event = {
            'event_id': str(time.time_ns()),
            'original_event_type': schema_class.__name__,
            'sender_entity_id': sender_entity_id,
            'sender_trust_score': trust_score,
            'sender_trust_level': trust_level,
            'routing_priority': priority.upper(),
            'processing_requirements': self._get_processing_requirements(trust_level),
            'credential_gates': credential_gates,
            'original_event_data': json.dumps(data.__dict__ if hasattr(data, '__dict__') else data),
            'routing_metadata': {
                'tenant_id': tenant_id,
                'topic_base': topic_base,
                'routed_at': datetime.utcnow().isoformat()
            },
            'timestamp': int(datetime.utcnow().timestamp() * 1000)
        }
        
        # Publish to priority queue
        priority_producer = self.priority_producers.get(priority, self.priority_producers['normal'])
        priority_producer.send(json.dumps(routing_event).encode('utf-8'))
        
        # Also publish to original topic for compatibility
        super().publish(topic_base, tenant_id, schema_class, data)
        
        # Emit trust activity event
        self._emit_trust_activity(sender_entity_id, 'event_published', {
            'event_type': schema_class.__name__,
            'trust_level': trust_level,
            'priority': priority
        })
        
    def _get_processing_requirements(self, trust_level: str) -> List[str]:
        """Get special processing requirements based on trust level"""
        requirements = []
        
        if trust_level == 'UNTRUSTED':
            requirements.extend(['manual_review', 'rate_limit_strict'])
        elif trust_level == 'LOW':
            requirements.extend(['enhanced_monitoring', 'rate_limit_normal'])
        elif trust_level == 'VERIFIED':
            requirements.extend(['fast_track', 'skip_basic_validation'])
            
        return requirements
        
    def _emit_trust_activity(
        self, 
        entity_id: str, 
        activity_type: str, 
        metadata: Dict[str, Any]
    ):
        """Emit a trust activity event for reputation tracking"""
        try:
            activity_event = {
                'entity_id': entity_id,
                'activity_type': activity_type,
                'timestamp': datetime.utcnow().isoformat(),
                'metadata': metadata
            }
            
            # Send to trust activity topic
            topic = "persistent://platformq/trust/activity-events"
            self.client.create_producer(topic).send(
                json.dumps(activity_event).encode('utf-8')
            )
        except Exception as e:
            logger.error(f"Failed to emit trust activity: {e}")
            
    def create_trust_consumer(
        self,
        subscription_name: str,
        minimum_trust_level: str = 'LOW',
        required_credentials: Optional[List[str]] = None
    ):
        """
        Create a consumer that only receives events from trusted entities.
        
        Args:
            subscription_name: Pulsar subscription name
            minimum_trust_level: Minimum trust level to receive events
            required_credentials: List of required credential types
            
        Returns:
            Pulsar Consumer with trust filtering
        """
        # Create filter expression
        trust_levels = ['UNTRUSTED', 'LOW', 'MEDIUM', 'HIGH', 'VERIFIED']
        min_level_idx = trust_levels.index(minimum_trust_level)
        allowed_levels = trust_levels[min_level_idx:]
        
        # Subscribe to appropriate priority topics
        topics = []
        if 'VERIFIED' in allowed_levels:
            topics.append("persistent://platformq/priority/critical-events")
        if 'HIGH' in allowed_levels:
            topics.append("persistent://platformq/priority/high-events")
        if 'MEDIUM' in allowed_levels:
            topics.append("persistent://platformq/priority/normal-events")
        if 'LOW' in allowed_levels or 'UNTRUSTED' in allowed_levels:
            topics.append("persistent://platformq/priority/low-events")
            
        consumer = self.client.subscribe(
            topics,
            subscription_name,
            consumer_type=ConsumerType.Shared
        )
        
        # Wrap consumer with trust filtering
        return TrustFilteredConsumer(
            consumer, 
            self, 
            minimum_trust_level,
            required_credentials
        )
        

class TrustFilteredConsumer:
    """Consumer wrapper that filters messages based on trust criteria"""
    
    def __init__(
        self,
        consumer,
        publisher: TrustAwareEventPublisher,
        minimum_trust_level: str,
        required_credentials: Optional[List[str]] = None
    ):
        self.consumer = consumer
        self.publisher = publisher
        self.minimum_trust_level = minimum_trust_level
        self.required_credentials = required_credentials or []
        
    def receive(self, timeout_millis: int = None):
        """Receive next trusted message"""
        while True:
            msg = self.consumer.receive(timeout_millis)
            if msg is None:
                return None
                
            # Parse routing event
            try:
                routing_event = json.loads(msg.data().decode('utf-8'))
                sender_id = routing_event['sender_entity_id']
                
                # Check credentials if required
                if self.required_credentials:
                    gates = [
                        {'credential_type': cred, 'required': True}
                        for cred in self.required_credentials
                    ]
                    if not self.publisher.check_credential_gates(sender_id, gates):
                        self.consumer.acknowledge(msg)
                        continue
                        
                # Message passed all filters
                return msg
                
            except Exception as e:
                logger.error(f"Error processing trust-filtered message: {e}")
                self.consumer.acknowledge(msg)
                
    def acknowledge(self, message):
        """Acknowledge a message"""
        self.consumer.acknowledge(message)
        
    def negative_acknowledge(self, message):
        """Negative acknowledge a message"""
        self.consumer.negative_acknowledge(message) 