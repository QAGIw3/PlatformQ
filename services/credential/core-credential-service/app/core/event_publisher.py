"""
Event Publisher for credential-related events
"""

import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from enum import Enum

from platformq_shared.pulsar import PulsarManager
from platformq_events.base import BaseEvent

logger = logging.getLogger(__name__)


class CredentialEventType(str, Enum):
    """Types of credential events"""
    ISSUED = "credential.issued"
    VERIFIED = "credential.verified"
    REVOKED = "credential.revoked"
    UPDATED = "credential.updated"
    EXPIRED = "credential.expired"
    ANCHORED = "credential.anchored"
    STORED = "credential.stored"


class CredentialEvent(BaseEvent):
    """Base credential event"""
    event_type: CredentialEventType
    credential_id: str
    issuer_did: Optional[str]
    subject_did: Optional[str]
    credential_type: Optional[str]
    tenant_id: Optional[str]
    metadata: Dict[str, Any] = {}


class CredentialEventPublisher:
    """Publishes credential events to Pulsar"""
    
    def __init__(self, pulsar_manager: PulsarManager, topic: str):
        self.pulsar_manager = pulsar_manager
        self.topic = topic
        self._producer = None
    
    async def publish_credential_issued(
        self,
        credential_id: str,
        credential: Dict[str, Any],
        issuer_did: str,
        subject_did: Optional[str],
        credential_type: str,
        tenant_id: Optional[str] = None,
        blockchain_info: Optional[Dict[str, Any]] = None,
        storage_info: Optional[Dict[str, Any]] = None
    ):
        """Publish credential issued event"""
        event = CredentialEvent(
            event_id=self._generate_event_id(),
            event_type=CredentialEventType.ISSUED,
            timestamp=datetime.utcnow(),
            credential_id=credential_id,
            issuer_did=issuer_did,
            subject_did=subject_did,
            credential_type=credential_type,
            tenant_id=tenant_id,
            metadata={
                "credential": credential,
                "blockchain_info": blockchain_info,
                "storage_info": storage_info
            }
        )
        
        await self._publish_event(event)
        
    async def publish_credential_verified(
        self,
        credential_id: str,
        verification_result: Dict[str, Any],
        verifier_did: Optional[str] = None,
        tenant_id: Optional[str] = None
    ):
        """Publish credential verified event"""
        event = CredentialEvent(
            event_id=self._generate_event_id(),
            event_type=CredentialEventType.VERIFIED,
            timestamp=datetime.utcnow(),
            credential_id=credential_id,
            tenant_id=tenant_id,
            metadata={
                "verification_result": verification_result,
                "verifier_did": verifier_did
            }
        )
        
        await self._publish_event(event)
    
    async def publish_credential_revoked(
        self,
        credential_id: str,
        issuer_did: str,
        reason: str,
        tenant_id: Optional[str] = None
    ):
        """Publish credential revoked event"""
        event = CredentialEvent(
            event_id=self._generate_event_id(),
            event_type=CredentialEventType.REVOKED,
            timestamp=datetime.utcnow(),
            credential_id=credential_id,
            issuer_did=issuer_did,
            tenant_id=tenant_id,
            metadata={
                "reason": reason,
                "revoked_at": datetime.utcnow().isoformat()
            }
        )
        
        await self._publish_event(event)
    
    async def publish_credential_anchored(
        self,
        credential_id: str,
        blockchain: str,
        transaction_hash: str,
        block_number: Optional[int] = None,
        tenant_id: Optional[str] = None
    ):
        """Publish credential blockchain anchored event"""
        event = CredentialEvent(
            event_id=self._generate_event_id(),
            event_type=CredentialEventType.ANCHORED,
            timestamp=datetime.utcnow(),
            credential_id=credential_id,
            tenant_id=tenant_id,
            metadata={
                "blockchain": blockchain,
                "transaction_hash": transaction_hash,
                "block_number": block_number
            }
        )
        
        await self._publish_event(event)
    
    async def publish_credential_stored(
        self,
        credential_id: str,
        storage_type: str,
        storage_id: str,
        encrypted: bool = False,
        tenant_id: Optional[str] = None
    ):
        """Publish credential stored event"""
        event = CredentialEvent(
            event_id=self._generate_event_id(),
            event_type=CredentialEventType.STORED,
            timestamp=datetime.utcnow(),
            credential_id=credential_id,
            tenant_id=tenant_id,
            metadata={
                "storage_type": storage_type,
                "storage_id": storage_id,
                "encrypted": encrypted
            }
        )
        
        await self._publish_event(event)
    
    async def _publish_event(self, event: CredentialEvent):
        """Publish event to Pulsar"""
        if not self._producer:
            # Create producer on first use
            try:
                self._producer = self.pulsar_manager.client.create_producer(self.topic)
                logger.info(f"Created producer for topic: {self.topic}")
            except Exception as e:
                logger.error(f"Failed to create producer: {e}")
                return
            
        try:
            # Convert event to JSON
            event_data = event.dict()
            message = json.dumps(event_data).encode('utf-8')
            
            # Add properties for routing
            properties = {
                'event_type': event.event_type,
                'credential_id': event.credential_id
            }
            
            if event.tenant_id:
                properties['tenant_id'] = event.tenant_id
            
            # Send message
            self._producer.send_async(
                content=message,
                properties=properties,
                callback=lambda res, msg_id: logger.debug(f"Published {event.event_type} event")
            )
            
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            # Don't raise - events are best effort
    
    def _generate_event_id(self) -> str:
        """Generate unique event ID"""
        import uuid
        return str(uuid.uuid4())
    
    async def close(self):
        """Close the publisher"""
        if self._producer:
            self._producer.close()
            self._producer = None 