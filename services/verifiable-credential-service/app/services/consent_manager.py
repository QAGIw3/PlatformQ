"""
Consent Management System for Verifiable Credentials

Implements GDPR-compliant consent management with:
- Granular consent tracking
- Purpose limitation
- Data minimization
- Right to withdraw
- Consent versioning
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import uuid

from fastapi import HTTPException
from sqlalchemy import Column, String, JSON, DateTime, Boolean, Integer, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID

from platformq_shared.database import DatabaseManager
from platformq_shared.events import EventPublisher
from platformq_shared.cache import CacheManager

logger = logging.getLogger(__name__)

Base = declarative_base()


class ConsentStatus(str, Enum):
    """Consent status values"""
    PENDING = "pending"
    ACTIVE = "active"
    WITHDRAWN = "withdrawn"
    EXPIRED = "expired"
    REJECTED = "rejected"


class LawfulBasis(str, Enum):
    """GDPR lawful basis for processing"""
    CONSENT = "consent"
    CONTRACT = "contract"
    LEGAL_OBLIGATION = "legal_obligation"
    VITAL_INTERESTS = "vital_interests"
    PUBLIC_TASK = "public_task"
    LEGITIMATE_INTERESTS = "legitimate_interests"


@dataclass
class ConsentScope:
    """Defines what data can be accessed"""
    data_categories: List[str]  # e.g., ["identity", "education", "employment"]
    attributes: List[str]  # Specific attributes within categories
    purposes: List[str]  # e.g., ["verification", "compliance", "analytics"]
    processing_types: List[str] = field(default_factory=list)  # e.g., ["read", "share", "store"]
    
    
@dataclass
class ConsentRequest:
    """Request for user consent"""
    subject_id: str
    requester_id: str
    scope: ConsentScope
    purpose_description: str
    retention_period_days: int
    lawful_basis: LawfulBasis = LawfulBasis.CONSENT
    special_categories: List[str] = field(default_factory=list)  # Sensitive data categories
    third_party_sharing: bool = False
    third_parties: List[str] = field(default_factory=list)
    automated_processing: bool = False
    cross_border_transfer: bool = False
    transfer_countries: List[str] = field(default_factory=list)
    

@dataclass
class ConsentRecord:
    """Recorded consent"""
    consent_id: str
    subject_id: str
    requester_id: str
    status: ConsentStatus
    scope: ConsentScope
    granted_at: Optional[datetime]
    expires_at: Optional[datetime]
    withdrawn_at: Optional[datetime]
    version: str
    lawful_basis: LawfulBasis
    metadata: Dict[str, Any] = field(default_factory=dict)


class ConsentModel(Base):
    """Database model for consent records"""
    __tablename__ = "consent_records"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    consent_id = Column(String, unique=True, nullable=False, index=True)
    subject_id = Column(String, nullable=False, index=True)
    requester_id = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False)
    scope = Column(JSON, nullable=False)
    purpose_description = Column(Text)
    retention_period_days = Column(Integer)
    lawful_basis = Column(String, nullable=False)
    special_categories = Column(JSON)
    third_party_sharing = Column(Boolean, default=False)
    third_parties = Column(JSON)
    automated_processing = Column(Boolean, default=False)
    cross_border_transfer = Column(Boolean, default=False)
    transfer_countries = Column(JSON)
    granted_at = Column(DateTime)
    expires_at = Column(DateTime)
    withdrawn_at = Column(DateTime)
    withdrawal_reason = Column(Text)
    version = Column(String, nullable=False)
    parent_consent_id = Column(String)  # For consent updates
    metadata = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ConsentManager:
    """Manages consent for verifiable credentials"""
    
    def __init__(self,
                 db_manager: DatabaseManager,
                 cache_manager: CacheManager,
                 event_publisher: EventPublisher):
        self.db = db_manager
        self.cache = cache_manager
        self.events = event_publisher
        self.logger = logger
        
        # Initialize database tables
        Base.metadata.create_all(self.db.engine)
        
        # Cache configuration
        self.cache_ttl = 3600  # 1 hour
        self.consent_version = "2.0"  # Current consent version
        
    async def request_consent(self, request: ConsentRequest) -> str:
        """Request consent from a data subject"""
        try:
            # Generate consent ID
            consent_id = self._generate_consent_id(request)
            
            # Check for existing active consent
            existing = await self._get_active_consent(
                request.subject_id,
                request.requester_id,
                request.scope
            )
            
            if existing:
                # Check if scope is already covered
                if self._is_scope_covered(existing.scope, request.scope):
                    return existing.consent_id
                    
            # Create consent record
            consent_record = ConsentModel(
                consent_id=consent_id,
                subject_id=request.subject_id,
                requester_id=request.requester_id,
                status=ConsentStatus.PENDING.value,
                scope=self._serialize_scope(request.scope),
                purpose_description=request.purpose_description,
                retention_period_days=request.retention_period_days,
                lawful_basis=request.lawful_basis.value,
                special_categories=request.special_categories,
                third_party_sharing=request.third_party_sharing,
                third_parties=request.third_parties,
                automated_processing=request.automated_processing,
                cross_border_transfer=request.cross_border_transfer,
                transfer_countries=request.transfer_countries,
                version=self.consent_version,
                metadata={
                    "request_time": datetime.utcnow().isoformat(),
                    "ip_address": request.__dict__.get("ip_address"),
                    "user_agent": request.__dict__.get("user_agent")
                }
            )
            
            # Save to database
            async with self.db.session() as session:
                session.add(consent_record)
                await session.commit()
                
            # Publish consent request event
            await self.events.publish("consent.requested", {
                "consent_id": consent_id,
                "subject_id": request.subject_id,
                "requester_id": request.requester_id,
                "scope": self._serialize_scope(request.scope)
            })
            
            # Send notification to user
            await self._notify_consent_request(request, consent_id)
            
            return consent_id
            
        except Exception as e:
            self.logger.error(f"Error requesting consent: {e}")
            raise HTTPException(status_code=500, detail="Failed to request consent")
            
    async def grant_consent(self, 
                           consent_id: str, 
                           subject_id: str,
                           granted_scope: Optional[ConsentScope] = None) -> bool:
        """Grant consent"""
        try:
            async with self.db.session() as session:
                # Get consent record
                consent = await session.query(ConsentModel).filter_by(
                    consent_id=consent_id,
                    subject_id=subject_id
                ).first()
                
                if not consent:
                    raise HTTPException(status_code=404, detail="Consent record not found")
                    
                if consent.status != ConsentStatus.PENDING.value:
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Consent is already {consent.status}"
                    )
                    
                # Update consent record
                consent.status = ConsentStatus.ACTIVE.value
                consent.granted_at = datetime.utcnow()
                
                # Set expiry based on retention period
                if consent.retention_period_days:
                    consent.expires_at = datetime.utcnow() + timedelta(
                        days=consent.retention_period_days
                    )
                    
                # Update scope if provided (data minimization)
                if granted_scope:
                    consent.scope = self._serialize_scope(granted_scope)
                    
                await session.commit()
                
            # Clear cache
            await self._clear_consent_cache(subject_id)
            
            # Publish consent granted event
            await self.events.publish("consent.granted", {
                "consent_id": consent_id,
                "subject_id": subject_id,
                "granted_at": consent.granted_at.isoformat()
            })
            
            # Log for audit
            await self._log_consent_event(
                consent_id=consent_id,
                subject_id=subject_id,
                event_type="consent_granted",
                metadata={"scope": consent.scope}
            )
            
            return True
            
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Error granting consent: {e}")
            raise HTTPException(status_code=500, detail="Failed to grant consent")
            
    async def withdraw_consent(self,
                              consent_id: str,
                              subject_id: str,
                              reason: Optional[str] = None) -> bool:
        """Withdraw consent (GDPR right to withdraw)"""
        try:
            async with self.db.session() as session:
                # Get consent record
                consent = await session.query(ConsentModel).filter_by(
                    consent_id=consent_id,
                    subject_id=subject_id
                ).first()
                
                if not consent:
                    raise HTTPException(status_code=404, detail="Consent record not found")
                    
                if consent.status != ConsentStatus.ACTIVE.value:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Cannot withdraw {consent.status} consent"
                    )
                    
                # Update consent record
                consent.status = ConsentStatus.WITHDRAWN.value
                consent.withdrawn_at = datetime.utcnow()
                consent.withdrawal_reason = reason
                
                await session.commit()
                
            # Clear cache
            await self._clear_consent_cache(subject_id)
            
            # Publish withdrawal event
            await self.events.publish("consent.withdrawn", {
                "consent_id": consent_id,
                "subject_id": subject_id,
                "withdrawn_at": consent.withdrawn_at.isoformat(),
                "reason": reason
            })
            
            # Notify systems to stop processing
            await self._notify_consent_withdrawal(consent_id, subject_id)
            
            # Log for audit
            await self._log_consent_event(
                consent_id=consent_id,
                subject_id=subject_id,
                event_type="consent_withdrawn",
                metadata={"reason": reason}
            )
            
            return True
            
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Error withdrawing consent: {e}")
            raise HTTPException(status_code=500, detail="Failed to withdraw consent")
            
    async def verify_consent(self,
                            subject_id: str,
                            requester_id: str,
                            required_scope: ConsentScope,
                            purpose: Optional[str] = None) -> Optional[ConsentRecord]:
        """Verify if valid consent exists for the requested scope"""
        try:
            # Check cache first
            cache_key = f"consent:{subject_id}:{requester_id}:{self._hash_scope(required_scope)}"
            cached = await self.cache.get(cache_key)
            
            if cached:
                return ConsentRecord(**json.loads(cached))
                
            # Query database
            async with self.db.session() as session:
                consents = await session.query(ConsentModel).filter_by(
                    subject_id=subject_id,
                    requester_id=requester_id,
                    status=ConsentStatus.ACTIVE.value
                ).all()
                
                for consent in consents:
                    # Check if consent is still valid
                    if consent.expires_at and consent.expires_at < datetime.utcnow():
                        # Mark as expired
                        consent.status = ConsentStatus.EXPIRED.value
                        await session.commit()
                        continue
                        
                    # Check if scope is covered
                    consent_scope = self._deserialize_scope(consent.scope)
                    if self._is_scope_covered(consent_scope, required_scope):
                        # Check purpose if specified
                        if purpose and purpose not in consent_scope.purposes:
                            continue
                            
                        # Valid consent found
                        consent_record = ConsentRecord(
                            consent_id=consent.consent_id,
                            subject_id=consent.subject_id,
                            requester_id=consent.requester_id,
                            status=ConsentStatus(consent.status),
                            scope=consent_scope,
                            granted_at=consent.granted_at,
                            expires_at=consent.expires_at,
                            withdrawn_at=consent.withdrawn_at,
                            version=consent.version,
                            lawful_basis=LawfulBasis(consent.lawful_basis),
                            metadata=consent.metadata or {}
                        )
                        
                        # Cache the result
                        await self.cache.set(
                            cache_key,
                            json.dumps(consent_record.__dict__, default=str),
                            ttl=self.cache_ttl
                        )
                        
                        # Log verification
                        await self._log_consent_event(
                            consent_id=consent.consent_id,
                            subject_id=subject_id,
                            event_type="consent_verified",
                            metadata={"purpose": purpose}
                        )
                        
                        return consent_record
                        
            return None
            
        except Exception as e:
            self.logger.error(f"Error verifying consent: {e}")
            return None
            
    async def get_consent_history(self, subject_id: str) -> List[ConsentRecord]:
        """Get consent history for a subject (GDPR right to access)"""
        try:
            async with self.db.session() as session:
                consents = await session.query(ConsentModel).filter_by(
                    subject_id=subject_id
                ).order_by(ConsentModel.created_at.desc()).all()
                
                history = []
                for consent in consents:
                    history.append(ConsentRecord(
                        consent_id=consent.consent_id,
                        subject_id=consent.subject_id,
                        requester_id=consent.requester_id,
                        status=ConsentStatus(consent.status),
                        scope=self._deserialize_scope(consent.scope),
                        granted_at=consent.granted_at,
                        expires_at=consent.expires_at,
                        withdrawn_at=consent.withdrawn_at,
                        version=consent.version,
                        lawful_basis=LawfulBasis(consent.lawful_basis),
                        metadata=consent.metadata or {}
                    ))
                    
                return history
                
        except Exception as e:
            self.logger.error(f"Error getting consent history: {e}")
            raise HTTPException(status_code=500, detail="Failed to get consent history")
            
    async def update_consent(self,
                            consent_id: str,
                            subject_id: str,
                            new_scope: ConsentScope) -> str:
        """Update existing consent with new scope"""
        try:
            # Get existing consent
            async with self.db.session() as session:
                existing = await session.query(ConsentModel).filter_by(
                    consent_id=consent_id,
                    subject_id=subject_id,
                    status=ConsentStatus.ACTIVE.value
                ).first()
                
                if not existing:
                    raise HTTPException(status_code=404, detail="Active consent not found")
                    
                # Create new consent record (versioning)
                new_consent_id = self._generate_consent_id(ConsentRequest(
                    subject_id=subject_id,
                    requester_id=existing.requester_id,
                    scope=new_scope,
                    purpose_description=existing.purpose_description,
                    retention_period_days=existing.retention_period_days
                ))
                
                new_consent = ConsentModel(
                    consent_id=new_consent_id,
                    subject_id=existing.subject_id,
                    requester_id=existing.requester_id,
                    status=ConsentStatus.PENDING.value,
                    scope=self._serialize_scope(new_scope),
                    purpose_description=existing.purpose_description,
                    retention_period_days=existing.retention_period_days,
                    lawful_basis=existing.lawful_basis,
                    special_categories=existing.special_categories,
                    third_party_sharing=existing.third_party_sharing,
                    third_parties=existing.third_parties,
                    automated_processing=existing.automated_processing,
                    cross_border_transfer=existing.cross_border_transfer,
                    transfer_countries=existing.transfer_countries,
                    version=self.consent_version,
                    parent_consent_id=consent_id,
                    metadata={
                        "update_reason": "scope_change",
                        "previous_consent_id": consent_id
                    }
                )
                
                session.add(new_consent)
                await session.commit()
                
            # Clear cache
            await self._clear_consent_cache(subject_id)
            
            # Notify user of update request
            await self._notify_consent_update(existing, new_consent_id, new_scope)
            
            return new_consent_id
            
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Error updating consent: {e}")
            raise HTTPException(status_code=500, detail="Failed to update consent")
            
    def _generate_consent_id(self, request: ConsentRequest) -> str:
        """Generate unique consent ID"""
        data = f"{request.subject_id}:{request.requester_id}:{datetime.utcnow().isoformat()}"
        return f"consent_{hashlib.sha256(data.encode()).hexdigest()[:16]}"
        
    def _serialize_scope(self, scope: ConsentScope) -> Dict[str, Any]:
        """Serialize consent scope for storage"""
        return {
            "data_categories": scope.data_categories,
            "attributes": scope.attributes,
            "purposes": scope.purposes,
            "processing_types": scope.processing_types
        }
        
    def _deserialize_scope(self, scope_data: Dict[str, Any]) -> ConsentScope:
        """Deserialize consent scope from storage"""
        return ConsentScope(
            data_categories=scope_data.get("data_categories", []),
            attributes=scope_data.get("attributes", []),
            purposes=scope_data.get("purposes", []),
            processing_types=scope_data.get("processing_types", [])
        )
        
    def _is_scope_covered(self, 
                         granted_scope: ConsentScope, 
                         required_scope: ConsentScope) -> bool:
        """Check if granted scope covers required scope"""
        # Check data categories
        if not set(required_scope.data_categories).issubset(set(granted_scope.data_categories)):
            return False
            
        # Check attributes
        if not set(required_scope.attributes).issubset(set(granted_scope.attributes)):
            return False
            
        # Check purposes
        if not set(required_scope.purposes).issubset(set(granted_scope.purposes)):
            return False
            
        # Check processing types
        if required_scope.processing_types:
            if not set(required_scope.processing_types).issubset(set(granted_scope.processing_types)):
                return False
                
        return True
        
    def _hash_scope(self, scope: ConsentScope) -> str:
        """Generate hash of scope for caching"""
        scope_str = json.dumps(self._serialize_scope(scope), sort_keys=True)
        return hashlib.sha256(scope_str.encode()).hexdigest()[:16]
        
    async def _get_active_consent(self,
                                 subject_id: str,
                                 requester_id: str,
                                 scope: ConsentScope) -> Optional[ConsentRecord]:
        """Get existing active consent if any"""
        return await self.verify_consent(subject_id, requester_id, scope)
        
    async def _clear_consent_cache(self, subject_id: str):
        """Clear consent cache for a subject"""
        # In production, use cache tags or pattern matching
        pattern = f"consent:{subject_id}:*"
        await self.cache.delete_pattern(pattern)
        
    async def _notify_consent_request(self, request: ConsentRequest, consent_id: str):
        """Notify user of consent request"""
        # Publish notification event
        await self.events.publish("notification.send", {
            "user_id": request.subject_id,
            "type": "consent_request",
            "title": "Data Access Consent Request",
            "message": f"{request.requester_id} is requesting access to your data",
            "data": {
                "consent_id": consent_id,
                "requester": request.requester_id,
                "scope": self._serialize_scope(request.scope),
                "purpose": request.purpose_description
            }
        })
        
    async def _notify_consent_withdrawal(self, consent_id: str, subject_id: str):
        """Notify systems of consent withdrawal"""
        # Publish to all systems that might be processing data
        await self.events.publish("consent.withdrawal.notify", {
            "consent_id": consent_id,
            "subject_id": subject_id,
            "action": "stop_processing",
            "timestamp": datetime.utcnow().isoformat()
        })
        
    async def _notify_consent_update(self, 
                                    existing: ConsentModel, 
                                    new_consent_id: str,
                                    new_scope: ConsentScope):
        """Notify user of consent update request"""
        await self.events.publish("notification.send", {
            "user_id": existing.subject_id,
            "type": "consent_update",
            "title": "Consent Update Request",
            "message": f"{existing.requester_id} is requesting to update data access permissions",
            "data": {
                "existing_consent_id": existing.consent_id,
                "new_consent_id": new_consent_id,
                "requester": existing.requester_id,
                "current_scope": existing.scope,
                "requested_scope": self._serialize_scope(new_scope)
            }
        })
        
    async def _log_consent_event(self, **kwargs):
        """Log consent event for audit trail"""
        event = {
            "timestamp": datetime.utcnow().isoformat(),
            "service": "consent_manager",
            **kwargs
        }
        
        # Publish to audit log
        await self.events.publish("audit.consent", event)
        
        # Also log locally
        self.logger.info(f"Consent event: {json.dumps(event)}") 