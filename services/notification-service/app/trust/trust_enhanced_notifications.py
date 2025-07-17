"""
Trust-Enhanced Notifications

Implements trust scoring and verification for notifications,
ensuring users receive authentic and prioritized communications.
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
import hashlib
import hmac

from pydantic import BaseModel, Field
import numpy as np
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization
import jwt

logger = logging.getLogger(__name__)


class NotificationTrustLevel(Enum):
    """Trust levels for notifications"""
    VERIFIED = "verified"          # Cryptographically verified
    TRUSTED = "trusted"            # From trusted source
    STANDARD = "standard"          # Normal notifications
    UNVERIFIED = "unverified"      # Cannot verify source
    SUSPICIOUS = "suspicious"      # Potentially malicious


class NotificationPriority(Enum):
    """Priority levels based on trust and importance"""
    CRITICAL = 5
    HIGH = 4
    MEDIUM = 3
    LOW = 2
    MINIMAL = 1


class TrustMetrics(BaseModel):
    """Trust metrics for a notification source"""
    source_id: str
    trust_score: float = Field(ge=0.0, le=1.0)
    verification_count: int = 0
    rejection_count: int = 0
    user_feedback_score: float = Field(ge=0.0, le=1.0)
    last_updated: datetime


class TrustedNotification(BaseModel):
    """Trust-enhanced notification model"""
    notification_id: str
    source_id: str
    recipient_id: str
    title: str
    content: str
    notification_type: str
    trust_level: NotificationTrustLevel
    trust_score: float = Field(ge=0.0, le=1.0)
    priority: NotificationPriority
    signature: Optional[str] = None
    verification_proof: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    expires_at: Optional[datetime] = None


class TrustEnhancedNotificationSystem:
    """Manages trust-enhanced notifications"""
    
    def __init__(self,
                 trust_threshold: float = 0.7,
                 verification_required_types: List[str] = None):
        self.trust_threshold = trust_threshold
        self.verification_required_types = verification_required_types or [
            "security_alert",
            "transaction_confirmation",
            "account_change",
            "system_critical"
        ]
        
        # Trust metrics cache
        self.trust_metrics = {}
        
        # Cryptographic keys for verification
        self.system_private_key = None
        self.system_public_key = None
        self._initialize_keys()
        
        # Trusted sources registry
        self.trusted_sources = {}
        
        # Notification queue with priority
        self.notification_queue = asyncio.PriorityQueue()
        
    def _initialize_keys(self):
        """Initialize system cryptographic keys"""
        self.system_private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        self.system_public_key = self.system_private_key.public_key()
        
    async def send_notification(self,
                              source_id: str,
                              recipient_id: str,
                              title: str,
                              content: str,
                              notification_type: str,
                              metadata: Dict[str, Any] = None,
                              require_verification: Optional[bool] = None) -> Dict[str, Any]:
        """Send a trust-enhanced notification"""
        try:
            # Determine if verification is required
            if require_verification is None:
                require_verification = notification_type in self.verification_required_types
                
            # Get trust metrics for source
            trust_metrics = await self._get_trust_metrics(source_id)
            trust_score = trust_metrics.trust_score if trust_metrics else 0.5
            
            # Determine trust level
            trust_level = await self._determine_trust_level(
                source_id,
                trust_score,
                require_verification
            )
            
            # Create notification
            notification = TrustedNotification(
                notification_id=self._generate_notification_id(),
                source_id=source_id,
                recipient_id=recipient_id,
                title=title,
                content=content,
                notification_type=notification_type,
                trust_level=trust_level,
                trust_score=trust_score,
                priority=self._calculate_priority(trust_level, notification_type),
                metadata=metadata or {},
                created_at=datetime.utcnow(),
                expires_at=datetime.utcnow() + timedelta(days=7)
            )
            
            # Sign notification if required
            if require_verification:
                notification.signature = await self._sign_notification(notification)
                notification.verification_proof = await self._generate_verification_proof(
                    notification
                )
                
            # Validate notification
            validation = await self._validate_notification(notification)
            
            if not validation["is_valid"]:
                return {
                    "success": False,
                    "error": f"Notification validation failed: {validation['reason']}"
                }
                
            # Queue notification for delivery
            await self._queue_notification(notification)
            
            # Update trust metrics
            await self._update_trust_metrics(source_id, "sent")
            
            return {
                "success": True,
                "notification_id": notification.notification_id,
                "trust_level": notification.trust_level.value,
                "trust_score": notification.trust_score,
                "priority": notification.priority.value,
                "queued": True
            }
            
        except Exception as e:
            logger.error(f"Error sending notification: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
    async def verify_notification(self,
                                notification_id: str,
                                recipient_id: str) -> Dict[str, Any]:
        """Verify a notification's authenticity"""
        try:
            # Retrieve notification
            notification = await self._get_notification(notification_id)
            
            if not notification:
                return {
                    "verified": False,
                    "error": "Notification not found"
                }
                
            # Check recipient
            if notification.recipient_id != recipient_id:
                return {
                    "verified": False,
                    "error": "Unauthorized recipient"
                }
                
            # Verify signature if present
            if notification.signature:
                signature_valid = await self._verify_signature(
                    notification,
                    notification.signature
                )
                
                if not signature_valid:
                    await self._report_suspicious_notification(notification)
                    return {
                        "verified": False,
                        "error": "Invalid signature"
                    }
                    
            # Verify source trust
            source_trusted = await self._verify_source_trust(notification.source_id)
            
            # Check expiration
            if notification.expires_at and datetime.utcnow() > notification.expires_at:
                return {
                    "verified": False,
                    "error": "Notification expired"
                }
                
            return {
                "verified": True,
                "trust_level": notification.trust_level.value,
                "trust_score": notification.trust_score,
                "source_trusted": source_trusted,
                "verification_proof": notification.verification_proof
            }
            
        except Exception as e:
            logger.error(f"Error verifying notification: {e}")
            return {
                "verified": False,
                "error": str(e)
            }
            
    async def update_notification_feedback(self,
                                         notification_id: str,
                                         recipient_id: str,
                                         feedback: str,
                                         trust_impact: float) -> Dict[str, Any]:
        """Update trust metrics based on user feedback"""
        try:
            # Get notification
            notification = await self._get_notification(notification_id)
            
            if not notification or notification.recipient_id != recipient_id:
                return {
                    "success": False,
                    "error": "Invalid notification or recipient"
                }
                
            # Update source trust metrics
            await self._update_trust_metrics(
                notification.source_id,
                "feedback",
                feedback,
                trust_impact
            )
            
            # Store feedback
            await self._store_feedback(
                notification_id,
                recipient_id,
                feedback,
                trust_impact
            )
            
            return {
                "success": True,
                "updated_trust_score": await self._get_source_trust_score(
                    notification.source_id
                )
            }
            
        except Exception as e:
            logger.error(f"Error updating feedback: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
    async def register_trusted_source(self,
                                    source_id: str,
                                    source_name: str,
                                    public_key: str,
                                    initial_trust_score: float = 0.8) -> Dict[str, Any]:
        """Register a trusted notification source"""
        try:
            # Parse public key
            public_key_obj = serialization.load_pem_public_key(
                public_key.encode()
            )
            
            # Register source
            self.trusted_sources[source_id] = {
                "name": source_name,
                "public_key": public_key_obj,
                "registered_at": datetime.utcnow(),
                "active": True
            }
            
            # Initialize trust metrics
            self.trust_metrics[source_id] = TrustMetrics(
                source_id=source_id,
                trust_score=initial_trust_score,
                verification_count=0,
                rejection_count=0,
                user_feedback_score=1.0,
                last_updated=datetime.utcnow()
            )
            
            return {
                "success": True,
                "source_id": source_id,
                "initial_trust_score": initial_trust_score
            }
            
        except Exception as e:
            logger.error(f"Error registering trusted source: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
    async def get_notification_trust_report(self,
                                          recipient_id: str,
                                          time_range: timedelta = timedelta(days=30)) -> Dict[str, Any]:
        """Generate trust report for recipient's notifications"""
        try:
            cutoff_time = datetime.utcnow() - time_range
            
            # Get recipient's notifications
            notifications = await self._get_recipient_notifications(
                recipient_id,
                cutoff_time
            )
            
            # Analyze trust levels
            trust_distribution = {
                level.value: 0 for level in NotificationTrustLevel
            }
            
            suspicious_sources = []
            verified_count = 0
            total_trust_score = 0
            
            for notification in notifications:
                trust_distribution[notification.trust_level.value] += 1
                total_trust_score += notification.trust_score
                
                if notification.trust_level == NotificationTrustLevel.VERIFIED:
                    verified_count += 1
                elif notification.trust_level == NotificationTrustLevel.SUSPICIOUS:
                    if notification.source_id not in suspicious_sources:
                        suspicious_sources.append(notification.source_id)
                        
            avg_trust_score = total_trust_score / len(notifications) if notifications else 0
            
            # Get source metrics
            source_metrics = {}
            unique_sources = set(n.source_id for n in notifications)
            
            for source_id in unique_sources:
                metrics = await self._get_trust_metrics(source_id)
                if metrics:
                    source_metrics[source_id] = {
                        "trust_score": metrics.trust_score,
                        "verification_count": metrics.verification_count,
                        "rejection_count": metrics.rejection_count
                    }
                    
            return {
                "recipient_id": recipient_id,
                "time_range": time_range.days,
                "total_notifications": len(notifications),
                "trust_distribution": trust_distribution,
                "verified_percentage": (verified_count / len(notifications) * 100) if notifications else 0,
                "average_trust_score": avg_trust_score,
                "suspicious_sources": suspicious_sources,
                "source_metrics": source_metrics,
                "recommendations": self._generate_trust_recommendations(
                    trust_distribution,
                    avg_trust_score,
                    suspicious_sources
                )
            }
            
        except Exception as e:
            logger.error(f"Error generating trust report: {e}")
            raise
            
    async def _determine_trust_level(self,
                                   source_id: str,
                                   trust_score: float,
                                   require_verification: bool) -> NotificationTrustLevel:
        """Determine notification trust level"""
        # Check if source is registered as trusted
        if source_id in self.trusted_sources and self.trusted_sources[source_id]["active"]:
            if require_verification:
                return NotificationTrustLevel.VERIFIED
            else:
                return NotificationTrustLevel.TRUSTED
                
        # Check trust score
        if trust_score >= self.trust_threshold:
            return NotificationTrustLevel.STANDARD
        elif trust_score >= 0.3:
            return NotificationTrustLevel.UNVERIFIED
        else:
            return NotificationTrustLevel.SUSPICIOUS
            
    def _calculate_priority(self,
                          trust_level: NotificationTrustLevel,
                          notification_type: str) -> NotificationPriority:
        """Calculate notification priority"""
        # Base priority on notification type
        type_priorities = {
            "security_alert": NotificationPriority.CRITICAL,
            "system_critical": NotificationPriority.CRITICAL,
            "transaction_confirmation": NotificationPriority.HIGH,
            "account_change": NotificationPriority.HIGH,
            "update": NotificationPriority.MEDIUM,
            "info": NotificationPriority.LOW
        }
        
        base_priority = type_priorities.get(
            notification_type,
            NotificationPriority.MEDIUM
        )
        
        # Adjust based on trust level
        if trust_level == NotificationTrustLevel.SUSPICIOUS:
            # Downgrade suspicious notifications
            return NotificationPriority(max(1, base_priority.value - 2))
        elif trust_level == NotificationTrustLevel.VERIFIED:
            # Upgrade verified notifications
            return NotificationPriority(min(5, base_priority.value + 1))
        else:
            return base_priority
            
    async def _sign_notification(self, notification: TrustedNotification) -> str:
        """Sign notification with system private key"""
        # Create signing payload
        payload = {
            "notification_id": notification.notification_id,
            "source_id": notification.source_id,
            "recipient_id": notification.recipient_id,
            "content_hash": hashlib.sha256(
                f"{notification.title}:{notification.content}".encode()
            ).hexdigest(),
            "timestamp": notification.created_at.isoformat()
        }
        
        # Sign with private key
        message = json.dumps(payload, sort_keys=True).encode()
        signature = self.system_private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        
        return signature.hex()
        
    async def _verify_signature(self,
                              notification: TrustedNotification,
                              signature: str) -> bool:
        """Verify notification signature"""
        try:
            # Get source public key
            if notification.source_id in self.trusted_sources:
                public_key = self.trusted_sources[notification.source_id]["public_key"]
            else:
                public_key = self.system_public_key
                
            # Recreate signing payload
            payload = {
                "notification_id": notification.notification_id,
                "source_id": notification.source_id,
                "recipient_id": notification.recipient_id,
                "content_hash": hashlib.sha256(
                    f"{notification.title}:{notification.content}".encode()
                ).hexdigest(),
                "timestamp": notification.created_at.isoformat()
            }
            
            message = json.dumps(payload, sort_keys=True).encode()
            
            # Verify signature
            public_key.verify(
                bytes.fromhex(signature),
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            
            return True
            
        except Exception:
            return False
            
    async def _generate_verification_proof(self,
                                         notification: TrustedNotification) -> Dict[str, Any]:
        """Generate verification proof for notification"""
        return {
            "method": "RSA-PSS-SHA256",
            "timestamp": datetime.utcnow().isoformat(),
            "source_verified": notification.source_id in self.trusted_sources,
            "content_hash": hashlib.sha256(
                f"{notification.title}:{notification.content}".encode()
            ).hexdigest(),
            "trust_chain": await self._get_trust_chain(notification.source_id)
        }
        
    async def _get_trust_chain(self, source_id: str) -> List[Dict[str, Any]]:
        """Get trust chain for a source"""
        chain = []
        
        # Add source registration
        if source_id in self.trusted_sources:
            chain.append({
                "event": "source_registered",
                "timestamp": self.trusted_sources[source_id]["registered_at"].isoformat(),
                "trust_score": 0.8
            })
            
        # Add trust metric updates
        if source_id in self.trust_metrics:
            metrics = self.trust_metrics[source_id]
            chain.append({
                "event": "metrics_updated",
                "timestamp": metrics.last_updated.isoformat(),
                "trust_score": metrics.trust_score,
                "verification_count": metrics.verification_count
            })
            
        return chain
        
    def _generate_notification_id(self) -> str:
        """Generate unique notification ID"""
        timestamp = datetime.utcnow().isoformat()
        random_bytes = np.random.bytes(16)
        data = f"{timestamp}:{random_bytes.hex()}".encode()
        return hashlib.sha256(data).hexdigest()[:16]
        
    def _generate_trust_recommendations(self,
                                      trust_distribution: Dict[str, int],
                                      avg_trust_score: float,
                                      suspicious_sources: List[str]) -> List[str]:
        """Generate trust recommendations"""
        recommendations = []
        
        if trust_distribution.get("suspicious", 0) > 0:
            recommendations.append(
                f"Block or investigate {len(suspicious_sources)} suspicious sources"
            )
            
        if avg_trust_score < 0.7:
            recommendations.append(
                "Consider enabling stricter verification requirements"
            )
            
        verified_pct = trust_distribution.get("verified", 0) / sum(trust_distribution.values()) * 100
        if verified_pct < 50:
            recommendations.append(
                "Encourage sources to register for verified status"
            )
            
        return recommendations
        
    async def _update_trust_metrics(self,
                                  source_id: str,
                                  action: str,
                                  feedback: Optional[str] = None,
                                  trust_impact: Optional[float] = None):
        """Update trust metrics for a source"""
        if source_id not in self.trust_metrics:
            self.trust_metrics[source_id] = TrustMetrics(
                source_id=source_id,
                trust_score=0.5,
                verification_count=0,
                rejection_count=0,
                user_feedback_score=0.5,
                last_updated=datetime.utcnow()
            )
            
        metrics = self.trust_metrics[source_id]
        
        if action == "sent":
            metrics.verification_count += 1
        elif action == "rejected":
            metrics.rejection_count += 1
            metrics.trust_score = max(0, metrics.trust_score - 0.1)
        elif action == "feedback" and trust_impact is not None:
            # Update trust score based on feedback
            metrics.trust_score = max(0, min(1, metrics.trust_score + trust_impact))
            
            # Update feedback score
            if feedback == "helpful":
                metrics.user_feedback_score = min(1, metrics.user_feedback_score + 0.05)
            elif feedback == "spam":
                metrics.user_feedback_score = max(0, metrics.user_feedback_score - 0.1)
                
        metrics.last_updated = datetime.utcnow()


class NotificationDeliveryManager:
    """Manages secure delivery of trust-enhanced notifications"""
    
    def __init__(self, trust_system: TrustEnhancedNotificationSystem):
        self.trust_system = trust_system
        self.delivery_channels = {}
        self.delivery_policies = {}
        
    async def deliver_notification(self,
                                 notification: TrustedNotification,
                                 channels: List[str]) -> Dict[str, Any]:
        """Deliver notification through specified channels"""
        delivery_results = {}
        
        for channel in channels:
            if channel not in self.delivery_channels:
                delivery_results[channel] = {
                    "delivered": False,
                    "error": "Channel not configured"
                }
                continue
                
            # Apply delivery policy based on trust level
            policy = self._get_delivery_policy(
                notification.trust_level,
                channel
            )
            
            if not policy["allowed"]:
                delivery_results[channel] = {
                    "delivered": False,
                    "error": "Trust level insufficient for channel"
                }
                continue
                
            # Deliver with appropriate security measures
            result = await self._deliver_to_channel(
                notification,
                channel,
                policy
            )
            
            delivery_results[channel] = result
            
        return {
            "notification_id": notification.notification_id,
            "delivery_results": delivery_results,
            "all_delivered": all(r.get("delivered", False) for r in delivery_results.values())
        }
        
    def _get_delivery_policy(self,
                           trust_level: NotificationTrustLevel,
                           channel: str) -> Dict[str, Any]:
        """Get delivery policy based on trust level and channel"""
        # High-security channels require higher trust
        if channel in ["sms", "push"]:
            return {
                "allowed": trust_level in [
                    NotificationTrustLevel.VERIFIED,
                    NotificationTrustLevel.TRUSTED
                ],
                "rate_limit": 10 if trust_level == NotificationTrustLevel.VERIFIED else 5,
                "require_confirmation": trust_level != NotificationTrustLevel.VERIFIED
            }
        else:
            # Email and in-app allow more notifications
            return {
                "allowed": trust_level != NotificationTrustLevel.SUSPICIOUS,
                "rate_limit": 50,
                "require_confirmation": False
            } 