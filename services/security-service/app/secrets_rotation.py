"""
Automated Secrets Rotation Service

Handles automatic rotation of database passwords, API keys, and certificates.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import json
import secrets
import string

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from platformq_shared.events import SecretsRotatedEvent
from platformq_shared.event_publisher import EventPublisher
import httpx
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa

logger = logging.getLogger(__name__)


class SecretType(Enum):
    """Types of secrets that can be rotated"""
    DATABASE_PASSWORD = "database_password"
    API_KEY = "api_key"
    JWT_SECRET = "jwt_secret"
    ENCRYPTION_KEY = "encryption_key"
    TLS_CERTIFICATE = "tls_certificate"
    SERVICE_TOKEN = "service_token"
    WEBHOOK_SECRET = "webhook_secret"


@dataclass
class RotationPolicy:
    """Policy for secret rotation"""
    secret_type: SecretType
    rotation_interval: timedelta
    grace_period: timedelta  # Time both old and new secrets are valid
    notification_lead_time: timedelta  # Notify before rotation
    require_manual_approval: bool = False
    validators: List[Callable] = None
    

@dataclass
class SecretMetadata:
    """Metadata for a secret"""
    path: str
    version: int
    created_at: datetime
    rotated_at: Optional[datetime]
    expires_at: Optional[datetime]
    rotation_count: int
    last_used_at: Optional[datetime]
    

class SecretsRotationService:
    """
    Manages automatic rotation of secrets across the platform.
    
    Features:
    - Configurable rotation policies
    - Grace periods for zero-downtime rotation
    - Audit logging
    - Event notifications
    - Rollback capability
    - Health checks
    """
    
    def __init__(self,
                 vault_client: VaultClient,
                 consul_client: ConsulClient,
                 event_publisher: EventPublisher):
        self.vault = vault_client
        self.consul = consul_client
        self.event_publisher = event_publisher
        self.rotation_policies: Dict[str, RotationPolicy] = {}
        self.rotation_history: Dict[str, List[SecretMetadata]] = {}
        self._rotation_tasks: Dict[str, asyncio.Task] = {}
        self._running = False
        
    def add_rotation_policy(self, name: str, policy: RotationPolicy) -> None:
        """Add a rotation policy for a secret"""
        self.rotation_policies[name] = policy
        logger.info(f"Added rotation policy for {name}: {policy}")
        
    async def start(self) -> None:
        """Start the rotation service"""
        self._running = True
        logger.info("Starting secrets rotation service")
        
        # Initialize default rotation policies
        self._initialize_default_policies()
        
        # Start rotation tasks for each policy
        for name, policy in self.rotation_policies.items():
            task = asyncio.create_task(self._rotation_loop(name, policy))
            self._rotation_tasks[name] = task
            
        logger.info(f"Started {len(self._rotation_tasks)} rotation tasks")
        
    async def stop(self) -> None:
        """Stop the rotation service"""
        self._running = False
        
        # Cancel all rotation tasks
        for task in self._rotation_tasks.values():
            task.cancel()
            
        # Wait for tasks to complete
        await asyncio.gather(*self._rotation_tasks.values(), return_exceptions=True)
        
        logger.info("Stopped secrets rotation service")
        
    def _initialize_default_policies(self) -> None:
        """Initialize default rotation policies"""
        # Database passwords - rotate every 30 days
        self.add_rotation_policy("database_passwords", RotationPolicy(
            secret_type=SecretType.DATABASE_PASSWORD,
            rotation_interval=timedelta(days=30),
            grace_period=timedelta(hours=24),
            notification_lead_time=timedelta(days=7),
            validators=[self._validate_database_password]
        ))
        
        # API keys - rotate every 90 days
        self.add_rotation_policy("api_keys", RotationPolicy(
            secret_type=SecretType.API_KEY,
            rotation_interval=timedelta(days=90),
            grace_period=timedelta(days=7),
            notification_lead_time=timedelta(days=14)
        ))
        
        # JWT secrets - rotate every 180 days
        self.add_rotation_policy("jwt_secrets", RotationPolicy(
            secret_type=SecretType.JWT_SECRET,
            rotation_interval=timedelta(days=180),
            grace_period=timedelta(days=30),
            notification_lead_time=timedelta(days=30),
            require_manual_approval=True
        ))
        
        # TLS certificates - rotate 30 days before expiry
        self.add_rotation_policy("tls_certificates", RotationPolicy(
            secret_type=SecretType.TLS_CERTIFICATE,
            rotation_interval=timedelta(days=60),  # Check every 60 days
            grace_period=timedelta(days=7),
            notification_lead_time=timedelta(days=30),
            validators=[self._validate_certificate]
        ))
        
        # Service tokens - rotate every 7 days
        self.add_rotation_policy("service_tokens", RotationPolicy(
            secret_type=SecretType.SERVICE_TOKEN,
            rotation_interval=timedelta(days=7),
            grace_period=timedelta(hours=1),
            notification_lead_time=timedelta(days=1)
        ))
        
    async def _rotation_loop(self, name: str, policy: RotationPolicy) -> None:
        """Main rotation loop for a policy"""
        while self._running:
            try:
                # Check if rotation is needed
                secrets = await self._get_secrets_for_policy(policy)
                
                for secret_path in secrets:
                    if await self._should_rotate(secret_path, policy):
                        # Check if manual approval is required
                        if policy.require_manual_approval:
                            await self._request_approval(secret_path, policy)
                        else:
                            await self._rotate_secret(secret_path, policy)
                            
                # Sleep until next check
                await asyncio.sleep(3600)  # Check every hour
                
            except Exception as e:
                logger.error(f"Error in rotation loop for {name}: {e}")
                await asyncio.sleep(300)  # Retry after 5 minutes
                
    async def _get_secrets_for_policy(self, policy: RotationPolicy) -> List[str]:
        """Get all secrets that match a policy"""
        secrets = []
        
        # Get secret paths based on type
        if policy.secret_type == SecretType.DATABASE_PASSWORD:
            # List all database credentials
            services = await self.consul.kv_get("services/list", [])
            for service in services:
                secrets.append(f"database/creds/{service}")
                
        elif policy.secret_type == SecretType.API_KEY:
            # List all API keys
            secrets.extend(await self.vault.list_secrets("api-keys/"))
            
        elif policy.secret_type == SecretType.JWT_SECRET:
            secrets.append("shared/jwt")
            
        elif policy.secret_type == SecretType.TLS_CERTIFICATE:
            # List all certificates
            certs = await self.vault.list_certificates()
            secrets.extend([f"pki/cert/{cert}" for cert in certs])
            
        elif policy.secret_type == SecretType.SERVICE_TOKEN:
            # List all service tokens
            services = await self.consul.kv_get("services/list", [])
            for service in services:
                secrets.append(f"services/{service}/token")
                
        return secrets
        
    async def _should_rotate(self, secret_path: str, policy: RotationPolicy) -> bool:
        """Check if a secret should be rotated"""
        try:
            # Get secret metadata
            metadata = await self._get_secret_metadata(secret_path)
            
            if not metadata:
                return True  # Rotate if no metadata
                
            # Check if past rotation interval
            if metadata.rotated_at:
                next_rotation = metadata.rotated_at + policy.rotation_interval
                if datetime.utcnow() >= next_rotation - policy.notification_lead_time:
                    return True
                    
            # Check certificate expiry
            if policy.secret_type == SecretType.TLS_CERTIFICATE:
                cert_data = await self.vault.get_secret(secret_path)
                if cert_data:
                    cert = x509.load_pem_x509_certificate(cert_data["certificate"].encode())
                    if cert.not_valid_after - timedelta(days=30) <= datetime.utcnow():
                        return True
                        
            return False
            
        except Exception as e:
            logger.error(f"Error checking rotation for {secret_path}: {e}")
            return False
            
    async def _rotate_secret(self, secret_path: str, policy: RotationPolicy) -> None:
        """Rotate a secret"""
        logger.info(f"Starting rotation for {secret_path}")
        
        try:
            # Generate new secret based on type
            new_secret = await self._generate_new_secret(secret_path, policy)
            
            # Validate new secret
            if policy.validators:
                for validator in policy.validators:
                    if not await validator(new_secret):
                        raise ValueError(f"Validation failed for {secret_path}")
                        
            # Store new version in Vault
            await self._store_new_version(secret_path, new_secret, policy)
            
            # Update services to use new secret
            await self._update_services(secret_path, new_secret, policy)
            
            # Schedule old version cleanup after grace period
            asyncio.create_task(self._cleanup_old_version(
                secret_path,
                policy.grace_period
            ))
            
            # Publish rotation event
            await self.event_publisher.publish_event(
                "platformq.security.secrets-rotated",
                SecretsRotatedEvent(
                    secret_path=secret_path,
                    secret_type=policy.secret_type.value,
                    rotated_at=datetime.utcnow().isoformat(),
                    next_rotation=(datetime.utcnow() + policy.rotation_interval).isoformat()
                )
            )
            
            # Update rotation history
            await self._update_rotation_history(secret_path)
            
            logger.info(f"Successfully rotated {secret_path}")
            
        except Exception as e:
            logger.error(f"Failed to rotate {secret_path}: {e}")
            
            # Attempt rollback
            await self._rollback_rotation(secret_path)
            
            # Send alert
            await self._send_rotation_alert(secret_path, str(e))
            
    async def _generate_new_secret(self, secret_path: str, 
                                  policy: RotationPolicy) -> Dict[str, Any]:
        """Generate a new secret based on type"""
        if policy.secret_type == SecretType.DATABASE_PASSWORD:
            # Generate strong password
            password = self._generate_password(32)
            
            # For dynamic database credentials, use Vault
            if "database/creds/" in secret_path:
                service = secret_path.split("/")[-1]
                creds = await self.vault.generate_database_credentials(
                    f"{service}-readwrite"
                )
                return {
                    "username": creds["username"],
                    "password": creds["password"]
                }
            else:
                return {"password": password}
                
        elif policy.secret_type == SecretType.API_KEY:
            # Generate API key
            return {
                "key": f"sk_{secrets.token_urlsafe(32)}",
                "created_at": datetime.utcnow().isoformat()
            }
            
        elif policy.secret_type == SecretType.JWT_SECRET:
            # Generate JWT secret
            return {
                "secret_key": secrets.token_urlsafe(64),
                "algorithm": "HS256"
            }
            
        elif policy.secret_type == SecretType.TLS_CERTIFICATE:
            # Generate new certificate using Vault PKI
            role = secret_path.split("/")[-1]
            cert_data = await self.vault.generate_certificate(
                role_name=role,
                common_name=f"{role}.platformq.local",
                ttl="90d"
            )
            return cert_data
            
        elif policy.secret_type == SecretType.SERVICE_TOKEN:
            # Generate service token
            return {
                "token": secrets.token_urlsafe(32),
                "expires_at": (datetime.utcnow() + timedelta(days=30)).isoformat()
            }
            
        else:
            raise ValueError(f"Unknown secret type: {policy.secret_type}")
            
    def _generate_password(self, length: int) -> str:
        """Generate a strong password"""
        alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
        return ''.join(secrets.choice(alphabet) for _ in range(length))
        
    async def _validate_database_password(self, secret: Dict[str, Any]) -> bool:
        """Validate database password meets requirements"""
        password = secret.get("password", "")
        
        # Check length
        if len(password) < 16:
            return False
            
        # Check complexity
        has_upper = any(c.isupper() for c in password)
        has_lower = any(c.islower() for c in password)
        has_digit = any(c.isdigit() for c in password)
        has_special = any(c in "!@#$%^&*" for c in password)
        
        return all([has_upper, has_lower, has_digit, has_special])
        
    async def _validate_certificate(self, secret: Dict[str, Any]) -> bool:
        """Validate certificate"""
        try:
            cert_pem = secret.get("certificate", "")
            cert = x509.load_pem_x509_certificate(cert_pem.encode())
            
            # Check validity period
            if cert.not_valid_after <= datetime.utcnow():
                return False
                
            # Check key usage
            key_usage = cert.extensions.get_extension_for_oid(
                x509.oid.ExtensionOID.KEY_USAGE
            )
            if not key_usage.value.digital_signature:
                return False
                
            return True
            
        except Exception:
            return False
            
    async def _store_new_version(self, secret_path: str, 
                                secret_data: Dict[str, Any],
                                policy: RotationPolicy) -> None:
        """Store new version of secret in Vault"""
        # Add metadata
        secret_data["_metadata"] = {
            "rotated_at": datetime.utcnow().isoformat(),
            "rotation_policy": policy.secret_type.value,
            "grace_period_ends": (datetime.utcnow() + policy.grace_period).isoformat(),
            "version": await self._get_next_version(secret_path)
        }
        
        # Store in Vault
        await self.vault.create_or_update_secret(secret_path, secret_data)
        
        # Store in Consul for service discovery
        await self.consul.kv_put(
            f"secrets/{secret_path}/current_version",
            secret_data["_metadata"]["version"]
        )
        
    async def _update_services(self, secret_path: str,
                              new_secret: Dict[str, Any],
                              policy: RotationPolicy) -> None:
        """Update services to use new secret"""
        # Get affected services
        affected_services = await self._get_affected_services(secret_path)
        
        for service in affected_services:
            try:
                # Notify service of rotation via API
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        f"http://{service}:8000/internal/rotate-secret",
                        json={
                            "secret_path": secret_path,
                            "secret_type": policy.secret_type.value,
                            "grace_period_ends": (datetime.utcnow() + policy.grace_period).isoformat()
                        }
                    )
                    response.raise_for_status()
                    
                logger.info(f"Notified {service} of secret rotation")
                
            except Exception as e:
                logger.error(f"Failed to notify {service}: {e}")
                
    async def _get_affected_services(self, secret_path: str) -> List[str]:
        """Get services affected by a secret rotation"""
        affected = []
        
        # Check service dependencies in Consul
        services = await self.consul.list_services()
        
        for service in services:
            # Get service configuration
            config = await self.consul.kv_get(f"services/{service}/config", {})
            
            # Check if service uses this secret
            if secret_path in str(config):
                affected.append(service)
                
        return affected
        
    async def _cleanup_old_version(self, secret_path: str, 
                                  grace_period: timedelta) -> None:
        """Clean up old version after grace period"""
        # Wait for grace period
        await asyncio.sleep(grace_period.total_seconds())
        
        try:
            # Delete old versions from Vault
            versions = await self.vault.list_secret_versions(secret_path)
            if len(versions) > 2:  # Keep current and previous
                for version in versions[:-2]:
                    await self.vault.delete_secret_version(secret_path, version)
                    
            logger.info(f"Cleaned up old versions of {secret_path}")
            
        except Exception as e:
            logger.error(f"Failed to cleanup old versions: {e}")
            
    async def _update_rotation_history(self, secret_path: str) -> None:
        """Update rotation history"""
        metadata = SecretMetadata(
            path=secret_path,
            version=await self._get_current_version(secret_path),
            created_at=datetime.utcnow(),
            rotated_at=datetime.utcnow(),
            expires_at=None,
            rotation_count=len(self.rotation_history.get(secret_path, [])) + 1,
            last_used_at=None
        )
        
        if secret_path not in self.rotation_history:
            self.rotation_history[secret_path] = []
            
        self.rotation_history[secret_path].append(metadata)
        
        # Store in Consul for persistence
        await self.consul.kv_put(
            f"rotation-history/{secret_path}",
            [m.__dict__ for m in self.rotation_history[secret_path]]
        )
        
    async def _rollback_rotation(self, secret_path: str) -> None:
        """Rollback a failed rotation"""
        logger.warning(f"Rolling back rotation for {secret_path}")
        
        try:
            # Get previous version
            versions = await self.vault.list_secret_versions(secret_path)
            if len(versions) >= 2:
                # Restore previous version as current
                previous = await self.vault.get_secret_version(
                    secret_path,
                    versions[-2]
                )
                await self.vault.create_or_update_secret(secret_path, previous)
                
                logger.info(f"Rolled back {secret_path} to previous version")
                
        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            
    async def _send_rotation_alert(self, secret_path: str, error: str) -> None:
        """Send alert for rotation failure"""
        alert = {
            "severity": "critical",
            "service": "secrets-rotation",
            "secret_path": secret_path,
            "error": error,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Send to monitoring system
        await self.event_publisher.publish_event(
            "platformq.security.rotation-failed",
            alert
        )
        
    async def _get_secret_metadata(self, secret_path: str) -> Optional[SecretMetadata]:
        """Get metadata for a secret"""
        try:
            secret = await self.vault.get_secret(secret_path)
            if secret and "_metadata" in secret:
                meta = secret["_metadata"]
                return SecretMetadata(
                    path=secret_path,
                    version=meta.get("version", 1),
                    created_at=datetime.fromisoformat(meta.get("created_at", datetime.utcnow().isoformat())),
                    rotated_at=datetime.fromisoformat(meta["rotated_at"]) if "rotated_at" in meta else None,
                    expires_at=datetime.fromisoformat(meta["expires_at"]) if "expires_at" in meta else None,
                    rotation_count=meta.get("rotation_count", 0),
                    last_used_at=datetime.fromisoformat(meta["last_used_at"]) if "last_used_at" in meta else None
                )
        except Exception:
            return None
            
    async def _get_current_version(self, secret_path: str) -> int:
        """Get current version number"""
        versions = await self.vault.list_secret_versions(secret_path)
        return len(versions)
        
    async def _get_next_version(self, secret_path: str) -> int:
        """Get next version number"""
        return await self._get_current_version(secret_path) + 1
        
    async def _request_approval(self, secret_path: str, policy: RotationPolicy) -> None:
        """Request manual approval for rotation"""
        # Create approval request
        request = {
            "id": secrets.token_urlsafe(16),
            "secret_path": secret_path,
            "policy": policy.secret_type.value,
            "requested_at": datetime.utcnow().isoformat(),
            "expires_at": (datetime.utcnow() + timedelta(days=7)).isoformat()
        }
        
        # Store in approval queue
        await self.consul.kv_put(
            f"rotation-approvals/{request['id']}",
            request
        )
        
        # Send notification
        await self.event_publisher.publish_event(
            "platformq.security.approval-required",
            request
        )
        
        logger.info(f"Approval requested for {secret_path}")
        
    async def health_check(self) -> Dict[str, Any]:
        """Check rotation service health"""
        return {
            "healthy": self._running,
            "active_policies": len(self.rotation_policies),
            "active_tasks": len([t for t in self._rotation_tasks.values() if not t.done()]),
            "rotation_history_size": sum(len(h) for h in self.rotation_history.values())
        } 