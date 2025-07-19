"""
Auth Service - Vault & Consul Integration
Based on the integration guide in docs/integration-guides/auth-service-vault-consul.md
"""

from typing import Dict, Any, Optional, List
import asyncio
from datetime import datetime, timedelta
from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from dataclasses import dataclass
import logging
import secrets
from jose import jwt, JWTError
import hashlib

logger = logging.getLogger(__name__)

@dataclass
class AuthConfig:
    """Auth service configuration from Consul"""
    # Rate limits
    login_attempts_limit: int = 5
    password_reset_limit: int = 3
    api_request_limit: int = 100
    rate_limit_window_minutes: int = 15
    
    # Session config
    session_timeout_minutes: int = 30
    max_concurrent_sessions: int = 5
    session_renewal_threshold: int = 5
    
    # Password policy
    password_min_length: int = 12
    password_require_uppercase: bool = True
    password_require_numbers: bool = True
    password_require_special: bool = True
    password_history_count: int = 5
    
    # Features
    oauth_enabled: bool = True
    mfa_required: bool = False
    passwordless_enabled: bool = True
    biometric_enabled: bool = False


class AuthServiceVaultIntegration:
    """Vault integration for authentication service"""
    
    def __init__(self, vault_client: VaultClient, service_name: str = "auth-service"):
        self.vault = vault_client
        self.service_name = service_name
        self._key_cache: Dict[str, Any] = {}
        self._rotation_tasks: Dict[str, asyncio.Task] = {}
        
    async def initialize(self):
        """Initialize Vault integration and start key rotation monitoring"""
        # Ensure all required secrets exist
        await self._ensure_secrets_exist()
        
        # Start key rotation monitoring
        await self._start_rotation_monitoring()
        
        # Load initial keys
        await self._load_jwt_keys()
        
        logger.info("Auth service Vault integration initialized")
        
    async def _ensure_secrets_exist(self):
        """Ensure all required secrets exist in Vault"""
        required_secrets = [
            "jwt/access-token-key",
            "jwt/refresh-token-key",
            "jwt/id-token-key",
            "session/encryption-key",
            "session/signing-key",
            "encryption/pii-encryption-key",
            "encryption/password-pepper"
        ]
        
        for secret_path in required_secrets:
            full_path = f"{self.service_name}/{secret_path}"
            try:
                await self.vault.get_secret(full_path)
                logger.debug(f"Secret exists: {full_path}")
            except Exception:
                # Generate initial secret if it doesn't exist
                logger.info(f"Generating initial secret for {full_path}")
                await self._generate_initial_secret(secret_path)
                
    async def _generate_initial_secret(self, secret_path: str):
        """Generate initial secret based on type"""
        if "key" in secret_path or "pepper" in secret_path:
            # Generate a cryptographic key
            key = secrets.token_urlsafe(32)
            metadata = {
                "generated_at": datetime.utcnow().isoformat(),
                "rotation_period": "180d" if "jwt" in secret_path else "90d",
                "algorithm": "HS256" if "jwt" in secret_path else "AES-256-GCM"
            }
        else:
            key = secrets.token_urlsafe(16)
            metadata = {"generated_at": datetime.utcnow().isoformat()}
            
        await self.vault.create_or_update_secret(
            f"{self.service_name}/{secret_path}",
            {"value": key, "metadata": metadata}
        )
        
    async def _load_jwt_keys(self):
        """Pre-load JWT keys into cache"""
        key_types = ["access", "refresh", "id"]
        for key_type in key_types:
            await self.get_jwt_signing_key(key_type)
            
    async def _start_rotation_monitoring(self):
        """Start monitoring for key rotation"""
        # Schedule JWT key rotation every 180 days
        self._rotation_tasks["jwt"] = asyncio.create_task(
            self._rotation_scheduler("jwt", timedelta(days=180))
        )
        
        # Schedule session key rotation every 90 days
        self._rotation_tasks["session"] = asyncio.create_task(
            self._rotation_scheduler("session", timedelta(days=90))
        )
        
    async def _rotation_scheduler(self, key_type: str, interval: timedelta):
        """Schedule key rotation at specified intervals"""
        while True:
            try:
                await asyncio.sleep(interval.total_seconds())
                
                if key_type == "jwt":
                    await self.rotate_jwt_keys()
                elif key_type == "session":
                    await self.rotate_session_keys()
                    
            except Exception as e:
                logger.error(f"Key rotation failed for {key_type}: {e}")
                # Retry after 1 hour on failure
                await asyncio.sleep(3600)
                
    async def get_jwt_signing_key(self, key_type: str = "access") -> str:
        """Get JWT signing key with caching"""
        cache_key = f"jwt/{key_type}-token-key"
        
        # Check cache first
        if cache_key in self._key_cache:
            cached = self._key_cache[cache_key]
            if cached["expires"] > datetime.utcnow():
                return cached["value"]
                
        # Fetch from Vault
        secret_path = f"{self.service_name}/{cache_key}"
        secret = await self.vault.get_secret(secret_path)
        key = secret["value"]
        
        # Cache for 1 hour
        self._key_cache[cache_key] = {
            "value": key,
            "expires": datetime.utcnow() + timedelta(hours=1),
            "version": secret.get("metadata", {}).get("version", 1)
        }
        
        return key
        
    async def get_oauth_credentials(self, provider: str) -> Dict[str, str]:
        """Get OAuth provider credentials"""
        base_path = f"{self.service_name}/oauth/providers/{provider}"
        
        try:
            client_id = await self.vault.get_secret(f"{base_path}/client-id")
            client_secret = await self.vault.get_secret(f"{base_path}/client-secret")
            
            return {
                "client_id": client_id["value"],
                "client_secret": client_secret["value"],
                "redirect_uri": client_id.get("redirect_uri", f"https://platformq.io/auth/callback/{provider}")
            }
        except Exception as e:
            logger.error(f"Failed to get OAuth credentials for {provider}: {e}")
            raise
            
    async def rotate_jwt_keys(self):
        """Rotate JWT signing keys with zero downtime"""
        logger.info("Starting JWT key rotation")
        
        key_types = ["access", "refresh", "id"]
        
        for key_type in key_types:
            secret_path = f"{self.service_name}/jwt/{key_type}-token-key"
            
            try:
                # Get current key for grace period
                current = await self.vault.get_secret(secret_path)
                
                # Generate new key
                new_key = secrets.token_urlsafe(32)
                
                # Update with both keys for grace period
                await self.vault.create_or_update_secret(
                    secret_path,
                    {
                        "value": new_key,
                        "previous": current.get("value"),
                        "rotated_at": datetime.utcnow().isoformat(),
                        "grace_period_ends": (
                            datetime.utcnow() + timedelta(hours=24)
                        ).isoformat(),
                        "metadata": {
                            "version": current.get("metadata", {}).get("version", 1) + 1,
                            "algorithm": "HS256"
                        }
                    }
                )
                
                # Clear cache
                cache_key = f"jwt/{key_type}-token-key"
                self._key_cache.pop(cache_key, None)
                
                logger.info(f"Rotated JWT key: {key_type}")
                
            except Exception as e:
                logger.error(f"Failed to rotate JWT key {key_type}: {e}")
                raise
                
        logger.info("JWT key rotation completed")
        
    async def rotate_session_keys(self):
        """Rotate session encryption and signing keys"""
        logger.info("Starting session key rotation")
        
        key_types = ["encryption-key", "signing-key"]
        
        for key_type in key_types:
            secret_path = f"{self.service_name}/session/{key_type}"
            
            try:
                # Generate new key
                new_key = secrets.token_urlsafe(32)
                
                # Update key
                await self.vault.create_or_update_secret(
                    secret_path,
                    {
                        "value": new_key,
                        "rotated_at": datetime.utcnow().isoformat(),
                        "metadata": {
                            "algorithm": "AES-256-GCM" if "encryption" in key_type else "HMAC-SHA256"
                        }
                    }
                )
                
                logger.info(f"Rotated session key: {key_type}")
                
            except Exception as e:
                logger.error(f"Failed to rotate session key {key_type}: {e}")
                raise
                
        logger.info("Session key rotation completed")
        
    async def verify_jwt_with_rotation(self, token: str, key_type: str = "access") -> Dict:
        """Verify JWT considering key rotation grace period"""
        secret_path = f"{self.service_name}/jwt/{key_type}-token-key"
        secret_data = await self.vault.get_secret(secret_path)
        
        # Try current key first
        try:
            return jwt.decode(token, secret_data["value"], algorithms=["HS256"])
        except JWTError as e:
            # Try previous key if in grace period
            if "previous" in secret_data and secret_data.get("grace_period_ends"):
                grace_end = datetime.fromisoformat(secret_data["grace_period_ends"])
                if datetime.utcnow() < grace_end:
                    try:
                        logger.info("JWT verified with previous key during grace period")
                        return jwt.decode(
                            token, 
                            secret_data["previous"], 
                            algorithms=["HS256"]
                        )
                    except JWTError:
                        pass
                        
            raise e
            
    async def encrypt_pii(self, data: str) -> str:
        """Encrypt PII data using Vault Transit engine"""
        # Ensure Transit key exists
        key_name = f"{self.service_name}-pii"
        
        try:
            await self.vault.read_transit_key(key_name)
        except:
            # Create key if it doesn't exist
            await self.vault.create_transit_key(
                key_name,
                key_type="aes256-gcm96",
                derived=True,  # Enable key derivation
                exportable=False
            )
            
        # Use Transit engine for encryption (key never leaves Vault)
        encrypted = await self.vault.encrypt_data(
            mount_point="transit",
            key_name=key_name,
            plaintext=data,
            context=hashlib.sha256(self.service_name.encode()).hexdigest()
        )
        return encrypted["ciphertext"]
        
    async def decrypt_pii(self, ciphertext: str) -> str:
        """Decrypt PII data using Vault Transit engine"""
        key_name = f"{self.service_name}-pii"
        
        decrypted = await self.vault.decrypt_data(
            mount_point="transit",
            key_name=key_name,
            ciphertext=ciphertext,
            context=hashlib.sha256(self.service_name.encode()).hexdigest()
        )
        return decrypted["plaintext"]
        
    async def get_password_pepper(self) -> str:
        """Get password pepper for additional hashing entropy"""
        secret_path = f"{self.service_name}/encryption/password-pepper"
        secret = await self.vault.get_secret(secret_path)
        return secret["value"]


class AuthServiceConsulIntegration:
    """Consul integration for auth service"""
    
    def __init__(self, consul_client: ConsulClient, service_name: str = "auth-service"):
        self.consul = consul_client
        self.service_name = service_name
        self._config_cache: Optional[AuthConfig] = None
        self._watchers: Dict[str, asyncio.Task] = {}
        self._rate_limit_windows: Dict[str, Dict] = {}
        
    async def initialize(self):
        """Initialize Consul integration"""
        # Register service
        await self._register_service()
        
        # Load initial configuration
        await self.reload_config()
        
        # Start configuration watchers
        await self._start_config_watchers()
        
        # Initialize distributed rate limiter
        await self._init_rate_limiter()
        
        logger.info("Auth service Consul integration initialized")
        
    async def _register_service(self):
        """Register auth service with Consul"""
        from platformq_shared.consul.consul_client import ServiceDefinition
        
        service = ServiceDefinition(
            name=self.service_name,
            port=8000,
            address="",  # Will be auto-detected
            tags=["auth", "identity", "critical", "vault-integrated"],
            meta={
                "version": "1.0.0",
                "protocol": "http",
                "capabilities": "jwt,oauth,mfa,passwordless",
                "vault_integration": "true"
            },
            check={
                "http": "http://localhost:8000/health",
                "interval": "10s",
                "timeout": "5s",
                "deregister_critical_service_after": "30s",
                "tls_skip_verify": False
            }
        )
        
        await self.consul.register_service(service)
        logger.info(f"Registered {self.service_name} with Consul")
        
    async def reload_config(self) -> AuthConfig:
        """Reload configuration from Consul"""
        base_path = f"services/{self.service_name}/config"
        
        try:
            # Fetch all config values
            config_data = {}
            
            # Rate limits
            rate_limits = await self.consul.kv_get_prefix(f"{base_path}/rate-limits/")
            for key, value in rate_limits.items():
                config_key = f"{key.replace('-', '_')}_limit"
                config_data[config_key] = int(value)
            
            # Session config
            session_config = await self.consul.kv_get_prefix(f"{base_path}/session/")
            for key, value in session_config.items():
                config_key = f"session_{key.replace('-', '_')}"
                config_data[config_key] = int(value) if value.isdigit() else value
            
            # Password policy
            password_policy = await self.consul.kv_get_prefix(f"{base_path}/password-policy/")
            for key, value in password_policy.items():
                config_key = f"password_{key.replace('-', '_')}"
                config_data[config_key] = self._parse_value(value)
            
            # Features
            features = await self.consul.kv_get_prefix(f"{base_path}/features/")
            for key, value in features.items():
                config_key = key.replace("-", "_")
                config_data[config_key] = value.lower() == "true"
            
            # Create config object
            self._config_cache = AuthConfig(**config_data)
            logger.info("Reloaded auth service configuration from Consul")
            
            return self._config_cache
            
        except Exception as e:
            logger.error(f"Failed to reload configuration: {e}")
            # Return default config on error
            if not self._config_cache:
                self._config_cache = AuthConfig()
            return self._config_cache
            
    def _parse_value(self, value: str) -> Any:
        """Parse string value to appropriate type"""
        if value.lower() in ["true", "false"]:
            return value.lower() == "true"
        if value.isdigit():
            return int(value)
        return value
        
    async def get_config(self) -> AuthConfig:
        """Get cached configuration"""
        if not self._config_cache:
            await self.reload_config()
        return self._config_cache
        
    async def _start_config_watchers(self):
        """Start watching for configuration changes"""
        watch_paths = [
            "rate-limits",
            "session",
            "password-policy", 
            "features"
        ]
        
        for path in watch_paths:
            full_path = f"services/{self.service_name}/config/{path}"
            watcher = asyncio.create_task(
                self._watch_config_changes(full_path)
            )
            self._watchers[path] = watcher
            
    async def _watch_config_changes(self, path: str):
        """Watch for configuration changes"""
        try:
            async for event in self.consul.watch_prefix(path):
                logger.info(f"Configuration changed at {path}")
                await self.reload_config()
                
                # Notify service of config change
                await self._notify_config_change()
        except asyncio.CancelledError:
            logger.info(f"Config watcher cancelled for {path}")
            raise
        except Exception as e:
            logger.error(f"Config watcher error for {path}: {e}")
            
    async def _notify_config_change(self):
        """Notify service components of configuration change"""
        # This would trigger updates to rate limiters, validators, etc.
        logger.info("Configuration change notification sent")
        
    async def _init_rate_limiter(self):
        """Initialize distributed rate limiter"""
        # Clean up old rate limit windows periodically
        asyncio.create_task(self._cleanup_rate_limits())
        
    async def _cleanup_rate_limits(self):
        """Clean up expired rate limit windows"""
        while True:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                # Clean up local cache
                now = datetime.utcnow()
                expired_keys = []
                
                for key, window in self._rate_limit_windows.items():
                    if window["expires"] < now:
                        expired_keys.append(key)
                        
                for key in expired_keys:
                    del self._rate_limit_windows[key]
                    
                logger.debug(f"Cleaned up {len(expired_keys)} expired rate limit windows")
                
            except Exception as e:
                logger.error(f"Rate limit cleanup error: {e}")
                
    async def check_rate_limit(self, key: str, limit: int, window_minutes: int = 15) -> bool:
        """Check distributed rate limit using Consul"""
        current_window = datetime.utcnow().replace(second=0, microsecond=0)
        window_key = f"{key}:{current_window.strftime('%Y%m%d%H%M')}"
        rate_key = f"rate-limits/{self.service_name}/{window_key}"
        
        try:
            # Try to increment atomically
            current = await self.consul.kv_get(rate_key, default="0")
            current_count = int(current)
            
            if current_count >= limit:
                logger.warning(f"Rate limit exceeded for {key}: {current_count}/{limit}")
                return False
                
            # Increment with CAS (Compare-And-Swap) for atomicity
            new_count = current_count + 1
            success = await self.consul.kv_put_cas(
                rate_key, 
                str(new_count),
                cas=current  # Compare-and-swap index
            )
            
            if success:
                # Set TTL for auto-cleanup
                ttl_seconds = window_minutes * 60 + 60  # Add 1 minute buffer
                await self.consul.kv_put(
                    rate_key,
                    str(new_count),
                    ttl=ttl_seconds
                )
                
                # Update local cache
                self._rate_limit_windows[window_key] = {
                    "count": new_count,
                    "expires": current_window + timedelta(minutes=window_minutes + 1)
                }
                
            return success
            
        except Exception as e:
            logger.error(f"Rate limit check failed: {e}")
            # Fail open in case of errors (allow the request)
            return True
            
    async def acquire_session_lock(self, user_id: str, session_id: str) -> Optional[str]:
        """Acquire distributed lock for session management"""
        lock_key = f"session-locks/{self.service_name}/{user_id}"
        
        try:
            # Create session
            consul_session = await self.consul.create_session(
                name=f"auth-session-{session_id}",
                ttl="30s",  # 30 second TTL
                behavior="delete"  # Delete keys on session expiry
            )
            
            # Try to acquire lock
            lock = await self.consul.acquire_lock(
                lock_key,
                session_id=consul_session["ID"],
                value={
                    "session_id": session_id,
                    "locked_at": datetime.utcnow().isoformat(),
                    "user_id": user_id
                }
            )
            
            if lock:
                return consul_session["ID"]
            else:
                # Lock already held
                await self.consul.destroy_session(consul_session["ID"])
                return None
                
        except Exception as e:
            logger.error(f"Failed to acquire session lock: {e}")
            return None
            
    async def release_session_lock(self, user_id: str, consul_session_id: str):
        """Release session lock"""
        try:
            await self.consul.destroy_session(consul_session_id)
            logger.debug(f"Released session lock for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to release session lock: {e}")
            
    async def update_health_status(self, check_name: str, status: str, output: str = ""):
        """Update service health check status"""
        health_key = f"services/{self.service_name}/health-checks/{check_name}"
        
        await self.consul.kv_put(
            health_key,
            {
                "status": status,  # passing, warning, critical
                "output": output,
                "checked_at": datetime.utcnow().isoformat()
            },
            ttl=300  # 5 minute TTL
        )
        
    async def get_feature_flag(self, feature: str, user_id: Optional[str] = None) -> bool:
        """Get feature flag with user-specific overrides"""
        # Check user-specific override first
        if user_id:
            user_flag = await self.consul.kv_get(
                f"feature-flags/{feature}/users/{user_id}",
                default=None
            )
            if user_flag is not None:
                return user_flag.lower() == "true"
                
        # Check service-specific flag
        service_flag = await self.consul.kv_get(
            f"feature-flags/{feature}/services/{self.service_name}",
            default=None
        )
        if service_flag is not None:
            return service_flag.lower() == "true"
            
        # Fall back to global flag
        global_flag = await self.consul.kv_get(
            f"feature-flags/{feature}/global",
            default="false"
        )
        return global_flag.lower() == "true"
        
    async def count_active_sessions(self, user_id: str) -> int:
        """Count active sessions for a user"""
        session_prefix = f"active-sessions/{self.service_name}/{user_id}/"
        sessions = await self.consul.kv_get_prefix(session_prefix)
        
        # Filter out expired sessions
        active_count = 0
        now = datetime.utcnow()
        
        for session_id, session_data in sessions.items():
            if isinstance(session_data, dict):
                expires_at = session_data.get("expires_at")
                if expires_at:
                    expiry = datetime.fromisoformat(expires_at)
                    if expiry > now:
                        active_count += 1
                        
        return active_count
        
    async def register_active_session(self, user_id: str, session_id: str, ttl_minutes: int):
        """Register an active session in Consul"""
        session_key = f"active-sessions/{self.service_name}/{user_id}/{session_id}"
        
        session_data = {
            "session_id": session_id,
            "created_at": datetime.utcnow().isoformat(),
            "expires_at": (datetime.utcnow() + timedelta(minutes=ttl_minutes)).isoformat(),
            "last_activity": datetime.utcnow().isoformat()
        }
        
        await self.consul.kv_put(
            session_key,
            session_data,
            ttl=ttl_minutes * 60 + 300  # TTL with 5 minute buffer
        )
        
    async def remove_active_session(self, user_id: str, session_id: str):
        """Remove an active session from Consul"""
        session_key = f"active-sessions/{self.service_name}/{user_id}/{session_id}"
        await self.consul.kv_delete(session_key) 