# Auth Service - Vault & Consul Integration Guide

## Overview
This guide covers integrating authentication services with HashiCorp Vault and Consul for secure secret management and dynamic configuration.

## Vault Integration

### 1. Secret Structure

```yaml
# Vault path structure for auth service
auth-service/
├── jwt/
│   ├── access-token-key      # JWT signing key (rotate: 180d)
│   ├── refresh-token-key     # Refresh token key (rotate: 90d)
│   └── id-token-key          # ID token key (rotate: 180d)
├── oauth/
│   ├── providers/
│   │   ├── github/
│   │   │   ├── client-id
│   │   │   └── client-secret
│   │   ├── google/
│   │   │   ├── client-id
│   │   │   └── client-secret
│   │   └── microsoft/
│   │       ├── client-id
│   │       └── client-secret
│   └── state-encryption-key  # OAuth state parameter encryption
├── mfa/
│   ├── totp-secret          # TOTP generation secret
│   └── backup-codes-key     # Backup codes encryption
├── session/
│   ├── encryption-key       # Session data encryption
│   └── signing-key          # Session cookie signing
├── email/
│   ├── smtp-password
│   ├── sendgrid-api-key
│   └── template-signing-key
└── encryption/
    ├── pii-encryption-key   # PII field encryption
    └── password-pepper      # Additional password hashing entropy
```

### 2. Implementation Code

```python
# auth_service/vault_integration.py
from typing import Dict, Any, Optional
import asyncio
from datetime import datetime, timedelta
from platformq_shared.vault.vault_client import VaultClient
import logging

logger = logging.getLogger(__name__)

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
        
    async def _ensure_secrets_exist(self):
        """Ensure all required secrets exist in Vault"""
        required_secrets = [
            "jwt/access-token-key",
            "jwt/refresh-token-key",
            "session/encryption-key",
            "encryption/pii-encryption-key"
        ]
        
        for secret_path in required_secrets:
            full_path = f"{self.service_name}/{secret_path}"
            try:
                await self.vault.get_secret(full_path)
            except Exception:
                # Generate initial secret if it doesn't exist
                logger.info(f"Generating initial secret for {full_path}")
                await self._generate_initial_secret(secret_path)
                
    async def _generate_initial_secret(self, secret_path: str):
        """Generate initial secret based on type"""
        import secrets
        
        if "key" in secret_path:
            # Generate a cryptographic key
            key = secrets.token_urlsafe(32)
            metadata = {
                "generated_at": datetime.utcnow().isoformat(),
                "rotation_period": "180d" if "jwt" in secret_path else "90d"
            }
        else:
            key = secrets.token_urlsafe(16)
            metadata = {"generated_at": datetime.utcnow().isoformat()}
            
        await self.vault.create_or_update_secret(
            f"{self.service_name}/{secret_path}",
            {"value": key, "metadata": metadata}
        )
        
    async def get_jwt_signing_key(self, key_type: str = "access") -> str:
        """Get JWT signing key with caching"""
        cache_key = f"jwt/{key_type}-token-key"
        
        # Check cache first
        if cache_key in self._key_cache:
            cached = self._key_cache[cache_key]
            if cached["expires"] > datetime.utcnow():
                return cached["value"]
                
        # Fetch from Vault
        secret = await self.vault.get_secret(f"{self.service_name}/{cache_key}")
        key = secret["value"]
        
        # Cache for 1 hour
        self._key_cache[cache_key] = {
            "value": key,
            "expires": datetime.utcnow() + timedelta(hours=1)
        }
        
        return key
        
    async def get_oauth_credentials(self, provider: str) -> Dict[str, str]:
        """Get OAuth provider credentials"""
        base_path = f"{self.service_name}/oauth/providers/{provider}"
        
        client_id = await self.vault.get_secret(f"{base_path}/client-id")
        client_secret = await self.vault.get_secret(f"{base_path}/client-secret")
        
        return {
            "client_id": client_id["value"],
            "client_secret": client_secret["value"]
        }
        
    async def rotate_jwt_keys(self):
        """Rotate JWT signing keys with zero downtime"""
        logger.info("Starting JWT key rotation")
        
        key_types = ["access", "refresh", "id"]
        
        for key_type in key_types:
            secret_path = f"{self.service_name}/jwt/{key_type}-token-key"
            
            # Generate new key
            import secrets
            new_key = secrets.token_urlsafe(32)
            
            # Get current key for grace period
            current = await self.vault.get_secret(secret_path)
            
            # Update with both keys for grace period
            await self.vault.create_or_update_secret(
                secret_path,
                {
                    "value": new_key,
                    "previous": current.get("value"),
                    "rotated_at": datetime.utcnow().isoformat(),
                    "grace_period_ends": (
                        datetime.utcnow() + timedelta(hours=24)
                    ).isoformat()
                }
            )
            
            # Clear cache
            cache_key = f"jwt/{key_type}-token-key"
            self._key_cache.pop(cache_key, None)
            
        logger.info("JWT key rotation completed")
        
    async def verify_jwt_with_rotation(self, token: str, key_type: str = "access") -> Dict:
        """Verify JWT considering key rotation"""
        from jose import jwt, JWTError
        
        secret_path = f"{self.service_name}/jwt/{key_type}-token-key"
        secret_data = await self.vault.get_secret(secret_path)
        
        # Try current key first
        try:
            return jwt.decode(token, secret_data["value"], algorithms=["HS256"])
        except JWTError:
            # Try previous key if in grace period
            if "previous" in secret_data and secret_data.get("grace_period_ends"):
                grace_end = datetime.fromisoformat(secret_data["grace_period_ends"])
                if datetime.utcnow() < grace_end:
                    try:
                        return jwt.decode(
                            token, 
                            secret_data["previous"], 
                            algorithms=["HS256"]
                        )
                    except JWTError:
                        pass
                        
            raise
            
    async def encrypt_pii(self, data: str) -> str:
        """Encrypt PII data using Vault Transit engine"""
        # Use Transit engine for encryption (key never leaves Vault)
        encrypted = await self.vault.encrypt_data(
            mount_point="transit",
            key_name=f"{self.service_name}-pii",
            plaintext=data
        )
        return encrypted["ciphertext"]
        
    async def decrypt_pii(self, ciphertext: str) -> str:
        """Decrypt PII data using Vault Transit engine"""
        decrypted = await self.vault.decrypt_data(
            mount_point="transit",
            key_name=f"{self.service_name}-pii",
            ciphertext=ciphertext
        )
        return decrypted["plaintext"]

# Usage in auth service
class AuthService:
    def __init__(self):
        self.vault_integration = AuthServiceVaultIntegration(vault_client)
        
    async def startup(self):
        await self.vault_integration.initialize()
        
    async def create_access_token(self, user_data: Dict) -> str:
        # Get signing key from Vault
        signing_key = await self.vault_integration.get_jwt_signing_key("access")
        
        # Create token
        from jose import jwt
        return jwt.encode(user_data, signing_key, algorithm="HS256")
        
    async def store_user_email(self, email: str) -> str:
        # Encrypt PII before storage
        return await self.vault_integration.encrypt_pii(email)
```

## Consul Integration

### 1. Configuration Structure

```yaml
# Consul KV structure for auth service
services/auth-service/
├── config/
│   ├── rate-limits/
│   │   ├── login-attempts          # 5
│   │   ├── password-reset         # 3
│   │   └── api-requests           # 100
│   ├── session/
│   │   ├── timeout-minutes        # 30
│   │   ├── max-concurrent         # 5
│   │   └── renewal-threshold      # 5
│   ├── password-policy/
│   │   ├── min-length            # 12
│   │   ├── require-uppercase     # true
│   │   ├── require-numbers       # true
│   │   └── history-count         # 5
│   └── features/
│       ├── oauth-enabled         # true
│       ├── mfa-required          # false
│       └── passwordless-enabled  # true
├── health-checks/
│   ├── database                  # pass/fail
│   ├── cache                     # pass/fail
│   └── external-providers        # pass/fail
└── metrics/
    ├── active-sessions           # counter
    ├── failed-logins            # counter
    └── token-refresh-rate       # gauge
```

### 2. Implementation Code

```python
# auth_service/consul_integration.py
from typing import Dict, Any, Optional, List
import asyncio
from platformq_shared.consul.consul_client import ConsulClient
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class AuthConfig:
    """Auth service configuration from Consul"""
    # Rate limits
    login_attempts_limit: int = 5
    password_reset_limit: int = 3
    api_request_limit: int = 100
    
    # Session config
    session_timeout_minutes: int = 30
    max_concurrent_sessions: int = 5
    session_renewal_threshold: int = 5
    
    # Password policy
    password_min_length: int = 12
    password_require_uppercase: bool = True
    password_require_numbers: bool = True
    password_history_count: int = 5
    
    # Features
    oauth_enabled: bool = True
    mfa_required: bool = False
    passwordless_enabled: bool = True

class AuthServiceConsulIntegration:
    """Consul integration for auth service"""
    
    def __init__(self, consul_client: ConsulClient, service_name: str = "auth-service"):
        self.consul = consul_client
        self.service_name = service_name
        self._config_cache: Optional[AuthConfig] = None
        self._watchers: Dict[str, asyncio.Task] = {}
        
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
        
    async def _register_service(self):
        """Register auth service with Consul"""
        from platformq_shared.consul.consul_client import ServiceDefinition
        
        service = ServiceDefinition(
            name=self.service_name,
            port=8000,
            tags=["auth", "identity", "critical"],
            meta={
                "version": "1.0.0",
                "protocol": "http"
            },
            check={
                "http": "http://localhost:8000/health",
                "interval": "10s",
                "timeout": "5s",
                "deregister_critical_service_after": "30s"
            }
        )
        
        await self.consul.register_service(service)
        logger.info(f"Registered {self.service_name} with Consul")
        
    async def reload_config(self) -> AuthConfig:
        """Reload configuration from Consul"""
        base_path = f"services/{self.service_name}/config"
        
        # Fetch all config values
        config_data = {}
        
        # Rate limits
        rate_limits = await self.consul.kv_get_prefix(f"{base_path}/rate-limits/")
        config_data.update({
            f"{k}_limit": int(v) for k, v in rate_limits.items()
        })
        
        # Session config
        session_config = await self.consul.kv_get_prefix(f"{base_path}/session/")
        config_data.update({
            f"session_{k}": int(v) if v.isdigit() else v 
            for k, v in session_config.items()
        })
        
        # Password policy
        password_policy = await self.consul.kv_get_prefix(f"{base_path}/password-policy/")
        config_data.update({
            f"password_{k}": self._parse_value(v) 
            for k, v in password_policy.items()
        })
        
        # Features
        features = await self.consul.kv_get_prefix(f"{base_path}/features/")
        config_data.update({
            k.replace("-", "_"): v.lower() == "true" 
            for k, v in features.items()
        })
        
        # Create config object
        self._config_cache = AuthConfig(**config_data)
        logger.info("Reloaded auth service configuration from Consul")
        
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
        async for event in self.consul.watch_prefix(path):
            logger.info(f"Configuration changed at {path}")
            await self.reload_config()
            
            # Notify service of config change
            await self._notify_config_change()
            
    async def _notify_config_change(self):
        """Notify service components of configuration change"""
        # This would trigger rate limiter updates, policy changes, etc.
        pass
        
    async def check_rate_limit(self, key: str, limit: int) -> bool:
        """Check distributed rate limit using Consul"""
        current_minute = datetime.utcnow().strftime("%Y%m%d%H%M")
        rate_key = f"rate-limits/{self.service_name}/{key}/{current_minute}"
        
        # Atomic increment
        try:
            current = await self.consul.kv_get(rate_key, default=0)
            if current >= limit:
                return False
                
            # Increment with CAS
            success = await self.consul.kv_put_cas(
                rate_key, 
                current + 1,
                cas=current  # Compare-and-swap
            )
            
            if success:
                # Set TTL of 2 minutes
                await self.consul.kv_put(
                    rate_key,
                    current + 1,
                    ttl=120
                )
                
            return success
            
        except Exception as e:
            logger.error(f"Rate limit check failed: {e}")
            # Fail open in case of errors
            return True
            
    async def acquire_session_lock(self, user_id: str) -> Optional[str]:
        """Acquire distributed lock for session management"""
        lock_key = f"session-locks/{self.service_name}/{user_id}"
        
        lock = await self.consul.acquire_lock(
            lock_key,
            ttl=30,  # 30 second TTL
            value={"locked_at": datetime.utcnow().isoformat()}
        )
        
        return lock.session_id if lock else None
        
    async def update_health_status(self, check_name: str, status: str):
        """Update service health check status"""
        health_key = f"services/{self.service_name}/health-checks/{check_name}"
        await self.consul.kv_put(
            health_key,
            {
                "status": status,
                "checked_at": datetime.utcnow().isoformat()
            }
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

# Usage in auth service
class EnhancedAuthService:
    def __init__(self):
        self.consul_integration = AuthServiceConsulIntegration(consul_client)
        
    async def startup(self):
        await self.consul_integration.initialize()
        
    async def login(self, username: str, password: str) -> Dict:
        # Check rate limit
        config = await self.consul_integration.get_config()
        
        if not await self.consul_integration.check_rate_limit(
            f"login:{username}",
            config.login_attempts_limit
        ):
            raise HTTPException(429, "Too many login attempts")
            
        # Check if passwordless is enabled
        if await self.consul_integration.get_feature_flag("passwordless", username):
            return await self.passwordless_login(username)
            
        # Validate password against policy
        if not self.validate_password_policy(password, config):
            raise HTTPException(400, "Password does not meet requirements")
            
        # Continue with login...
```

## Testing Guide

### 1. Unit Tests

```python
# tests/test_vault_integration.py
import pytest
from unittest.mock import AsyncMock, MagicMock
from auth_service.vault_integration import AuthServiceVaultIntegration

@pytest.fixture
async def vault_client_mock():
    mock = AsyncMock()
    mock.get_secret = AsyncMock()
    mock.create_or_update_secret = AsyncMock()
    return mock

@pytest.mark.asyncio
async def test_jwt_key_rotation(vault_client_mock):
    # Setup
    integration = AuthServiceVaultIntegration(vault_client_mock)
    
    # Mock current key
    vault_client_mock.get_secret.return_value = {
        "value": "old-key-value"
    }
    
    # Test rotation
    await integration.rotate_jwt_keys()
    
    # Verify new key was stored with previous key
    calls = vault_client_mock.create_or_update_secret.call_args_list
    assert len(calls) == 3  # access, refresh, id tokens
    
    for call in calls:
        args, _ = call
        secret_data = args[1]
        assert "value" in secret_data
        assert "previous" in secret_data
        assert secret_data["previous"] == "old-key-value"
        assert "rotated_at" in secret_data
        assert "grace_period_ends" in secret_data

@pytest.mark.asyncio
async def test_pii_encryption(vault_client_mock):
    # Setup
    integration = AuthServiceVaultIntegration(vault_client_mock)
    
    # Mock encryption response
    vault_client_mock.encrypt_data.return_value = {
        "ciphertext": "vault:v1:encrypted-data"
    }
    
    # Test encryption
    result = await integration.encrypt_pii("user@example.com")
    
    # Verify
    assert result == "vault:v1:encrypted-data"
    vault_client_mock.encrypt_data.assert_called_once_with(
        mount_point="transit",
        key_name="auth-service-pii",
        plaintext="user@example.com"
    )
```

### 2. Integration Tests

```python
# tests/test_consul_integration.py
import pytest
from auth_service.consul_integration import AuthServiceConsulIntegration

@pytest.mark.integration
async def test_configuration_loading(consul_client):
    # Setup test data in Consul
    await consul_client.kv_put("services/auth-service/config/rate-limits/login-attempts", "5")
    await consul_client.kv_put("services/auth-service/config/features/mfa-required", "true")
    
    # Test
    integration = AuthServiceConsulIntegration(consul_client)
    config = await integration.reload_config()
    
    # Verify
    assert config.login_attempts_limit == 5
    assert config.mfa_required is True

@pytest.mark.integration  
async def test_distributed_rate_limiting(consul_client):
    integration = AuthServiceConsulIntegration(consul_client)
    
    # Test rate limiting
    key = "test-user-login"
    limit = 3
    
    # Should pass for first attempts
    for i in range(limit):
        assert await integration.check_rate_limit(key, limit) is True
        
    # Should fail after limit
    assert await integration.check_rate_limit(key, limit) is False
```

## Deployment Configuration

### 1. Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: auth-service
  annotations:
    vault.hashicorp.com/agent-inject: "true"
    vault.hashicorp.com/role: "auth-service"
    vault.hashicorp.com/agent-inject-secret-jwt: "auth-service/jwt/access-token-key"
spec:
  template:
    metadata:
      annotations:
        consul.hashicorp.com/connect-inject: "true"
        consul.hashicorp.com/connect-service: "auth-service"
    spec:
      containers:
      - name: auth-service
        env:
        - name: VAULT_ADDR
          value: "http://vault:8200"
        - name: CONSUL_ADDR  
          value: "http://consul:8500"
```

### 2. Docker Compose

```yaml
auth-service:
  image: platformq/auth-service:latest
  environment:
    - VAULT_ADDR=http://vault:8200
    - VAULT_TOKEN=${VAULT_TOKEN}
    - CONSUL_ADDR=http://consul:8500
  depends_on:
    - vault
    - consul
  labels:
    - "consul.service=auth-service"
    - "consul.tags=auth,identity"
```

## Monitoring & Alerting

### 1. Key Metrics

```python
# Prometheus metrics
auth_jwt_rotation_total = Counter(
    'auth_jwt_rotation_total',
    'Total number of JWT key rotations'
)

auth_vault_errors_total = Counter(
    'auth_vault_errors_total',
    'Total number of Vault errors',
    ['operation']
)

auth_consul_config_reload_total = Counter(
    'auth_consul_config_reload_total',
    'Total number of configuration reloads'
)
```

### 2. Alerts

```yaml
# Prometheus alerts
groups:
  - name: auth_service_vault
    rules:
      - alert: AuthServiceVaultConnectionFailed
        expr: auth_vault_errors_total > 10
        for: 5m
        annotations:
          summary: "Auth service cannot connect to Vault"
          
      - alert: JWTKeyRotationOverdue
        expr: time() - auth_last_jwt_rotation_timestamp > 86400 * 180
        annotations:
          summary: "JWT keys haven't been rotated in 180 days"
```

## Security Best Practices

1. **Key Rotation**
   - Automate JWT key rotation every 180 days
   - Maintain 24-hour grace period for zero downtime
   - Monitor rotation success/failure

2. **Secret Access**
   - Use Vault AppRole authentication
   - Implement least privilege policies
   - Audit all secret access

3. **Configuration Management**
   - Version control Consul configurations
   - Use Consul ACLs for write protection
   - Monitor configuration changes

4. **Error Handling**
   - Fail securely (deny by default)
   - Log security events
   - Implement circuit breakers

## Troubleshooting

### Common Issues

1. **Vault Connection Failed**
   ```bash
   # Check Vault status
   vault status
   
   # Verify AppRole
   vault read auth/approle/role/auth-service
   
   # Test authentication
   vault write auth/approle/login role_id=$ROLE_ID secret_id=$SECRET_ID
   ```

2. **Consul Config Not Loading**
   ```bash
   # Check Consul KV
   consul kv get -recurse services/auth-service/
   
   # Verify service registration
   consul catalog services
   
   # Check ACLs
   consul acl token read -id $TOKEN_ID
   ```

3. **JWT Verification Failing During Rotation**
   ```python
   # Add debug logging
   logger.debug(f"Current key: {secret_data.get('value')[:8]}...")
   logger.debug(f"Previous key: {secret_data.get('previous', 'None')[:8]}...")
   logger.debug(f"Grace period ends: {secret_data.get('grace_period_ends')}")
   ``` 