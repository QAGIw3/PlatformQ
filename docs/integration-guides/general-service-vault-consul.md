# General Purpose Service - Vault & Consul Integration Guide

## Overview
This guide provides a comprehensive template for integrating ANY PlatformQ service with Vault and Consul. Use this as a starting point for services not covered by specific guides.

## Quick Start Template

### 1. Basic Service Structure

```python
# your_service/vault_consul_integration.py
from typing import Dict, Any, Optional, List
import asyncio
from datetime import datetime, timedelta
from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from platformq_shared.middleware.security_middleware import SecurityMiddleware
import logging

logger = logging.getLogger(__name__)

class YourServiceIntegration:
    """
    Template for integrating any service with Vault & Consul.
    Customize based on your service's specific needs.
    """
    
    def __init__(self, 
                 vault_client: VaultClient,
                 consul_client: ConsulClient,
                 service_name: str):
        self.vault = vault_client
        self.consul = consul_client
        self.service_name = service_name
        self._config_cache: Dict[str, Any] = {}
        self._secret_cache: Dict[str, Any] = {}
        
    async def initialize(self):
        """Initialize service with Vault & Consul"""
        try:
            # 1. Register with Consul
            await self._register_service()
            
            # 2. Set up Vault secrets
            await self._setup_vault_secrets()
            
            # 3. Load configuration
            await self._load_configuration()
            
            # 4. Start watchers
            await self._start_watchers()
            
            logger.info(f"{self.service_name} initialized with Vault & Consul")
            
        except Exception as e:
            logger.error(f"Failed to initialize {self.service_name}: {e}")
            raise
```

## Vault Integration Patterns

### 1. Standard Secret Structure

```yaml
# Recommended Vault path structure
{service-name}/
├── api-keys/               # External API credentials
│   ├── primary-api-key
│   └── backup-api-key
├── database/               # Database credentials
│   ├── connection-string
│   └── roles/
│       ├── reader
│       └── writer
├── encryption/             # Encryption keys
│   ├── data-encryption-key
│   └── signing-key
├── certificates/           # TLS/SSL certificates
│   ├── server-cert
│   └── client-cert
└── config/                 # Sensitive configuration
    ├── feature-flags
    └── rate-limits
```

### 2. Common Vault Operations

```python
class VaultOperations:
    """Common Vault operations for any service"""
    
    async def get_api_credentials(self, provider: str) -> Dict[str, str]:
        """Get external API credentials"""
        path = f"{self.service_name}/api-keys/{provider}"
        
        try:
            secret = await self.vault.get_secret(path)
            return {
                "api_key": secret.get("api_key"),
                "api_secret": secret.get("api_secret"),
                "endpoint": secret.get("endpoint", "https://api.provider.com")
            }
        except Exception as e:
            logger.error(f"Failed to get {provider} credentials: {e}")
            raise
            
    async def get_database_connection(self, role: str = "reader") -> Dict[str, Any]:
        """Get database connection with dynamic credentials"""
        # For dynamic credentials
        if await self._supports_dynamic_credentials():
            creds = await self.vault.generate_database_credentials(
                database=self.service_name,
                role=role,
                ttl="1h"
            )
            return {
                "username": creds["username"],
                "password": creds["password"],
                "lease_id": creds["lease_id"]
            }
        
        # For static credentials
        path = f"{self.service_name}/database/connection-string"
        secret = await self.vault.get_secret(path)
        return {"connection_string": secret["value"]}
        
    async def encrypt_sensitive_data(self, data: str, context: str = "default") -> str:
        """Encrypt sensitive data using Transit engine"""
        key_name = f"{self.service_name}-{context}"
        
        # Ensure key exists
        try:
            await self.vault.read_transit_key(key_name)
        except:
            await self.vault.create_transit_key(
                key_name,
                type="aes256-gcm96",
                exportable=False
            )
            
        # Encrypt data
        result = await self.vault.encrypt_data(
            mount_point="transit",
            key_name=key_name,
            plaintext=data
        )
        
        return result["ciphertext"]
        
    async def rotate_api_keys(self):
        """Rotate API keys with zero downtime"""
        providers = await self._list_api_providers()
        
        for provider in providers:
            old_path = f"{self.service_name}/api-keys/{provider}"
            new_path = f"{self.service_name}/api-keys/{provider}-new"
            
            # Get current key
            current = await self.vault.get_secret(old_path)
            
            # Generate new key (this would be provider-specific)
            new_key = await self._generate_new_api_key(provider)
            
            # Store new key
            await self.vault.create_or_update_secret(new_path, new_key)
            
            # Update with grace period
            await self.vault.create_or_update_secret(old_path, {
                **new_key,
                "previous_key": current.get("api_key"),
                "rotation_time": datetime.utcnow().isoformat(),
                "grace_period_ends": (datetime.utcnow() + timedelta(hours=24)).isoformat()
            })
            
    async def manage_certificates(self) -> Dict[str, str]:
        """Get or generate TLS certificates"""
        cert_path = f"{self.service_name}/certificates/server-cert"
        
        try:
            # Try to get existing certificate
            cert_data = await self.vault.get_secret(cert_path)
            
            # Check expiration
            from cryptography import x509
            from cryptography.hazmat.backends import default_backend
            
            cert = x509.load_pem_x509_certificate(
                cert_data["certificate"].encode(),
                default_backend()
            )
            
            # Renew if expiring within 30 days
            if cert.not_valid_after - datetime.utcnow() < timedelta(days=30):
                raise Exception("Certificate expiring soon")
                
            return cert_data
            
        except:
            # Generate new certificate using PKI engine
            cert = await self.vault.generate_certificate(
                mount_point="pki",
                role="internal-services",
                common_name=f"{self.service_name}.platformq.internal",
                ttl="365d"
            )
            
            # Store certificate
            await self.vault.create_or_update_secret(cert_path, {
                "certificate": cert["certificate"],
                "private_key": cert["private_key"],
                "ca_chain": cert["ca_chain"],
                "serial_number": cert["serial_number"],
                "expiration": cert["expiration"]
            })
            
            return cert
```

## Consul Integration Patterns

### 1. Standard Configuration Structure

```yaml
# Recommended Consul KV structure
services/{service-name}/
├── config/
│   ├── features/           # Feature flags
│   │   ├── enable-v2-api   # true/false
│   │   └── beta-features   # ["feature1", "feature2"]
│   ├── limits/             # Rate limits & quotas
│   │   ├── requests-per-minute  # 1000
│   │   └── max-payload-size     # 10485760
│   ├── timeouts/           # Timeout configurations
│   │   ├── request-timeout  # 30
│   │   └── idle-timeout     # 300
│   └── dependencies/       # Service dependencies
│       ├── required        # ["auth-service", "storage-service"]
│       └── optional        # ["cache-service"]
├── health/
│   ├── status             # healthy/unhealthy/degraded
│   ├── last-check         # 2024-01-01T10:00:00Z
│   └── dependencies/      # Health of dependencies
│       ├── database       # healthy
│       └── cache          # unhealthy
├── discovery/
│   ├── endpoints/         # Service endpoints
│   │   ├── primary        # https://service.internal:8000
│   │   └── secondary      # https://service-backup.internal:8000
│   └── capabilities/      # Service capabilities
│       └── versions       # ["v1", "v2"]
└── operations/
    ├── maintenance-mode   # false
    ├── circuit-breakers/  # Circuit breaker states
    └── metrics/           # Runtime metrics
```

### 2. Common Consul Operations

```python
class ConsulOperations:
    """Common Consul operations for any service"""
    
    async def register_service(self):
        """Register service with health checks"""
        from platformq_shared.consul.consul_client import ServiceDefinition
        
        # Determine service metadata
        metadata = await self._get_service_metadata()
        
        service = ServiceDefinition(
            name=self.service_name,
            port=metadata.get("port", 8000),
            tags=metadata.get("tags", ["api", "http"]),
            meta={
                "version": metadata.get("version", "1.0.0"),
                "protocol": metadata.get("protocol", "http"),
                "environment": metadata.get("environment", "production")
            },
            check={
                "http": f"http://localhost:{metadata.get('port', 8000)}/health",
                "interval": "10s",
                "timeout": "5s",
                "deregister_critical_service_after": "60s"
            }
        )
        
        await self.consul.register_service(service)
        
        # Register service endpoints
        await self._register_endpoints()
        
    async def get_feature_flag(self, feature: str, default: Any = False) -> Any:
        """Get feature flag with caching"""
        cache_key = f"feature_{feature}"
        
        # Check cache
        if cache_key in self._config_cache:
            cached = self._config_cache[cache_key]
            if cached["expires"] > datetime.utcnow():
                return cached["value"]
                
        # Get from Consul
        path = f"services/{self.service_name}/config/features/{feature}"
        value = await self.consul.kv_get(path, default=default)
        
        # Parse value
        if isinstance(value, str):
            if value.lower() in ["true", "false"]:
                value = value.lower() == "true"
            elif value.isdigit():
                value = int(value)
                
        # Cache for 5 minutes
        self._config_cache[cache_key] = {
            "value": value,
            "expires": datetime.utcnow() + timedelta(minutes=5)
        }
        
        return value
        
    async def get_service_config(self) -> Dict[str, Any]:
        """Get complete service configuration"""
        base_path = f"services/{self.service_name}/config"
        
        # Get all config values
        config = await self.consul.kv_get_prefix(base_path)
        
        # Structure configuration
        return {
            "features": config.get("features", {}),
            "limits": {
                "requests_per_minute": int(config.get("limits/requests-per-minute", 1000)),
                "max_payload_size": int(config.get("limits/max-payload-size", 10485760))
            },
            "timeouts": {
                "request_timeout": int(config.get("timeouts/request-timeout", 30)),
                "idle_timeout": int(config.get("timeouts/idle-timeout", 300))
            },
            "dependencies": config.get("dependencies", {})
        }
        
    async def update_health_status(self, status: str, details: Optional[Dict] = None):
        """Update service health status"""
        health_path = f"services/{self.service_name}/health"
        
        health_data = {
            "status": status,  # healthy, unhealthy, degraded
            "last_check": datetime.utcnow().isoformat(),
            "details": details or {},
            "version": await self._get_service_version()
        }
        
        await self.consul.kv_put(f"{health_path}/status", status)
        await self.consul.kv_put(f"{health_path}/last-check", health_data["last_check"])
        
        # Update dependency health
        if "dependencies" in health_data["details"]:
            for dep, dep_status in health_data["details"]["dependencies"].items():
                await self.consul.kv_put(
                    f"{health_path}/dependencies/{dep}",
                    dep_status
                )
                
    async def coordinate_with_peers(self, operation: str, data: Any) -> bool:
        """Coordinate operations with service peers"""
        coordination_key = f"services/{self.service_name}/coordination/{operation}"
        
        # Create session for coordination
        session = await self.consul.create_session(
            name=f"{self.service_name}-{operation}",
            ttl="30s",
            behavior="delete"
        )
        
        # Try to acquire lock
        lock = await self.consul.acquire_lock(
            coordination_key,
            session_id=session["ID"],
            value=data
        )
        
        if lock:
            try:
                # Perform coordinated operation
                logger.info(f"Acquired lock for {operation}")
                return True
            finally:
                await lock.release()
        else:
            logger.warning(f"Could not acquire lock for {operation}")
            return False
            
    async def implement_circuit_breaker(self, 
                                      operation: str,
                                      failure_threshold: int = 5,
                                      timeout: int = 60) -> bool:
        """Implement circuit breaker pattern"""
        breaker_key = f"services/{self.service_name}/operations/circuit-breakers/{operation}"
        
        # Get current state
        breaker_state = await self.consul.kv_get(breaker_key, default={
            "state": "closed",
            "failures": 0,
            "last_failure": None,
            "last_success": None
        })
        
        # Check state
        if breaker_state["state"] == "open":
            # Check if timeout has passed
            if breaker_state.get("last_failure"):
                last_failure = datetime.fromisoformat(breaker_state["last_failure"])
                if datetime.utcnow() - last_failure > timedelta(seconds=timeout):
                    # Try half-open
                    breaker_state["state"] = "half-open"
                    await self.consul.kv_put(breaker_key, breaker_state)
                else:
                    return False
            else:
                return False
                
        return breaker_state["state"] != "open"
        
    async def record_failure(self, operation: str):
        """Record operation failure for circuit breaker"""
        breaker_key = f"services/{self.service_name}/operations/circuit-breakers/{operation}"
        
        breaker_state = await self.consul.kv_get(breaker_key, default={
            "state": "closed",
            "failures": 0
        })
        
        breaker_state["failures"] += 1
        breaker_state["last_failure"] = datetime.utcnow().isoformat()
        
        # Trip breaker if threshold reached
        if breaker_state["failures"] >= 5:
            breaker_state["state"] = "open"
            logger.error(f"Circuit breaker opened for {operation}")
            
        await self.consul.kv_put(breaker_key, breaker_state)
        
    async def record_success(self, operation: str):
        """Record operation success for circuit breaker"""
        breaker_key = f"services/{self.service_name}/operations/circuit-breakers/{operation}"
        
        breaker_state = await self.consul.kv_get(breaker_key, default={})
        
        breaker_state["state"] = "closed"
        breaker_state["failures"] = 0
        breaker_state["last_success"] = datetime.utcnow().isoformat()
        
        await self.consul.kv_put(breaker_key, breaker_state)
```

## Complete Service Template

### 1. Service Implementation

```python
# your_service/main.py
from fastapi import FastAPI, Depends, HTTPException
from contextlib import asynccontextmanager
import asyncio

class YourService:
    """Complete service implementation with Vault & Consul"""
    
    def __init__(self):
        self.app = FastAPI()
        self.vault_consul = None
        self.config = None
        
    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Application lifespan management"""
        # Startup
        await self.startup()
        yield
        # Shutdown
        await self.shutdown()
        
    async def startup(self):
        """Service startup procedure"""
        # 1. Initialize Vault & Consul
        self.vault_consul = YourServiceIntegration(
            vault_client=await create_vault_client(),
            consul_client=await create_consul_client(),
            service_name="your-service"
        )
        
        await self.vault_consul.initialize()
        
        # 2. Load configuration
        self.config = await self.vault_consul.consul.get_service_config()
        
        # 3. Set up routes
        self._setup_routes()
        
        # 4. Start background tasks
        asyncio.create_task(self._health_check_loop())
        asyncio.create_task(self._config_refresh_loop())
        
        logger.info("Service started successfully")
        
    async def shutdown(self):
        """Service shutdown procedure"""
        # Deregister from Consul
        await self.vault_consul.consul.deregister_service()
        
        # Clean up resources
        logger.info("Service shutdown complete")
        
    def _setup_routes(self):
        """Set up API routes"""
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint"""
            try:
                # Check dependencies
                deps_healthy = await self._check_dependencies()
                
                if deps_healthy:
                    await self.vault_consul.consul.update_health_status("healthy")
                    return {"status": "healthy", "service": "your-service"}
                else:
                    await self.vault_consul.consul.update_health_status("degraded")
                    return {"status": "degraded", "service": "your-service"}
                    
            except Exception as e:
                await self.vault_consul.consul.update_health_status("unhealthy", {
                    "error": str(e)
                })
                raise HTTPException(status_code=503, detail="Service unhealthy")
                
        @self.app.post("/api/v1/process")
        async def process_request(
            data: Dict[str, Any],
            auth: Dict = Depends(self._authenticate)
        ):
            """Main service endpoint"""
            # Check feature flag
            if await self.vault_consul.consul.get_feature_flag("enable-v2-processing"):
                return await self._process_v2(data)
            else:
                return await self._process_v1(data)
                
    async def _authenticate(self, authorization: str = Header(None)) -> Dict:
        """Authenticate requests using Vault"""
        if not authorization:
            raise HTTPException(401, "Missing authorization")
            
        # Verify token with auth service
        # This would use the auth service integration
        return {"user_id": "authenticated-user"}
        
    async def _check_dependencies(self) -> bool:
        """Check health of service dependencies"""
        deps = self.config.get("dependencies", {}).get("required", [])
        
        for dep in deps:
            try:
                # Check dependency health via Consul
                health = await self.vault_consul.consul.check_service_health(dep)
                if not health:
                    logger.warning(f"Dependency {dep} is unhealthy")
                    return False
            except Exception as e:
                logger.error(f"Failed to check {dep}: {e}")
                return False
                
        return True
        
    async def _health_check_loop(self):
        """Periodic health check"""
        while True:
            try:
                await asyncio.sleep(30)  # Every 30 seconds
                await self._check_dependencies()
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                
    async def _config_refresh_loop(self):
        """Periodic configuration refresh"""
        while True:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                self.config = await self.vault_consul.consul.get_service_config()
                logger.info("Configuration refreshed")
            except Exception as e:
                logger.error(f"Config refresh failed: {e}")
```

### 2. Docker Integration

```dockerfile
# Dockerfile
FROM python:3.10-slim

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application
COPY . /app
WORKDIR /app

# Vault agent for automatic secret injection
RUN apt-get update && apt-get install -y wget unzip && \
    wget https://releases.hashicorp.com/vault/1.15.0/vault_1.15.0_linux_amd64.zip && \
    unzip vault_1.15.0_linux_amd64.zip && \
    mv vault /usr/local/bin/

# Consul agent for service mesh
RUN wget https://releases.hashicorp.com/consul/1.16.0/consul_1.16.0_linux_amd64.zip && \
    unzip consul_1.16.0_linux_amd64.zip && \
    mv consul /usr/local/bin/

# Entry point script
COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

```bash
#!/bin/bash
# docker-entrypoint.sh

# Start Consul Connect sidecar
consul connect proxy -sidecar-for ${SERVICE_NAME} &

# Start Vault agent for secret injection
vault agent -config=/vault/agent.hcl &

# Wait for agents to be ready
sleep 5

# Start the application
exec "$@"
```

### 3. Kubernetes Integration

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: your-service
  annotations:
    vault.hashicorp.com/agent-inject: "true"
    vault.hashicorp.com/role: "your-service"
    vault.hashicorp.com/agent-inject-secret-api-keys: "your-service/api-keys"
spec:
  template:
    metadata:
      annotations:
        consul.hashicorp.com/connect-inject: "true"
        consul.hashicorp.com/connect-service: "your-service"
    spec:
      serviceAccountName: your-service
      containers:
      - name: your-service
        image: your-service:latest
        env:
        - name: VAULT_ADDR
          value: "http://vault:8200"
        - name: CONSUL_ADDR
          value: "http://consul:8500"
        - name: SERVICE_NAME
          value: "your-service"
        ports:
        - containerPort: 8000
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 5
```

## Security Best Practices

### 1. Secret Management

```python
# NEVER hardcode secrets
# BAD
API_KEY = "sk-1234567890"  # NEVER DO THIS

# GOOD
api_key = await vault.get_secret("service/api-keys/provider")

# Use short-lived credentials when possible
# GOOD
async with vault.get_database_connection("postgres", ttl="5m") as conn:
    # Connection automatically closed and credentials revoked
    pass
```

### 2. Configuration Management

```python
# Separate configuration from secrets
# Configuration (Consul) - Non-sensitive, dynamic
config = await consul.get_service_config()
rate_limit = config["limits"]["requests_per_minute"]

# Secrets (Vault) - Sensitive, encrypted
api_key = await vault.get_secret("service/api-keys/external")

# Feature flags (Consul) - Dynamic toggles
if await consul.get_feature_flag("new-algorithm"):
    use_new_algorithm()
```

### 3. Zero Trust Principles

```python
# Always authenticate and authorize
@app.middleware("http")
async def security_middleware(request: Request, call_next):
    # Authenticate every request
    auth_result = await authenticate_request(request)
    if not auth_result.authenticated:
        return JSONResponse(status_code=401, content={"error": "Unauthorized"})
        
    # Authorize based on policies
    if not await authorize_action(auth_result.user, request.url.path):
        return JSONResponse(status_code=403, content={"error": "Forbidden"})
        
    # Add security headers
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    
    return response
```

## Monitoring & Observability

### 1. Metrics

```python
# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge

# Service metrics
request_count = Counter(
    'service_requests_total',
    'Total requests',
    ['method', 'endpoint', 'status']
)

request_duration = Histogram(
    'service_request_duration_seconds',
    'Request duration',
    ['method', 'endpoint']
)

active_connections = Gauge(
    'service_active_connections',
    'Active connections'
)

vault_secret_access = Counter(
    'service_vault_secret_access_total',
    'Vault secret access',
    ['secret_path', 'status']
)

consul_config_updates = Counter(
    'service_consul_config_updates_total',
    'Consul configuration updates'
)
```

### 2. Structured Logging

```python
import structlog

logger = structlog.get_logger()

# Log with context
logger.info(
    "request_processed",
    service=self.service_name,
    user_id=auth_result.user_id,
    duration=duration,
    status=response.status_code,
    vault_operations=vault_ops_count,
    consul_operations=consul_ops_count
)
```

### 3. Health Checks

```python
class HealthChecker:
    async def check_vault_connectivity(self) -> bool:
        """Check Vault connectivity"""
        try:
            await self.vault.get_secret(f"{self.service_name}/health-check")
            return True
        except:
            return False
            
    async def check_consul_connectivity(self) -> bool:
        """Check Consul connectivity"""
        try:
            await self.consul.kv_get(f"services/{self.service_name}/health/status")
            return True
        except:
            return False
            
    async def comprehensive_health_check(self) -> Dict[str, Any]:
        """Comprehensive health check"""
        return {
            "service": self.service_name,
            "timestamp": datetime.utcnow().isoformat(),
            "vault": await self.check_vault_connectivity(),
            "consul": await self.check_consul_connectivity(),
            "dependencies": await self.check_dependencies(),
            "metrics": {
                "uptime": self.get_uptime(),
                "request_rate": self.get_request_rate(),
                "error_rate": self.get_error_rate()
            }
        }
```

## Troubleshooting Guide

### Common Issues

1. **Vault Token Expired**
   ```bash
   # Check token
   vault token lookup
   
   # Renew token
   vault token renew
   
   # Use AppRole for automatic renewal
   vault write auth/approle/login role_id=$ROLE_ID secret_id=$SECRET_ID
   ```

2. **Consul Service Not Registered**
   ```bash
   # Check service registration
   consul catalog services
   
   # Check service health
   consul health checks service-name
   
   # Manual registration
   consul services register service.json
   ```

3. **Configuration Not Updating**
   ```python
   # Force configuration refresh
   await self.consul.reload_configurations()
   
   # Check watchers
   for path, watcher in self._watchers.items():
       if not watcher.done():
           logger.info(f"Watcher active for {path}")
   ```

## Migration Checklist

When migrating an existing service to use Vault & Consul:

- [ ] Identify all hardcoded secrets and move to Vault
- [ ] Identify all configuration and move to Consul
- [ ] Set up service registration with health checks
- [ ] Implement dynamic configuration reloading
- [ ] Add secret rotation capabilities
- [ ] Implement circuit breakers
- [ ] Add comprehensive health checks
- [ ] Set up monitoring and alerting
- [ ] Test failover scenarios
- [ ] Document all Vault paths and Consul keys

## Next Steps

1. **Customize for Your Service**
   - Adapt the template to your specific needs
   - Add service-specific Vault paths
   - Define custom Consul configuration

2. **Test Thoroughly**
   - Test secret rotation
   - Test configuration updates
   - Test failure scenarios
   - Load test with dynamic scaling

3. **Monitor and Iterate**
   - Monitor metrics
   - Analyze logs
   - Optimize based on usage patterns
   - Regular security audits 