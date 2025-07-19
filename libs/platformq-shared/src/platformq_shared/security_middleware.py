"""
Security Middleware for PlatformQ Services

Provides unified security features using HashiCorp Vault and Consul:
- Secret management
- Dynamic credentials
- Service discovery
- Distributed locking
- Configuration management
- mTLS and service mesh integration
"""

import os
import logging
import asyncio
from typing import Optional, Dict, Any, Callable, List
from datetime import datetime, timedelta
import hvac
import consul.aio
from fastapi import Request, Response, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp
import json
import base64

logger = logging.getLogger(__name__)


class VaultConsulMiddleware(BaseHTTPMiddleware):
    """Middleware that integrates Vault and Consul security features"""
    
    def __init__(
        self,
        app: ASGIApp,
        service_name: str,
        vault_addr: Optional[str] = None,
        vault_token: Optional[str] = None,
        consul_host: Optional[str] = None,
        consul_port: Optional[int] = None,
        enable_mtls: bool = True,
        enable_rate_limiting: bool = True,
        enable_audit_logging: bool = True
    ):
        super().__init__(app)
        self.service_name = service_name
        self.vault_addr = vault_addr or os.environ.get('VAULT_ADDR', 'http://vault:8200')
        self.vault_token = vault_token or os.environ.get('VAULT_TOKEN')
        self.consul_host = consul_host or os.environ.get('CONSUL_HOST', 'consul')
        self.consul_port = consul_port or int(os.environ.get('CONSUL_PORT', '8500'))
        self.enable_mtls = enable_mtls
        self.enable_rate_limiting = enable_rate_limiting
        self.enable_audit_logging = enable_audit_logging
        
        # Initialize clients
        self.vault_client = None
        self.consul_client = None
        self._initialized = False

    async def dispatch(self, request: Request, call_next):
        """Process requests with security features"""
        # Initialize on first request
        if not self._initialized:
            await self._initialize()
        
        # Add security headers
        request.state.vault_client = self.vault_client
        request.state.consul_client = self.consul_client
        request.state.service_name = self.service_name
        
        # Verify mTLS if enabled
        if self.enable_mtls:
            if not await self._verify_mtls(request):
                return Response(
                    content="mTLS verification failed",
                    status_code=403
                )
        
        # Apply rate limiting if enabled
        if self.enable_rate_limiting:
            if not await self._check_rate_limit(request):
                return Response(
                    content="Rate limit exceeded",
                    status_code=429
                )
        
        # Audit logging
        if self.enable_audit_logging:
            await self._audit_log_request(request)
        
        # Process request
        response = await call_next(request)
        
        # Add security response headers
        response.headers["X-Service-Name"] = self.service_name
        response.headers["X-Request-ID"] = request.state.request_id
        
        return response

    async def _initialize(self):
        """Initialize Vault and Consul clients"""
        try:
            # Initialize Vault client
            self.vault_client = hvac.Client(
                url=self.vault_addr,
                token=self.vault_token
            )
            if not self.vault_client.is_authenticated():
                raise Exception("Vault authentication failed")
            
            # Initialize Consul client
            self.consul_client = consul.aio.Consul(
                host=self.consul_host,
                port=self.consul_port
            )
            
            self._initialized = True
            logger.info(f"Security middleware initialized for {self.service_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize security middleware: {e}")
            raise

    async def _verify_mtls(self, request: Request) -> bool:
        """Verify mTLS certificate"""
        # In production, this would verify the client certificate
        # For now, check for service mesh headers
        mesh_headers = {
            'x-forwarded-client-cert',
            'x-consul-token',
            'x-vault-token'
        }
        
        return any(header in request.headers for header in mesh_headers)

    async def _check_rate_limit(self, request: Request) -> bool:
        """Check rate limits using Consul KV"""
        # Get client identifier
        client_id = request.headers.get('x-api-key', request.client.host)
        
        # Check rate limit in Consul
        rate_key = f"rate-limits/{self.service_name}/{client_id}"
        _, data = await self.consul_client.kv.get(rate_key)
        
        if data:
            rate_data = json.loads(data['Value'])
            requests = rate_data.get('requests', 0)
            window_start = datetime.fromisoformat(rate_data.get('window_start'))
            
            # Check if window expired
            if datetime.utcnow() - window_start > timedelta(minutes=1):
                # Reset window
                requests = 0
                window_start = datetime.utcnow()
            
            # Check limit (default 100 requests per minute)
            limit = rate_data.get('limit', 100)
            if requests >= limit:
                return False
            
            # Update counter
            rate_data['requests'] = requests + 1
            rate_data['window_start'] = window_start.isoformat()
        else:
            # Create new rate limit entry
            rate_data = {
                'requests': 1,
                'window_start': datetime.utcnow().isoformat(),
                'limit': 100
            }
        
        # Store updated rate data
        await self.consul_client.kv.put(rate_key, json.dumps(rate_data))
        
        # Add rate limit headers to request
        request.state.rate_limit_remaining = rate_data['limit'] - rate_data['requests']
        
        return True

    async def _audit_log_request(self, request: Request):
        """Log request for audit purposes"""
        import uuid
        
        # Generate request ID
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        
        # Create audit entry
        audit_entry = {
            'request_id': request_id,
            'service': self.service_name,
            'timestamp': datetime.utcnow().isoformat(),
            'method': request.method,
            'path': request.url.path,
            'client': request.client.host if request.client else 'unknown',
            'headers': dict(request.headers)
        }
        
        # Remove sensitive headers
        sensitive_headers = ['authorization', 'x-api-key', 'cookie']
        for header in sensitive_headers:
            if header in audit_entry['headers']:
                audit_entry['headers'][header] = '[REDACTED]'
        
        # Store in Consul for centralized logging
        audit_key = f"audit-logs/{self.service_name}/{request_id}"
        await self.consul_client.kv.put(
            audit_key,
            json.dumps(audit_entry),
            # Auto-delete after 30 days
            ttl=2592000
        )


class SecureServiceRegistry:
    """Service registry with Consul integration"""
    
    def __init__(self, consul_client: consul.aio.Consul, service_name: str):
        self.consul_client = consul_client
        self.service_name = service_name
        self._service_cache = {}
        self._cache_ttl = 60  # seconds

    async def get_service_url(self, service_name: str) -> str:
        """Get service URL from Consul"""
        # Check cache
        if service_name in self._service_cache:
            cached_data = self._service_cache[service_name]
            if datetime.utcnow() - cached_data['timestamp'] < timedelta(seconds=self._cache_ttl):
                return cached_data['url']
        
        # Query Consul
        _, services = await self.consul_client.health.service(
            service_name,
            passing=True
        )
        
        if not services:
            raise HTTPException(
                status_code=503,
                detail=f"Service {service_name} not available"
            )
        
        # Get first healthy instance
        service = services[0]
        service_url = f"http://{service['Service']['Address']}:{service['Service']['Port']}"
        
        # Cache result
        self._service_cache[service_name] = {
            'url': service_url,
            'timestamp': datetime.utcnow()
        }
        
        return service_url

    async def register_service(
        self,
        address: str,
        port: int,
        tags: List[str],
        health_check_url: str
    ):
        """Register service with Consul"""
        service_id = f"{self.service_name}-{os.environ.get('HOSTNAME', 'local')}"
        
        await self.consul_client.agent.service.register(
            name=self.service_name,
            service_id=service_id,
            address=address,
            port=port,
            tags=tags,
            check=consul.Check.http(
                health_check_url,
                interval="10s",
                timeout="5s",
                deregister_critical_service_after="30s"
            )
        )

    async def deregister_service(self):
        """Deregister service from Consul"""
        service_id = f"{self.service_name}-{os.environ.get('HOSTNAME', 'local')}"
        await self.consul_client.agent.service.deregister(service_id)


class DistributedLockManager:
    """Distributed lock manager using Consul"""
    
    def __init__(self, consul_client: consul.aio.Consul, service_name: str):
        self.consul_client = consul_client
        self.service_name = service_name
        self._active_locks = {}
        self._refresh_tasks = {}

    async def acquire_lock(
        self,
        resource_id: str,
        ttl: int = 3600,
        wait: bool = False,
        timeout: int = 30
    ) -> bool:
        """Acquire distributed lock"""
        lock_key = f"locks/{self.service_name}/{resource_id}"
        
        # Create session
        session_response = await self.consul_client.session.create(
            name=f"{self.service_name}-lock-{resource_id}",
            ttl=ttl,
            behavior='delete'
        )
        session_id = session_response['ID']
        
        # Try to acquire lock
        start_time = datetime.utcnow()
        while True:
            success = await self.consul_client.kv.put(
                lock_key,
                json.dumps({
                    'holder': self.service_name,
                    'session_id': session_id,
                    'acquired_at': datetime.utcnow().isoformat()
                }),
                acquire=session_id
            )
            
            if success:
                self._active_locks[resource_id] = {
                    'session_id': session_id,
                    'lock_key': lock_key
                }
                
                # Start refresh task
                self._refresh_tasks[resource_id] = asyncio.create_task(
                    self._refresh_lock(resource_id, session_id, ttl)
                )
                
                return True
            
            if not wait:
                # Cleanup session
                await self.consul_client.session.destroy(session_id)
                return False
            
            # Check timeout
            if (datetime.utcnow() - start_time).total_seconds() > timeout:
                await self.consul_client.session.destroy(session_id)
                return False
            
            # Wait before retry
            await asyncio.sleep(1)

    async def release_lock(self, resource_id: str):
        """Release distributed lock"""
        if resource_id not in self._active_locks:
            return
        
        lock_data = self._active_locks[resource_id]
        
        # Cancel refresh task
        if resource_id in self._refresh_tasks:
            self._refresh_tasks[resource_id].cancel()
            del self._refresh_tasks[resource_id]
        
        # Release lock
        await self.consul_client.kv.delete(lock_data['lock_key'])
        
        # Destroy session
        await self.consul_client.session.destroy(lock_data['session_id'])
        
        del self._active_locks[resource_id]

    async def _refresh_lock(self, resource_id: str, session_id: str, ttl: int):
        """Refresh lock periodically"""
        refresh_interval = ttl // 3
        
        while resource_id in self._active_locks:
            try:
                await asyncio.sleep(refresh_interval)
                await self.consul_client.session.renew(session_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Failed to refresh lock {resource_id}: {e}")
                break


class SecretManager:
    """Centralized secret management with Vault"""
    
    def __init__(self, vault_client: hvac.Client, service_name: str):
        self.vault_client = vault_client
        self.service_name = service_name
        self.secret_path = f"secret/data/{service_name}"
        self._secret_cache = {}
        self._cache_ttl = 300  # 5 minutes

    async def get_secret(self, key: str, force_refresh: bool = False) -> Optional[str]:
        """Get secret from Vault"""
        # Check cache
        if not force_refresh and key in self._secret_cache:
            cached_data = self._secret_cache[key]
            if datetime.utcnow() - cached_data['timestamp'] < timedelta(seconds=self._cache_ttl):
                return cached_data['value']
        
        # Read from Vault
        try:
            response = self.vault_client.read(f"{self.secret_path}/{key}")
            if response and 'data' in response and 'data' in response['data']:
                value = response['data']['data'].get('value')
                
                # Cache result
                self._secret_cache[key] = {
                    'value': value,
                    'timestamp': datetime.utcnow()
                }
                
                return value
        except Exception as e:
            logger.error(f"Failed to get secret {key}: {e}")
        
        return None

    async def set_secret(self, key: str, value: str):
        """Set secret in Vault"""
        try:
            self.vault_client.write(
                f"{self.secret_path}/{key}",
                value=value
            )
            
            # Update cache
            self._secret_cache[key] = {
                'value': value,
                'timestamp': datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Failed to set secret {key}: {e}")
            raise

    async def encrypt_data(self, data: str, encryption_key: str = "default") -> str:
        """Encrypt data using Vault Transit engine"""
        try:
            plaintext_b64 = base64.b64encode(data.encode()).decode()
            
            response = self.vault_client.write(
                f"transit/encrypt/{encryption_key}",
                plaintext=plaintext_b64
            )
            
            return response['data']['ciphertext']
            
        except Exception as e:
            logger.error(f"Failed to encrypt data: {e}")
            raise

    async def decrypt_data(self, ciphertext: str, encryption_key: str = "default") -> str:
        """Decrypt data using Vault Transit engine"""
        try:
            response = self.vault_client.write(
                f"transit/decrypt/{encryption_key}",
                ciphertext=ciphertext
            )
            
            plaintext_b64 = response['data']['plaintext']
            return base64.b64decode(plaintext_b64).decode()
            
        except Exception as e:
            logger.error(f"Failed to decrypt data: {e}")
            raise


def create_security_dependencies(
    service_name: str,
    vault_addr: Optional[str] = None,
    consul_host: Optional[str] = None
) -> Dict[str, Any]:
    """Create security dependencies for FastAPI"""
    
    # Initialize clients
    vault_client = hvac.Client(
        url=vault_addr or os.environ.get('VAULT_ADDR', 'http://vault:8200'),
        token=os.environ.get('VAULT_TOKEN')
    )
    
    consul_client = consul.aio.Consul(
        host=consul_host or os.environ.get('CONSUL_HOST', 'consul'),
        port=int(os.environ.get('CONSUL_PORT', '8500'))
    )
    
    # Create managers
    service_registry = SecureServiceRegistry(consul_client, service_name)
    lock_manager = DistributedLockManager(consul_client, service_name)
    secret_manager = SecretManager(vault_client, service_name)
    
    return {
        'vault_client': vault_client,
        'consul_client': consul_client,
        'service_registry': service_registry,
        'lock_manager': lock_manager,
        'secret_manager': secret_manager
    }


# Export convenience functions
__all__ = [
    'VaultConsulMiddleware',
    'SecureServiceRegistry',
    'DistributedLockManager',
    'SecretManager',
    'create_security_dependencies'
] 