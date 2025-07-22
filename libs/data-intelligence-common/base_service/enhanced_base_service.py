"""
Enhanced Base Service for DataIntelligence Suite
Includes distributed caching, event-driven patterns, health checks,
rate limiting, circuit breakers, and comprehensive monitoring
"""

import asyncio
import logging
import os
import time
from typing import Dict, Any, Optional, List, Callable, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from contextlib import asynccontextmanager
from collections import deque
import aiohttp
from fastapi import FastAPI, Request, Response, HTTPException, status, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, Gauge, generate_latest
import pulsar
from pybreaker import CircuitBreaker
from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from platformq_shared.monitoring import MetricsCollector, setup_logging
from ..integrations.ignite_client import IgniteClient, IgniteCacheManager, CacheConfig, CacheMode

logger = logging.getLogger(__name__)


# Metrics
REQUEST_COUNT = Counter('service_requests_total', 'Total requests', ['service', 'method', 'status'])
REQUEST_DURATION = Histogram('service_request_duration_seconds', 'Request duration', ['service', 'method'])
ACTIVE_REQUESTS = Gauge('service_active_requests', 'Active requests', ['service'])
CACHE_HITS = Counter('cache_hits_total', 'Cache hits', ['service', 'cache'])
CACHE_MISSES = Counter('cache_misses_total', 'Cache misses', ['service', 'cache'])
CIRCUIT_BREAKER_STATE = Gauge('circuit_breaker_state', 'Circuit breaker state', ['service', 'breaker'])
EVENT_PUBLISHED = Counter('events_published_total', 'Events published', ['service', 'event_type'])
EVENT_CONSUMED = Counter('events_consumed_total', 'Events consumed', ['service', 'event_type'])


@dataclass
class ServiceConfig:
    """Configuration for enhanced base service"""
    name: str
    version: str = "1.0.0"
    
    # Vault & Consul
    vault_addr: str = os.getenv("VAULT_ADDR", "http://localhost:8200")
    vault_token: Optional[str] = os.getenv("VAULT_TOKEN")
    consul_addr: str = os.getenv("CONSUL_ADDR", "http://localhost:8500")
    
    # Ignite
    ignite_nodes: List[Tuple[str, int]] = None
    enable_caching: bool = True
    
    # Pulsar
    pulsar_url: str = os.getenv("PULSAR_URL", "pulsar://localhost:6650")
    enable_events: bool = True
    
    # Rate limiting
    enable_rate_limiting: bool = True
    rate_limit_requests: int = 100
    rate_limit_window: timedelta = timedelta(minutes=1)
    
    # Circuit breaker
    enable_circuit_breaker: bool = True
    circuit_breaker_failures: int = 5
    circuit_breaker_timeout: int = 60
    circuit_breaker_expected_exception: type = Exception
    
    # Health check
    health_check_interval: int = 30
    
    # Monitoring
    enable_metrics: bool = True
    metrics_port: int = 9090
    
    # CORS
    cors_origins: List[str] = None
    
    def __post_init__(self):
        if self.ignite_nodes is None:
            self.ignite_nodes = [("localhost", 10800)]
        if self.cors_origins is None:
            self.cors_origins = ["*"]


class RateLimiter:
    """Token bucket rate limiter"""
    
    def __init__(self, requests: int, window: timedelta):
        self.requests = requests
        self.window = window
        self.requests_per_second = requests / window.total_seconds()
        self._buckets: Dict[str, Tuple[float, float]] = {}
        
    async def is_allowed(self, key: str) -> bool:
        """Check if request is allowed"""
        now = time.time()
        
        if key not in self._buckets:
            self._buckets[key] = (self.requests - 1, now)
            return True
            
        tokens, last_update = self._buckets[key]
        
        # Refill tokens based on time elapsed
        elapsed = now - last_update
        tokens = min(self.requests, tokens + elapsed * self.requests_per_second)
        
        if tokens >= 1:
            self._buckets[key] = (tokens - 1, now)
            return True
            
        self._buckets[key] = (tokens, now)
        return False


class HealthCheck:
    """Health check implementation"""
    
    def __init__(self):
        self.checks: Dict[str, Callable] = {}
        self.last_results: Dict[str, Dict[str, Any]] = {}
        
    def add_check(self, name: str, check_func: Callable) -> None:
        """Add a health check"""
        self.checks[name] = check_func
        
    async def run_checks(self) -> Dict[str, Any]:
        """Run all health checks"""
        results = {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "checks": {}
        }
        
        for name, check_func in self.checks.items():
            try:
                if asyncio.iscoroutinefunction(check_func):
                    result = await check_func()
                else:
                    result = check_func()
                    
                results["checks"][name] = {
                    "status": "healthy" if result else "unhealthy",
                    "details": result
                }
                
                if not result:
                    results["status"] = "unhealthy"
                    
            except Exception as e:
                results["checks"][name] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
                results["status"] = "unhealthy"
                
        self.last_results = results
        return results


class EnhancedBaseService:
    """Enhanced base service with all Phase 3 features"""
    
    def __init__(self, config: ServiceConfig):
        self.config = config
        self.app = FastAPI(
            title=config.name,
            version=config.version,
            docs_url="/docs",
            redoc_url="/redoc"
        )
        
        # Core clients
        self.vault_client: Optional[VaultClient] = None
        self.consul_client: Optional[ConsulClient] = None
        self.ignite_client: Optional[IgniteClient] = None
        self.cache_manager: Optional[IgniteCacheManager] = None
        self.pulsar_client: Optional[pulsar.Client] = None
        
        # Publishers and consumers
        self._publishers: Dict[str, pulsar.Producer] = {}
        self._consumers: Dict[str, pulsar.Consumer] = {}
        
        # Rate limiter
        self.rate_limiter = RateLimiter(
            config.rate_limit_requests,
            config.rate_limit_window
        ) if config.enable_rate_limiting else None
        
        # Circuit breakers
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        
        # Health check
        self.health_check = HealthCheck()
        
        # Metrics collector
        self.metrics_collector = MetricsCollector(config.name) if config.enable_metrics else None
        
        # Setup
        self._setup_middleware()
        self._setup_routes()
        self._setup_exception_handlers()
        
    def _setup_middleware(self):
        """Setup middleware"""
        # CORS
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=self.config.cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"]
        )
        
        # Request tracking
        @self.app.middleware("http")
        async def track_requests(request: Request, call_next):
            start_time = time.time()
            
            # Track active requests
            ACTIVE_REQUESTS.labels(service=self.config.name).inc()
            
            try:
                # Rate limiting
                if self.config.enable_rate_limiting:
                    client_id = request.client.host if request.client else "unknown"
                    if not await self.rate_limiter.is_allowed(client_id):
                        REQUEST_COUNT.labels(
                            service=self.config.name,
                            method=request.method,
                            status=429
                        ).inc()
                        return JSONResponse(
                            status_code=429,
                            content={"detail": "Rate limit exceeded"}
                        )
                
                response = await call_next(request)
                
                # Metrics
                duration = time.time() - start_time
                REQUEST_COUNT.labels(
                    service=self.config.name,
                    method=request.method,
                    status=response.status_code
                ).inc()
                REQUEST_DURATION.labels(
                    service=self.config.name,
                    method=request.method
                ).observe(duration)
                
                return response
                
            finally:
                ACTIVE_REQUESTS.labels(service=self.config.name).dec()
                
    def _setup_routes(self):
        """Setup standard routes"""
        
        @self.app.get("/health")
        async def health():
            """Health check endpoint"""
            return await self.health_check.run_checks()
            
        @self.app.get("/metrics")
        async def metrics():
            """Prometheus metrics endpoint"""
            if not self.config.enable_metrics:
                raise HTTPException(404)
            return Response(content=generate_latest(), media_type="text/plain")
            
        @self.app.get("/info")
        async def info():
            """Service information"""
            return {
                "name": self.config.name,
                "version": self.config.version,
                "features": {
                    "caching": self.config.enable_caching,
                    "events": self.config.enable_events,
                    "rate_limiting": self.config.enable_rate_limiting,
                    "circuit_breaker": self.config.enable_circuit_breaker,
                    "metrics": self.config.enable_metrics
                }
            }
            
    def _setup_exception_handlers(self):
        """Setup exception handlers"""
        
        @self.app.exception_handler(HTTPException)
        async def http_exception_handler(request: Request, exc: HTTPException):
            return JSONResponse(
                status_code=exc.status_code,
                content={"detail": exc.detail}
            )
            
        @self.app.exception_handler(Exception)
        async def general_exception_handler(request: Request, exc: Exception):
            logger.exception(f"Unhandled exception: {exc}")
            return JSONResponse(
                status_code=500,
                content={"detail": "Internal server error"}
            )
            
    @asynccontextmanager
    async def lifespan(self):
        """Manage service lifecycle"""
        # Startup
        await self._startup()
        
        yield
        
        # Shutdown
        await self._shutdown()
        
    async def _startup(self):
        """Initialize service components"""
        logger.info(f"Starting {self.config.name} v{self.config.version}")
        
        # Initialize Vault client
        if self.config.vault_token:
            self.vault_client = VaultClient(
                vault_addr=self.config.vault_addr,
                token=self.config.vault_token
            )
            await self._load_secrets()
            
        # Initialize Consul client
        self.consul_client = ConsulClient(self.config.consul_addr)
        await self._register_service()
        
        # Initialize Ignite client
        if self.config.enable_caching:
            self.ignite_client = IgniteClient(
                nodes=self.config.ignite_nodes,
                partition_aware=True
            )
            await self.ignite_client.connect_async()
            
            self.cache_manager = IgniteCacheManager(self.ignite_client)
            self.cache_manager.initialize_standard_caches()
            
            # Add cache health check
            self.health_check.add_check("cache", self._check_cache_health)
            
        # Initialize Pulsar client
        if self.config.enable_events:
            self.pulsar_client = pulsar.Client(
                self.config.pulsar_url,
                authentication=self._get_pulsar_auth()
            )
            
            # Add Pulsar health check
            self.health_check.add_check("pulsar", self._check_pulsar_health)
            
        # Add standard health checks
        self.health_check.add_check("service", lambda: True)
        
        # Start background tasks
        asyncio.create_task(self._health_check_loop())
        
        logger.info(f"{self.config.name} started successfully")
        
    async def _shutdown(self):
        """Cleanup service components"""
        logger.info(f"Shutting down {self.config.name}")
        
        # Deregister from Consul
        if self.consul_client:
            await self._deregister_service()
            
        # Close Ignite connection
        if self.ignite_client:
            await self.ignite_client.disconnect_async()
            
        # Close Pulsar connections
        if self.pulsar_client:
            for producer in self._publishers.values():
                producer.close()
            for consumer in self._consumers.values():
                consumer.close()
            self.pulsar_client.close()
            
        logger.info(f"{self.config.name} shutdown complete")
        
    async def _load_secrets(self):
        """Load secrets from Vault"""
        if not self.vault_client:
            return
            
        try:
            # Load service-specific secrets
            secrets = await self.vault_client.read_secret(
                f"services/{self.config.name}"
            )
            
            # Apply secrets to environment
            for key, value in secrets.items():
                os.environ[key.upper()] = str(value)
                
            logger.info("Loaded secrets from Vault")
            
        except Exception as e:
            logger.error(f"Failed to load secrets: {e}")
            
    async def _register_service(self):
        """Register service with Consul"""
        if not self.consul_client:
            return
            
        try:
            # Get service port from environment or default
            port = int(os.getenv("SERVICE_PORT", "8000"))
            
            await self.consul_client.register_service(
                name=self.config.name,
                service_id=f"{self.config.name}-{os.getenv('HOSTNAME', 'local')}",
                address=os.getenv("SERVICE_HOST", "localhost"),
                port=port,
                tags=[
                    f"version={self.config.version}",
                    "dataintelligence",
                    "fastapi"
                ],
                check={
                    "http": f"http://localhost:{port}/health",
                    "interval": f"{self.config.health_check_interval}s"
                }
            )
            
            logger.info("Registered with Consul")
            
        except Exception as e:
            logger.error(f"Failed to register with Consul: {e}")
            
    async def _deregister_service(self):
        """Deregister service from Consul"""
        if not self.consul_client:
            return
            
        try:
            await self.consul_client.deregister_service(
                f"{self.config.name}-{os.getenv('HOSTNAME', 'local')}"
            )
            logger.info("Deregistered from Consul")
            
        except Exception as e:
            logger.error(f"Failed to deregister from Consul: {e}")
            
    def _get_pulsar_auth(self):
        """Get Pulsar authentication"""
        # Can be extended to support JWT, OAuth2, etc.
        return None
        
    async def _check_cache_health(self) -> bool:
        """Check cache health"""
        if not self.ignite_client:
            return False
            
        try:
            # Try to perform a simple operation
            test_key = f"health_check_{self.config.name}"
            await self.ignite_client.put_async("configuration", test_key, "healthy")
            value = await self.ignite_client.get_async("configuration", test_key)
            return value == "healthy"
            
        except Exception as e:
            logger.error(f"Cache health check failed: {e}")
            return False
            
    async def _check_pulsar_health(self) -> bool:
        """Check Pulsar health"""
        if not self.pulsar_client:
            return False
            
        try:
            # Check if client is connected
            return True  # Pulsar client doesn't have direct health check
            
        except Exception as e:
            logger.error(f"Pulsar health check failed: {e}")
            return False
            
    async def _health_check_loop(self):
        """Background health check loop"""
        while True:
            try:
                await asyncio.sleep(self.config.health_check_interval)
                await self.health_check.run_checks()
                
            except Exception as e:
                logger.error(f"Health check loop error: {e}")
                
    def get_circuit_breaker(self, name: str) -> CircuitBreaker:
        """Get or create circuit breaker"""
        if not self.config.enable_circuit_breaker:
            return None
            
        if name not in self._circuit_breakers:
            self._circuit_breakers[name] = CircuitBreaker(
                fail_max=self.config.circuit_breaker_failures,
                reset_timeout=self.config.circuit_breaker_timeout,
                expected_exception=self.config.circuit_breaker_expected_exception
            )
            
            # Track circuit breaker state
            def state_change_listener(breaker, old_state, new_state):
                state_value = {"closed": 0, "open": 1, "half_open": 0.5}.get(new_state.name.lower(), -1)
                CIRCUIT_BREAKER_STATE.labels(
                    service=self.config.name,
                    breaker=name
                ).set(state_value)
                
            self._circuit_breakers[name].add_listener(state_change_listener)
            
        return self._circuit_breakers[name]
        
    async def get_cache(self, cache_name: str, key: str) -> Optional[Any]:
        """Get value from cache with metrics"""
        if not self.ignite_client:
            return None
            
        try:
            value = await self.ignite_client.get_async(cache_name, key)
            
            if value is not None:
                CACHE_HITS.labels(service=self.config.name, cache=cache_name).inc()
            else:
                CACHE_MISSES.labels(service=self.config.name, cache=cache_name).inc()
                
            return value
            
        except Exception as e:
            logger.error(f"Cache get failed: {e}")
            CACHE_MISSES.labels(service=self.config.name, cache=cache_name).inc()
            return None
            
    async def put_cache(self, cache_name: str, key: str, value: Any,
                       ttl: Optional[timedelta] = None) -> bool:
        """Put value in cache"""
        if not self.ignite_client:
            return False
            
        try:
            await self.ignite_client.put_async(cache_name, key, value, ttl)
            return True
            
        except Exception as e:
            logger.error(f"Cache put failed: {e}")
            return False
            
    async def publish_event(self, topic: str, event: Dict[str, Any],
                           event_type: Optional[str] = None) -> bool:
        """Publish event to Pulsar"""
        if not self.pulsar_client:
            return False
            
        try:
            # Get or create producer
            if topic not in self._publishers:
                self._publishers[topic] = self.pulsar_client.create_producer(topic)
                
            # Add metadata
            event["_metadata"] = {
                "source": self.config.name,
                "timestamp": datetime.utcnow().isoformat(),
                "event_type": event_type or "generic"
            }
            
            # Publish
            self._publishers[topic].send(
                event.encode() if isinstance(event, str) else 
                str(event).encode()
            )
            
            EVENT_PUBLISHED.labels(
                service=self.config.name,
                event_type=event_type or "generic"
            ).inc()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            return False
            
    def subscribe_event(self, topic: str, subscription: str,
                       handler: Callable[[Dict[str, Any]], None]) -> None:
        """Subscribe to events from Pulsar"""
        if not self.pulsar_client:
            return
            
        async def consumer_loop():
            try:
                # Create consumer
                consumer = self.pulsar_client.subscribe(
                    topic,
                    subscription,
                    consumer_type=pulsar.ConsumerType.Shared
                )
                self._consumers[f"{topic}:{subscription}"] = consumer
                
                while True:
                    msg = consumer.receive()
                    
                    try:
                        # Parse message
                        data = msg.data().decode()
                        event = eval(data) if data.startswith("{") else {"data": data}
                        
                        # Extract metadata
                        metadata = event.get("_metadata", {})
                        event_type = metadata.get("event_type", "generic")
                        
                        EVENT_CONSUMED.labels(
                            service=self.config.name,
                            event_type=event_type
                        ).inc()
                        
                        # Call handler
                        if asyncio.iscoroutinefunction(handler):
                            await handler(event)
                        else:
                            handler(event)
                            
                        # Acknowledge
                        consumer.acknowledge(msg)
                        
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        consumer.negative_acknowledge(msg)
                        
            except Exception as e:
                logger.error(f"Consumer loop error: {e}")
                
        # Start consumer loop
        asyncio.create_task(consumer_loop())
        
    def run(self, host: str = "0.0.0.0", port: int = 8000):
        """Run the service"""
        import uvicorn
        
        # Setup logging
        setup_logging(self.config.name)
        
        # Run with lifespan
        self.app.router.lifespan_context = self.lifespan
        
        uvicorn.run(
            self.app,
            host=host,
            port=port,
            log_config=None  # Use our logging config
        ) 