"""
Base Service Class for PlatformQ Services

Provides standardized initialization, configuration, and lifecycle management.
"""

import asyncio
import logging
import os
from typing import Dict, Any, Optional, List, Callable
from contextlib import asynccontextmanager
from datetime import datetime
import signal

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from prometheus_fastapi_instrumentator import Instrumentator
import uvloop
from pyignite import Client as IgniteClient
import pulsar
import consul.aio
import hvac
from pydantic import BaseModel

from .auth import AuthConfig, UnifiedAuth
from platformq_direct_comm import DirectCommunicator

# Use uvloop for better performance
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

logger = logging.getLogger(__name__)


class ServiceConfig(BaseModel):
    """Base service configuration"""
    # Service identity
    service_name: str
    service_version: str = "1.0.0"
    service_id: Optional[str] = None  # Auto-generated if not provided
    
    # API settings
    host: str = "0.0.0.0"
    port: int = 8000
    api_prefix: str = "/api/v1"
    
    # CORS settings
    allowed_origins: List[str] = ["*"]
    
    # Dependencies
    ignite_host: str = os.getenv("IGNITE_HOST", "localhost")
    ignite_port: int = int(os.getenv("IGNITE_PORT", "10800"))
    
    pulsar_url: str = os.getenv("PULSAR_URL", "pulsar://localhost:6650")
    pulsar_token: Optional[str] = os.getenv("PULSAR_TOKEN")
    
    consul_host: str = os.getenv("CONSUL_HOST", "localhost")
    consul_port: int = int(os.getenv("CONSUL_PORT", "8500"))
    
    vault_addr: str = os.getenv("VAULT_ADDR", "http://localhost:8200")
    vault_token: Optional[str] = os.getenv("VAULT_TOKEN")
    
    # Features
    enable_direct_comm: bool = True
    enable_monitoring: bool = True
    enable_health_check: bool = True
    enable_vault_consul: bool = True
    
    # Performance
    request_timeout: int = 30
    shutdown_timeout: int = 30
    
    class Config:
        extra = "allow"  # Allow service-specific config


class ServiceMetrics:
    """Standard service metrics"""
    
    def __init__(self, service_name: str):
        self.request_count = Counter(
            f'{service_name}_requests_total',
            'Total requests',
            ['method', 'endpoint', 'status']
        )
        
        self.request_duration = Histogram(
            f'{service_name}_request_duration_seconds',
            'Request duration',
            ['method', 'endpoint']
        )
        
        self.active_requests = Gauge(
            f'{service_name}_active_requests',
            'Active requests'
        )
        
        self.error_count = Counter(
            f'{service_name}_errors_total',
            'Total errors',
            ['error_type']
        )
        
        self.dependency_health = Gauge(
            f'{service_name}_dependency_health',
            'Dependency health status',
            ['dependency']
        )


class PlatformQService:
    """Base class for all PlatformQ services"""
    
    def __init__(self, config: ServiceConfig):
        self.config = config
        self.start_time = datetime.utcnow()
        
        # Generate service ID if not provided
        if not self.config.service_id:
            import uuid
            self.config.service_id = f"{self.config.service_name}-{uuid.uuid4().hex[:8]}"
            
        # Core dependencies
        self.app: Optional[FastAPI] = None
        self.ignite_client: Optional[IgniteClient] = None
        self.pulsar_client: Optional[pulsar.Client] = None
        self.consul_client: Optional[consul.aio.Consul] = None
        self.vault_client: Optional[hvac.Client] = None
        self.direct_comm: Optional[DirectCommunicator] = None
        self.auth: Optional[UnifiedAuth] = None
        self.metrics: Optional[ServiceMetrics] = None
        
        # HTTP client for service-to-service calls
        self.http_client: Optional[httpx.AsyncClient] = None
        
        # Lifecycle hooks
        self._startup_hooks: List[Callable] = []
        self._shutdown_hooks: List[Callable] = []
        
        # Shutdown event
        self._shutdown_event = asyncio.Event()
        
    def create_app(self) -> FastAPI:
        """Create FastAPI application with standard configuration"""
        
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            """Application lifespan manager"""
            await self.startup()
            yield
            await self.shutdown()
            
        self.app = FastAPI(
            title=self.config.service_name,
            version=self.config.service_version,
            lifespan=lifespan
        )
        
        # Add CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=self.config.allowed_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Add standard middleware
        self._add_middleware()
        
        # Add standard routes
        self._add_standard_routes()
        
        # Initialize metrics
        if self.config.enable_monitoring:
            self.metrics = ServiceMetrics(self.config.service_name)
            Instrumentator().instrument(self.app).expose(self.app)
            
        return self.app
        
    def _add_middleware(self):
        """Add standard middleware"""
        
        @self.app.middleware("http")
        async def add_process_time_header(request: Request, call_next):
            """Add request processing time header"""
            import time
            start_time = time.time()
            
            # Track active requests
            if self.metrics:
                self.metrics.active_requests.inc()
                
            try:
                response = await call_next(request)
                process_time = time.time() - start_time
                response.headers["X-Process-Time"] = str(process_time)
                
                # Record metrics
                if self.metrics:
                    self.metrics.request_count.labels(
                        method=request.method,
                        endpoint=request.url.path,
                        status=response.status_code
                    ).inc()
                    
                    self.metrics.request_duration.labels(
                        method=request.method,
                        endpoint=request.url.path
                    ).observe(process_time)
                    
                return response
            finally:
                if self.metrics:
                    self.metrics.active_requests.dec()
                    
        @self.app.exception_handler(Exception)
        async def global_exception_handler(request: Request, exc: Exception):
            """Global exception handler"""
            logger.error(f"Unhandled exception: {exc}", exc_info=True)
            
            if self.metrics:
                self.metrics.error_count.labels(error_type=type(exc).__name__).inc()
                
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Internal server error",
                    "detail": str(exc) if os.getenv("DEBUG") else "An error occurred"
                }
            )
            
    def _add_standard_routes(self):
        """Add standard service routes"""
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint"""
            health_status = await self.check_health()
            status_code = 200 if health_status["status"] == "healthy" else 503
            return JSONResponse(content=health_status, status_code=status_code)
            
        @self.app.get("/ready")
        async def readiness_check():
            """Readiness check endpoint"""
            ready = await self.check_readiness()
            status_code = 200 if ready["ready"] else 503
            return JSONResponse(content=ready, status_code=status_code)
            
        @self.app.get("/metrics")
        async def metrics():
            """Prometheus metrics endpoint"""
            return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
            
        @self.app.get("/info")
        async def service_info():
            """Service information endpoint"""
            return {
                "service_name": self.config.service_name,
                "service_id": self.config.service_id,
                "version": self.config.service_version,
                "uptime_seconds": (datetime.utcnow() - self.start_time).total_seconds()
            }
            
    async def startup(self):
        """Standard startup sequence"""
        logger.info(f"Starting {self.config.service_name} v{self.config.service_version}")
        
        try:
            # Initialize HTTP client
            self.http_client = httpx.AsyncClient(timeout=self.config.request_timeout)
            
            # Initialize authentication
            self.auth = UnifiedAuth(AuthConfig())
            await self.auth.__aenter__()
            
            # Initialize Ignite
            if self.config.ignite_host:
                await self._init_ignite()
                
            # Initialize Pulsar
            if self.config.pulsar_url:
                await self._init_pulsar()
                
            # Initialize Consul
            if self.config.enable_vault_consul and self.config.consul_host:
                await self._init_consul()
                
            # Initialize Vault
            if self.config.enable_vault_consul and self.config.vault_addr:
                await self._init_vault()
                
            # Initialize Direct Communication
            if self.config.enable_direct_comm and self.ignite_client:
                await self._init_direct_comm()
                
            # Run custom startup hooks
            for hook in self._startup_hooks:
                await hook()
                
            # Register with Consul
            if self.consul_client:
                await self._register_service()
                
            # Setup signal handlers
            self._setup_signal_handlers()
            
            logger.info(f"{self.config.service_name} started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start {self.config.service_name}: {e}")
            raise
            
    async def shutdown(self):
        """Standard shutdown sequence"""
        logger.info(f"Shutting down {self.config.service_name}")
        
        # Signal shutdown
        self._shutdown_event.set()
        
        try:
            # Run custom shutdown hooks
            for hook in self._shutdown_hooks:
                await hook()
                
            # Deregister from Consul
            if self.consul_client:
                await self._deregister_service()
                
            # Cleanup Direct Communication
            if self.direct_comm:
                await self.direct_comm.stop()
                
            # Cleanup Pulsar
            if self.pulsar_client:
                self.pulsar_client.close()
                
            # Cleanup Ignite
            if self.ignite_client:
                self.ignite_client.close()
                
            # Cleanup Consul
            if self.consul_client:
                await self.consul_client.close()
                
            # Cleanup auth
            if self.auth:
                await self.auth.__aexit__(None, None, None)
                
            # Cleanup HTTP client
            if self.http_client:
                await self.http_client.aclose()
                
            logger.info(f"{self.config.service_name} shutdown complete")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            
    async def _init_ignite(self):
        """Initialize Apache Ignite connection"""
        try:
            self.ignite_client = IgniteClient()
            self.ignite_client.connect([(self.config.ignite_host, self.config.ignite_port)])
            logger.info("Connected to Apache Ignite")
            
            if self.metrics:
                self.metrics.dependency_health.labels(dependency="ignite").set(1)
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            if self.metrics:
                self.metrics.dependency_health.labels(dependency="ignite").set(0)
            raise
            
    async def _init_pulsar(self):
        """Initialize Apache Pulsar connection"""
        try:
            auth = pulsar.AuthenticationToken(self.config.pulsar_token) if self.config.pulsar_token else None
            self.pulsar_client = pulsar.Client(
                self.config.pulsar_url,
                authentication=auth
            )
            logger.info("Connected to Apache Pulsar")
            
            if self.metrics:
                self.metrics.dependency_health.labels(dependency="pulsar").set(1)
        except Exception as e:
            logger.error(f"Failed to connect to Pulsar: {e}")
            if self.metrics:
                self.metrics.dependency_health.labels(dependency="pulsar").set(0)
            raise
            
    async def _init_consul(self):
        """Initialize Consul connection"""
        try:
            self.consul_client = consul.aio.Consul(
                host=self.config.consul_host,
                port=self.config.consul_port
            )
            logger.info("Connected to Consul")
            
            if self.metrics:
                self.metrics.dependency_health.labels(dependency="consul").set(1)
        except Exception as e:
            logger.error(f"Failed to connect to Consul: {e}")
            if self.metrics:
                self.metrics.dependency_health.labels(dependency="consul").set(0)
                
    async def _init_vault(self):
        """Initialize Vault connection"""
        try:
            self.vault_client = hvac.Client(
                url=self.config.vault_addr,
                token=self.config.vault_token
            )
            if self.vault_client.is_authenticated():
                logger.info("Connected to Vault")
                
                if self.metrics:
                    self.metrics.dependency_health.labels(dependency="vault").set(1)
            else:
                raise Exception("Vault authentication failed")
        except Exception as e:
            logger.error(f"Failed to connect to Vault: {e}")
            if self.metrics:
                self.metrics.dependency_health.labels(dependency="vault").set(0)
                
    async def _init_direct_comm(self):
        """Initialize direct communication"""
        try:
            self.direct_comm = DirectCommunicator(
                service_id=self.config.service_id,
                ignite_client=self.ignite_client,
                batch_size=100,
                process_interval_ms=1.0,
                enable_circuit_breaker=True,
                enable_batching=True,
                enable_compression=True,
                enable_replay=True
            )
            await self.direct_comm.start()
            logger.info("Direct communication initialized")
            
            if self.metrics:
                self.metrics.dependency_health.labels(dependency="direct_comm").set(1)
        except Exception as e:
            logger.error(f"Failed to initialize direct communication: {e}")
            if self.metrics:
                self.metrics.dependency_health.labels(dependency="direct_comm").set(0)
                
    async def _register_service(self):
        """Register service with Consul"""
        try:
            service_def = {
                "ID": self.config.service_id,
                "Name": self.config.service_name,
                "Tags": ["platformq", "market-service"],
                "Address": self.config.host,
                "Port": self.config.port,
                "Check": {
                    "HTTP": f"http://{self.config.host}:{self.config.port}/health",
                    "Interval": "10s",
                    "Timeout": "5s"
                }
            }
            
            await self.consul_client.agent.service.register(service_def)
            logger.info(f"Registered service with Consul: {self.config.service_id}")
        except Exception as e:
            logger.error(f"Failed to register with Consul: {e}")
            
    async def _deregister_service(self):
        """Deregister service from Consul"""
        try:
            await self.consul_client.agent.service.deregister(self.config.service_id)
            logger.info(f"Deregistered service from Consul: {self.config.service_id}")
        except Exception as e:
            logger.error(f"Failed to deregister from Consul: {e}")
            
    def _setup_signal_handlers(self):
        """Setup graceful shutdown signal handlers"""
        def signal_handler(sig, frame):
            logger.info(f"Received signal {sig}, initiating shutdown...")
            asyncio.create_task(self.shutdown())
            
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
    async def check_health(self) -> Dict[str, Any]:
        """Check service health"""
        health = {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "dependencies": {}
        }
        
        # Check Ignite
        if self.ignite_client:
            try:
                # Simple check - try to get cache names
                self.ignite_client.get_cache_names()
                health["dependencies"]["ignite"] = "healthy"
            except:
                health["dependencies"]["ignite"] = "unhealthy"
                health["status"] = "degraded"
                
        # Check Pulsar
        if self.pulsar_client:
            # Pulsar client doesn't have a simple health check
            health["dependencies"]["pulsar"] = "assumed_healthy"
            
        # Check Consul
        if self.consul_client:
            try:
                await self.consul_client.agent.self()
                health["dependencies"]["consul"] = "healthy"
            except:
                health["dependencies"]["consul"] = "unhealthy"
                health["status"] = "degraded"
                
        return health
        
    async def check_readiness(self) -> Dict[str, Any]:
        """Check if service is ready to handle requests"""
        health = await self.check_health()
        return {
            "ready": health["status"] == "healthy",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    def add_startup_hook(self, hook: Callable):
        """Add a custom startup hook"""
        self._startup_hooks.append(hook)
        
    def add_shutdown_hook(self, hook: Callable):
        """Add a custom shutdown hook"""
        self._shutdown_hooks.append(hook)
        
    def create_service_client(self, service_name: str) -> "ServiceClient":
        """Create a client for another service"""
        from .service_client import ServiceClient
        return ServiceClient(
            service_name=service_name,
            consul_client=self.consul_client,
            http_client=self.http_client
        ) 