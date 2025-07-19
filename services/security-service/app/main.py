"""
Security Service

Orchestrates security operations including secrets rotation, policy management,
security monitoring, and Zero-Trust architecture enforcement for PlatformQ.
"""

import os
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Depends, status, Header
from fastapi.responses import JSONResponse
import consul
import json

from platformq_shared import create_base_app
from platformq_shared.vault.vault_client import VaultClient, VaultConfig
from platformq_shared.consul.consul_client import ConsulClient, ConsulConfig
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.authorization.opa_client import OPAClient, OPAConfig
from platformq_shared.security_middleware import (
    VaultConsulMiddleware,
    SecureServiceRegistry,
    DistributedLockManager,
    SecretManager
)

from .secrets_rotation import SecretsRotationService, RotationPolicy, SecretType
from .api.endpoints import router as api_router
from .models import SecurityEvent, PolicyUpdate, SecretRotationStatus
from .monitoring import SecurityMonitor
from .compliance import ComplianceChecker
from .service_mesh import ServiceMeshCoordinator, mTLSManager
from .kong_integration import KongAPIGatewayManager
from .zero_trust import ZeroTrustPolicyEngine

logger = logging.getLogger(__name__)

# Global instances
secrets_rotation_service = None
security_monitor = None
compliance_checker = None
vault_client = None
consul_client = None
opa_client = None
service_mesh_coordinator = None
kong_manager = None
zero_trust_engine = None
mtls_manager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global secrets_rotation_service, security_monitor, compliance_checker
    global vault_client, consul_client, opa_client
    global service_mesh_coordinator, kong_manager, zero_trust_engine, mtls_manager
    
    # Startup
    logger.info("Starting Security Service...")
    
    # Initialize clients
    vault_client = VaultClient(VaultConfig(
        host=os.getenv("VAULT_ADDR", "localhost"),
        port=int(os.getenv("VAULT_PORT", 8200)),
        token=os.getenv("VAULT_TOKEN"),
        role_id=os.getenv("VAULT_ROLE_ID"),
        secret_id=os.getenv("VAULT_SECRET_ID"),
        namespace=os.getenv("VAULT_NAMESPACE", "")
    ))
    await vault_client.initialize()
    app.state.vault_client = vault_client
    
    consul_client = ConsulClient(ConsulConfig(
        host=os.getenv("CONSUL_ADDR", "localhost"),
        port=int(os.getenv("CONSUL_PORT", 8500)),
        token=os.getenv("CONSUL_TOKEN"),
        datacenter=os.getenv("CONSUL_DATACENTER", "dc1"),
        enable_service_mesh=True
    ))
    await consul_client.initialize()
    app.state.consul_client = consul_client
    
    opa_client = OPAClient(OPAConfig(
        host=os.getenv("OPA_ADDR", "localhost"),
        port=int(os.getenv("OPA_PORT", 8181)),
        policy_path="/v1/data/platformq/security"
    ))
    await opa_client.initialize()
    app.state.opa_client = opa_client
    
    # Initialize event publisher
    event_publisher = EventPublisher(
        service_name="security-service",
        pulsar_url=os.getenv("PULSAR_URL", "pulsar://localhost:6650")
    )
    await event_publisher.initialize()
    app.state.event_publisher = event_publisher
    
    # Initialize service mesh coordinator
    service_mesh_coordinator = ServiceMeshCoordinator(
        consul_client=consul_client,
        vault_client=vault_client,
        service_name="security-service"
    )
    await service_mesh_coordinator.initialize()
    app.state.service_mesh = service_mesh_coordinator
    
    # Initialize mTLS manager
    mtls_manager = mTLSManager(
        vault_client=vault_client,
        consul_client=consul_client,
        ca_path=os.getenv("CA_PATH", "pki"),
        intermediate_path=os.getenv("INTERMEDIATE_PATH", "pki_int")
    )
    await mtls_manager.initialize()
    app.state.mtls_manager = mtls_manager
    
    # Initialize Kong API Gateway manager
    kong_manager = KongAPIGatewayManager(
        kong_admin_url=os.getenv("KONG_ADMIN_URL", "http://kong-admin:8001"),
        vault_client=vault_client,
        consul_client=consul_client
    )
    await kong_manager.initialize()
    app.state.kong_manager = kong_manager
    
    # Initialize Zero-Trust policy engine
    zero_trust_engine = ZeroTrustPolicyEngine(
        opa_client=opa_client,
        vault_client=vault_client,
        consul_client=consul_client,
        event_publisher=event_publisher
    )
    await zero_trust_engine.initialize()
    app.state.zero_trust_engine = zero_trust_engine
    
    # Initialize secrets rotation service
    secrets_rotation_service = SecretsRotationService(
        vault_client=vault_client,
        consul_client=consul_client,
        event_publisher=event_publisher,
        kong_manager=kong_manager,
        service_mesh_coordinator=service_mesh_coordinator
    )
    await secrets_rotation_service.start()
    app.state.secrets_rotation = secrets_rotation_service
    
    # Initialize security monitor
    security_monitor = SecurityMonitor(
        vault_client=vault_client,
        consul_client=consul_client,
        opa_client=opa_client,
        event_publisher=event_publisher,
        zero_trust_engine=zero_trust_engine
    )
    await security_monitor.start()
    app.state.security_monitor = security_monitor
    
    # Initialize compliance checker
    compliance_checker = ComplianceChecker(
        vault_client=vault_client,
        consul_client=consul_client,
        opa_client=opa_client,
        zero_trust_engine=zero_trust_engine
    )
    await compliance_checker.initialize()
    app.state.compliance_checker = compliance_checker
    
    # Load initial security policies
    await load_initial_policies()
    
    # Register with Consul service mesh
    await register_service_mesh()
    
    # Start background tasks
    asyncio.create_task(monitor_certificate_expiry())
    asyncio.create_task(sync_kong_configurations())
    asyncio.create_task(enforce_zero_trust_policies())
    
    logger.info("Security Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Security Service...")
    
    if secrets_rotation_service:
        await secrets_rotation_service.stop()
        
    if security_monitor:
        await security_monitor.stop()
        
    if zero_trust_engine:
        await zero_trust_engine.shutdown()
        
    if kong_manager:
        await kong_manager.shutdown()
        
    if mtls_manager:
        await mtls_manager.shutdown()
        
    if service_mesh_coordinator:
        await service_mesh_coordinator.shutdown()
        
    if event_publisher:
        await event_publisher.close()
        
    if opa_client:
        await opa_client.close()
        
    logger.info("Security Service shutdown complete")


# Create FastAPI app with security middleware
app = create_base_app(
    service_name="security-service",
    version="2.0.0",
    description="Zero-Trust security orchestration service for PlatformQ"
)

# Add security middleware
app.add_middleware(
    VaultConsulMiddleware,
    service_name="security-service",
    enable_mtls=True,
    enable_rate_limiting=True,
    enable_audit_logging=True
)

# Set lifespan
app.router.lifespan_context = lifespan

# Include API router
app.include_router(api_router, prefix="/api/v1", tags=["security"])


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "security-service",
        "version": "2.0.0",
        "status": "operational",
        "features": [
            "secrets-rotation",
            "policy-management",
            "security-monitoring",
            "compliance-checking",
            "audit-logging",
            "mtls-enforcement",
            "service-mesh-integration",
            "kong-api-gateway",
            "zero-trust-architecture",
            "dynamic-credentials",
            "certificate-management"
        ]
    }


# Health check endpoint
@app.get("/health")
async def health_check():
    """Comprehensive health check endpoint"""
    health_status = {
        "service": "healthy",
        "vault": "unknown",
        "consul": "unknown",
        "opa": "unknown",
        "secrets_rotation": "unknown",
        "security_monitor": "unknown",
        "service_mesh": "unknown",
        "kong_gateway": "unknown",
        "zero_trust": "unknown"
    }
    
    # Check Vault
    if vault_client:
        try:
            vault_health = await vault_client.health_check()
            health_status["vault"] = "healthy" if vault_health.get("initialized") else "unhealthy"
        except Exception:
            health_status["vault"] = "unhealthy"
            
    # Check Consul
    if consul_client:
        try:
            consul_health = await consul_client.health_check()
            health_status["consul"] = "healthy" if consul_health.get("healthy") else "unhealthy"
        except Exception:
            health_status["consul"] = "unhealthy"
            
    # Check OPA
    if opa_client:
        try:
            opa_health = await opa_client.health_check()
            health_status["opa"] = "healthy" if opa_health else "unhealthy"
        except Exception:
            health_status["opa"] = "unhealthy"
            
    # Check Service Mesh
    if service_mesh_coordinator:
        try:
            mesh_status = await service_mesh_coordinator.get_mesh_status()
            health_status["service_mesh"] = "healthy" if mesh_status.get("connected") else "unhealthy"
        except Exception:
            health_status["service_mesh"] = "unhealthy"
            
    # Check Kong Gateway
    if kong_manager:
        try:
            kong_status = await kong_manager.health_check()
            health_status["kong_gateway"] = "healthy" if kong_status else "unhealthy"
        except Exception:
            health_status["kong_gateway"] = "unhealthy"
            
    # Check Zero Trust Engine
    if zero_trust_engine:
        try:
            zt_status = await zero_trust_engine.get_status()
            health_status["zero_trust"] = "healthy" if zt_status.get("active") else "unhealthy"
        except Exception:
            health_status["zero_trust"] = "unhealthy"
            
    # Check secrets rotation
    if secrets_rotation_service:
        health_status["secrets_rotation"] = "healthy" if secrets_rotation_service._running else "stopped"
        
    # Check security monitor
    if security_monitor:
        health_status["security_monitor"] = "healthy" if security_monitor.is_running else "stopped"
        
    # Determine overall health
    health_status["overall"] = "healthy" if all(
        status == "healthy" for key, status in health_status.items() 
        if key not in ["service", "overall"]
    ) else "degraded"
    
    return health_status


# Consul health check endpoint
@app.get("/health/consul")
async def consul_health():
    """Consul-specific health check"""
    try:
        if service_mesh_coordinator:
            mesh_status = await service_mesh_coordinator.get_mesh_status()
            return {"status": "healthy", "mesh": mesh_status}
        return {"status": "unhealthy", "error": "Service mesh not initialized"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


# Policy management endpoints
@app.post("/api/v1/policies/zero-trust")
async def create_zero_trust_policy(policy: Dict[str, Any]):
    """Create a new zero-trust policy"""
    if not zero_trust_engine:
        raise HTTPException(status_code=503, detail="Zero-trust engine not available")
    
    try:
        result = await zero_trust_engine.create_policy(policy)
        return {"status": "created", "policy_id": result["id"]}
    except Exception as e:
        logger.error(f"Failed to create policy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/policies/zero-trust/{policy_id}")
async def get_zero_trust_policy(policy_id: str):
    """Get a zero-trust policy by ID"""
    if not zero_trust_engine:
        raise HTTPException(status_code=503, detail="Zero-trust engine not available")
    
    try:
        policy = await zero_trust_engine.get_policy(policy_id)
        if not policy:
            raise HTTPException(status_code=404, detail="Policy not found")
        return policy
    except Exception as e:
        logger.error(f"Failed to get policy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Certificate management endpoints
@app.post("/api/v1/certificates/issue")
async def issue_certificate(
    common_name: str,
    ttl: str = "720h",
    alt_names: Optional[List[str]] = None
):
    """Issue a new TLS certificate"""
    if not mtls_manager:
        raise HTTPException(status_code=503, detail="mTLS manager not available")
    
    try:
        cert_data = await mtls_manager.issue_certificate(
            common_name=common_name,
            ttl=ttl,
            alt_names=alt_names or []
        )
        return {
            "certificate": cert_data["certificate"],
            "private_key": cert_data["private_key"],
            "ca_chain": cert_data["ca_chain"],
            "serial_number": cert_data["serial_number"],
            "expiry": cert_data["expiry"]
        }
    except Exception as e:
        logger.error(f"Failed to issue certificate: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/certificates/rotate/{service_name}")
async def rotate_service_certificate(service_name: str):
    """Rotate TLS certificate for a service"""
    if not mtls_manager:
        raise HTTPException(status_code=503, detail="mTLS manager not available")
    
    try:
        result = await mtls_manager.rotate_service_certificate(service_name)
        return {
            "status": "rotated",
            "service": service_name,
            "new_serial": result["serial_number"],
            "expiry": result["expiry"]
        }
    except Exception as e:
        logger.error(f"Failed to rotate certificate: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Kong API Gateway management
@app.post("/api/v1/kong/services")
async def register_kong_service(service_config: Dict[str, Any]):
    """Register a service with Kong API Gateway"""
    if not kong_manager:
        raise HTTPException(status_code=503, detail="Kong manager not available")
    
    try:
        result = await kong_manager.register_service(service_config)
        return {"status": "registered", "service": result}
    except Exception as e:
        logger.error(f"Failed to register Kong service: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/kong/plugins/{service_name}")
async def configure_kong_plugins(service_name: str, plugins: List[Dict[str, Any]]):
    """Configure Kong plugins for a service"""
    if not kong_manager:
        raise HTTPException(status_code=503, detail="Kong manager not available")
    
    try:
        results = []
        for plugin in plugins:
            result = await kong_manager.configure_plugin(service_name, plugin)
            results.append(result)
        return {"status": "configured", "plugins": results}
    except Exception as e:
        logger.error(f"Failed to configure Kong plugins: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Service mesh operations
@app.get("/api/v1/mesh/services")
async def get_mesh_services():
    """Get all services in the service mesh"""
    if not service_mesh_coordinator:
        raise HTTPException(status_code=503, detail="Service mesh not available")
    
    try:
        services = await service_mesh_coordinator.get_services()
        return {"services": services}
    except Exception as e:
        logger.error(f"Failed to get mesh services: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/mesh/intentions")
async def create_service_intention(
    source: str,
    destination: str,
    action: str = "allow",
    description: Optional[str] = None
):
    """Create service mesh intention (allow/deny traffic)"""
    if not service_mesh_coordinator:
        raise HTTPException(status_code=503, detail="Service mesh not available")
    
    try:
        result = await service_mesh_coordinator.create_intention(
            source=source,
            destination=destination,
            action=action,
            description=description
        )
        return {"status": "created", "intention": result}
    except Exception as e:
        logger.error(f"Failed to create intention: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Security metrics endpoint
@app.get("/api/v1/metrics/security")
async def get_security_metrics():
    """Get comprehensive security metrics"""
    metrics = {
        "timestamp": datetime.utcnow().isoformat(),
        "secrets_rotation": {},
        "certificates": {},
        "policy_violations": {},
        "service_mesh": {},
        "api_gateway": {}
    }
    
    # Get secrets rotation metrics
    if secrets_rotation_service:
        metrics["secrets_rotation"] = await secrets_rotation_service.get_metrics()
    
    # Get certificate metrics
    if mtls_manager:
        metrics["certificates"] = await mtls_manager.get_certificate_metrics()
    
    # Get policy violation metrics
    if zero_trust_engine:
        metrics["policy_violations"] = await zero_trust_engine.get_violation_metrics()
    
    # Get service mesh metrics
    if service_mesh_coordinator:
        metrics["service_mesh"] = await service_mesh_coordinator.get_metrics()
    
    # Get API gateway metrics
    if kong_manager:
        metrics["api_gateway"] = await kong_manager.get_metrics()
    
    return metrics


# Helper functions
async def load_initial_policies():
    """Load initial security policies from Consul"""
    try:
        # Load Zero-Trust policies
        if zero_trust_engine:
            await zero_trust_engine.load_policies_from_consul()
        
        # Load OPA policies
        if opa_client:
            policies_path = "platformq/security/policies"
            _, policies = await consul_client.kv.get(policies_path, recurse=True)
            if policies:
                for policy in policies:
                    await opa_client.put_policy(
                        policy['Key'].split('/')[-1],
                        policy['Value']
                    )
        
        logger.info("Loaded initial security policies")
    except Exception as e:
        logger.error(f"Failed to load initial policies: {e}")


async def register_service_mesh():
    """Register security service with Consul Connect"""
    try:
        if service_mesh_coordinator:
            await service_mesh_coordinator.register_service(
                name="security-service",
                port=8000,
                tags=["security", "critical", "zero-trust"],
                meta={
                    "version": "2.0.0",
                    "protocol": "http",
                    "secure": "true"
                }
            )
        logger.info("Registered with service mesh")
    except Exception as e:
        logger.error(f"Failed to register with service mesh: {e}")


async def monitor_certificate_expiry():
    """Monitor TLS certificate expiry"""
    while True:
        try:
            if mtls_manager:
                expiring_certs = await mtls_manager.get_expiring_certificates(days=30)
                for cert in expiring_certs:
                    logger.warning(f"Certificate expiring soon: {cert}")
                    # Trigger rotation if auto-rotation enabled
                    if cert.get("auto_rotate"):
                        await mtls_manager.rotate_service_certificate(cert["service"])
        except Exception as e:
            logger.error(f"Certificate monitoring error: {e}")
        
        await asyncio.sleep(3600)  # Check every hour


async def sync_kong_configurations():
    """Sync Kong configurations with Consul"""
    while True:
        try:
            if kong_manager and consul_client:
                # Get service configurations from Consul
                _, services = await consul_client.kv.get("kong/services", recurse=True)
                if services:
                    for service_data in services:
                        service_config = json.loads(service_data['Value'])
                        await kong_manager.sync_service_config(service_config)
        except Exception as e:
            logger.error(f"Kong sync error: {e}")
        
        await asyncio.sleep(300)  # Sync every 5 minutes


async def enforce_zero_trust_policies():
    """Continuously enforce zero-trust policies"""
    while True:
        try:
            if zero_trust_engine:
                violations = await zero_trust_engine.check_policy_violations()
                for violation in violations:
                    logger.warning(f"Policy violation detected: {violation}")
                    # Take enforcement action
                    await zero_trust_engine.enforce_policy(violation)
        except Exception as e:
            logger.error(f"Policy enforcement error: {e}")
        
        await asyncio.sleep(60)  # Check every minute 