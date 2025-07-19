"""
Security Service

Orchestrates security operations including secrets rotation, policy management,
and security monitoring for PlatformQ.
"""

import os
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Depends, status, Header
from fastapi.responses import JSONResponse

from platformq_shared import create_base_app
from platformq_shared.vault.vault_client import VaultClient, VaultConfig
from platformq_shared.consul.consul_client import ConsulClient, ConsulConfig
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.authorization.opa_client import OPAClient, OPAConfig

from .secrets_rotation import SecretsRotationService, RotationPolicy, SecretType
from .api.endpoints import router as api_router
from .models import SecurityEvent, PolicyUpdate, SecretRotationStatus
from .monitoring import SecurityMonitor
from .compliance import ComplianceChecker

logger = logging.getLogger(__name__)

# Global instances
secrets_rotation_service = None
security_monitor = None
compliance_checker = None
vault_client = None
consul_client = None
opa_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global secrets_rotation_service, security_monitor, compliance_checker
    global vault_client, consul_client, opa_client
    
    # Startup
    logger.info("Starting Security Service...")
    
    # Initialize clients
    vault_client = VaultClient(VaultConfig(
        host=os.getenv("VAULT_ADDR", "localhost"),
        port=int(os.getenv("VAULT_PORT", 8200)),
        token=os.getenv("VAULT_TOKEN"),
        role_id=os.getenv("VAULT_ROLE_ID"),
        secret_id=os.getenv("VAULT_SECRET_ID")
    ))
    await vault_client.initialize()
    app.state.vault_client = vault_client
    
    consul_client = ConsulClient(ConsulConfig(
        host=os.getenv("CONSUL_ADDR", "localhost"),
        port=int(os.getenv("CONSUL_PORT", 8500)),
        token=os.getenv("CONSUL_TOKEN"),
        datacenter=os.getenv("CONSUL_DATACENTER", "dc1")
    ))
    await consul_client.initialize()
    app.state.consul_client = consul_client
    
    opa_client = OPAClient(OPAConfig(
        host=os.getenv("OPA_ADDR", "localhost"),
        port=int(os.getenv("OPA_PORT", 8181))
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
    
    # Initialize secrets rotation service
    secrets_rotation_service = SecretsRotationService(
        vault_client=vault_client,
        consul_client=consul_client,
        event_publisher=event_publisher
    )
    await secrets_rotation_service.start()
    app.state.secrets_rotation = secrets_rotation_service
    
    # Initialize security monitor
    security_monitor = SecurityMonitor(
        vault_client=vault_client,
        consul_client=consul_client,
        opa_client=opa_client,
        event_publisher=event_publisher
    )
    await security_monitor.start()
    app.state.security_monitor = security_monitor
    
    # Initialize compliance checker
    compliance_checker = ComplianceChecker(
        vault_client=vault_client,
        consul_client=consul_client,
        opa_client=opa_client
    )
    await compliance_checker.initialize()
    app.state.compliance_checker = compliance_checker
    
    # Load initial security policies
    await load_initial_policies()
    
    logger.info("Security Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Security Service...")
    
    if secrets_rotation_service:
        await secrets_rotation_service.stop()
        
    if security_monitor:
        await security_monitor.stop()
        
    if event_publisher:
        await event_publisher.close()
        
    if opa_client:
        await opa_client.close()
        
    logger.info("Security Service shutdown complete")


# Create FastAPI app
app = create_base_app(
    service_name="security-service",
    version="1.0.0",
    description="Security orchestration service for PlatformQ"
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
        "version": "1.0.0",
        "status": "operational",
        "features": [
            "secrets-rotation",
            "policy-management",
            "security-monitoring",
            "compliance-checking",
            "audit-logging"
        ]
    }


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    health_status = {
        "service": "healthy",
        "vault": "unknown",
        "consul": "unknown",
        "opa": "unknown",
        "secrets_rotation": "unknown",
        "security_monitor": "unknown"
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
            health_status["opa"] = "healthy" if opa_health.get("healthy") else "unhealthy"
        except Exception:
            health_status["opa"] = "unhealthy"
            
    # Check secrets rotation
    if secrets_rotation_service:
        try:
            rotation_health = await secrets_rotation_service.health_check()
            health_status["secrets_rotation"] = "healthy" if rotation_health.get("healthy") else "unhealthy"
        except Exception:
            health_status["secrets_rotation"] = "unhealthy"
            
    # Check security monitor
    if security_monitor:
        try:
            monitor_health = await security_monitor.health_check()
            health_status["security_monitor"] = "healthy" if monitor_health.get("healthy") else "unhealthy"
        except Exception:
            health_status["security_monitor"] = "unhealthy"
            
    # Overall health
    all_healthy = all(status == "healthy" for status in health_status.values() if status != "unknown")
    
    return JSONResponse(
        status_code=200 if all_healthy else 503,
        content=health_status
    )


async def load_initial_policies():
    """Load initial security policies into OPA"""
    logger.info("Loading initial security policies...")
    
    # Platform-wide security policies
    policies = {
        "platform_security": """
        package platformq.security
        
        # Enforce encryption for sensitive data
        encryption_required {
            input.data_classification == "sensitive"
            not input.encrypted
        }
        
        # Require MFA for admin actions
        mfa_required {
            input.context.roles[_] == "admin"
            input.action in ["delete", "modify_policy", "access_secrets"]
            not input.context.mfa_verified
        }
        
        # Audit logging requirements
        audit_required {
            input.resource in ["secrets", "policies", "compliance_data"]
        }
        
        # IP allowlist for admin access
        ip_allowed {
            input.context.roles[_] == "admin"
            input.context.ip in data.admin_ip_allowlist
        }
        """,
        
        "secret_access": """
        package platformq.secrets
        
        # Secret access control
        allow {
            input.action == "read"
            input.context.service_name == data.secret_permissions[input.secret_path].allowed_services[_]
        }
        
        allow {
            input.action == "rotate"
            input.context.roles[_] == "security_admin"
        }
        
        # Prevent access to expired secrets
        deny {
            data.secret_metadata[input.secret_path].expired
        }
        """,
        
        "compliance": """
        package platformq.compliance
        
        # Data residency requirements
        data_residency_compliant {
            input.data_location in data.allowed_regions[input.tenant_id]
        }
        
        # GDPR compliance
        gdpr_compliant {
            input.action == "delete_user_data"
            input.context.verified_identity
            input.context.request_timestamp < data.gdpr_deadline
        }
        
        # SOC2 requirements
        soc2_compliant {
            input.encryption_at_rest
            input.encryption_in_transit
            input.access_logged
            input.data_backup_enabled
        }
        """
    }
    
    # Load policies into OPA
    for name, policy in policies.items():
        try:
            await opa_client.update_policy(name, policy)
            logger.info(f"Loaded policy: {name}")
        except Exception as e:
            logger.error(f"Failed to load policy {name}: {e}")
            
    # Load initial data
    initial_data = {
        "admin_ip_allowlist": [
            "10.0.0.0/8",
            "172.16.0.0/12",
            "192.168.0.0/16"
        ],
        "allowed_regions": {
            "default": ["us-east-1", "eu-west-1"],
            "eu_tenants": ["eu-west-1", "eu-central-1"]
        },
        "secret_permissions": {},
        "gdpr_deadline": 2592000  # 30 days in seconds
    }
    
    await opa_client.update_data("platformq/security", initial_data)
    
    logger.info("Initial security policies loaded")


# API Endpoints

@app.post("/api/v1/policies/{policy_name}")
async def update_policy(
    policy_name: str,
    policy_update: PolicyUpdate,
    current_user: Dict = Depends(get_current_user)
):
    """Update a security policy"""
    # Check authorization
    if "security_admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to update policies"
        )
        
    try:
        # Validate policy syntax
        test_results = await opa_client.test_policy(
            policy_update.policy,
            policy_update.test_cases or []
        )
        
        if test_results["failed"] > 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Policy validation failed: {test_results}"
            )
            
        # Update policy
        await opa_client.update_policy(policy_name, policy_update.policy)
        
        # Audit log
        await app.state.event_publisher.publish_event(
            "platformq.security.policy-updated",
            {
                "policy_name": policy_name,
                "updated_by": current_user["user_id"],
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        return {"status": "success", "policy": policy_name}
        
    except Exception as e:
        logger.error(f"Failed to update policy: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update policy: {str(e)}"
        )


@app.get("/api/v1/rotation/status")
async def get_rotation_status(
    current_user: Dict = Depends(get_current_user)
) -> List[SecretRotationStatus]:
    """Get status of all secret rotations"""
    if not secrets_rotation_service:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Secrets rotation service not available"
        )
        
    rotation_history = secrets_rotation_service.rotation_history
    status_list = []
    
    for secret_path, history in rotation_history.items():
        if history:
            latest = history[-1]
            status_list.append(SecretRotationStatus(
                secret_path=secret_path,
                last_rotated=latest.rotated_at,
                next_rotation=latest.rotated_at + timedelta(days=30),  # Default
                rotation_count=latest.rotation_count,
                status="active"
            ))
            
    return status_list


@app.post("/api/v1/rotation/trigger/{secret_path:path}")
async def trigger_rotation(
    secret_path: str,
    current_user: Dict = Depends(get_current_user)
):
    """Manually trigger secret rotation"""
    if "security_admin" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions to trigger rotation"
        )
        
    # Find matching policy
    policy = None
    for name, pol in secrets_rotation_service.rotation_policies.items():
        if pol.secret_type == SecretType.DATABASE_PASSWORD and "database" in secret_path:
            policy = pol
            break
            
    if not policy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No rotation policy found for {secret_path}"
        )
        
    try:
        await secrets_rotation_service._rotate_secret(secret_path, policy)
        return {"status": "success", "message": f"Rotation triggered for {secret_path}"}
    except Exception as e:
        logger.error(f"Failed to trigger rotation: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger rotation: {str(e)}"
        )


@app.get("/api/v1/compliance/status")
async def get_compliance_status(
    current_user: Dict = Depends(get_current_user)
):
    """Get compliance status"""
    if not compliance_checker:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Compliance checker not available"
        )
        
    try:
        status = await compliance_checker.get_compliance_status(
            tenant_id=current_user.get("tenant_id")
        )
        return status
    except Exception as e:
        logger.error(f"Failed to get compliance status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get compliance status: {str(e)}"
        )


@app.get("/api/v1/security/events")
async def get_security_events(
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    event_type: Optional[str] = None,
    limit: int = 100,
    current_user: Dict = Depends(get_current_user)
) -> List[SecurityEvent]:
    """Get security events"""
    if "security_admin" not in current_user.get("roles", []):
        # Filter events by tenant for non-admins
        tenant_filter = current_user.get("tenant_id")
    else:
        tenant_filter = None
        
    try:
        events = await security_monitor.get_events(
            start_time=start_time,
            end_time=end_time,
            event_type=event_type,
            tenant_id=tenant_filter,
            limit=limit
        )
        return events
    except Exception as e:
        logger.error(f"Failed to get security events: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get security events: {str(e)}"
        )


# Utility functions

async def get_current_user(
    authorization: str = Header(None),
    x_user_id: str = Header(None),
    x_tenant_id: str = Header(None),
    x_roles: str = Header(None)
) -> Dict[str, Any]:
    """Get current user from headers"""
    if not all([x_user_id, x_tenant_id]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication headers"
        )
        
    return {
        "user_id": x_user_id,
        "tenant_id": x_tenant_id,
        "roles": x_roles.split(",") if x_roles else []
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 