"""
Data platform administration API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from enum import Enum
import uuid

router = APIRouter()


class ServiceStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    MAINTENANCE = "maintenance"


class ResourceType(str, Enum):
    COMPUTE = "compute"
    STORAGE = "storage"
    NETWORK = "network"
    MEMORY = "memory"


class ConfigSection(str, Enum):
    QUERY_ENGINE = "query_engine"
    CATALOG = "catalog"
    GOVERNANCE = "governance"
    QUALITY = "quality"
    LAKE = "lake"
    PIPELINES = "pipelines"
    SECURITY = "security"
    PERFORMANCE = "performance"


class SystemConfig(BaseModel):
    """System configuration"""
    section: ConfigSection
    settings: Dict[str, Any]
    description: Optional[str] = None


class ResourceQuota(BaseModel):
    """Resource quota definition"""
    resource_type: ResourceType
    limit: float
    unit: str
    soft_limit: Optional[float] = None
    alert_threshold: float = Field(0.8, ge=0, le=1)


@router.get("/health")
async def get_system_health(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get overall system health"""
    return {
        "status": ServiceStatus.HEALTHY,
        "components": {
            "query_engine": {
                "status": "healthy",
                "latency_ms": 5.2,
                "active_queries": 15
            },
            "catalog": {
                "status": "healthy",
                "asset_count": 1250,
                "last_sync": datetime.utcnow() - timedelta(minutes=5)
            },
            "data_lake": {
                "status": "healthy",
                "storage_used_tb": 1.2,
                "storage_available_tb": 8.8
            },
            "pipeline_engine": {
                "status": "healthy",
                "active_pipelines": 25,
                "running_jobs": 8
            }
        },
        "uptime_hours": 720,
        "last_check": datetime.utcnow()
    }


@router.get("/metrics/system")
async def get_system_metrics(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    time_range: str = Query("1h", description="Time range")
):
    """Get system-wide metrics"""
    return {
        "resources": {
            "cpu": {
                "usage_percent": 45.5,
                "cores_used": 18.2,
                "cores_total": 40
            },
            "memory": {
                "usage_percent": 62.3,
                "used_gb": 99.7,
                "total_gb": 160
            },
            "storage": {
                "usage_percent": 35.0,
                "used_tb": 3.5,
                "total_tb": 10.0
            },
            "network": {
                "ingress_mbps": 125.5,
                "egress_mbps": 89.3
            }
        },
        "throughput": {
            "queries_per_second": 250,
            "data_processed_gb_per_hour": 500,
            "pipeline_executions_per_hour": 120
        },
        "time_range": time_range
    }


@router.get("/config")
async def get_configuration(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    section: Optional[ConfigSection] = Query(None)
):
    """Get system configuration"""
    configs = {
        "query_engine": {
            "max_concurrent_queries": 100,
            "query_timeout_seconds": 300,
            "result_cache_enabled": True,
            "result_cache_ttl_seconds": 3600
        },
        "catalog": {
            "auto_discovery_enabled": True,
            "sync_interval_minutes": 30,
            "metadata_cache_size_mb": 512
        },
        "governance": {
            "audit_logging_enabled": True,
            "compliance_checks_enabled": True,
            "data_classification_auto": True
        },
        "performance": {
            "query_optimization_enabled": True,
            "adaptive_execution_enabled": True,
            "cost_based_optimizer_enabled": True
        }
    }
    
    if section:
        return {section.value: configs.get(section.value, {})}
    return configs


@router.patch("/config")
async def update_configuration(
    config: SystemConfig,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Update system configuration"""
    return {
        "section": config.section,
        "status": "updated",
        "applied_at": datetime.utcnow(),
        "applied_by": user_id,
        "restart_required": config.section in [ConfigSection.QUERY_ENGINE, ConfigSection.SECURITY]
    }


@router.get("/users")
async def list_platform_users(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    role: Optional[str] = Query(None),
    active: Optional[bool] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List platform users"""
    return {
        "users": [
            {
                "user_id": "user_123",
                "username": "data_analyst",
                "email": "analyst@company.com",
                "roles": ["data_analyst", "viewer"],
                "active": True,
                "last_login": datetime.utcnow() - timedelta(hours=2),
                "created_at": datetime.utcnow() - timedelta(days=90)
            },
            {
                "user_id": "user_456",
                "username": "data_engineer",
                "email": "engineer@company.com",
                "roles": ["data_engineer", "admin"],
                "active": True,
                "last_login": datetime.utcnow() - timedelta(minutes=30),
                "created_at": datetime.utcnow() - timedelta(days=180)
            }
        ],
        "total": 2
    }


@router.post("/users/{user_id}/roles")
async def update_user_roles(
    user_id: str,
    roles: List[str] = Query(..., description="New roles"),
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    admin_user_id: str = Query(..., description="Admin user ID")
):
    """Update user roles"""
    return {
        "user_id": user_id,
        "roles": roles,
        "updated_by": admin_user_id,
        "updated_at": datetime.utcnow()
    }


@router.get("/quotas")
async def get_resource_quotas(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get resource quotas"""
    return {
        "tenant_quotas": {
            "storage_tb": {"limit": 10, "used": 3.5, "percentage": 35},
            "compute_hours": {"limit": 10000, "used": 2500, "percentage": 25},
            "concurrent_queries": {"limit": 50, "used": 15, "percentage": 30},
            "pipelines": {"limit": 100, "used": 25, "percentage": 25}
        },
        "user_quotas": {
            "query_rate_per_minute": 60,
            "storage_gb_per_user": 100,
            "concurrent_jobs": 5
        }
    }


@router.patch("/quotas")
async def update_resource_quotas(
    quotas: List[ResourceQuota],
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    admin_user_id: str = Query(..., description="Admin user ID")
):
    """Update resource quotas"""
    return {
        "quotas_updated": len(quotas),
        "updated_by": admin_user_id,
        "updated_at": datetime.utcnow(),
        "effective_from": datetime.utcnow() + timedelta(minutes=5)
    }


@router.post("/maintenance/start")
async def start_maintenance_mode(
    duration_minutes: int = Query(30, ge=1, le=480),
    message: str = Query(..., description="Maintenance message"),
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    admin_user_id: str = Query(..., description="Admin user ID")
):
    """Start maintenance mode"""
    return {
        "maintenance_id": str(uuid.uuid4()),
        "status": "active",
        "started_at": datetime.utcnow(),
        "expected_end": datetime.utcnow() + timedelta(minutes=duration_minutes),
        "message": message,
        "initiated_by": admin_user_id
    }


@router.post("/maintenance/stop")
async def stop_maintenance_mode(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    admin_user_id: str = Query(..., description="Admin user ID")
):
    """Stop maintenance mode"""
    return {
        "status": "stopped",
        "stopped_at": datetime.utcnow(),
        "stopped_by": admin_user_id
    }


@router.get("/audit-logs")
async def get_admin_audit_logs(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    action_type: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """Get administrative audit logs"""
    return {
        "logs": [
            {
                "log_id": "log_789",
                "timestamp": datetime.utcnow() - timedelta(hours=1),
                "user_id": "admin_123",
                "action": "config_update",
                "section": "query_engine",
                "details": {
                    "setting": "max_concurrent_queries",
                    "old_value": 50,
                    "new_value": 100
                },
                "ip_address": "192.168.1.100"
            }
        ],
        "total": 1
    }


@router.post("/backup/create")
async def create_backup(
    backup_type: str = Query("full", description="full or incremental"),
    components: List[str] = Query(..., description="Components to backup"),
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    admin_user_id: str = Query(..., description="Admin user ID")
):
    """Create system backup"""
    backup_id = str(uuid.uuid4())
    
    return {
        "backup_id": backup_id,
        "type": backup_type,
        "components": components,
        "status": "started",
        "estimated_size_gb": 250,
        "estimated_time_minutes": 30,
        "started_at": datetime.utcnow(),
        "initiated_by": admin_user_id
    }


@router.get("/alerts/config")
async def get_alert_configuration(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get alert configuration"""
    return {
        "alert_rules": [
            {
                "rule_id": "high_cpu",
                "name": "High CPU Usage",
                "condition": "cpu_usage > 80",
                "duration_minutes": 5,
                "severity": "warning",
                "enabled": True,
                "channels": ["email", "slack"]
            },
            {
                "rule_id": "storage_full",
                "name": "Storage Nearly Full",
                "condition": "storage_usage > 90",
                "duration_minutes": 1,
                "severity": "critical",
                "enabled": True,
                "channels": ["email", "pagerduty"]
            }
        ],
        "notification_channels": {
            "email": {"configured": True, "recipients": ["ops@company.com"]},
            "slack": {"configured": True, "webhook": "https://hooks.slack.com/..."},
            "pagerduty": {"configured": True, "service_key": "***"}
        }
    }


@router.get("/performance/recommendations")
async def get_performance_recommendations(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get performance optimization recommendations"""
    return {
        "recommendations": [
            {
                "id": "rec_1",
                "category": "query_optimization",
                "title": "Enable result caching",
                "description": "Many repeated queries detected",
                "impact": "high",
                "effort": "low",
                "estimated_improvement": "30% query latency reduction"
            },
            {
                "id": "rec_2",
                "category": "storage_optimization",
                "title": "Compact small files in data lake",
                "description": "Too many small files affecting performance",
                "impact": "medium",
                "effort": "medium",
                "estimated_improvement": "20% faster table scans"
            }
        ],
        "generated_at": datetime.utcnow()
    } 