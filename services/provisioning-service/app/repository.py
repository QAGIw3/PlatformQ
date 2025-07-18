"""
Repository for Provisioning Service

Uses the enhanced repository patterns from platformq-shared.
"""

import uuid
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from cassandra.cluster import Session as CassandraSession

from platformq_shared import CassandraRepository, PostgreSQLRepository, QueryBuilder
from platformq_shared.event_publisher import EventPublisher
from platformq_events import (
    TenantProvisionedEvent,
    ResourceScaledEvent,
    QuotaUpdatedEvent,
    ProvisioningFailedEvent
)

from .schemas import (
    TenantProvisioningRequest,
    ResourceQuota,
    ScalingPolicy,
    ProvisioningStatus
)

logger = logging.getLogger(__name__)


class TenantProvisioningRepository(CassandraRepository):
    """Repository for tenant provisioning records"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(
            db_session_factory,
            table_name="tenant_provisioning",
            partition_keys=["tenant_id"],
            clustering_keys=["provisioned_at"]
        )
        self.event_publisher = event_publisher
        
    def create_provisioning_record(self, tenant_id: uuid.UUID, tenant_name: str,
                                 tier: str, requested_by: str) -> Dict[str, Any]:
        """Create a new provisioning record"""
        with self.get_session() as session:
            provisioning_id = uuid.uuid4()
            now = datetime.now(timezone.utc)
            
            query = """
                INSERT INTO tenant_provisioning (
                    tenant_id, provisioning_id, tenant_name,
                    tier, status, requested_by,
                    provisioned_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            session.execute(session.prepare(query), [
                tenant_id,
                provisioning_id,
                tenant_name,
                tier,
                'pending',
                requested_by,
                now,
                now
            ])
            
            record = {
                "provisioning_id": provisioning_id,
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "tier": tier,
                "status": "pending",
                "provisioned_at": now
            }
            
            logger.info(f"Created provisioning record for tenant {tenant_id}")
            return record
            
    def update_provisioning_status(self, tenant_id: uuid.UUID, status: str,
                                 details: Optional[Dict[str, Any]] = None) -> bool:
        """Update provisioning status"""
        with self.get_session() as session:
            now = datetime.now(timezone.utc)
            
            # Get latest provisioning record
            query = """
                SELECT * FROM tenant_provisioning 
                WHERE tenant_id = ?
                ORDER BY provisioned_at DESC
                LIMIT 1
            """
            result = session.execute(session.prepare(query), [tenant_id]).one()
            
            if not result:
                logger.error(f"No provisioning record found for tenant {tenant_id}")
                return False
                
            update_query = """
                UPDATE tenant_provisioning 
                SET status = ?, updated_at = ?, details = ?
                WHERE tenant_id = ? AND provisioned_at = ?
            """
            
            session.execute(session.prepare(update_query), [
                status,
                now,
                details or {},
                tenant_id,
                result.provisioned_at
            ])
            
            # Publish event based on status
            if self.event_publisher:
                if status == "completed":
                    event = TenantProvisionedEvent(
                        tenant_id=str(tenant_id),
                        tenant_name=result.tenant_name,
                        tier=result.tier,
                        provisioned_at=now.isoformat(),
                        resources=details.get("resources", {}) if details else {}
                    )
                    topic = f"persistent://platformq/system/tenant-provisioned-events"
                elif status == "failed":
                    event = ProvisioningFailedEvent(
                        tenant_id=str(tenant_id),
                        tenant_name=result.tenant_name,
                        error=details.get("error", "Unknown error") if details else "Unknown error",
                        failed_at=now.isoformat()
                    )
                    topic = f"persistent://platformq/system/provisioning-failed-events"
                else:
                    return True
                    
                self.event_publisher.publish_event(topic=topic, event=event)
                
            return True
            
    def get_tenant_provisioning_history(self, tenant_id: uuid.UUID,
                                      limit: int = 10) -> List[Dict[str, Any]]:
        """Get provisioning history for a tenant"""
        with self.get_session() as session:
            query = """
                SELECT * FROM tenant_provisioning 
                WHERE tenant_id = ?
                ORDER BY provisioned_at DESC
                LIMIT ?
            """
            results = session.execute(session.prepare(query), [tenant_id, limit])
            return [dict(row) for row in results]


class ResourceQuotaRepository(PostgreSQLRepository):
    """Repository for resource quotas"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(db_session_factory)
        self.event_publisher = event_publisher
        
    def create_quota(self, tenant_id: str, tier: str,
                    cpu_cores: float, memory_gb: float,
                    storage_gb: float, max_pods: int) -> Dict[str, Any]:
        """Create resource quota for tenant"""
        with self.get_session() as db:
            quota_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc)
            
            query = """
                INSERT INTO resource_quotas (
                    quota_id, tenant_id, tier,
                    cpu_cores, memory_gb, storage_gb, max_pods,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
            """
            
            result = db.execute(query, [
                quota_id, tenant_id, tier,
                cpu_cores, memory_gb, storage_gb, max_pods,
                now, now
            ]).fetchone()
            
            quota = dict(result)
            
            # Publish event
            if self.event_publisher:
                event = QuotaUpdatedEvent(
                    tenant_id=tenant_id,
                    quota_id=quota_id,
                    tier=tier,
                    cpu_cores=cpu_cores,
                    memory_gb=memory_gb,
                    storage_gb=storage_gb,
                    max_pods=max_pods,
                    updated_at=now.isoformat()
                )
                
                self.event_publisher.publish_event(
                    topic=f"persistent://platformq/{tenant_id}/quota-updated-events",
                    event=event
                )
                
            return quota
            
    def update_quota(self, tenant_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update resource quota"""
        with self.get_session() as db:
            # Build update query
            set_clauses = []
            values = []
            
            for key, value in updates.items():
                if key in ['cpu_cores', 'memory_gb', 'storage_gb', 'max_pods']:
                    set_clauses.append(f"{key} = %s")
                    values.append(value)
                    
            if not set_clauses:
                return None
                
            values.extend([datetime.now(timezone.utc), tenant_id])
            
            query = f"""
                UPDATE resource_quotas 
                SET {', '.join(set_clauses)}, updated_at = %s
                WHERE tenant_id = %s
                RETURNING *
            """
            
            result = db.execute(query, values).fetchone()
            
            if result:
                quota = dict(result)
                
                # Publish event
                if self.event_publisher:
                    event = QuotaUpdatedEvent(
                        tenant_id=tenant_id,
                        quota_id=quota['quota_id'],
                        tier=quota['tier'],
                        cpu_cores=quota['cpu_cores'],
                        memory_gb=quota['memory_gb'],
                        storage_gb=quota['storage_gb'],
                        max_pods=quota['max_pods'],
                        updated_at=quota['updated_at'].isoformat()
                    )
                    
                    self.event_publisher.publish_event(
                        topic=f"persistent://platformq/{tenant_id}/quota-updated-events",
                        event=event
                    )
                    
                return quota
                
            return None
            
    def get_quota(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        """Get current quota for tenant"""
        with self.get_session() as db:
            query = "SELECT * FROM resource_quotas WHERE tenant_id = %s"
            result = db.execute(query, [tenant_id]).fetchone()
            return dict(result) if result else None
            
    def get_quota_usage(self, tenant_id: str) -> Dict[str, Any]:
        """Get current resource usage vs quota"""
        with self.get_session() as db:
            # Get quota
            quota = self.get_quota(tenant_id)
            if not quota:
                return {}
                
            # Get current usage from metrics
            usage_query = """
                SELECT 
                    SUM(cpu_usage) as cpu_used,
                    SUM(memory_usage) as memory_used,
                    SUM(storage_usage) as storage_used,
                    COUNT(DISTINCT pod_name) as pod_count
                FROM resource_metrics
                WHERE tenant_id = %s
                AND timestamp > NOW() - INTERVAL '5 minutes'
            """
            
            usage = db.execute(usage_query, [tenant_id]).fetchone()
            
            return {
                "quota": quota,
                "usage": {
                    "cpu_cores": usage['cpu_used'] or 0,
                    "memory_gb": usage['memory_used'] or 0,
                    "storage_gb": usage['storage_used'] or 0,
                    "pods": usage['pod_count'] or 0
                },
                "utilization": {
                    "cpu_percent": (usage['cpu_used'] or 0) / quota['cpu_cores'] * 100 if quota['cpu_cores'] > 0 else 0,
                    "memory_percent": (usage['memory_used'] or 0) / quota['memory_gb'] * 100 if quota['memory_gb'] > 0 else 0,
                    "storage_percent": (usage['storage_used'] or 0) / quota['storage_gb'] * 100 if quota['storage_gb'] > 0 else 0,
                    "pods_percent": (usage['pod_count'] or 0) / quota['max_pods'] * 100 if quota['max_pods'] > 0 else 0
                }
            }


class ScalingPolicyRepository(PostgreSQLRepository):
    """Repository for scaling policies"""
    
    def __init__(self, db_session_factory):
        super().__init__(db_session_factory)
        
    def create_policy(self, service_name: str, min_replicas: int,
                     max_replicas: int, target_cpu: float,
                     target_memory: float, scale_up_cooldown: int,
                     scale_down_cooldown: int) -> Dict[str, Any]:
        """Create scaling policy"""
        with self.get_session() as db:
            policy_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc)
            
            query = """
                INSERT INTO scaling_policies (
                    policy_id, service_name, min_replicas, max_replicas,
                    target_cpu_percent, target_memory_percent,
                    scale_up_cooldown_seconds, scale_down_cooldown_seconds,
                    enabled, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
            """
            
            result = db.execute(query, [
                policy_id, service_name, min_replicas, max_replicas,
                target_cpu, target_memory, scale_up_cooldown,
                scale_down_cooldown, True, now, now
            ]).fetchone()
            
            return dict(result)
            
    def update_policy(self, service_name: str,
                     updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update scaling policy"""
        with self.get_session() as db:
            # Build update query
            set_clauses = []
            values = []
            
            allowed_fields = [
                'min_replicas', 'max_replicas', 'target_cpu_percent',
                'target_memory_percent', 'scale_up_cooldown_seconds',
                'scale_down_cooldown_seconds', 'enabled'
            ]
            
            for key, value in updates.items():
                if key in allowed_fields:
                    set_clauses.append(f"{key} = %s")
                    values.append(value)
                    
            if not set_clauses:
                return None
                
            values.extend([datetime.now(timezone.utc), service_name])
            
            query = f"""
                UPDATE scaling_policies 
                SET {', '.join(set_clauses)}, updated_at = %s
                WHERE service_name = %s
                RETURNING *
            """
            
            result = db.execute(query, values).fetchone()
            return dict(result) if result else None
            
    def get_policy(self, service_name: str) -> Optional[Dict[str, Any]]:
        """Get scaling policy for service"""
        with self.get_session() as db:
            query = "SELECT * FROM scaling_policies WHERE service_name = %s"
            result = db.execute(query, [service_name]).fetchone()
            return dict(result) if result else None
            
    def get_all_policies(self, enabled_only: bool = True) -> List[Dict[str, Any]]:
        """Get all scaling policies"""
        with self.get_session() as db:
            query = "SELECT * FROM scaling_policies"
            if enabled_only:
                query += " WHERE enabled = true"
                
            results = db.execute(query).fetchall()
            return [dict(row) for row in results]


class ScalingEventRepository(PostgreSQLRepository):
    """Repository for scaling events"""
    
    def __init__(self, db_session_factory, event_publisher: Optional[EventPublisher] = None):
        super().__init__(db_session_factory)
        self.event_publisher = event_publisher
        
    def record_scaling_event(self, service_name: str, tenant_id: Optional[str],
                           previous_replicas: int, new_replicas: int,
                           reason: str, metrics: Dict[str, float]) -> Dict[str, Any]:
        """Record a scaling event"""
        with self.get_session() as db:
            event_id = str(uuid.uuid4())
            now = datetime.now(timezone.utc)
            
            query = """
                INSERT INTO scaling_events (
                    event_id, service_name, tenant_id,
                    previous_replicas, new_replicas, scaling_reason,
                    metrics, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
            """
            
            result = db.execute(query, [
                event_id, service_name, tenant_id,
                previous_replicas, new_replicas, reason,
                metrics, now
            ]).fetchone()
            
            event = dict(result)
            
            # Publish event
            if self.event_publisher:
                scaling_event = ResourceScaledEvent(
                    service_name=service_name,
                    tenant_id=tenant_id or "system",
                    previous_replicas=previous_replicas,
                    new_replicas=new_replicas,
                    reason=reason,
                    metrics=metrics,
                    scaled_at=now.isoformat()
                )
                
                topic = f"persistent://platformq/{tenant_id or 'system'}/resource-scaled-events"
                self.event_publisher.publish_event(topic=topic, event=scaling_event)
                
            return event
            
    def get_scaling_history(self, service_name: Optional[str] = None,
                          tenant_id: Optional[str] = None,
                          limit: int = 100) -> List[Dict[str, Any]]:
        """Get scaling event history"""
        with self.get_session() as db:
            query = "SELECT * FROM scaling_events WHERE 1=1"
            params = []
            
            if service_name:
                query += " AND service_name = %s"
                params.append(service_name)
                
            if tenant_id:
                query += " AND tenant_id = %s"
                params.append(tenant_id)
                
            query += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)
            
            results = db.execute(query, params).fetchall()
            return [dict(row) for row in results]


class ResourceMetricsRepository(PostgreSQLRepository):
    """Repository for resource metrics"""
    
    def __init__(self, db_session_factory):
        super().__init__(db_session_factory)
        
    def store_metrics(self, service_name: str, tenant_id: Optional[str],
                     cpu_usage: float, memory_usage: float,
                     request_rate: float, error_rate: float,
                     response_time_p99: float, pod_count: int) -> None:
        """Store resource metrics"""
        with self.get_session() as db:
            now = datetime.now(timezone.utc)
            
            query = """
                INSERT INTO resource_metrics (
                    service_name, tenant_id, timestamp,
                    cpu_usage, memory_usage, request_rate,
                    error_rate, response_time_p99, pod_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            db.execute(query, [
                service_name, tenant_id, now,
                cpu_usage, memory_usage, request_rate,
                error_rate, response_time_p99, pod_count
            ])
            
    def get_latest_metrics(self, service_name: str,
                          tenant_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get latest metrics for service"""
        with self.get_session() as db:
            query = """
                SELECT * FROM resource_metrics 
                WHERE service_name = %s
            """
            params = [service_name]
            
            if tenant_id:
                query += " AND tenant_id = %s"
                params.append(tenant_id)
                
            query += " ORDER BY timestamp DESC LIMIT 1"
            
            result = db.execute(query, params).fetchone()
            return dict(result) if result else None
            
    def get_metrics_history(self, service_name: str,
                           tenant_id: Optional[str] = None,
                           hours: int = 24) -> List[Dict[str, Any]]:
        """Get metrics history"""
        with self.get_session() as db:
            query = """
                SELECT * FROM resource_metrics 
                WHERE service_name = %s
                AND timestamp > NOW() - INTERVAL '%s hours'
            """
            params = [service_name, hours]
            
            if tenant_id:
                query += " AND tenant_id = %s"
                params.append(tenant_id)
                
            query += " ORDER BY timestamp ASC"
            
            results = db.execute(query, params).fetchall()
            return [dict(row) for row in results] 