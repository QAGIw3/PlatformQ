"""Repository for quota management data"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
import json
from uuid import uuid4
import pickle

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RetryPolicy, ExponentialReconnectionPolicy
from pyignite import Client
from pyignite.datatypes import String, TimestampObject

from platformq_resource_common import (
    ResourceQuota,
    ResourceUsage,
    ResourceType,
    QuotaAlert,
    QuotaStatus
)

from .config import settings

logger = logging.getLogger(__name__)


class QuotaRepository:
    """Repository for quota management data"""
    
    def __init__(self):
        self.cassandra_cluster = None
        self.cassandra_session = None
        self.ignite_client = None
        self._init_connections()
        
    def _init_connections(self):
        """Initialize database connections"""
        # Initialize Cassandra
        try:
            auth_provider = None
            if settings.cassandra_username:
                auth_provider = PlainTextAuthProvider(
                    username=settings.cassandra_username,
                    password=settings.cassandra_password
                )
                
            self.cassandra_cluster = Cluster(
                settings.cassandra_hosts.split(','),
                auth_provider=auth_provider,
                default_retry_policy=RetryPolicy(),
                reconnection_policy=ExponentialReconnectionPolicy(1.0, 600.0)
            )
            self.cassandra_session = self.cassandra_cluster.connect()
            
            # Create keyspace and tables
            self._create_cassandra_schema()
            
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            
        # Initialize Ignite
        try:
            self.ignite_client = Client()
            self.ignite_client.connect(settings.ignite_host, settings.ignite_port)
            
            # Create cache if not exists
            self.quota_cache = self.ignite_client.get_or_create_cache(
                settings.ignite_cache_name
            )
            
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            
    def _create_cassandra_schema(self):
        """Create Cassandra schema"""
        # Create keyspace
        self.cassandra_session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {settings.cassandra_keyspace}
            WITH replication = {{
                'class': 'SimpleStrategy',
                'replication_factor': 3
            }}
        """)
        
        self.cassandra_session.set_keyspace(settings.cassandra_keyspace)
        
        # Quotas table
        self.cassandra_session.execute("""
            CREATE TABLE IF NOT EXISTS resource_quotas (
                tenant_id text,
                resource_type text,
                limit double,
                used double,
                period text,
                status text,
                created_at timestamp,
                updated_at timestamp,
                PRIMARY KEY (tenant_id, resource_type)
            )
        """)
        
        # Resource usage table
        self.cassandra_session.execute("""
            CREATE TABLE IF NOT EXISTS resource_usage (
                tenant_id text,
                resource_type text,
                current_usage double,
                updated_at timestamp,
                PRIMARY KEY (tenant_id, resource_type)
            )
        """)
        
        # Usage history table (time series)
        self.cassandra_session.execute("""
            CREATE TABLE IF NOT EXISTS resource_usage_history (
                tenant_id text,
                resource_type text,
                timestamp timestamp,
                usage double,
                delta double,
                operation text,
                resource_id text,
                PRIMARY KEY ((tenant_id, resource_type), timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
        """)
        
        # Quota alerts table
        self.cassandra_session.execute("""
            CREATE TABLE IF NOT EXISTS quota_alerts (
                tenant_id text,
                alert_id text,
                resource_type text,
                threshold_percentage int,
                current_usage double,
                quota_limit double,
                alert_type text,
                message text,
                triggered_at timestamp,
                PRIMARY KEY (tenant_id, alert_id)
            ) WITH CLUSTERING ORDER BY (alert_id DESC)
        """)
        
        # Alert history table (to track sent alerts)
        self.cassandra_session.execute("""
            CREATE TABLE IF NOT EXISTS alert_history (
                tenant_id text,
                resource_type text,
                threshold int,
                sent_at timestamp,
                PRIMARY KEY ((tenant_id, resource_type), threshold, sent_at)
            ) WITH CLUSTERING ORDER BY (threshold DESC, sent_at DESC)
        """)
        
        # Create indexes
        self.cassandra_session.execute("""
            CREATE INDEX IF NOT EXISTS idx_quotas_status
            ON resource_quotas (status)
        """)
        
        self.cassandra_session.execute("""
            CREATE INDEX IF NOT EXISTS idx_alerts_type
            ON quota_alerts (alert_type)
        """)
        
    async def save_quota(self, quota: ResourceQuota) -> None:
        """Save resource quota"""
        query = """
            INSERT INTO resource_quotas (
                tenant_id, resource_type, limit, used, period,
                status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.cassandra_session.execute(query, (
            quota.tenant_id,
            quota.resource_type.value,
            quota.limit,
            quota.used,
            quota.period,
            quota.status.value,
            quota.created_at,
            quota.updated_at
        ))
        
        # Cache in Ignite
        cache_key = f"quota:{quota.tenant_id}:{quota.resource_type.value}"
        self.quota_cache.put(cache_key, pickle.dumps(quota))
        
    async def get_quota(
        self,
        tenant_id: str,
        resource_type: ResourceType
    ) -> Optional[ResourceQuota]:
        """Get resource quota"""
        # Try cache first
        cache_key = f"quota:{tenant_id}:{resource_type.value}"
        cached = self.quota_cache.get(cache_key)
        if cached:
            return pickle.loads(cached)
            
        # Query Cassandra
        query = """
            SELECT * FROM resource_quotas
            WHERE tenant_id = ? AND resource_type = ?
        """
        
        result = self.cassandra_session.execute(
            query,
            (tenant_id, resource_type.value)
        )
        row = result.one()
        
        if row:
            quota = ResourceQuota(
                tenant_id=row.tenant_id,
                resource_type=ResourceType(row.resource_type),
                limit=row.limit,
                used=row.used,
                period=row.period,
                status=QuotaStatus(row.status),
                created_at=row.created_at,
                updated_at=row.updated_at
            )
            
            # Update cache
            self.quota_cache.put(cache_key, pickle.dumps(quota))
            
            return quota
            
        return None
        
    async def get_all_quotas(self, tenant_id: str) -> List[ResourceQuota]:
        """Get all quotas for tenant"""
        query = "SELECT * FROM resource_quotas WHERE tenant_id = ?"
        results = self.cassandra_session.execute(query, (tenant_id,))
        
        quotas = []
        for row in results:
            quota = ResourceQuota(
                tenant_id=row.tenant_id,
                resource_type=ResourceType(row.resource_type),
                limit=row.limit,
                used=row.used,
                period=row.period,
                status=QuotaStatus(row.status),
                created_at=row.created_at,
                updated_at=row.updated_at
            )
            quotas.append(quota)
            
        return quotas
        
    async def update_quota_status(
        self,
        tenant_id: str,
        resource_type: ResourceType,
        status: QuotaStatus
    ) -> None:
        """Update quota status"""
        query = """
            UPDATE resource_quotas
            SET status = ?, updated_at = ?
            WHERE tenant_id = ? AND resource_type = ?
        """
        
        self.cassandra_session.execute(query, (
            status.value,
            datetime.now(timezone.utc),
            tenant_id,
            resource_type.value
        ))
        
        # Invalidate cache
        cache_key = f"quota:{tenant_id}:{resource_type.value}"
        self.quota_cache.remove(cache_key)
        
    async def get_resource_usage(
        self,
        tenant_id: str,
        resource_type: ResourceType
    ) -> float:
        """Get current resource usage"""
        # Try cache first
        cache_key = f"usage:{tenant_id}:{resource_type.value}"
        cached = self.quota_cache.get(cache_key)
        if cached:
            return float(cached)
            
        # Query Cassandra
        query = """
            SELECT current_usage FROM resource_usage
            WHERE tenant_id = ? AND resource_type = ?
        """
        
        result = self.cassandra_session.execute(
            query,
            (tenant_id, resource_type.value)
        )
        row = result.one()
        
        usage = row.current_usage if row else 0.0
        
        # Update cache
        self.quota_cache.put(cache_key, usage)
        
        return usage
        
    async def update_resource_usage(
        self,
        tenant_id: str,
        resource_type: ResourceType,
        delta: float
    ) -> None:
        """Update resource usage"""
        # Get current usage
        current_usage = await self.get_resource_usage(tenant_id, resource_type)
        new_usage = max(0, current_usage + delta)
        
        # Update usage
        query = """
            INSERT INTO resource_usage (
                tenant_id, resource_type, current_usage, updated_at
            ) VALUES (?, ?, ?, ?)
        """
        
        self.cassandra_session.execute(query, (
            tenant_id,
            resource_type.value,
            new_usage,
            datetime.now(timezone.utc)
        ))
        
        # Record in history
        history_query = """
            INSERT INTO resource_usage_history (
                tenant_id, resource_type, timestamp, usage,
                delta, operation, resource_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        self.cassandra_session.execute(history_query, (
            tenant_id,
            resource_type.value,
            datetime.now(timezone.utc),
            new_usage,
            delta,
            "increment" if delta > 0 else "decrement",
            ""  # Resource ID would come from event
        ))
        
        # Update cache
        cache_key = f"usage:{tenant_id}:{resource_type.value}"
        self.quota_cache.put(cache_key, new_usage)
        
        # Update quota used field
        quota_update = """
            UPDATE resource_quotas
            SET used = ?, updated_at = ?
            WHERE tenant_id = ? AND resource_type = ?
        """
        
        self.cassandra_session.execute(quota_update, (
            new_usage,
            datetime.now(timezone.utc),
            tenant_id,
            resource_type.value
        ))
        
    async def save_quota_alert(self, alert: QuotaAlert) -> None:
        """Save quota alert"""
        alert_id = str(uuid4())
        
        query = """
            INSERT INTO quota_alerts (
                tenant_id, alert_id, resource_type, threshold_percentage,
                current_usage, quota_limit, alert_type, message, triggered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        self.cassandra_session.execute(query, (
            alert.tenant_id,
            alert_id,
            alert.resource_type.value,
            alert.threshold_percentage,
            alert.current_usage,
            alert.quota_limit,
            alert.alert_type,
            alert.message,
            alert.triggered_at
        ))
        
        # Record in alert history
        history_query = """
            INSERT INTO alert_history (
                tenant_id, resource_type, threshold, sent_at
            ) VALUES (?, ?, ?, ?)
        """
        
        self.cassandra_session.execute(history_query, (
            alert.tenant_id,
            alert.resource_type.value,
            alert.threshold_percentage,
            alert.triggered_at
        ))
        
    async def check_alert_sent(
        self,
        tenant_id: str,
        resource_type: ResourceType,
        threshold: int
    ) -> bool:
        """Check if alert was already sent for threshold"""
        # Check alerts sent in last 24 hours
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
        
        query = """
            SELECT COUNT(*) as count FROM alert_history
            WHERE tenant_id = ? AND resource_type = ?
            AND threshold = ? AND sent_at > ?
            ALLOW FILTERING
        """
        
        result = self.cassandra_session.execute(
            query,
            (tenant_id, resource_type.value, threshold, cutoff_time)
        )
        
        row = result.one()
        return row.count > 0 if row else False
        
    async def get_quota_alerts(
        self,
        tenant_id: str,
        days: int = 7
    ) -> List[QuotaAlert]:
        """Get recent quota alerts"""
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
        
        query = """
            SELECT * FROM quota_alerts
            WHERE tenant_id = ? AND triggered_at > ?
            ALLOW FILTERING
        """
        
        results = self.cassandra_session.execute(
            query,
            (tenant_id, cutoff_time)
        )
        
        alerts = []
        for row in results:
            alert = QuotaAlert(
                tenant_id=row.tenant_id,
                resource_type=ResourceType(row.resource_type),
                threshold_percentage=row.threshold_percentage,
                current_usage=row.current_usage,
                quota_limit=row.quota_limit,
                alert_type=row.alert_type,
                message=row.message,
                triggered_at=row.triggered_at
            )
            alerts.append(alert)
            
        return alerts
        
    async def get_usage_history(
        self,
        tenant_id: str,
        resource_type: ResourceType,
        hours: int = 24
    ) -> List[Dict[str, Any]]:
        """Get usage history"""
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        query = """
            SELECT * FROM resource_usage_history
            WHERE tenant_id = ? AND resource_type = ?
            AND timestamp > ?
        """
        
        results = self.cassandra_session.execute(
            query,
            (tenant_id, resource_type.value, cutoff_time)
        )
        
        history = []
        for row in results:
            history.append({
                "timestamp": row.timestamp,
                "usage": row.usage,
                "delta": row.delta,
                "operation": row.operation,
                "resource_id": row.resource_id
            })
            
        return history
        
    async def delete_old_usage_history(self, cutoff_date: datetime) -> None:
        """Delete old usage history records"""
        # Get all tenants and resource types
        # In production, this would be more efficient
        
        tenants_query = "SELECT DISTINCT tenant_id FROM resource_usage"
        tenants = self.cassandra_session.execute(tenants_query)
        
        for tenant_row in tenants:
            types_query = """
                SELECT DISTINCT resource_type FROM resource_usage
                WHERE tenant_id = ?
            """
            types = self.cassandra_session.execute(
                types_query,
                (tenant_row.tenant_id,)
            )
            
            for type_row in types:
                # Delete old records
                delete_query = """
                    DELETE FROM resource_usage_history
                    WHERE tenant_id = ? AND resource_type = ?
                    AND timestamp < ?
                """
                
                self.cassandra_session.execute(
                    delete_query,
                    (tenant_row.tenant_id, type_row.resource_type, cutoff_date)
                )
                
    async def close(self):
        """Close connections"""
        if self.cassandra_cluster:
            self.cassandra_cluster.shutdown()
        if self.ignite_client:
            self.ignite_client.close() 