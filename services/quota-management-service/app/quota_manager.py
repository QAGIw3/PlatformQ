"""Quota Manager for resource quota enforcement and tracking"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
import asyncio
from enum import Enum

import pulsar
from pulsar.schema import JsonSchema

from platformq_resource_common import (
    ResourceQuota,
    ResourceUsage,
    ResourceType,
    QuotaAlert,
    QuotaStatus
)

from .config import settings
from .repository import QuotaRepository

logger = logging.getLogger(__name__)


class QuotaAction(Enum):
    """Actions for quota enforcement"""
    ALLOW = "allow"
    WARN = "warn"
    BLOCK = "block"


class QuotaEventSchema(JsonSchema):
    """Schema for quota events"""
    event_type: str
    tenant_id: str
    resource_type: str
    current_usage: float
    quota_limit: float
    percentage_used: float
    action: str
    message: Optional[str] = None
    timestamp: str


class ResourceEventSchema(JsonSchema):
    """Schema for resource lifecycle events"""
    event_type: str  # created, updated, deleted
    tenant_id: str
    resource_id: str
    resource_type: str
    resource_size: Dict[str, float]  # e.g., {"cpu": 4, "memory": 8}
    timestamp: str


class QuotaManager:
    """Manages quota enforcement and tracking"""
    
    def __init__(self, repository: QuotaRepository):
        self.repository = repository
        self.pulsar_client = None
        self.quota_event_producer = None
        self.resource_event_consumer = None
        self._usage_cache = {}  # In-memory cache for usage
        self._cache_timestamps = {}
        self._init_pulsar()
        
    def _init_pulsar(self):
        """Initialize Pulsar client and producers/consumers"""
        try:
            self.pulsar_client = pulsar.Client(settings.pulsar_url)
            
            # Producer for quota events
            self.quota_event_producer = self.pulsar_client.create_producer(
                topic=f"{settings.pulsar_topic_prefix}{settings.pulsar_quota_events_topic}",
                schema=QuotaEventSchema(),
                producer_name=f"{settings.service_name}-quota-events"
            )
            
            # Consumer for resource events
            self.resource_event_consumer = self.pulsar_client.subscribe(
                topic=f"{settings.pulsar_topic_prefix}{settings.pulsar_resource_events_topic}",
                subscription_name=settings.pulsar_subscription,
                schema=ResourceEventSchema()
            )
            
        except Exception as e:
            logger.error(f"Failed to initialize Pulsar: {e}")
            
    async def check_quota(
        self,
        tenant_id: str,
        resource_type: ResourceType,
        requested_amount: float
    ) -> Tuple[QuotaAction, Optional[str]]:
        """Check if quota allows the requested resource allocation"""
        
        # Get current quota
        quota = await self.repository.get_quota(tenant_id, resource_type)
        if not quota:
            # Use default quota
            quota = await self._create_default_quota(tenant_id, resource_type)
            
        # Get current usage
        usage = await self.get_current_usage(tenant_id, resource_type)
        
        # Calculate new usage
        new_usage = usage + requested_amount
        usage_percentage = (new_usage / quota.limit * 100) if quota.limit > 0 else 0
        
        # Determine action
        action = QuotaAction.ALLOW
        message = None
        
        if usage_percentage >= settings.quota_hard_limit_threshold * 100:
            action = QuotaAction.BLOCK
            message = f"Quota exceeded: {resource_type} usage would be {usage_percentage:.1f}% of limit"
        elif usage_percentage >= settings.quota_soft_limit_threshold * 100:
            action = QuotaAction.WARN
            message = f"Quota warning: {resource_type} usage would be {usage_percentage:.1f}% of limit"
            
        # Publish quota event
        await self._publish_quota_event(
            tenant_id=tenant_id,
            resource_type=resource_type,
            current_usage=new_usage,
            quota_limit=quota.limit,
            percentage_used=usage_percentage,
            action=action,
            message=message
        )
        
        # Check for alerts
        if settings.quota_alert_enabled:
            await self._check_quota_alerts(
                tenant_id=tenant_id,
                resource_type=resource_type,
                usage_percentage=usage_percentage,
                quota=quota
            )
            
        return action, message
        
    async def get_current_usage(
        self,
        tenant_id: str,
        resource_type: ResourceType
    ) -> float:
        """Get current resource usage for tenant"""
        
        # Check cache first
        cache_key = f"{tenant_id}:{resource_type}"
        if cache_key in self._usage_cache:
            timestamp = self._cache_timestamps.get(cache_key)
            if timestamp and (datetime.now(timezone.utc) - timestamp).seconds < settings.usage_cache_ttl_seconds:
                return self._usage_cache[cache_key]
                
        # Get from repository
        usage = await self.repository.get_resource_usage(tenant_id, resource_type)
        
        # Update cache
        self._usage_cache[cache_key] = usage
        self._cache_timestamps[cache_key] = datetime.now(timezone.utc)
        
        return usage
        
    async def update_usage(
        self,
        tenant_id: str,
        resource_type: ResourceType,
        delta: float
    ) -> None:
        """Update resource usage"""
        
        # Update in repository
        await self.repository.update_resource_usage(tenant_id, resource_type, delta)
        
        # Invalidate cache
        cache_key = f"{tenant_id}:{resource_type}"
        if cache_key in self._usage_cache:
            del self._usage_cache[cache_key]
            del self._cache_timestamps[cache_key]
            
        # Get updated usage for quota check
        new_usage = await self.get_current_usage(tenant_id, resource_type)
        
        # Check quota
        quota = await self.repository.get_quota(tenant_id, resource_type)
        if quota:
            usage_percentage = (new_usage / quota.limit * 100) if quota.limit > 0 else 0
            
            # Update quota status
            if usage_percentage >= 100:
                quota.status = QuotaStatus.EXCEEDED
            elif usage_percentage >= settings.quota_soft_limit_threshold * 100:
                quota.status = QuotaStatus.WARNING
            else:
                quota.status = QuotaStatus.OK
                
            await self.repository.update_quota_status(tenant_id, resource_type, quota.status)
            
    async def set_quota(
        self,
        tenant_id: str,
        resource_type: ResourceType,
        limit: float,
        period: Optional[str] = None
    ) -> ResourceQuota:
        """Set resource quota for tenant"""
        
        # Get current usage
        current_usage = await self.get_current_usage(tenant_id, resource_type)
        
        # Determine status
        usage_percentage = (current_usage / limit * 100) if limit > 0 else 0
        if usage_percentage >= 100:
            status = QuotaStatus.EXCEEDED
        elif usage_percentage >= settings.quota_soft_limit_threshold * 100:
            status = QuotaStatus.WARNING
        else:
            status = QuotaStatus.OK
            
        # Create quota
        quota = ResourceQuota(
            tenant_id=tenant_id,
            resource_type=resource_type,
            limit=limit,
            used=current_usage,
            period=period or "monthly",
            status=status,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
        
        # Save quota
        await self.repository.save_quota(quota)
        
        # Publish event
        await self._publish_quota_event(
            tenant_id=tenant_id,
            resource_type=resource_type,
            current_usage=current_usage,
            quota_limit=limit,
            percentage_used=usage_percentage,
            action=QuotaAction.ALLOW,
            message=f"Quota set to {limit} for {resource_type}"
        )
        
        return quota
        
    async def get_quota_status(
        self,
        tenant_id: str
    ) -> Dict[str, Any]:
        """Get comprehensive quota status for tenant"""
        
        # Get all quotas
        quotas = await self.repository.get_all_quotas(tenant_id)
        
        status = {
            "tenant_id": tenant_id,
            "quotas": [],
            "summary": {
                "total_quotas": len(quotas),
                "exceeded": 0,
                "warning": 0,
                "ok": 0
            }
        }
        
        for quota in quotas:
            # Get current usage
            usage = await self.get_current_usage(tenant_id, quota.resource_type)
            usage_percentage = (usage / quota.limit * 100) if quota.limit > 0 else 0
            
            quota_info = {
                "resource_type": quota.resource_type,
                "limit": quota.limit,
                "used": usage,
                "available": max(0, quota.limit - usage),
                "percentage_used": usage_percentage,
                "status": quota.status.value,
                "period": quota.period
            }
            
            status["quotas"].append(quota_info)
            
            # Update summary
            if quota.status == QuotaStatus.EXCEEDED:
                status["summary"]["exceeded"] += 1
            elif quota.status == QuotaStatus.WARNING:
                status["summary"]["warning"] += 1
            else:
                status["summary"]["ok"] += 1
                
        return status
        
    async def _create_default_quota(
        self,
        tenant_id: str,
        resource_type: ResourceType
    ) -> ResourceQuota:
        """Create default quota for tenant"""
        
        # Map resource type to default limit
        default_limits = {
            ResourceType.COMPUTE: settings.default_quota_cpu_cores,
            ResourceType.MEMORY: settings.default_quota_memory_gb,
            ResourceType.STORAGE: settings.default_quota_storage_gb,
            ResourceType.INSTANCES: settings.default_quota_instances,
            ResourceType.NETWORK: settings.default_quota_networks,
            ResourceType.DATABASE: settings.default_quota_databases
        }
        
        limit = default_limits.get(resource_type, 100)
        
        return await self.set_quota(tenant_id, resource_type, limit)
        
    async def _publish_quota_event(
        self,
        tenant_id: str,
        resource_type: ResourceType,
        current_usage: float,
        quota_limit: float,
        percentage_used: float,
        action: QuotaAction,
        message: Optional[str] = None
    ) -> None:
        """Publish quota event to Pulsar"""
        
        if not self.quota_event_producer:
            return
            
        try:
            event = QuotaEventSchema(
                event_type="quota_check",
                tenant_id=tenant_id,
                resource_type=resource_type.value,
                current_usage=current_usage,
                quota_limit=quota_limit,
                percentage_used=percentage_used,
                action=action.value,
                message=message,
                timestamp=datetime.now(timezone.utc).isoformat()
            )
            
            self.quota_event_producer.send(event)
            
        except Exception as e:
            logger.error(f"Failed to publish quota event: {e}")
            
    async def _check_quota_alerts(
        self,
        tenant_id: str,
        resource_type: ResourceType,
        usage_percentage: float,
        quota: ResourceQuota
    ) -> None:
        """Check and generate quota alerts"""
        
        alert_thresholds = [int(t) for t in settings.quota_alert_thresholds.split(",")]
        
        for threshold in alert_thresholds:
            if usage_percentage >= threshold:
                # Check if alert already sent
                alert_sent = await self.repository.check_alert_sent(
                    tenant_id=tenant_id,
                    resource_type=resource_type,
                    threshold=threshold
                )
                
                if not alert_sent:
                    alert = QuotaAlert(
                        tenant_id=tenant_id,
                        resource_type=resource_type,
                        threshold_percentage=threshold,
                        current_usage=quota.used,
                        quota_limit=quota.limit,
                        alert_type="threshold_exceeded" if threshold >= 100 else "threshold_warning",
                        message=f"{resource_type} usage has reached {usage_percentage:.1f}% of quota",
                        triggered_at=datetime.now(timezone.utc)
                    )
                    
                    await self.repository.save_quota_alert(alert)
                    
                    # Publish alert event
                    await self._publish_quota_event(
                        tenant_id=tenant_id,
                        resource_type=resource_type,
                        current_usage=quota.used,
                        quota_limit=quota.limit,
                        percentage_used=usage_percentage,
                        action=QuotaAction.WARN,
                        message=alert.message
                    )
                    
    async def process_resource_events(self) -> None:
        """Process resource lifecycle events"""
        
        while True:
            try:
                # Receive message with timeout
                msg = self.resource_event_consumer.receive(timeout_millis=1000)
                
                event = msg.value()
                logger.info(f"Processing resource event: {event.event_type} for {event.resource_id}")
                
                # Update usage based on event type
                if event.event_type == "created":
                    # Add resource usage
                    for resource_type, amount in event.resource_size.items():
                        await self.update_usage(
                            tenant_id=event.tenant_id,
                            resource_type=ResourceType(resource_type),
                            delta=amount
                        )
                        
                elif event.event_type == "deleted":
                    # Remove resource usage
                    for resource_type, amount in event.resource_size.items():
                        await self.update_usage(
                            tenant_id=event.tenant_id,
                            resource_type=ResourceType(resource_type),
                            delta=-amount
                        )
                        
                elif event.event_type == "updated":
                    # Handle resize - this would need old and new sizes
                    # For now, skip
                    pass
                    
                # Acknowledge message
                self.resource_event_consumer.acknowledge(msg)
                
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error processing resource event: {e}")
                    
            await asyncio.sleep(0.1)
            
    async def cleanup_old_usage_data(self) -> None:
        """Clean up old usage history data"""
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(
            days=settings.usage_history_retention_days
        )
        
        await self.repository.delete_old_usage_history(cutoff_date)
        
    async def close(self) -> None:
        """Close connections"""
        
        if self.quota_event_producer:
            self.quota_event_producer.close()
        if self.resource_event_consumer:
            self.resource_event_consumer.close()
        if self.pulsar_client:
            self.pulsar_client.close() 