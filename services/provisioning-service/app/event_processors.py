"""
Event Processors for Provisioning Service

Handles tenant provisioning and resource management events.
"""

import logging
import json
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid
import asyncio

from platformq_shared import (
    EventProcessor,
    event_handler,
    batch_event_handler,
    ProcessingResult,
    ProcessingStatus,
    ServiceClients
)
from platformq_events import (
    TenantCreatedEvent,
    UserCreatedEvent,
    ResourceAnomalyEvent,
    ServiceMetricsEvent,
    QuotaExceededEvent,
    ScalingRequestEvent,
    TenantUpgradedEvent,
    TenantDeletedEvent,
    ResourceCleanupRequestEvent
)

from .repository import (
    TenantProvisioningRepository,
    ResourceQuotaRepository,
    ScalingPolicyRepository,
    ScalingEventRepository,
    ResourceMetricsRepository
)
from .workflow import provision_tenant
from .dynamic_provisioning import (
    ResourceMonitor,
    ScalingEngine,
    TenantResourceManager,
    TenantTier
)
from .dependencies import (
    get_cassandra_session,
    get_minio_client,
    get_pulsar_admin,
    get_openproject_client,
    get_ignite_client,
    get_elasticsearch_client
)

logger = logging.getLogger(__name__)


class TenantProvisioningProcessor(EventProcessor):
    """Process tenant provisioning events"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 provisioning_repo: TenantProvisioningRepository,
                 quota_repo: ResourceQuotaRepository,
                 resource_manager: TenantResourceManager,
                 service_clients: ServiceClients):
        super().__init__(service_name, pulsar_url)
        self.provisioning_repo = provisioning_repo
        self.quota_repo = quota_repo
        self.resource_manager = resource_manager
        self.service_clients = service_clients
        
    async def on_start(self):
        """Initialize processor resources"""
        logger.info("Starting tenant provisioning processor")
        
    async def on_stop(self):
        """Cleanup processor resources"""
        logger.info("Stopping tenant provisioning processor")
        
    @event_handler("persistent://platformq/system/tenant-created-events", TenantCreatedEvent)
    async def handle_tenant_created(self, event: TenantCreatedEvent, msg):
        """Provision resources for new tenant"""
        try:
            tenant_id = uuid.UUID(event.tenant_id)
            
            # Create provisioning record
            record = self.provisioning_repo.create_provisioning_record(
                tenant_id=tenant_id,
                tenant_name=event.tenant_name,
                tier=event.tier,
                requested_by=event.created_by
            )
            
            # Get dependencies
            cassandra_session = get_cassandra_session()
            minio_client = get_minio_client()
            pulsar_admin = get_pulsar_admin()
            openproject_client = get_openproject_client()
            ignite_client = get_ignite_client()
            es_client = get_elasticsearch_client()
            
            try:
                # Provision tenant resources
                provision_tenant(
                    tenant_id=event.tenant_id,
                    tenant_name=event.tenant_name,
                    cassandra_session=cassandra_session,
                    minio_client=minio_client,
                    pulsar_admin=pulsar_admin,
                    openproject_client=openproject_client,
                    ignite_client=ignite_client,
                    es_client=es_client,
                )
                
                # Create resource quota based on tier
                tier_enum = TenantTier(event.tier.lower())
                self.resource_manager.create_tenant_quota(event.tenant_id, tier_enum)
                
                # Get quota details
                quotas = self.resource_manager.get_tier_quotas(tier_enum)
                
                # Store quota in repository
                self.quota_repo.create_quota(
                    tenant_id=event.tenant_id,
                    tier=event.tier,
                    cpu_cores=quotas['cpu_cores'],
                    memory_gb=quotas['memory_gb'],
                    storage_gb=quotas['storage_gb'],
                    max_pods=quotas['max_pods']
                )
                
                # Update provisioning status
                self.provisioning_repo.update_provisioning_status(
                    tenant_id=tenant_id,
                    status="completed",
                    details={
                        "resources": {
                            "cassandra_keyspace": f"tenant_{event.tenant_id}",
                            "minio_bucket": f"tenant-{event.tenant_id}",
                            "pulsar_namespace": f"platformq/{event.tenant_id}",
                            "openproject": "configured",
                            "ignite_cache": f"tenant_{event.tenant_id}",
                            "elasticsearch_index": f"tenant_{event.tenant_id}"
                        },
                        "quota": quotas
                    }
                )
                
                logger.info(f"Successfully provisioned tenant {event.tenant_id}")
                
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            except Exception as e:
                # Update provisioning status to failed
                self.provisioning_repo.update_provisioning_status(
                    tenant_id=tenant_id,
                    status="failed",
                    details={"error": str(e)}
                )
                raise
                
        except Exception as e:
            logger.error(f"Error provisioning tenant: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/tenant-upgraded-events", TenantUpgradedEvent)
    async def handle_tenant_upgraded(self, event: TenantUpgradedEvent, msg):
        """Handle tenant tier upgrade"""
        try:
            # Update quota based on new tier
            new_tier = TenantTier(event.new_tier.lower())
            quotas = self.resource_manager.get_tier_quotas(new_tier)
            
            # Update in repository
            updated = self.quota_repo.update_quota(
                tenant_id=event.tenant_id,
                updates={
                    'tier': event.new_tier,
                    'cpu_cores': quotas['cpu_cores'],
                    'memory_gb': quotas['memory_gb'],
                    'storage_gb': quotas['storage_gb'],
                    'max_pods': quotas['max_pods']
                }
            )
            
            if updated:
                # Update resource manager
                self.resource_manager.update_tenant_tier(event.tenant_id, new_tier)
                
                logger.info(f"Upgraded tenant {event.tenant_id} from {event.previous_tier} to {event.new_tier}")
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error upgrading tenant: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/system/tenant-deleted-events", TenantDeletedEvent)
    async def handle_tenant_deleted(self, event: TenantDeletedEvent, msg):
        """Handle tenant deletion and cleanup"""
        try:
            # TODO: Implement comprehensive cleanup
            # - Delete Cassandra keyspace
            # - Remove MinIO bucket
            # - Delete Pulsar namespace
            # - Clean up Ignite caches
            # - Remove Elasticsearch indices
            # - Delete resource quotas
            
            logger.info(f"Processing deletion for tenant {event.tenant_id}")
            
            # For now, just mark as deleted in provisioning records
            self.provisioning_repo.update_provisioning_status(
                tenant_id=uuid.UUID(event.tenant_id),
                status="deleted",
                details={"deleted_at": datetime.utcnow().isoformat()}
            )
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error deleting tenant: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )


class ResourceScalingProcessor(EventProcessor):
    """Process resource scaling events"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 scaling_engine: ScalingEngine,
                 policy_repo: ScalingPolicyRepository,
                 event_repo: ScalingEventRepository,
                 metrics_repo: ResourceMetricsRepository,
                 service_clients: ServiceClients):
        super().__init__(service_name, pulsar_url)
        self.scaling_engine = scaling_engine
        self.policy_repo = policy_repo
        self.event_repo = event_repo
        self.metrics_repo = metrics_repo
        self.service_clients = service_clients
        
    @event_handler("persistent://platformq/system/resource-anomaly-events", ResourceAnomalyEvent)
    async def handle_resource_anomaly(self, event: ResourceAnomalyEvent, msg):
        """Handle resource anomalies"""
        try:
            logger.warning(f"Resource anomaly detected: {event.anomaly_type} for {event.service_name}")
            
            # Get scaling policy
            policy = self.policy_repo.get_policy(event.service_name)
            if not policy or not policy['enabled']:
                logger.info(f"No active scaling policy for {event.service_name}")
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Determine scaling action
            current_replicas = event.current_replicas
            target_replicas = current_replicas
            
            if event.anomaly_type == "high_cpu":
                if event.metric_value > policy['target_cpu_percent']:
                    target_replicas = min(current_replicas + 1, policy['max_replicas'])
                    reason = f"CPU usage {event.metric_value}% > target {policy['target_cpu_percent']}%"
                    
            elif event.anomaly_type == "high_memory":
                if event.metric_value > policy['target_memory_percent']:
                    target_replicas = min(current_replicas + 1, policy['max_replicas'])
                    reason = f"Memory usage {event.metric_value}% > target {policy['target_memory_percent']}%"
                    
            elif event.anomaly_type == "low_utilization":
                if current_replicas > policy['min_replicas']:
                    target_replicas = current_replicas - 1
                    reason = f"Low utilization detected"
                    
            else:
                logger.warning(f"Unknown anomaly type: {event.anomaly_type}")
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
                
            # Check if scaling is needed
            if target_replicas != current_replicas:
                # Check cooldown period
                if await self.scaling_engine.check_cooldown(
                    event.service_name,
                    scale_up=(target_replicas > current_replicas)
                ):
                    # Perform scaling
                    success = await self.scaling_engine.scale_service(
                        event.service_name,
                        target_replicas
                    )
                    
                    if success:
                        # Record scaling event
                        self.event_repo.record_scaling_event(
                            service_name=event.service_name,
                            tenant_id=event.tenant_id,
                            previous_replicas=current_replicas,
                            new_replicas=target_replicas,
                            reason=reason,
                            metrics={
                                "anomaly_type": event.anomaly_type,
                                "metric_value": event.metric_value
                            }
                        )
                        
                        # Trigger Airflow workflow for complex scaling
                        await self.service_clients.post(
                            "http://workflow-service:8000/api/v1/workflows/scaling_workflow/trigger",
                            json={
                                "anomaly": event.dict(),
                                "scaling_action": {
                                    "service": event.service_name,
                                    "from_replicas": current_replicas,
                                    "to_replicas": target_replicas,
                                    "reason": reason
                                }
                            }
                        )
                        
                        logger.info(f"Scaled {event.service_name} from {current_replicas} to {target_replicas}: {reason}")
                else:
                    logger.info(f"Scaling for {event.service_name} in cooldown period")
                    
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error handling resource anomaly: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/scaling-request-events", ScalingRequestEvent)
    async def handle_scaling_request(self, event: ScalingRequestEvent, msg):
        """Handle manual scaling requests"""
        try:
            # Validate request
            policy = self.policy_repo.get_policy(event.service_name)
            if policy:
                if event.target_replicas < policy['min_replicas']:
                    logger.warning(f"Requested replicas below minimum for {event.service_name}")
                    event.target_replicas = policy['min_replicas']
                elif event.target_replicas > policy['max_replicas']:
                    logger.warning(f"Requested replicas above maximum for {event.service_name}")
                    event.target_replicas = policy['max_replicas']
                    
            # Get current state
            current_metrics = self.metrics_repo.get_latest_metrics(
                event.service_name,
                event.tenant_id
            )
            
            current_replicas = current_metrics['pod_count'] if current_metrics else 1
            
            # Perform scaling
            success = await self.scaling_engine.scale_service(
                event.service_name,
                event.target_replicas
            )
            
            if success:
                # Record event
                self.event_repo.record_scaling_event(
                    service_name=event.service_name,
                    tenant_id=event.tenant_id,
                    previous_replicas=current_replicas,
                    new_replicas=event.target_replicas,
                    reason=f"Manual scaling request: {event.reason}",
                    metrics={}
                )
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error handling scaling request: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @batch_event_handler(
        "persistent://platformq/*/service-metrics-events",
        ServiceMetricsEvent,
        max_batch_size=100,
        max_wait_time_ms=10000
    )
    async def handle_service_metrics(self, events: List[ServiceMetricsEvent], msgs):
        """Process batch of service metrics"""
        try:
            # Store metrics
            for event in events:
                self.metrics_repo.store_metrics(
                    service_name=event.service_name,
                    tenant_id=event.tenant_id,
                    cpu_usage=event.cpu_usage,
                    memory_usage=event.memory_usage,
                    request_rate=event.request_rate,
                    error_rate=event.error_rate,
                    response_time_p99=event.response_time_p99,
                    pod_count=event.pod_count
                )
                
            # Analyze for auto-scaling
            services_analyzed = set()
            for event in events:
                if event.service_name not in services_analyzed:
                    services_analyzed.add(event.service_name)
                    
                    # Get policy
                    policy = self.policy_repo.get_policy(event.service_name)
                    if policy and policy['enabled']:
                        # Check if scaling needed
                        await self.scaling_engine.analyze_and_scale(
                            event.service_name,
                            event.tenant_id
                        )
                        
            logger.info(f"Processed {len(events)} metrics events for {len(services_analyzed)} services")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing service metrics: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )


class QuotaManagementProcessor(EventProcessor):
    """Process quota management events"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 quota_repo: ResourceQuotaRepository,
                 resource_manager: TenantResourceManager,
                 service_clients: ServiceClients):
        super().__init__(service_name, pulsar_url)
        self.quota_repo = quota_repo
        self.resource_manager = resource_manager
        self.service_clients = service_clients
        
    @event_handler("persistent://platformq/*/quota-exceeded-events", QuotaExceededEvent)
    async def handle_quota_exceeded(self, event: QuotaExceededEvent, msg):
        """Handle quota exceeded events"""
        try:
            logger.warning(f"Quota exceeded for tenant {event.tenant_id}: {event.resource_type}")
            
            # Get current quota and usage
            quota_info = self.quota_repo.get_quota_usage(event.tenant_id)
            
            # Send notification
            await self.service_clients.post(
                "http://notification-service:8000/api/v1/notifications",
                json={
                    "tenant_id": event.tenant_id,
                    "type": "quota_warning",
                    "severity": "high",
                    "title": f"{event.resource_type.upper()} Quota Exceeded",
                    "message": f"Your {event.resource_type} usage ({event.current_usage:.1f}) has exceeded the quota ({event.quota_limit:.1f})",
                    "data": {
                        "resource_type": event.resource_type,
                        "current_usage": event.current_usage,
                        "quota_limit": event.quota_limit,
                        "recommendation": "Consider upgrading your plan or optimizing resource usage"
                    }
                }
            )
            
            # Apply resource limits if critical
            if event.current_usage > event.quota_limit * 1.2:  # 20% over quota
                logger.error(f"Critical quota violation for tenant {event.tenant_id}")
                
                # Enforce hard limits
                await self.resource_manager.enforce_quota_limits(
                    event.tenant_id,
                    event.resource_type
                )
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error handling quota exceeded: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )


class ResourceCleanupProcessor(EventProcessor):
    """Process resource cleanup events"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 service_clients: ServiceClients):
        super().__init__(service_name, pulsar_url)
        self.service_clients = service_clients
        
    @event_handler("persistent://platformq/system/resource-cleanup-request-events", ResourceCleanupRequestEvent)
    async def handle_cleanup_request(self, event: ResourceCleanupRequestEvent, msg):
        """Handle resource cleanup requests"""
        try:
            logger.info(f"Processing cleanup request for {event.resource_type}: {event.resource_id}")
            
            if event.resource_type == "orphaned_pods":
                # Clean up orphaned Kubernetes pods
                await self._cleanup_orphaned_pods(event.tenant_id)
                
            elif event.resource_type == "unused_storage":
                # Clean up unused storage
                await self._cleanup_unused_storage(event.tenant_id)
                
            elif event.resource_type == "stale_caches":
                # Clean up stale Ignite caches
                await self._cleanup_stale_caches(event.tenant_id)
                
            else:
                logger.warning(f"Unknown cleanup type: {event.resource_type}")
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing cleanup request: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    async def _cleanup_orphaned_pods(self, tenant_id: str):
        """Clean up orphaned Kubernetes pods"""
        # TODO: Implement Kubernetes API calls to clean up pods
        logger.info(f"Cleaning up orphaned pods for tenant {tenant_id}")
        
    async def _cleanup_unused_storage(self, tenant_id: str):
        """Clean up unused storage"""
        # TODO: Implement MinIO cleanup for unused objects
        logger.info(f"Cleaning up unused storage for tenant {tenant_id}")
        
    async def _cleanup_stale_caches(self, tenant_id: str):
        """Clean up stale Ignite caches"""
        # TODO: Implement Ignite cache cleanup
        logger.info(f"Cleaning up stale caches for tenant {tenant_id}")


class UserProvisioningProcessor(EventProcessor):
    """Process user provisioning events"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 service_clients: ServiceClients):
        super().__init__(service_name, pulsar_url)
        self.service_clients = service_clients
        
    @event_handler("persistent://platformq/*/user-created-events", UserCreatedEvent)
    async def handle_user_created(self, event: UserCreatedEvent, msg):
        """Provision resources for new user"""
        try:
            logger.info(f"Provisioning resources for user {event.user_id}")
            
            # Create user-specific resources
            # - Personal storage space in MinIO
            # - User preferences cache in Ignite
            # - Initialize user quotas
            
            # For now, just log
            logger.info(f"User {event.username} provisioned in tenant {event.tenant_id}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error provisioning user: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            ) 