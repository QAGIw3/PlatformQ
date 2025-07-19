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
            # Comprehensive tenant cleanup
            logger.info(f"Starting comprehensive cleanup for tenant {event.tenant_id}")
            cleanup_results = {}
            
            # 1. Delete Cassandra keyspace
            try:
                keyspace_name = f"tenant_{event.tenant_id.replace('-', '_')}"
                await self.service_clients.cassandra_session.execute(
                    f"DROP KEYSPACE IF EXISTS {keyspace_name}"
                )
                cleanup_results['cassandra'] = "success"
                logger.info(f"Deleted Cassandra keyspace: {keyspace_name}")
            except Exception as e:
                cleanup_results['cassandra'] = f"failed: {str(e)}"
                logger.error(f"Failed to delete Cassandra keyspace: {e}")
            
            # 2. Remove MinIO bucket
            try:
                bucket_name = f"tenant-{event.tenant_id}"
                # List and delete all objects first
                objects = self.service_clients.minio_client.list_objects(
                    bucket_name, recursive=True
                )
                for obj in objects:
                    self.service_clients.minio_client.remove_object(
                        bucket_name, obj.object_name
                    )
                # Remove bucket
                self.service_clients.minio_client.remove_bucket(bucket_name)
                cleanup_results['minio'] = "success"
                logger.info(f"Deleted MinIO bucket: {bucket_name}")
            except Exception as e:
                cleanup_results['minio'] = f"failed: {str(e)}"
                logger.error(f"Failed to delete MinIO bucket: {e}")
            
            # 3. Delete Pulsar namespace
            try:
                namespace = f"platformq/{event.tenant_id}"
                # Delete all topics in namespace first
                topics = self.service_clients.pulsar_admin.namespaces().get_topics(namespace)
                for topic in topics:
                    self.service_clients.pulsar_admin.topics().delete(topic)
                # Delete namespace
                self.service_clients.pulsar_admin.namespaces().delete(namespace)
                cleanup_results['pulsar'] = "success"
                logger.info(f"Deleted Pulsar namespace: {namespace}")
            except Exception as e:
                cleanup_results['pulsar'] = f"failed: {str(e)}"
                logger.error(f"Failed to delete Pulsar namespace: {e}")
            
            # 4. Clean up Ignite caches
            try:
                # Get all caches for tenant
                cache_prefix = f"tenant_{event.tenant_id}_"
                for cache_name in self.service_clients.ignite_client.get_cache_names():
                    if cache_name.startswith(cache_prefix):
                        cache = self.service_clients.ignite_client.get_cache(cache_name)
                        cache.destroy()
                        logger.info(f"Destroyed Ignite cache: {cache_name}")
                cleanup_results['ignite'] = "success"
            except Exception as e:
                cleanup_results['ignite'] = f"failed: {str(e)}"
                logger.error(f"Failed to clean up Ignite caches: {e}")
            
            # 5. Remove Elasticsearch indices
            try:
                index_pattern = f"tenant-{event.tenant_id}-*"
                indices = self.service_clients.es_client.indices.get(index=index_pattern)
                for index_name in indices:
                    self.service_clients.es_client.indices.delete(index=index_name)
                    logger.info(f"Deleted Elasticsearch index: {index_name}")
                cleanup_results['elasticsearch'] = "success"
            except Exception as e:
                cleanup_results['elasticsearch'] = f"failed: {str(e)}"
                logger.error(f"Failed to delete Elasticsearch indices: {e}")
            
            # 6. Delete resource quotas from Kubernetes
            try:
                namespace = f"tenant-{event.tenant_id}"
                # Delete ResourceQuota
                self.service_clients.k8s_core_v1.delete_namespaced_resource_quota(
                    name=f"{namespace}-quota",
                    namespace=namespace
                )
                # Delete LimitRange
                self.service_clients.k8s_core_v1.delete_namespaced_limit_range(
                    name=f"{namespace}-limits",
                    namespace=namespace
                )
                cleanup_results['k8s_quotas'] = "success"
                logger.info(f"Deleted Kubernetes resource quotas for namespace: {namespace}")
            except Exception as e:
                cleanup_results['k8s_quotas'] = f"failed: {str(e)}"
                logger.error(f"Failed to delete K8s resource quotas: {e}")
            
            # Update provisioning records with cleanup results
            self.provisioning_repo.update_provisioning_status(
                tenant_id=uuid.UUID(event.tenant_id),
                status="deleted",
                details={
                    "deleted_at": datetime.utcnow().isoformat(),
                    "cleanup_results": cleanup_results
                }
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
        logger.info(f"Cleaning up orphaned pods for tenant {tenant_id}")
        
        try:
            namespace = f"tenant-{tenant_id}"
            
            # List all pods in tenant namespace
            pods = self.service_clients.k8s_core_v1.list_namespaced_pod(
                namespace=namespace
            )
            
            orphaned_count = 0
            for pod in pods.items:
                # Check if pod is orphaned (no owner references)
                if not pod.metadata.owner_references:
                    # Check pod age - consider orphaned if older than 1 hour
                    age = datetime.utcnow() - pod.metadata.creation_timestamp.replace(tzinfo=None)
                    if age > timedelta(hours=1):
                        # Delete orphaned pod
                        self.service_clients.k8s_core_v1.delete_namespaced_pod(
                            name=pod.metadata.name,
                            namespace=namespace
                        )
                        orphaned_count += 1
                        logger.info(f"Deleted orphaned pod: {pod.metadata.name}")
                
                # Check for failed/evicted pods
                elif pod.status.phase in ["Failed", "Evicted"]:
                    # Delete failed pods older than 30 minutes
                    age = datetime.utcnow() - pod.metadata.creation_timestamp.replace(tzinfo=None)
                    if age > timedelta(minutes=30):
                        self.service_clients.k8s_core_v1.delete_namespaced_pod(
                            name=pod.metadata.name,
                            namespace=namespace
                        )
                        orphaned_count += 1
                        logger.info(f"Deleted {pod.status.phase} pod: {pod.metadata.name}")
            
            # Clean up completed jobs
            jobs = self.service_clients.k8s_batch_v1.list_namespaced_job(
                namespace=namespace
            )
            
            for job in jobs.items:
                if job.status.succeeded and job.status.completion_time:
                    age = datetime.utcnow() - job.status.completion_time.replace(tzinfo=None)
                    if age > timedelta(hours=24):  # Clean up jobs older than 24 hours
                        self.service_clients.k8s_batch_v1.delete_namespaced_job(
                            name=job.metadata.name,
                            namespace=namespace,
                            propagation_policy='Background'
                        )
                        logger.info(f"Deleted completed job: {job.metadata.name}")
            
            logger.info(f"Cleaned up {orphaned_count} orphaned pods for tenant {tenant_id}")
            
        except Exception as e:
            logger.error(f"Error cleaning up orphaned pods: {e}")
        
    async def _cleanup_unused_storage(self, tenant_id: str):
        """Clean up unused storage"""
        logger.info(f"Cleaning up unused storage for tenant {tenant_id}")
        
        try:
            bucket_name = f"tenant-{tenant_id}"
            
            # Get storage usage statistics
            total_size = 0
            object_count = 0
            old_objects = []
            
            # List all objects
            objects = self.service_clients.minio_client.list_objects(
                bucket_name, recursive=True
            )
            
            for obj in objects:
                total_size += obj.size
                object_count += 1
                
                # Check object age
                age = datetime.utcnow() - obj.last_modified.replace(tzinfo=None)
                
                # Mark old temporary files for deletion
                if ('temp/' in obj.object_name or 
                    '/tmp/' in obj.object_name or 
                    obj.object_name.endswith('.tmp')):
                    if age > timedelta(hours=24):
                        old_objects.append(obj.object_name)
                
                # Mark old cache files for deletion
                elif 'cache/' in obj.object_name:
                    if age > timedelta(days=7):
                        old_objects.append(obj.object_name)
                
                # Mark old log files for deletion
                elif obj.object_name.endswith('.log'):
                    if age > timedelta(days=30):
                        old_objects.append(obj.object_name)
            
            # Delete old objects
            deleted_count = 0
            deleted_size = 0
            
            for obj_name in old_objects:
                try:
                    # Get object size before deletion
                    stat = self.service_clients.minio_client.stat_object(
                        bucket_name, obj_name
                    )
                    deleted_size += stat.size
                    
                    # Delete object
                    self.service_clients.minio_client.remove_object(
                        bucket_name, obj_name
                    )
                    deleted_count += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to delete object {obj_name}: {e}")
            
            # Set lifecycle policy for automatic cleanup
            lifecycle_config = {
                "Rules": [
                    {
                        "ID": "cleanup-temp-files",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "temp/"},
                        "Expiration": {"Days": 1}
                    },
                    {
                        "ID": "cleanup-old-logs",
                        "Status": "Enabled", 
                        "Filter": {"Prefix": "logs/"},
                        "Expiration": {"Days": 30}
                    },
                    {
                        "ID": "cleanup-cache",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "cache/"},
                        "Expiration": {"Days": 7}
                    }
                ]
            }
            
            # Note: MinIO Python client doesn't have direct lifecycle API
            # In production, use mc client or S3 API
            
            logger.info(
                f"Storage cleanup for tenant {tenant_id}: "
                f"Deleted {deleted_count} objects ({deleted_size / 1024 / 1024:.2f} MB), "
                f"Remaining: {object_count - deleted_count} objects ({(total_size - deleted_size) / 1024 / 1024:.2f} MB)"
            )
            
        except Exception as e:
            logger.error(f"Error cleaning up unused storage: {e}")
        
    async def _cleanup_stale_caches(self, tenant_id: str):
        """Clean up stale Ignite caches"""
        logger.info(f"Cleaning up stale caches for tenant {tenant_id}")
        
        try:
            cache_prefix = f"tenant_{tenant_id}_"
            cleaned_entries = 0
            
            # Get all caches for tenant
            for cache_name in self.service_clients.ignite_client.get_cache_names():
                if cache_name.startswith(cache_prefix):
                    cache = self.service_clients.ignite_client.get_cache(cache_name)
                    
                    # Scan cache entries
                    with cache.scan() as cursor:
                        for key, value in cursor:
                            try:
                                # Check if entry has timestamp
                                if isinstance(value, dict) and 'timestamp' in value:
                                    # Parse timestamp
                                    if isinstance(value['timestamp'], str):
                                        entry_time = datetime.fromisoformat(
                                            value['timestamp'].replace('Z', '+00:00')
                                        )
                                    else:
                                        entry_time = datetime.fromtimestamp(value['timestamp'])
                                    
                                    # Remove entries older than 7 days
                                    age = datetime.utcnow() - entry_time
                                    if age > timedelta(days=7):
                                        cache.remove(key)
                                        cleaned_entries += 1
                                
                                # Check TTL-based entries
                                elif isinstance(value, dict) and 'ttl' in value:
                                    created_at = value.get('created_at', 0)
                                    ttl = value.get('ttl', 0)
                                    
                                    if created_at + ttl < datetime.utcnow().timestamp():
                                        cache.remove(key)
                                        cleaned_entries += 1
                                        
                            except Exception as e:
                                logger.warning(f"Error processing cache entry {key}: {e}")
                    
                    # Get cache metrics
                    cache_size = cache.get_size()
                    logger.info(
                        f"Cache {cache_name}: Removed {cleaned_entries} stale entries, "
                        f"Remaining size: {cache_size}"
                    )
            
            # Clear empty caches
            for cache_name in self.service_clients.ignite_client.get_cache_names():
                if cache_name.startswith(cache_prefix):
                    cache = self.service_clients.ignite_client.get_cache(cache_name)
                    if cache.get_size() == 0:
                        cache.destroy()
                        logger.info(f"Destroyed empty cache: {cache_name}")
            
        except Exception as e:
            logger.error(f"Error cleaning up stale caches: {e}")


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