"""Provisioning Orchestrator

Orchestrates the provisioning of resources across all infrastructure services.
"""

import logging
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime

from platformq_provisioning_common import (
    IProvisioningOrchestrator,
    ProvisioningRequest,
    ProvisioningResult,
    ProvisioningStatus,
    ProvisioningError,
    ResourceType,
    InfrastructureResource
)

from .config import Settings
from .provisioners import (
    CassandraProvisioner,
    MinioProvisioner,
    PulsarProvisioner,
    IgniteProvisioner,
    ElasticsearchProvisioner,
    JanusGraphProvisioner,
    KubernetesProvisioner,
    OpenProjectProvisioner,
    NextcloudProvisioner,
    VaultProvisioner,
    ConsulProvisioner
)
from .repository import ProvisioningRepository

logger = logging.getLogger(__name__)


class ProvisioningOrchestrator(IProvisioningOrchestrator):
    """Orchestrates tenant provisioning across all services"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.repository = ProvisioningRepository(settings)
        
        # Initialize provisioners
        self.provisioners = {
            ResourceType.CASSANDRA_KEYSPACE: CassandraProvisioner(settings),
            ResourceType.MINIO_BUCKET: MinioProvisioner(settings),
            ResourceType.PULSAR_NAMESPACE: PulsarProvisioner(settings),
            ResourceType.IGNITE_CACHE: IgniteProvisioner(settings),
            ResourceType.ELASTICSEARCH_INDEX: ElasticsearchProvisioner(settings),
            ResourceType.JANUSGRAPH_SCHEMA: JanusGraphProvisioner(settings),
            ResourceType.KUBERNETES_NAMESPACE: KubernetesProvisioner(settings),
            ResourceType.OPENPROJECT_PROJECT: OpenProjectProvisioner(settings),
            ResourceType.NEXTCLOUD_USER: NextcloudProvisioner(settings),
            ResourceType.VAULT_SECRETS: VaultProvisioner(settings),
            ResourceType.CONSUL_CONFIG: ConsulProvisioner(settings)
        }
        
        # Provisioning order (dependencies)
        self.provisioning_order = [
            ResourceType.KUBERNETES_NAMESPACE,  # Create namespace first
            ResourceType.VAULT_SECRETS,         # Set up secrets
            ResourceType.CONSUL_CONFIG,         # Configure services
            ResourceType.CASSANDRA_KEYSPACE,   # Create data storage
            ResourceType.MINIO_BUCKET,         # Object storage
            ResourceType.IGNITE_CACHE,         # Caching layer
            ResourceType.ELASTICSEARCH_INDEX,   # Search indices
            ResourceType.JANUSGRAPH_SCHEMA,    # Graph schema
            ResourceType.PULSAR_NAMESPACE,     # Messaging
            ResourceType.OPENPROJECT_PROJECT,  # Project management
            ResourceType.NEXTCLOUD_USER        # File sharing
        ]
    
    async def initialize(self):
        """Initialize the orchestrator and provisioners"""
        await self.repository.initialize()
        
        # Initialize all provisioners
        init_tasks = []
        for provisioner in self.provisioners.values():
            if hasattr(provisioner, 'initialize'):
                init_tasks.append(provisioner.initialize())
        
        if init_tasks:
            await asyncio.gather(*init_tasks, return_exceptions=True)
        
        logger.info("Provisioning orchestrator initialized")
    
    async def shutdown(self):
        """Shutdown the orchestrator"""
        # Shutdown all provisioners
        shutdown_tasks = []
        for provisioner in self.provisioners.values():
            if hasattr(provisioner, 'shutdown'):
                shutdown_tasks.append(provisioner.shutdown())
        
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)
        
        await self.repository.close()
        logger.info("Provisioning orchestrator shutdown")
    
    async def provision_tenant(self, request: ProvisioningRequest) -> ProvisioningResult:
        """Orchestrate provisioning of all resources for a tenant"""
        logger.info(f"Starting provisioning for tenant {request.tenant_id}")
        
        # Create result object
        result = ProvisioningResult(
            request_id=request.request_id,
            tenant_id=request.tenant_id,
            status=ProvisioningStatus.IN_PROGRESS,
            started_at=datetime.utcnow(),
            metadata=request.metadata
        )
        
        # Store request in repository
        await self.repository.create_request(request)
        
        try:
            # Determine which resources to provision
            resources_to_provision = request.resources_to_provision or list(ResourceType)
            
            # Filter by provisioning order
            ordered_resources = [
                r for r in self.provisioning_order 
                if r in resources_to_provision
            ]
            
            if self.settings.parallel_provisioning:
                # Group resources that can be provisioned in parallel
                await self._provision_parallel(request, result, ordered_resources)
            else:
                # Provision sequentially
                await self._provision_sequential(request, result, ordered_resources)
            
            # Update final status
            if result.failed_resources:
                result.status = ProvisioningStatus.PARTIALLY_COMPLETED
            else:
                result.status = ProvisioningStatus.COMPLETED
            
            result.completed_at = datetime.utcnow()
            result.total_duration_seconds = (
                result.completed_at - result.started_at
            ).total_seconds()
            
        except Exception as e:
            logger.error(f"Provisioning failed for tenant {request.tenant_id}: {e}")
            result.status = ProvisioningStatus.FAILED
            
            # Rollback if needed
            if result.provisioned_resources:
                await self._rollback_provisioning(request.tenant_id, result)
        
        # Update repository
        await self.repository.update_request_status(request.request_id, result.status)
        
        logger.info(f"Provisioning completed for tenant {request.tenant_id}: {result.status}")
        return result
    
    async def _provision_sequential(
        self,
        request: ProvisioningRequest,
        result: ProvisioningResult,
        resources: List[ResourceType]
    ):
        """Provision resources sequentially"""
        for resource_type in resources:
            if resource_type not in self.provisioners:
                logger.warning(f"No provisioner for {resource_type}")
                continue
            
            provisioner = self.provisioners[resource_type]
            
            try:
                # Provision resource
                resource = await provisioner.provision(
                    request.tenant_id,
                    request.tenant_name,
                    request.metadata
                )
                
                result.provisioned_resources.append(resource)
                await self.repository.add_provisioned_resource(
                    request.request_id, resource
                )
                
            except Exception as e:
                logger.error(f"Failed to provision {resource_type}: {e}")
                
                failed_resource = InfrastructureResource(
                    resource_type=resource_type,
                    resource_name=f"{resource_type}-{request.tenant_id}",
                    tenant_id=request.tenant_id,
                    status=ProvisioningStatus.FAILED,
                    error_message=str(e)
                )
                result.failed_resources.append(failed_resource)
                
                # Stop on first failure if not dry run
                if not request.dry_run:
                    raise
    
    async def _provision_parallel(
        self,
        request: ProvisioningRequest,
        result: ProvisioningResult,
        resources: List[ResourceType]
    ):
        """Provision resources in parallel where possible"""
        # Group resources by dependency level
        dependency_groups = self._group_by_dependencies(resources)
        
        for group in dependency_groups:
            # Provision all resources in this group in parallel
            tasks = []
            
            for resource_type in group:
                if resource_type not in self.provisioners:
                    continue
                
                provisioner = self.provisioners[resource_type]
                task = self._provision_resource_async(
                    provisioner, resource_type, request
                )
                tasks.append(task)
            
            # Wait for all tasks in this group
            if tasks:
                resources_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for resource_result in resources_results:
                    if isinstance(resource_result, Exception):
                        # Handle failure
                        logger.error(f"Provisioning failed: {resource_result}")
                        # Continue with other resources
                    elif isinstance(resource_result, InfrastructureResource):
                        if resource_result.status == ProvisioningStatus.FAILED:
                            result.failed_resources.append(resource_result)
                        else:
                            result.provisioned_resources.append(resource_result)
                            await self.repository.add_provisioned_resource(
                                request.request_id, resource_result
                            )
    
    async def _provision_resource_async(
        self,
        provisioner,
        resource_type: ResourceType,
        request: ProvisioningRequest
    ) -> InfrastructureResource:
        """Provision a single resource asynchronously"""
        try:
            return await provisioner.provision(
                request.tenant_id,
                request.tenant_name,
                request.metadata
            )
        except Exception as e:
            logger.error(f"Failed to provision {resource_type}: {e}")
            return InfrastructureResource(
                resource_type=resource_type,
                resource_name=f"{resource_type}-{request.tenant_id}",
                tenant_id=request.tenant_id,
                status=ProvisioningStatus.FAILED,
                error_message=str(e)
            )
    
    def _group_by_dependencies(
        self,
        resources: List[ResourceType]
    ) -> List[List[ResourceType]]:
        """Group resources by dependency level for parallel provisioning"""
        # Define dependency groups
        groups = [
            # Infrastructure layer
            [ResourceType.KUBERNETES_NAMESPACE, ResourceType.VAULT_SECRETS, 
             ResourceType.CONSUL_CONFIG],
            # Data layer
            [ResourceType.CASSANDRA_KEYSPACE, ResourceType.MINIO_BUCKET,
             ResourceType.IGNITE_CACHE],
            # Service layer
            [ResourceType.ELASTICSEARCH_INDEX, ResourceType.JANUSGRAPH_SCHEMA,
             ResourceType.PULSAR_NAMESPACE],
            # Application layer
            [ResourceType.OPENPROJECT_PROJECT, ResourceType.NEXTCLOUD_USER]
        ]
        
        # Filter groups to only include requested resources
        filtered_groups = []
        for group in groups:
            filtered_group = [r for r in group if r in resources]
            if filtered_group:
                filtered_groups.append(filtered_group)
        
        return filtered_groups
    
    async def _rollback_provisioning(
        self,
        tenant_id: str,
        result: ProvisioningResult
    ):
        """Rollback provisioned resources on failure"""
        logger.info(f"Rolling back provisioning for tenant {tenant_id}")
        
        # Deprovision in reverse order
        for resource in reversed(result.provisioned_resources):
            if resource.resource_type not in self.provisioners:
                continue
            
            provisioner = self.provisioners[resource.resource_type]
            
            try:
                await provisioner.deprovision(
                    tenant_id,
                    resource.resource_name
                )
                logger.info(f"Rolled back {resource.resource_type}")
            except Exception as e:
                logger.error(f"Failed to rollback {resource.resource_type}: {e}")
        
        result.rollback_performed = True
    
    async def deprovision_tenant(self, tenant_id: str) -> ProvisioningResult:
        """Deprovision all resources for a tenant"""
        logger.info(f"Starting deprovisioning for tenant {tenant_id}")
        
        # Get tenant resources
        resources = await self.repository.get_tenant_resources(tenant_id)
        
        result = ProvisioningResult(
            request_id=f"deprov-{tenant_id}",
            tenant_id=tenant_id,
            status=ProvisioningStatus.IN_PROGRESS,
            started_at=datetime.utcnow()
        )
        
        # Deprovision in reverse order
        for resource_type in reversed(self.provisioning_order):
            if resource_type not in self.provisioners:
                continue
            
            # Find resource
            resource = next(
                (r for r in resources if r.resource_type == resource_type),
                None
            )
            
            if not resource:
                continue
            
            provisioner = self.provisioners[resource_type]
            
            try:
                success = await provisioner.deprovision(
                    tenant_id,
                    resource.resource_name
                )
                
                if success:
                    logger.info(f"Deprovisioned {resource_type}")
                else:
                    result.failed_resources.append(resource)
                    
            except Exception as e:
                logger.error(f"Failed to deprovision {resource_type}: {e}")
                result.failed_resources.append(resource)
        
        result.status = (
            ProvisioningStatus.COMPLETED 
            if not result.failed_resources 
            else ProvisioningStatus.PARTIALLY_COMPLETED
        )
        result.completed_at = datetime.utcnow()
        
        return result
    
    async def get_provisioning_status(
        self,
        request_id: str
    ) -> Optional[ProvisioningResult]:
        """Get the status of a provisioning request"""
        return await self.repository.get_provisioning_result(request_id)
    
    async def retry_failed_resources(
        self,
        request_id: str
    ) -> ProvisioningResult:
        """Retry provisioning of failed resources"""
        # Get original request
        request = await self.repository.get_request(request_id)
        if not request:
            raise ProvisioningError(f"Request {request_id} not found")
        
        # Get current result
        current_result = await self.repository.get_provisioning_result(request_id)
        if not current_result or not current_result.failed_resources:
            raise ProvisioningError("No failed resources to retry")
        
        # Create new request for failed resources
        retry_request = ProvisioningRequest(
            tenant_id=request.tenant_id,
            tenant_name=request.tenant_name,
            tier=request.tier,
            requested_by=request.requested_by,
            resources_to_provision=[
                r.resource_type for r in current_result.failed_resources
            ],
            metadata={
                **request.metadata,
                "retry_of": request_id,
                "retry_attempt": current_result.metadata.get("retry_attempt", 0) + 1
            }
        )
        
        # Provision failed resources
        return await self.provision_tenant(retry_request) 