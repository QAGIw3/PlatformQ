"""
Infrastructure Provisioning Orchestrator

Manages provisioning of infrastructure resources in a coordinated manner.
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import uuid

from platformq_resource_common import (
    ResourceType, InfrastructureResource, ProvisioningRequest,
    ProvisioningResult, ProvisioningStatus, ResourceStatus,
    IResourceProvisioner
)

from .core.config import Settings
from .repository import InfrastructureRepository

# Import provisioners
from .provisioners.cassandra import CassandraProvisioner
from .provisioners.elasticsearch import ElasticsearchProvisioner
from .provisioners.ignite import IgniteProvisioner
from .provisioners.minio import MinioProvisioner
from .provisioners.pulsar import PulsarProvisioner
from .provisioners.consul import ConsulProvisioner
from .provisioners.vault import VaultProvisioner
from .provisioners.janusgraph import JanusGraphProvisioner

logger = logging.getLogger(__name__)


class InfrastructureOrchestrator:
    """Orchestrates infrastructure provisioning"""
    
    def __init__(self, settings: Settings, repository: InfrastructureRepository):
        self.settings = settings
        self.repository = repository
        self.provisioners: Dict[ResourceType, IResourceProvisioner] = {}
        self._initialized = False
    
    async def initialize(self):
        """Initialize orchestrator and provisioners"""
        if self._initialized:
            return
        
        logger.info("Initializing infrastructure orchestrator")
        
        # Initialize provisioners
        await self._initialize_provisioners()
        
        self._initialized = True
        logger.info("Infrastructure orchestrator initialized")
    
    async def shutdown(self):
        """Shutdown orchestrator and cleanup"""
        logger.info("Shutting down infrastructure orchestrator")
        
        # Shutdown provisioners
        for provisioner in self.provisioners.values():
            if hasattr(provisioner, 'shutdown'):
                await provisioner.shutdown()
        
        self._initialized = False
    
    async def _initialize_provisioners(self):
        """Initialize all available provisioners"""
        provisioner_classes = [
            CassandraProvisioner,
            ElasticsearchProvisioner,
            IgniteProvisioner,
            MinioProvisioner,
            PulsarProvisioner,
            ConsulProvisioner,
            VaultProvisioner,
            JanusGraphProvisioner,
        ]
        
        for provisioner_class in provisioner_classes:
            try:
                provisioner = provisioner_class(self.settings)
                if hasattr(provisioner, 'initialize'):
                    await provisioner.initialize()
                
                resource_type = provisioner.get_resource_type()
                self.provisioners[resource_type] = provisioner
                logger.info(f"Initialized provisioner for {resource_type.value}")
                
            except Exception as e:
                logger.error(f"Failed to initialize {provisioner_class.__name__}: {e}")
    
    async def provision_resources(
        self,
        request: ProvisioningRequest
    ) -> ProvisioningResult:
        """Provision infrastructure resources"""
        logger.info(f"Starting infrastructure provisioning for tenant {request.tenant_id}")
        
        # Generate request ID if not provided
        if not request.request_id:
            request.request_id = str(uuid.uuid4())
        
        # Create result object
        result = ProvisioningResult(
            request_id=request.request_id,
            status=ProvisioningStatus.IN_PROGRESS,
            started_at=datetime.utcnow()
        )
        
        # Save initial request
        await self.repository.create_request(request)
        
        try:
            # Determine resources to provision
            resources = request.resources if request.resources else self._get_default_resources()
            
            if self.settings.parallel_provisioning:
                await self._provision_parallel(request, result, resources)
            else:
                await self._provision_sequential(request, result, resources)
            
            # Update final status
            if result.failed_resources:
                result.status = ProvisioningStatus.PARTIALLY_COMPLETED
            else:
                result.status = ProvisioningStatus.COMPLETED
            
        except Exception as e:
            logger.error(f"Infrastructure provisioning failed: {e}")
            result.status = ProvisioningStatus.FAILED
            result.errors.append(str(e))
        
        finally:
            result.completed_at = datetime.utcnow()
            result.duration_seconds = (
                result.completed_at - result.started_at
            ).total_seconds()
            
            # Save final result
            await self.repository.update_request_status(
                request.request_id,
                result.status
            )
        
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
                logger.warning(f"No provisioner for {resource_type.value}")
                result.failed_resources.append({
                    "resource_type": resource_type.value,
                    "error": "No provisioner available"
                })
                continue
            
            provisioner = self.provisioners[resource_type]
            
            try:
                logger.info(f"Provisioning {resource_type.value} for tenant {request.tenant_id}")
                
                resource = await provisioner.provision(
                    tenant_id=request.tenant_id,
                    tenant_name=request.tenant_name,
                    metadata=request.metadata
                )
                
                result.provisioned_resources.append(resource)
                
                # Save to repository
                await self.repository.add_provisioned_resource(
                    request.request_id,
                    resource
                )
                
                logger.info(f"Successfully provisioned {resource_type.value}")
                
            except Exception as e:
                logger.error(f"Failed to provision {resource_type.value}: {e}")
                result.failed_resources.append({
                    "resource_type": resource_type.value,
                    "error": str(e)
                })
                
                # Rollback on failure if not parallel
                if not self.settings.parallel_provisioning:
                    await self._rollback_provisioning(request.tenant_id, result)
                    break
    
    async def _provision_parallel(
        self,
        request: ProvisioningRequest,
        result: ProvisioningResult,
        resources: List[ResourceType]
    ):
        """Provision resources in parallel"""
        tasks = []
        
        for resource_type in resources:
            if resource_type not in self.provisioners:
                logger.warning(f"No provisioner for {resource_type.value}")
                result.failed_resources.append({
                    "resource_type": resource_type.value,
                    "error": "No provisioner available"
                })
                continue
            
            provisioner = self.provisioners[resource_type]
            task = self._provision_resource_async(
                provisioner,
                resource_type,
                request
            )
            tasks.append(task)
        
        # Execute all provisioning tasks
        provisioning_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for idx, provision_result in enumerate(provisioning_results):
            if isinstance(provision_result, Exception):
                resource_type = resources[idx]
                logger.error(f"Failed to provision {resource_type.value}: {provision_result}")
                result.failed_resources.append({
                    "resource_type": resource_type.value,
                    "error": str(provision_result)
                })
            elif provision_result:
                result.provisioned_resources.append(provision_result)
                await self.repository.add_provisioned_resource(
                    request.request_id,
                    provision_result
                )
    
    async def _provision_resource_async(
        self,
        provisioner: IResourceProvisioner,
        resource_type: ResourceType,
        request: ProvisioningRequest
    ) -> InfrastructureResource:
        """Provision a single resource asynchronously"""
        logger.info(f"Provisioning {resource_type.value} for tenant {request.tenant_id}")
        
        resource = await provisioner.provision(
            tenant_id=request.tenant_id,
            tenant_name=request.tenant_name,
            metadata=request.metadata
        )
        
        logger.info(f"Successfully provisioned {resource_type.value}")
        return resource
    
    async def deprovision_resources(
        self,
        tenant_id: str,
        resources: Optional[List[ResourceType]] = None,
        force: bool = False
    ) -> ProvisioningResult:
        """Deprovision infrastructure resources"""
        logger.info(f"Starting infrastructure deprovisioning for tenant {tenant_id}")
        
        request_id = str(uuid.uuid4())
        result = ProvisioningResult(
            request_id=request_id,
            status=ProvisioningStatus.IN_PROGRESS,
            started_at=datetime.utcnow()
        )
        
        try:
            # Get tenant resources
            tenant_resources = await self.repository.get_tenant_resources(tenant_id)
            
            # Filter resources if specified
            if resources:
                tenant_resources = [
                    r for r in tenant_resources
                    if r.resource_type in resources
                ]
            
            # Deprovision each resource
            for resource in tenant_resources:
                if resource.resource_type not in self.provisioners:
                    logger.warning(f"No provisioner for {resource.resource_type.value}")
                    continue
                
                provisioner = self.provisioners[resource.resource_type]
                
                try:
                    logger.info(f"Deprovisioning {resource.resource_type.value}")
                    
                    success = await provisioner.deprovision(
                        tenant_id=tenant_id,
                        resource_name=resource.resource_name
                    )
                    
                    if success:
                        # Update resource status
                        resource.status = ResourceStatus.DELETED
                        await self.repository.update_resource_status(
                            resource.resource_id,
                            ResourceStatus.DELETED
                        )
                    else:
                        result.failed_resources.append({
                            "resource_type": resource.resource_type.value,
                            "error": "Deprovisioning failed"
                        })
                    
                except Exception as e:
                    logger.error(f"Failed to deprovision {resource.resource_type.value}: {e}")
                    result.failed_resources.append({
                        "resource_type": resource.resource_type.value,
                        "error": str(e)
                    })
                    
                    if not force:
                        raise
            
            # Update final status
            if result.failed_resources:
                result.status = ProvisioningStatus.PARTIALLY_COMPLETED
            else:
                result.status = ProvisioningStatus.COMPLETED
            
        except Exception as e:
            logger.error(f"Infrastructure deprovisioning failed: {e}")
            result.status = ProvisioningStatus.FAILED
            result.errors.append(str(e))
        
        finally:
            result.completed_at = datetime.utcnow()
            result.duration_seconds = (
                result.completed_at - result.started_at
            ).total_seconds()
        
        return result
    
    async def validate_resources(
        self,
        tenant_id: str,
        resources: Optional[List[ResourceType]] = None
    ) -> Dict[str, Any]:
        """Validate infrastructure resources"""
        logger.info(f"Validating infrastructure for tenant {tenant_id}")
        
        validation_results = {}
        tenant_resources = await self.repository.get_tenant_resources(tenant_id)
        
        # Filter resources if specified
        if resources:
            tenant_resources = [
                r for r in tenant_resources
                if r.resource_type in resources
            ]
        
        for resource in tenant_resources:
            if resource.resource_type not in self.provisioners:
                validation_results[resource.resource_type.value] = {
                    "valid": False,
                    "error": "No provisioner available"
                }
                continue
            
            provisioner = self.provisioners[resource.resource_type]
            
            try:
                is_valid = await provisioner.validate(tenant_id)
                validation_results[resource.resource_type.value] = {
                    "valid": is_valid,
                    "resource_id": resource.resource_id,
                    "status": resource.status.value
                }
                
            except Exception as e:
                logger.error(f"Validation failed for {resource.resource_type.value}: {e}")
                validation_results[resource.resource_type.value] = {
                    "valid": False,
                    "error": str(e)
                }
        
        return validation_results
    
    async def cleanup_orphaned_resources(
        self,
        tenant_id: Optional[str] = None,
        resource_types: Optional[List[ResourceType]] = None,
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """Cleanup orphaned infrastructure resources"""
        logger.info("Starting orphaned resource cleanup")
        
        orphaned = []
        cleaned = []
        errors = []
        
        # TODO: Implement orphaned resource detection
        # This would involve checking each infrastructure system
        # for resources that don't have corresponding entries in our DB
        
        return {
            "orphaned": orphaned,
            "cleaned": cleaned,
            "errors": errors
        }
    
    async def _rollback_provisioning(
        self,
        tenant_id: str,
        result: ProvisioningResult
    ):
        """Rollback provisioned resources on failure"""
        logger.info(f"Rolling back provisioning for tenant {tenant_id}")
        
        for resource in result.provisioned_resources:
            if resource.resource_type not in self.provisioners:
                continue
            
            provisioner = self.provisioners[resource.resource_type]
            
            try:
                await provisioner.deprovision(
                    tenant_id=tenant_id,
                    resource_name=resource.resource_name
                )
                logger.info(f"Rolled back {resource.resource_type.value}")
                
            except Exception as e:
                logger.error(f"Failed to rollback {resource.resource_type.value}: {e}")
        
        result.status = ProvisioningStatus.ROLLED_BACK
    
    def _get_default_resources(self) -> List[ResourceType]:
        """Get default resources to provision"""
        return [
            ResourceType.CASSANDRA,
            ResourceType.ELASTICSEARCH,
            ResourceType.IGNITE,
            ResourceType.MINIO,
            ResourceType.PULSAR,
            ResourceType.CONSUL,
            ResourceType.VAULT,
        ]
    
    def get_available_provisioners(self) -> List[IResourceProvisioner]:
        """Get list of available provisioners"""
        return list(self.provisioners.values()) 