"""Event processor for Tenant Provisioning Service"""

import logging
from typing import Optional

from platformq_shared import (
    EventProcessor,
    event_handler,
    ProcessingResult,
    ProcessingStatus
)
from platformq_events import (
    TenantCreatedEvent,
    TenantDeletedEvent,
    TenantUpgradedEvent
)
from platformq_provisioning_common import (
    ProvisioningRequest,
    ProvisioningStatus as ProvStatus,
    TenantTier
)

from .orchestrator import ProvisioningOrchestrator

logger = logging.getLogger(__name__)


class TenantProvisioningProcessor(EventProcessor):
    """Process tenant lifecycle events"""
    
    def __init__(
        self,
        service_name: str,
        pulsar_url: str,
        orchestrator: ProvisioningOrchestrator
    ):
        super().__init__(service_name, pulsar_url)
        self.orchestrator = orchestrator
    
    async def on_start(self):
        """Initialize event processor"""
        logger.info("Starting tenant provisioning event processor")
    
    async def on_stop(self):
        """Cleanup event processor"""
        logger.info("Stopping tenant provisioning event processor")
    
    @event_handler("persistent://platformq/system/tenant-created-events", TenantCreatedEvent)
    async def handle_tenant_created(self, event: TenantCreatedEvent, msg):
        """Handle tenant creation events"""
        try:
            logger.info(f"Processing tenant created event for {event.tenant_id}")
            
            # Create provisioning request
            request = ProvisioningRequest(
                tenant_id=event.tenant_id,
                tenant_name=event.tenant_name,
                tier=TenantTier(event.tier.lower()),
                requested_by=event.created_by,
                metadata={
                    "event_id": event.event_id,
                    "organization": event.organization,
                    "admin_email": event.admin_email
                }
            )
            
            # Provision tenant resources
            result = await self.orchestrator.provision_tenant(request)
            
            if result.status == ProvStatus.COMPLETED:
                logger.info(f"Successfully provisioned resources for tenant {event.tenant_id}")
                return ProcessingResult(
                    status=ProcessingStatus.SUCCESS,
                    message=f"Provisioned {len(result.provisioned_resources)} resources"
                )
            elif result.status == ProvStatus.PARTIALLY_COMPLETED:
                logger.warning(f"Partially provisioned resources for tenant {event.tenant_id}")
                return ProcessingResult(
                    status=ProcessingStatus.PARTIAL,
                    message=f"Provisioned {len(result.provisioned_resources)} resources, "
                           f"{len(result.failed_resources)} failed"
                )
            else:
                logger.error(f"Failed to provision resources for tenant {event.tenant_id}")
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    message="Provisioning failed"
                )
                
        except Exception as e:
            logger.error(f"Error processing tenant created event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
    
    @event_handler("persistent://platformq/system/tenant-deleted-events", TenantDeletedEvent)
    async def handle_tenant_deleted(self, event: TenantDeletedEvent, msg):
        """Handle tenant deletion events"""
        try:
            logger.info(f"Processing tenant deleted event for {event.tenant_id}")
            
            # Deprovision tenant resources
            result = await self.orchestrator.deprovision_tenant(event.tenant_id)
            
            if result.status == ProvStatus.COMPLETED:
                logger.info(f"Successfully deprovisioned resources for tenant {event.tenant_id}")
                return ProcessingResult(
                    status=ProcessingStatus.SUCCESS,
                    message="All resources deprovisioned"
                )
            else:
                logger.warning(f"Partially deprovisioned resources for tenant {event.tenant_id}")
                return ProcessingResult(
                    status=ProcessingStatus.PARTIAL,
                    message=f"{len(result.failed_resources)} resources failed to deprovision"
                )
                
        except Exception as e:
            logger.error(f"Error processing tenant deleted event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
    
    @event_handler("persistent://platformq/system/tenant-upgraded-events", TenantUpgradedEvent)
    async def handle_tenant_upgraded(self, event: TenantUpgradedEvent, msg):
        """Handle tenant tier upgrade events"""
        try:
            logger.info(f"Processing tenant upgraded event for {event.tenant_id}: "
                       f"{event.old_tier} -> {event.new_tier}")
            
            # For upgrades, we might need to provision additional resources
            # or adjust quotas based on the new tier
            
            # Create a provisioning request for additional resources
            request = ProvisioningRequest(
                tenant_id=event.tenant_id,
                tenant_name=event.tenant_name,
                tier=TenantTier(event.new_tier.lower()),
                requested_by=event.upgraded_by,
                metadata={
                    "event_id": event.event_id,
                    "upgrade": True,
                    "old_tier": event.old_tier,
                    "new_tier": event.new_tier
                }
            )
            
            # For now, just log the upgrade
            # In a real implementation, this would adjust resources based on tier
            logger.info(f"Tenant {event.tenant_id} upgraded from {event.old_tier} to {event.new_tier}")
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                message="Tenant upgrade processed"
            )
            
        except Exception as e:
            logger.error(f"Error processing tenant upgraded event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            ) 