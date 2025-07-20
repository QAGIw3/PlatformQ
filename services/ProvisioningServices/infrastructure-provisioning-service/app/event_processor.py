"""
Infrastructure Event Processor

Handles events related to infrastructure provisioning.
"""
import asyncio
import logging
from typing import Dict, Any, List

from event_processor import EventProcessor, event_handler

from platformq_resource_common import (
    TenantCreatedEvent, TenantDeletedEvent, TenantUpgradedEvent,
    ResourceType, ProvisioningRequest
)

from .orchestrator import InfrastructureOrchestrator
from .repository import InfrastructureRepository

logger = logging.getLogger(__name__)


class InfrastructureEventProcessor(EventProcessor):
    """Processes infrastructure-related events"""
    
    def __init__(
        self,
        service_name: str,
        pulsar_url: str,
        orchestrator: InfrastructureOrchestrator,
        repository: InfrastructureRepository
    ):
        super().__init__(service_name, pulsar_url)
        self.orchestrator = orchestrator
        self.repository = repository
    
    async def on_start(self):
        """Called when event processor starts"""
        logger.info("Infrastructure event processor started")
    
    async def on_stop(self):
        """Called when event processor stops"""
        logger.info("Infrastructure event processor stopped")
    
    @event_handler("persistent://platformq/system/tenant-created-events", TenantCreatedEvent)
    async def handle_tenant_created(self, event: TenantCreatedEvent, msg):
        """Handle tenant creation events"""
        logger.info(f"Handling tenant created event for tenant {event.tenant_id}")
        
        try:
            # Create provisioning request
            request = ProvisioningRequest(
                tenant_id=event.tenant_id,
                tenant_name=event.tenant_name,
                tier=event.tier,
                resources=self._get_resources_for_tier(event.tier),
                metadata={
                    "source": "tenant_created_event",
                    "tier": event.tier.value,
                    "initial_quotas": event.initial_quotas
                },
                requested_by="system"
            )
            
            # Provision infrastructure
            result = await self.orchestrator.provision_resources(request)
            
            if result.status.value in ["completed", "partially_completed"]:
                logger.info(f"Successfully provisioned infrastructure for tenant {event.tenant_id}")
                await msg.ack()
            else:
                logger.error(f"Failed to provision infrastructure for tenant {event.tenant_id}")
                # Negative ack to retry
                await msg.nack()
                
        except Exception as e:
            logger.error(f"Error handling tenant created event: {e}")
            await msg.nack()
    
    @event_handler("persistent://platformq/system/tenant-deleted-events", TenantDeletedEvent)
    async def handle_tenant_deleted(self, event: TenantDeletedEvent, msg):
        """Handle tenant deletion events"""
        logger.info(f"Handling tenant deleted event for tenant {event.tenant_id}")
        
        try:
            # Deprovision all infrastructure
            result = await self.orchestrator.deprovision_resources(
                tenant_id=event.tenant_id,
                force=True  # Force deletion even if resources are in use
            )
            
            if result.status.value in ["completed", "partially_completed"]:
                logger.info(f"Successfully deprovisioned infrastructure for tenant {event.tenant_id}")
                await msg.ack()
            else:
                logger.error(f"Failed to deprovision infrastructure for tenant {event.tenant_id}")
                # For deletion, we might want to ack anyway to avoid blocking
                await msg.ack()
                
        except Exception as e:
            logger.error(f"Error handling tenant deleted event: {e}")
            await msg.ack()  # Ack to avoid blocking deletion
    
    @event_handler("persistent://platformq/system/tenant-upgraded-events", TenantUpgradedEvent)
    async def handle_tenant_upgraded(self, event: TenantUpgradedEvent, msg):
        """Handle tenant upgrade events"""
        logger.info(f"Handling tenant upgraded event for tenant {event.tenant_id}: {event.old_tier} -> {event.new_tier}")
        
        try:
            # Determine resources to add/modify based on tier change
            new_resources = self._get_resources_for_tier(event.new_tier)
            current_resources = await self.repository.get_tenant_resources(event.tenant_id)
            current_types = {r.resource_type for r in current_resources}
            
            # Find resources to provision
            resources_to_add = [r for r in new_resources if r not in current_types]
            
            if resources_to_add:
                # Create provisioning request for new resources
                request = ProvisioningRequest(
                    tenant_id=event.tenant_id,
                    tenant_name=f"tenant-{event.tenant_id}",  # Default name
                    tier=event.new_tier,
                    resources=resources_to_add,
                    metadata={
                        "source": "tenant_upgraded_event",
                        "old_tier": event.old_tier.value,
                        "new_tier": event.new_tier.value
                    },
                    requested_by=event.upgraded_by
                )
                
                # Provision additional infrastructure
                result = await self.orchestrator.provision_resources(request)
                
                if result.status.value in ["completed", "partially_completed"]:
                    logger.info(f"Successfully provisioned additional infrastructure for tenant {event.tenant_id}")
                else:
                    logger.error(f"Failed to provision additional infrastructure for tenant {event.tenant_id}")
            
            # TODO: Handle resource scaling/modification for tier changes
            
            await msg.ack()
            
        except Exception as e:
            logger.error(f"Error handling tenant upgraded event: {e}")
            await msg.nack()
    
    def _get_resources_for_tier(self, tier) -> List[ResourceType]:
        """Get infrastructure resources based on tenant tier"""
        # Base resources for all tiers
        base_resources = [
            ResourceType.CASSANDRA,
            ResourceType.MINIO,
            ResourceType.CONSUL,
        ]
        
        # Additional resources by tier
        tier_resources = {
            "free": [],
            "starter": [
                ResourceType.PULSAR,
                ResourceType.IGNITE,
            ],
            "professional": [
                ResourceType.PULSAR,
                ResourceType.IGNITE,
                ResourceType.ELASTICSEARCH,
                ResourceType.VAULT,
            ],
            "enterprise": [
                ResourceType.PULSAR,
                ResourceType.IGNITE,
                ResourceType.ELASTICSEARCH,
                ResourceType.VAULT,
                ResourceType.JANUSGRAPH,
            ],
            "custom": [
                ResourceType.PULSAR,
                ResourceType.IGNITE,
                ResourceType.ELASTICSEARCH,
                ResourceType.VAULT,
                ResourceType.JANUSGRAPH,
            ]
        }
        
        tier_value = tier.value if hasattr(tier, 'value') else str(tier).lower()
        return base_resources + tier_resources.get(tier_value, [])