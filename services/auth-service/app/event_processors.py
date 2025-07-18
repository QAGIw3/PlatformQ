"""
Event Processors for Auth Service

Handles auth-related events from the platform.
"""

import logging
from typing import Optional
from uuid import UUID

from platformq_shared import (
    EventProcessor,
    event_handler,
    ProcessingResult,
    ProcessingStatus
)
from platformq_events import (
    UserInvitedEvent,
    TenantProvisionedEvent,
    UserBlockchainLinkedEvent
)

from .repository import UserRepository, TenantRepository, InvitationRepository
from .schemas.user import UserCreate
from .schemas.tenant import TenantCreate
from .schemas.invitation import InvitationCreate

logger = logging.getLogger(__name__)


class AuthEventProcessor(EventProcessor):
    """Process authentication-related events"""
    
    def __init__(self, service_name: str, pulsar_url: str, db_session):
        super().__init__(service_name, pulsar_url)
        self.db_session = db_session
        
    async def on_start(self):
        """Initialize processor resources"""
        logger.info("Starting auth event processor")
        
    async def on_stop(self):
        """Cleanup processor resources"""
        logger.info("Stopping auth event processor")
        
    @event_handler("persistent://platformq/*/user-invited-events", UserInvitedEvent)
    async def handle_user_invited(self, event: UserInvitedEvent, msg):
        """Handle user invitation events from other services"""
        try:
            # Create invitation in database
            invitation_repo = InvitationRepository(self.db_session.get_session())
            
            invitation = InvitationCreate(
                email=event.email,
                role=event.role,
                tenant_id=event.tenant_id,
                invited_by=event.invited_by,
                expires_at=event.expires_at
            )
            
            created = invitation_repo.add(invitation)
            
            logger.info(f"Created invitation for {event.email} in tenant {event.tenant_id}")
            
            # Could publish invitation created event here if needed
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing user invitation: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/tenant-provisioned-events", TenantProvisionedEvent)
    async def handle_tenant_provisioned(self, event: TenantProvisionedEvent, msg):
        """Handle tenant provisioning completion"""
        try:
            # Update tenant status or add additional configuration
            tenant_repo = TenantRepository(self.db_session.get_session())
            
            tenant = tenant_repo.get(event.tenant_id)
            if tenant:
                # Update with provisioning details
                tenant.status = "active"
                tenant.provisioned_resources = event.resources
                tenant_repo.update(event.tenant_id, tenant)
                
                logger.info(f"Updated tenant {event.tenant_id} with provisioning details")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing tenant provisioned event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/user-blockchain-linked-events", UserBlockchainLinkedEvent)
    async def handle_blockchain_linked(self, event: UserBlockchainLinkedEvent, msg):
        """Handle user blockchain wallet linking"""
        try:
            user_repo = UserRepository(self.db_session.get_session())
            
            user = user_repo.get(event.user_id)
            if user:
                user.wallet_address = event.wallet_address
                user.did = event.did
                updated = user_repo.update(event.user_id, user)
                
                logger.info(f"Linked wallet {event.wallet_address} to user {event.user_id}")
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing blockchain link event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            ) 