"""
Event Processors for {{ cookiecutter.service_name }}

Handles events using the new event processing framework.
"""

import logging
from typing import Optional, List
from datetime import datetime
import uuid

from platformq_shared import (
    EventProcessor,
    event_handler,
    batch_event_handler,
    ProcessingResult,
    ProcessingStatus
)
from platformq_events import (
    # Import relevant events for your service
    GenericEvent,
    UserCreatedEvent,
    TenantCreatedEvent
)

{% if cookiecutter.database_type != "none" %}
from .repository import {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Repository
{% endif %}
from .schemas import (
    {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Create
)

logger = logging.getLogger(__name__)


class {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}EventProcessor(EventProcessor):
    """Process {{ cookiecutter.service_name }} related events"""
    
    def __init__(self, service_name: str, pulsar_url: str{% if cookiecutter.database_type != "none" %}, repository: {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}Repository{% endif %}):
        super().__init__(service_name, pulsar_url)
        {% if cookiecutter.database_type != "none" %}self.repository = repository{% endif %}
        
    async def on_start(self):
        """Initialize processor resources"""
        logger.info("Starting {{ cookiecutter.service_name }} event processor")
        # Add any initialization logic here
        
    async def on_stop(self):
        """Cleanup processor resources"""
        logger.info("Stopping {{ cookiecutter.service_name }} event processor")
        # Add any cleanup logic here
        
    @event_handler("persistent://platformq/*/user-created-events", UserCreatedEvent)
    async def handle_user_created(self, event: UserCreatedEvent, msg):
        """Handle user creation events"""
        try:
            logger.info(f"Processing user created event for user {event.user_id}")
            
            # Add your business logic here
            # Example: Create default settings for the user
            {% if cookiecutter.database_type != "none" %}
            # self.repository.create_user_defaults(
            #     user_id=event.user_id,
            #     tenant_id=event.tenant_id
            # )
            {% endif %}
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing user created event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/tenant-created-events", TenantCreatedEvent)
    async def handle_tenant_created(self, event: TenantCreatedEvent, msg):
        """Handle tenant creation events"""
        try:
            logger.info(f"Processing tenant created event for tenant {event.tenant_id}")
            
            # Add your business logic here
            # Example: Initialize tenant-specific resources
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing tenant created event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                message=str(e)
            )
            
    @batch_event_handler(
        "persistent://platformq/*/generic-batch-events",
        GenericEvent,
        max_batch_size=100,
        max_wait_time_ms=5000
    )
    async def handle_batch_events(self, events: List[GenericEvent], msgs):
        """Handle events in batches for efficiency"""
        try:
            logger.info(f"Processing batch of {len(events)} events")
            
            # Process events in batch
            # This is more efficient for high-volume events
            
            successful_count = 0
            for event in events:
                try:
                    # Process individual event
                    # Add your logic here
                    successful_count += 1
                except Exception as e:
                    logger.error(f"Failed to process event {event.id}: {e}")
                    
            logger.info(f"Successfully processed {successful_count}/{len(events)} events")
            
            # Return success if at least some events were processed
            if successful_count > 0:
                return ProcessingResult(status=ProcessingStatus.SUCCESS)
            else:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    message="No events were successfully processed"
                )
                
        except Exception as e:
            logger.error(f"Error processing batch events: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )


# Add more event processors as needed
class {{ cookiecutter.service_name.replace('-', '_').title().replace('_', '') }}AsyncProcessor(EventProcessor):
    """Process long-running or async tasks"""
    
    def __init__(self, service_name: str, pulsar_url: str):
        super().__init__(service_name, pulsar_url)
        
    @event_handler(
        "persistent://platformq/*/{{ cookiecutter.service_slug }}-async-tasks",
        GenericEvent,
        processing_timeout_ms=300000  # 5 minutes for long tasks
    )
    async def handle_async_task(self, event: GenericEvent, msg):
        """Handle long-running async tasks"""
        try:
            logger.info(f"Starting async task {event.id}")
            
            # Perform long-running operation
            # Example: ML model training, large data processing, etc.
            
            # You can update progress periodically
            await msg.update_progress(0.25, "Processing phase 1")
            # ... do work ...
            await msg.update_progress(0.50, "Processing phase 2")
            # ... do work ...
            await msg.update_progress(0.75, "Processing phase 3")
            # ... do work ...
            
            logger.info(f"Completed async task {event.id}")
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                result_data={"task_id": event.id, "status": "completed"}
            )
            
        except Exception as e:
            logger.error(f"Error in async task: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                message=str(e)
            ) 