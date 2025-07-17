import aiopulsar
import asyncio
import logging
import json

from ..core.config import settings
from ..services.trust_score import trust_score_service

logger = logging.getLogger(__name__)

class ActivityConsumer:
    def __init__(self):
        self.client = None
        self.consumer = None
        self.topics = [
            "persistent://public/default/project-milestone-completed",
            "persistent://public/default/asset-peer-reviewed"
        ]
        self.subscription_name = "graph-intelligence-service-subscription"

    async def start(self):
        """Connect to Pulsar and subscribe to topics."""
        self.client = aiopulsar.Client(settings.PULSAR_URL)
        self.consumer = await self.client.subscribe(
            self.topics,
            self.subscription_name,
            consumer_type=aiopulsar.ConsumerType.Shared
        )
        logger.info("Activity consumer started and subscribed to topics.")

    async def consume_messages(self):
        """Continuously consume messages from Pulsar."""
        if not self.consumer:
            await self.start()
            
        logger.info("Starting to consume user activity events...")
        while True:
            try:
                msg = await self.consumer.receive()
                try:
                    event_data = json.loads(msg.data().decode('utf-8'))
                    logger.info(f"Received activity event: {event_data.get('type')}")
                    
                    activity_type = event_data.get("type")
                    user_id = event_data.get("userId") or event_data.get("reviewerId")
                    activity_id = event_data.get("milestoneId") or event_data.get("assetId")
                    
                    if activity_type and user_id and activity_id:
                        # Assuming trust_score_service is synchronous. If it were async, we'd await it.
                        trust_score_service.add_user_activity(
                            user_id=user_id,
                            activity_type=activity_type,
                            activity_id=activity_id
                        )
                        
                    await self.consumer.acknowledge(msg)
                except Exception as e:
                    logger.error(f"Failed to process activity message: {e}")
                    await self.consumer.negative_acknowledge(msg)
            except Exception as e:
                logger.error(f"Error receiving activity message from Pulsar: {e}")
                await asyncio.sleep(5) # Wait before retrying connection/receive

    async def close(self):
        """Close the Pulsar client connection."""
        if self.consumer:
            await self.consumer.close()
        if self.client:
            await self.client.close()
        logger.info("Activity consumer stopped.") 