import aiopulsar
import asyncio
import logging
import json
from ..core.config import settings
from platformq.events import IndexableEntityEvent
from ..services.indexer import Indexer

logger = logging.getLogger(__name__)

class SearchConsumer:
    def __init__(self, indexer: Indexer):
        self.client = None
        self.consumer = None
        self.indexer = indexer
        self.topic = "persistent://platformq/public/search-indexing"
        self.subscription_name = "search-service-subscription"

    async def start(self):
        """Connect to Pulsar and subscribe to the topic."""
        self.client = aiopulsar.Client(settings.PULSAR_URL)
        self.consumer = await self.client.subscribe(
            self.topic,
            self.subscription_name,
            consumer_type=aiopulsar.ConsumerType.Shared,
            schema=aiopulsar.schema.AvroSchema(IndexableEntityEvent)
        )
        logger.info("Search consumer started.")

    async def consume_messages(self):
        """Continuously consume messages from Pulsar."""
        if not self.consumer:
            await self.start()
            
        logger.info("Starting to consume search indexing events...")
        while True:
            try:
                msg = await self.consumer.receive()
                try:
                    event = msg.value()
                    logger.info(f"Received search event for entity: {event.entity_id}")
                    
                    await self.indexer.handle_event(event)
                        
                    await self.consumer.acknowledge(msg)
                except Exception as e:
                    logger.error(f"Failed to process search event: {e}")
                    await self.consumer.negative_acknowledge(msg)
            except Exception as e:
                logger.error(f"Error receiving search event from Pulsar: {e}")
                await asyncio.sleep(5)

    async def close(self):
        """Close the Pulsar client connection."""
        if self.consumer:
            await self.consumer.close()
        if self.client:
            await self.client.close()
        logger.info("Search consumer stopped.")
