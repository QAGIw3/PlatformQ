import aiopulsar
import asyncio
import logging
import json

from ..core.config import settings
from ..services.graph_service import graph_service # Placeholder for our new service

logger = logging.getLogger(__name__)

class DigitalAssetConsumer:
    def __init__(self):
        self.client = None
        self.consumer = None
        # The topic name is based on the publisher in digital-asset-service
        self.topic = "persistent://public/default/digital-asset-created-events-*" 
        self.subscription_name = "graph-intelligence-asset-subscription"

    async def start(self):
        """Connect to Pulsar and subscribe to the asset creation topic."""
        self.client = aiopulsar.Client(settings.PULSAR_URL)
        self.consumer = await self.client.subscribe(
            self.topic,
            self.subscription_name,
            consumer_type=aiopulsar.ConsumerType.Shared,
            topic_pattern=True # Important for wildcard topic matching
        )
        logger.info("Digital asset consumer started and subscribed to topic pattern.")

    async def consume_messages(self):
        """Continuously consume messages from Pulsar."""
        if not self.consumer:
            await self.start()
            
        logger.info("Starting to consume digital asset creation events...")
        while True:
            try:
                msg = await self.consumer.receive()
                try:
                    # The event schema is defined in platformq_shared, but we can decode it as JSON
                    # as Pulsar's Python client can serialize Avro to JSON automatically.
                    event_data = json.loads(msg.data().decode('utf-8'))
                    logger.info(f"Received DigitalAssetCreated event for asset: {event_data.get('asset_id')}")
                    
                    asset_id = event_data.get("asset_id")
                    asset_name = event_data.get("asset_name")
                    asset_type = event_data.get("asset_type")
                    owner_id = event_data.get("owner_id")

                    if asset_id and owner_id and asset_type:
                        # Call the graph service to update the graph
                        await graph_service.add_asset_to_graph(
                            asset_id=asset_id,
                            asset_name=asset_name,
                            asset_type=asset_type,
                            owner_id=owner_id,
                        )
                        
                    await self.consumer.acknowledge(msg)
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode message: {msg.data()}")
                    await self.consumer.acknowledge(msg) # Acknowledge to avoid reprocessing a bad message
                except Exception as e:
                    logger.error(f"Failed to process digital asset message: {e}")
                    await self.consumer.negative_acknowledge(msg)
            except Exception as e:
                logger.error(f"Error receiving digital asset message from Pulsar: {e}")
                await asyncio.sleep(5) # Wait before retrying

    async def close(self):
        """Close the Pulsar client connection."""
        if self.consumer:
            await self.consumer.close()
        if self.client:
            await self.client.close()
        logger.info("Digital asset consumer stopped.") 