import aiopulsar
import asyncio
import logging
import json

from ..core.config import settings
from ..repository import AssetRepository
from platformq.events import DatasetLineageEvent

logger = logging.getLogger(__name__)

class DataLakeAssetConsumer:
    def __init__(self, asset_repository: AssetRepository):
        self.client = None
        self.consumer = None
        self.asset_repository = asset_repository
        self.topic = "persistent://platformq/public/dataset-lineage"
        self.subscription_name = "digital-asset-service-subscription"

    async def start(self):
        """Connect to Pulsar and subscribe to the topic."""
        self.client = aiopulsar.Client(settings.PULSAR_URL)
        self.consumer = await self.client.subscribe(
            self.topic,
            self.subscription_name,
            consumer_type=aiopulsar.ConsumerType.Shared,
            schema=aiopulsar.schema.AvroSchema(DatasetLineageEvent)
        )
        logger.info("Data Lake asset consumer started.")

    async def consume_messages(self):
        """Continuously consume messages from Pulsar."""
        if not self.consumer:
            await self.start()
            
        logger.info("Starting to consume data lake lineage events for asset creation...")
        while True:
            try:
                msg = await self.consumer.receive()
                try:
                    event = msg.value()
                    logger.info(f"Received lineage event for dataset: {event.dataset_id}")
                    
                    if event.is_gold_layer:
                        logger.info(f"Gold layer dataset detected. Creating digital asset for: {event.dataset_name}")
                        await self.create_asset_from_event(event)
                        
                    await self.consumer.acknowledge(msg)
                except Exception as e:
                    logger.error(f"Failed to process lineage event: {e}")
                    await self.consumer.negative_acknowledge(msg)
            except Exception as e:
                logger.error(f"Error receiving lineage event from Pulsar: {e}")
                await asyncio.sleep(5)

    async def create_asset_from_event(self, event: DatasetLineageEvent):
        """Create a new digital asset from a DatasetLineageEvent."""
        asset_data = {
            "name": event.dataset_name,
            "description": f"Automated asset creation for Gold layer dataset: {event.dataset_name}",
            "type": "dataset",
            "tags": ["data-lake", "gold-layer", "automated"],
            "owner_id": event.triggered_by or "data-lake-service",
            "metadata": {
                "source_dataset_id": event.dataset_id,
                "data_lake_path": event.output_path,
                "schema": event.schema,
                "quality_report": event.quality_report,
                "source_datasets": event.source_datasets
            }
        }
        
        try:
            # The repository method needs to be async to be called from here
            new_asset = await self.asset_repository.create_asset(asset_data)
            logger.info(f"Successfully created digital asset {new_asset.id} for dataset {event.dataset_id}")
        except Exception as e:
            logger.error(f"Failed to create digital asset for dataset {event.dataset_id}: {e}")

    async def close(self):
        """Close the Pulsar client connection."""
        if self.consumer:
            await self.consumer.close()
        if self.client:
            await self.client.close()
        logger.info("Data Lake asset consumer stopped.") 