import pulsar
import logging
from pulsar.schema import AvroSchema
from platformq.events import DatasetLineageEvent
from ...core.config import settings

logger = logging.getLogger(__name__)

class DataLakeConsumer:
    def __init__(self, graph_processor):
        self.client = pulsar.Client(settings.PULSAR_URL)
        self.consumer = self.client.subscribe(
            topic="persistent://platformq/public/dataset-lineage",
            subscription_name="graph-intelligence-subscription",
            schema=AvroSchema(DatasetLineageEvent)
        )
        self.graph_processor = graph_processor

    async def consume_messages(self):
        logger.info("Starting to consume data lake lineage events...")
        while True:
            try:
                msg = self.consumer.receive()
                try:
                    event = msg.value()
                    logger.info(f"Received lineage event: {event.dataset_id}")
                    await self.graph_processor.process_lineage_event(event)
                    self.consumer.acknowledge(msg)
                except Exception as e:
                    logger.error(f"Failed to process message: {e}")
                    self.consumer.negative_acknowledge(msg)
            except Exception as e:
                logger.error(f"Error receiving message from Pulsar: {e}")

    def close(self):
        self.client.close() 