import logging
import json
import asyncio
from pulsar import Client, ConsumerType
from .druid_analytics import DruidIngestionService

logger = logging.getLogger(__name__)

class SimulationAnalyticsConsumer:
    def __init__(self, pulsar_client: Client, druid_ingestion_service: DruidIngestionService):
        self.pulsar_client = pulsar_client
        self.druid_ingestion_service = druid_ingestion_service
        self.consumer = None
        self.running = False

    async def start(self):
        """Start the consumer"""
        self.consumer = self.pulsar_client.subscribe(
            topic="persistent://public/default/simulation-metrics",
            subscription_name="realtime-analytics-simulation-subscription",
            consumer_type=ConsumerType.Shared
        )
        self.running = True
        asyncio.create_task(self._consume())
        logger.info("Simulation analytics consumer started.")

    async def _consume(self):
        """Consume simulation metrics messages"""
        while self.running:
            try:
                msg = self.consumer.receive(timeout_millis=1000)
                if msg is None:
                    await asyncio.sleep(1)
                    continue

                event_data = json.loads(msg.data().decode('utf-8'))
                logger.info(f"Received simulation metrics: {event_data}")

                session_id = event_data.get("session_id")
                metrics = json.loads(event_data.get("metrics", "{}"))

                if session_id and metrics:
                    await self.druid_ingestion_service.ingest_simulation_metrics(session_id, metrics)

                self.consumer.acknowledge(msg)
            except Exception as e:
                logger.error(f"Error in simulation analytics consumer: {e}")
                if 'msg' in locals() and msg:
                    self.consumer.negative_acknowledge(msg)
                await asyncio.sleep(5)

    def stop(self):
        """Stop the consumer"""
        self.running = False
        if self.consumer:
            self.consumer.close()
        logger.info("Simulation analytics consumer stopped.") 