import logging
import json
import asyncio
from pulsar import Client, ConsumerType
from .collaboration_engine import CollaborationEngine

logger = logging.getLogger(__name__)

class SimulationConsumer:
    def __init__(self, pulsar_client: Client, collaboration_engine: CollaborationEngine):
        self.pulsar_client = pulsar_client
        self.collaboration_engine = collaboration_engine
        self.consumer = None
        self.running = False

    async def start(self):
        """Start the consumer"""
        self.consumer = self.pulsar_client.subscribe(
            topic="persistent://public/default/simulation-operations",
            subscription_name="cad-collaboration-simulation-subscription",
            consumer_type=ConsumerType.Shared
        )
        self.running = True
        asyncio.create_task(self._consume())
        logger.info("Simulation operations consumer started.")

    async def _consume(self):
        """Consume simulation operation messages"""
        while self.running:
            try:
                msg = self.consumer.receive(timeout_millis=1000)
                if msg is None:
                    await asyncio.sleep(1)
                    continue

                event_data = json.loads(msg.data().decode('utf-8'))
                logger.info(f"Received simulation operation: {event_data}")

                session_id = event_data.get("session_id")
                operation_data = {
                    "type": event_data.get("operation_type"),
                    "data": json.loads(event_data.get("data", "{}")),
                    "id": event_data.get("operation_id")
                }

                if session_id:
                    await self.collaboration_engine.apply_remote_operation(session_id, operation_data)

                self.consumer.acknowledge(msg)
            except Exception as e:
                logger.error(f"Error in simulation consumer: {e}")
                if 'msg' in locals() and msg:
                    self.consumer.negative_acknowledge(msg)
                await asyncio.sleep(5)

    def stop(self):
        """Stop the consumer"""
        self.running = False
        if self.consumer:
            self.consumer.close()
        logger.info("Simulation operations consumer stopped.") 