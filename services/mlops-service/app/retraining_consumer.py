import logging
import json
import asyncio
from pulsar import Client, ConsumerType
from .auto_retrainer import AutomatedRetrainer, RetrainingTrigger

logger = logging.getLogger(__name__)

class RetrainingConsumer:
    def __init__(self, pulsar_client: Client, retrainer: AutomatedRetrainer):
        self.pulsar_client = pulsar_client
        self.retrainer = retrainer
        self.consumer = None
        self.running = False

    async def start(self):
        """Start the consumer"""
        self.consumer = self.pulsar_client.subscribe(
            topic="persistent://public/default/model-retraining-requests",
            subscription_name="mlops-retraining-subscription",
            consumer_type=ConsumerType.Shared
        )
        self.running = True
        asyncio.create_task(self._consume())
        logger.info("Retraining consumer started.")

    async def _consume(self):
        """Consume retraining messages"""
        while self.running:
            try:
                msg = self.consumer.receive(timeout_millis=1000)
                if msg is None:
                    await asyncio.sleep(1)
                    continue

                event_data = json.loads(msg.data().decode('utf-8'))
                logger.info(f"Received retraining request: {event_data}")

                reason = event_data.get("reason")
                trigger = None
                if reason == "DATA_DRIFT_DETECTED":
                    trigger = RetrainingTrigger.DRIFT_DETECTED
                elif reason == "PERFORMANCE_DEGRADATION":
                    trigger = RetrainingTrigger.PERFORMANCE_DEGRADATION
                
                if trigger:
                    await self.retrainer.trigger_retraining(
                        model_name=event_data["model_name"],
                        version=event_data["model_version"],
                        tenant_id=event_data["tenant_id"],
                        trigger=trigger,
                        trigger_metrics=event_data
                    )

                self.consumer.acknowledge(msg)
            except Exception as e:
                logger.error(f"Error in retraining consumer: {e}")
                if 'msg' in locals() and msg:
                    self.consumer.negative_acknowledge(msg)
                await asyncio.sleep(5)

    def stop(self):
        """Stop the consumer"""
        self.running = False
        if self.consumer:
            self.consumer.close()
        logger.info("Retraining consumer stopped.") 