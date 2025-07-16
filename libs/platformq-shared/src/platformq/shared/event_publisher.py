import pulsar
from pulsar.schema import *
import logging

logger = logging.getLogger(__name__)


class EventPublisher:
    def __init__(self, pulsar_url: str):
        self.pulsar_url = pulsar_url
        self.client = None
        self.producers = {}

    def connect(self):
        try:
            self.client = pulsar.Client(self.pulsar_url)
            logger.info(f"Successfully connected to Pulsar at {self.pulsar_url}.")
        except Exception as e:
            logger.error(f"Failed to connect to Pulsar: {e}")
            raise

    def close(self):
        if self.client:
            self.client.close()
            logger.info("Pulsar client connection closed.")

    def get_producer(self, topic: str, schema: Record):
        if topic not in self.producers:
            self.producers[topic] = self.client.create_producer(topic, schema=AvroSchema(schema))
        return self.producers[topic]

    def publish(self, topic_base: str, tenant_id: str, schema_class: Record, data: object):
        if not self.client:
            raise RuntimeError("Pulsar client is not connected.")

        topic = f"persistent://platformq/{tenant_id}/{topic_base}"

        try:
            producer = self.get_producer(topic, schema_class)
            producer.send(data)
            logger.info(f"Published Avro event to tenant topic '{topic}'")
        except Exception as e:
            logger.error(f"Failed to publish event to topic '{topic}': {e}")
