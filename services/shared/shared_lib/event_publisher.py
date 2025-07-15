import io
import logging

import avro.io
import avro.schema
import pulsar

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

    def get_producer(self, topic: str):
        if topic not in self.producers:
            self.producers[topic] = self.client.create_producer(topic)
        return self.producers[topic]

    def publish(self, topic_base: str, tenant_id: str, schema_path: str, data: dict):
        if not self.client:
            raise RuntimeError("Pulsar client is not connected.")

        # Construct tenant-specific topic name
        # e.g., persistent://platformq/tenant-1234/user-events
        topic = f"persistent://platformq/{tenant_id}/{topic_base}"

        try:
            # Assuming schema files are accessible from the execution path
            schema = avro.schema.parse(open(schema_path).read())
            writer = avro.io.DatumWriter(schema)
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(data, encoder)

            message = bytes_writer.getvalue()
            producer = self.get_producer(topic)
            producer.send(message)
            logger.info(f"Published event to tenant topic '{topic}'")
        except Exception as e:
            logger.error(f"Failed to publish event to topic '{topic}': {e}")
