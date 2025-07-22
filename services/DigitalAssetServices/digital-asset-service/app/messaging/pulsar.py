import pulsar
import json

# TODO: Move to config
PULSAR_SERVICE_URL = "pulsar://localhost:6650"
TRUST_ENGINE_TOPIC = "persistent://public/default/trust-engine-events"

class PulsarService:
    def __init__(self):
        try:
            self.client = pulsar.Client(PULSAR_SERVICE_URL)
            self.producer = self.client.create_producer(TRUST_ENGINE_TOPIC)
        except Exception as e:
            # Handle connection errors, e.g., log them
            # In a real app, you might want a more robust retry mechanism
            print(f"Failed to connect to Pulsar: {e}")
            self.client = None
            self.producer = None

    def publish_event(self, event_data: dict):
        if self.producer:
            self.producer.send(json.dumps(event_data).encode('utf-8'))
            print(f"Published event: {event_data}")

    def close(self):
        if self.client:
            self.client.close()

pulsar_service = PulsarService()

# Ensure the client is closed when the application shuts down
# In a FastAPI app, you would use a shutdown event handler.
# For this example, we'll rely on the process exit.
import atexit
atexit.register(pulsar_service.close) 