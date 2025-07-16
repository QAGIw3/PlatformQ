import pulsar
import json
import threading
import asyncio
from ..main import handle_trust_event

# TODO: Move to config
PULSAR_SERVICE_URL = "pulsar://localhost:6650"
TRUST_ENGINE_TOPIC = "persistent://public/default/trust-engine-events"
SUBSCRIPTION_NAME = "verifiable-credential-service-subscription"

class PulsarConsumer:
    def __init__(self, app):
        self.app = app
        self.client = pulsar.Client(PULSAR_SERVICE_URL)
        self.consumer = self.client.subscribe(
            TRUST_ENGINE_TOPIC,
            subscription_name=SUBSCRIPTION_NAME,
            consumer_type=pulsar.ConsumerType.Shared
        )
        self.running = False

    def start(self):
        self.running = True
        thread = threading.Thread(target=self._consume)
        thread.daemon = True
        thread.start()
        print("Pulsar consumer for trust events started...")

    def _consume(self):
        while self.running:
            try:
                msg = self.consumer.receive()
                try:
                    event_data = json.loads(msg.data().decode('utf-8'))
                    print(f"Received trust event: {event_data}")
                    
                    # Determine event type based on payload structure
                    event_type = None
                    if "proposal_id" in event_data:
                        event_type = "DAOProposalApproved"
                    elif "milestone_id" in event_data:
                        event_type = "ProjectMilestoneCompleted"
                    elif "review_id" in event_data:
                        event_type = "AssetPeerReviewed"

                    if event_type:
                        # Since handle_trust_event is async, run it in an event loop
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        loop.run_until_complete(
                            handle_trust_event(
                                event_type=event_type,
                                event_data=event_data,
                                publisher=self.app.state.event_publisher
                            )
                        )

                    self.consumer.acknowledge(msg)
                except Exception as e:
                    print(f"Failed to process message: {e}")
                    self.consumer.negative_acknowledge(msg)
            except Exception as e:
                print(f"Error receiving message from Pulsar: {e}")
                # In a real app, you might want a backoff and retry mechanism
                if not self.running:
                    break
    
    def stop(self):
        self.running = False
        self.client.close()
        print("Pulsar consumer stopped.")

# This needs to be initialized with the app object now
# pulsar_consumer = PulsarConsumer(app)
# I will update main.py to handle this.

# In a real app, you would manage the lifecycle of the consumer
# with the application's lifecycle.
# For example, in a FastAPI app, you'd use startup and shutdown events.
def start_consumer():
    # This function will need to be updated to pass the app object
    # pulsar_consumer.start()
    print("start_consumer function is not yet updated to pass app object.")

def stop_consumer():
    # This function will need to be updated to pass the app object
    # pulsar_consumer.stop()
    print("stop_consumer function is not yet updated to pass app object.")

# This is a simple way to run the consumer for demonstration.
# A more robust solution would be needed for a production environment.
# start_consumer()
# atexit.register(stop_consumer) 