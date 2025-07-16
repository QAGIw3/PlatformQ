import pulsar
import json
import threading
from ..services.trust_score import trust_score_service

# TODO: Move to config
PULSAR_SERVICE_URL = "pulsar://localhost:6650"
TOPICS = [
    "persistent://public/default/project-milestone-completed",
    "persistent://public/default/asset-peer-reviewed"
]
SUBSCRIPTION_NAME = "graph-intelligence-service-subscription"

class PulsarConsumer:
    def __init__(self):
        self.client = pulsar.Client(PULSAR_SERVICE_URL)
        self.consumer = self.client.subscribe(
            TOPICS,
            subscription_name=SUBSCRIPTION_NAME,
            consumer_type=pulsar.ConsumerType.Shared
        )
        self.running = False

    def start(self):
        self.running = True
        thread = threading.Thread(target=self._consume)
        thread.daemon = True
        thread.start()
        print("Pulsar consumer started...")

    def _consume(self):
        while self.running:
            try:
                msg = self.consumer.receive()
                try:
                    event_data = json.loads(msg.data().decode('utf-8'))
                    print(f"Received event: {event_data}")
                    
                    activity_type = event_data.get("type")
                    user_id = event_data.get("userId") or event_data.get("reviewerId")
                    activity_id = event_data.get("milestoneId") or event_data.get("assetId")
                    
                    if activity_type and user_id and activity_id:
                        trust_score_service.add_user_activity(
                            user_id=user_id,
                            activity_type=activity_type,
                            activity_id=activity_id
                        )

                    self.consumer.acknowledge(msg)
                except Exception as e:
                    print(f"Failed to process message: {e}")
                    self.consumer.negative_acknowledge(msg)
            except Exception as e:
                print(f"Error receiving message from Pulsar: {e}")
                if not self.running:
                    break
    
    def stop(self):
        self.running = False
        self.client.close()
        print("Pulsar consumer stopped.")

pulsar_consumer = PulsarConsumer()

def start_consumer():
    pulsar_consumer.start()

def stop_consumer():
    pulsar_consumer.stop() 