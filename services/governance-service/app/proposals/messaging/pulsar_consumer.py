import pulsar
import json
import threading
import uuid
import time

from ..crud.proposal import proposal as crud_proposal
from ..schemas.proposal import Proposal

# TODO: Move to config
PULSAR_SERVICE_URL = "pulsar://localhost:6650"
GOVERNANCE_TOPIC = "persistent://public/default/onchain-governance-events"
TRUST_ENGINE_TOPIC = "persistent://public/default/trust-engine-events"
SUBSCRIPTION_NAME = "proposals-service-subscription"


class PulsarConsumer:
    def __init__(self):
        self.client = pulsar.Client(PULSAR_SERVICE_URL)
        self.consumer = self.client.subscribe(
            GOVERNANCE_TOPIC,
            subscription_name=SUBSCRIPTION_NAME,
            consumer_type=pulsar.ConsumerType.Shared
        )
        self.producer = self.client.create_producer(TRUST_ENGINE_TOPIC)
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
                    print(f"Received governance event: {event_data}")
                    event_type = event_data.get("eventType")

                    if event_type == "ProposalCreated":
                        crud_proposal.update_status_from_event(
                            onchain_proposal_id=event_data["proposalId"],
                            new_status="on-chain-pending"
                        )
                    elif event_type == "ProposalExecuted":
                        self._handle_proposal_executed(event_data)
                    elif event_type in ["ProposalCanceled", "ProposalDefeated"]:
                         crud_proposal.update_status_from_event(
                            onchain_proposal_id=event_data["proposalId"],
                            new_status=event_type.replace("Proposal", "").lower() # "canceled" or "defeated"
                        )

                    self.consumer.acknowledge(msg)
                except Exception as e:
                    print(f"Failed to process message: {e}")
                    self.consumer.negative_acknowledge(msg)
            except Exception as e:
                print(f"Error receiving message from Pulsar: {e}")
                if not self.running:
                    break
    
    def _handle_proposal_executed(self, event_data: dict):
        onchain_proposal_id = event_data["proposalId"]
        proposal = crud_proposal.update_status_from_event(
            onchain_proposal_id=onchain_proposal_id,
            new_status="executed"
        )
        if not proposal:
            print(f"Could not find proposal with onchain ID {onchain_proposal_id} to handle execution.")
            return

        voters = crud_proposal.get_voters(proposal.id)
        
        trust_event = {
            "event_id": str(uuid.uuid4()),
            "proposal_id": str(proposal.id),
            "proposer_id": proposal.proposer,
            "voter_ids": voters,
            "timestamp": int(time.time() * 1000)
        }
        
        # This assumes a schema 'DAOProposalApproved' is defined and expected by consumers.
        # We are sending a JSON payload. The consumer will need to know the schema.
        self.producer.send(json.dumps(trust_event).encode('utf-8'))
        print(f"Published DAOProposalApproved event for proposal {proposal.id} to {TRUST_ENGINE_TOPIC}")


    def stop(self):
        self.running = False
        self.producer.close()
        self.client.close()
        print("Pulsar consumer stopped.")

pulsar_consumer = PulsarConsumer()

def start_consumer():
    pulsar_consumer.start()

def stop_consumer():
    pulsar_consumer.stop() 