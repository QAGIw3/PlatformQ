import asyncio
import json
import os
import threading
from web3 import Web3
import pulsar

# Configuration
NODE_URL = os.environ.get("NODE_URL", "ws://localhost:8545") # Use WebSocket for event listening
GOVERNOR_CONTRACT_ADDRESS = os.environ.get("GOVERNOR_CONTRACT_ADDRESS", "0xDc64a140Aa3E981100a9becA4E685f962f0cF6C9")
PULSAR_SERVICE_URL = os.environ.get("PULSAR_URL", "pulsar://localhost:6650")
ABI_PATH = os.path.join(os.path.dirname(__file__), 'abi.json')
ONCHAIN_GOVERNANCE_EVENTS_TOPIC = "persistent://public/default/onchain-governance-events"

class BlockchainEventBridge:
    def __init__(self):
        self.w3 = Web3(Web3.WebsocketProvider(NODE_URL))
        with open(ABI_PATH) as f:
            abi = json.load(f)["abi"]
        self.governor = self.w3.eth.contract(address=GOVERNOR_CONTRACT_ADDRESS, abi=abi)
        
        self.pulsar_client = pulsar.Client(PULSAR_SERVICE_URL)
        self.pulsar_producer = self.pulsar_client.create_producer(ONCHAIN_GOVERNANCE_EVENTS_TOPIC)
        
        self.running = False

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self.listen_for_events)
        self.thread.daemon = True
        self.thread.start()
        print("Blockchain event bridge started...")

    def listen_for_events(self):
        event_filter = self.governor.events.ProposalCreated.createFilter(fromBlock='latest')
        while self.running:
            for event in event_filter.get_new_entries():
                self.handle_event(event)
            asyncio.sleep(2)

    def handle_event(self, event):
        event_data = {
            "eventType": "ProposalCreated",
            "proposalId": str(event.args.proposalId),
            "proposer": event.args.proposer,
            # Add other event args as needed
        }
        self.pulsar_producer.send(json.dumps(event_data).encode('utf-8'))
        print(f"Published event: {event_data}")

    def stop(self):
        self.running = False
        if self.thread.is_alive():
            self.thread.join()
        self.pulsar_client.close()
        print("Blockchain event bridge stopped.")

bridge = BlockchainEventBridge()

# In a real FastAPI app, you would use startup and shutdown events.
def main():
    try:
        bridge.start()
        while True:
            asyncio.sleep(1)
    except KeyboardInterrupt:
        bridge.stop()

if __name__ == "__main__":
    main()
