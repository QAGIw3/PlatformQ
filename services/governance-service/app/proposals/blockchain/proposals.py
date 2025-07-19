from web3 import Web3
import json
import os

# Assuming the service runs from the root of the `proposals-service` directory
ABI_PATH = os.path.join(os.path.dirname(__file__), '..', 'abi', 'PlatformQGovernor.json')


# TODO: Move these to a config file
NODE_URL = "http://localhost:8545" # Default for local Hardhat node
GOVERNOR_CONTRACT_ADDRESS = "0xDc64a140Aa3E981100a9becA4E685f962f0cF6C9"
GOVERNOR_ABI_PATH = ABI_PATH

class ProposalBlockchainService:
    def __init__(self):
        self.w3 = Web3(Web3.HTTPProvider(NODE_URL))
        with open(GOVERNOR_ABI_PATH) as f:
            abi = json.load(f)["abi"]
        self.governor = self.w3.eth.contract(address=GOVERNOR_CONTRACT_ADDRESS, abi=abi)

    def propose(self, targets: list, values: list, calldatas: list, description: str) -> str:
        """
        Submits a proposal to the PlatformQGovernor contract.
        Returns the on-chain proposal ID.
        """
        # TODO: Handle account and signing
        # For now, this is a placeholder
        tx_hash = self.governor.functions.propose(
            targets,
            values,
            calldatas,
            description
        ).transact({'from': self.w3.eth.accounts[0]}) # Using default account for now

        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        
        # Extract proposalId from the ProposalCreated event
        # This part depends on the exact event signature in your contract
        # and might need adjustment.
        for log in receipt['logs']:
            # A more robust way would be to use contract.events.ProposalCreated().processReceipt(receipt)
            # but that requires a bit more setup.
            # This is a simplified example.
            if log['topics'][0] == self.w3.keccak(text="ProposalCreated(uint256,address,address[],uint256[],string[],bytes[],uint256,uint256,string)"):
                proposal_id = self.w3.toInt(log['topics'][1])
                return str(proposal_id)

        raise Exception("Could not find ProposalCreated event in transaction receipt")

proposals_blockchain_service = ProposalBlockchainService() 