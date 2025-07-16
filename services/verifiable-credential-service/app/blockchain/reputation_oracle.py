from web3 import Web3
import json
import os

# TODO: Move to a config file
NODE_URL = "http://localhost:8545"
REPUTATION_ORACLE_ADDRESS = "0x9fE46736679d2D9a65F0992F2272dE9f3c7fa6e0"
ABI_PATH = os.path.join(os.path.dirname(__file__), 'ReputationOracle.json')
# This should be a securely stored private key for a wallet with gas funds
OPERATOR_PRIVATE_KEY = os.environ.get("REPUTATION_OPERATOR_PRIVATE_KEY")

class ReputationOracleService:
    def __init__(self):
        self.w3 = Web3(Web3.HTTPProvider(NODE_URL))
        with open(ABI_PATH) as f:
            abi = json.load(f)["abi"]
        self.contract = self.w3.eth.contract(address=REPUTATION_ORACLE_ADDRESS, abi=abi)
        self.operator_account = self.w3.eth.account.from_key(OPERATOR_PRIVATE_KEY) if OPERATOR_PRIVATE_KEY else self.w3.eth.accounts[0]

    def set_reputation(self, user_address: str, score: int):
        """
        Sets the reputation score for a user on the blockchain.
        """
        nonce = self.w3.eth.getTransactionCount(self.operator_account.address)
        txn = self.contract.functions.setReputation(
            user_address,
            score
        ).buildTransaction({
            'from': self.operator_account.address,
            'nonce': nonce
        })
        
        signed_txn = self.w3.eth.account.sign_transaction(txn, private_key=self.operator_account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed_txn.rawTransaction)
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

reputation_oracle_service = ReputationOracleService() 