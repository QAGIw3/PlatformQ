from web3 import Web3
import json
import os
import logging
from typing import Dict, Any, Optional
from eth_account.messages import encode_defunct

logger = logging.getLogger(__name__)

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
        self.operator_address = self.operator_account.address
        self.private_key = self.operator_account.key if hasattr(self.operator_account, 'key') else self.operator_account.privateKey

    def set_reputation(self, user_address: str, score: int):
        """
        Sets the reputation score for a user on the blockchain (legacy method).
        """
        nonce = self.w3.eth.get_transaction_count(self.operator_address)
        txn = self.contract.functions.setReputation(
            user_address,
            score
        ).build_transaction({
            'from': self.operator_address,
            'nonce': nonce,
            'gas': 100000,
            'gasPrice': self.w3.to_wei('20', 'gwei')
        })
        
        signed_txn = self.w3.eth.account.sign_transaction(txn, private_key=self.private_key)
        tx_hash = self.w3.eth.send_raw_transaction(signed_txn.rawTransaction)
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        return receipt

    async def update_reputation(self, entity_address: str, new_score: int, vc_hash: bytes = None, expiry: int = None) -> Dict[str, Any]:
        """
        Update reputation score on-chain with optional VC verification.
        """
        try:
            # Ensure score is within valid range
            if new_score < 0 or new_score > 100:
                raise ValueError("Score must be between 0 and 100")
            
            nonce = self.w3.eth.get_transaction_count(self.operator_address)
            
            # Use VC-based update if hash provided
            if vc_hash and expiry:
                # Create signature for VC validation
                message = encode_defunct(text=f"{entity_address}:{new_score}:{vc_hash.hex()}:{expiry}")
                signature = self.w3.eth.account.sign_message(message, private_key=self.private_key)
                
                # Build transaction for VC-based update
                txn = self.contract.functions.updateTrustScoreWithVC(
                    entity_address,
                    new_score,
                    vc_hash,
                    expiry,
                    signature.signature
                ).build_transaction({
                    'from': self.operator_address,
                    'nonce': nonce,
                    'gas': 150000,
                    'gasPrice': self.w3.to_wei('20', 'gwei')
                })
            else:
                # Legacy update
                txn = self.contract.functions.setReputation(
                    entity_address,
                    new_score
                ).build_transaction({
                    'from': self.operator_address,
                    'nonce': nonce,
                    'gas': 100000,
                    'gasPrice': self.w3.to_wei('20', 'gwei')
                })
            
            # Sign and send transaction
            signed_txn = self.w3.eth.account.sign_transaction(txn, private_key=self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_txn.rawTransaction)
            
            # Wait for confirmation
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            
            return {
                'success': True,
                'tx_hash': receipt['transactionHash'].hex(),
                'block_number': receipt['blockNumber'],
                'vc_anchored': bool(vc_hash)
            }
        except Exception as e:
            logger.error(f"Failed to update reputation: {e}")
            return {'success': False, 'error': str(e)}
    
    def get_trust_score(self, entity_address: str) -> Dict[str, Any]:
        """
        Get trust score with VC validation status.
        """
        try:
            result = self.contract.functions.getTrustScore(entity_address).call()
            return {
                'score': result[0],
                'vc_hash': result[1].hex() if result[1] else None,
                'expiry': result[2],
                'is_valid': result[3]
            }
        except Exception as e:
            logger.error(f"Failed to get trust score: {e}")
            return {'error': str(e)}
    
    def has_valid_trust_score(self, entity_address: str, min_score: int = 0) -> bool:
        """
        Check if an entity has a valid trust score above minimum.
        """
        try:
            return self.contract.functions.hasValidTrustScore(entity_address, min_score).call()
        except Exception as e:
            logger.error(f"Failed to check trust score validity: {e}")
            return False
    
    def authorize_issuer(self, issuer_address: str, authorized: bool = True):
        """
        Authorize or revoke an issuer for updating trust scores (owner only).
        """
        try:
            nonce = self.w3.eth.get_transaction_count(self.operator_address)
            txn = self.contract.functions.setAuthorizedIssuer(
                issuer_address,
                authorized
            ).build_transaction({
                'from': self.operator_address,
                'nonce': nonce,
                'gas': 80000,
                'gasPrice': self.w3.to_wei('20', 'gwei')
            })
            
            signed_txn = self.w3.eth.account.sign_transaction(txn, private_key=self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_txn.rawTransaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            
            logger.info(f"Issuer {issuer_address} {'authorized' if authorized else 'revoked'}")
            return receipt
        except Exception as e:
            logger.error(f"Failed to authorize issuer: {e}")
            raise

reputation_oracle_service = ReputationOracleService() 