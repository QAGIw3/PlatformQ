"""
Ethereum/EVM chain adapter
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Callable
from web3 import Web3
from web3.contract import Contract
from eth_account import Account
from hexbytes import HexBytes

from .base import ChainAdapter, ChainType

logger = logging.getLogger(__name__)


class EthereumAdapter(ChainAdapter):
    """Adapter for Ethereum and EVM-compatible chains"""
    
    def __init__(self, chain_type: ChainType, chain_id: str, config: Dict[str, Any]):
        super().__init__(chain_type, chain_id, config)
        self.w3 = None
        self.contracts = {}
        self.event_filters = {}
        self._event_loop_task = None
        
    async def connect(self) -> bool:
        """Connect to Ethereum node"""
        try:
            # Support both HTTP and WebSocket providers
            if self.config['node_url'].startswith('ws'):
                self.w3 = Web3(Web3.WebsocketProvider(self.config['node_url']))
            else:
                self.w3 = Web3(Web3.HTTPProvider(self.config['node_url']))
            
            if self.w3.is_connected():
                self._connected = True
                chain_id = self.w3.eth.chain_id
                logger.info(f"Connected to {self.chain_type.value} chain ID: {chain_id}")
                
                # Start event monitoring loop
                self._event_loop_task = asyncio.create_task(self._event_monitoring_loop())
                return True
            else:
                logger.error(f"Failed to connect to {self.chain_type.value}")
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to {self.chain_type.value}: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from Ethereum node"""
        if self._event_loop_task:
            self._event_loop_task.cancel()
            try:
                await self._event_loop_task
            except asyncio.CancelledError:
                pass
        
        self._connected = False
        logger.info(f"Disconnected from {self.chain_type.value}")
    
    def load_contract(self, address: str, abi_path: str) -> Contract:
        """Load a contract instance"""
        with open(abi_path) as f:
            abi = json.load(f)
            if "abi" in abi:
                abi = abi["abi"]
        
        contract = self.w3.eth.contract(
            address=Web3.to_checksum_address(address),
            abi=abi
        )
        self.contracts[address] = contract
        return contract
    
    async def subscribe_to_events(self, contract_address: str, event_name: str,
                                 handler: Callable[[Dict[str, Any]], None]) -> bool:
        """Subscribe to contract events"""
        try:
            if contract_address not in self.contracts:
                logger.error(f"Contract {contract_address} not loaded")
                return False
            
            contract = self.contracts[contract_address]
            event = getattr(contract.events, event_name, None)
            
            if not event:
                logger.error(f"Event {event_name} not found in contract")
                return False
            
            # Create event filter
            event_filter = event.create_filter(fromBlock='latest')
            filter_key = f"{contract_address}:{event_name}"
            self.event_filters[filter_key] = event_filter
            
            # Register handler
            self.register_event_handler(filter_key, handler)
            
            logger.info(f"Subscribed to {event_name} events on {contract_address}")
            return True
            
        except Exception as e:
            logger.error(f"Error subscribing to events: {e}")
            return False
    
    async def _event_monitoring_loop(self):
        """Monitor blockchain events"""
        while self._connected:
            try:
                for filter_key, event_filter in self.event_filters.items():
                    new_events = event_filter.get_new_entries()
                    
                    for event in new_events:
                        event_data = self._process_event(event)
                        self.emit_event(filter_key, event_data)
                
                await asyncio.sleep(2)  # Poll every 2 seconds
                
            except Exception as e:
                logger.error(f"Error in event monitoring: {e}")
                await asyncio.sleep(5)
    
    def _process_event(self, event) -> Dict[str, Any]:
        """Process raw event into standard format"""
        return {
            'address': event['address'],
            'blockNumber': event['blockNumber'],
            'transactionHash': event['transactionHash'].hex(),
            'event': event['event'] if hasattr(event, 'event') else 'Unknown',
            'args': dict(event['args']) if hasattr(event, 'args') else {},
            'chainType': self.chain_type.value,
            'chainId': self.chain_id
        }
    
    async def get_latest_block(self) -> int:
        """Get latest block number"""
        return self.w3.eth.block_number
    
    async def get_reputation_balance(self, address: str) -> int:
        """Get reputation balance from reputation oracle"""
        if 'reputation_oracle_address' not in self.config:
            return 0
        
        oracle = self.contracts.get(self.config['reputation_oracle_address'])
        if not oracle:
            return 0
        
        try:
            reputation = oracle.functions.getReputation(
                Web3.to_checksum_address(address)
            ).call()
            return reputation
        except Exception as e:
            logger.error(f"Error getting reputation: {e}")
            return 0
    
    async def submit_proposal(self, proposal_data: Dict[str, Any]) -> str:
        """Submit a proposal to the governor contract"""
        governor = self.contracts.get(self.config['governor_address'])
        if not governor:
            raise Exception("Governor contract not loaded")
        
        try:
            # Build transaction
            tx = governor.functions.propose(
                proposal_data['targets'],
                proposal_data['values'],
                proposal_data['calldatas'],
                proposal_data['description']
            ).build_transaction({
                'from': proposal_data['proposer'],
                'gas': 500000,
                'gasPrice': self.w3.eth.gas_price,
                'nonce': self.w3.eth.get_transaction_count(proposal_data['proposer'])
            })
            
            # Sign and send transaction
            if 'private_key' in proposal_data:
                signed_tx = self.w3.eth.account.sign_transaction(
                    tx, proposal_data['private_key']
                )
                tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            else:
                # For development/testing
                tx_hash = self.w3.eth.send_transaction(tx)
            
            # Wait for receipt
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            
            # Extract proposal ID from events
            proposal_created_event = governor.events.ProposalCreated().process_receipt(receipt)
            if proposal_created_event:
                return str(proposal_created_event[0]['args']['proposalId'])
            
            return tx_hash.hex()
            
        except Exception as e:
            logger.error(f"Error submitting proposal: {e}")
            raise
    
    async def cast_vote(self, proposal_id: str, support: bool,
                       voter_address: str, signature: Optional[str] = None) -> str:
        """Cast a vote on a proposal"""
        governor = self.contracts.get(self.config['governor_address'])
        if not governor:
            raise Exception("Governor contract not loaded")
        
        try:
            support_value = 1 if support else 0
            
            if signature:
                # Vote by signature
                # Parse signature
                v, r, s = self._parse_signature(signature)
                tx = governor.functions.castVoteBySig(
                    int(proposal_id),
                    support_value,
                    v, r, s
                ).build_transaction({
                    'from': voter_address,
                    'gas': 150000,
                    'gasPrice': self.w3.eth.gas_price
                })
            else:
                # Direct vote
                tx = governor.functions.castVote(
                    int(proposal_id),
                    support_value
                ).build_transaction({
                    'from': voter_address,
                    'gas': 150000,
                    'gasPrice': self.w3.eth.gas_price,
                    'nonce': self.w3.eth.get_transaction_count(voter_address)
                })
            
            # Send transaction (simplified for example)
            tx_hash = self.w3.eth.send_transaction(tx)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            
            return tx_hash.hex()
            
        except Exception as e:
            logger.error(f"Error casting vote: {e}")
            raise
    
    async def get_proposal_state(self, proposal_id: str) -> Dict[str, Any]:
        """Get proposal state"""
        governor = self.contracts.get(self.config['governor_address'])
        if not governor:
            raise Exception("Governor contract not loaded")
        
        try:
            state = governor.functions.state(int(proposal_id)).call()
            votes = governor.functions.proposalVotes(int(proposal_id)).call()
            
            state_names = ['Pending', 'Active', 'Canceled', 'Defeated', 
                          'Succeeded', 'Queued', 'Expired', 'Executed']
            
            return {
                'proposalId': proposal_id,
                'state': state_names[state] if state < len(state_names) else 'Unknown',
                'forVotes': str(votes[1]),
                'againstVotes': str(votes[0]),
                'abstainVotes': str(votes[2])
            }
            
        except Exception as e:
            logger.error(f"Error getting proposal state: {e}")
            raise
    
    async def get_voting_power(self, address: str, block_number: Optional[int] = None) -> int:
        """Get voting power at specific block"""
        governor = self.contracts.get(self.config['governor_address'])
        if not governor:
            return 0
        
        try:
            if block_number is None:
                block_number = self.w3.eth.block_number
            
            votes = governor.functions.getVotes(
                Web3.to_checksum_address(address),
                block_number
            ).call()
            
            return votes
            
        except Exception as e:
            logger.error(f"Error getting voting power: {e}")
            return 0
    
    async def execute_proposal(self, proposal_id: str) -> str:
        """Execute a passed proposal"""
        governor = self.contracts.get(self.config['governor_address'])
        if not governor:
            raise Exception("Governor contract not loaded")
        
        # Implementation depends on specific governor contract
        # This is a placeholder
        raise NotImplementedError("Proposal execution not implemented")
    
    def validate_address(self, address: str) -> bool:
        """Validate Ethereum address"""
        try:
            Web3.to_checksum_address(address)
            return True
        except:
            return False
    
    def format_address(self, address: str) -> str:
        """Format to checksum address"""
        try:
            return Web3.to_checksum_address(address)
        except:
            return address
    
    def _parse_signature(self, signature: str) -> tuple:
        """Parse signature into v, r, s components"""
        if signature.startswith('0x'):
            signature = signature[2:]
        
        if len(signature) != 130:
            raise ValueError("Invalid signature length")
        
        r = int(signature[:64], 16)
        s = int(signature[64:128], 16)
        v = int(signature[128:], 16)
        
        return v, r, s 