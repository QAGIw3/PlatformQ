"""
Hyperledger Fabric chain adapter
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Callable
from hfc.fabric import Client as FabricClient
from hfc.fabric_network import gateway
from hfc.util import get_peer_org_user

from .base import ChainAdapter, ChainType

logger = logging.getLogger(__name__)


class HyperledgerAdapter(ChainAdapter):
    """Adapter for Hyperledger Fabric blockchain"""
    
    def __init__(self, chain_type: ChainType, chain_id: str, config: Dict[str, Any]):
        super().__init__(chain_type, chain_id, config)
        self.fabric_client = None
        self.gateway = None
        self.network = None
        self.contracts = {}
        self.event_hubs = {}
        
    async def connect(self) -> bool:
        """Connect to Hyperledger Fabric network"""
        try:
            # Initialize Fabric client
            self.fabric_client = FabricClient(net_profile=self.config['network_profile'])
            
            # Create user context
            org_name = self.config['org_name']
            user_name = self.config['user_name']
            user = get_peer_org_user(
                self.fabric_client,
                org_name,
                user_name,
                self.config.get('user_cert_path'),
                self.config.get('user_key_path')
            )
            
            # Create gateway
            self.gateway = gateway.Gateway()
            options = gateway.GatewayOptions(
                wallet=self.config.get('wallet'),
                identity=user_name,
                discovery={'enabled': True, 'asLocalhost': False}
            )
            
            await self.gateway.connect(self.config['connection_profile'], options)
            
            # Get network
            self.network = await self.gateway.get_network(self.config['channel_name'])
            
            self._connected = True
            logger.info(f"Connected to Hyperledger Fabric network on channel {self.config['channel_name']}")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to Hyperledger Fabric: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from Fabric network"""
        if self.gateway:
            self.gateway.disconnect()
        
        # Close event hubs
        for hub in self.event_hubs.values():
            hub.disconnect()
        
        self._connected = False
        logger.info("Disconnected from Hyperledger Fabric")
    
    def get_contract(self, chaincode_name: str) -> Any:
        """Get chaincode contract instance"""
        if chaincode_name not in self.contracts:
            contract = self.network.get_contract(chaincode_name)
            self.contracts[chaincode_name] = contract
        return self.contracts[chaincode_name]
    
    async def subscribe_to_events(self, contract_address: str, event_name: str,
                                 handler: Callable[[Dict[str, Any]], None]) -> bool:
        """Subscribe to chaincode events"""
        try:
            contract = self.get_contract(contract_address)
            
            # Create event listener
            async def event_listener(event):
                event_data = self._process_fabric_event(event, event_name)
                if event_data:
                    handler(event_data)
            
            # Add listener
            await contract.add_contract_listener(
                event_listener,
                event_name=event_name
            )
            
            logger.info(f"Subscribed to {event_name} events on chaincode {contract_address}")
            return True
            
        except Exception as e:
            logger.error(f"Error subscribing to Fabric events: {e}")
            return False
    
    def _process_fabric_event(self, event: Any, event_name: str) -> Optional[Dict[str, Any]]:
        """Process Fabric event"""
        try:
            return {
                'event': event_name,
                'chaincodeId': event.chaincode_id,
                'transactionId': event.tx_id,
                'blockNumber': event.block_number,
                'payload': json.loads(event.payload.decode('utf-8')) if event.payload else {},
                'chainType': self.chain_type.value,
                'chainId': self.chain_id
            }
        except Exception as e:
            logger.error(f"Error processing Fabric event: {e}")
            return None
    
    async def get_latest_block(self) -> int:
        """Get latest block number"""
        try:
            # Query ledger info
            channel = self.network.get_channel()
            ledger_info = await channel.query_info()
            return ledger_info.height - 1
        except Exception as e:
            logger.error(f"Error getting latest block: {e}")
            return 0
    
    async def get_reputation_balance(self, address: str) -> int:
        """Get reputation from chaincode"""
        if 'reputation_chaincode' not in self.config:
            return 0
        
        try:
            contract = self.get_contract(self.config['reputation_chaincode'])
            
            # Query reputation
            result = await contract.evaluate_transaction(
                'getReputation',
                address
            )
            
            return int(result.decode('utf-8')) if result else 0
            
        except Exception as e:
            logger.error(f"Error getting Fabric reputation: {e}")
            return 0
    
    async def submit_proposal(self, proposal_data: Dict[str, Any]) -> str:
        """Submit proposal to Fabric governance chaincode"""
        if 'governance_chaincode' not in self.config:
            raise Exception("Governance chaincode not configured")
        
        try:
            contract = self.get_contract(self.config['governance_chaincode'])
            
            # Submit transaction
            result = await contract.submit_transaction(
                'createProposal',
                json.dumps({
                    'title': proposal_data['title'],
                    'description': proposal_data['description'],
                    'proposer': proposal_data['proposer'],
                    'actions': proposal_data.get('actions', [])
                })
            )
            
            # Return proposal ID from result
            proposal_id = result.decode('utf-8')
            return proposal_id
            
        except Exception as e:
            logger.error(f"Error submitting Fabric proposal: {e}")
            raise
    
    async def cast_vote(self, proposal_id: str, support: bool,
                       voter_address: str, signature: Optional[str] = None) -> str:
        """Cast vote on Fabric proposal"""
        try:
            contract = self.get_contract(self.config['governance_chaincode'])
            
            vote_value = "FOR" if support else "AGAINST"
            
            result = await contract.submit_transaction(
                'castVote',
                proposal_id,
                vote_value,
                voter_address
            )
            
            return result.decode('utf-8')
            
        except Exception as e:
            logger.error(f"Error casting Fabric vote: {e}")
            raise
    
    async def get_proposal_state(self, proposal_id: str) -> Dict[str, Any]:
        """Get Fabric proposal state"""
        try:
            contract = self.get_contract(self.config['governance_chaincode'])
            
            result = await contract.evaluate_transaction(
                'getProposal',
                proposal_id
            )
            
            proposal = json.loads(result.decode('utf-8'))
            
            return {
                'proposalId': proposal_id,
                'state': proposal.get('state', 'unknown'),
                'forVotes': str(proposal.get('forVotes', 0)),
                'againstVotes': str(proposal.get('againstVotes', 0)),
                'abstainVotes': str(proposal.get('abstainVotes', 0)),
                'startTime': proposal.get('votingStartTime'),
                'endTime': proposal.get('votingEndTime')
            }
            
        except Exception as e:
            logger.error(f"Error getting Fabric proposal state: {e}")
            raise
    
    async def get_voting_power(self, address: str, block_number: Optional[int] = None) -> int:
        """Get voting power from Fabric"""
        try:
            contract = self.get_contract(self.config['governance_chaincode'])
            
            # Query voting power
            result = await contract.evaluate_transaction(
                'getVotingPower',
                address
            )
            
            return int(result.decode('utf-8')) if result else 0
            
        except:
            return 0
    
    async def execute_proposal(self, proposal_id: str) -> str:
        """Execute Fabric proposal"""
        try:
            contract = self.get_contract(self.config['governance_chaincode'])
            
            result = await contract.submit_transaction(
                'executeProposal',
                proposal_id
            )
            
            return result.decode('utf-8')
            
        except Exception as e:
            logger.error(f"Error executing Fabric proposal: {e}")
            raise
    
    def validate_address(self, address: str) -> bool:
        """Validate Fabric identity"""
        # In Fabric, this would validate certificate or identity format
        # Simplified for example
        return len(address) > 0
    
    def format_address(self, address: str) -> str:
        """Format Fabric identity"""
        return address 