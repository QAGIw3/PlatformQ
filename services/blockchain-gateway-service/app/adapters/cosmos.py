"""
Cosmos chain adapter
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Callable
from cosmpy.aerial.client import LedgerClient, NetworkConfig
from cosmpy.aerial.wallet import LocalWallet
from cosmpy.aerial.contract import LedgerContract
from cosmpy.crypto.keypairs import PrivateKey
from cosmpy.aerial.tx import Transaction
from cosmpy.aerial.tx_helpers import SubmittedTx

from .base import ChainAdapter, ChainType

logger = logging.getLogger(__name__)


class CosmosAdapter(ChainAdapter):
    """Adapter for Cosmos SDK based blockchains"""
    
    def __init__(self, chain_type: ChainType, chain_id: str, config: Dict[str, Any]):
        super().__init__(chain_type, chain_id, config)
        self.client = None
        self.wallet = None
        self.contracts = {}
        self._event_listeners = {}
        
    async def connect(self) -> bool:
        """Connect to Cosmos chain"""
        try:
            # Create network configuration
            network_config = NetworkConfig(
                chain_id=self.chain_id,
                url=self.config['rpc_url'],
                fee_minimum_gas_price=self.config.get('min_gas_price', 0.025),
                fee_denomination=self.config.get('fee_denom', 'uatom'),
                staking_denomination=self.config.get('staking_denom', 'uatom')
            )
            
            # Initialize client
            self.client = LedgerClient(network_config)
            
            # Initialize wallet if private key provided
            if 'private_key' in self.config:
                private_key = PrivateKey(bytes.fromhex(self.config['private_key']))
                self.wallet = LocalWallet(private_key)
            
            # Test connection
            height = self.client.query_height()
            if height > 0:
                self._connected = True
                logger.info(f"Connected to Cosmos chain {self.chain_id} at height {height}")
                
                # Start event monitoring
                asyncio.create_task(self._event_monitoring_loop())
                return True
            else:
                logger.error(f"Failed to connect to Cosmos chain {self.chain_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to Cosmos: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from Cosmos chain"""
        self._connected = False
        # Cosmos SDK client doesn't need explicit disconnect
        logger.info(f"Disconnected from Cosmos chain {self.chain_id}")
    
    def load_contract(self, address: str, schema_path: str) -> LedgerContract:
        """Load a CosmWasm contract"""
        try:
            # Load contract schema
            with open(schema_path) as f:
                schema = json.load(f)
            
            contract = LedgerContract(
                client=self.client,
                address=address,
                schema=schema
            )
            
            self.contracts[address] = contract
            return contract
            
        except Exception as e:
            logger.error(f"Error loading Cosmos contract: {e}")
            raise
    
    async def subscribe_to_events(self, contract_address: str, event_name: str,
                                 handler: Callable[[Dict[str, Any]], None]) -> bool:
        """Subscribe to contract events"""
        try:
            # Cosmos SDK uses different event subscription mechanism
            # This is a simplified implementation
            key = f"{contract_address}:{event_name}"
            if key not in self._event_listeners:
                self._event_listeners[key] = []
            
            self._event_listeners[key].append(handler)
            
            logger.info(f"Subscribed to {event_name} events on {contract_address}")
            return True
            
        except Exception as e:
            logger.error(f"Error subscribing to Cosmos events: {e}")
            return False
    
    async def _event_monitoring_loop(self):
        """Monitor blockchain events via transaction queries"""
        last_height = await self.get_latest_block()
        
        while self._connected:
            try:
                current_height = await self.get_latest_block()
                
                # Query transactions in new blocks
                for height in range(last_height + 1, current_height + 1):
                    txs = self.client.query_tx_by_height(height)
                    
                    for tx in txs:
                        # Parse events from transaction
                        events = self._parse_tx_events(tx)
                        
                        for event in events:
                            # Emit to registered handlers
                            key = f"{event['contract']}:{event['type']}"
                            if key in self._event_listeners:
                                for handler in self._event_listeners[key]:
                                    await handler(event)
                
                last_height = current_height
                await asyncio.sleep(self.config.get('poll_interval', 3))
                
            except Exception as e:
                logger.error(f"Error in Cosmos event monitoring: {e}")
                await asyncio.sleep(5)
    
    def _parse_tx_events(self, tx: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse events from transaction"""
        events = []
        
        # Extract events from tx result
        if 'logs' in tx:
            for log in tx['logs']:
                for event in log.get('events', []):
                    parsed_event = {
                        'type': event['type'],
                        'attributes': {
                            attr['key']: attr['value'] 
                            for attr in event.get('attributes', [])
                        },
                        'height': tx['height'],
                        'txHash': tx['txhash'],
                        'chainType': self.chain_type.value,
                        'chainId': self.chain_id
                    }
                    
                    # Extract contract address if available
                    if 'contract_address' in parsed_event['attributes']:
                        parsed_event['contract'] = parsed_event['attributes']['contract_address']
                    
                    events.append(parsed_event)
        
        return events
    
    async def get_latest_block(self) -> int:
        """Get latest block height"""
        return self.client.query_height()
    
    async def get_reputation_balance(self, address: str) -> int:
        """Get reputation from on-chain contract"""
        if 'reputation_contract' not in self.config:
            return 0
        
        contract = self.contracts.get(self.config['reputation_contract'])
        if not contract:
            return 0
        
        try:
            # Query reputation
            result = contract.query({
                "get_reputation": {
                    "address": address
                }
            })
            
            return result.get('reputation', 0)
            
        except Exception as e:
            logger.error(f"Error getting Cosmos reputation: {e}")
            return 0
    
    async def submit_proposal(self, proposal_data: Dict[str, Any]) -> str:
        """Submit proposal to Cosmos governance module"""
        if not self.wallet:
            raise Exception("Wallet not initialized")
        
        governance_contract = self.contracts.get(self.config['governance_contract'])
        if not governance_contract:
            raise Exception("Governance contract not loaded")
        
        try:
            # Create proposal message
            msg = governance_contract.create_execute_msg({
                "create_proposal": {
                    "title": proposal_data['title'],
                    "description": proposal_data['description'],
                    "actions": proposal_data.get('actions', []),
                    "voting_period": proposal_data.get('voting_period', 604800)  # 7 days
                }
            })
            
            # Create and sign transaction
            tx = Transaction()
            tx.add_message(msg)
            
            # Estimate gas
            gas_limit = self.client.estimate_gas(tx)
            tx.set_gas_limit(gas_limit)
            
            # Sign and broadcast
            tx.sign(self.wallet.signer())
            tx.complete()
            
            submitted_tx = self.client.broadcast_tx(tx)
            submitted_tx.wait_for_inclusion()
            
            # Extract proposal ID from events
            proposal_id = self._extract_proposal_id(submitted_tx)
            
            return proposal_id
            
        except Exception as e:
            logger.error(f"Error submitting Cosmos proposal: {e}")
            raise
    
    def _extract_proposal_id(self, tx: SubmittedTx) -> str:
        """Extract proposal ID from transaction events"""
        # Parse events to find proposal ID
        # This is implementation-specific
        return f"cosmos-{tx.tx_hash}"
    
    async def cast_vote(self, proposal_id: str, support: bool,
                       voter_address: str, signature: Optional[str] = None) -> str:
        """Cast vote on Cosmos proposal"""
        if not self.wallet:
            raise Exception("Wallet not initialized")
        
        governance_contract = self.contracts.get(self.config['governance_contract'])
        if not governance_contract:
            raise Exception("Governance contract not loaded")
        
        try:
            # Create vote message
            vote_option = "yes" if support else "no"
            
            msg = governance_contract.create_execute_msg({
                "vote": {
                    "proposal_id": int(proposal_id),
                    "vote": vote_option
                }
            })
            
            # Create and sign transaction
            tx = Transaction()
            tx.add_message(msg)
            tx.set_gas_limit(200000)
            tx.sign(self.wallet.signer())
            tx.complete()
            
            # Broadcast
            submitted_tx = self.client.broadcast_tx(tx)
            submitted_tx.wait_for_inclusion()
            
            return submitted_tx.tx_hash
            
        except Exception as e:
            logger.error(f"Error casting Cosmos vote: {e}")
            raise
    
    async def get_proposal_state(self, proposal_id: str) -> Dict[str, Any]:
        """Get Cosmos proposal state"""
        governance_contract = self.contracts.get(self.config['governance_contract'])
        if not governance_contract:
            raise Exception("Governance contract not loaded")
        
        try:
            # Query proposal
            result = governance_contract.query({
                "get_proposal": {
                    "proposal_id": int(proposal_id)
                }
            })
            
            return {
                'proposalId': proposal_id,
                'state': result.get('status', 'unknown'),
                'forVotes': str(result.get('yes_votes', 0)),
                'againstVotes': str(result.get('no_votes', 0)),
                'abstainVotes': str(result.get('abstain_votes', 0)),
                'startTime': result.get('start_time'),
                'endTime': result.get('end_time')
            }
            
        except Exception as e:
            logger.error(f"Error getting Cosmos proposal state: {e}")
            raise
    
    async def get_voting_power(self, address: str, block_number: Optional[int] = None) -> int:
        """Get voting power from staked tokens or governance contract"""
        try:
            # Query staked balance
            balance = self.client.query_bank_balance(address)
            
            # In Cosmos, voting power often equals staked tokens
            # This is a simplified implementation
            return int(balance)
            
        except:
            return 0
    
    async def execute_proposal(self, proposal_id: str) -> str:
        """Execute a passed Cosmos proposal"""
        if not self.wallet:
            raise Exception("Wallet not initialized")
        
        governance_contract = self.contracts.get(self.config['governance_contract'])
        if not governance_contract:
            raise Exception("Governance contract not loaded")
        
        try:
            msg = governance_contract.create_execute_msg({
                "execute_proposal": {
                    "proposal_id": int(proposal_id)
                }
            })
            
            tx = Transaction()
            tx.add_message(msg)
            tx.set_gas_limit(500000)
            tx.sign(self.wallet.signer())
            tx.complete()
            
            submitted_tx = self.client.broadcast_tx(tx)
            submitted_tx.wait_for_inclusion()
            
            return submitted_tx.tx_hash
            
        except Exception as e:
            logger.error(f"Error executing Cosmos proposal: {e}")
            raise
    
    def validate_address(self, address: str) -> bool:
        """Validate Cosmos address"""
        try:
            # Cosmos addresses start with chain-specific prefix
            prefix = self.config.get('address_prefix', 'cosmos')
            return address.startswith(prefix) and len(address) > 10
        except:
            return False
    
    def format_address(self, address: str) -> str:
        """Format Cosmos address"""
        return address 

    async def mint_asset_nft(self, to: str, uri: str, royalty_recipient: str, royalty_fraction: int) -> str:
        """Mints NFT on Cosmos chain"""
        # Cosmos-specific implementation
        contract = self.contracts.get("platform_asset")
        if not contract:
            raise Exception("Platform asset contract not loaded")
        
        # Create and send transaction
        tx = contract.execute({
            "mint": {
                "to": to,
                "uri": uri,
                "royalty_recipient": royalty_recipient,
                "royalty_fraction": royalty_fraction
            }
        }, sender=self.wallet)
        
        return tx.tx_hash

    async def create_license_offer(self, asset_id: str, price: int, duration: int, license_type: str, max_usage: int, royalty_percentage: int) -> str:
        """Creates license offer on Cosmos"""
        contract = self.contracts.get("usage_license")
        if not contract:
            raise Exception("Usage license contract not loaded")
            
        tx = contract.execute({
            "create_offer": {
                "asset_id": asset_id,
                "price": str(price),
                "duration": duration,
                "license_type": license_type,
                "max_usage": max_usage,
                "royalty_percentage": royalty_percentage
            }
        }, sender=self.wallet)
        
        return tx.tx_hash

    async def purchase_license(self, asset_id: str, offer_index: int, license_type: int) -> str:
        """Purchase license on Cosmos"""
        contract = self.contracts.get("usage_license")
        if not contract:
            raise Exception("Usage license contract not loaded")
            
        tx = contract.execute({
            "purchase_license": {
                "asset_id": asset_id,
                "offer_index": offer_index,
                "license_type": license_type
            }
        }, sender=self.wallet)
        
        return tx.tx_hash

    async def distribute_royalty(self, token_id: int, sale_price: int) -> str:
        """Distribute royalty on Cosmos"""
        contract = self.contracts.get("royalty_distributor")
        if not contract:
            raise Exception("Royalty distributor contract not loaded")
            
        tx = contract.execute({
            "distribute": {
                "token_id": token_id,
                "sale_price": str(sale_price)
            }
        }, sender=self.wallet)
        
        return tx.tx_hash 