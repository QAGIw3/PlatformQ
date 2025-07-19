"""
NEAR chain adapter
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Callable
import base64
from near_api_py import connect, AsyncNearConnection
from near_api_py.account import Account
from near_api_py.providers import JsonProvider
from near_api_py.signer import Signer, InMemorySigner
from near_api_py.transaction import create_transaction, sign_transaction
from near_api_py.dapps.core import ContractAccount

from .base import ChainAdapter, ChainType

logger = logging.getLogger(__name__)


class NEARAdapter(ChainAdapter):
    """Adapter for NEAR blockchain"""
    
    def __init__(self, chain_type: ChainType, chain_id: str, config: Dict[str, Any]):
        super().__init__(chain_type, chain_id, config)
        self.connection = None
        self.account = None
        self.contracts = {}
        self._event_polling_task = None
        
    async def connect(self) -> bool:
        """Connect to NEAR network"""
        try:
            # Create connection configuration
            network_id = self.config.get('network_id', 'mainnet')
            node_url = self.config['rpc_url']
            
            # Initialize connection
            self.connection = AsyncNearConnection(
                network_id=network_id,
                provider=JsonProvider(node_url)
            )
            
            # Initialize account if credentials provided
            if 'account_id' in self.config and 'private_key' in self.config:
                signer = InMemorySigner.from_secret_key(
                    self.config['account_id'],
                    self.config['private_key']
                )
                
                self.account = await Account.from_connection(
                    self.connection,
                    self.config['account_id'],
                    signer
                )
            
            # Test connection
            status = await self.connection.provider.status()
            if status:
                self._connected = True
                logger.info(f"Connected to NEAR {network_id} at block {status['sync_info']['latest_block_height']}")
                
                # Start event polling
                self._event_polling_task = asyncio.create_task(self._event_polling_loop())
                return True
            else:
                logger.error(f"Failed to connect to NEAR {network_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to NEAR: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from NEAR"""
        if self._event_polling_task:
            self._event_polling_task.cancel()
            try:
                await self._event_polling_task
            except asyncio.CancelledError:
                pass
        
        self._connected = False
        logger.info("Disconnected from NEAR")
    
    def load_contract(self, contract_id: str) -> ContractAccount:
        """Load a NEAR contract"""
        try:
            if not self.account:
                raise Exception("Account not initialized")
            
            contract = ContractAccount(
                self.account,
                contract_id
            )
            
            self.contracts[contract_id] = contract
            return contract
            
        except Exception as e:
            logger.error(f"Error loading NEAR contract: {e}")
            raise
    
    async def subscribe_to_events(self, contract_address: str, event_name: str,
                                 handler: Callable[[Dict[str, Any]], None]) -> bool:
        """Subscribe to contract events via indexer"""
        try:
            # NEAR doesn't have native event subscription
            # Use polling or indexer service
            key = f"{contract_address}:{event_name}"
            self.register_event_handler(key, handler)
            
            logger.info(f"Subscribed to {event_name} events on {contract_address}")
            return True
            
        except Exception as e:
            logger.error(f"Error subscribing to NEAR events: {e}")
            return False
    
    async def _event_polling_loop(self):
        """Poll for events using NEAR RPC"""
        last_block = await self.get_latest_block()
        
        while self._connected:
            try:
                current_block = await self.get_latest_block()
                
                # Query transactions in new blocks
                for block_height in range(last_block + 1, current_block + 1):
                    block = await self.connection.provider.block({'block_id': block_height})
                    
                    # Parse transactions for events
                    for chunk in block.get('chunks', []):
                        chunk_data = await self.connection.provider.chunk({'chunk_id': chunk['chunk_hash']})
                        
                        for tx in chunk_data.get('transactions', []):
                            events = await self._parse_transaction_events(tx)
                            
                            for event in events:
                                key = f"{event['contract']}:{event['event_type']}"
                                self.emit_event(key, event)
                
                last_block = current_block
                await asyncio.sleep(self.config.get('poll_interval', 2))
                
            except Exception as e:
                logger.error(f"Error in NEAR event polling: {e}")
                await asyncio.sleep(5)
    
    async def _parse_transaction_events(self, tx: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse events from NEAR transaction"""
        events = []
        
        # Get transaction outcome
        if 'outcome' in tx:
            for log in tx['outcome'].get('logs', []):
                # NEAR events are typically JSON in logs
                try:
                    if log.startswith('EVENT_JSON:'):
                        event_data = json.loads(log[11:])  # Skip 'EVENT_JSON:'
                        
                        events.append({
                            'event_type': event_data.get('event'),
                            'contract': tx['receiver_id'],
                            'data': event_data.get('data'),
                            'tx_hash': tx['hash'],
                            'block_height': tx['block_height'],
                            'chainType': self.chain_type.value,
                            'chainId': self.chain_id
                        })
                except:
                    pass
        
        return events
    
    async def get_latest_block(self) -> int:
        """Get latest block height"""
        status = await self.connection.provider.status()
        return status['sync_info']['latest_block_height']
    
    async def get_reputation_balance(self, address: str) -> int:
        """Get reputation from NEAR contract"""
        if 'reputation_contract' not in self.config:
            return 0
        
        contract = self.contracts.get(self.config['reputation_contract'])
        if not contract:
            return 0
        
        try:
            # Call view method
            result = await contract.view_function(
                'get_reputation',
                {'account_id': address}
            )
            
            return result.get('reputation', 0)
            
        except Exception as e:
            logger.error(f"Error getting NEAR reputation: {e}")
            return 0
    
    async def submit_proposal(self, proposal_data: Dict[str, Any]) -> str:
        """Submit proposal to NEAR governance contract"""
        if not self.account:
            raise Exception("Account not initialized")
        
        governance_contract = self.contracts.get(self.config['governance_contract'])
        if not governance_contract:
            raise Exception("Governance contract not loaded")
        
        try:
            # Call contract method
            result = await governance_contract.function_call(
                'create_proposal',
                {
                    'title': proposal_data['title'],
                    'description': proposal_data['description'],
                    'kind': {
                        'type': proposal_data.get('proposal_type', 'Vote'),
                        'actions': proposal_data.get('actions', [])
                    }
                },
                gas=self.config.get('gas_limit', 300000000000000),  # 300 TGas
                amount=self.config.get('proposal_bond', 1000000000000000000000000)  # 1 NEAR
            )
            
            # Extract proposal ID from result
            if result.logs:
                for log in result.logs:
                    if 'proposal_id' in log:
                        return json.loads(log)['proposal_id']
            
            return result.transaction.hash
            
        except Exception as e:
            logger.error(f"Error submitting NEAR proposal: {e}")
            raise
    
    async def cast_vote(self, proposal_id: str, support: bool,
                       voter_address: str, signature: Optional[str] = None) -> str:
        """Cast vote on NEAR proposal"""
        if not self.account:
            raise Exception("Account not initialized")
        
        governance_contract = self.contracts.get(self.config['governance_contract'])
        if not governance_contract:
            raise Exception("Governance contract not loaded")
        
        try:
            # NEAR voting
            vote_type = 'VoteApprove' if support else 'VoteReject'
            
            result = await governance_contract.function_call(
                'act_proposal',
                {
                    'id': int(proposal_id),
                    'action': vote_type
                },
                gas=self.config.get('gas_limit', 200000000000000)  # 200 TGas
            )
            
            return result.transaction.hash
            
        except Exception as e:
            logger.error(f"Error casting NEAR vote: {e}")
            raise
    
    async def get_proposal_state(self, proposal_id: str) -> Dict[str, Any]:
        """Get NEAR proposal state"""
        governance_contract = self.contracts.get(self.config['governance_contract'])
        if not governance_contract:
            raise Exception("Governance contract not loaded")
        
        try:
            # View proposal
            proposal = await governance_contract.view_function(
                'get_proposal',
                {'id': int(proposal_id)}
            )
            
            # Get vote counts
            votes = await governance_contract.view_function(
                'get_proposal_votes',
                {'id': int(proposal_id)}
            )
            
            return {
                'proposalId': proposal_id,
                'state': proposal.get('status', 'unknown'),
                'forVotes': str(votes.get('yes', 0)),
                'againstVotes': str(votes.get('no', 0)),
                'abstainVotes': '0',  # NEAR typically doesn't have abstain
                'startTime': proposal.get('submission_time'),
                'endTime': proposal.get('voting_period_end')
            }
            
        except Exception as e:
            logger.error(f"Error getting NEAR proposal state: {e}")
            raise
    
    async def get_voting_power(self, address: str, block_number: Optional[int] = None) -> int:
        """Get voting power from governance token or staking"""
        try:
            if 'voting_token_contract' in self.config:
                # Token-based voting
                token_contract = self.contracts.get(self.config['voting_token_contract'])
                if token_contract:
                    balance = await token_contract.view_function(
                        'ft_balance_of',
                        {'account_id': address}
                    )
                    return int(balance)
            
            # Default to account balance
            account_info = await self.connection.provider.query({
                'request_type': 'view_account',
                'account_id': address,
                'finality': 'final'
            })
            
            # Convert yoctoNEAR to NEAR (simplified)
            return int(account_info.get('amount', 0)) // 10**24
            
        except:
            return 0
    
    async def execute_proposal(self, proposal_id: str) -> str:
        """Execute a passed NEAR proposal"""
        if not self.account:
            raise Exception("Account not initialized")
        
        governance_contract = self.contracts.get(self.config['governance_contract'])
        if not governance_contract:
            raise Exception("Governance contract not loaded")
        
        try:
            result = await governance_contract.function_call(
                'finalize',
                {'id': int(proposal_id)},
                gas=self.config.get('gas_limit', 300000000000000)  # 300 TGas
            )
            
            return result.transaction.hash
            
        except Exception as e:
            logger.error(f"Error executing NEAR proposal: {e}")
            raise
    
    def validate_address(self, address: str) -> bool:
        """Validate NEAR account ID"""
        try:
            # NEAR account IDs are lowercase, alphanumeric + - _ .
            # Must be 2-64 chars, can't start/end with special chars
            import re
            pattern = r'^[a-z0-9]([\-\_\.a-z0-9]){1,62}[a-z0-9]$'
            return bool(re.match(pattern, address))
        except:
            return False
    
    def format_address(self, address: str) -> str:
        """Format NEAR account ID"""
        return address.lower() 

    async def mint_asset_nft(self, to: str, uri: str, royalty_recipient: str, royalty_fraction: int) -> str:
        """Mints NFT on NEAR"""
        contract_id = self.config.get("platform_asset_contract", "platform-asset.near")
        
        # Create transaction
        tx = await self.account.function_call(
            contract_id,
            "mint",
            {
                "receiver_id": to,
                "metadata": {
                    "media": uri,
                    "copies": 1
                },
                "royalty": {
                    royalty_recipient: royalty_fraction
                }
            },
            gas=100000000000000,  # 100 TGas
            amount=10000000000000000000000  # 0.01 NEAR for storage
        )
        
        return tx.transaction.hash

    async def create_license_offer(self, asset_id: str, price: int, duration: int, license_type: str, max_usage: int, royalty_percentage: int) -> str:
        """Creates license offer on NEAR"""
        contract_id = self.config.get("usage_license_contract", "usage-license.near")
        
        tx = await self.account.function_call(
            contract_id,
            "create_license_offer",
            {
                "asset_id": asset_id,
                "price": str(price),
                "duration": duration,
                "license_type": license_type,
                "max_usage": max_usage,
                "royalty_percentage": royalty_percentage
            },
            gas=50000000000000  # 50 TGas
        )
        
        return tx.transaction.hash

    async def purchase_license(self, asset_id: str, offer_index: int, license_type: int) -> str:
        """Purchase license on NEAR"""
        contract_id = self.config.get("usage_license_contract", "usage-license.near")
        
        # Get offer details to know the price
        offer = await self.connection.view_function(
            contract_id,
            "get_offer",
            {"asset_id": asset_id, "index": offer_index}
        )
        
        tx = await self.account.function_call(
            contract_id,
            "purchase_license",
            {
                "asset_id": asset_id,
                "offer_index": offer_index,
                "license_type": license_type
            },
            gas=100000000000000,  # 100 TGas
            amount=int(offer["price"])  # Attach the price
        )
        
        return tx.transaction.hash

    async def distribute_royalty(self, token_id: int, sale_price: int) -> str:
        """Distribute royalty on NEAR"""
        contract_id = self.config.get("royalty_distributor_contract", "royalty-distributor.near")
        
        tx = await self.account.function_call(
            contract_id,
            "distribute_royalty",
            {
                "token_id": str(token_id),
                "sale_price": str(sale_price)
            },
            gas=50000000000000,  # 50 TGas
            amount=sale_price  # Attach the royalty amount
        )
        
        return tx.transaction.hash 