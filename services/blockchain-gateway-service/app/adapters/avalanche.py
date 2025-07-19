"""
Avalanche chain adapter
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Callable
from web3 import Web3
from web3.contract import Contract
from eth_account import Account

from .ethereum import EthereumAdapter
from .base import ChainType

logger = logging.getLogger(__name__)


class AvalancheAdapter(EthereumAdapter):
    """
    Adapter for Avalanche C-Chain (EVM compatible)
    Extends EthereumAdapter with Avalanche-specific optimizations
    """
    
    def __init__(self, chain_type: ChainType, chain_id: str, config: Dict[str, Any]):
        # Set Avalanche-specific defaults
        if 'gas_price_multiplier' not in config:
            config['gas_price_multiplier'] = 1.1  # Avalanche has dynamic fees
        
        if 'block_time' not in config:
            config['block_time'] = 2  # ~2 second blocks
        
        super().__init__(chain_type, chain_id, config)
        self.subnet_id = config.get('subnet_id', 'C')  # C-Chain by default
        
    async def connect(self) -> bool:
        """Connect to Avalanche network with specific configurations"""
        connected = await super().connect()
        
        if connected:
            # Additional Avalanche-specific setup
            try:
                # Check if connected to correct network
                chain_id = await self.w3.eth.chain_id
                if str(chain_id) != self.chain_id:
                    logger.warning(f"Connected to chain {chain_id}, expected {self.chain_id}")
                
                # Get Avalanche-specific network info
                if hasattr(self.w3.provider, 'make_request'):
                    network_info = self.w3.provider.make_request('info.getNetworkID', [])
                    logger.info(f"Connected to Avalanche network: {network_info}")
                
            except Exception as e:
                logger.error(f"Error during Avalanche-specific setup: {e}")
        
        return connected
    
    async def submit_proposal(self, proposal_data: Dict[str, Any]) -> str:
        """
        Submit proposal with Avalanche-optimized gas settings
        """
        # Avalanche C-Chain has different gas dynamics
        if 'gas' not in proposal_data:
            # Higher default gas limit for Avalanche
            proposal_data['gas'] = 600000
        
        # Use dynamic fee calculation for Avalanche
        if 'maxFeePerGas' not in proposal_data and 'maxPriorityFeePerGas' not in proposal_data:
            base_fee = self.w3.eth.gas_price
            priority_fee = self.w3.to_wei(25, 'gwei')  # Avalanche typically uses 25 nAVAX
            
            proposal_data['maxPriorityFeePerGas'] = priority_fee
            proposal_data['maxFeePerGas'] = base_fee + priority_fee
        
        return await super().submit_proposal(proposal_data)
    
    async def _event_monitoring_loop(self):
        """
        Enhanced event monitoring for Avalanche's faster block time
        """
        while self._connected:
            try:
                for filter_key, event_filter in self.event_filters.items():
                    new_events = event_filter.get_new_entries()
                    
                    for event in new_events:
                        event_data = self._process_event(event)
                        # Add Avalanche-specific metadata
                        event_data['subnet'] = self.subnet_id
                        event_data['finality'] = 'instant'  # Avalanche has instant finality
                        
                        self.emit_event(filter_key, event_data)
                
                # Faster polling for Avalanche's quick blocks
                await asyncio.sleep(self.config.get('poll_interval', 1))
                
            except Exception as e:
                logger.error(f"Error in Avalanche event monitoring: {e}")
                await asyncio.sleep(3)
    
    async def get_subnet_info(self) -> Dict[str, Any]:
        """Get information about Avalanche subnet"""
        try:
            if hasattr(self.w3.provider, 'make_request'):
                # Get subnet info via Avalanche-specific RPC
                subnet_info = self.w3.provider.make_request(
                    'platform.getSubnets',
                    [{'ids': [self.subnet_id]}]
                )
                return subnet_info
        except Exception as e:
            logger.error(f"Error getting subnet info: {e}")
        
        return {}
    
    async def get_validator_set(self) -> List[Dict[str, Any]]:
        """Get current validator set for the subnet"""
        try:
            if hasattr(self.w3.provider, 'make_request'):
                validators = self.w3.provider.make_request(
                    'platform.getCurrentValidators',
                    [{'subnetID': self.subnet_id}]
                )
                return validators.get('validators', [])
        except Exception as e:
            logger.error(f"Error getting validators: {e}")
        
        return []
    
    async def estimate_cross_subnet_fee(self, target_subnet: str) -> int:
        """Estimate fee for cross-subnet transfers"""
        try:
            # Base fee for cross-subnet communication
            base_fee = self.w3.to_wei(0.01, 'ether')  # 0.01 AVAX
            
            # Additional fee based on network congestion
            gas_price = await self.w3.eth.gas_price
            congestion_multiplier = min(gas_price / self.w3.to_wei(25, 'gwei'), 3)
            
            return int(base_fee * congestion_multiplier)
            
        except Exception as e:
            logger.error(f"Error estimating cross-subnet fee: {e}")
            return self.w3.to_wei(0.01, 'ether')
    
    async def get_voting_power(self, address: str, block_number: Optional[int] = None) -> int:
        """
        Get voting power including staked AVAX
        """
        # First get standard ERC20/governor voting power
        base_power = await super().get_voting_power(address, block_number)
        
        # Add staked AVAX if validator
        try:
            if hasattr(self.w3.provider, 'make_request'):
                # Check if address is a validator
                stake_info = self.w3.provider.make_request(
                    'platform.getStake',
                    [{'addresses': [address]}]
                )
                
                staked_amount = stake_info.get('stakedAVAX', 0)
                # Add staked amount to voting power
                base_power += staked_amount
                
        except Exception as e:
            logger.error(f"Error getting staked AVAX: {e}")
        
        return base_power
    
    def validate_address(self, address: str) -> bool:
        """Validate Avalanche C-Chain address (same as Ethereum)"""
        return super().validate_address(address)
    
    async def create_subnet_proposal(self, proposal_data: Dict[str, Any]) -> str:
        """
        Create a proposal for subnet governance
        Avalanche-specific feature for subnet DAOs
        """
        if 'subnet_governance_contract' not in self.config:
            raise Exception("Subnet governance contract not configured")
        
        subnet_gov = self.contracts.get(self.config['subnet_governance_contract'])
        if not subnet_gov:
            raise Exception("Subnet governance contract not loaded")
        
        try:
            # Build subnet-specific proposal
            tx = subnet_gov.functions.proposeSubnetChange(
                proposal_data['subnet_id'],
                proposal_data['change_type'],  # e.g., 'ADD_VALIDATOR', 'MODIFY_PARAMS'
                proposal_data['params']
            ).build_transaction({
                'from': proposal_data['proposer'],
                'gas': 800000,  # Higher gas for subnet operations
                'maxPriorityFeePerGas': self.w3.to_wei(25, 'gwei'),
                'maxFeePerGas': self.w3.eth.gas_price + self.w3.to_wei(25, 'gwei'),
                'nonce': self.w3.eth.get_transaction_count(proposal_data['proposer'])
            })
            
            # Sign and send
            if 'private_key' in proposal_data:
                signed_tx = self.w3.eth.account.sign_transaction(
                    tx, proposal_data['private_key']
                )
                tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            else:
                tx_hash = self.w3.eth.send_transaction(tx)
            
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            
            # Extract proposal ID
            proposal_created_event = subnet_gov.events.SubnetProposalCreated().process_receipt(receipt)
            if proposal_created_event:
                return str(proposal_created_event[0]['args']['proposalId'])
            
            return tx_hash.hex()
            
        except Exception as e:
            logger.error(f"Error creating subnet proposal: {e}")
            raise 

    async def mint_asset_nft(self, to: str, uri: str, royalty_recipient: str, royalty_fraction: int) -> str:
        """Mints a new asset NFT on Avalanche"""
        if royalty_fraction < 0 or royalty_fraction > 10000:
            raise ValueError("Royalty fraction must be between 0 and 10000")
        if not Web3.is_address(to) or not Web3.is_address(royalty_recipient):
            raise ValueError("Invalid address")
            
        contract = self.contracts.get("PlatformAsset")
        if not contract:
            raise Exception("PlatformAsset contract not loaded")
        
        tx_hash = await self._send_transaction(
            contract,
            "safeMint",
            to,
            uri,
            royalty_recipient,
            royalty_fraction
        )
        return tx_hash

    async def create_license_offer(self, asset_id: str, price: int, duration: int, license_type: str, max_usage: int, royalty_percentage: int) -> str:
        """Creates a new license offer for an asset on Avalanche"""
        contract = self.contracts.get("UsageLicense")
        if not contract:
            raise Exception("UsageLicense contract not loaded")

        tx_hash = await self._send_transaction(
            contract,
            "createLicenseOffer",
            asset_id,
            price,
            duration,
            license_type,
            max_usage,
            royalty_percentage
        )
        return tx_hash

    async def purchase_license(self, asset_id: str, offer_index: int, license_type: int) -> str:
        """Purchases a license for an asset on Avalanche"""
        contract = self.contracts.get("UsageLicense")
        if not contract:
            raise Exception("UsageLicense contract not loaded")

        tx_hash = await self._send_transaction(
            contract,
            "purchaseLicense",
            asset_id,
            offer_index,
            license_type
        )
        return tx_hash

    async def distribute_royalty(self, token_id: int, sale_price: int) -> str:
        """Distributes royalties for a sale on Avalanche"""
        contract = self.contracts.get("RoyaltyDistributor")
        if not contract:
            raise Exception("RoyaltyDistributor contract not loaded")

        tx_hash = await self._send_transaction(
            contract,
            "distributeRoyalty",
            token_id,
            sale_price
        )
        return tx_hash
        
    async def _send_transaction(self, contract: Contract, function_name: str, *args, **kwargs) -> str:
        """Helper method to send a transaction to a contract function"""
        if not self.account:
            raise Exception("No account set up for transactions")
        
        nonce = self.w3.eth.get_transaction_count(self.account.address)
        tx = contract.functions[function_name](*args).build_transaction({
            'from': self.account.address,
            'nonce': nonce,
            'gas': 2000000,
            'gasPrice': self.w3.to_wei('25', 'gwei'),  # Avalanche typical gas price
            **kwargs
        })
        signed_tx = self.w3.eth.account.sign_transaction(tx, self.account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        return tx_hash.hex() 