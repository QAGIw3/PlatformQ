"""
Compute Resource Tokenizer

Tokenizes compute hours into tradeable ERC-20 tokens with fractional ownership.
Enables DeFi primitives like AMMs, lending, and yield farming for compute resources.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from decimal import Decimal
import hashlib
import json
import logging

from web3 import Web3
from eth_account import Account
import httpx
from pyignite import Client as IgniteClient

from platformq_shared.event_publisher import EventPublisher
from ..models import ComputeOffering, ComputeTokenHolder, ComputeLiquidityPosition

logger = logging.getLogger(__name__)


class ComputeTokenStandard:
    """ERC-20 compatible token standard for compute hours"""
    
    # Token metadata
    name: str = "Compute Hour Token"
    symbol: str = "CHT"
    decimals: int = 18  # Standard ERC-20 decimals
    
    @staticmethod
    def hours_to_tokens(hours: float) -> int:
        """Convert compute hours to token units"""
        return int(hours * (10 ** ComputeTokenStandard.decimals))
    
    @staticmethod
    def tokens_to_hours(tokens: int) -> float:
        """Convert token units to compute hours"""
        return tokens / (10 ** ComputeTokenStandard.decimals)


class ComputeTokenizer:
    """
    Tokenizes compute resources into fractional ownership tokens.
    Enables DeFi primitives for compute marketplace.
    """
    
    def __init__(
        self,
        web3_provider: str,
        contract_addresses: Dict[str, str],
        ignite_client: IgniteClient,
        event_publisher: EventPublisher,
        vc_service_url: str,
        private_key: Optional[str] = None
    ):
        self.w3 = Web3(Web3.HTTPProvider(web3_provider))
        self.contracts = contract_addresses
        self.ignite = ignite_client
        self.event_publisher = event_publisher
        self.vc_service_url = vc_service_url
        
        # Initialize account if private key provided
        if private_key:
            self.account = Account.from_key(private_key)
        else:
            self.account = None
            
        # Contract ABIs (simplified for example)
        self.token_factory_abi = self._load_abi("ComputeTokenFactory")
        self.compute_token_abi = self._load_abi("ComputeToken")
        self.amm_router_abi = self._load_abi("ComputeAMMRouter")
        self.lending_pool_abi = self._load_abi("ComputeLendingPool")
        
        # HTTP client for external services
        self.http_client = httpx.AsyncClient()
    
    async def tokenize_compute_offering(
        self,
        offering: ComputeOffering,
        total_hours: int,
        min_fraction_hours: float = 0.1
    ) -> Dict[str, Any]:
        """
        Tokenize a compute offering into fractional ERC-20 tokens.
        
        Args:
            offering: The compute offering to tokenize
            total_hours: Total hours to tokenize
            min_fraction_hours: Minimum fractional unit (default 0.1 hour)
            
        Returns:
            Token deployment details
        """
        try:
            # Generate token metadata
            token_name = f"{offering.provider_id} {offering.resource_type.value.upper()} Hours"
            token_symbol = self._generate_token_symbol(offering)
            
            # Convert hours to token units
            total_supply = ComputeTokenStandard.hours_to_tokens(total_hours)
            min_fraction = ComputeTokenStandard.hours_to_tokens(min_fraction_hours)
            
            # Deploy token contract
            factory_contract = self.w3.eth.contract(
                address=self.contracts["token_factory"],
                abi=self.token_factory_abi
            )
            
            # Build transaction
            tx_data = factory_contract.functions.createComputeToken(
                token_name,
                token_symbol,
                total_supply,
                min_fraction,
                offering.offering_id,
                self._encode_resource_specs(offering.resource_specs)
            ).build_transaction({
                'from': self.account.address,
                'gas': 3000000,
                'gasPrice': self.w3.eth.gas_price,
                'nonce': self.w3.eth.get_transaction_count(self.account.address)
            })
            
            # Sign and send transaction
            signed_tx = self.w3.eth.account.sign_transaction(tx_data, self.account.key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            # Wait for confirmation
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            
            # Extract token address from events
            token_address = self._extract_token_address(receipt)
            
            # Update offering in database
            await self._update_offering_tokenization(
                offering,
                token_address,
                total_supply,
                min_fraction
            )
            
            # Issue verifiable credential for tokenization
            vc = await self._issue_tokenization_credential(
                offering,
                token_address,
                total_hours
            )
            
            # Emit tokenization event
            await self.event_publisher.publish_event({
                "event_type": "compute_tokenized",
                "offering_id": offering.offering_id,
                "token_address": token_address,
                "token_name": token_name,
                "token_symbol": token_symbol,
                "total_supply": str(total_supply),
                "total_hours": total_hours,
                "credential_id": vc["id"],
                "timestamp": datetime.utcnow().isoformat()
            }, "persistent://public/default/compute-tokenization-events")
            
            return {
                "success": True,
                "token_address": token_address,
                "token_name": token_name,
                "token_symbol": token_symbol,
                "total_supply": str(total_supply),
                "tx_hash": tx_hash.hex(),
                "credential": vc
            }
            
        except Exception as e:
            logger.error(f"Failed to tokenize compute offering: {e}")
            raise
    
    async def create_liquidity_pool(
        self,
        token_address: str,
        paired_token: str,  # USDC, ETH, etc.
        initial_compute_hours: float,
        initial_paired_amount: float
    ) -> Dict[str, Any]:
        """
        Create AMM liquidity pool for compute tokens.
        
        Args:
            token_address: Address of compute token
            paired_token: Address of paired token (USDC, ETH, etc.)
            initial_compute_hours: Initial compute hours to add
            initial_paired_amount: Initial paired token amount
            
        Returns:
            Pool creation details
        """
        try:
            # Get AMM router contract
            router_contract = self.w3.eth.contract(
                address=self.contracts["amm_router"],
                abi=self.amm_router_abi
            )
            
            # Convert hours to tokens
            compute_tokens = ComputeTokenStandard.hours_to_tokens(initial_compute_hours)
            paired_tokens = self._normalize_token_amount(paired_token, initial_paired_amount)
            
            # Approve router to spend tokens
            await self._approve_token_spending(
                token_address,
                self.contracts["amm_router"],
                compute_tokens
            )
            
            await self._approve_token_spending(
                paired_token,
                self.contracts["amm_router"],
                paired_tokens
            )
            
            # Add liquidity
            tx_data = router_contract.functions.addLiquidity(
                token_address,  # Token A
                paired_token,   # Token B
                compute_tokens, # Amount A desired
                paired_tokens,  # Amount B desired
                int(compute_tokens * 0.95),  # Amount A min (5% slippage)
                int(paired_tokens * 0.95),   # Amount B min
                self.account.address,         # Recipient
                int((datetime.utcnow() + timedelta(minutes=15)).timestamp())  # Deadline
            ).build_transaction({
                'from': self.account.address,
                'gas': 500000,
                'gasPrice': self.w3.eth.gas_price,
                'nonce': self.w3.eth.get_transaction_count(self.account.address)
            })
            
            # Sign and send
            signed_tx = self.w3.eth.account.sign_transaction(tx_data, self.account.key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            
            # Extract pool address and LP tokens received
            pool_address, lp_tokens = self._extract_pool_details(receipt)
            
            # Save liquidity position
            await self._save_liquidity_position(
                pool_address,
                token_address,
                paired_token,
                compute_tokens,
                paired_tokens,
                lp_tokens
            )
            
            # Calculate initial price
            initial_price = paired_tokens / compute_tokens
            
            # Emit pool creation event
            await self.event_publisher.publish_event({
                "event_type": "compute_pool_created",
                "token_address": token_address,
                "paired_token": paired_token,
                "pool_address": pool_address,
                "initial_compute_hours": initial_compute_hours,
                "initial_paired_amount": initial_paired_amount,
                "initial_price": initial_price,
                "lp_tokens": str(lp_tokens),
                "timestamp": datetime.utcnow().isoformat()
            }, "persistent://public/default/compute-amm-events")
            
            return {
                "success": True,
                "pool_address": pool_address,
                "lp_tokens": str(lp_tokens),
                "initial_price": initial_price,
                "tx_hash": tx_hash.hex()
            }
            
        except Exception as e:
            logger.error(f"Failed to create liquidity pool: {e}")
            raise
    
    async def enable_lending(
        self,
        token_address: str,
        collateral_factor: float = 0.75,
        base_borrow_rate: float = 0.02,  # 2% APR
        utilization_rate_slope: float = 0.1
    ) -> Dict[str, Any]:
        """
        Enable lending/borrowing for compute tokens.
        
        Args:
            token_address: Address of compute token
            collateral_factor: Max borrow value as percentage of collateral
            base_borrow_rate: Base annual borrow rate
            utilization_rate_slope: Rate increase per utilization percentage
            
        Returns:
            Lending pool details
        """
        try:
            # Deploy or get lending pool
            lending_pool = self.w3.eth.contract(
                address=self.contracts["lending_pool"],
                abi=self.lending_pool_abi
            )
            
            # Add compute token as supported asset
            tx_data = lending_pool.functions.addMarket(
                token_address,
                int(collateral_factor * 10000),  # Basis points
                int(base_borrow_rate * 10000),
                int(utilization_rate_slope * 10000),
                self.contracts["price_oracle"]  # Oracle for compute token pricing
            ).build_transaction({
                'from': self.account.address,
                'gas': 200000,
                'gasPrice': self.w3.eth.gas_price,
                'nonce': self.w3.eth.get_transaction_count(self.account.address)
            })
            
            # Execute transaction
            signed_tx = self.w3.eth.account.sign_transaction(tx_data, self.account.key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            
            # Emit lending enabled event
            await self.event_publisher.publish_event({
                "event_type": "compute_lending_enabled",
                "token_address": token_address,
                "collateral_factor": collateral_factor,
                "base_borrow_rate": base_borrow_rate,
                "utilization_slope": utilization_rate_slope,
                "timestamp": datetime.utcnow().isoformat()
            }, "persistent://public/default/compute-lending-events")
            
            return {
                "success": True,
                "lending_pool": self.contracts["lending_pool"],
                "tx_hash": tx_hash.hex()
            }
            
        except Exception as e:
            logger.error(f"Failed to enable lending: {e}")
            raise
    
    async def create_yield_strategy(
        self,
        token_addresses: List[str],
        strategy_type: str,  # "liquidity_mining", "staking", "auto_compound"
        reward_token: str,
        rewards_per_hour: float,
        lock_period_hours: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Create yield generation strategy for compute tokens.
        
        Args:
            token_addresses: List of compute tokens to include
            strategy_type: Type of yield strategy
            reward_token: Token used for rewards
            rewards_per_hour: Reward emission rate
            lock_period_hours: Optional lock period
            
        Returns:
            Strategy deployment details
        """
        # Implementation would deploy yield farming contracts
        # This is simplified for the example
        
        strategy_id = hashlib.sha256(
            f"{'-'.join(token_addresses)}-{strategy_type}-{datetime.utcnow()}".encode()
        ).hexdigest()[:16]
        
        # Calculate APY based on rewards and typical deposit size
        estimated_apy = self._calculate_strategy_apy(
            rewards_per_hour,
            strategy_type,
            lock_period_hours
        )
        
        # Save strategy to Ignite
        strategy_data = {
            "strategy_id": strategy_id,
            "strategy_type": strategy_type,
            "token_addresses": token_addresses,
            "reward_token": reward_token,
            "rewards_per_hour": rewards_per_hour,
            "lock_period_hours": lock_period_hours,
            "estimated_apy": estimated_apy,
            "created_at": datetime.utcnow().isoformat(),
            "total_staked": 0,
            "is_active": True
        }
        
        cache = self.ignite.get_cache("compute_yield_strategies")
        cache.put(strategy_id, strategy_data)
        
        # Emit strategy creation event
        await self.event_publisher.publish_event({
            "event_type": "yield_strategy_created",
            "strategy_id": strategy_id,
            "strategy_type": strategy_type,
            "estimated_apy": estimated_apy,
            "timestamp": datetime.utcnow().isoformat()
        }, "persistent://public/default/compute-yield-events")
        
        return {
            "success": True,
            "strategy_id": strategy_id,
            "estimated_apy": estimated_apy,
            "strategy_data": strategy_data
        }
    
    def _generate_token_symbol(self, offering: ComputeOffering) -> str:
        """Generate unique token symbol for offering"""
        # Take first letters of provider and resource type
        provider_prefix = offering.provider_id[:3].upper()
        resource_prefix = offering.resource_type.value[:3].upper()
        
        # Add unique suffix based on offering ID
        suffix = hashlib.sha256(offering.offering_id.encode()).hexdigest()[:4].upper()
        
        return f"{provider_prefix}{resource_prefix}{suffix}"
    
    def _encode_resource_specs(self, specs: Dict[str, Any]) -> bytes:
        """Encode resource specifications for on-chain storage"""
        # Simplified encoding - in production use more efficient encoding
        return json.dumps(specs, sort_keys=True).encode()
    
    def _extract_token_address(self, receipt) -> str:
        """Extract deployed token address from transaction receipt"""
        # Parse logs to find TokenCreated event
        # This is simplified - actual implementation would decode logs properly
        return receipt.logs[0]['address']
    
    async def _update_offering_tokenization(
        self,
        offering: ComputeOffering,
        token_address: str,
        total_supply: int,
        min_fraction: int
    ):
        """Update offering with tokenization details"""
        cache = self.ignite.get_cache("compute_offerings")
        
        offering_data = cache.get(offering.offering_id)
        offering_data.update({
            "is_tokenized": True,
            "token_address": token_address,
            "total_supply": total_supply,
            "circulating_supply": 0,
            "min_fraction_size": ComputeTokenStandard.tokens_to_hours(min_fraction),
            "tokenization_timestamp": datetime.utcnow().isoformat()
        })
        
        cache.put(offering.offering_id, offering_data)
    
    async def _issue_tokenization_credential(
        self,
        offering: ComputeOffering,
        token_address: str,
        total_hours: int
    ) -> Dict[str, Any]:
        """Issue verifiable credential for tokenization"""
        credential_request = {
            "type": ["VerifiableCredential", "ComputeTokenizationCredential"],
            "credentialSubject": {
                "offering_id": offering.offering_id,
                "provider_id": offering.provider_id,
                "token_address": token_address,
                "total_hours": total_hours,
                "resource_type": offering.resource_type.value,
                "resource_specs": offering.resource_specs,
                "tokenization_date": datetime.utcnow().isoformat()
            },
            "expirationDate": (datetime.utcnow() + timedelta(days=365)).isoformat()
        }
        
        response = await self.http_client.post(
            f"{self.vc_service_url}/api/v1/credentials/issue",
            json=credential_request
        )
        response.raise_for_status()
        
        return response.json()
    
    def _load_abi(self, contract_name: str) -> List[Dict]:
        """Load contract ABI"""
        # In production, load from compiled contracts
        # This is simplified example ABI
        if contract_name == "ComputeTokenFactory":
            return [{
                "name": "createComputeToken",
                "type": "function",
                "inputs": [
                    {"name": "name", "type": "string"},
                    {"name": "symbol", "type": "string"},
                    {"name": "totalSupply", "type": "uint256"},
                    {"name": "minFraction", "type": "uint256"},
                    {"name": "offeringId", "type": "string"},
                    {"name": "resourceSpecs", "type": "bytes"}
                ],
                "outputs": [{"name": "tokenAddress", "type": "address"}]
            }]
        # Add other ABIs as needed
        return []
    
    async def _approve_token_spending(
        self,
        token_address: str,
        spender: str,
        amount: int
    ):
        """Approve token spending"""
        token_contract = self.w3.eth.contract(
            address=token_address,
            abi=self.compute_token_abi
        )
        
        tx_data = token_contract.functions.approve(
            spender,
            amount
        ).build_transaction({
            'from': self.account.address,
            'gas': 100000,
            'gasPrice': self.w3.eth.gas_price,
            'nonce': self.w3.eth.get_transaction_count(self.account.address)
        })
        
        signed_tx = self.w3.eth.account.sign_transaction(tx_data, self.account.key)
        tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        self.w3.eth.wait_for_transaction_receipt(tx_hash)
    
    def _normalize_token_amount(self, token_address: str, amount: float) -> int:
        """Normalize token amount based on decimals"""
        # For simplicity, assume 18 decimals for all tokens
        # In production, query token contract for decimals
        return int(amount * (10 ** 18))
    
    def _extract_pool_details(self, receipt) -> Tuple[str, int]:
        """Extract pool address and LP tokens from receipt"""
        # Simplified - actual implementation would decode events
        return receipt.logs[0]['address'], int(receipt.logs[1]['data'], 16)
    
    async def _save_liquidity_position(
        self,
        pool_address: str,
        token_a: str,
        token_b: str,
        amount_a: int,
        amount_b: int,
        lp_tokens: int
    ):
        """Save liquidity position to database"""
        position_data = {
            "position_id": f"lp_{pool_address}_{self.account.address}",
            "pool_address": pool_address,
            "wallet_address": self.account.address,
            "token_a": token_a,
            "token_b": token_b,
            "amount_a": str(amount_a),
            "amount_b": str(amount_b),
            "lp_tokens": str(lp_tokens),
            "created_at": datetime.utcnow().isoformat()
        }
        
        cache = self.ignite.get_cache("compute_liquidity_positions")
        cache.put(position_data["position_id"], position_data)
    
    def _calculate_strategy_apy(
        self,
        rewards_per_hour: float,
        strategy_type: str,
        lock_period_hours: Optional[int]
    ) -> float:
        """Calculate estimated APY for yield strategy"""
        base_apy = rewards_per_hour * 24 * 365  # Annualized
        
        # Apply multipliers based on strategy type
        if strategy_type == "liquidity_mining":
            base_apy *= 1.5  # Higher rewards for providing liquidity
        elif strategy_type == "staking" and lock_period_hours:
            # Higher APY for longer lock periods
            lock_multiplier = 1 + (lock_period_hours / (24 * 30))  # Bonus per month
            base_apy *= lock_multiplier
        
        return min(base_apy, 200.0)  # Cap at 200% APY for safety
    
    async def close(self):
        """Cleanup resources"""
        await self.http_client.aclose()