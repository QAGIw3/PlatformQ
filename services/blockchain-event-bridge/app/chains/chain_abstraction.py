"""
Chain Abstraction Layer

Provides unified interface for multi-chain operations
"""

import logging
from typing import Dict, Any, List, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum
from abc import ABC, abstractmethod
import asyncio
from decimal import Decimal
from web3 import Web3
import json
from datetime import datetime

from .base import ChainInterface, ChainConfig

logger = logging.getLogger(__name__)


class TransactionType(Enum):
    TRANSFER = "transfer"
    APPROVE = "approve"
    MINT = "mint"
    BURN = "burn"
    SWAP = "swap"
    STAKE = "stake"
    CONTRACT_CALL = "contract_call"


class GasStrategy(Enum):
    SLOW = "slow"  # Save gas, lower priority
    STANDARD = "standard"  # Normal priority
    FAST = "fast"  # High priority
    INSTANT = "instant"  # Maximum priority


@dataclass
class UnifiedTransaction:
    """Chain-agnostic transaction representation"""
    tx_type: TransactionType
    from_address: str
    to_address: str
    value: Decimal
    data: Optional[Dict[str, Any]] = None
    gas_strategy: GasStrategy = GasStrategy.STANDARD
    nonce: Optional[int] = None
    metadata: Dict[str, Any] = None


@dataclass
class UnifiedBalance:
    """Unified balance representation across chains"""
    address: str
    chain: str
    native_balance: Decimal
    token_balances: Dict[str, Decimal]  # token_address -> balance
    usd_value: Optional[Decimal] = None
    last_updated: Optional[str] = None


@dataclass
class UnifiedTokenInfo:
    """Unified token information"""
    symbol: str
    name: str
    decimals: int
    total_supply: Optional[Decimal] = None
    addresses: Dict[str, str] = None  # chain -> address mapping
    is_native: bool = False


class ChainAbstraction:
    """Provides unified interface for multi-chain operations"""
    
    def __init__(self):
        self.chains: Dict[str, ChainInterface] = {}
        self.token_registry: Dict[str, UnifiedTokenInfo] = {}
        self.gas_oracles: Dict[str, Any] = {}
        self._price_cache: Dict[str, Dict[str, Decimal]] = {}
        self._monitoring_task: Optional[asyncio.Task] = None
        
        # Initialize common tokens
        self._initialize_token_registry()
        
    def _initialize_token_registry(self):
        """Initialize registry with common tokens"""
        # USDC
        self.token_registry["USDC"] = UnifiedTokenInfo(
            symbol="USDC",
            name="USD Coin",
            decimals=6,
            addresses={
                "ethereum": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                "polygon": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
                "bsc": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
                "avalanche": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
                "arbitrum": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
                "optimism": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
                "fantom": "0x04068DA6C83AFCFA0e13ba15A6696662335D5B75"
            }
        )
        
        # WETH
        self.token_registry["WETH"] = UnifiedTokenInfo(
            symbol="WETH",
            name="Wrapped Ether",
            decimals=18,
            addresses={
                "ethereum": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                "polygon": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
                "bsc": "0x2170Ed0880ac9A755fd29B2688956BD959F933F8",
                "avalanche": "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
                "arbitrum": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                "optimism": "0x4200000000000000000000000000000000000006",
                "fantom": "0x74b23882a30290451A17c44f4F05243b6b58C76d"
            }
        )
        
        # Native tokens
        self.token_registry["ETH"] = UnifiedTokenInfo(
            symbol="ETH",
            name="Ethereum",
            decimals=18,
            is_native=True
        )
        
        self.token_registry["MATIC"] = UnifiedTokenInfo(
            symbol="MATIC",
            name="Polygon",
            decimals=18,
            is_native=True
        )
        
        self.token_registry["BNB"] = UnifiedTokenInfo(
            symbol="BNB",
            name="Binance Coin",
            decimals=18,
            is_native=True
        )
        
    async def add_chain(self, chain_name: str, chain_interface: ChainInterface):
        """Add a new chain to the abstraction layer"""
        self.chains[chain_name] = chain_interface
        logger.info(f"Added chain: {chain_name}")
        
        # Initialize gas oracle for chain
        await self._initialize_gas_oracle(chain_name)
        
    async def remove_chain(self, chain_name: str):
        """Remove a chain from the abstraction layer"""
        if chain_name in self.chains:
            del self.chains[chain_name]
            logger.info(f"Removed chain: {chain_name}")
            
    async def get_unified_balance(self, 
                                address: str, 
                                chains: Optional[List[str]] = None,
                                tokens: Optional[List[str]] = None) -> List[UnifiedBalance]:
        """Get unified balance across multiple chains"""
        if not chains:
            chains = list(self.chains.keys())
            
        balances = []
        
        for chain_name in chains:
            if chain_name not in self.chains:
                continue
                
            chain = self.chains[chain_name]
            
            try:
                # Get native balance
                native_balance = await chain.get_balance(address)
                
                # Get token balances
                token_balances = {}
                if tokens:
                    for token_symbol in tokens:
                        token_info = self.token_registry.get(token_symbol)
                        if token_info and chain_name in token_info.addresses:
                            token_address = token_info.addresses[chain_name]
                            balance = await chain.get_token_balance(address, token_address)
                            token_balances[token_symbol] = balance
                            
                # Calculate USD value
                usd_value = await self._calculate_usd_value(chain_name, native_balance, token_balances)
                
                unified_balance = UnifiedBalance(
                    address=address,
                    chain=chain_name,
                    native_balance=native_balance,
                    token_balances=token_balances,
                    usd_value=usd_value,
                    last_updated=datetime.utcnow().isoformat()
                )
                
                balances.append(unified_balance)
                
            except Exception as e:
                logger.error(f"Error getting balance for {chain_name}: {e}")
                
        return balances
        
    async def execute_transaction(self,
                                chain_name: str,
                                transaction: UnifiedTransaction,
                                private_key: str) -> Dict[str, Any]:
        """Execute a transaction on any chain"""
        if chain_name not in self.chains:
            raise ValueError(f"Chain {chain_name} not supported")
            
        chain = self.chains[chain_name]
        
        # Convert unified transaction to chain-specific format
        chain_tx = await self._convert_to_chain_transaction(chain_name, transaction)
        
        # Apply gas strategy
        gas_params = await self._get_gas_params(chain_name, transaction.gas_strategy)
        chain_tx.update(gas_params)
        
        # Execute transaction
        result = await chain.send_transaction(chain_tx, private_key)
        
        # Return unified result
        return {
            "chain": chain_name,
            "tx_hash": result.get("hash"),
            "status": result.get("status"),
            "gas_used": result.get("gas_used"),
            "effective_gas_price": result.get("effective_gas_price"),
            "block_number": result.get("block_number")
        }
        
    async def execute_multi_chain_transaction(self,
                                            transactions: List[Tuple[str, UnifiedTransaction]],
                                            private_key: str,
                                            atomic: bool = False) -> List[Dict[str, Any]]:
        """Execute transactions across multiple chains"""
        results = []
        
        if atomic:
            # Use cross-chain atomic swap protocol
            return await self._execute_atomic_multi_chain(transactions, private_key)
            
        # Execute transactions in parallel
        tasks = []
        for chain_name, transaction in transactions:
            task = self.execute_transaction(chain_name, transaction, private_key)
            tasks.append(task)
            
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    "chain": transactions[i][0],
                    "error": str(result),
                    "status": "failed"
                })
            else:
                processed_results.append(result)
                
        return processed_results
        
    async def find_best_route(self,
                            from_chain: str,
                            to_chain: str,
                            token_symbol: str,
                            amount: Decimal) -> Dict[str, Any]:
        """Find the best route for cross-chain transfers"""
        routes = []
        
        # Direct bridge route
        direct_route = await self._check_direct_bridge(from_chain, to_chain, token_symbol, amount)
        if direct_route:
            routes.append(direct_route)
            
        # Multi-hop routes through common chains
        hub_chains = ["ethereum", "polygon", "bsc"]  # Common hub chains
        
        for hub in hub_chains:
            if hub != from_chain and hub != to_chain:
                multi_hop = await self._check_multi_hop_route(
                    from_chain, hub, to_chain, token_symbol, amount
                )
                if multi_hop:
                    routes.append(multi_hop)
                    
        # Sort by total cost (fees + gas)
        routes.sort(key=lambda r: r["total_cost"])
        
        if routes:
            best_route = routes[0]
            best_route["alternatives"] = routes[1:3]  # Include top 3 alternatives
            return best_route
            
        return {
            "error": "No route found",
            "from_chain": from_chain,
            "to_chain": to_chain,
            "token": token_symbol,
            "amount": str(amount)
        }
        
    async def estimate_gas_across_chains(self,
                                       transaction_type: TransactionType,
                                       chains: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        """Estimate gas costs across multiple chains"""
        if not chains:
            chains = list(self.chains.keys())
            
        estimates = {}
        
        for chain_name in chains:
            if chain_name not in self.chains:
                continue
                
            try:
                # Get gas prices
                gas_prices = await self._get_gas_prices(chain_name)
                
                # Estimate gas units based on transaction type
                gas_units = self._estimate_gas_units(transaction_type)
                
                # Calculate costs
                estimates[chain_name] = {
                    "gas_units": gas_units,
                    "gas_prices": gas_prices,
                    "estimated_cost": {
                        "slow": self._calculate_gas_cost(gas_units, gas_prices["slow"]),
                        "standard": self._calculate_gas_cost(gas_units, gas_prices["standard"]),
                        "fast": self._calculate_gas_cost(gas_units, gas_prices["fast"])
                    },
                    "native_token": self._get_native_token(chain_name)
                }
                
            except Exception as e:
                logger.error(f"Error estimating gas for {chain_name}: {e}")
                estimates[chain_name] = {"error": str(e)}
                
        return estimates
        
    async def get_transaction_status(self,
                                   chain_name: str,
                                   tx_hash: str) -> Dict[str, Any]:
        """Get unified transaction status"""
        if chain_name not in self.chains:
            raise ValueError(f"Chain {chain_name} not supported")
            
        chain = self.chains[chain_name]
        
        # Get chain-specific status
        status = await chain.get_transaction_status(tx_hash)
        
        # Convert to unified format
        return {
            "chain": chain_name,
            "tx_hash": tx_hash,
            "status": self._normalize_status(status.get("status")),
            "confirmations": status.get("confirmations", 0),
            "block_number": status.get("block_number"),
            "gas_used": status.get("gas_used"),
            "success": status.get("success", False),
            "error": status.get("error")
        }
        
    async def _convert_to_chain_transaction(self,
                                          chain_name: str,
                                          transaction: UnifiedTransaction) -> Dict[str, Any]:
        """Convert unified transaction to chain-specific format"""
        chain = self.chains[chain_name]
        
        # Base transaction
        chain_tx = {
            "from": transaction.from_address,
            "to": transaction.to_address,
            "value": str(int(transaction.value * 10**18))  # Convert to wei
        }
        
        # Add nonce if specified
        if transaction.nonce is not None:
            chain_tx["nonce"] = transaction.nonce
        else:
            # Get next nonce
            chain_tx["nonce"] = await chain.get_nonce(transaction.from_address)
            
        # Handle different transaction types
        if transaction.tx_type == TransactionType.TRANSFER:
            # Native transfer, data already set
            pass
            
        elif transaction.tx_type == TransactionType.APPROVE:
            # ERC20 approve
            token_address = transaction.data.get("token_address")
            spender = transaction.data.get("spender")
            amount = transaction.data.get("amount")
            
            chain_tx["to"] = token_address
            chain_tx["value"] = "0"
            chain_tx["data"] = self._encode_approve(spender, amount)
            
        elif transaction.tx_type == TransactionType.CONTRACT_CALL:
            # Generic contract call
            chain_tx["data"] = transaction.data.get("data", "0x")
            chain_tx["value"] = str(transaction.data.get("value", 0))
            
        return chain_tx
        
    async def _get_gas_params(self,
                            chain_name: str,
                            strategy: GasStrategy) -> Dict[str, Any]:
        """Get gas parameters based on strategy"""
        gas_prices = await self._get_gas_prices(chain_name)
        
        if strategy == GasStrategy.SLOW:
            gas_price = gas_prices["slow"]
        elif strategy == GasStrategy.STANDARD:
            gas_price = gas_prices["standard"]
        elif strategy == GasStrategy.FAST:
            gas_price = gas_prices["fast"]
        else:  # INSTANT
            gas_price = gas_prices["fast"] * Decimal("1.5")  # 50% premium
            
        # Check if chain supports EIP-1559
        if self._supports_eip1559(chain_name):
            return {
                "maxFeePerGas": str(int(gas_price * 10**9)),  # Convert to wei
                "maxPriorityFeePerGas": str(int(gas_price * 10**9 * 0.1))  # 10% tip
            }
        else:
            return {
                "gasPrice": str(int(gas_price * 10**9))
            }
            
    async def _calculate_usd_value(self,
                                 chain_name: str,
                                 native_balance: Decimal,
                                 token_balances: Dict[str, Decimal]) -> Decimal:
        """Calculate total USD value of balances"""
        total_usd = Decimal("0")
        
        # Get prices
        prices = await self._get_token_prices(chain_name)
        
        # Native token value
        native_symbol = self._get_native_token(chain_name)
        if native_symbol in prices:
            total_usd += native_balance * prices[native_symbol]
            
        # Token values
        for token_symbol, balance in token_balances.items():
            if token_symbol in prices:
                total_usd += balance * prices[token_symbol]
                
        return total_usd
        
    async def _get_gas_prices(self, chain_name: str) -> Dict[str, Decimal]:
        """Get current gas prices for a chain"""
        # Check cache first
        if chain_name in self.gas_oracles:
            return self.gas_oracles[chain_name].get_gas_prices()
            
        # Fallback to chain RPC
        chain = self.chains[chain_name]
        gas_price = await chain.get_gas_price()
        
        return {
            "slow": gas_price * Decimal("0.8"),
            "standard": gas_price,
            "fast": gas_price * Decimal("1.2")
        }
        
    def _estimate_gas_units(self, tx_type: TransactionType) -> int:
        """Estimate gas units for transaction type"""
        gas_estimates = {
            TransactionType.TRANSFER: 21000,
            TransactionType.APPROVE: 45000,
            TransactionType.MINT: 65000,
            TransactionType.BURN: 45000,
            TransactionType.SWAP: 150000,
            TransactionType.STAKE: 80000,
            TransactionType.CONTRACT_CALL: 100000  # Default estimate
        }
        
        return gas_estimates.get(tx_type, 100000)
        
    def _calculate_gas_cost(self, gas_units: int, gas_price: Decimal) -> Decimal:
        """Calculate gas cost in native token"""
        return Decimal(gas_units) * gas_price / Decimal(10**9)  # Convert from gwei
        
    def _get_native_token(self, chain_name: str) -> str:
        """Get native token symbol for chain"""
        native_tokens = {
            "ethereum": "ETH",
            "polygon": "MATIC",
            "bsc": "BNB",
            "avalanche": "AVAX",
            "arbitrum": "ETH",
            "optimism": "ETH",
            "fantom": "FTM"
        }
        
        return native_tokens.get(chain_name, "UNKNOWN")
        
    def _supports_eip1559(self, chain_name: str) -> bool:
        """Check if chain supports EIP-1559"""
        eip1559_chains = [
            "ethereum", "polygon", "avalanche", "arbitrum", "optimism"
        ]
        
        return chain_name in eip1559_chains
        
    def _normalize_status(self, status: Any) -> str:
        """Normalize transaction status across chains"""
        if isinstance(status, bool):
            return "success" if status else "failed"
        elif isinstance(status, int):
            return "success" if status == 1 else "failed"
        elif isinstance(status, str):
            return status.lower()
        else:
            return "unknown"
            
    async def _get_token_prices(self, chain_name: str) -> Dict[str, Decimal]:
        """Get token prices (stub - integrate with price oracle)"""
        # In production, integrate with price oracle service
        return {
            "ETH": Decimal("2000"),
            "MATIC": Decimal("0.8"),
            "BNB": Decimal("300"),
            "AVAX": Decimal("35"),
            "FTM": Decimal("0.3"),
            "USDC": Decimal("1"),
            "WETH": Decimal("2000")
        }
        
    async def _check_direct_bridge(self,
                                 from_chain: str,
                                 to_chain: str,
                                 token: str,
                                 amount: Decimal) -> Optional[Dict[str, Any]]:
        """Check if direct bridge exists between chains"""
        # In production, check actual bridge availability
        # For now, simulate some routes
        
        if token == "USDC" and from_chain in ["ethereum", "polygon"] and to_chain in ["ethereum", "polygon"]:
            return {
                "type": "direct",
                "from_chain": from_chain,
                "to_chain": to_chain,
                "token": token,
                "amount": str(amount),
                "estimated_time": 15,  # minutes
                "bridge_fee": str(amount * Decimal("0.001")),  # 0.1%
                "gas_cost": str(Decimal("10")),  # USD
                "total_cost": str(amount * Decimal("0.001") + Decimal("10"))
            }
            
        return None
        
    async def _check_multi_hop_route(self,
                                   from_chain: str,
                                   hub_chain: str,
                                   to_chain: str,
                                   token: str,
                                   amount: Decimal) -> Optional[Dict[str, Any]]:
        """Check multi-hop route through hub chain"""
        # Check both legs
        leg1 = await self._check_direct_bridge(from_chain, hub_chain, token, amount)
        if not leg1:
            return None
            
        leg2 = await self._check_direct_bridge(hub_chain, to_chain, token, amount)
        if not leg2:
            return None
            
        # Combine costs
        total_fee = Decimal(leg1["bridge_fee"]) + Decimal(leg2["bridge_fee"])
        total_gas = Decimal(leg1["gas_cost"]) + Decimal(leg2["gas_cost"])
        total_time = leg1["estimated_time"] + leg2["estimated_time"]
        
        return {
            "type": "multi_hop",
            "from_chain": from_chain,
            "to_chain": to_chain,
            "hub_chain": hub_chain,
            "token": token,
            "amount": str(amount),
            "estimated_time": total_time,
            "bridge_fee": str(total_fee),
            "gas_cost": str(total_gas),
            "total_cost": str(total_fee + total_gas),
            "legs": [leg1, leg2]
        }
        
    def _encode_approve(self, spender: str, amount: Decimal) -> str:
        """Encode ERC20 approve function call"""
        # In production, use proper ABI encoding
        # This is a simplified version
        function_selector = "0x095ea7b3"
        padded_spender = spender[2:].zfill(64)
        padded_amount = hex(int(amount * 10**18))[2:].zfill(64)
        
        return function_selector + padded_spender + padded_amount
        
    async def _execute_atomic_multi_chain(self,
                                        transactions: List[Tuple[str, UnifiedTransaction]],
                                        private_key: str) -> List[Dict[str, Any]]:
        """Execute atomic multi-chain transaction"""
        # This would integrate with a cross-chain atomic swap protocol
        # For now, return error
        return [{
            "error": "Atomic multi-chain transactions not yet implemented",
            "transactions": len(transactions)
        }]
        
    async def _initialize_gas_oracle(self, chain_name: str):
        """Initialize gas oracle for a chain"""
        # In production, connect to gas oracle service
        # For now, use simple cache
        self.gas_oracles[chain_name] = {
            "get_gas_prices": lambda: {
                "slow": Decimal("20"),
                "standard": Decimal("30"),
                "fast": Decimal("50")
            }
        } 