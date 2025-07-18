"""
Cosmos blockchain adapter implementation.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from decimal import Decimal
import aiohttp
from cosmpy.aerial.client import LedgerClient, NetworkConfig
from cosmpy.aerial.wallet import LocalWallet
from cosmpy.aerial.tx import Transaction as CosmosTx
from cosmpy.crypto.keypairs import PrivateKey

from ..interfaces import IBlockchainAdapter
from ..types import (
    ChainType, TransactionResult, GasEstimate, BlockchainEvent,
    ChainMetadata, TransactionStatus
)
from ..models import Transaction as PlatformTransaction, SmartContract
from ..utils import retry_with_backoff, normalize_address

logger = logging.getLogger(__name__)


class CosmosAdapter(IBlockchainAdapter):
    """Cosmos SDK blockchain adapter"""
    
    def __init__(self, rpc_url: str, chain_id: str = "cosmoshub-4", 
                 prefix: str = "cosmos", denom: str = "uatom"):
        self.rpc_url = rpc_url
        self._chain_id_str = chain_id
        self._chain_id = hash(chain_id) % 1000000  # Numeric representation
        self.prefix = prefix
        self.denom = denom
        self.client: Optional[LedgerClient] = None
        self._connected = False
        
    @property
    def chain_type(self) -> ChainType:
        return ChainType.COSMOS
        
    @property
    def chain_id(self) -> int:
        return self._chain_id
        
    @retry_with_backoff(max_retries=3)
    async def connect(self) -> bool:
        """Connect to Cosmos network"""
        try:
            # Extract host and port from RPC URL
            url_parts = self.rpc_url.replace("http://", "").replace("https://", "").split(":")
            host = url_parts[0]
            port = int(url_parts[1]) if len(url_parts) > 1 else 443
            
            # Create network config
            cfg = NetworkConfig(
                chain_id=self._chain_id_str,
                url=f"rest+{self.rpc_url}",
                fee_minimum_gas_price=0.025,
                fee_denomination=self.denom,
                staking_denomination=self.denom
            )
            
            self.client = LedgerClient(cfg)
            
            # Test connection by getting node info
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.rpc_url}/node_info") as resp:
                    if resp.status == 200:
                        self._connected = True
                        logger.info(f"Connected to Cosmos network {self._chain_id_str}")
                        return True
                        
            raise ConnectionError("Failed to connect to Cosmos network")
            
        except Exception as e:
            logger.error(f"Failed to connect to Cosmos: {e}")
            self._connected = False
            raise
            
    async def disconnect(self) -> None:
        """Disconnect from Cosmos network"""
        self.client = None
        self._connected = False
        logger.info("Disconnected from Cosmos network")
        
    async def get_balance(self, address: str, token_address: Optional[str] = None) -> Decimal:
        """Get balance in native token or IBC token"""
        if not self._connected:
            raise ConnectionError("Not connected to Cosmos network")
            
        try:
            # Use REST API for balance query
            denom_to_query = token_address if token_address else self.denom
            
            async with aiohttp.ClientSession() as session:
                url = f"{self.rpc_url}/cosmos/bank/v1beta1/balances/{address}/{denom_to_query}"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        balance = data.get("balance", {}).get("amount", "0")
                        # Convert from smallest unit (e.g., uatom) to main unit (e.g., ATOM)
                        return Decimal(balance) / Decimal(10**6)
                    return Decimal("0")
                    
        except Exception as e:
            logger.error(f"Error getting balance: {e}")
            raise
            
    async def send_transaction(self, transaction: PlatformTransaction) -> TransactionResult:
        """Send a transaction to Cosmos network"""
        if not self._connected:
            raise ConnectionError("Not connected to Cosmos network")
            
        try:
            # TODO This is a mock implementation since we don't have the private key
            # In production, this would use the actual wallet
            
            tx_hash = f"cosmos_tx_{hash(str(transaction))}"
            
            return TransactionResult(
                transaction_hash=tx_hash,
                status=TransactionStatus.PENDING,
                block_number=None,
                gas_used=Decimal("50000"),
                gas_price=Decimal("0.025"),
                confirmations=0,
                timestamp=None,
                raw_receipt={"tx_hash": tx_hash}
            )
            
        except Exception as e:
            logger.error(f"Error sending transaction: {e}")
            raise
            
    async def estimate_gas(self, transaction: PlatformTransaction) -> GasEstimate:
        """Estimate gas for Cosmos transaction"""
        if not self._connected:
            raise ConnectionError("Not connected to Cosmos network")
            
        # Basic gas estimation for Cosmos
        base_gas = 50000
        if transaction.data:
            # Add gas for data
            base_gas += len(transaction.data) * 10
            
        gas_price = Decimal("0.025")  # Standard gas price in uatom
        
        return GasEstimate(
            gas_limit=base_gas,
            gas_price=gas_price,
            total_cost=Decimal(base_gas) * gas_price / Decimal(10**6),
            priority_fees={
                "slow": gas_price * Decimal("0.8"),
                "standard": gas_price,
                "fast": gas_price * Decimal("1.5")
            }
        )
        
    async def get_transaction_receipt(self, tx_hash: str) -> Optional[TransactionResult]:
        """Get transaction receipt"""
        if not self._connected:
            raise ConnectionError("Not connected to Cosmos network")
            
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.rpc_url}/cosmos/tx/v1beta1/txs/{tx_hash}"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        tx_response = data.get("tx_response", {})
                        
                        return TransactionResult(
                            transaction_hash=tx_hash,
                            status=TransactionStatus.SUCCESS if tx_response.get("code", 0) == 0 else TransactionStatus.FAILED,
                            block_number=int(tx_response.get("height", 0)),
                            gas_used=Decimal(tx_response.get("gas_used", "0")),
                            gas_price=Decimal("0.025"),
                            confirmations=1,
                            timestamp=tx_response.get("timestamp"),
                            raw_receipt=tx_response
                        )
            return None
            
        except Exception as e:
            logger.error(f"Error getting transaction receipt: {e}")
            return None
            
    async def deploy_contract(self, contract: SmartContract) -> str:
        """Deploy CosmWasm contract"""
        if not self._connected:
            raise ConnectionError("Not connected to Cosmos network")
            
        # TODO CosmWasm contract deployment is complex
        # This is a placeholder implementation
        contract_address = f"cosmos1{hash(contract.bytecode) % 10**38}"
        logger.info(f"Mock deployed CosmWasm contract: {contract_address}")
        return contract_address
        
    async def call_contract(self, contract_address: str, method: str, 
                          params: List[Any], abi: List[Dict[str, Any]]) -> Any:
        """Query CosmWasm contract"""
        if not self._connected:
            raise ConnectionError("Not connected to Cosmos network")
            
        try:
            # Query CosmWasm contract
            async with aiohttp.ClientSession() as session:
                query_msg = {method: params[0] if params else {}}
                url = f"{self.rpc_url}/cosmwasm/wasm/v1/contract/{contract_address}/smart/{query_msg}"
                
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return data.get("data")
            return None
            
        except Exception as e:
            logger.error(f"Error calling contract: {e}")
            return None
            
    async def get_block_number(self) -> int:
        """Get current block height"""
        if not self._connected:
            raise ConnectionError("Not connected to Cosmos network")
            
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.rpc_url}/cosmos/base/tendermint/v1beta1/blocks/latest"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return int(data["block"]["header"]["height"])
            return 0
            
        except Exception as e:
            logger.error(f"Error getting block number: {e}")
            raise
            
    async def get_chain_metadata(self) -> ChainMetadata:
        """Get current chain metadata"""
        if not self._connected:
            raise ConnectionError("Not connected to Cosmos network")
            
        try:
            block_height = await self.get_block_number()
            
            # Get node info
            async with aiohttp.ClientSession() as session:
                url = f"{self.rpc_url}/node_info"
                async with session.get(url) as resp:
                    node_info = await resp.json() if resp.status == 200 else {}
                    
            return ChainMetadata(
                chain_id=self.chain_id,
                chain_type=self.chain_type,
                latest_block=block_height,
                gas_price=Decimal("0.025"),
                network_name=self._chain_id_str,
                currency_symbol=self.denom.upper().replace("U", ""),
                currency_decimals=6,
                is_testnet="testnet" in self._chain_id_str,
                extra_data={
                    "prefix": self.prefix,
                    "denom": self.denom,
                    "node_info": node_info
                }
            )
            
        except Exception as e:
            logger.error(f"Error getting chain metadata: {e}")
            raise 