"""
Interfaces (protocols) for blockchain operations.
Using Python's Protocol for structural subtyping.
"""

from typing import Protocol, Dict, Any, Optional, List, AsyncIterator
from abc import abstractmethod
from decimal import Decimal

from .types import (
    ChainType, TransactionResult, GasEstimate, BlockchainEvent,
    ChainMetadata, TransactionType, GasStrategy
)
from .models import Transaction, SmartContract


class IBlockchainAdapter(Protocol):
    """Interface for blockchain adapters"""
    
    @property
    @abstractmethod
    def chain_type(self) -> ChainType:
        """Get the chain type this adapter handles"""
        ...
        
    @property
    @abstractmethod
    def chain_id(self) -> int:
        """Get the chain ID"""
        ...
        
    @abstractmethod
    async def connect(self) -> bool:
        """Connect to the blockchain network"""
        ...
        
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the blockchain network"""
        ...
        
    @abstractmethod
    async def get_balance(self, address: str, token_address: Optional[str] = None) -> Decimal:
        """Get balance of native currency or token"""
        ...
        
    @abstractmethod
    async def send_transaction(self, transaction: Transaction) -> TransactionResult:
        """Send a transaction to the blockchain"""
        ...
        
    @abstractmethod
    async def estimate_gas(self, transaction: Transaction) -> GasEstimate:
        """Estimate gas for a transaction"""
        ...
        
    @abstractmethod
    async def get_transaction_receipt(self, tx_hash: str) -> Optional[TransactionResult]:
        """Get transaction receipt by hash"""
        ...
        
    @abstractmethod
    async def deploy_contract(self, contract: SmartContract) -> str:
        """Deploy a smart contract"""
        ...
        
    @abstractmethod
    async def call_contract(self, contract_address: str, method: str, 
                          params: List[Any], abi: List[Dict[str, Any]]) -> Any:
        """Call a smart contract method (read-only)"""
        ...
        
    @abstractmethod
    async def get_block_number(self) -> int:
        """Get current block number"""
        ...
        
    @abstractmethod
    async def get_chain_metadata(self) -> ChainMetadata:
        """Get current chain metadata"""
        ...
        

class ITransactionManager(Protocol):
    """Interface for transaction management"""
    
    @abstractmethod
    async def prepare_transaction(self, from_address: str, to_address: str,
                                value: Decimal, data: Optional[bytes] = None,
                                gas_strategy: GasStrategy = GasStrategy.STANDARD) -> Transaction:
        """Prepare a transaction with proper nonce and gas settings"""
        ...
        
    @abstractmethod
    async def sign_transaction(self, transaction: Transaction, private_key: str) -> str:
        """Sign a transaction and return the signed data"""
        ...
        
    @abstractmethod
    async def broadcast_transaction(self, signed_tx: str) -> str:
        """Broadcast a signed transaction and return the hash"""
        ...
        
    @abstractmethod
    async def wait_for_confirmation(self, tx_hash: str, confirmations: int = 1) -> TransactionResult:
        """Wait for transaction confirmation"""
        ...
        
    @abstractmethod
    async def get_transaction_status(self, tx_hash: str) -> TransactionResult:
        """Get current status of a transaction"""
        ...
        
    @abstractmethod
    async def cancel_transaction(self, tx_hash: str, gas_multiplier: float = 1.1) -> str:
        """Attempt to cancel a pending transaction by replacement"""
        ...
        

class IGasOptimizer(Protocol):
    """Interface for gas optimization"""
    
    @abstractmethod
    async def get_optimal_gas_price(self, chain_type: ChainType, 
                                   strategy: GasStrategy = GasStrategy.STANDARD) -> GasEstimate:
        """Get optimal gas price for a chain and strategy"""
        ...
        
    @abstractmethod
    async def estimate_transaction_cost(self, transaction: Transaction) -> GasEstimate:
        """Estimate total cost of a transaction"""
        ...
        
    @abstractmethod
    async def batch_transactions(self, transactions: List[Transaction]) -> Transaction:
        """Batch multiple transactions into one for gas savings"""
        ...
        
    @abstractmethod
    async def find_optimal_time(self, chain_type: ChainType) -> Dict[str, Any]:
        """Find optimal time to submit transaction based on gas prices"""
        ...
        

class IEventIndexer(Protocol):
    """Interface for blockchain event indexing"""
    
    @abstractmethod
    async def subscribe_to_events(self, contract_address: str, event_names: List[str],
                                from_block: Optional[int] = None) -> AsyncIterator[BlockchainEvent]:
        """Subscribe to contract events"""
        ...
        
    @abstractmethod
    async def get_past_events(self, contract_address: str, event_name: str,
                            from_block: int, to_block: int) -> List[BlockchainEvent]:
        """Get historical events"""
        ...
        
    @abstractmethod
    async def index_events(self, events: List[BlockchainEvent]) -> None:
        """Index events for fast retrieval"""
        ...
        
    @abstractmethod
    async def query_events(self, filters: Dict[str, Any]) -> List[BlockchainEvent]:
        """Query indexed events"""
        ... 