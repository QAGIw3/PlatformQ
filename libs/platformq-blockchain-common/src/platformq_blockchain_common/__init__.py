"""
PlatformQ Blockchain Common Library

Shared blockchain types, interfaces, and utilities used across all blockchain services.
"""

from .types import (
    ChainType,
    TransactionStatus,
    TransactionResult,
    GasStrategy,
    BlockchainError,
    ChainConfig
)

from .interfaces import (
    IBlockchainAdapter,
    ITransactionManager,
    IGasOptimizer,
    IEventIndexer
)

from .models import (
    Transaction,
    SmartContract,
    BlockchainEvent,
    GasEstimate,
    ChainMetadata
)

from .utils import (
    calculate_transaction_hash,
    validate_address,
    normalize_address,
    estimate_gas_price,
    format_wei
)

from .connection_pool import ConnectionPool
from .adapter_factory import AdapterFactory
from .adapters import (
    BaseAdapter,
    EVMAdapter,
    SolanaAdapter,
    CosmosAdapter,
    PolkadotAdapter
)

__all__ = [
    # Types
    "ChainType",
    "TransactionStatus",
    "TransactionResult",
    "GasStrategy",
    "BlockchainError",
    "ChainConfig",
    
    # Interfaces
    "IBlockchainAdapter",
    "ITransactionManager",
    "IGasOptimizer",
    "IEventIndexer",
    
    # Models
    "Transaction",
    "SmartContract",
    "BlockchainEvent",
    "GasEstimate",
    "ChainMetadata",
    
    # Utils
    "calculate_transaction_hash",
    "validate_address",
    "normalize_address",
    "estimate_gas_price",
    "format_wei",
    
    # Connection Pool & Factory
    "ConnectionPool",
    "AdapterFactory",
    
    # Adapters
    "BaseAdapter",
    "EVMAdapter",
    "SolanaAdapter",
    "CosmosAdapter",
    "PolkadotAdapter"
]

__version__ = "1.0.0" 