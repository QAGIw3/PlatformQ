"""
Core types and enums for blockchain operations across PlatformQ services.
"""

from enum import Enum, auto
from typing import Optional, Dict, Any, List
from decimal import Decimal
from dataclasses import dataclass, field
from datetime import datetime


class ChainType(Enum):
    """Supported blockchain types"""
    ETHEREUM = "ethereum"
    POLYGON = "polygon"
    ARBITRUM = "arbitrum"
    OPTIMISM = "optimism"
    AVALANCHE = "avalanche"
    BSC = "bsc"
    SOLANA = "solana"
    COSMOS = "cosmos"
    NEAR = "near"
    HYPERLEDGER = "hyperledger"
    

class TransactionStatus(Enum):
    """Transaction status states"""
    PENDING = "pending"
    SUBMITTED = "submitted"
    CONFIRMED = "confirmed"
    FAILED = "failed"
    REVERTED = "reverted"
    

class GasStrategy(Enum):
    """Gas pricing strategies"""
    SLOW = "slow"  # Save gas, lower priority
    STANDARD = "standard"  # Normal priority
    FAST = "fast"  # High priority
    INSTANT = "instant"  # Maximum priority
    CUSTOM = "custom"  # User-defined gas parameters


class TransactionType(Enum):
    """Types of blockchain transactions"""
    TRANSFER = "transfer"
    CONTRACT_CALL = "contract_call"
    CONTRACT_DEPLOY = "contract_deploy"
    APPROVE = "approve"
    MINT = "mint"
    BURN = "burn"
    STAKE = "stake"
    UNSTAKE = "unstake"
    BRIDGE = "bridge"


@dataclass
class ChainConfig:
    """Configuration for a blockchain network"""
    chain_id: int
    chain_type: ChainType
    name: str
    rpc_url: str
    ws_url: Optional[str] = None
    explorer_url: Optional[str] = None
    native_currency: str = "ETH"
    decimals: int = 18
    block_time: float = 12.0  # seconds
    confirmation_blocks: int = 12
    supports_eip1559: bool = True
    max_gas_price: Optional[int] = None
    contracts: Dict[str, str] = field(default_factory=dict)
    

@dataclass
class TransactionResult:
    """Result of a blockchain transaction"""
    transaction_hash: str
    block_number: int
    block_hash: str
    gas_used: int
    gas_price: int
    status: TransactionStatus
    chain_type: ChainType
    timestamp: datetime
    from_address: str
    to_address: Optional[str] = None
    value: Decimal = Decimal(0)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    error_message: Optional[str] = None
    

@dataclass
class GasEstimate:
    """Gas estimation for a transaction"""
    gas_limit: int
    gas_price: int
    max_fee_per_gas: Optional[int] = None
    max_priority_fee_per_gas: Optional[int] = None
    total_cost_wei: int = 0
    total_cost_native: Decimal = Decimal(0)
    total_cost_usd: Optional[Decimal] = None
    

@dataclass  
class BlockchainEvent:
    """Blockchain event data"""
    event_name: str
    contract_address: str
    transaction_hash: str
    block_number: int
    log_index: int
    args: Dict[str, Any]
    chain_type: ChainType
    timestamp: datetime
    removed: bool = False
    

@dataclass
class ChainMetadata:
    """Metadata about a blockchain"""
    chain_type: ChainType
    latest_block: int
    syncing: bool
    peer_count: int
    gas_price: int
    base_fee: Optional[int] = None
    priority_fee: Optional[int] = None
    

class BlockchainError(Exception):
    """Base exception for blockchain operations"""
    def __init__(self, message: str, chain_type: Optional[ChainType] = None, 
                 error_code: Optional[str] = None):
        self.chain_type = chain_type
        self.error_code = error_code
        super().__init__(message)
        

class TransactionRevertedError(BlockchainError):
    """Transaction was reverted"""
    pass
    

class InsufficientFundsError(BlockchainError):
    """Insufficient funds for transaction"""
    pass
    

class GasPriceTooHighError(BlockchainError):
    """Gas price exceeds maximum allowed"""
    pass
    

class ChainNotSupportedError(BlockchainError):
    """Blockchain not supported"""
    pass 