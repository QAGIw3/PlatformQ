"""
Core data models for blockchain operations.
"""

from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime

from .types import ChainType, TransactionType, GasStrategy


@dataclass
class Transaction:
    """Blockchain transaction model"""
    from_address: str
    to_address: str
    value: Decimal = Decimal(0)
    data: Optional[bytes] = None
    nonce: Optional[int] = None
    gas_limit: Optional[int] = None
    gas_price: Optional[int] = None
    max_fee_per_gas: Optional[int] = None
    max_priority_fee_per_gas: Optional[int] = None
    chain_id: Optional[int] = None
    transaction_type: TransactionType = TransactionType.TRANSFER
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "from": self.from_address,
            "to": self.to_address,
            "value": str(self.value),
            "data": self.data.hex() if self.data else None,
            "nonce": self.nonce,
            "gas": self.gas_limit,
            "gasPrice": self.gas_price,
            "maxFeePerGas": self.max_fee_per_gas,
            "maxPriorityFeePerGas": self.max_priority_fee_per_gas,
            "chainId": self.chain_id,
            "type": self.transaction_type.value,
            "metadata": self.metadata
        }
        

@dataclass
class SmartContract:
    """Smart contract model"""
    name: str
    abi: List[Dict[str, Any]]
    bytecode: str
    source_code: Optional[str] = None
    compiler_version: Optional[str] = None
    optimization_enabled: bool = True
    constructor_args: List[Any] = field(default_factory=list)
    libraries: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    

@dataclass
class ChainAccount:
    """Blockchain account/wallet model"""
    address: str
    chain_type: ChainType
    balance: Decimal = Decimal(0)
    nonce: int = 0
    is_contract: bool = False
    label: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    

@dataclass
class TokenInfo:
    """Token information model"""
    address: str
    symbol: str
    name: str
    decimals: int
    total_supply: Optional[Decimal] = None
    chain_type: ChainType = ChainType.ETHEREUM
    is_native: bool = False
    logo_uri: Optional[str] = None
    price_usd: Optional[Decimal] = None
    

@dataclass
class GasMetrics:
    """Gas usage metrics for optimization"""
    average_gas_price: int
    fast_gas_price: int
    slow_gas_price: int
    base_fee: Optional[int] = None
    priority_fee: Optional[int] = None
    estimated_wait_slow: int = 300  # seconds
    estimated_wait_standard: int = 60
    estimated_wait_fast: int = 15
    last_updated: datetime = field(default_factory=datetime.utcnow)
    

@dataclass
class CrossChainMessage:
    """Message for cross-chain communication"""
    source_chain: ChainType
    target_chain: ChainType
    sender: str
    receiver: str
    payload: bytes
    nonce: int
    timestamp: datetime = field(default_factory=datetime.utcnow)
    message_hash: Optional[str] = None
    status: str = "pending"
    

@dataclass
class BridgeTransaction:
    """Cross-chain bridge transaction"""
    bridge_id: str
    source_chain: ChainType
    target_chain: ChainType
    source_token: str
    target_token: str
    amount: Decimal
    sender: str
    receiver: str
    source_tx_hash: Optional[str] = None
    target_tx_hash: Optional[str] = None
    status: str = "pending"
    created_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict) 