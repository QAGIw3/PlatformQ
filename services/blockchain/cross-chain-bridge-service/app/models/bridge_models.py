from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum


class TransferStatus(str, Enum):
    """Status of a cross-chain transfer"""
    PENDING = "pending"
    LOCKED = "locked"
    ATTESTING = "attesting"
    MINTING = "minting"
    COMPLETED = "completed"
    FAILED = "failed"
    REFUNDED = "refunded"
    EXPIRED = "expired"


class TokenType(str, Enum):
    """Type of token being transferred"""
    NATIVE = "native"
    ERC20 = "erc20"
    ERC721 = "erc721"
    ERC1155 = "erc1155"
    SPL = "spl"  # Solana
    CW20 = "cw20"  # Cosmos


class BridgeTransferRequest(BaseModel):
    """Request to initiate a cross-chain transfer"""
    bridge_name: str = Field(..., description="Name of the bridge route (e.g., 'eth-polygon')")
    from_address: str = Field(..., description="Source address initiating the transfer")
    to_address: str = Field(..., description="Destination address to receive tokens")
    token_address: Optional[str] = Field(None, description="Token contract address (None for native)")
    token_type: TokenType = Field(default=TokenType.NATIVE)
    amount: str = Field(..., description="Amount to transfer (in smallest unit)")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)
    
    @validator('amount')
    def validate_amount(cls, v):
        try:
            amount = int(v)
            if amount <= 0:
                raise ValueError("Amount must be positive")
            return v
        except ValueError as e:
            raise ValueError(f"Invalid amount: {e}")


class BridgeTransfer(BaseModel):
    """Cross-chain transfer record"""
    transfer_id: str = Field(..., description="Unique transfer identifier")
    bridge_name: str
    source_chain: str
    target_chain: str
    from_address: str
    to_address: str
    token_address: Optional[str]
    token_type: TokenType
    amount: str
    fee_amount: str = Field(default="0")
    status: TransferStatus = Field(default=TransferStatus.PENDING)
    
    # Transaction hashes
    lock_tx_hash: Optional[str] = None
    mint_tx_hash: Optional[str] = None
    refund_tx_hash: Optional[str] = None
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    locked_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Attestation tracking
    attestations_required: int = Field(default=2)
    attestations_received: int = Field(default=0)
    attestation_ids: List[str] = Field(default_factory=list)
    
    # Additional data
    metadata: Dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str] = None
    retry_count: int = Field(default=0)


class BridgeAttestation(BaseModel):
    """Attestation for a bridge transfer"""
    attestation_id: str = Field(..., description="Unique attestation identifier")
    transfer_id: str = Field(..., description="Transfer being attested")
    validator_address: str = Field(..., description="Address of the validator")
    signature: str = Field(..., description="Validator's signature")
    block_number: int = Field(..., description="Block number of the lock transaction")
    block_hash: str = Field(..., description="Block hash of the lock transaction")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BridgeEvent(BaseModel):
    """Event emitted during bridge operations"""
    event_id: str
    transfer_id: str
    event_type: str  # lock_initiated, lock_confirmed, attestation_received, mint_initiated, etc.
    chain: str
    transaction_hash: Optional[str] = None
    block_number: Optional[int] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    data: Dict[str, Any] = Field(default_factory=dict)


class TransferStatusResponse(BaseModel):
    """Response for transfer status query"""
    transfer: BridgeTransfer
    events: List[BridgeEvent] = Field(default_factory=list)
    estimated_completion_time: Optional[datetime] = None
    next_action: Optional[str] = None


class BridgeRoute(BaseModel):
    """Information about a bridge route"""
    name: str
    source_chain: str
    target_chain: str
    supported_tokens: List[Dict[str, str]] = Field(default_factory=list)
    fee_percentage: float
    min_amount: str
    max_amount: str
    estimated_time_seconds: int
    is_active: bool = Field(default=True)


class BridgeStatistics(BaseModel):
    """Statistics for bridge operations"""
    bridge_name: str
    total_transfers: int = Field(default=0)
    successful_transfers: int = Field(default=0)
    failed_transfers: int = Field(default=0)
    total_volume: str = Field(default="0")
    average_completion_time_seconds: float = Field(default=0.0)
    last_24h_transfers: int = Field(default=0)
    last_24h_volume: str = Field(default="0")


class TokenMapping(BaseModel):
    """Mapping of tokens across chains"""
    source_chain: str
    source_token: str
    target_chain: str
    target_token: str
    decimals_source: int
    decimals_target: int
    is_wrapped: bool = Field(default=True)


class RelayerInfo(BaseModel):
    """Information about bridge relayers"""
    relayer_id: str
    address: str
    chains: List[str]
    min_balance_required: Dict[str, str]  # chain -> min balance
    current_balances: Dict[str, str]  # chain -> current balance
    is_active: bool = Field(default=True)
    last_activity: datetime = Field(default_factory=datetime.utcnow)


class BridgeHealthStatus(BaseModel):
    """Health status of bridge routes"""
    bridge_name: str
    is_operational: bool
    source_chain_connected: bool
    target_chain_connected: bool
    relayer_status: str
    pending_transfers: int
    last_successful_transfer: Optional[datetime]
    issues: List[str] = Field(default_factory=list)


class TransferBatch(BaseModel):
    """Batch of transfers for optimization"""
    batch_id: str
    bridge_name: str
    transfer_ids: List[str]
    total_amount: str
    status: str
    created_at: datetime = Field(default_factory=datetime.utcnow) 