"""
Transaction models
"""

from enum import Enum
from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field, validator


class TransactionStatus(str, Enum):
    """Transaction status"""
    PENDING = "pending"
    QUEUED = "queued"
    PROCESSING = "processing"
    SIGNING = "signing"
    BROADCASTING = "broadcasting"
    BROADCAST = "broadcast"
    CONFIRMING = "confirming"
    CONFIRMED = "confirmed"
    FAILED = "failed"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


class TransactionType(str, Enum):
    """Transaction type"""
    TRANSFER = "transfer"
    CONTRACT_CALL = "contract_call"
    CONTRACT_DEPLOY = "contract_deploy"
    TOKEN_TRANSFER = "token_transfer"
    NFT_TRANSFER = "nft_transfer"
    SWAP = "swap"
    BRIDGE = "bridge"
    BATCH = "batch"


class TransactionPriority(str, Enum):
    """Transaction priority"""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class Transaction(BaseModel):
    """Transaction model"""
    id: str = Field(..., description="Unique transaction ID")
    chain: str = Field(..., description="Blockchain identifier")
    type: TransactionType = Field(..., description="Transaction type")
    from_address: str = Field(..., description="Sender address")
    to_address: str = Field(..., description="Recipient address")
    value: str = Field("0", description="Value in wei/smallest unit")
    data: Optional[str] = Field(None, description="Transaction data")
    
    # Gas settings
    gas_limit: Optional[int] = Field(None, description="Gas limit")
    gas_price: Optional[str] = Field(None, description="Gas price in wei")
    max_fee_per_gas: Optional[str] = Field(None, description="Max fee per gas (EIP-1559)")
    max_priority_fee_per_gas: Optional[str] = Field(None, description="Max priority fee (EIP-1559)")
    
    # Transaction metadata
    nonce: Optional[int] = Field(None, description="Transaction nonce")
    priority: TransactionPriority = Field(TransactionPriority.NORMAL, description="Processing priority")
    tags: Dict[str, str] = Field(default_factory=dict, description="Custom tags")
    
    # Timing
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation time")
    expires_at: Optional[datetime] = Field(None, description="Expiration time")
    
    # Security
    requires_approval: bool = Field(True, description="Requires approval before signing")
    approval_policy: Optional[str] = Field(None, description="Approval policy ID")
    approved_by: Optional[List[str]] = Field(None, description="List of approvers")
    
    @validator('from_address', 'to_address')
    def validate_address(cls, v):
        """Validate address format"""
        if not v or not v.startswith('0x'):
            raise ValueError("Invalid address format")
        return v.lower()


class TransactionRequest(BaseModel):
    """Transaction submission request"""
    chain: str = Field(..., description="Blockchain identifier")
    type: TransactionType = Field(..., description="Transaction type")
    from_address: str = Field(..., description="Sender address")
    to_address: str = Field(..., description="Recipient address")
    value: Optional[str] = Field("0", description="Value in wei/smallest unit")
    data: Optional[str] = Field(None, description="Transaction data")
    
    # Optional gas settings
    gas_limit: Optional[int] = Field(None, description="Gas limit")
    gas_price: Optional[str] = Field(None, description="Gas price in wei")
    max_fee_per_gas: Optional[str] = Field(None, description="Max fee per gas (EIP-1559)")
    max_priority_fee_per_gas: Optional[str] = Field(None, description="Max priority fee (EIP-1559)")
    
    # Processing options
    priority: TransactionPriority = Field(TransactionPriority.NORMAL, description="Processing priority")
    wait_for_confirmation: bool = Field(False, description="Wait for confirmation")
    confirmation_blocks: Optional[int] = Field(None, description="Required confirmations")
    expires_in_seconds: Optional[int] = Field(None, description="Expiration time in seconds")
    
    # Metadata
    tags: Dict[str, str] = Field(default_factory=dict, description="Custom tags")
    callback_url: Optional[str] = Field(None, description="Status callback URL")


class TransactionResult(BaseModel):
    """Transaction processing result"""
    id: str = Field(..., description="Transaction ID")
    status: TransactionStatus = Field(..., description="Current status")
    tx_hash: Optional[str] = Field(None, description="Blockchain transaction hash")
    block_number: Optional[int] = Field(None, description="Block number")
    confirmations: int = Field(0, description="Number of confirmations")
    gas_used: Optional[int] = Field(None, description="Gas used")
    effective_gas_price: Optional[str] = Field(None, description="Effective gas price")
    error: Optional[str] = Field(None, description="Error message if failed")
    
    # Timing
    created_at: datetime = Field(..., description="Creation time")
    broadcast_at: Optional[datetime] = Field(None, description="Broadcast time")
    confirmed_at: Optional[datetime] = Field(None, description="Confirmation time")
    
    # Additional data
    receipt: Optional[Dict[str, Any]] = Field(None, description="Transaction receipt")
    logs: Optional[List[Dict[str, Any]]] = Field(None, description="Transaction logs")


class TransactionEvent(BaseModel):
    """Transaction status event"""
    transaction_id: str = Field(..., description="Transaction ID")
    status: TransactionStatus = Field(..., description="New status")
    previous_status: Optional[TransactionStatus] = Field(None, description="Previous status")
    tx_hash: Optional[str] = Field(None, description="Transaction hash")
    block_number: Optional[int] = Field(None, description="Block number")
    confirmations: Optional[int] = Field(None, description="Confirmations")
    error: Optional[str] = Field(None, description="Error message")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event time")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class BatchTransaction(BaseModel):
    """Batch transaction model"""
    id: str = Field(..., description="Batch ID")
    chain: str = Field(..., description="Blockchain identifier")
    transactions: List[Transaction] = Field(..., description="List of transactions")
    atomic: bool = Field(True, description="All or nothing execution")
    priority: TransactionPriority = Field(TransactionPriority.NORMAL, description="Batch priority")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation time") 