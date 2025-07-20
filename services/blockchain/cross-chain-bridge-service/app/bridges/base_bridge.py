from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Tuple
import logging
from datetime import datetime

from ..models.bridge_models import (
    BridgeTransfer, BridgeAttestation, BridgeEvent,
    TransferStatus, TokenType
)


class BaseBridge(ABC):
    """Base class for cross-chain bridge implementations"""
    
    def __init__(
        self,
        source_chain: str,
        target_chain: str,
        source_rpc: str,
        target_rpc: str,
        config: Dict[str, Any]
    ):
        self.source_chain = source_chain
        self.target_chain = target_chain
        self.source_rpc = source_rpc
        self.target_rpc = target_rpc
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{source_chain}-{target_chain}")
        
        # Bridge contract addresses
        self.source_bridge_contract = config.get('source_bridge_contract')
        self.target_bridge_contract = config.get('target_bridge_contract')
        
    @abstractmethod
    async def initialize(self) -> None:
        """Initialize bridge connections and contracts"""
        pass
    
    @abstractmethod
    async def lock_tokens(
        self,
        transfer: BridgeTransfer,
        private_key: Optional[str] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Lock tokens on the source chain
        
        Returns:
            Tuple of (transaction_hash, transaction_data)
        """
        pass
    
    @abstractmethod
    async def mint_tokens(
        self,
        transfer: BridgeTransfer,
        attestations: List[BridgeAttestation],
        private_key: Optional[str] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Mint tokens on the target chain after validation
        
        Returns:
            Tuple of (transaction_hash, transaction_data)
        """
        pass
    
    @abstractmethod
    async def verify_lock_transaction(
        self,
        transaction_hash: str,
        expected_transfer: BridgeTransfer
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Verify that tokens were properly locked on source chain
        
        Returns:
            Tuple of (is_valid, transaction_details)
        """
        pass
    
    @abstractmethod
    async def verify_mint_transaction(
        self,
        transaction_hash: str,
        expected_transfer: BridgeTransfer
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Verify that tokens were properly minted on target chain
        
        Returns:
            Tuple of (is_valid, transaction_details)
        """
        pass
    
    @abstractmethod
    async def get_token_balance(
        self,
        chain: str,
        address: str,
        token_address: Optional[str] = None
    ) -> str:
        """Get token balance for an address"""
        pass
    
    @abstractmethod
    async def estimate_fees(
        self,
        transfer: BridgeTransfer
    ) -> Dict[str, str]:
        """
        Estimate fees for the transfer
        
        Returns:
            Dict with 'lock_fee', 'mint_fee', 'bridge_fee' in smallest units
        """
        pass
    
    @abstractmethod
    async def create_attestation(
        self,
        transfer: BridgeTransfer,
        lock_tx_hash: str,
        validator_key: str
    ) -> BridgeAttestation:
        """Create an attestation for a completed lock transaction"""
        pass
    
    @abstractmethod
    async def verify_attestation(
        self,
        attestation: BridgeAttestation,
        transfer: BridgeTransfer
    ) -> bool:
        """Verify an attestation signature"""
        pass
    
    async def refund_tokens(
        self,
        transfer: BridgeTransfer,
        reason: str,
        private_key: Optional[str] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Refund locked tokens if transfer fails
        Default implementation - can be overridden
        """
        self.logger.info(f"Refunding transfer {transfer.transfer_id}: {reason}")
        # Implementation depends on specific bridge design
        raise NotImplementedError("Refund not implemented for this bridge")
    
    async def get_wrapped_token_address(
        self,
        source_token: str,
        source_chain: str,
        target_chain: str
    ) -> Optional[str]:
        """Get wrapped token address on target chain"""
        # Default implementation - can be overridden
        token_key = f"{source_chain}:{source_token}"
        return self.config.get('wrapped_tokens', {}).get(token_key)
    
    async def check_bridge_limits(
        self,
        transfer: BridgeTransfer
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if transfer meets bridge limits
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        amount = int(transfer.amount)
        min_amount = int(self.config.get('min_amount', '0'))
        max_amount = int(self.config.get('max_amount', '999999999999999999999999'))
        
        if amount < min_amount:
            return False, f"Amount below minimum: {min_amount}"
        
        if amount > max_amount:
            return False, f"Amount exceeds maximum: {max_amount}"
        
        return True, None
    
    async def get_confirmations_required(self, chain: str) -> int:
        """Get number of confirmations required for a chain"""
        if chain == self.source_chain:
            return self.config.get('source_confirmations', 12)
        elif chain == self.target_chain:
            return self.config.get('target_confirmations', 12)
        else:
            return 12  # Default
    
    def calculate_bridge_fee(self, amount: str) -> str:
        """Calculate bridge fee based on amount"""
        fee_percentage = self.config.get('fee_percentage', 0.1)
        amount_int = int(amount)
        fee = int(amount_int * fee_percentage / 100)
        return str(fee)
    
    async def is_contract_paused(self, chain: str) -> bool:
        """Check if bridge contract is paused"""
        # Default implementation - should be overridden
        return False
    
    async def get_nonce_for_transfer(
        self,
        transfer_id: str,
        chain: str
    ) -> Optional[int]:
        """Get nonce for a specific transfer"""
        # Can be used for replay protection
        return None
    
    def create_bridge_event(
        self,
        transfer_id: str,
        event_type: str,
        chain: str,
        data: Dict[str, Any],
        tx_hash: Optional[str] = None,
        block_number: Optional[int] = None
    ) -> BridgeEvent:
        """Create a bridge event"""
        return BridgeEvent(
            event_id=f"{transfer_id}-{event_type}-{datetime.utcnow().timestamp()}",
            transfer_id=transfer_id,
            event_type=event_type,
            chain=chain,
            transaction_hash=tx_hash,
            block_number=block_number,
            data=data
        ) 