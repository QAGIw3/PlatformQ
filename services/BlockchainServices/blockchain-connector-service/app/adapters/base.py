"""
Base adapter for blockchain connections
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import logging

from ..models.chain_types import ChainType
from ..config import ChainConfig

logger = logging.getLogger(__name__)


class BaseChainAdapter(ABC):
    """Base class for blockchain adapters"""
    
    def __init__(self, chain_type: ChainType, config: ChainConfig):
        self.chain_type = chain_type
        self.config = config
        self._connected = False
        self.current_endpoint = None
        
    @abstractmethod
    async def connect(self) -> bool:
        """Connect to the blockchain"""
        pass
        
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the blockchain"""
        pass
        
    @abstractmethod
    async def get_latest_block(self) -> int:
        """Get the latest block number"""
        pass
        
    @abstractmethod
    async def get_balance(
        self,
        address: str,
        token_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get balance for an address
        
        Args:
            address: Wallet address
            token_address: Token contract address (None for native balance)
            
        Returns:
            Balance information including amount and decimals
        """
        pass
        
    @abstractmethod
    async def get_transaction(self, tx_hash: str) -> Dict[str, Any]:
        """
        Get transaction details
        
        Args:
            tx_hash: Transaction hash
            
        Returns:
            Transaction details
        """
        pass
        
    @abstractmethod
    async def broadcast_transaction(self, signed_tx: str) -> str:
        """
        Broadcast a signed transaction
        
        Args:
            signed_tx: Signed transaction data
            
        Returns:
            Transaction hash
        """
        pass
        
    @abstractmethod
    async def estimate_gas(
        self,
        from_address: str,
        to_address: str,
        value: str,
        data: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Estimate gas for a transaction
        
        Args:
            from_address: Sender address
            to_address: Recipient address
            value: Amount to send
            data: Transaction data
            
        Returns:
            Gas estimate including gas limit and gas price
        """
        pass
        
    @abstractmethod
    async def get_gas_price(self) -> Dict[str, Any]:
        """
        Get current gas price
        
        Returns:
            Gas price information
        """
        pass
        
    @abstractmethod
    async def get_nonce(self, address: str) -> int:
        """
        Get next nonce for an address
        
        Args:
            address: Wallet address
            
        Returns:
            Next nonce
        """
        pass
        
    @abstractmethod
    async def call_contract(
        self,
        contract_address: str,
        method: str,
        params: List[Any],
        abi: List[Dict[str, Any]]
    ) -> Any:
        """
        Call a smart contract method (read-only)
        
        Args:
            contract_address: Contract address
            method: Method name
            params: Method parameters
            abi: Contract ABI
            
        Returns:
            Method result
        """
        pass
        
    @property
    def is_connected(self) -> bool:
        """Check if adapter is connected"""
        return self._connected
        
    def get_best_endpoint(self) -> Optional[str]:
        """Get the best available endpoint"""
        if not self.config.endpoints:
            return None
            
        # Sort by priority and health
        sorted_endpoints = sorted(
            self.config.endpoints,
            key=lambda e: (e.priority, -getattr(e, 'health_score', 1.0))
        )
        
        return sorted_endpoints[0].url if sorted_endpoints else None
        
    async def validate_address(self, address: str) -> bool:
        """
        Validate blockchain address format
        
        Args:
            address: Address to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Default implementation - override in specific adapters
        return bool(address)
        
    async def get_block(self, block_number: int) -> Dict[str, Any]:
        """
        Get block details
        
        Args:
            block_number: Block number
            
        Returns:
            Block details
        """
        # Default implementation - override if needed
        raise NotImplementedError("get_block not implemented")
        
    async def get_logs(
        self,
        from_block: int,
        to_block: int,
        address: Optional[str] = None,
        topics: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get logs/events
        
        Args:
            from_block: Starting block
            to_block: Ending block
            address: Contract address filter
            topics: Topic filters
            
        Returns:
            List of logs
        """
        # Default implementation - override if needed
        raise NotImplementedError("get_logs not implemented") 