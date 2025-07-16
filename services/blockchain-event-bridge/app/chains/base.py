"""
Base chain adapter for multi-chain support
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Callable
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ChainType(Enum):
    """Supported blockchain types"""
    ETHEREUM = "ETHEREUM"
    POLYGON = "POLYGON"
    ARBITRUM = "ARBITRUM"
    SOLANA = "SOLANA"
    HYPERLEDGER = "HYPERLEDGER"
    COSMOS = "COSMOS"
    NEAR = "NEAR"
    AVALANCHE = "AVALANCHE"


class ChainAdapter(ABC):
    """Base class for blockchain adapters"""
    
    def __init__(self, chain_type: ChainType, chain_id: str, config: Dict[str, Any]):
        self.chain_type = chain_type
        self.chain_id = chain_id
        self.config = config
        self.event_handlers = {}
        self._connected = False
        
    @abstractmethod
    async def connect(self) -> bool:
        """Connect to the blockchain"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the blockchain"""
        pass
    
    @abstractmethod
    async def subscribe_to_events(self, contract_address: str, event_name: str, 
                                 handler: Callable[[Dict[str, Any]], None]) -> bool:
        """Subscribe to specific contract events"""
        pass
    
    @abstractmethod
    async def get_latest_block(self) -> int:
        """Get the latest block number"""
        pass
    
    @abstractmethod
    async def get_reputation_balance(self, address: str) -> int:
        """Get reputation/voting power for an address"""
        pass
    
    @abstractmethod
    async def submit_proposal(self, proposal_data: Dict[str, Any]) -> str:
        """Submit a proposal to the chain"""
        pass
    
    @abstractmethod
    async def cast_vote(self, proposal_id: str, support: bool, 
                       voter_address: str, signature: Optional[str] = None) -> str:
        """Cast a vote on a proposal"""
        pass
    
    @abstractmethod
    async def get_proposal_state(self, proposal_id: str) -> Dict[str, Any]:
        """Get current state of a proposal"""
        pass
    
    @abstractmethod
    async def get_voting_power(self, address: str, block_number: Optional[int] = None) -> int:
        """Get voting power at a specific block"""
        pass
    
    @abstractmethod
    async def execute_proposal(self, proposal_id: str) -> str:
        """Execute a passed proposal"""
        pass
    
    def register_event_handler(self, event_name: str, handler: Callable):
        """Register a handler for a specific event type"""
        if event_name not in self.event_handlers:
            self.event_handlers[event_name] = []
        self.event_handlers[event_name].append(handler)
        
    def emit_event(self, event_name: str, event_data: Dict[str, Any]):
        """Emit an event to all registered handlers"""
        if event_name in self.event_handlers:
            for handler in self.event_handlers[event_name]:
                try:
                    handler(event_data)
                except Exception as e:
                    logger.error(f"Error in event handler for {event_name}: {e}")
    
    @property
    def is_connected(self) -> bool:
        """Check if adapter is connected"""
        return self._connected
    
    def format_address(self, address: str) -> str:
        """Format address for the specific chain"""
        return address
    
    def validate_address(self, address: str) -> bool:
        """Validate address format for the chain"""
        return True 