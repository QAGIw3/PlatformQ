"""
Base adapter implementation with common functionality.
"""

import logging
from typing import Optional, Dict, Any, List
from abc import ABC
from decimal import Decimal

from ..types import ChainType, ChainConfig, TransactionResult, GasEstimate, ChainMetadata
from ..interfaces import IBlockchainAdapter
from ..models import Transaction, SmartContract

logger = logging.getLogger(__name__)


class BaseAdapter(IBlockchainAdapter, ABC):
    """Base adapter with common functionality"""
    
    def __init__(self, config: ChainConfig):
        self.config = config
        self._chain_type = config.chain_type
        self._chain_id = config.chain_id
        self._connected = False
        
    @property
    def chain_type(self) -> ChainType:
        return self._chain_type
        
    @property 
    def chain_id(self) -> int:
        return self._chain_id
        
    @property
    def is_connected(self) -> bool:
        return self._connected
        
    async def get_chain_metadata(self) -> ChainMetadata:
        """Get current chain metadata"""
        # Base implementation - override in subclasses
        block_number = await self.get_block_number()
        return ChainMetadata(
            chain_type=self.chain_type,
            latest_block=block_number,
            syncing=False,
            peer_count=1,
            gas_price=0  # Override in subclass
        ) 