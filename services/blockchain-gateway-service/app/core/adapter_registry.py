"""
Registry for managing blockchain adapters.
"""

import logging
from typing import Dict, Optional, List, Type
from platformq_blockchain_common import (
    ChainType, ChainConfig, IBlockchainAdapter,
    ConnectionPool
)
from platformq_blockchain_common.adapters import EVMAdapter

logger = logging.getLogger(__name__)


class AdapterRegistry:
    """
    Registry for blockchain adapters.
    Manages adapter instances and provides factory functionality.
    """
    
    def __init__(self, connection_pool: ConnectionPool):
        self.connection_pool = connection_pool
        self._adapter_classes: Dict[ChainType, Type[IBlockchainAdapter]] = {}
        self._configs: Dict[ChainType, ChainConfig] = {}
        
        # Register default adapters
        self._register_defaults()
        
    def _register_defaults(self):
        """Register default adapter implementations"""
        # EVM-compatible chains
        evm_chains = [
            ChainType.ETHEREUM,
            ChainType.POLYGON,
            ChainType.ARBITRUM,
            ChainType.OPTIMISM,
            ChainType.AVALANCHE,
            ChainType.BSC
        ]
        
        for chain_type in evm_chains:
            self._adapter_classes[chain_type] = EVMAdapter
            
        # TODO: Add other chain types when implemented
        # self._adapter_classes[ChainType.SOLANA] = SolanaAdapter
        # self._adapter_classes[ChainType.COSMOS] = CosmosAdapter
        # self._adapter_classes[ChainType.NEAR] = NEARAdapter
        
    def register_adapter_class(self, chain_type: ChainType, 
                             adapter_class: Type[IBlockchainAdapter]):
        """Register a custom adapter class for a chain type"""
        self._adapter_classes[chain_type] = adapter_class
        logger.info(f"Registered adapter class {adapter_class.__name__} for {chain_type.value}")
        
    def register_chain_config(self, config: ChainConfig):
        """Register configuration for a chain"""
        self._configs[config.chain_type] = config
        
        # Also register with connection pool
        self.connection_pool.register_chain(config)
        
        logger.info(f"Registered configuration for {config.chain_type.value}")
        
    def get_adapter_class(self, chain_type: ChainType) -> Optional[Type[IBlockchainAdapter]]:
        """Get adapter class for a chain type"""
        return self._adapter_classes.get(chain_type)
        
    def get_config(self, chain_type: ChainType) -> Optional[ChainConfig]:
        """Get configuration for a chain"""
        return self._configs.get(chain_type)
        
    def is_chain_supported(self, chain_type: ChainType) -> bool:
        """Check if a chain is supported"""
        return chain_type in self._adapter_classes and chain_type in self._configs
        
    def get_supported_chains(self) -> List[ChainType]:
        """Get list of supported chains"""
        return list(set(self._adapter_classes.keys()) & set(self._configs.keys()))
        
    async def create_adapter(self, chain_type: ChainType) -> IBlockchainAdapter:
        """Create an adapter instance for a chain"""
        if not self.is_chain_supported(chain_type):
            raise ValueError(f"Chain {chain_type.value} is not supported")
            
        adapter_class = self._adapter_classes[chain_type]
        config = self._configs[chain_type]
        
        # Create adapter instance
        adapter = adapter_class(config)
        
        logger.info(f"Created adapter for {chain_type.value}")
        return adapter 