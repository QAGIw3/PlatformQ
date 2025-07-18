"""
Blockchain adapter factory for creating chain-specific adapters.
"""

import logging
from typing import Dict, Type, Any
from urllib.parse import urlparse

from .interfaces import IBlockchainAdapter
from .types import ChainType, ChainConfig
from .adapters import (
    EVMAdapter,
    SolanaAdapter,
    CosmosAdapter,
    PolkadotAdapter
)

logger = logging.getLogger(__name__)


class AdapterFactory:
    """Factory for creating blockchain adapters"""
    
    # Mapping of chain types to adapter classes
    _adapters: Dict[ChainType, Type[IBlockchainAdapter]] = {
        # EVM-compatible chains
        ChainType.ETHEREUM: EVMAdapter,
        ChainType.POLYGON: EVMAdapter,
        ChainType.ARBITRUM: EVMAdapter,
        ChainType.OPTIMISM: EVMAdapter,
        ChainType.AVALANCHE: EVMAdapter,
        ChainType.BSC: EVMAdapter,
        
        # Non-EVM chains
        ChainType.SOLANA: SolanaAdapter,
        ChainType.COSMOS: CosmosAdapter,
        ChainType.POLKADOT: PolkadotAdapter,
    }
    
    # Chain-specific configuration defaults
    _chain_defaults = {
        ChainType.ETHEREUM: {"chain_id": 1, "currency_symbol": "ETH"},
        ChainType.POLYGON: {"chain_id": 137, "currency_symbol": "MATIC"},
        ChainType.ARBITRUM: {"chain_id": 42161, "currency_symbol": "ETH"},
        ChainType.OPTIMISM: {"chain_id": 10, "currency_symbol": "ETH"},
        ChainType.AVALANCHE: {"chain_id": 43114, "currency_symbol": "AVAX"},
        ChainType.BSC: {"chain_id": 56, "currency_symbol": "BNB"},
        ChainType.SOLANA: {"chain_id": 101, "currency_symbol": "SOL"},
        ChainType.COSMOS: {
            "chain_id": "cosmoshub-4",
            "prefix": "cosmos",
            "denom": "uatom",
            "currency_symbol": "ATOM"
        },
        ChainType.POLKADOT: {
            "chain_name": "Polkadot",
            "currency_symbol": "DOT"
        }
    }
    
    @classmethod
    def create_adapter(cls, config: ChainConfig) -> IBlockchainAdapter:
        """
        Create an adapter instance for the specified chain configuration.
        
        Args:
            config: Chain configuration containing RPC URL and chain type
            
        Returns:
            Configured blockchain adapter instance
            
        Raises:
            ValueError: If chain type is not supported
        """
        if config.chain_type not in cls._adapters:
            raise ValueError(f"Unsupported chain type: {config.chain_type}")
            
        adapter_class = cls._adapters[config.chain_type]
        defaults = cls._chain_defaults.get(config.chain_type, {})
        
        # Parse RPC URL to extract components
        parsed_url = urlparse(config.rpc_url)
        
        # Create adapter based on chain type
        if config.chain_type in [ChainType.ETHEREUM, ChainType.POLYGON, 
                                ChainType.ARBITRUM, ChainType.OPTIMISM,
                                ChainType.AVALANCHE, ChainType.BSC]:
            # EVM chains
            return adapter_class(
                rpc_url=config.rpc_url,
                chain_id=config.chain_id or defaults.get("chain_id", 1),
                private_key=getattr(config, "private_key", None)
            )
            
        elif config.chain_type == ChainType.SOLANA:
            return adapter_class(
                rpc_url=config.rpc_url,
                chain_id=config.chain_id or defaults.get("chain_id", 101)
            )
            
        elif config.chain_type == ChainType.COSMOS:
            return adapter_class(
                rpc_url=config.rpc_url,
                chain_id=getattr(config, "cosmos_chain_id", defaults.get("chain_id")),
                prefix=getattr(config, "prefix", defaults.get("prefix")),
                denom=getattr(config, "denom", defaults.get("denom"))
            )
            
        elif config.chain_type == ChainType.POLKADOT:
            # Polkadot uses websocket URLs
            ws_url = config.rpc_url
            if ws_url.startswith("http"):
                ws_url = ws_url.replace("http", "ws", 1)
                
            return adapter_class(
                ws_url=ws_url,
                chain_name=getattr(config, "chain_name", defaults.get("chain_name"))
            )
            
        else:
            raise ValueError(f"No implementation for chain type: {config.chain_type}")
            
    @classmethod
    def register_adapter(cls, chain_type: ChainType, 
                        adapter_class: Type[IBlockchainAdapter]):
        """
        Register a custom adapter implementation for a chain type.
        
        Args:
            chain_type: The blockchain type
            adapter_class: The adapter class to use
        """
        cls._adapters[chain_type] = adapter_class
        logger.info(f"Registered adapter {adapter_class.__name__} for {chain_type.value}")
        
    @classmethod
    def get_supported_chains(cls) -> list[ChainType]:
        """Get list of supported chain types"""
        return list(cls._adapters.keys())
        
    @classmethod
    def is_chain_supported(cls, chain_type: ChainType) -> bool:
        """Check if a chain type is supported"""
        return chain_type in cls._adapters 