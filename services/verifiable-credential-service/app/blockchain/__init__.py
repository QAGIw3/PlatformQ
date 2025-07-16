# Blockchain integration package for multi-chain support
from .base import BlockchainClient, ChainType, CredentialAnchor
from .ethereum import EthereumClient
from .polygon import PolygonClient
from .fabric import FabricClient
from .bridge import (
    CrossChainBridge,
    AtomicCrossChainBridge,
    BridgeStatus,
    BridgeRequest
)

__all__ = [
    "BlockchainClient",
    "ChainType",
    "CredentialAnchor",
    "EthereumClient", 
    "PolygonClient",
    "FabricClient",
    "CrossChainBridge",
    "AtomicCrossChainBridge",
    "BridgeStatus",
    "BridgeRequest"
] 