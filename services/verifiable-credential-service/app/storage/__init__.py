"""
Storage module for Verifiable Credential Service

Provides decentralized storage capabilities using IPFS.
"""

from .ipfs_storage import (
    IPFSCredentialStorage,
    DistributedCredentialNetwork
)

__all__ = [
    'IPFSCredentialStorage',
    'DistributedCredentialNetwork'
] 