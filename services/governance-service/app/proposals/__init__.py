"""
Proposals module for governance service
"""

from .repository import ProposalRepository
from .schemas.proposal import Proposal, ProposalCreate, CrossChainProposalCreate

__all__ = [
    "ProposalRepository",
    "Proposal",
    "ProposalCreate",
    "CrossChainProposalCreate"
] 