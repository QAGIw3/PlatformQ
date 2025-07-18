"""
Base voting strategy interface and types.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Any, List


class VotingMechanism(Enum):
    """Supported voting mechanisms"""
    SIMPLE = "SIMPLE"
    QUADRATIC = "QUADRATIC"
    TIME_WEIGHTED = "TIME_WEIGHTED"
    CONVICTION = "CONVICTION"
    RANKED_CHOICE = "RANKED_CHOICE"
    DELEGATION = "DELEGATION"


class VotingStrategy(ABC):
    """Abstract base class for voting strategies"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    @abstractmethod
    async def calculate_voting_power(
        self, 
        voter: str, 
        base_power: int,
        proposal_data: Dict[str, Any],
        chain_id: str
    ) -> int:
        """
        Calculate the voting power for a voter on a specific proposal.
        
        Args:
            voter: Voter address
            base_power: Base voting power (e.g., token balance)
            proposal_data: Proposal-specific data
            chain_id: Blockchain identifier
            
        Returns:
            Calculated voting power
        """
        pass
    
    @abstractmethod
    async def aggregate_votes(
        self,
        votes: List[Dict[str, Any]],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Aggregate votes according to the strategy.
        
        Args:
            votes: List of individual votes
            proposal_data: Proposal-specific data
            
        Returns:
            Aggregated voting results
        """
        pass
    
    @abstractmethod
    async def validate_vote(
        self,
        voter: str,
        vote_data: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> bool:
        """
        Validate a vote according to strategy rules.
        
        Args:
            voter: Voter address
            vote_data: Vote details
            proposal_data: Proposal-specific data
            
        Returns:
            True if vote is valid
        """
        pass
    
    @abstractmethod
    async def calculate_outcome(
        self,
        aggregated_votes: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calculate the final outcome of voting.
        
        Args:
            aggregated_votes: Aggregated voting data
            proposal_data: Proposal-specific data
            
        Returns:
            Voting outcome with details
        """
        pass
    
    def get_mechanism_metadata(self) -> Dict[str, Any]:
        """Get metadata about this voting mechanism"""
        return {
            "name": self.__class__.__name__,
            "config": self.config
        } 