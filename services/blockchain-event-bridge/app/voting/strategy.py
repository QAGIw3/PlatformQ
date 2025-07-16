"""
Base voting strategy interface
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from enum import Enum
import math


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
        self.mechanism = VotingMechanism.SIMPLE
        
    @abstractmethod
    async def calculate_voting_power(
        self, 
        voter: str, 
        base_power: int,
        proposal_data: Dict[str, Any],
        chain_id: str
    ) -> int:
        """Calculate actual voting power based on the mechanism"""
        pass
    
    @abstractmethod
    async def aggregate_votes(
        self,
        votes: List[Dict[str, Any]],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Aggregate votes according to the mechanism"""
        pass
    
    @abstractmethod
    async def validate_vote(
        self,
        voter: str,
        vote_data: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> bool:
        """Validate if a vote is valid according to the mechanism rules"""
        pass
    
    @abstractmethod
    async def calculate_outcome(
        self,
        aggregated_votes: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate the outcome based on the voting mechanism"""
        pass
    
    def get_mechanism_metadata(self) -> Dict[str, Any]:
        """Get metadata about the voting mechanism"""
        return {
            "mechanism": self.mechanism.value,
            "config": self.config,
            "description": self.__doc__
        } 