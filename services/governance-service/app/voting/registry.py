"""
Registry for voting strategies.
"""

import logging
from typing import Dict, Optional, Type

from .strategy import VotingStrategy, VotingMechanism
from .simple import SimpleVoting
from .quadratic import QuadraticVoting
from .conviction import ConvictionVoting
from .delegation import VoteDelegation
from .time_weighted import TimeWeightedVoting

logger = logging.getLogger(__name__)


class VotingStrategyRegistry:
    """Registry for voting strategy implementations"""
    
    def __init__(self):
        self._strategies: Dict[VotingMechanism, Type[VotingStrategy]] = {}
        self._instances: Dict[VotingMechanism, VotingStrategy] = {}
        
        # Register default strategies
        self._register_defaults()
        
    def _register_defaults(self):
        """Register default voting strategies"""
        self.register(VotingMechanism.SIMPLE, SimpleVoting)
        self.register(VotingMechanism.QUADRATIC, QuadraticVoting)
        self.register(VotingMechanism.CONVICTION, ConvictionVoting)
        self.register(VotingMechanism.DELEGATION, VoteDelegation)
        self.register(VotingMechanism.TIME_WEIGHTED, TimeWeightedVoting)
        
    def register(self, mechanism: VotingMechanism, strategy_class: Type[VotingStrategy]):
        """Register a voting strategy"""
        self._strategies[mechanism] = strategy_class
        logger.info(f"Registered voting strategy: {mechanism.value}")
        
    def get_strategy(self, mechanism: VotingMechanism, config: Dict) -> Optional[VotingStrategy]:
        """Get or create a voting strategy instance"""
        if mechanism not in self._strategies:
            logger.error(f"Unknown voting mechanism: {mechanism}")
            return None
            
        # Create instance if not exists
        if mechanism not in self._instances:
            strategy_class = self._strategies[mechanism]
            self._instances[mechanism] = strategy_class(config)
            logger.info(f"Created voting strategy instance: {mechanism.value}")
            
        return self._instances[mechanism]
        
    def get_available_mechanisms(self) -> list[VotingMechanism]:
        """Get list of available voting mechanisms"""
        return list(self._strategies.keys()) 