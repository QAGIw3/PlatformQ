"""
Strategy pattern implementation for algorithm selection.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Type


class Strategy(ABC):
    """Abstract strategy interface"""
    
    @abstractmethod
    def execute(self, *args, **kwargs) -> Any:
        """Execute strategy"""
        pass


class StrategyContext:
    """Context for strategy execution"""
    
    def __init__(self, strategy: Strategy = None):
        self._strategy = strategy
        
    def set_strategy(self, strategy: Strategy):
        """Set active strategy"""
        self._strategy = strategy
        
    def execute(self, *args, **kwargs) -> Any:
        """Execute current strategy"""
        if not self._strategy:
            raise ValueError("No strategy set")
        return self._strategy.execute(*args, **kwargs)


class StrategyRegistry:
    """Registry for managing strategies"""
    
    def __init__(self):
        self._strategies: Dict[str, Strategy] = {}
        
    def register(self, name: str, strategy: Strategy):
        """Register strategy"""
        self._strategies[name] = strategy
        
    def get(self, name: str) -> Strategy:
        """Get strategy by name"""
        return self._strategies[name]
        
    def execute(self, name: str, *args, **kwargs) -> Any:
        """Execute named strategy"""
        strategy = self.get(name)
        return strategy.execute(*args, **kwargs) 