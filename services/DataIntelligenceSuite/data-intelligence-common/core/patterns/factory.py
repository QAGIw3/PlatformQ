"""
Factory pattern implementations for object creation.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Type, TypeVar

T = TypeVar('T')


class Factory(ABC):
    """Abstract factory interface"""
    
    @abstractmethod
    def create(self, *args, **kwargs) -> Any:
        """Create object"""
        pass


class AbstractFactory(ABC):
    """Abstract factory for families of objects"""
    pass


class FactoryRegistry:
    """Registry for managing factories"""
    
    def __init__(self):
        self._factories: Dict[str, Factory] = {}
        
    def register(self, name: str, factory: Factory):
        """Register factory"""
        self._factories[name] = factory
        
    def get(self, name: str) -> Factory:
        """Get factory by name"""
        return self._factories[name]
        
    def create(self, name: str, *args, **kwargs) -> Any:
        """Create object using named factory"""
        factory = self.get(name)
        return factory.create(*args, **kwargs)


class Builder(ABC):
    """Builder pattern for complex object construction"""
    
    @abstractmethod
    def build(self) -> Any:
        """Build the object"""
        pass 