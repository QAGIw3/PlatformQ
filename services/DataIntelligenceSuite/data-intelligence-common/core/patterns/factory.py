"""
Factory pattern implementations for object creation.
"""

from abc import ABC
from typing import Any, Dict, Type, TypeVar, Optional, Callable, List
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

T = TypeVar('T')


class Factory(ABC):
    """Abstract factory interface"""
    
    def create(self, *args, **kwargs) -> Any:
        """Create object"""
        raise NotImplementedError(f"Factory {self.__class__.__name__} must implement create method")


class AbstractFactory(ABC):
    """Abstract factory for families of objects"""
    
    def create_product_a(self) -> Any:
        """Create product A"""
        raise NotImplementedError(f"Factory {self.__class__.__name__} must implement create_product_a")
        
    def create_product_b(self) -> Any:
        """Create product B"""
        raise NotImplementedError(f"Factory {self.__class__.__name__} must implement create_product_b")


class FactoryRegistry:
    """Registry for managing factories"""
    
    def __init__(self):
        self._factories: Dict[str, Factory] = {}
        self._aliases: Dict[str, str] = {}
        
    def register(self, name: str, factory: Factory, aliases: Optional[List[str]] = None):
        """Register factory with optional aliases"""
        if name in self._factories:
            raise ValueError(f"Factory '{name}' already registered")
            
        self._factories[name] = factory
        logger.info(f"Registered factory: {name}")
        
        # Register aliases
        if aliases:
            for alias in aliases:
                if alias in self._aliases:
                    raise ValueError(f"Alias '{alias}' already registered")
                self._aliases[alias] = name
                logger.info(f"Registered alias '{alias}' for factory '{name}'")
        
    def unregister(self, name: str):
        """Unregister factory"""
        if name not in self._factories:
            raise KeyError(f"Factory '{name}' not found")
            
        # Remove factory
        del self._factories[name]
        
        # Remove aliases
        aliases_to_remove = [alias for alias, factory_name in self._aliases.items() if factory_name == name]
        for alias in aliases_to_remove:
            del self._aliases[alias]
            
        logger.info(f"Unregistered factory: {name}")
        
    def get(self, name: str) -> Factory:
        """Get factory by name or alias"""
        # Check if it's an alias
        if name in self._aliases:
            name = self._aliases[name]
            
        if name not in self._factories:
            available = ", ".join(self._factories.keys())
            raise KeyError(f"Factory '{name}' not found. Available: {available}")
            
        return self._factories[name]
        
    def create(self, name: str, *args, **kwargs) -> Any:
        """Create object using named factory"""
        factory = self.get(name)
        return factory.create(*args, **kwargs)
        
    def list_factories(self) -> List[str]:
        """List all registered factory names"""
        return list(self._factories.keys())
        
    def list_aliases(self) -> Dict[str, str]:
        """List all aliases and their factory names"""
        return dict(self._aliases)


class Builder(ABC):
    """Builder pattern for complex object construction"""
    
    def __init__(self):
        self._reset()
        
    def _reset(self):
        """Reset builder to initial state"""
        self._product = None
        
    def build(self) -> Any:
        """Build the object"""
        if self._product is None:
            raise RuntimeError("Cannot build: product not initialized. Call builder methods first.")
        
        product = self._product
        self._reset()  # Reset for next build
        return product


class GenericFactory(Factory):
    """Generic factory that uses a creation function"""
    
    def __init__(self, creation_func: Callable[..., T], validator: Optional[Callable[[T], bool]] = None):
        self._creation_func = creation_func
        self._validator = validator
        
    def create(self, *args, **kwargs) -> T:
        """Create object using creation function"""
        obj = self._creation_func(*args, **kwargs)
        
        # Validate if validator provided
        if self._validator and not self._validator(obj):
            raise ValueError(f"Created object failed validation")
            
        return obj


class SingletonFactory(Factory):
    """Factory that ensures only one instance is created"""
    
    def __init__(self, cls: Type[T]):
        self._cls = cls
        self._instance: Optional[T] = None
        
    def create(self, *args, **kwargs) -> T:
        """Create or return existing instance"""
        if self._instance is None:
            self._instance = self._cls(*args, **kwargs)
            logger.info(f"Created singleton instance of {self._cls.__name__}")
        else:
            if args or kwargs:
                logger.warning(f"Singleton {self._cls.__name__} already created, ignoring arguments")
                
        return self._instance
        
    def reset(self):
        """Reset singleton instance"""
        self._instance = None


class PrototypeFactory(Factory):
    """Factory that clones a prototype object"""
    
    def __init__(self, prototype: T):
        self._prototype = prototype
        
    def create(self, *args, **kwargs) -> T:
        """Clone prototype object"""
        import copy
        
        # Deep copy the prototype
        obj = copy.deepcopy(self._prototype)
        
        # Apply any modifications from kwargs
        for key, value in kwargs.items():
            if hasattr(obj, key):
                setattr(obj, key, value)
                
        return obj


@dataclass
class FactoryConfig:
    """Configuration for factory creation"""
    factory_type: str
    params: Dict[str, Any]
    aliases: Optional[List[str]] = None


class ConfigurableFactory(Factory):
    """Factory that can be configured dynamically"""
    
    def __init__(self, config: FactoryConfig, registry: FactoryRegistry):
        self.config = config
        self.registry = registry
        self._base_factory: Optional[Factory] = None
        
    def _get_base_factory(self) -> Factory:
        """Get or create base factory"""
        if self._base_factory is None:
            factory_type = self.config.factory_type
            
            if factory_type == "generic":
                # Create generic factory from config
                creation_func = self.config.params.get("creation_func")
                if not creation_func:
                    raise ValueError("Generic factory requires 'creation_func' parameter")
                self._base_factory = GenericFactory(creation_func)
                
            elif factory_type == "singleton":
                # Create singleton factory
                cls = self.config.params.get("class")
                if not cls:
                    raise ValueError("Singleton factory requires 'class' parameter")
                self._base_factory = SingletonFactory(cls)
                
            elif factory_type == "prototype":
                # Create prototype factory
                prototype = self.config.params.get("prototype")
                if not prototype:
                    raise ValueError("Prototype factory requires 'prototype' parameter")
                self._base_factory = PrototypeFactory(prototype)
                
            else:
                # Try to get from registry
                self._base_factory = self.registry.get(factory_type)
                
        return self._base_factory
        
    def create(self, *args, **kwargs) -> Any:
        """Create object using configured factory"""
        factory = self._get_base_factory()
        
        # Merge config params with provided kwargs
        merged_kwargs = {**self.config.params, **kwargs}
        
        return factory.create(*args, **merged_kwargs)


class ChainedBuilder(Builder):
    """Builder with method chaining support"""
    
    def __init__(self):
        super().__init__()
        self._steps: List[Callable] = []
        
    def add_step(self, step: Callable) -> 'ChainedBuilder':
        """Add build step"""
        self._steps.append(step)
        return self
        
    def with_property(self, name: str, value: Any) -> 'ChainedBuilder':
        """Add property to product"""
        def set_property(product):
            setattr(product, name, value)
            return product
            
        return self.add_step(set_property)
        
    def build(self) -> Any:
        """Build product by applying all steps"""
        if not self._product:
            raise RuntimeError("Product not initialized. Set product before building.")
            
        # Apply all steps
        product = self._product
        for step in self._steps:
            product = step(product)
            
        # Reset for next build
        self._reset()
        return product
        
    def _reset(self):
        """Reset builder state"""
        super()._reset()
        self._steps = []
        
    def set_product(self, product: Any) -> 'ChainedBuilder':
        """Set base product"""
        self._product = product
        return self 