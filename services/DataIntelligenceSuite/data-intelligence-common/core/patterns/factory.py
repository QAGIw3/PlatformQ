"""
Factory pattern implementations for object creation.

This module consolidates all factory patterns from across the codebase into a
unified implementation.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Type, TypeVar, Optional, Callable, List, Union, Tuple, Set
import logging
from dataclasses import dataclass, field
from enum import Enum
import importlib
import pkgutil
import threading
import copy

logger = logging.getLogger(__name__)

T = TypeVar('T')


class Factory(ABC):
    """Abstract factory interface"""
    
    @abstractmethod
    def create(self, *args, **kwargs) -> Any:
        """Create object"""
        pass


class AbstractFactory(ABC):
    """Abstract factory for families of objects"""
    
    @abstractmethod
    def create_product_a(self) -> Any:
        """Create product A"""
        pass
        
    @abstractmethod
    def create_product_b(self) -> Any:
        """Create product B"""
        pass


class FactoryRegistry:
    """
    Registry for managing factories.
    
    This consolidates factory registry functionality from:
    - core/patterns/factory.py (original)
    - clients/base_plugin.py (PluginRegistry)
    - core/pipelines/executors.py (ExecutorFactory)
    """
    
    def __init__(self):
        self._factories: Dict[str, Factory] = {}
        self._aliases: Dict[str, str] = {}
        self._lock = threading.Lock()
        
    def register(self, name: str, factory: Union[Factory, Type, Callable], 
                 aliases: Optional[List[str]] = None, replace: bool = False):
        """
        Register factory with optional aliases.
        
        Args:
            name: Name to register factory under
            factory: Factory instance, class, or callable
            aliases: Optional list of aliases
            replace: Whether to replace existing factory
        """
        with self._lock:
            if name in self._factories and not replace:
                raise ValueError(f"Factory '{name}' already registered")
                
            # Wrap non-Factory objects
            if not isinstance(factory, Factory):
                if isinstance(factory, type):
                    # It's a class - wrap in GenericFactory
                    factory = GenericFactory(lambda *args, **kwargs: factory(*args, **kwargs))
                elif callable(factory):
                    # It's a callable - wrap in GenericFactory
                    factory = GenericFactory(factory)
                else:
                    raise TypeError(f"Factory must be Factory instance, class, or callable")
                
            self._factories[name] = factory
            logger.info(f"Registered factory: {name}")
            
            # Register aliases
            if aliases:
                for alias in aliases:
                    if alias in self._aliases and not replace:
                        raise ValueError(f"Alias '{alias}' already registered")
                    self._aliases[alias] = name
                    logger.info(f"Registered alias '{alias}' for factory '{name}'")
        
    def unregister(self, name: str):
        """Unregister factory"""
        with self._lock:
            if name not in self._factories:
                raise KeyError(f"Factory '{name}' not found")
                
            # Remove factory
            del self._factories[name]
            
            # Remove aliases
            aliases_to_remove = [alias for alias, factory_name in self._aliases.items() 
                               if factory_name == name]
            for alias in aliases_to_remove:
                del self._aliases[alias]
                
            logger.info(f"Unregistered factory: {name}")
        
    def get(self, name: str) -> Factory:
        """Get factory by name or alias"""
        with self._lock:
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
        with self._lock:
            return list(self._factories.keys())
        
    def list_aliases(self) -> Dict[str, str]:
        """List all aliases and their factory names"""
        with self._lock:
            return dict(self._aliases)
            
    def has_factory(self, name: str) -> bool:
        """Check if factory exists"""
        with self._lock:
            return name in self._factories or name in self._aliases


# Global registry instance
_global_registry = FactoryRegistry()


def get_global_registry() -> FactoryRegistry:
    """Get the global factory registry"""
    return _global_registry


class Builder(ABC):
    """Builder pattern for complex object construction"""
    
    def __init__(self):
        self._reset()
        
    @abstractmethod
    def _reset(self):
        """Reset builder to initial state"""
        pass
        
    @abstractmethod
    def build(self) -> Any:
        """Build the object"""
        pass


class GenericFactory(Factory):
    """Generic factory that uses a creation function"""
    
    def __init__(self, creation_func: Callable[..., T], 
                 validator: Optional[Callable[[T], bool]] = None):
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
        self._lock = threading.Lock()
        
    def create(self, *args, **kwargs) -> T:
        """Create or return existing instance"""
        if self._instance is None:
            with self._lock:
                # Double-check locking
                if self._instance is None:
                    self._instance = self._cls(*args, **kwargs)
                    logger.info(f"Created singleton instance of {self._cls.__name__}")
        else:
            if args or kwargs:
                logger.warning(f"Singleton {self._cls.__name__} already created, ignoring arguments")
                
        return self._instance
        
    def reset(self):
        """Reset singleton instance"""
        with self._lock:
            self._instance = None


class PrototypeFactory(Factory):
    """Factory that clones a prototype object"""
    
    def __init__(self, prototype: T):
        self._prototype = prototype
        
    def create(self, *args, **kwargs) -> T:
        """Clone prototype object"""
        # Deep copy the prototype
        obj = copy.deepcopy(self._prototype)
        
        # Apply any modifications from kwargs
        for key, value in kwargs.items():
            if hasattr(obj, key):
                setattr(obj, key, value)
                
        return obj


class PooledFactory(Factory):
    """Factory that maintains a pool of reusable objects"""
    
    def __init__(self, cls: Type[T], pool_size: int = 10, 
                 reset_func: Optional[Callable[[T], None]] = None):
        self._cls = cls
        self._pool_size = pool_size
        self._reset_func = reset_func
        self._pool: List[T] = []
        self._in_use: Set[T] = set()
        self._lock = threading.Lock()
        
    def create(self, *args, **kwargs) -> T:
        """Get object from pool or create new one"""
        with self._lock:
            # Try to get from pool
            if self._pool:
                obj = self._pool.pop()
                self._in_use.add(obj)
                
                # Reset object if reset function provided
                if self._reset_func:
                    self._reset_func(obj)
                    
                return obj
                
            # Create new object if pool is empty
            if len(self._in_use) < self._pool_size:
                obj = self._cls(*args, **kwargs)
                self._in_use.add(obj)
                return obj
                
            raise RuntimeError(f"Pool exhausted (size: {self._pool_size})")
            
    def release(self, obj: T):
        """Return object to pool"""
        with self._lock:
            if obj in self._in_use:
                self._in_use.remove(obj)
                self._pool.append(obj)


@dataclass
class FactoryConfig:
    """Configuration for factory creation"""
    factory_type: str
    params: Dict[str, Any] = field(default_factory=dict)
    aliases: Optional[List[str]] = None


class ConfigurableFactory(Factory):
    """Factory that can be configured dynamically"""
    
    def __init__(self, config: FactoryConfig, registry: Optional[FactoryRegistry] = None):
        self.config = config
        self.registry = registry or get_global_registry()
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
                
            elif factory_type == "pooled":
                # Create pooled factory
                cls = self.config.params.get("class")
                pool_size = self.config.params.get("pool_size", 10)
                reset_func = self.config.params.get("reset_func")
                if not cls:
                    raise ValueError("Pooled factory requires 'class' parameter")
                self._base_factory = PooledFactory(cls, pool_size, reset_func)
                
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
        self._product = None
        
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
        self._product = None
        self._steps = []
        
    def set_product(self, product: Any) -> 'ChainedBuilder':
        """Set base product"""
        self._product = product
        return self


class PluginFactory(Factory):
    """
    Factory for plugin-based architecture.
    
    This consolidates plugin loading functionality from clients/factory.py
    """
    
    def __init__(self, plugin_interface: Type, plugin_paths: List[str]):
        self.plugin_interface = plugin_interface
        self.plugin_paths = plugin_paths
        self._plugins: Dict[str, Type] = {}
        self._discovered = False
        
    def discover_plugins(self):
        """Discover and register all available plugins"""
        if self._discovered:
            return
            
        for plugin_path in self.plugin_paths:
            try:
                # Import the module
                module = importlib.import_module(plugin_path)
                
                # Discover all modules in the package
                if hasattr(module, '__path__'):
                    for importer, modname, ispkg in pkgutil.iter_modules(module.__path__):
                        if not ispkg:
                            try:
                                # Import the module
                                full_module_name = f"{plugin_path}.{modname}"
                                plugin_module = importlib.import_module(full_module_name)
                                
                                # Find classes that implement the plugin interface
                                for name, obj in vars(plugin_module).items():
                                    if (isinstance(obj, type) and 
                                        issubclass(obj, self.plugin_interface) and 
                                        obj is not self.plugin_interface):
                                        plugin_name = getattr(obj, 'name', name.lower())
                                        self._plugins[plugin_name] = obj
                                        logger.info(f"Discovered plugin: {plugin_name}")
                                        
                            except Exception as e:
                                logger.warning(f"Failed to load plugin {modname}: {e}")
                                
            except Exception as e:
                logger.warning(f"Failed to discover plugins in {plugin_path}: {e}")
                
        self._discovered = True
        
    def create(self, plugin_name: str, *args, **kwargs) -> Any:
        """Create plugin instance"""
        # Ensure plugins are discovered
        self.discover_plugins()
        
        plugin_class = self._plugins.get(plugin_name)
        if not plugin_class:
            available = ", ".join(self._plugins.keys())
            raise ValueError(f"Plugin '{plugin_name}' not found. Available: {available}")
            
        return plugin_class(*args, **kwargs)
        
    def list_plugins(self) -> List[str]:
        """List available plugin names"""
        self.discover_plugins()
        return list(self._plugins.keys())


class TypedFactory(Factory, Generic[T]):
    """
    Type-safe factory for creating objects of a specific type.
    
    This provides better type hints for IDEs and type checkers.
    """
    
    def __init__(self, base_type: Type[T]):
        self.base_type = base_type
        self._creators: Dict[str, Callable[..., T]] = {}
        
    def register_creator(self, name: str, creator: Callable[..., T]):
        """Register a creator function for a named type"""
        self._creators[name] = creator
        
    def create(self, type_name: str, *args, **kwargs) -> T:
        """Create object of the specified type"""
        creator = self._creators.get(type_name)
        if not creator:
            available = ", ".join(self._creators.keys())
            raise ValueError(f"Type '{type_name}' not registered. Available: {available}")
            
        obj = creator(*args, **kwargs)
        
        # Verify type
        if not isinstance(obj, self.base_type):
            raise TypeError(f"Creator for '{type_name}' returned {type(obj)}, expected {self.base_type}")
            
        return obj


def create_factory(factory_type: str, **kwargs) -> Factory:
    """
    Convenience function to create factories.
    
    Args:
        factory_type: Type of factory to create
        **kwargs: Arguments for factory constructor
        
    Returns:
        Factory instance
    """
    factories = {
        "generic": GenericFactory,
        "singleton": SingletonFactory,
        "prototype": PrototypeFactory,
        "pooled": PooledFactory,
        "plugin": PluginFactory,
        "typed": TypedFactory,
        "configurable": ConfigurableFactory
    }
    
    factory_class = factories.get(factory_type)
    if not factory_class:
        raise ValueError(f"Unknown factory type: {factory_type}")
        
    return factory_class(**kwargs)


# Export all public classes and functions
__all__ = [
    # Base interfaces
    'Factory',
    'AbstractFactory',
    'Builder',
    
    # Registry
    'FactoryRegistry',
    'get_global_registry',
    
    # Factory implementations
    'GenericFactory',
    'SingletonFactory',
    'PrototypeFactory',
    'PooledFactory',
    'ConfigurableFactory',
    'PluginFactory',
    'TypedFactory',
    
    # Builder implementations
    'ChainedBuilder',
    
    # Configuration
    'FactoryConfig',
    
    # Convenience functions
    'create_factory'
] 