"""
Storage client plugins

Plugins for object and file storage systems.
"""

from typing import Dict, Type
from ...base_plugin import ClientPlugin

# Plugin registry for storage
_plugins: Dict[str, Type[ClientPlugin]] = {}

def register_plugin(name: str, plugin_class: Type[ClientPlugin]):
    """Register a storage plugin"""
    _plugins[name] = plugin_class

def get_plugin(name: str) -> Type[ClientPlugin]:
    """Get a storage plugin by name"""
    return _plugins.get(name)

__all__ = ["register_plugin", "get_plugin"] 