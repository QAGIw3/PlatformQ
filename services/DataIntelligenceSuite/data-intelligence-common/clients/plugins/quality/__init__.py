"""
Data Quality client plugins

Plugins for data quality and validation tools.
"""

from typing import Dict, Type
from ...base_plugin import ClientPlugin

# Plugin registry for quality
_plugins: Dict[str, Type[ClientPlugin]] = {}

def register_plugin(name: str, plugin_class: Type[ClientPlugin]):
    """Register a quality plugin"""
    _plugins[name] = plugin_class

def get_plugin(name: str) -> Type[ClientPlugin]:
    """Get a quality plugin by name"""
    return _plugins.get(name)

__all__ = ["register_plugin", "get_plugin"] 