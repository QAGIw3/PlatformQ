"""
Analytics client plugins

Plugins for analytics and processing engines.
"""

from typing import Dict, Type
from ...base_plugin import ClientPlugin

# Plugin registry for analytics
_plugins: Dict[str, Type[ClientPlugin]] = {}

def register_plugin(name: str, plugin_class: Type[ClientPlugin]):
    """Register an analytics plugin"""
    _plugins[name] = plugin_class

def get_plugin(name: str) -> Type[ClientPlugin]:
    """Get an analytics plugin by name"""
    return _plugins.get(name)

__all__ = ["register_plugin", "get_plugin"] 