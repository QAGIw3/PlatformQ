"""
Integration Hub Connectors

Provides database and service connectors for the Integration Hub.
"""

from typing import Dict, Type
import importlib
import pkgutil
from pathlib import Path

from data_intelligence_common.core.patterns.factory import PluginFactory
from data_intelligence_common.monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

# Connector factory
connector_factory = PluginFactory("connector")


def load_connectors():
    """Dynamically load all connector plugins"""
    # Get the directory containing connectors
    connectors_dir = Path(__file__).parent
    
    # Import all Python modules in this directory
    for finder, name, ispkg in pkgutil.iter_modules([str(connectors_dir)]):
        if name.startswith('_'):
            continue
            
        try:
            # Import the module
            module = importlib.import_module(f'.{name}', package=__name__)
            
            # Check for connector class and type
            if hasattr(module, '__connector_class__') and hasattr(module, '__connector_type__'):
                connector_class = getattr(module, '__connector_class__')
                connector_type = getattr(module, '__connector_type__')
                
                # Register with factory
                connector_factory.register(connector_type, connector_class)
                logger.info(f"Registered connector: {connector_type}")
                
        except Exception as e:
            logger.error(f"Failed to load connector {name}: {e}")


# Load all connectors on import
load_connectors()

# Export factory
__all__ = ['connector_factory', 'load_connectors'] 