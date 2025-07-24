"""
Analytics Engine Algorithms

Provides data analysis algorithms for the Analytics Engine.
"""

from typing import Dict, Type
import importlib
import pkgutil
from pathlib import Path

from data_intelligence_common.core.patterns.factory import PluginFactory
from data_intelligence_common.monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

# Algorithm factory
algorithm_factory = PluginFactory("algorithm")


def load_algorithms():
    """Dynamically load all algorithm plugins"""
    # Get the directory containing algorithms
    algorithms_dir = Path(__file__).parent
    
    # Import all Python modules in this directory
    for finder, name, ispkg in pkgutil.iter_modules([str(algorithms_dir)]):
        if name.startswith('_'):
            continue
            
        try:
            # Import the module
            module = importlib.import_module(f'.{name}', package=__name__)
            
            # Check for algorithm class and name
            if hasattr(module, '__algorithm_class__') and hasattr(module, '__algorithm_name__'):
                algorithm_class = getattr(module, '__algorithm_class__')
                algorithm_name = getattr(module, '__algorithm_name__')
                
                # Register with factory
                algorithm_factory.register(algorithm_name, algorithm_class)
                logger.info(f"Registered algorithm: {algorithm_name}")
                
        except Exception as e:
            logger.error(f"Failed to load algorithm {name}: {e}")


# Load all algorithms on import
load_algorithms()

# Export factory and common algorithm names
__all__ = ['algorithm_factory', 'load_algorithms']

# Common algorithm names for easy access
ANOMALY_DETECTION = "anomaly_detection"
FORECASTING = "forecasting"
CLUSTERING = "clustering"
CLASSIFICATION = "classification"
REGRESSION = "regression" 