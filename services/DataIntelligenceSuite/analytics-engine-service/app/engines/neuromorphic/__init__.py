"""
Neuromorphic Computing Engine

Provides brain-inspired computing capabilities using spiking neural networks.
"""

from .neuromorphic_engine import (
    NeuromorphicEngine,
    NeuromorphicConfig,
    NeuromorphicFramework,
    SpikeCoding,
    NeuronModel,
    SpikeEvent,
    NeuromorphicMetrics
)
from .models import (
    SpikingNeuralNetwork,
    SpikingLayer
)
from .spike_processing import (
    SpikeEncoder,
    SpikeDecoder,
    SpikePattern
)

__all__ = [
    # Main engine
    "NeuromorphicEngine",
    
    # Configuration
    "NeuromorphicConfig",
    "NeuromorphicFramework",
    "SpikeCoding", 
    "NeuronModel",
    
    # Data structures
    "SpikeEvent",
    "NeuromorphicMetrics",
    
    # Models
    "SpikingNeuralNetwork",
    "SpikingLayer",
    
    # Processing
    "SpikeEncoder",
    "SpikeDecoder",
    "SpikePattern"
] 