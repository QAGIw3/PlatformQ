"""
Domain adapters for the collaboration platform
"""

from .base import BaseDomainAdapter, DomainRegistry, DomainOperation, DomainState, OperationType
from .simulation_adapter import SimulationAdapter
from .cad_adapter import CADAdapter

__all__ = [
    "BaseDomainAdapter",
    "DomainRegistry", 
    "DomainOperation",
    "DomainState",
    "OperationType",
    "SimulationAdapter",
    "CADAdapter"
] 