"""
FastAPI Dependencies
"""
from typing import Generator
from ..services import (
    PathRegistryService,
    BandwidthManagerService,
    CircuitManagerService,
    PricingEngineService
)


# Service instances (initialized at startup)
path_registry_service = None
bandwidth_manager_service = None
circuit_manager_service = None
pricing_engine_service = None


def get_path_registry() -> PathRegistryService:
    """Get path registry service instance"""
    if not path_registry_service:
        raise RuntimeError("Path registry service not initialized")
    return path_registry_service


def get_bandwidth_manager() -> BandwidthManagerService:
    """Get bandwidth manager service instance"""
    if not bandwidth_manager_service:
        raise RuntimeError("Bandwidth manager service not initialized")
    return bandwidth_manager_service


def get_circuit_manager() -> CircuitManagerService:
    """Get circuit manager service instance"""
    if not circuit_manager_service:
        raise RuntimeError("Circuit manager service not initialized")
    return circuit_manager_service


def get_pricing_engine() -> PricingEngineService:
    """Get pricing engine service instance"""
    if not pricing_engine_service:
        raise RuntimeError("Pricing engine service not initialized")
    return pricing_engine_service 