"""
FastAPI Dependencies for Oracle Service
"""
from fastapi import Header, HTTPException, Depends
from typing import Optional

from ..oracles.quantum_oracle import QuantumOracle
from ..oracles.ai_oracle import AIOracle
from ..oracles.network_oracle import NetworkOracle
from ..config import settings


# Oracle instances (initialized at startup)
quantum_oracle_instance = None
ai_oracle_instance = None
network_oracle_instance = None
quality_aggregator_instance = None
availability_monitor_instance = None
price_aggregator_instance = None
performance_oracle_instance = None


def get_quantum_oracle() -> QuantumOracle:
    """Get quantum oracle instance"""
    if not quantum_oracle_instance:
        raise RuntimeError("Quantum oracle not initialized")
    return quantum_oracle_instance


def get_ai_oracle() -> AIOracle:
    """Get AI oracle instance"""
    if not ai_oracle_instance:
        raise RuntimeError("AI oracle not initialized")
    return ai_oracle_instance


def get_network_oracle() -> NetworkOracle:
    """Get network oracle instance"""
    if not network_oracle_instance:
        raise RuntimeError("Network oracle not initialized")
    return network_oracle_instance


async def verify_api_key(
    x_oracle_api_key: Optional[str] = Header(None)
) -> str:
    """Verify API key for oracle access"""
    if not settings.REQUIRE_API_KEY:
        return "no-auth"
    
    if not x_oracle_api_key:
        raise HTTPException(
            status_code=401,
            detail="API key required"
        )
    
    # In production, would validate against stored keys
    # For now, accept any non-empty key
    if not x_oracle_api_key:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )
    
    return x_oracle_api_key 