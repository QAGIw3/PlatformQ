"""
Dependencies for Oracle Service

Provides dependency injection for oracle components.
"""

from typing import Optional, Dict, Any
from functools import lru_cache
import os
from fastapi import HTTPException, Header

from .core.blockchain import BlockchainClient
from .oracles import (
    QuantumOracle,
    AIOracle,
    NetworkOracle,
    QualityAggregator,
    AvailabilityMonitor,
    PriceAggregator,
    PerformanceOracle
)
from .config import settings


# Cached instances
_blockchain_client: Optional[BlockchainClient] = None
_quantum_oracle: Optional[QuantumOracle] = None
_ai_oracle: Optional[AIOracle] = None
_network_oracle: Optional[NetworkOracle] = None
_quality_aggregator: Optional[QualityAggregator] = None
_availability_monitor: Optional[AvailabilityMonitor] = None
_price_aggregator: Optional[PriceAggregator] = None
_performance_oracle: Optional[PerformanceOracle] = None


@lru_cache()
def get_blockchain_client() -> BlockchainClient:
    """Get blockchain client instance"""
    global _blockchain_client
    
    if _blockchain_client is None:
        _blockchain_client = BlockchainClient(
            rpc_url=settings.BLOCKCHAIN_RPC_URL,
            chain_id=settings.CHAIN_ID,
            private_key=settings.ORACLE_PRIVATE_KEY
        )
    
    return _blockchain_client


async def get_quantum_oracle() -> QuantumOracle:
    """Get quantum oracle instance"""
    global _quantum_oracle
    
    if _quantum_oracle is None:
        blockchain = get_blockchain_client()
        
        _quantum_oracle = QuantumOracle(
            blockchain_client=blockchain,
            oracle_contract_address=settings.QUANTUM_ORACLE_ADDRESS,
            signing_key=settings.ORACLE_SIGNING_KEY
        )
        
        await _quantum_oracle.initialize()
    
    return _quantum_oracle


async def get_ai_oracle() -> AIOracle:
    """Get AI oracle instance"""
    global _ai_oracle
    
    if _ai_oracle is None:
        blockchain = get_blockchain_client()
        
        _ai_oracle = AIOracle(
            blockchain_client=blockchain,
            oracle_contract_address=settings.AI_ORACLE_ADDRESS,
            signing_key=settings.ORACLE_SIGNING_KEY
        )
        
        await _ai_oracle.initialize()
    
    return _ai_oracle


async def get_network_oracle() -> NetworkOracle:
    """Get network oracle instance"""
    global _network_oracle
    
    if _network_oracle is None:
        blockchain = get_blockchain_client()
        
        _network_oracle = NetworkOracle(
            blockchain_client=blockchain,
            oracle_contract_address=settings.NETWORK_ORACLE_ADDRESS,
            signing_key=settings.ORACLE_SIGNING_KEY
        )
        
        await _network_oracle.initialize()
    
    return _network_oracle


async def get_quality_aggregator() -> QualityAggregator:
    """Get quality aggregator instance"""
    global _quality_aggregator
    
    if _quality_aggregator is None:
        blockchain = get_blockchain_client()
        quantum_oracle = await get_quantum_oracle()
        ai_oracle = await get_ai_oracle()
        network_oracle = await get_network_oracle()
        
        _quality_aggregator = QualityAggregator(
            blockchain_client=blockchain,
            quantum_oracle=quantum_oracle,
            ai_oracle=ai_oracle,
            network_oracle=network_oracle,
            oracle_contract_address=settings.QUALITY_ORACLE_ADDRESS,
            signing_key=settings.ORACLE_SIGNING_KEY
        )
    
    return _quality_aggregator


async def get_availability_monitor() -> AvailabilityMonitor:
    """Get availability monitor instance"""
    global _availability_monitor
    
    if _availability_monitor is None:
        blockchain = get_blockchain_client()
        
        _availability_monitor = AvailabilityMonitor(
            blockchain_client=blockchain,
            monitor_contract_address=settings.AVAILABILITY_MONITOR_ADDRESS,
            signing_key=settings.ORACLE_SIGNING_KEY,
            check_interval=settings.AVAILABILITY_CHECK_INTERVAL
        )
        
        await _availability_monitor.initialize()
    
    return _availability_monitor


async def get_price_aggregator() -> PriceAggregator:
    """Get price aggregator instance"""
    global _price_aggregator
    
    if _price_aggregator is None:
        blockchain = get_blockchain_client()
        
        market_addresses = {
            'quantum': settings.QUANTUM_MARKET_ADDRESS,
            'ai': settings.AI_MARKET_ADDRESS,
            'network': settings.NETWORK_MARKET_ADDRESS
        }
        
        amm_addresses = {
            'quantum': settings.QUANTUM_AMM_ADDRESS,
            'ai': settings.AI_AMM_ADDRESS,
            'network': settings.NETWORK_AMM_ADDRESS
        }
        
        _price_aggregator = PriceAggregator(
            blockchain_client=blockchain,
            oracle_contract_address=settings.PRICE_ORACLE_ADDRESS,
            signing_key=settings.ORACLE_SIGNING_KEY,
            market_addresses=market_addresses,
            amm_addresses=amm_addresses
        )
        
        await _price_aggregator.initialize()
    
    return _price_aggregator


async def get_performance_oracle() -> PerformanceOracle:
    """Get performance oracle instance"""
    global _performance_oracle
    
    if _performance_oracle is None:
        blockchain = get_blockchain_client()
        quantum_oracle = await get_quantum_oracle()
        ai_oracle = await get_ai_oracle()
        network_oracle = await get_network_oracle()
        
        _performance_oracle = PerformanceOracle(
            blockchain_client=blockchain,
            oracle_contract_address=settings.PERFORMANCE_ORACLE_ADDRESS,
            signing_key=settings.ORACLE_SIGNING_KEY,
            quantum_oracle=quantum_oracle,
            ai_oracle=ai_oracle,
            network_oracle=network_oracle
        )
    
    return _performance_oracle


async def verify_api_key(x_api_key: str = Header(...)) -> str:
    """
    Verify API key for service-to-service authentication
    """
    valid_api_keys = settings.VALID_API_KEYS
    
    if not valid_api_keys or x_api_key not in valid_api_keys:
        raise HTTPException(
            status_code=403,
            detail="Invalid API key"
        )
    
    return x_api_key


def get_current_user() -> Dict[str, Any]:
    """
    Get current user from authentication.
    In production, this would validate JWT and extract user info.
    """
    # For development, return a test user
    return {
        "address": os.getenv("DEFAULT_USER_ADDRESS", "0x0000000000000000000000000000000000000000"),
        "roles": ["user"]
    } 