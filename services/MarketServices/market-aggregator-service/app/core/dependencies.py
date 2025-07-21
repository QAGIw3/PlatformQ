"""
FastAPI Dependencies for Market Aggregator Service
"""
from fastapi import Header, HTTPException, Depends
from typing import Optional

from ..aggregators.bundle_optimizer import BundleOptimizer
from ..aggregators.arbitrage_detector import ArbitrageDetector
from ..core.market_client import MarketClient
from ..config import settings


# Service instances (initialized at startup)
market_client_instance = None
bundle_optimizer_instance = None
arbitrage_detector_instance = None


def get_market_client() -> MarketClient:
    """Get market client instance"""
    if not market_client_instance:
        raise RuntimeError("Market client not initialized")
    return market_client_instance


def get_bundle_optimizer() -> BundleOptimizer:
    """Get bundle optimizer instance"""
    if not bundle_optimizer_instance:
        raise RuntimeError("Bundle optimizer not initialized")
    return bundle_optimizer_instance


def get_arbitrage_detector() -> ArbitrageDetector:
    """Get arbitrage detector instance"""
    if not arbitrage_detector_instance:
        raise RuntimeError("Arbitrage detector not initialized")
    return arbitrage_detector_instance


async def verify_api_key(
    x_api_key: Optional[str] = Header(None)
) -> str:
    """Verify API key for protected endpoints"""
    # In production, would validate against stored keys
    # For now, just check if header is present
    if not x_api_key:
        return "anonymous"
    
    return x_api_key 