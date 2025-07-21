"""Oracle data API endpoints"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List, Dict
from datetime import datetime
from decimal import Decimal

from ..oracle.blockchain_oracle_adapter import OracleAggregator
from ..dependencies import get_oracle_aggregator, get_aggregator


router = APIRouter(tags=["oracle"])


@router.get("/price/{asset_pair}")
async def get_oracle_price(
    asset_pair: str,
    aggregator: OracleAggregator = Depends(get_oracle_aggregator)
):
    """
    Get aggregated price from on-chain oracles.
    
    Fetches prices from multiple oracle sources (Chainlink, Band, etc.)
    and returns an aggregated price with confidence scores.
    """
    try:
        result = await aggregator.get_aggregated_price(asset_pair)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sources")
async def list_oracle_sources(
    aggregator: OracleAggregator = Depends(get_oracle_aggregator)
):
    """List available oracle sources and their health status"""
    sources = []
    
    for name, adapter in aggregator.adapters.items():
        try:
            is_healthy = await adapter.is_healthy()
            sources.append({
                "name": name,
                "weight": aggregator.weights.get(name, 1.0),
                "healthy": is_healthy,
                "type": adapter.__class__.__name__
            })
        except:
            sources.append({
                "name": name,
                "weight": aggregator.weights.get(name, 1.0),
                "healthy": False,
                "type": adapter.__class__.__name__
            })
    
    return {"sources": sources}


@router.get("/price/batch")
async def get_oracle_prices_batch(
    asset_pairs: str = Query(..., description="Comma-separated asset pairs"),
    aggregator: OracleAggregator = Depends(get_oracle_aggregator)
):
    """Get oracle prices for multiple asset pairs"""
    pairs = [p.strip() for p in asset_pairs.split(",")]
    results = {}
    errors = {}
    
    for pair in pairs:
        try:
            results[pair] = await aggregator.get_aggregated_price(pair)
        except Exception as e:
            errors[pair] = str(e)
    
    return {
        "prices": results,
        "errors": errors,
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/composite-price/{asset_pair}")
async def get_composite_price(
    asset_pair: str,
    aggregator: OracleAggregator = Depends(get_oracle_aggregator),
    market_aggregator = Depends(get_aggregator)
):
    """
    Get composite price combining off-chain market data and on-chain oracle data.
    
    This provides the most accurate price by combining:
    - Real-time exchange prices from market data
    - On-chain oracle prices from DeFi protocols
    """
    try:
        # Get oracle price
        oracle_data = await aggregator.get_aggregated_price(asset_pair)
        
        # Get market price
        market_state = await market_aggregator.get_market_state(asset_pair)
        
        if market_state and market_state.last_price:
            # Combine both sources
            oracle_price = Decimal(oracle_data["aggregated_price"])
            market_price = market_state.last_price
            
            # Weight: 70% market, 30% oracle (configurable)
            composite_price = (market_price * Decimal("0.7")) + (oracle_price * Decimal("0.3"))
            
            return {
                "asset_pair": asset_pair,
                "composite_price": str(composite_price),
                "market_price": str(market_price),
                "oracle_price": str(oracle_price),
                "oracle_sources": oracle_data["sources"],
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            # No market data, use oracle only
            return {
                "asset_pair": asset_pair,
                "composite_price": oracle_data["aggregated_price"],
                "market_price": None,
                "oracle_price": oracle_data["aggregated_price"],
                "oracle_sources": oracle_data["sources"],
                "timestamp": datetime.utcnow().isoformat()
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 