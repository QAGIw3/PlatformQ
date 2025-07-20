"""
Gas Optimization API endpoints
"""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from ..models.optimization import (
    OptimizationRequest, GasRecommendation, GasPricePrediction,
    OptimizationMetrics
)
from ..core.gas_optimizer import GasOptimizer

router = APIRouter(prefix="/api/v1", tags=["optimization"])


# Dependency to get gas optimizer
def get_optimizer(request) -> GasOptimizer:
    """Get gas optimizer instance"""
    return request.app.state.gas_optimizer


@router.post("/optimize")
async def optimize_gas(
    request: OptimizationRequest,
    optimizer: GasOptimizer = Depends(get_optimizer)
) -> GasRecommendation:
    """Get gas optimization recommendation"""
    try:
        recommendation = await optimizer.optimize(request)
        return recommendation
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/predict/{chain}")
async def predict_gas_prices(
    chain: str,
    horizon_minutes: int = Query(60, ge=5, le=1440, description="Prediction horizon in minutes"),
    optimizer: GasOptimizer = Depends(get_optimizer)
) -> GasPricePrediction:
    """Predict future gas prices"""
    try:
        prediction = await optimizer.predict_gas_prices(chain, horizon_minutes)
        return prediction
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prices/{chain}")
async def get_current_prices(
    chain: str,
    optimizer: GasOptimizer = Depends(get_optimizer)
) -> dict:
    """Get current gas prices for a chain"""
    try:
        prices = await optimizer._get_current_gas_prices(chain)
        
        return {
            "chain": chain,
            "prices": prices,
            "updated_at": optimizer._gas_prices.get(chain, {}).get('updated_at')
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategies")
async def get_available_strategies() -> dict:
    """Get list of available optimization strategies"""
    return {
        "strategies": [
            {
                "name": "standard",
                "description": "Standard gas pricing for immediate execution"
            },
            {
                "name": "batch",
                "description": "Batch multiple transactions to save gas"
            },
            {
                "name": "meta_transaction",
                "description": "Use relayers to pay gas on your behalf"
            },
            {
                "name": "l2_migration",
                "description": "Migrate to Layer 2 for lower costs"
            },
            {
                "name": "time_based",
                "description": "Wait for optimal gas prices based on patterns"
            }
        ]
    }


@router.get("/relayers/{chain}")
async def get_relayers(
    chain: str,
    optimizer: GasOptimizer = Depends(get_optimizer)
) -> dict:
    """Get available relayers for a chain"""
    relayers = optimizer.meta_tx_strategy.settings.RELAYER_ADDRESSES.get(chain, [])
    
    return {
        "chain": chain,
        "relayers": [
            {
                "address": addr,
                "stats": optimizer.meta_tx_strategy._relayer_stats.get(f"{chain}:{addr}", {})
            }
            for addr in relayers
        ]
    }


@router.get("/l2-options/{chain}")
async def get_l2_options(
    chain: str,
    optimizer: GasOptimizer = Depends(get_optimizer)
) -> dict:
    """Get Layer 2 options for a chain"""
    l2_options = optimizer.l2_strategy._l2_mappings.get(chain, [])
    
    return {
        "chain": chain,
        "l2_options": [
            {
                "chain": l2,
                "cost_multiplier": optimizer.settings.L2_COST_MULTIPLIER.get(l2, 0.1),
                "bridge_info": optimizer.l2_strategy._bridge_info.get((chain, l2), {})
            }
            for l2 in l2_options
        ]
    }


@router.get("/metrics")
async def get_metrics(
    chain: Optional[str] = Query(None, description="Filter by chain"),
    optimizer: GasOptimizer = Depends(get_optimizer)
) -> OptimizationMetrics:
    """Get optimization metrics"""
    # TODO: Implement metrics aggregation
    # This would collect metrics from Prometheus or calculate from stored data
    
    from datetime import datetime, timedelta
    
    return OptimizationMetrics(
        total_optimizations=0,
        total_gas_saved="0",
        average_savings_percentage=0.0,
        strategy_usage={},
        strategy_savings={},
        average_response_time=0.0,
        model_accuracy=0.0,
        period_start=datetime.utcnow() - timedelta(hours=24),
        period_end=datetime.utcnow()
    )


@router.get("/health")
async def health_check(
    optimizer: GasOptimizer = Depends(get_optimizer)
) -> dict:
    """Health check endpoint"""
    return {
        "status": "healthy" if optimizer._running else "unhealthy",
        "strategies": {
            "batch": True,
            "meta_tx": True,
            "time_based": True,
            "l2": True
        },
        "chains_tracked": list(optimizer._gas_prices.keys()),
        "models_trained": list(optimizer._prediction_models.keys())
    } 