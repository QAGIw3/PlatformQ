"""Internal API endpoints for service-to-service communication."""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from ..dependencies import (
    get_state_manager,
    get_event_processor,
    get_matching_engine,
    get_derivatives_adapter,
    get_compute_adapter
)
from ..integrations import DerivativesAdapter, ComputeMarketAdapter
from ..integrations.compute_market_adapter import ComputeResourceType, ComputeMarketType


logger = logging.getLogger(__name__)


router = APIRouter(prefix="/internal", tags=["internal"])


# Derivatives endpoints

@router.post("/derivatives/register-market")
async def register_derivatives_market(
    market_id: str,
    product_type: str,
    contract_specs: Dict[str, Any],
    adapter: DerivativesAdapter = Depends(get_derivatives_adapter)
) -> Dict[str, Any]:
    """Register a derivatives market - internal use only"""
    try:
        success = await adapter.register_derivatives_market(
            market_id=market_id,
            product_type=product_type,
            contract_specs=contract_specs
        )
        
        return {
            "success": success,
            "market_id": market_id
        }
        
    except Exception as e:
        logger.error(f"Failed to register derivatives market: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/derivatives/submit-order")
async def submit_derivatives_order(
    order_data: Dict[str, Any],
    neuromorphic_hint: Optional[Dict[str, Any]] = None,
    adapter: DerivativesAdapter = Depends(get_derivatives_adapter)
) -> Dict[str, Any]:
    """Submit derivatives order - internal use only"""
    try:
        result = await adapter.submit_derivatives_order(
            order_data=order_data,
            neuromorphic_hint=neuromorphic_hint
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to submit derivatives order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/derivatives/orderbook")
async def get_derivatives_orderbook(
    market_id: str,
    depth: int = 20,
    aggregate: bool = False,
    adapter: DerivativesAdapter = Depends(get_derivatives_adapter)
) -> Dict[str, Any]:
    """Get derivatives orderbook - internal use only"""
    try:
        orderbook = await adapter.get_derivatives_orderbook(
            market_id=market_id,
            depth=depth,
            aggregate=aggregate
        )
        
        if not orderbook:
            raise HTTPException(status_code=404, detail="Market not found")
        
        return orderbook
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get derivatives orderbook: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/derivatives/settlement")
async def trigger_derivatives_settlement(
    market_id: str,
    settlement_price: str,
    adapter: DerivativesAdapter = Depends(get_derivatives_adapter)
) -> Dict[str, Any]:
    """Trigger derivatives settlement - internal use only"""
    try:
        result = await adapter.trigger_settlement(
            market_id=market_id,
            settlement_price=Decimal(settlement_price)
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to trigger settlement: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Compute market endpoints

@router.post("/compute/create-market")
async def create_compute_market(
    resource_type: str,
    market_type: str,
    specifications: Dict[str, Any],
    adapter: ComputeMarketAdapter = Depends(get_compute_adapter)
) -> Dict[str, Any]:
    """Create compute market - internal use only"""
    try:
        market_id = await adapter.create_compute_market(
            resource_type=ComputeResourceType(resource_type),
            market_type=ComputeMarketType(market_type),
            specifications=specifications
        )
        
        return {
            "success": True,
            "market_id": market_id
        }
        
    except Exception as e:
        logger.error(f"Failed to create compute market: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/compute/register-provider")
async def register_compute_provider(
    provider_id: str,
    resources: Dict[str, Dict[str, Any]],
    adapter: ComputeMarketAdapter = Depends(get_compute_adapter)
) -> Dict[str, Any]:
    """Register compute provider - internal use only"""
    try:
        # Convert string keys to enum types
        typed_resources = {}
        for resource_str, specs in resources.items():
            try:
                resource_type = ComputeResourceType(resource_str)
                typed_resources[resource_type] = specs
            except ValueError:
                logger.warning(f"Skipping unknown resource type: {resource_str}")
        
        success = await adapter.register_provider(
            provider_id=provider_id,
            resources=typed_resources
        )
        
        return {
            "success": success,
            "provider_id": provider_id
        }
        
    except Exception as e:
        logger.error(f"Failed to register compute provider: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/compute/submit-order")
async def submit_compute_order(
    user_id: str,
    resource_type: str,
    market_type: str,
    quantity: str,
    duration_hours: Optional[int] = None,
    specifications: Optional[Dict[str, Any]] = None,
    adapter: ComputeMarketAdapter = Depends(get_compute_adapter)
) -> Dict[str, Any]:
    """Submit compute order - internal use only"""
    try:
        result = await adapter.submit_compute_order(
            user_id=user_id,
            resource_type=ComputeResourceType(resource_type),
            market_type=ComputeMarketType(market_type),
            quantity=Decimal(quantity),
            duration_hours=duration_hours,
            specifications=specifications
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to submit compute order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/compute/metrics")
async def get_compute_metrics(
    adapter: ComputeMarketAdapter = Depends(get_compute_adapter)
) -> Dict[str, Any]:
    """Get compute market metrics - internal use only"""
    try:
        metrics = await adapter.get_compute_metrics()
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to get compute metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/compute/allocate")
async def allocate_compute_resources(
    allocation_id: str,
    user_id: str,
    resource_type: str,
    quantity: str,
    duration_hours: int,
    specifications: Dict[str, Any],
    adapter: ComputeMarketAdapter = Depends(get_compute_adapter)
) -> Dict[str, Any]:
    """Allocate compute resources - internal use only"""
    try:
        result = await adapter.allocate_resources(
            allocation_id=allocation_id,
            user_id=user_id,
            resource_type=ComputeResourceType(resource_type),
            quantity=Decimal(quantity),
            duration_hours=duration_hours,
            specifications=specifications
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to allocate compute resources: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/compute/release/{allocation_id}")
async def release_compute_resources(
    allocation_id: str,
    adapter: ComputeMarketAdapter = Depends(get_compute_adapter)
) -> Dict[str, Any]:
    """Release compute resources - internal use only"""
    try:
        success = await adapter.release_resources(allocation_id)
        
        return {
            "success": success,
            "allocation_id": allocation_id
        }
        
    except Exception as e:
        logger.error(f"Failed to release compute resources: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Health check for internal services

@router.get("/health")
async def internal_health_check(
    matching_engine = Depends(get_matching_engine)
) -> Dict[str, Any]:
    """Health check for internal services"""
    metrics = matching_engine.get_metrics()
    
    return {
        "status": "healthy",
        "service": "trading-core-service",
        "timestamp": datetime.utcnow().isoformat(),
        "metrics": {
            "orders_processed": metrics["global"]["orders_processed"],
            "active_markets": metrics["global"]["active_markets"],
            "latency_p99_ms": metrics["global"]["latency_ms"]["p99"]
        }
    } 