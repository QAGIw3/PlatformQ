"""
Partner Capacity Management API endpoints
"""

from typing import List, Dict, Any, Optional
from decimal import Decimal
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.main import partner_capacity_manager, wholesale_arbitrage_engine


router = APIRouter(prefix="/api/v1/partners", tags=["partner-capacity"])


class PartnerContractRequest(BaseModel):
    """Request to register a partner contract"""
    provider: str
    type: str
    tiers: List[List[str]]  # [(volume, discount)]
    regions: List[str]
    resources: List[str]
    minimum: str
    duration_days: int
    penalty: Optional[str] = "0"


class CapacityPurchaseRequest(BaseModel):
    """Request to purchase wholesale capacity"""
    provider: str
    resource_type: str
    region: str
    quantity: str
    duration_hours: int
    start_date: Optional[datetime] = None


class InventoryResponse(BaseModel):
    """Capacity inventory response"""
    provider: str
    region: str
    resource_type: str
    total_capacity: str
    available_capacity: str
    allocated_capacity: str
    wholesale_price: str
    retail_price: str
    utilization_rate: str
    last_updated: datetime


@router.post("/contracts")
async def register_partner_contract(
    request: PartnerContractRequest,
    current_user: dict = Depends(get_current_user)
):
    """Register a new partner wholesale contract"""
    if not partner_capacity_manager:
        raise HTTPException(status_code=503, detail="Partner capacity manager not initialized")
    
    try:
        # Convert string decimals to proper format
        tiers = [(Decimal(t[0]), Decimal(t[1])) for t in request.tiers]
        
        contract_details = {
            "type": request.type,
            "tiers": tiers,
            "regions": request.regions,
            "resources": request.resources,
            "minimum": request.minimum,
            "duration_days": request.duration_days,
            "penalty": request.penalty
        }
        
        contract = await partner_capacity_manager.register_contract(
            request.provider,
            contract_details
        )
        
        return {
            "contract_id": contract.partner_id,
            "provider": contract.provider.value,
            "type": contract.contract_type,
            "regions": contract.regions_available,
            "resources": contract.resource_types,
            "minimum_commitment": str(contract.minimum_commitment),
            "start_date": contract.start_date.isoformat(),
            "end_date": contract.end_date.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/best-price")
async def get_best_wholesale_price(
    resource_type: str = Query(..., description="Type of compute resource"),
    region: str = Query(..., description="Region/location"),
    quantity: str = Query(..., description="Quantity needed"),
    duration_hours: int = Query(..., description="Duration in hours"),
    current_user: dict = Depends(get_current_user)
):
    """Get best wholesale price across all partners"""
    if not partner_capacity_manager:
        raise HTTPException(status_code=503, detail="Partner capacity manager not initialized")
    
    try:
        provider, price = await partner_capacity_manager.get_best_price(
            resource_type,
            region,
            Decimal(quantity),
            timedelta(hours=duration_hours)
        )
        
        if not provider:
            raise HTTPException(status_code=404, detail="No providers available")
        
        return {
            "provider": provider.value,
            "wholesale_price": str(price),
            "total_cost": str(price * Decimal(quantity) * duration_hours),
            "resource_type": resource_type,
            "region": region,
            "quantity": quantity,
            "duration_hours": duration_hours
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/purchase")
async def purchase_wholesale_capacity(
    request: CapacityPurchaseRequest,
    current_user: dict = Depends(get_current_user)
):
    """Purchase wholesale capacity from a partner"""
    if not partner_capacity_manager:
        raise HTTPException(status_code=503, detail="Partner capacity manager not initialized")
    
    try:
        order = await partner_capacity_manager.purchase_capacity(
            request.provider,
            request.resource_type,
            request.region,
            Decimal(request.quantity),
            timedelta(hours=request.duration_hours),
            request.start_date
        )
        
        return {
            "order_id": order.order_id,
            "provider": order.provider.value,
            "status": order.status,
            "quantity": str(order.quantity),
            "wholesale_price": str(order.wholesale_price),
            "total_cost": str(order.total_cost),
            "start_date": order.start_date.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventory")
async def get_available_inventory(
    resource_type: str = Query(..., description="Type of compute resource"),
    region: Optional[str] = Query(None, description="Region/location filter"),
    current_user: dict = Depends(get_current_user)
):
    """Get available capacity inventory"""
    if not partner_capacity_manager:
        raise HTTPException(status_code=503, detail="Partner capacity manager not initialized")
    
    try:
        inventory_list = await partner_capacity_manager.get_available_inventory(
            resource_type,
            region
        )
        
        return {
            "total_inventory": len(inventory_list),
            "inventory": [
                InventoryResponse(
                    provider=inv.provider.value,
                    region=inv.region,
                    resource_type=inv.resource_type,
                    total_capacity=str(inv.total_capacity),
                    available_capacity=str(inv.available_capacity),
                    allocated_capacity=str(inv.allocated_capacity),
                    wholesale_price=str(inv.wholesale_price),
                    retail_price=str(inv.retail_price),
                    utilization_rate=str(inv.utilization_rate),
                    last_updated=inv.last_updated
                )
                for inv in inventory_list
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/allocate")
async def allocate_from_inventory(
    resource_type: str,
    region: str,
    quantity: str,
    current_user: dict = Depends(get_current_user)
):
    """Allocate capacity from existing inventory"""
    if not partner_capacity_manager:
        raise HTTPException(status_code=503, detail="Partner capacity manager not initialized")
    
    try:
        allocation = await partner_capacity_manager.allocate_from_inventory(
            resource_type,
            region,
            Decimal(quantity)
        )
        
        if not allocation:
            raise HTTPException(
                status_code=404,
                detail="Insufficient capacity available in inventory"
            )
        
        return {
            "allocation_id": allocation["allocation_id"],
            "provider": allocation["provider"].value,
            "wholesale_price": str(allocation["wholesale_price"]),
            "quantity": quantity,
            "region": region,
            "resource_type": resource_type
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/arbitrage/opportunities")
async def get_arbitrage_opportunities(
    current_user: dict = Depends(get_current_user)
):
    """Get current arbitrage opportunities"""
    if not wholesale_arbitrage_engine:
        raise HTTPException(status_code=503, detail="Arbitrage engine not initialized")
    
    try:
        opportunities = []
        for opp in wholesale_arbitrage_engine.opportunities.values():
            # Only show recent opportunities
            if opp.created_at > datetime.utcnow() - timedelta(hours=1):
                opportunities.append({
                    "opportunity_id": opp.opportunity_id,
                    "provider": opp.provider.value,
                    "resource_type": opp.resource_type,
                    "region": opp.region,
                    "quantity": str(opp.quantity),
                    "wholesale_price": str(opp.wholesale_price),
                    "futures_price": str(opp.futures_price),
                    "expected_profit": str(opp.expected_profit),
                    "profit_margin": str(opp.profit_margin),
                    "risk_score": opp.risk_score,
                    "confidence": opp.confidence,
                    "created_at": opp.created_at.isoformat()
                })
        
        # Sort by expected profit
        opportunities.sort(key=lambda x: float(x["expected_profit"]), reverse=True)
        
        return {
            "total_opportunities": len(opportunities),
            "opportunities": opportunities[:20]  # Top 20
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/arbitrage/metrics")
async def get_arbitrage_metrics(
    current_user: dict = Depends(get_current_user)
):
    """Get arbitrage performance metrics"""
    if not wholesale_arbitrage_engine:
        raise HTTPException(status_code=503, detail="Arbitrage engine not initialized")
    
    try:
        metrics = await wholesale_arbitrage_engine.get_arbitrage_metrics()
        return metrics
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/arbitrage/execute/{opportunity_id}")
async def execute_arbitrage_opportunity(
    opportunity_id: str,
    current_user: dict = Depends(get_current_user)
):
    """Manually execute a specific arbitrage opportunity"""
    if not wholesale_arbitrage_engine:
        raise HTTPException(status_code=503, detail="Arbitrage engine not initialized")
    
    try:
        opportunity = wholesale_arbitrage_engine.opportunities.get(opportunity_id)
        if not opportunity:
            raise HTTPException(status_code=404, detail="Opportunity not found")
        
        execution = await wholesale_arbitrage_engine.execute_arbitrage(opportunity)
        
        return {
            "execution_id": execution.execution_id,
            "opportunity_id": execution.opportunity_id,
            "status": execution.status,
            "wholesale_order_id": execution.wholesale_order_id,
            "futures_contract_id": execution.futures_contract_id,
            "quantity_purchased": str(execution.quantity_purchased),
            "wholesale_cost": str(execution.wholesale_cost),
            "expected_revenue": str(execution.expected_revenue)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 