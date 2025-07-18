"""
Cross-Chain Liquidity API

Endpoints for cross-chain liquidity aggregation with compliance.
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from app.liquidity.cross_chain_aggregator import (
    CrossChainLiquidityAggregator,
    Chain,
    ExecutionType,
    ExecutionRoute
)
from app.auth import get_current_user
from platformq_shared import ProcessingStatus

router = APIRouter(
    prefix="/api/v1/liquidity",
    tags=["liquidity"]
)

# Initialize aggregator
liquidity_aggregator = CrossChainLiquidityAggregator()


class FindRouteRequest(BaseModel):
    """Request to find best execution route"""
    token_in: str
    token_out: str
    amount_in: Decimal = Field(..., gt=0)
    execution_type: ExecutionType = ExecutionType.SMART_ROUTE
    max_slippage: Decimal = Field(0.01, ge=0, le=0.1)  # Max 10%
    preferred_chains: Optional[List[Chain]] = None


class ExecuteOrderRequest(BaseModel):
    """Request to execute cross-chain order"""
    route_id: str
    amount_in: Decimal = Field(..., gt=0)
    deadline_minutes: Optional[int] = Field(None, ge=1, le=60)


class LiquidityDepthRequest(BaseModel):
    """Request for liquidity depth"""
    token_a: str
    token_b: str
    chains: Optional[List[Chain]] = None


class ArbitrageMonitorRequest(BaseModel):
    """Request to monitor arbitrage opportunities"""
    token_pairs: List[Tuple[str, str]] = Field(..., min_items=1, max_items=20)
    min_profit_threshold: Decimal = Field(50, gt=0)


@router.post("/find-route")
async def find_best_route(
    request: FindRouteRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Find the best execution route across chains
    
    Analyzes liquidity across multiple chains and protocols to find
    the optimal execution path while respecting compliance requirements.
    """
    try:
        result = await liquidity_aggregator.find_best_execution_route(
            tenant_id=current_user["tenant_id"],
            user_id=current_user["user_id"],
            token_in=request.token_in,
            token_out=request.token_out,
            amount_in=request.amount_in,
            execution_type=request.execution_type,
            max_slippage=request.max_slippage,
            preferred_chains=request.preferred_chains
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "routes": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/execute")
async def execute_cross_chain_order(
    request: ExecuteOrderRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Execute a cross-chain order using a specific route
    
    Executes the order across multiple chains, handling bridging
    and compliance checks automatically.
    """
    try:
        # Get route from cache (would be stored from find-route)
        # For now, create a mock route
        route = ExecutionRoute(
            route_id=request.route_id,
            total_cost=request.amount_in,
            expected_output=request.amount_in * Decimal("0.98"),
            price_impact=Decimal("0.01"),
            total_fees=request.amount_in * Decimal("0.005"),
            steps=[],  # Would have actual steps
            chains_involved=[Chain.ETHEREUM],
            estimated_time=timedelta(minutes=5),
            expires_at=datetime.utcnow() + timedelta(minutes=10),
            compliance_checked=True,
            required_kyc_tier=1,
            slippage_tolerance=Decimal("0.01"),
            confidence_score=0.95
        )
        
        # Set deadline
        deadline = None
        if request.deadline_minutes:
            deadline = datetime.utcnow() + timedelta(minutes=request.deadline_minutes)
        
        result = await liquidity_aggregator.execute_cross_chain_order(
            tenant_id=current_user["tenant_id"],
            user_id=current_user["user_id"],
            route=route,
            amount_in=request.amount_in,
            deadline=deadline
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "execution": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/liquidity-depth")
async def get_liquidity_depth(
    request: LiquidityDepthRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get aggregated liquidity depth across chains
    
    Shows the combined order book depth from all available
    liquidity sources across specified chains.
    """
    try:
        result = await liquidity_aggregator.get_liquidity_depth(
            token_pair=(request.token_a, request.token_b),
            chains=request.chains
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "depth": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/arbitrage/monitor")
async def monitor_arbitrage(
    request: ArbitrageMonitorRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Monitor cross-chain arbitrage opportunities
    
    Identifies price discrepancies across chains that can be
    profitably arbitraged after accounting for fees and compliance.
    """
    try:
        result = await liquidity_aggregator.monitor_arbitrage_opportunities(
            tenant_id=current_user["tenant_id"],
            user_id=current_user["user_id"],
            token_pairs=request.token_pairs,
            min_profit_threshold=request.min_profit_threshold
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "opportunities": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/supported-chains")
async def get_supported_chains(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get list of supported chains with their capabilities
    """
    try:
        chains = []
        
        for chain in Chain:
            chains.append({
                "chain": chain.value,
                "name": chain.name.title(),
                "features": {
                    "spot_trading": True,
                    "derivatives": chain in [Chain.ETHEREUM, Chain.ARBITRUM, Chain.POLYGON],
                    "lending": chain in [Chain.ETHEREUM, Chain.POLYGON, Chain.AVALANCHE],
                    "compliance_enabled": True
                },
                "avg_block_time": {
                    Chain.ETHEREUM: 12,
                    Chain.POLYGON: 2,
                    Chain.ARBITRUM: 0.25,
                    Chain.AVALANCHE: 2,
                    Chain.BSC: 3,
                    Chain.SOLANA: 0.4
                }.get(chain, 5),
                "gas_token": {
                    Chain.ETHEREUM: "ETH",
                    Chain.POLYGON: "MATIC",
                    Chain.ARBITRUM: "ETH",
                    Chain.AVALANCHE: "AVAX",
                    Chain.BSC: "BNB",
                    Chain.SOLANA: "SOL"
                }.get(chain, "ETH")
            })
        
        return {
            "status": "success",
            "chains": chains,
            "total": len(chains)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/bridge-fees")
async def get_bridge_fees(
    from_chain: Optional[Chain] = None,
    to_chain: Optional[Chain] = None,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get current bridge fees between chains
    """
    try:
        fees = []
        
        # Get all bridge fees
        for (chain1, chain2), fee in liquidity_aggregator.bridge_fees.items():
            if from_chain and chain1 != from_chain:
                continue
            if to_chain and chain2 != to_chain:
                continue
                
            fees.append({
                "from_chain": chain1.value,
                "to_chain": chain2.value,
                "fee_percentage": f"{float(fee) * 100:.2f}%",
                "fee_decimal": str(fee),
                "estimated_time_minutes": 5 if chain1 == Chain.ETHEREUM else 2
            })
        
        return {
            "status": "success",
            "bridge_fees": fees,
            "note": "Fees may vary based on network congestion"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/gas-prices")
async def get_current_gas_prices(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get current gas prices across chains
    """
    try:
        gas_prices = []
        
        for chain, price in liquidity_aggregator.gas_prices.items():
            gas_prices.append({
                "chain": chain.value,
                "gas_price": str(price),
                "unit": "Gwei" if chain != Chain.SOLANA else "SOL",
                "estimated_swap_cost_usd": str(
                    liquidity_aggregator._estimate_gas_cost(chain)
                )
            })
        
        return {
            "status": "success",
            "gas_prices": gas_prices,
            "updated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/simulate")
async def simulate_execution(
    request: FindRouteRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Simulate order execution without actually executing
    
    Useful for showing users expected results before committing.
    """
    try:
        # Find route
        route_result = await liquidity_aggregator.find_best_execution_route(
            tenant_id=current_user["tenant_id"],
            user_id=current_user["user_id"],
            token_in=request.token_in,
            token_out=request.token_out,
            amount_in=request.amount_in,
            execution_type=request.execution_type,
            max_slippage=request.max_slippage,
            preferred_chains=request.preferred_chains
        )
        
        if route_result.status != ProcessingStatus.SUCCESS:
            raise HTTPException(status_code=400, detail=route_result.error)
        
        best_route = route_result.data["route"]
        
        # Simulate execution
        simulation = {
            "input": {
                "token": request.token_in,
                "amount": str(request.amount_in)
            },
            "output": {
                "token": request.token_out,
                "expected_amount": best_route["expected_output"],
                "minimum_amount": str(
                    Decimal(best_route["expected_output"]) * 
                    (Decimal("1") - request.max_slippage)
                )
            },
            "route_summary": {
                "steps": len(best_route["steps"]),
                "chains": best_route["chains"],
                "estimated_time_seconds": best_route["estimated_time_seconds"],
                "total_fees": best_route["total_fees"]
            },
            "breakdown": {
                "protocol_fees": str(Decimal(best_route["total_fees"]) * Decimal("0.3")),
                "gas_fees": str(Decimal(best_route["total_fees"]) * Decimal("0.5")),
                "bridge_fees": str(Decimal(best_route["total_fees"]) * Decimal("0.2"))
            },
            "price_impact": best_route["price_impact"],
            "effective_price": str(
                request.amount_in / Decimal(best_route["expected_output"])
            )
        }
        
        return {
            "status": "success",
            "simulation": simulation
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 