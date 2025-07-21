"""
Compute-Backed Stablecoin API endpoints

Stablecoins pegged to computational units (FLOPS, GPU-hours, etc.)
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.engines.compute_stablecoin import (
    ComputeStablecoinEngine,
    StablecoinType,
    PegMechanism
)

router = APIRouter(
    prefix="/api/v1/compute-stablecoin",
    tags=["compute_stablecoin"]
)

# Global stablecoin engine instance (initialized in main.py)
stablecoin_engine: Optional[ComputeStablecoinEngine] = None


def set_stablecoin_engine(engine: ComputeStablecoinEngine):
    """Set the stablecoin engine instance from main.py"""
    global stablecoin_engine
    stablecoin_engine = engine


class CreateStablecoinRequest(BaseModel):
    """Request to create a new stablecoin"""
    symbol: str = Field(..., min_length=3, max_length=10, description="Coin symbol")
    name: str = Field(..., min_length=3, max_length=50, description="Coin name")
    coin_type: str = Field(..., description="Type of stablecoin")
    peg_mechanism: str = Field(..., description="Stabilization mechanism")
    peg_target: str = Field(..., description="Target value in compute units")
    initial_collateral_ratio: str = Field(default="1.0", description="Initial collateral ratio")
    rebase_enabled: bool = Field(default=False, description="Enable supply rebasing")


class MintStablecoinRequest(BaseModel):
    """Request to mint stablecoins"""
    coin_id: str = Field(..., description="Stablecoin ID")
    amount: str = Field(..., description="Amount to mint")
    collateral: Dict[str, str] = Field(..., description="Collateral by resource type")


class RedeemStablecoinRequest(BaseModel):
    """Request to redeem stablecoins"""
    coin_id: str = Field(..., description="Stablecoin ID")
    amount: str = Field(..., description="Amount to redeem")
    preferred_resource: Optional[str] = Field(None, description="Preferred compute resource")


class CreateBasketCoinRequest(BaseModel):
    """Request to create a basket-backed stablecoin"""
    symbol: str = Field(..., min_length=3, max_length=10)
    name: str = Field(..., min_length=3, max_length=50)
    basket_composition: Dict[str, str] = Field(..., description="Resource weights (must sum to 1)")


@router.post("/create")
async def create_stablecoin(
    request: CreateStablecoinRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Create a new compute-backed stablecoin
    
    Types:
    - FLOPS_COIN: Pegged to TFLOPS
    - GPU_HOUR_COIN: Pegged to GPU compute hours
    - STORAGE_COIN: Pegged to TB storage
    - BANDWIDTH_COIN: Pegged to Gbps bandwidth
    - COMPUTE_BASKET: Basket of compute resources
    
    Mechanisms:
    - ALGORITHMIC: Pure algorithmic stabilization
    - COLLATERALIZED: Backed by compute collateral
    - HYBRID: Mix of algorithmic and collateral
    - REBASE: Elastic supply rebase
    """
    if not stablecoin_engine:
        raise HTTPException(status_code=503, detail="Stablecoin engine not available")
    
    # Create stablecoin
    coin = await stablecoin_engine.create_stablecoin(
        symbol=request.symbol,
        name=request.name,
        coin_type=StablecoinType(request.coin_type),
        peg_mechanism=PegMechanism(request.peg_mechanism),
        peg_target=Decimal(request.peg_target),
        initial_collateral_ratio=Decimal(request.initial_collateral_ratio),
        rebase_enabled=request.rebase_enabled
    )
    
    return {
        "coin_id": coin.coin_id,
        "symbol": coin.symbol,
        "name": coin.name,
        "coin_type": coin.coin_type.value,
        "peg_mechanism": coin.peg_mechanism.value,
        "peg_target": str(coin.peg_target),
        "collateral_ratio": str(coin.collateral_ratio),
        "rebase_enabled": coin.rebase_enabled,
        "created_at": coin.created_at.isoformat()
    }


@router.post("/basket")
async def create_basket_coin(
    request: CreateBasketCoinRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Create a stablecoin backed by a basket of compute resources
    
    Provides diversified exposure to multiple compute types
    """
    if not stablecoin_engine:
        raise HTTPException(status_code=503, detail="Stablecoin engine not available")
    
    # Convert weights to Decimal
    basket_composition = {
        resource: Decimal(weight)
        for resource, weight in request.basket_composition.items()
    }
    
    # Create basket coin
    coin = await stablecoin_engine.create_compute_basket_coin(
        symbol=request.symbol,
        name=request.name,
        basket_composition=basket_composition
    )
    
    return {
        "coin_id": coin.coin_id,
        "symbol": coin.symbol,
        "name": coin.name,
        "coin_type": coin.coin_type.value,
        "peg_target": str(coin.peg_target),
        "basket_composition": request.basket_composition,
        "created_at": coin.created_at.isoformat()
    }


@router.post("/mint")
async def mint_stablecoin(
    request: MintStablecoinRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Mint new stablecoins by providing compute collateral
    
    Lock compute resources as collateral and receive stablecoins
    """
    if not stablecoin_engine:
        raise HTTPException(status_code=503, detail="Stablecoin engine not available")
    
    # Convert collateral to Decimal
    collateral = {
        resource: Decimal(amount)
        for resource, amount in request.collateral.items()
    }
    
    # Mint coins
    result = await stablecoin_engine.mint_stablecoin(
        user_id=current_user["user_id"],
        coin_id=request.coin_id,
        amount=Decimal(request.amount),
        collateral=collateral
    )
    
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error"))
    
    return result


@router.post("/redeem")
async def redeem_stablecoin(
    request: RedeemStablecoinRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Redeem stablecoins for compute resources
    
    Burn stablecoins and receive allocated compute capacity
    """
    if not stablecoin_engine:
        raise HTTPException(status_code=503, detail="Stablecoin engine not available")
    
    # Redeem coins
    result = await stablecoin_engine.redeem_stablecoin(
        user_id=current_user["user_id"],
        coin_id=request.coin_id,
        amount=Decimal(request.amount),
        preferred_resource=request.preferred_resource
    )
    
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error"))
    
    return result


@router.get("/price/{coin_id}")
async def get_coin_price(coin_id: str):
    """
    Get current price and peg status of a stablecoin
    
    Shows market price, deviation from peg, and collateral coverage
    """
    if not stablecoin_engine:
        raise HTTPException(status_code=503, detail="Stablecoin engine not available")
    
    price_data = await stablecoin_engine.get_coin_price(coin_id)
    
    return price_data


@router.post("/rebase/{coin_id}")
async def rebase_supply(
    coin_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Manually trigger supply rebase for elastic supply coins
    
    Adjusts all balances proportionally to maintain peg
    """
    if not stablecoin_engine:
        raise HTTPException(status_code=503, detail="Stablecoin engine not available")
    
    # Only authorized users can trigger rebase
    # In production, check governance permissions
    
    result = await stablecoin_engine.rebase_supply(coin_id)
    
    if not result["success"]:
        raise HTTPException(status_code=400, detail=result.get("error"))
    
    return result


@router.get("/balance")
async def get_balance(
    coin_id: Optional[str] = Query(None, description="Filter by coin ID"),
    current_user: dict = Depends(get_current_user)
):
    """Get user's stablecoin balances"""
    if not stablecoin_engine:
        raise HTTPException(status_code=503, detail="Stablecoin engine not available")
    
    balances = await stablecoin_engine.get_user_balance(
        current_user["user_id"],
        coin_id
    )
    
    return {"balances": balances}


@router.get("/list")
async def list_stablecoins():
    """List all available stablecoins"""
    if not stablecoin_engine:
        raise HTTPException(status_code=503, detail="Stablecoin engine not available")
    
    coins = []
    for coin in stablecoin_engine.stablecoins.values():
        price_data = await stablecoin_engine.get_coin_price(coin.coin_id)
        
        coins.append({
            "coin_id": coin.coin_id,
            "symbol": coin.symbol,
            "name": coin.name,
            "coin_type": coin.coin_type.value,
            "peg_mechanism": coin.peg_mechanism.value,
            "peg_target": str(coin.peg_target),
            "market_price": price_data["market_price"],
            "deviation_percent": price_data["deviation_percent"],
            "circulating_supply": str(coin.circulating_supply),
            "market_cap": str(coin.market_cap),
            "collateral_ratio": str(coin.collateral_ratio),
            "rebase_enabled": coin.rebase_enabled
        })
    
    return {"stablecoins": coins}


@router.get("/stats")
async def get_stablecoin_stats():
    """Get overall stablecoin market statistics"""
    if not stablecoin_engine:
        raise HTTPException(status_code=503, detail="Stablecoin engine not available")
    
    stats = await stablecoin_engine.get_stablecoin_stats()
    
    return stats


@router.get("/collateral/{coin_id}")
async def get_collateral_info(coin_id: str):
    """Get collateral information for a stablecoin"""
    if not stablecoin_engine:
        raise HTTPException(status_code=503, detail="Stablecoin engine not available")
    
    coin = stablecoin_engine.stablecoins.get(coin_id)
    if not coin:
        raise HTTPException(status_code=404, detail="Stablecoin not found")
    
    # Get vaults
    vaults = stablecoin_engine.vaults.get(coin_id, [])
    
    vault_info = []
    total_locked = Decimal("0")
    
    for vault in vaults:
        vault_info.append({
            "vault_id": vault.vault_id,
            "collateral_type": vault.collateral_type,
            "locked_amount": str(vault.locked_amount),
            "value_locked": str(vault.value_locked),
            "providers": len(vault.providers),
            "last_update": vault.last_update.isoformat()
        })
        total_locked += vault.value_locked
    
    return {
        "coin_id": coin_id,
        "total_collateral_value": str(total_locked),
        "collateral_ratio": str(coin.collateral_ratio),
        "required_collateral": str(coin.circulating_supply * coin.peg_target * coin.collateral_ratio),
        "collateral_health": "healthy" if total_locked >= coin.circulating_supply * coin.peg_target * coin.collateral_ratio else "undercollateralized",
        "vaults": vault_info
    }


@router.get("/arbitrage")
async def get_arbitrage_opportunities():
    """
    Get current arbitrage opportunities for maintaining peg
    
    Shows profitable trades to help stabilize coin prices
    """
    if not stablecoin_engine:
        raise HTTPException(status_code=503, detail="Stablecoin engine not available")
    
    opportunities = []
    
    for coin in stablecoin_engine.stablecoins.values():
        # Get current price
        price_data = await stablecoin_engine.get_coin_price(coin.coin_id)
        market_price = Decimal(price_data["market_price"])
        peg_target = coin.peg_target
        
        deviation = abs(market_price - peg_target) / peg_target
        
        if deviation > Decimal("0.01"):  # 1% deviation
            if market_price > peg_target:
                # Mint and sell opportunity
                opportunities.append({
                    "coin_id": coin.coin_id,
                    "symbol": coin.symbol,
                    "strategy": "mint_and_sell",
                    "market_price": str(market_price),
                    "peg_target": str(peg_target),
                    "profit_per_unit": str(market_price - peg_target),
                    "profit_percent": str((market_price - peg_target) / peg_target * 100),
                    "description": f"Mint {coin.symbol} at {peg_target} and sell at {market_price}"
                })
            else:
                # Buy and redeem opportunity
                opportunities.append({
                    "coin_id": coin.coin_id,
                    "symbol": coin.symbol,
                    "strategy": "buy_and_redeem",
                    "market_price": str(market_price),
                    "peg_target": str(peg_target),
                    "profit_per_unit": str(peg_target - market_price),
                    "profit_percent": str((peg_target - market_price) / market_price * 100),
                    "description": f"Buy {coin.symbol} at {market_price} and redeem at {peg_target}"
                })
    
    return {
        "opportunities": opportunities,
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/history/stabilization")
async def get_stabilization_history(
    coin_id: Optional[str] = Query(None, description="Filter by coin"),
    limit: int = Query(default=100, ge=1, le=1000)
):
    """Get history of stabilization actions"""
    if not stablecoin_engine:
        raise HTTPException(status_code=503, detail="Stablecoin engine not available")
    
    # In production, query from storage
    # For now, return empty
    return {
        "events": [],
        "total": 0
    }


@router.post("/transfer")
async def transfer_stablecoin(
    coin_id: str,
    to_user_id: str,
    amount: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Transfer stablecoins between users
    
    Simple balance transfer within the platform
    """
    if not stablecoin_engine:
        raise HTTPException(status_code=503, detail="Stablecoin engine not available")
    
    amount_decimal = Decimal(amount)
    from_user_id = current_user["user_id"]
    
    # Check balance
    from_balance = stablecoin_engine.balances[from_user_id].get(coin_id, Decimal("0"))
    if from_balance < amount_decimal:
        raise HTTPException(status_code=400, detail="Insufficient balance")
    
    # Transfer
    stablecoin_engine.balances[from_user_id][coin_id] -= amount_decimal
    stablecoin_engine.balances[to_user_id][coin_id] += amount_decimal
    
    # Emit event
    await stablecoin_engine.pulsar.publish('compute.stablecoin.transfer', {
        'coin_id': coin_id,
        'from_user': from_user_id,
        'to_user': to_user_id,
        'amount': str(amount_decimal),
        'timestamp': datetime.utcnow().isoformat()
    })
    
    return {
        "success": True,
        "coin_id": coin_id,
        "from_user": from_user_id,
        "to_user": to_user_id,
        "amount": str(amount_decimal),
        "new_balance": str(stablecoin_engine.balances[from_user_id][coin_id])
    }


@router.get("/compute-values")
async def get_compute_unit_values():
    """
    Get current values of different compute units
    
    Shows conversion rates between different compute resources
    """
    if not stablecoin_engine:
        raise HTTPException(status_code=503, detail="Stablecoin engine not available")
    
    # Get current prices
    gpu_price = await stablecoin_engine._get_compute_unit_price("gpu")
    cpu_price = await stablecoin_engine._get_compute_unit_price("cpu")
    storage_price = await stablecoin_engine._get_compute_unit_price("storage")
    bandwidth_price = await stablecoin_engine._get_compute_unit_price("bandwidth")
    
    # Get conversions
    gpu_flops = await stablecoin_engine._get_flops_per_unit("gpu")
    cpu_flops = await stablecoin_engine._get_flops_per_unit("cpu")
    
    return {
        "unit_prices": {
            "gpu_hour": str(gpu_price),
            "cpu_hour": str(cpu_price),
            "tb_storage_hour": str(storage_price),
            "gbps_bandwidth_hour": str(bandwidth_price)
        },
        "flops_conversions": {
            "gpu_tflops": str(gpu_flops),
            "cpu_tflops": str(cpu_flops)
        },
        "gpu_hour_equivalents": {
            "cpu_hours": "100",  # 1 GPU hour = 100 CPU hours
            "tpu_hours": "0.238", # 1 GPU hour = 0.238 TPU hours
            "storage_tb": "10",   # 1 GPU hour = 10 TB storage hours
        },
        "timestamp": datetime.utcnow().isoformat()
    } 