"""Futures trading API endpoints."""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Annotated
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.dependencies import (
    get_settings, get_cache_manager, get_funding_engine,
    get_settlement_engine, get_current_user
)
from app.models.futures import (
    FuturesContract, FuturesPosition, FuturesOrder,
    FundingRate, SettlementRecord, MarginRequirement,
    FuturesMarketStats, ContractType, PositionSide
)
from app.config import Settings
from app.cache.ignite_manager import FuturesCacheManager
from app.core.funding_engine import FundingEngine
from app.core.settlement_engine import SettlementEngine
from platformq_trading_common import publish_event, EventType


router = APIRouter()


# Request/Response Models

class CreateContractRequest(BaseModel):
    """Request to create a new futures contract."""
    symbol: str
    underlying_asset: str
    quote_asset: str
    contract_type: ContractType
    settlement_type: str
    contract_size: Decimal
    tick_size: Decimal
    expiry_date: Optional[datetime] = None
    initial_margin_rate: Decimal
    maintenance_margin_rate: Decimal
    max_leverage: int
    funding_interval_hours: Optional[int] = None


class OpenPositionRequest(BaseModel):
    """Request to open a futures position."""
    symbol: str
    side: PositionSide
    size: Decimal
    order_type: str = "market"
    price: Optional[Decimal] = None
    reduce_only: bool = False
    post_only: bool = False


class UpdateMarginRequest(BaseModel):
    """Request to update position margin."""
    position_id: str
    margin_delta: Decimal  # Positive to add, negative to remove


# Contract Endpoints

@router.post("/contracts", response_model=FuturesContract)
async def create_contract(
    request: CreateContractRequest,
    user_id: Annotated[str, Depends(get_current_user)],
    cache: Annotated[FuturesCacheManager, Depends(get_cache_manager)],
    funding_engine: Annotated[FundingEngine, Depends(get_funding_engine)]
):
    """Create a new futures contract (admin only)."""
    # In production, check admin permissions
    
    contract = FuturesContract(
        **request.dict(),
        created_at=datetime.utcnow()
    )
    
    # Store contract
    await cache.store_contract(contract)
    
    # Start funding cycle for perpetuals
    if contract.contract_type == ContractType.PERPETUAL:
        await funding_engine.start_funding_cycle(contract.symbol)
    
    # Publish event
    await publish_event(
        EventType.CONTRACT_CREATED,
        {
            "symbol": contract.symbol,
            "contract_type": contract.contract_type,
            "created_by": user_id,
            "timestamp": datetime.utcnow().isoformat()
        }
    )
    
    return contract


@router.get("/contracts", response_model=List[FuturesContract])
async def list_contracts(
    cache: Annotated[FuturesCacheManager, Depends(get_cache_manager)],
    active_only: bool = True
):
    """List available futures contracts."""
    if active_only:
        return await cache.get_active_contracts()
    
    # In production, implement full contract listing
    return await cache.get_active_contracts()


@router.get("/contracts/{symbol}", response_model=FuturesContract)
async def get_contract(
    symbol: str,
    cache: Annotated[FuturesCacheManager, Depends(get_cache_manager)]
):
    """Get futures contract details."""
    contract = await cache.get_contract(symbol)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    return contract


# Position Endpoints

@router.post("/positions/open", response_model=FuturesPosition)
async def open_position(
    request: OpenPositionRequest,
    user_id: Annotated[str, Depends(get_current_user)],
    cache: Annotated[FuturesCacheManager, Depends(get_cache_manager)],
    settings: Annotated[Settings, Depends(get_settings)]
):
    """Open a new futures position."""
    # Get contract details
    contract = await cache.get_contract(request.symbol)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    
    if not contract.is_active:
        raise HTTPException(status_code=400, detail="Contract is not active")
    
    # Calculate required margin
    position_value = request.size * request.price if request.price else request.size * Decimal("50000")  # Use market price
    required_margin = position_value * contract.initial_margin_rate
    
    # Check user balance (in production)
    # ...
    
    # Create position
    position = FuturesPosition(
        position_id=str(uuid.uuid4()),
        user_id=user_id,
        symbol=request.symbol,
        side=request.side,
        size=request.size,
        entry_price=request.price or Decimal("50000"),  # Use actual market price
        mark_price=request.price or Decimal("50000"),
        unrealized_pnl=Decimal("0"),
        realized_pnl=Decimal("0"),
        margin_used=required_margin,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )
    
    # Calculate liquidation price
    if position.side == PositionSide.LONG:
        position.liquidation_price = position.entry_price * (
            Decimal("1") - contract.maintenance_margin_rate
        )
    else:
        position.liquidation_price = position.entry_price * (
            Decimal("1") + contract.maintenance_margin_rate
        )
    
    # Store position
    await cache.store_position(position)
    
    # Create order
    order = FuturesOrder(
        order_id=str(uuid.uuid4()),
        user_id=user_id,
        symbol=request.symbol,
        side=request.side,
        size=request.size,
        price=request.price,
        order_type=request.order_type,
        reduce_only=request.reduce_only,
        post_only=request.post_only,
        status="filled" if request.order_type == "market" else "pending"
    )
    
    await cache.store_order(order)
    
    # Publish event
    await publish_event(
        EventType.POSITION_OPENED,
        {
            "position_id": position.position_id,
            "user_id": user_id,
            "symbol": request.symbol,
            "side": request.side.value,
            "size": str(request.size),
            "entry_price": str(position.entry_price),
            "margin": str(required_margin),
            "timestamp": datetime.utcnow().isoformat()
        }
    )
    
    return position


@router.get("/positions", response_model=List[FuturesPosition])
async def get_positions(
    user_id: Annotated[str, Depends(get_current_user)],
    cache: Annotated[FuturesCacheManager, Depends(get_cache_manager)],
    symbol: Optional[str] = None
):
    """Get user's futures positions."""
    return await cache.get_user_positions(user_id, symbol)


@router.post("/positions/close/{position_id}")
async def close_position(
    position_id: str,
    user_id: Annotated[str, Depends(get_current_user)],
    cache: Annotated[FuturesCacheManager, Depends(get_cache_manager)]
):
    """Close a futures position."""
    position = await cache.get_position(position_id)
    if not position:
        raise HTTPException(status_code=404, detail="Position not found")
    
    if position.user_id != user_id:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # In production, execute market order to close
    # For now, just close the position
    
    # Calculate final P&L
    final_pnl = position.realized_pnl + position.unrealized_pnl
    
    # Return margin
    # await return_margin(user_id, position.margin_used)
    
    # Close position
    await cache.close_position(position_id)
    
    # Publish event
    await publish_event(
        EventType.POSITION_CLOSED,
        {
            "position_id": position_id,
            "user_id": user_id,
            "symbol": position.symbol,
            "final_pnl": str(final_pnl),
            "timestamp": datetime.utcnow().isoformat()
        }
    )
    
    return {"message": "Position closed", "final_pnl": str(final_pnl)}


@router.post("/positions/margin")
async def update_margin(
    request: UpdateMarginRequest,
    user_id: Annotated[str, Depends(get_current_user)],
    cache: Annotated[FuturesCacheManager, Depends(get_cache_manager)]
):
    """Add or remove margin from a position."""
    position = await cache.get_position(request.position_id)
    if not position:
        raise HTTPException(status_code=404, detail="Position not found")
    
    if position.user_id != user_id:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Update margin
    new_margin = position.margin_used + request.margin_delta
    if new_margin < 0:
        raise HTTPException(status_code=400, detail="Invalid margin amount")
    
    position.margin_used = new_margin
    
    # Recalculate liquidation price
    contract = await cache.get_contract(position.symbol)
    if contract:
        position_value = position.size * position.mark_price
        margin_ratio = new_margin / position_value
        
        if position.side == PositionSide.LONG:
            position.liquidation_price = position.mark_price * (
                Decimal("1") - margin_ratio + contract.maintenance_margin_rate
            )
        else:
            position.liquidation_price = position.mark_price * (
                Decimal("1") + margin_ratio - contract.maintenance_margin_rate
            )
    
    await cache.update_position(position)
    
    return {"message": "Margin updated", "new_margin": str(new_margin)}


# Funding Endpoints

@router.get("/funding/{symbol}", response_model=FundingRate)
async def get_funding_rate(
    symbol: str,
    cache: Annotated[FuturesCacheManager, Depends(get_cache_manager)]
):
    """Get current funding rate for a perpetual contract."""
    funding_rate = await cache.get_current_funding_rate(symbol)
    if not funding_rate:
        raise HTTPException(status_code=404, detail="Funding rate not found")
    return funding_rate


@router.get("/funding/{symbol}/history", response_model=List[FundingRate])
async def get_funding_history(
    symbol: str,
    cache: Annotated[FuturesCacheManager, Depends(get_cache_manager)],
    limit: int = Query(100, ge=1, le=1000)
):
    """Get funding rate history for a perpetual contract."""
    return await cache.get_funding_history(symbol, limit)


# Settlement Endpoints

@router.get("/settlements", response_model=List[SettlementRecord])
async def get_settlements(
    settlement_engine: Annotated[SettlementEngine, Depends(get_settlement_engine)],
    symbol: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000)
):
    """Get settlement history."""
    return await settlement_engine.get_settlement_history(symbol, limit)


# Market Data Endpoints

@router.get("/market/{symbol}", response_model=FuturesMarketStats)
async def get_market_stats(
    symbol: str,
    cache: Annotated[FuturesCacheManager, Depends(get_cache_manager)]
):
    """Get market statistics for a futures contract."""
    stats = await cache.get_market_stats(symbol)
    if not stats:
        # Return default stats
        stats = FuturesMarketStats(
            symbol=symbol,
            last_price=Decimal("0"),
            mark_price=Decimal("0"),
            index_price=Decimal("0"),
            volume_24h=Decimal("0"),
            turnover_24h=Decimal("0"),
            open_interest=Decimal("0"),
            high_24h=Decimal("0"),
            low_24h=Decimal("0"),
            price_change_24h=Decimal("0"),
            price_change_percent_24h=Decimal("0")
        )
    return stats


@router.get("/margin-requirements/{symbol}")
async def get_margin_requirements(
    symbol: str,
    user_id: Annotated[str, Depends(get_current_user)],
    cache: Annotated[FuturesCacheManager, Depends(get_cache_manager)],
    size: Decimal = Query(..., description="Position size"),
    side: PositionSide = Query(...),
    price: Optional[Decimal] = None
):
    """Calculate margin requirements for a potential position."""
    contract = await cache.get_contract(symbol)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")
    
    # Get current price if not provided
    if not price:
        stats = await cache.get_market_stats(symbol)
        price = stats.mark_price if stats else Decimal("50000")
    
    # Calculate position value
    position_value = size * price
    
    # Calculate margins
    initial_margin = position_value * contract.initial_margin_rate
    maintenance_margin = position_value * contract.maintenance_margin_rate
    
    # Calculate liquidation price
    if side == PositionSide.LONG:
        liquidation_price = price * (Decimal("1") - contract.maintenance_margin_rate)
    else:
        liquidation_price = price * (Decimal("1") + contract.maintenance_margin_rate)
    
    return MarginRequirement(
        user_id=user_id,
        symbol=symbol,
        initial_margin=initial_margin,
        maintenance_margin=maintenance_margin,
        current_margin=initial_margin,
        margin_ratio=Decimal("1"),
        available_balance=Decimal("100000"),  # Mock balance
        liquidation_price=liquidation_price
    ) 