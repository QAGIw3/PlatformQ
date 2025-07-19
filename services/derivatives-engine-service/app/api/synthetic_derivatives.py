"""
Synthetic Derivatives API

API endpoints for universal synthetic derivatives platform.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime
from pydantic import BaseModel, Field

from app.auth.dependencies import get_current_user
from app.engines.synthetic_derivatives_engine import (
    SyntheticDerivativesEngine,
    SyntheticAssetType,
    SyntheticInstrumentType,
    CollateralType
)
from app.integrations import get_synthetic_derivatives_engine

router = APIRouter(prefix="/synthetic", tags=["synthetic_derivatives"])


# Request/Response Models
class CreateSyntheticAssetRequest(BaseModel):
    name: str = Field(..., description="Asset name")
    symbol: str = Field(..., description="Asset symbol")
    asset_type: SyntheticAssetType = Field(..., description="Type of synthetic asset")
    price_feeds: List[str] = Field(..., description="Oracle price feed IDs")
    underlying_reference: str = Field(..., description="Reference to underlying asset")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional metadata")


class OpenSyntheticPositionRequest(BaseModel):
    synthetic_asset_id: str = Field(..., description="ID of synthetic asset")
    size: Decimal = Field(..., description="Position size (notional)")
    direction: str = Field(..., description="Position direction: long or short")
    collateral: Dict[str, Decimal] = Field(..., description="Collateral assets and amounts")
    instrument_type: SyntheticInstrumentType = Field(default=SyntheticInstrumentType.PERPETUAL)
    expires_at: Optional[datetime] = Field(default=None, description="Expiry for non-perpetual positions")


class CreateSyntheticIndexRequest(BaseModel):
    name: str = Field(..., description="Index name")
    symbol: str = Field(..., description="Index symbol")
    components: List[Dict[str, Any]] = Field(..., description="Component assets with weights")
    rebalance_frequency: str = Field(default="monthly", description="Rebalancing frequency")
    methodology: Optional[Dict[str, Any]] = Field(default=None, description="Index methodology")


class CreateSyntheticDerivativeRequest(BaseModel):
    underlying_asset_id: str = Field(..., description="Underlying asset or index ID")
    instrument_type: SyntheticInstrumentType = Field(..., description="Type of derivative")
    contract_size: Decimal = Field(default=Decimal("1"), description="Contract size")
    expiry_date: Optional[datetime] = Field(default=None, description="Expiry date")
    strike_price: Optional[Decimal] = Field(default=None, description="Strike price for options")
    option_type: Optional[str] = Field(default=None, description="Option type: call or put")


# Endpoints
@router.post("/assets")
async def create_synthetic_asset(
    request: CreateSyntheticAssetRequest,
    current_user=Depends(get_current_user),
    engine: SyntheticDerivativesEngine = Depends(get_synthetic_derivatives_engine)
) -> Dict[str, Any]:
    """Create a new synthetic asset"""
    try:
        asset = await engine.create_synthetic_asset(
            name=request.name,
            symbol=request.symbol,
            asset_type=request.asset_type,
            price_feeds=request.price_feeds,
            underlying_reference=request.underlying_reference,
            metadata=request.metadata
        )
        
        return {
            "asset_id": asset.asset_id,
            "symbol": asset.symbol,
            "asset_type": asset.asset_type.value,
            "volatility_30d": str(asset.volatility_30d),
            "liquidity_score": str(asset.liquidity_score),
            "created_at": asset.created_at.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/assets")
async def list_synthetic_assets(
    asset_type: Optional[SyntheticAssetType] = Query(None),
    active_only: bool = Query(True),
    current_user=Depends(get_current_user),
    engine: SyntheticDerivativesEngine = Depends(get_synthetic_derivatives_engine)
) -> List[Dict[str, Any]]:
    """List available synthetic assets"""
    assets = []
    
    for asset in engine.synthetic_assets.values():
        if active_only and not asset.is_active:
            continue
            
        if asset_type and asset.asset_type != asset_type:
            continue
            
        assets.append({
            "asset_id": asset.asset_id,
            "name": asset.name,
            "symbol": asset.symbol,
            "asset_type": asset.asset_type.value,
            "underlying_reference": asset.underlying_reference,
            "volatility_30d": str(asset.volatility_30d),
            "liquidity_score": str(asset.liquidity_score),
            "is_active": asset.is_active
        })
        
    return assets


@router.get("/assets/{asset_id}")
async def get_synthetic_asset(
    asset_id: str,
    current_user=Depends(get_current_user),
    engine: SyntheticDerivativesEngine = Depends(get_synthetic_derivatives_engine)
) -> Dict[str, Any]:
    """Get details of a synthetic asset"""
    asset = engine.synthetic_assets.get(asset_id)
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
        
    # Get current price
    try:
        current_price = await engine._get_asset_price(asset)
    except:
        current_price = None
        
    return {
        "asset_id": asset.asset_id,
        "name": asset.name,
        "symbol": asset.symbol,
        "asset_type": asset.asset_type.value,
        "price_feeds": asset.price_feeds,
        "underlying_reference": asset.underlying_reference,
        "current_price": str(current_price) if current_price else None,
        "volatility_30d": str(asset.volatility_30d),
        "liquidity_score": str(asset.liquidity_score),
        "metadata": asset.metadata,
        "is_active": asset.is_active,
        "created_at": asset.created_at.isoformat()
    }


@router.post("/positions/open")
async def open_synthetic_position(
    request: OpenSyntheticPositionRequest,
    current_user=Depends(get_current_user),
    engine: SyntheticDerivativesEngine = Depends(get_synthetic_derivatives_engine)
) -> Dict[str, Any]:
    """Open a synthetic position"""
    try:
        position = await engine.open_synthetic_position(
            user_id=current_user.user_id,
            synthetic_asset_id=request.synthetic_asset_id,
            size=request.size,
            direction=request.direction,
            collateral=request.collateral,
            instrument_type=request.instrument_type,
            expires_at=request.expires_at
        )
        
        return {
            "position_id": position.position_id,
            "synthetic_asset_id": position.synthetic_asset_id,
            "size": str(position.size),
            "direction": position.direction,
            "entry_price": str(position.entry_price),
            "leverage": str(position.leverage),
            "margin_ratio": str(position.margin_ratio),
            "collateral_amount": str(position.collateral_amount),
            "opened_at": position.opened_at.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/positions/{position_id}/close")
async def close_synthetic_position(
    position_id: str,
    current_user=Depends(get_current_user),
    engine: SyntheticDerivativesEngine = Depends(get_synthetic_derivatives_engine)
) -> Dict[str, Any]:
    """Close a synthetic position"""
    try:
        result = await engine.close_synthetic_position(
            position_id=position_id,
            user_id=current_user.user_id
        )
        return result
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/positions")
async def list_user_positions(
    active_only: bool = Query(True),
    current_user=Depends(get_current_user),
    engine: SyntheticDerivativesEngine = Depends(get_synthetic_derivatives_engine)
) -> List[Dict[str, Any]]:
    """List user's synthetic positions"""
    positions = []
    
    for position in engine.synthetic_positions.values():
        if position.user_id != current_user.user_id:
            continue
            
        if active_only and not position.is_active:
            continue
            
        # Get asset info
        asset = engine.synthetic_assets.get(position.synthetic_asset_id)
        
        positions.append({
            "position_id": position.position_id,
            "asset_symbol": asset.symbol if asset else "UNKNOWN",
            "size": str(position.size),
            "direction": position.direction,
            "entry_price": str(position.entry_price),
            "unrealized_pnl": str(position.unrealized_pnl),
            "leverage": str(position.leverage),
            "margin_ratio": str(position.margin_ratio),
            "is_active": position.is_active,
            "opened_at": position.opened_at.isoformat()
        })
        
    return positions


@router.post("/indices")
async def create_synthetic_index(
    request: CreateSyntheticIndexRequest,
    current_user=Depends(get_current_user),
    engine: SyntheticDerivativesEngine = Depends(get_synthetic_derivatives_engine)
) -> Dict[str, Any]:
    """Create a custom synthetic index"""
    try:
        # Convert components format
        components = [
            (comp["asset_id"], Decimal(str(comp["weight"])))
            for comp in request.components
        ]
        
        index = await engine.create_synthetic_index(
            name=request.name,
            symbol=request.symbol,
            components=components,
            rebalance_frequency=request.rebalance_frequency,
            methodology=request.methodology
        )
        
        return {
            "index_id": index.index_id,
            "symbol": index.symbol,
            "base_value": str(index.base_value),
            "current_value": str(index.current_value),
            "component_count": len(index.components),
            "rebalance_frequency": index.rebalance_frequency,
            "created_at": index.last_rebalance.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/indices")
async def list_synthetic_indices(
    current_user=Depends(get_current_user),
    engine: SyntheticDerivativesEngine = Depends(get_synthetic_derivatives_engine)
) -> List[Dict[str, Any]]:
    """List available synthetic indices"""
    indices = []
    
    for index in engine.synthetic_indices.values():
        if not index.is_active:
            continue
            
        indices.append({
            "index_id": index.index_id,
            "name": index.name,
            "symbol": index.symbol,
            "current_value": str(index.current_value),
            "component_count": len(index.components),
            "rebalance_frequency": index.rebalance_frequency,
            "last_rebalance": index.last_rebalance.isoformat()
        })
        
    return indices


@router.post("/derivatives")
async def create_synthetic_derivative(
    request: CreateSyntheticDerivativeRequest,
    current_user=Depends(get_current_user),
    engine: SyntheticDerivativesEngine = Depends(get_synthetic_derivatives_engine)
) -> Dict[str, Any]:
    """Create a derivative on a synthetic asset"""
    try:
        derivative = await engine.create_synthetic_derivative(
            underlying_asset_id=request.underlying_asset_id,
            instrument_type=request.instrument_type,
            contract_size=request.contract_size,
            expiry_date=request.expiry_date,
            strike_price=request.strike_price,
            option_type=request.option_type
        )
        
        return {
            "derivative_id": derivative.derivative_id,
            "underlying_asset_id": derivative.underlying_asset_id,
            "instrument_type": derivative.instrument_type.value,
            "contract_size": str(derivative.contract_size),
            "expiry_date": derivative.expiry_date.isoformat() if derivative.expiry_date else None,
            "strike_price": str(derivative.strike_price) if derivative.strike_price else None,
            "option_type": derivative.option_type,
            "is_tradable": derivative.is_tradable
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/stats")
async def get_synthetic_stats(
    current_user=Depends(get_current_user),
    engine: SyntheticDerivativesEngine = Depends(get_synthetic_derivatives_engine)
) -> Dict[str, Any]:
    """Get platform-wide synthetic derivatives statistics"""
    # Calculate stats
    total_assets = len(engine.synthetic_assets)
    active_assets = sum(1 for a in engine.synthetic_assets.values() if a.is_active)
    
    total_positions = len(engine.synthetic_positions)
    active_positions = sum(1 for p in engine.synthetic_positions.values() if p.is_active)
    
    total_indices = len(engine.synthetic_indices)
    total_derivatives = len(engine.synthetic_derivatives)
    
    # Calculate total value locked
    total_collateral = Decimal("0")
    for position in engine.synthetic_positions.values():
        if position.is_active:
            total_collateral += position.collateral_amount
            
    return {
        "synthetic_assets": {
            "total": total_assets,
            "active": active_assets
        },
        "positions": {
            "total": total_positions,
            "active": active_positions
        },
        "indices": {
            "total": total_indices
        },
        "derivatives": {
            "total": total_derivatives
        },
        "total_value_locked": str(total_collateral)
    } 